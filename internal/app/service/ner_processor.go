// internal/app/service/ner_processor.go
package service

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/terratensor/geoupdater/internal/core/domain"
	"github.com/terratensor/geoupdater/internal/core/ports"
	"github.com/terratensor/geoupdater/internal/version"
)

// NERProcessor обработчик NER данных
type NERProcessor struct {
	repo       ports.NERRepository
	parser     ports.Parser
	failedRepo ports.FailedRecordsRepository
	logger     ports.Logger
	metrics    ports.MetricsCollector
	config     *NERConfig
	stats      *NERStats
	mu         sync.RWMutex
}

// NERConfig конфигурация NER процессора
type NERConfig struct {
	BatchSize  int
	Workers    int
	MaxRetries int
	SaveFailed bool
}

// NERStats статистика NER обработки
type NERStats struct {
	TotalProcessed int64
	TotalSuccess   int64
	TotalFailed    int64
	TotalSkipped   int64
	TotalFiles     int
	StartTime      time.Time
	LastProcessed  time.Time
}

// NewNERProcessor создает новый NER процессор
func NewNERProcessor(
	repo ports.NERRepository,
	parser ports.Parser,
	failedRepo ports.FailedRecordsRepository,
	logger ports.Logger,
	metrics ports.MetricsCollector,
	config *NERConfig,
) *NERProcessor {

	if config == nil {
		config = &NERConfig{
			BatchSize:  1000,
			Workers:    5,
			MaxRetries: 3,
			SaveFailed: true,
		}
	}

	return &NERProcessor{
		repo:       repo,
		parser:     parser,
		failedRepo: failedRepo,
		logger:     logger,
		metrics:    metrics,
		config:     config,
		stats: &NERStats{
			StartTime: time.Now(),
		},
	}
}

// ProcessDocuments обрабатывает поток NER данных
func (p *NERProcessor) ProcessDocuments(ctx context.Context, dataChan <-chan *domain.NERData) (*domain.BatchResult, error) {
	p.logger.Info("starting NER document processing")

	overallResult := domain.NewBatchResult()
	documentCount := 0

	// Канал для передачи документов на обновление
	updateChan := make(chan *domain.NERDocument, p.config.BatchSize*2)
	resultChan := make(chan *domain.BatchResult, p.config.Workers)

	// Запускаем воркеров
	var wgWorkers sync.WaitGroup
	for i := 0; i < p.config.Workers; i++ {
		wgWorkers.Add(1)
		go p.updateWorker(ctx, i, updateChan, resultChan, &wgWorkers)
	}

	// Сбор результатов
	resultDone := make(chan struct{})
	go func() {
		for result := range resultChan {
			if result != nil {
				overallResult.Merge(result)
			}
		}
		close(resultDone)
	}()

	// Основной цикл обработки
	processDone := make(chan struct{})
	go func() {
		defer close(updateChan)
		defer close(processDone)

		batch := make([]*domain.NERData, 0, p.config.BatchSize)

		for data := range dataChan {
			documentCount++

			select {
			case <-ctx.Done():
				p.logger.Info("context done, stopping NER processing",
					ports.Int("processed", documentCount))
				return
			default:
			}

			batch = append(batch, data)

			if len(batch) >= p.config.BatchSize {
				p.processBatch(ctx, batch, updateChan)
				batch = make([]*domain.NERData, 0, p.config.BatchSize)
			}
		}

		if len(batch) > 0 {
			p.processBatch(ctx, batch, updateChan)
		}

		p.logger.Info("finished processing all NER data",
			ports.Int("total_records", documentCount))
	}()

	// Ожидание завершения
	<-processDone
	p.logger.Debug("NER processing done, waiting for workers")

	wgWorkers.Wait()
	p.logger.Debug("all NER workers finished")
	close(resultChan)

	<-resultDone
	p.logger.Debug("all NER results collected")

	// Устанавливаем правильное количество документов
	overallResult.Total = documentCount
	if overallResult.Failed == 0 {
		overallResult.Success = documentCount
	}

	// Обновляем статистику
	p.mu.Lock()
	p.stats.TotalProcessed += int64(documentCount)
	p.stats.TotalSuccess += int64(overallResult.Success)
	p.stats.TotalFailed += int64(overallResult.Failed)
	p.stats.TotalSkipped += int64(overallResult.Skipped)
	p.stats.LastProcessed = time.Now()
	p.mu.Unlock()

	return overallResult, nil
}

// processBatch обрабатывает батч NER данных
func (p *NERProcessor) processBatch(ctx context.Context, batch []*domain.NERData, updateChan chan<- *domain.NERDocument) {
	p.logger.Debug("processing NER batch", ports.Int("batch_size", len(batch)))

	for _, data := range batch {
		doc := domain.NewNERDocumentFromData(data)

		if doc.DocID == 0 {
			p.logger.Error("invalid document: missing doc_id",
				ports.Any("data", data))

			if p.config.SaveFailed {
				p.saveFailedRecord(ctx, data, fmt.Errorf("missing doc_id"), "")
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case updateChan <- doc:
		}
	}
}

// updateWorker воркер для обновления NER документов
func (p *NERProcessor) updateWorker(ctx context.Context, id int,
	updateChan <-chan *domain.NERDocument,
	resultChan chan<- *domain.BatchResult,
	wg *sync.WaitGroup) {

	defer wg.Done()

	p.logger.Debug("NER update worker started", ports.Int("worker_id", id))

	batch := make([]*domain.NERDocument, 0, p.config.BatchSize)
	batchResult := domain.NewBatchResult()

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		p.logger.Debug("NER worker flushing batch",
			ports.Int("worker_id", id),
			ports.Int("batch_size", len(batch)))

		result, err := p.repo.BulkUpdate(ctx, batch)
		if err != nil {
			p.logger.Error("NER bulk update failed",
				ports.Int("worker_id", id),
				ports.Int("batch_size", len(batch)),
				ports.Error(err))

			for _, doc := range batch {
				batchResult.AddFailed(doc.DocID, err, domain.ErrorTypeManticore, 1)
			}
		} else {
			batchResult.Merge(result)
		}

		batch = make([]*domain.NERDocument, 0, p.config.BatchSize)
	}

	for {
		select {
		case <-ctx.Done():
			flushBatch()
			select {
			case resultChan <- batchResult:
			default:
			}
			return

		case doc, ok := <-updateChan:
			if !ok {
				flushBatch()
				select {
				case resultChan <- batchResult:
				default:
				}
				return
			}

			batch = append(batch, doc)

			if len(batch) >= p.config.BatchSize {
				flushBatch()
			}
		}
	}
}

// saveFailedRecord сохраняет неудачную NER запись
func (p *NERProcessor) saveFailedRecord(ctx context.Context, data *domain.NERData, err error, filename string) {
	if p.failedRepo == nil || !p.config.SaveFailed {
		return
	}

	// Конвертируем NERData в GeoUpdateData для совместимости
	geoData := &domain.GeoUpdateData{
		DocID:           data.DocID,
		GeohashesString: []string{},
		GeohashesUint64: []uint64{},
	}

	record := domain.NewFailedRecord(geoData, err, filename)

	if err := p.failedRepo.Save(ctx, record); err != nil {
		p.logger.Error("failed to save failed NER record",
			ports.Uint64("doc_id", data.DocID),
			ports.Error(err))
	}
}

// ProcessFile обрабатывает один NER файл
func (p *NERProcessor) ProcessFile(ctx context.Context, filename string) (*domain.BatchResult, error) {
	p.logger.Info("processing NER file", ports.String("filename", filename))

	start := time.Now()
	defer func() {
		p.metrics.RecordFileProcessed(filename, 0, time.Since(start))
	}()

	dataChan, errChan := p.parser.ParseNERFile(ctx, filename)

	// Собираем ошибки парсинга
	go func() {
		for err := range errChan {
			p.logger.Error("NER parse error",
				ports.String("filename", filename),
				ports.Error(err))
		}
	}()

	result, err := p.ProcessDocuments(ctx, dataChan)
	if err != nil {
		return nil, fmt.Errorf("failed to process NER documents: %w", err)
	}

	p.mu.Lock()
	p.stats.TotalFiles++
	p.mu.Unlock()

	return result, nil
}

// processFileWithReport обрабатывает файл и возвращает детальный отчет
func (p *NERProcessor) processFileWithReport(ctx context.Context, filename string) (*domain.FileReport, *domain.BatchResult, error) {
	p.logger.Info("processing NER file", ports.String("filename", filename))

	startTime := time.Now()

	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Парсим файл и собираем все данные для подсчета
	dataChan, errChan := p.parser.ParseNERFile(ctx, filename)

	var allData []*domain.NERData
	var firstID, lastID uint64
	var parseErrors int

	for data := range dataChan {
		allData = append(allData, data)
		if firstID == 0 {
			firstID = data.DocID
		}
		lastID = data.DocID
	}

	// Проверяем ошибки парсинга
	select {
	case err := <-errChan:
		if err != nil {
			parseErrors++
			p.logger.Error("parse error", ports.Error(err))
		}
	default:
	}

	if len(allData) == 0 {
		return nil, nil, fmt.Errorf("no valid data in file")
	}

	// Создаем новый канал для обработки
	processChan := make(chan *domain.NERData, len(allData))
	for _, data := range allData {
		processChan <- data
	}
	close(processChan)

	// Обрабатываем документы
	result, err := p.ProcessDocuments(ctx, processChan)
	if err != nil {
		return nil, nil, err
	}

	endTime := time.Now()

	fileReport := &domain.FileReport{
		Filename:  filename,
		Size:      fileInfo.Size(),
		Lines:     len(allData) + parseErrors,
		Valid:     len(allData),
		Errors:    parseErrors,
		StartTime: startTime,
		EndTime:   endTime,
		Duration:  endTime.Sub(startTime).String(),
		Success:   result.Success,
		Failed:    result.Failed,
		Skipped:   result.Skipped,
		FirstID:   firstID,
		LastID:    lastID,
	}

	p.logger.Info("NER file processing completed",
		ports.String("filename", filename),
		ports.Int("documents", len(allData)),
		ports.String("result", result.Summary()))

	return fileReport, result, nil
}

// ProcessFiles обрабатывает несколько NER файлов
func (p *NERProcessor) ProcessFiles(ctx context.Context, filenames []string) (*domain.BatchResult, error) {
	p.logger.Info("processing multiple NER files", ports.Int("count", len(filenames)))

	overallResult := domain.NewBatchResult()

	for _, filename := range filenames {
		select {
		case <-ctx.Done():
			return overallResult, ctx.Err()
		default:
			result, err := p.ProcessFile(ctx, filename)
			if err != nil {
				p.logger.Error("failed to process NER file",
					ports.String("filename", filename),
					ports.Error(err))
			}
			if result != nil {
				overallResult.Merge(result)
			}
		}
	}

	return overallResult, nil
}

// ProcessFilesWithReport обрабатывает файлы и возвращает отчет
func (p *NERProcessor) ProcessFilesWithReport(ctx context.Context, filenames []string) (*domain.ProcessingReport, error) {
	p.logger.Info("processing multiple NER files", ports.Int("count", len(filenames)))

	report := domain.NewProcessingReport(
		version.Short(),
		"ner",
		p.config.Workers,
		p.config.BatchSize,
	)

	totalDocuments := 0

	for _, filename := range filenames {
		select {
		case <-ctx.Done():
			report.Complete()
			return report, ctx.Err()
		default:
			fileReport, result, err := p.processFileWithReport(ctx, filename)
			if err != nil {
				p.logger.Error("failed to process NER file",
					ports.String("filename", filename),
					ports.Error(err))
				fileReport = &domain.FileReport{
					Filename: filename,
					Errors:   1,
				}
			}
			if fileReport != nil {
				report.AddFile(*fileReport)
			}
			if result != nil {
				totalDocuments += fileReport.Valid
			}
		}
	}

	report.Complete()

	// Выводим сводку
	fmt.Println("\n=== NER Processing Report ===")
	fmt.Printf("Version:     %s\n", report.Version)
	fmt.Printf("Mode:        %s\n", report.Mode)
	fmt.Printf("Duration:    %s\n", report.Duration)
	fmt.Printf("Files:       %d\n", report.TotalFiles)
	fmt.Printf("Documents:   %d\n", totalDocuments)
	fmt.Printf("Success:     %d\n", report.Stats.TotalSuccess)
	fmt.Printf("Failed:      %d\n", report.Stats.TotalFailed)
	fmt.Printf("Skipped:     %d\n", report.Stats.TotalSkipped)
	fmt.Println("================================")

	return report, nil
}

// GetStats возвращает статистику NER обработки
func (p *NERProcessor) GetStats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	now := time.Now()

	return map[string]interface{}{
		"total_processed": p.stats.TotalProcessed,
		"total_success":   p.stats.TotalSuccess,
		"total_failed":    p.stats.TotalFailed,
		"total_skipped":   p.stats.TotalSkipped,
		"total_files":     p.stats.TotalFiles,
		"uptime_seconds":  now.Sub(p.stats.StartTime).Seconds(),
		"last_processed":  p.stats.LastProcessed,
	}
}
