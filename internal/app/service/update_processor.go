// internal/app/service/update_processor.go - исправленная версия с отчетом
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

// UpdateProcessor реализует бизнес-логику обновления документов
type UpdateProcessor struct {
	repo       ports.Repository
	parser     ports.Parser
	failedRepo ports.FailedRecordsRepository
	logger     ports.Logger
	metrics    ports.MetricsCollector
	config     *Config
	stats      *ProcessorStats
	mu         sync.RWMutex
}

// Config конфигурация процессора
type Config struct {
	UpdateMode      domain.UpdateMode
	BatchSize       int
	Workers         int
	MaxRetries      int
	RetryDelay      time.Duration
	SaveFailed      bool
	EnableReprocess bool
}

// ProcessorStats статистика работы процессора
type ProcessorStats struct {
	TotalProcessed int64
	TotalSuccess   int64
	TotalFailed    int64
	TotalSkipped   int64
	TotalFiles     int
	ProcessingTime time.Duration
	StartTime      time.Time
	LastProcessed  time.Time
}

// NewUpdateProcessor создает новый экземпляр UpdateProcessor
func NewUpdateProcessor(
	repo ports.Repository,
	parser ports.Parser,
	failedRepo ports.FailedRecordsRepository,
	logger ports.Logger,
	metrics ports.MetricsCollector,
	config *Config,
) *UpdateProcessor {

	if config == nil {
		config = &Config{
			UpdateMode:      domain.ModeMerge,
			BatchSize:       1000,
			Workers:         5,
			MaxRetries:      3,
			RetryDelay:      1 * time.Second,
			SaveFailed:      true,
			EnableReprocess: true,
		}
	}

	return &UpdateProcessor{
		repo:       repo,
		parser:     parser,
		failedRepo: failedRepo,
		logger:     logger,
		metrics:    metrics,
		config:     config,
		stats: &ProcessorStats{
			StartTime: time.Now(),
		},
	}
}

// ProcessDocuments обрабатывает поток документов с данными для обновления
func (p *UpdateProcessor) ProcessDocuments(ctx context.Context, dataChan <-chan *domain.GeoUpdateData) (*domain.BatchResult, error) {
	p.logger.Info("starting document processing")

	overallResult := domain.NewBatchResult()

	// Канал для передачи документов на обновление
	updateChan := make(chan *domain.Document, p.config.BatchSize*2)
	resultChan := make(chan *domain.BatchResult, p.config.Workers)

	// Запускаем воркеров для обновления
	var wgWorkers sync.WaitGroup
	for i := 0; i < p.config.Workers; i++ {
		wgWorkers.Add(1)
		go p.updateWorker(ctx, i, updateChan, resultChan, &wgWorkers)
	}

	// Горутина для сбора результатов
	resultDone := make(chan struct{})
	go func() {
		for result := range resultChan {
			if result != nil {
				p.logger.Debug("received result from worker",
					ports.String("summary", result.Summary()))
				overallResult.Merge(result)
			}
		}
		close(resultDone)
	}()

	// Основной цикл обработки данных
	processDone := make(chan struct{})
	go func() {
		defer close(updateChan)
		defer close(processDone)

		batch := make([]*domain.GeoUpdateData, 0, p.config.BatchSize)
		batchCount := 0

		for data := range dataChan {
			batchCount++
			// Проверяем контекст
			select {
			case <-ctx.Done():
				p.logger.Info("context done, stopping processing",
					ports.Int("processed", batchCount))
				return
			default:
			}

			batch = append(batch, data)

			if len(batch) >= p.config.BatchSize {
				p.logger.Debug("processing batch",
					ports.Int("batch_size", len(batch)),
					ports.Int("total_processed", batchCount))
				p.processBatch(ctx, batch, updateChan)
				batch = make([]*domain.GeoUpdateData, 0, p.config.BatchSize)
			}
		}

		// Обрабатываем остаток
		if len(batch) > 0 {
			p.logger.Debug("processing final batch",
				ports.Int("batch_size", len(batch)),
				ports.Int("total_processed", batchCount))
			p.processBatch(ctx, batch, updateChan)
		}

		p.logger.Info("finished processing all data",
			ports.Int("total_records", batchCount))
	}()

	// Ждем завершения обработки
	<-processDone
	p.logger.Debug("processing done, waiting for workers")

	// Ждем завершения воркеров
	wgWorkers.Wait()
	p.logger.Debug("all workers finished")
	close(resultChan)

	// Ждем сбора результатов
	<-resultDone
	p.logger.Debug("all results collected")

	return overallResult, nil
}

// processBatch обрабатывает батч данных
func (p *UpdateProcessor) processBatch(ctx context.Context, batch []*domain.GeoUpdateData, updateChan chan<- *domain.Document) {
	// Собираем все ID для пакетного поиска
	ids := make([]uint64, len(batch))
	for i, data := range batch {
		ids[i] = data.DocID
	}

	p.logger.Debug("processing batch",
		ports.Int("batch_size", len(batch)),
		ports.Any("first_few_ids", ids[:min(5, len(ids))]))

	// Получаем существующие документы
	existingDocs, err := p.repo.GetDocumentsBatch(ctx, ids)
	if err != nil {
		p.logger.Error("failed to get documents batch",
			ports.Int("batch_size", len(batch)),
			ports.Error(err))

		// В случае ошибки пытаемся обработать по одному
		for _, data := range batch {
			p.processSingle(ctx, data, updateChan)
		}
		return
	}

	p.logger.Debug("found documents",
		ports.Int("requested", len(ids)),
		ports.Int("found", len(existingDocs)))

	// Обрабатываем каждый документ
	for _, data := range batch {
		if doc, ok := existingDocs[data.DocID]; ok {
			// Документ найден - обновляем
			if err := doc.Merge(data, p.config.UpdateMode); err != nil {
				p.logger.Error("failed to merge document",
					ports.Uint64("id", data.DocID),
					ports.Error(err))

				if p.config.SaveFailed {
					p.saveFailedRecord(ctx, data, err, "")
				}
				continue
			}

			p.logger.Debug("sending document to update channel",
				ports.Uint64("id", doc.ID))

			// Отправляем на обновление
			select {
			case <-ctx.Done():
				p.logger.Debug("context done while sending to update channel")
				return
			case updateChan <- doc:
				p.logger.Debug("document sent to update channel",
					ports.Uint64("id", doc.ID))
			}
		} else {
			// Документ не найден
			p.logger.Warn("document not found",
				ports.Uint64("id", data.DocID))

			if p.config.SaveFailed {
				p.saveFailedRecord(ctx, data, ports.ErrNotFound, "")
			}
		}
	}
}

// processSingle обрабатывает один документ
func (p *UpdateProcessor) processSingle(ctx context.Context, data *domain.GeoUpdateData, updateChan chan<- *domain.Document) {
	doc, err := p.repo.GetDocument(ctx, data.DocID)
	if err != nil {
		if err == ports.ErrNotFound {
			p.logger.Warn("document not found",
				ports.Uint64("id", data.DocID))
		} else {
			p.logger.Error("failed to get document",
				ports.Uint64("id", data.DocID),
				ports.Error(err))
		}

		if p.config.SaveFailed {
			p.saveFailedRecord(ctx, data, err, "")
		}
		return
	}

	if err := doc.Merge(data, p.config.UpdateMode); err != nil {
		p.logger.Error("failed to merge document",
			ports.Uint64("id", data.DocID),
			ports.Error(err))

		if p.config.SaveFailed {
			p.saveFailedRecord(ctx, data, err, "")
		}
		return
	}

	select {
	case <-ctx.Done():
		return
	case updateChan <- doc:
	}
}

// updateWorker воркер для обновления документов
func (p *UpdateProcessor) updateWorker(ctx context.Context, id int, updateChan <-chan *domain.Document,
	resultChan chan<- *domain.BatchResult, wg *sync.WaitGroup) {

	defer wg.Done()
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("worker panicked",
				ports.Int("worker_id", id),
				ports.Any("panic", r))
		}
	}()

	p.logger.Debug("update worker started", ports.Int("worker_id", id))

	batch := make([]*domain.Document, 0, p.config.BatchSize)
	batchResult := domain.NewBatchResult()

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}

		p.logger.Debug("worker flushing batch",
			ports.Int("worker_id", id),
			ports.Int("batch_size", len(batch)))

		// Проверяем контекст перед отправкой
		select {
		case <-ctx.Done():
			p.logger.Debug("context done, not sending batch")
			return
		default:
		}

		result, err := p.repo.BulkReplace(ctx, batch)
		if err != nil {
			p.logger.Error("bulk replace failed",
				ports.Int("worker_id", id),
				ports.Int("batch_size", len(batch)),
				ports.Error(err))

			for _, doc := range batch {
				batchResult.AddFailed(doc.ID, err, domain.ErrorTypeManticore, 1)
			}
		} else {
			p.logger.Debug("bulk replace successful",
				ports.Int("worker_id", id),
				ports.String("summary", result.Summary()))
			batchResult.Merge(result)
		}

		batch = make([]*domain.Document, 0, p.config.BatchSize)
	}

	for {
		select {
		case <-ctx.Done():
			p.logger.Debug("worker context done", ports.Int("worker_id", id))
			flushBatch()
			// Проверяем что канал результата не закрыт
			select {
			case resultChan <- batchResult:
				p.logger.Debug("worker sent final result", ports.Int("worker_id", id))
			default:
				p.logger.Debug("result channel full or closed", ports.Int("worker_id", id))
			}
			return

		case doc, ok := <-updateChan:
			if !ok {
				p.logger.Debug("update channel closed", ports.Int("worker_id", id))
				flushBatch()
				// Проверяем что канал результата не закрыт
				select {
				case resultChan <- batchResult:
					p.logger.Debug("worker sent final result", ports.Int("worker_id", id))
				default:
				}
				return
			}

			p.logger.Debug("worker received document",
				ports.Int("worker_id", id),
				ports.Uint64("id", doc.ID))

			batch = append(batch, doc)

			if len(batch) >= p.config.BatchSize {
				p.logger.Debug("batch full, flushing",
					ports.Int("worker_id", id),
					ports.Int("batch_size", len(batch)))
				flushBatch()
			}
		}
	}
}

// saveFailedRecord сохраняет неудачную запись
func (p *UpdateProcessor) saveFailedRecord(ctx context.Context, data *domain.GeoUpdateData, err error, filename string) {
	if p.failedRepo == nil {
		return
	}

	record := domain.NewFailedRecord(data, err, filename)

	if err := p.failedRepo.Save(ctx, record); err != nil {
		p.logger.Error("failed to save failed record",
			ports.Uint64("id", data.DocID),
			ports.Error(err))
	}
}

// ProcessFile обрабатывает один файл
func (p *UpdateProcessor) ProcessFile(ctx context.Context, filename string) (*domain.BatchResult, error) {
	p.logger.Info("processing file", ports.String("filename", filename))

	start := time.Now()
	defer func() {
		p.metrics.RecordFileProcessed(filename, 0, time.Since(start))
	}()

	dataChan, errChan := p.parser.ParseFile(ctx, filename)

	// Запускаем горутину для сбора ошибок парсинга
	go func() {
		for err := range errChan {
			p.logger.Error("parse error",
				ports.String("filename", filename),
				ports.Error(err))
		}
	}()

	result, err := p.ProcessDocuments(ctx, dataChan)
	if err != nil {
		return nil, fmt.Errorf("failed to process documents: %w", err)
	}

	// Сохраняем информацию о файле в результате
	result.Timestamp = time.Now()

	p.logger.Info("file processing completed",
		ports.String("filename", filename),
		ports.String("result", result.Summary()))

	p.mu.Lock()
	p.stats.TotalFiles++
	p.mu.Unlock()

	return result, nil
}

// ProcessFileWithReport обрабатывает один файл и возвращает детальный отчет
func (p *UpdateProcessor) ProcessFileWithReport(ctx context.Context, filename string) (*domain.FileReport, error) {
	p.logger.Info("processing file", ports.String("filename", filename))

	startTime := time.Now()

	// Получаем размер файла
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Создаем канал для данных и собираем ID в процессе обработки
	dataChan := make(chan *domain.GeoUpdateData, p.config.BatchSize)

	// Запускаем парсер в отдельной горутине
	parseDataChan, parseErrChan := p.parser.ParseFile(ctx, filename)

	// Счетчики для отчета
	var validCount, errorCount int
	var firstID, lastID uint64
	var allIDs []uint64

	// Горутина для сбора данных и статистики
	go func() {
		defer close(dataChan)

		for data := range parseDataChan {
			validCount++
			allIDs = append(allIDs, data.DocID)
			if firstID == 0 {
				firstID = data.DocID
			}
			lastID = data.DocID

			// Передаем данные дальше для обработки
			select {
			case <-ctx.Done():
				return
			case dataChan <- data:
			}
		}
		p.logger.Debug("collected IDs for report",
			ports.Int("count", len(allIDs)),
			ports.Uint64("first", firstID),
			ports.Uint64("last", lastID))
	}()

	// Горутина для сбора ошибок парсинга
	go func() {
		for err := range parseErrChan {
			errorCount++
			p.logger.Error("parse error",
				ports.String("filename", filename),
				ports.Error(err))
		}
	}()

	// Обрабатываем документы
	result, err := p.ProcessDocuments(ctx, dataChan)
	if err != nil {
		return nil, fmt.Errorf("failed to process documents: %w", err)
	}

	endTime := time.Now()

	// ВАЖНО: result.Success считает батчи, а нам нужно количество документов
	// Используем validCount как количество обработанных документов
	report := &domain.FileReport{
		Filename:  filename,
		Size:      fileInfo.Size(),
		Lines:     validCount + errorCount,
		Valid:     validCount,
		Errors:    errorCount,
		StartTime: startTime,
		EndTime:   endTime,
		Duration:  endTime.Sub(startTime).String(),
		Success:   validCount,    // Используем validCount вместо result.Success
		Failed:    result.Failed, // Количество документов, которые не удалось обновить
		Skipped:   result.Skipped,
		FirstID:   firstID,
		LastID:    lastID,
	}

	p.logger.Info("file processing completed",
		ports.String("filename", filename),
		ports.Int("documents", validCount),
		ports.Int("batches", result.Success), // Для информации
		ports.Any("report", report))

	return report, nil
}

// ProcessFiles обрабатывает несколько файлов
func (p *UpdateProcessor) ProcessFiles(ctx context.Context, filenames []string) (*domain.BatchResult, error) {
	p.logger.Info("processing multiple files", ports.Int("count", len(filenames)))

	overallResult := domain.NewBatchResult()

	for _, filename := range filenames {
		select {
		case <-ctx.Done():
			return overallResult, ctx.Err()
		default:
			result, err := p.ProcessFile(ctx, filename)
			if err != nil {
				p.logger.Error("failed to process file",
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

// ProcessFilesWithReport обрабатывает несколько файлов и возвращает общий отчет
func (p *UpdateProcessor) ProcessFilesWithReport(ctx context.Context, filenames []string) (*domain.ProcessingReport, error) {
	p.logger.Info("processing multiple files", ports.Int("count", len(filenames)))

	report := domain.NewProcessingReport(
		version.Short(), // Передаём версию
		p.config.UpdateMode,
		p.config.Workers,
		p.config.BatchSize,
	)

	for _, filename := range filenames {
		select {
		case <-ctx.Done():
			report.Complete()
			return report, ctx.Err()
		default:
			fileReport, err := p.ProcessFileWithReport(ctx, filename)
			if err != nil {
				p.logger.Error("failed to process file",
					ports.String("filename", filename),
					ports.Error(err))
				// Добавляем минимальный отчет с ошибкой
				fileReport = &domain.FileReport{
					Filename: filename,
					Errors:   1,
				}
			}
			if fileReport != nil {
				report.AddFile(*fileReport)
			}
		}
	}

	report.Complete()
	return report, nil
}

// ReprocessFailed повторно обрабатывает неудачные записи
func (p *UpdateProcessor) ReprocessFailed(ctx context.Context) (*domain.BatchResult, error) {
	if !p.config.EnableReprocess || p.failedRepo == nil {
		p.logger.Info("reprocessing disabled or no failed repo")
		return domain.NewBatchResult(), nil
	}

	p.logger.Info("starting reprocessing of failed records")

	// Загружаем failed записи
	records, err := p.failedRepo.LoadAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load failed records: %w", err)
	}

	if len(records) == 0 {
		p.logger.Info("no failed records to reprocess")
		return domain.NewBatchResult(), nil
	}

	p.logger.Info("loaded failed records for reprocessing",
		ports.Int("count", len(records)))

	// Конвертируем в GeoUpdateData
	dataChan := make(chan *domain.GeoUpdateData, len(records))
	for _, record := range records {
		// Увеличиваем счетчик попыток
		record.IncrementAttempt()

		// Проверяем не превышен ли лимит попыток
		if !record.CanRetry(p.config.MaxRetries) {
			p.logger.Warn("record exceeded max retries",
				ports.Uint64("id", record.Data.DocID),
				ports.Int("attempts", record.Attempts))
			continue
		}

		dataChan <- record.Data
	}
	close(dataChan)

	// Обрабатываем
	result, err := p.ProcessDocuments(ctx, dataChan)
	if err != nil {
		return nil, err
	}

	// Очищаем успешно обработанные записи
	if result.Success > 0 {
		p.logger.Info("records successfully reprocessed",
			ports.Int("success", result.Success))
	}

	return result, nil
}

// GetStats возвращает статистику обработки
func (p *UpdateProcessor) GetStats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	now := time.Now()

	return map[string]interface{}{
		"total_processed":  p.stats.TotalProcessed,
		"total_success":    p.stats.TotalSuccess,
		"total_failed":     p.stats.TotalFailed,
		"total_skipped":    p.stats.TotalSkipped,
		"total_files":      p.stats.TotalFiles,
		"uptime_seconds":   now.Sub(p.stats.StartTime).Seconds(),
		"last_processed":   p.stats.LastProcessed,
		"processing_rate":  p.calculateRate(),
		"update_mode":      p.config.UpdateMode,
		"batch_size":       p.config.BatchSize,
		"workers":          p.config.Workers,
		"max_retries":      p.config.MaxRetries,
		"save_failed":      p.config.SaveFailed,
		"enable_reprocess": p.config.EnableReprocess,
	}
}

// calculateRate вычисляет скорость обработки
func (p *UpdateProcessor) calculateRate() float64 {
	if p.stats.TotalProcessed == 0 {
		return 0
	}
	elapsed := time.Since(p.stats.StartTime).Seconds()
	return float64(p.stats.TotalProcessed) / elapsed
}

// ResetStats сбрасывает статистику
func (p *UpdateProcessor) ResetStats() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stats = &ProcessorStats{
		StartTime: time.Now(),
	}
}

// min вспомогательная функция
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
