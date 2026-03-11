// internal/app/service/update_processor.go
package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/terratensor/geoupdater/internal/core/domain"
	"github.com/terratensor/geoupdater/internal/core/ports"
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
	overallResult := domain.NewBatchResult()

	// Канал для передачи документов на обновление
	updateChan := make(chan *domain.Document, p.config.BatchSize)
	resultChan := make(chan *domain.BatchResult, p.config.Workers)
	errorChan := make(chan error, p.config.Workers)

	// Запускаем воркеров для обновления
	var wg sync.WaitGroup
	for i := 0; i < p.config.Workers; i++ {
		wg.Add(1)
		go p.updateWorker(ctx, i, updateChan, resultChan, errorChan, &wg)
	}

	// Горутина для сбора результатов
	done := make(chan bool)
	go func() {
		for result := range resultChan {
			overallResult.Merge(result)
		}
		done <- true
	}()

	// Основной цикл обработки данных
	var wgCollector sync.WaitGroup
	wgCollector.Add(1)
	go func() {
		defer wgCollector.Done()
		defer close(updateChan)

		batch := make([]*domain.GeoUpdateData, 0, p.config.BatchSize)

		for data := range dataChan {
			batch = append(batch, data)

			if len(batch) >= p.config.BatchSize {
				p.processBatch(ctx, batch, updateChan)
				batch = make([]*domain.GeoUpdateData, 0, p.config.BatchSize)
			}
		}

		// Обрабатываем остаток
		if len(batch) > 0 {
			p.processBatch(ctx, batch, updateChan)
		}
	}()

	// Ждем завершения обработки
	wgCollector.Wait()
	close(resultChan)
	<-done

	// Проверяем ошибки
	select {
	case err := <-errorChan:
		if err != nil {
			p.logger.Error("worker error", ports.Error(err))
		}
	default:
	}

	// Обновляем статистику
	p.mu.Lock()
	p.stats.TotalProcessed += int64(overallResult.Total)
	p.stats.TotalSuccess += int64(overallResult.Success)
	p.stats.TotalFailed += int64(overallResult.Failed)
	p.stats.TotalSkipped += int64(overallResult.Skipped)
	p.stats.LastProcessed = time.Now()
	p.mu.Unlock()

	return overallResult, nil
}

// processBatch обрабатывает батч данных
func (p *UpdateProcessor) processBatch(ctx context.Context, batch []*domain.GeoUpdateData, updateChan chan<- *domain.Document) {
	// Собираем все ID для пакетного поиска
	ids := make([]uint64, len(batch))
	for i, data := range batch {
		ids[i] = data.DocID
	}

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

			// Отправляем на обновление
			select {
			case <-ctx.Done():
				return
			case updateChan <- doc:
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
	resultChan chan<- *domain.BatchResult, errorChan chan<- error, wg *sync.WaitGroup) {

	defer wg.Done()

	p.logger.Debug("update worker started", ports.Int("worker_id", id))

	batch := make([]*domain.Document, 0, p.config.BatchSize)
	batchResult := domain.NewBatchResult()

	flushBatch := func() {
		if len(batch) == 0 {
			return
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
			batchResult.Merge(result)
		}

		batch = make([]*domain.Document, 0, p.config.BatchSize)
	}

	for {
		select {
		case <-ctx.Done():
			flushBatch()
			resultChan <- batchResult
			return

		case doc, ok := <-updateChan:
			if !ok {
				flushBatch()
				resultChan <- batchResult
				return
			}

			batch = append(batch, doc)

			if len(batch) >= p.config.BatchSize {
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
		// Собираем ID успешно обработанных
		// В реальности нужно более точно отслеживать какие записи успешно обработаны
		// Для простоты пока пропускаем
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
