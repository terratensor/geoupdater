// internal/app/service/updater.go
package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/terratensor/geoupdater/internal/core/domain"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

// UpdateService реализует бизнес-логику обновления
type UpdateService struct {
	repo       ports.Repository
	parser     ports.Parser
	failedRepo ports.FailedRecordsRepository
	logger     ports.Logger
	metrics    ports.MetricsCollector
	config     *Config
}

type Config struct {
	Mode       domain.UpdateMode
	BatchSize  int
	Workers    int
	MaxRetries int
}

// NewUpdateService создаёт новый сервис
func NewUpdateService(
	repo ports.Repository,
	parser ports.Parser,
	failedRepo ports.FailedRecordsRepository,
	logger ports.Logger,
	metrics ports.MetricsCollector,
	config *Config,
) *UpdateService {
	return &UpdateService{
		repo:       repo,
		parser:     parser,
		failedRepo: failedRepo,
		logger:     logger,
		metrics:    metrics,
		config:     config,
	}
}

// ProcessFile обрабатывает один файл
func (s *UpdateService) ProcessFile(ctx context.Context, filename string) (*domain.BatchResult, error) {
	s.logger.Info("processing file", ports.String("filename", filename))

	// Парсим файл
	dataChan, errChan := s.parser.ParseFile(ctx, filename)

	// Собираем ID для пакетного поиска
	var allIDs []uint64
	var allData []*domain.GeoUpdateData

	for data := range dataChan {
		allIDs = append(allIDs, data.DocID)
		allData = append(allData, data)
	}

	// Проверяем ошибки парсинга
	select {
	case err := <-errChan:
		if err != nil {
			return nil, fmt.Errorf("parse error: %w", err)
		}
	default:
	}

	// Получаем существующие документы пачками
	existingDocs, err := s.repo.GetDocumentsBatch(ctx, allIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to get documents: %w", err)
	}

	// Подготавливаем документы для обновления
	var docsToUpdate []*domain.Document
	var failed []*domain.FailedRecord

	for _, data := range allData {
		doc, exists := existingDocs[data.DocID]
		if !exists {
			// Документ не найден - пропускаем и логируем
			s.logger.Warn("document not found",
				ports.Uint64("id", data.DocID),
				ports.String("filename", filename))
			failed = append(failed, domain.NewFailedRecord(data,
				fmt.Errorf("document not found"), filename))
			continue
		}

		// Объединяем данные
		if err := doc.Merge(data, s.config.Mode); err != nil {
			s.logger.Error("merge failed",
				ports.Uint64("id", data.DocID),
				ports.Error(err))
			failed = append(failed, domain.NewFailedRecord(data, err, filename))
			continue
		}

		docsToUpdate = append(docsToUpdate, doc)
	}

	// Сохраняем неудачные записи
	if len(failed) > 0 {
		if err := s.failedRepo.SaveBatch(ctx, failed); err != nil {
			s.logger.Error("failed to save failed records", ports.Error(err))
		}
	}

	// Выполняем bulk replace
	if len(docsToUpdate) > 0 {
		result, err := s.repo.BulkReplace(ctx, docsToUpdate)
		if err != nil {
			return nil, fmt.Errorf("bulk replace failed: %w", err)
		}
		return result, nil
	}

	return domain.NewBatchResult(), nil
}

// ProcessFiles обрабатывает несколько файлов параллельно
func (s *UpdateService) ProcessFiles(ctx context.Context, filenames []string) (*domain.BatchResult, error) {
	overall := domain.NewBatchResult()

	var wg sync.WaitGroup
	results := make(chan *domain.BatchResult, len(filenames))
	errors := make(chan error, len(filenames))

	// Ограничиваем количество параллельных задач
	semaphore := make(chan struct{}, s.config.Workers)

	for _, filename := range filenames {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			result, err := s.ProcessFile(ctx, f)
			if err != nil {
				errors <- fmt.Errorf("file %s: %w", f, err)
				return
			}
			results <- result
		}(filename)
	}

	// Ждём завершения
	go func() {
		wg.Wait()
		close(results)
		close(errors)
	}()

	// Собираем результаты
	for result := range results {
		overall.Merge(result)
	}

	// Проверяем ошибки
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return overall, fmt.Errorf("errors occurred: %v", errs)
	}

	return overall, nil
}
