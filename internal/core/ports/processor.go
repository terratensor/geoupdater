// internal/core/ports/processor.go
package ports

import (
	"context"

	"github.com/terratensor/geoupdater/internal/core/domain"
)

// UpdateProcessor определяет интерфейс для обработчика обновлений
type UpdateProcessor interface {
	// ProcessDocuments обрабатывает документы с данными для обновления
	ProcessDocuments(ctx context.Context, dataChan <-chan *domain.GeoUpdateData) (*domain.BatchResult, error)

	// ProcessFile обрабатывает один файл
	ProcessFile(ctx context.Context, filename string) (*domain.BatchResult, error)

	// ProcessFiles обрабатывает несколько файлов
	ProcessFiles(ctx context.Context, filenames []string) (*domain.BatchResult, error)

	// ReprocessFailed повторно обрабатывает неудачные записи
	ReprocessFailed(ctx context.Context) (*domain.BatchResult, error)

	// GetStats возвращает статистику обработки
	GetStats() map[string]interface{}
}

// ProcessorConfig конфигурация процессора
type ProcessorConfig interface {
	// SetMode устанавливает режим обновления (replace/merge)
	SetMode(mode domain.UpdateMode)

	// SetBatchSize устанавливает размер батча
	SetBatchSize(size int)

	// SetWorkers устанавливает количество воркеров
	SetWorkers(workers int)

	// SetMaxRetries устанавливает максимальное количество попыток
	SetMaxRetries(retries int)
}
