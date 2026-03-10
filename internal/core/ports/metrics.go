// internal/core/ports/metrics.go
package ports

import (
	"context"
	"time"
)

// MetricsCollector определяет интерфейс для сбора метрик
type MetricsCollector interface {
	// RecordDocumentProcessed записывает метрику обработки документа
	RecordDocumentProcessed(duration time.Duration, success bool)

	// RecordBatchProcessed записывает метрику обработки батча
	RecordBatchProcessed(size int, duration time.Duration, errors int)

	// RecordFileProcessed записывает метрику обработки файла
	RecordFileProcessed(filename string, size int, duration time.Duration)

	// RecordManticoreOperation записывает метрику операции с Manticore
	RecordManticoreOperation(operation string, duration time.Duration, err error)

	// GetStats возвращает текущую статистику
	GetStats(ctx context.Context) (map[string]interface{}, error)
}
