// internal/core/ports/file_watcher.go
package ports

import (
	"context"
)

// FileWatcher определяет интерфейс для отслеживания новых файлов
type FileWatcher interface {
	// Watch начинает отслеживание директории на предмет новых файлов
	Watch(ctx context.Context) (<-chan string, <-chan error)

	// WatchPattern отслеживает файлы по паттерну
	WatchPattern(ctx context.Context, pattern string) (<-chan string, <-chan error)

	// GetProcessedFiles возвращает список уже обработанных файлов
	GetProcessedFiles(ctx context.Context) ([]string, error)

	// MarkProcessed отмечает файл как обработанный
	MarkProcessed(ctx context.Context, filename string) error
}
