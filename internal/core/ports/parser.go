// internal/core/ports/parser.go
package ports

import (
	"context"
	"io"

	"github.com/terratensor/geoupdater/internal/core/domain"
)

// Parser определяет интерфейс для парсинга NDJSON файлов
type Parser interface {
	// ParseFile читает файл и возвращает канал с данными
	// Канал ошибок возвращает ошибки парсинга
	ParseFile(ctx context.Context, filename string) (<-chan *domain.GeoUpdateData, <-chan error)

	// ParseFiles параллельно читает несколько файлов
	// Возвращает объединенный канал данных из всех файлов
	ParseFiles(ctx context.Context, filenames []string) (<-chan *domain.GeoUpdateData, <-chan error)

	// ParseReader читает данные из io.Reader (для тестирования)
	ParseReader(ctx context.Context, reader io.Reader) (<-chan *domain.GeoUpdateData, <-chan error)
}

// ParserConfig конфигурация парсера
type ParserConfig interface {
	// SetBatchSize устанавливает размер буфера для канала
	SetBatchSize(size int)

	// SetWorkers устанавливает количество воркеров для параллельного парсинга
	SetWorkers(workers int)

	// SetValidate устанавливает флаг валидации данных
	SetValidate(validate bool)
}
