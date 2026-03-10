// internal/core/ports/repository.go
package ports

import (
	"context"

	"github.com/terratensor/geoupdater/internal/core/domain"
)

// Repository определяет интерфейс для работы с Manticore Search
type Repository interface {
	// GetDocument получает документ по ID
	// Возвращает domain.ErrNotFound если документ не существует
	GetDocument(ctx context.Context, id string) (*domain.Document, error)

	// GetDocumentsBatch получает пачку документов по списку ID
	// Возвращает map[id]*domain.Document для найденных документов
	GetDocumentsBatch(ctx context.Context, ids []string) (map[string]*domain.Document, error)

	// ReplaceDocument заменяет документ полностью
	ReplaceDocument(ctx context.Context, doc *domain.Document) error

	// BulkReplace выполняет массовую замену документов
	// Возвращает результаты операции и возможные ошибки
	BulkReplace(ctx context.Context, docs []*domain.Document) (*domain.BatchResult, error)

	// Ping проверяет соединение с Manticore
	Ping(ctx context.Context) error

	// Close закрывает соединение с Manticore
	Close() error
}

// RepositoryFactory создает репозитории с разными настройками
type RepositoryFactory interface {
	// Create создает новый экземпляр репозитория
	Create(ctx context.Context) (Repository, error)

	// CreateWithBatchSize создает репозиторий с определенным размером батча
	CreateWithBatchSize(ctx context.Context, batchSize int) (Repository, error)
}
