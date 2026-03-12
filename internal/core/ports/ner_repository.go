// internal/core/ports/ner_repository.go
package ports

import (
	"context"

	"github.com/terratensor/geoupdater/internal/core/domain"
)

// NERRepository определяет интерфейс для работы с NER таблицей
type NERRepository interface {
	// EnsureTable проверяет/создает таблицу
	EnsureTable(ctx context.Context) error

	// GetDocument получает NER документ по doc_id
	GetDocument(ctx context.Context, docID uint64) (*domain.NERDocument, error)

	// UpdateDocument обновляет или вставляет NER документ
	UpdateDocument(ctx context.Context, doc *domain.NERDocument) error

	// BulkUpdate массовое обновление NER документов
	BulkUpdate(ctx context.Context, docs []*domain.NERDocument) (*domain.BatchResult, error)
}
