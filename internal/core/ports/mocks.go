// internal/core/ports/mocks.go
package ports

import (
	"context"

	"github.com/terratensor/geoupdater/internal/core/domain"
)

// Для удобства тестирования создадим интерфейсы-заглушки

// MockRepository заглушка для тестов - ИСПРАВЛЕННАЯ ВЕРСИЯ
type MockRepository struct {
	GetDocumentFunc       func(ctx context.Context, id uint64) (*domain.Document, error)               // было string
	GetDocumentsBatchFunc func(ctx context.Context, ids []uint64) (map[uint64]*domain.Document, error) // было string
	ReplaceDocumentFunc   func(ctx context.Context, doc *domain.Document) error
	BulkReplaceFunc       func(ctx context.Context, docs []*domain.Document) (*domain.BatchResult, error)
	PingFunc              func(ctx context.Context) error
	CloseFunc             func() error
}

// Реализация методов с правильными типами
func (m *MockRepository) GetDocument(ctx context.Context, id uint64) (*domain.Document, error) {
	if m.GetDocumentFunc != nil {
		return m.GetDocumentFunc(ctx, id)
	}
	return nil, nil
}

func (m *MockRepository) GetDocumentsBatch(ctx context.Context, ids []uint64) (map[uint64]*domain.Document, error) {
	if m.GetDocumentsBatchFunc != nil {
		return m.GetDocumentsBatchFunc(ctx, ids)
	}
	return nil, nil
}

func (m *MockRepository) ReplaceDocument(ctx context.Context, doc *domain.Document) error {
	if m.ReplaceDocumentFunc != nil {
		return m.ReplaceDocumentFunc(ctx, doc)
	}
	return nil
}

func (m *MockRepository) BulkReplace(ctx context.Context, docs []*domain.Document) (*domain.BatchResult, error) {
	if m.BulkReplaceFunc != nil {
		return m.BulkReplaceFunc(ctx, docs)
	}
	return &domain.BatchResult{}, nil
}

func (m *MockRepository) Ping(ctx context.Context) error {
	if m.PingFunc != nil {
		return m.PingFunc(ctx)
	}
	return nil
}

func (m *MockRepository) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}
