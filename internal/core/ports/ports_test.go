// internal/core/ports/ports_test.go
package ports

import (
	"context"
	"testing"

	"github.com/terratensor/geoupdater/internal/core/domain"
)

// Проверяем, что все интерфейсы можно реализовать
func TestInterfacesAreSatisfiable(t *testing.T) {
	ctx := context.Background()

	// Проверяем Repository - обновляем сигнатуры методов
	var _ Repository = (*MockRepository)(nil)

	// Обновляем мок-структуру для работы с uint64
	repo := &MockRepository{
		GetDocumentFunc: func(ctx context.Context, id uint64) (*domain.Document, error) {
			return &domain.Document{ID: id}, nil
		},
		// Можно добавить моки для других методов, если они используются в тесте
		GetDocumentsBatchFunc: func(ctx context.Context, ids []uint64) (map[uint64]*domain.Document, error) {
			return nil, nil
		},
		ReplaceDocumentFunc: func(ctx context.Context, doc *domain.Document) error {
			return nil
		},
		BulkReplaceFunc: func(ctx context.Context, docs []*domain.Document) (*domain.BatchResult, error) {
			return &domain.BatchResult{}, nil
		},
		PingFunc: func(ctx context.Context) error {
			return nil
		},
		CloseFunc: func() error {
			return nil
		},
	}

	// Теперь передаём uint64
	doc, err := repo.GetDocument(ctx, 123)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if doc.ID != 123 {
		t.Errorf("expected ID 123, got %d", doc.ID)
	}
}

// Заглушка для тестирования логгера (без изменений)
type mockLogger struct{}

func (m *mockLogger) Debug(msg string, fields ...Field)      {}
func (m *mockLogger) Info(msg string, fields ...Field)       {}
func (m *mockLogger) Warn(msg string, fields ...Field)       {}
func (m *mockLogger) Error(msg string, fields ...Field)      {}
func (m *mockLogger) Fatal(msg string, fields ...Field)      {}
func (m *mockLogger) With(fields ...Field) Logger            { return m }
func (m *mockLogger) WithContext(ctx context.Context) Logger { return m }
func (m *mockLogger) Sync() error                            { return nil }

func TestLoggerInterface(t *testing.T) {
	var _ Logger = (*mockLogger)(nil)
}
