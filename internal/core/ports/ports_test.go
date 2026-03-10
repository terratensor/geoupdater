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

	// Проверяем Repository
	var _ Repository = (*MockRepository)(nil)

	// Проверяем Logger
	var _ Logger = (*mockLogger)(nil)

	// Проверяем работу с дженериками
	repo := &MockRepository{
		GetDocumentFunc: func(ctx context.Context, id string) (*domain.Document, error) {
			return &domain.Document{ID: id}, nil
		},
	}

	doc, err := repo.GetDocument(ctx, "123")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if doc.ID != "123" {
		t.Errorf("expected ID 123, got %s", doc.ID)
	}
}

// Заглушка для тестирования логгера
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
