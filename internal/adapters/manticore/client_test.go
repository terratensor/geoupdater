// internal/adapters/manticore/client_test.go
package manticore

import (
	"context"
	"testing"
	"time"

	"github.com/terratensor/geoupdater/internal/adapters/logger"
	"github.com/terratensor/geoupdater/internal/core/domain"
)

func TestNewClient(t *testing.T) {
	// Пропускаем тест если нет реальной Manticore
	t.Skip("Skipping test that requires real Manticore instance")

	log, _ := logger.NewZapLogger(logger.DefaultConfig())
	metrics := &mockMetrics{}

	client, err := NewClient(DefaultConfig(), log, metrics)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	if err := client.Ping(context.Background()); err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

// mockMetrics для тестов
type mockMetrics struct{}

func (m *mockMetrics) RecordDocumentProcessed(duration time.Duration, success bool)                 {}
func (m *mockMetrics) RecordBatchProcessed(size int, duration time.Duration, errors int)            {}
func (m *mockMetrics) RecordFileProcessed(filename string, size int, duration time.Duration)        {}
func (m *mockMetrics) RecordManticoreOperation(operation string, duration time.Duration, err error) {}
func (m *mockMetrics) GetStats(ctx context.Context) (map[string]interface{}, error)                 { return nil, nil }

// internal/adapters/manticore/client_test.go - исправляем TestSplitIntoBatches
func TestSplitIntoBatches(t *testing.T) {
	client := &Client{}

	docs := make([]*domain.Document, 2500)
	for i := 0; i < 2500; i++ {
		docs[i] = &domain.Document{ID: uint64(i)}
	}

	batches := client.splitIntoBatches(docs, 1000)

	if len(batches) != 3 {
		t.Errorf("Expected 3 batches, got %d", len(batches))
	}

	if len(batches[0]) != 1000 {
		t.Errorf("First batch size expected 1000, got %d", len(batches[0]))
	}

	if len(batches[2]) != 500 {
		t.Errorf("Last batch size expected 500, got %d", len(batches[2]))
	}
}
