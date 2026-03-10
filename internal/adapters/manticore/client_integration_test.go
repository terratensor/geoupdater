// internal/adapters/manticore/client_integration_test.go
package manticore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/terratensor/geoupdater/internal/adapters/logger"
	"github.com/terratensor/geoupdater/internal/core/domain"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

// TestClientIntegration реальные тесты с Manticore
func TestClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Создаем логгер
	logCfg := logger.DefaultConfig()
	logCfg.Console = true
	logCfg.Level = "debug"
	log, err := logger.NewZapLogger(logCfg)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// Создаем клиент
	cfg := DefaultConfig()
	cfg.Host = "localhost"
	cfg.Port = 9308
	cfg.TableName = "library2026" // Используем реальную таблицу

	metrics := &mockMetrics{}

	client, err := NewClient(cfg, log, metrics)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	t.Run("Ping", func(t *testing.T) {
		err := client.Ping(context.Background())
		if err != nil {
			t.Errorf("Ping failed: %v", err)
		}
	})

	t.Run("GetDocument", func(t *testing.T) {
		testID := uint64(6056452479959171091) // теперь uint64

		doc, err := client.GetDocument(context.Background(), testID)
		if err != nil {
			if err == ports.ErrNotFound {
				t.Logf("Document %d not found", testID)
				return
			}
			t.Fatalf("GetDocument failed: %v", err)
		}

		t.Logf("Retrieved document: ID=%d, Title=%s", doc.ID, doc.Title)

		if doc.ID != testID {
			t.Errorf("Expected ID %d, got %d", testID, doc.ID)
		}
	})

	t.Run("GetDocumentsBatch", func(t *testing.T) {
		ids := []uint64{
			6056452479959171091,
			6056452479959171088,
			6056452479959171104,
		}

		docs, err := client.GetDocumentsBatch(context.Background(), ids)
		if err != nil {
			t.Fatalf("GetDocumentsBatch failed: %v", err)
		}

		t.Logf("Retrieved %d documents", len(docs))
		for id, doc := range docs {
			t.Logf("  - %d: %s", id, doc.Title)
		}
	})

	t.Run("ReplaceDocument", func(t *testing.T) {
		// Сначала получаем существующий документ
		testID := uint64(6056452479959171091)

		existing, err := client.GetDocument(context.Background(), testID)
		if err != nil {
			if err == ports.ErrNotFound {
				t.Skipf("Document %d not found, skipping replace test", testID)
			}
			t.Fatalf("Failed to get document: %v", err)
		}

		// Создаем тестовые данные для обновления
		updateData := &domain.GeoUpdateData{
			DocID:           testID,
			GeohashesString: []string{"test1", "test2", "test3"},
			GeohashesUint64: []int64{111111, 222222, 333333},
		}

		// Сохраняем оригинальные значения для восстановления
		originalStrings := existing.GeohashesString
		originalUint64 := existing.GeohashesUint64

		// Обновляем документ
		err = existing.Merge(updateData, domain.ModeMerge)
		if err != nil {
			t.Fatalf("Merge failed: %v", err)
		}

		// Сохраняем в Manticore
		err = client.ReplaceDocument(context.Background(), existing)
		if err != nil {
			t.Fatalf("ReplaceDocument failed: %v", err)
		}

		// Проверяем что сохранилось
		updated, err := client.GetDocument(context.Background(), testID)
		if err != nil {
			t.Fatalf("Failed to get updated document: %v", err)
		}

		t.Logf("Updated document: %s", updated.GeohashesString)

		// Восстанавливаем оригинальные значения
		existing.GeohashesString = originalStrings
		existing.GeohashesUint64 = originalUint64
		err = client.ReplaceDocument(context.Background(), existing)
		if err != nil {
			t.Errorf("Failed to restore original document: %v", err)
		}
	})

	t.Run("BulkReplace", func(t *testing.T) {
		// Получаем несколько документов для теста
		ids := []uint64{
			uint64(6056452479959171091),
			uint64(6056452479959171088),
		}

		existingDocs, err := client.GetDocumentsBatch(context.Background(), ids)
		if err != nil {
			t.Fatalf("Failed to get documents: %v", err)
		}

		if len(existingDocs) == 0 {
			t.Skip("No documents found for bulk test")
		}

		// Подготавливаем документы для обновления
		var docsToUpdate []*domain.Document
		originalValues := make(map[uint64]string) // для восстановления

		for id, doc := range existingDocs {
			// Сохраняем оригинал
			originalValues[id] = doc.GeohashesString

			// Создаем тестовые данные
			updateData := &domain.GeoUpdateData{
				DocID:           id,
				GeohashesString: []string{fmt.Sprintf("bulk_test_%d", id)},
				GeohashesUint64: []int64{int64(time.Now().UnixNano())},
			}

			doc.Merge(updateData, domain.ModeMerge)
			docsToUpdate = append(docsToUpdate, doc)
		}

		// Выполняем bulk replace
		result, err := client.BulkReplace(context.Background(), docsToUpdate)
		if err != nil {
			t.Fatalf("BulkReplace failed: %v", err)
		}

		t.Logf("Bulk result: %s", result.Summary())

		// Проверяем что обновилось
		for id := range existingDocs {
			updated, err := client.GetDocument(context.Background(), id)
			if err != nil {
				t.Errorf("Failed to get document %d after bulk: %v", id, err)
				continue
			}
			t.Logf("Document %d after bulk: %s", id, updated.GeohashesString)
		}

		// Восстанавливаем оригинальные значения
		var restoreDocs []*domain.Document
		for id, doc := range existingDocs {
			doc.GeohashesString = originalValues[id]
			restoreDocs = append(restoreDocs, doc)
		}

		_, err = client.BulkReplace(context.Background(), restoreDocs)
		if err != nil {
			t.Errorf("Failed to restore documents: %v", err)
		}
	})
}

// BenchmarkGetDocument бенчмарк для получения документа
func BenchmarkGetDocument(b *testing.B) {
	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	cfg := DefaultConfig()
	cfg.Host = "localhost"
	cfg.Port = 9308

	metrics := &mockMetrics{}

	client, err := NewClient(cfg, log, metrics)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	testID := uint64(6056452479959171091)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.GetDocument(ctx, testID)
		if err != nil && err != ports.ErrNotFound {
			b.Fatalf("GetDocument failed: %v", err)
		}
	}
}

// BenchmarkBulkReplace бенчмарк для bulk операций
func BenchmarkBulkReplace(b *testing.B) {
	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	cfg := DefaultConfig()
	cfg.Host = "localhost"
	cfg.Port = 9308

	metrics := &mockMetrics{}

	client, err := NewClient(cfg, log, metrics)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Подготавливаем тестовые документы
	docs := make([]*domain.Document, 100)
	for i := 0; i < 100; i++ {
		docs[i] = &domain.Document{
			ID:              uint64(i + 1000000), // uint64, не строка
			Source:          "benchmark",
			GeohashesString: "test1, test2, test3",
			GeohashesUint64: []int64{1, 2, 3},
		}
	}

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := client.BulkReplace(ctx, docs)
		if err != nil {
			b.Fatalf("BulkReplace failed: %v", err)
		}
	}
}
