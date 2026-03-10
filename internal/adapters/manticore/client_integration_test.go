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
	cfg.TableName = "library2026"
	cfg.Timeout = 30 * time.Second
	cfg.BatchSize = 1000

	metrics := &mockMetrics{}

	client, err := NewClient(cfg, log, metrics)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Тест 1: Проверка соединения
	t.Run("Ping", func(t *testing.T) {
		err := client.Ping(ctx)
		if err != nil {
			t.Errorf("Ping failed: %v", err)
		}
	})

	// Тест 2: Поиск одного документа
	t.Run("GetDocument", func(t *testing.T) {
		testID := uint64(6056452479959171091)

		doc, err := client.GetDocument(ctx, testID)
		if err != nil {
			if err == ports.ErrNotFound {
				t.Logf("Document %d not found, skipping test", testID)
				return
			}
			t.Fatalf("GetDocument failed: %v", err)
		}

		t.Logf("Retrieved document: ID=%d, Title=%s", doc.ID, doc.Title)

		if doc.ID != testID {
			t.Errorf("ID mismatch: got %d, want %d", doc.ID, testID)
		}
	})

	// Тест 3: Пакетный поиск документов
	t.Run("GetDocumentsBatch", func(t *testing.T) {
		ids := []uint64{
			6056452479959171091,
			6056452479959171088,
			6056452479959171104,
		}

		docs, err := client.GetDocumentsBatch(ctx, ids)
		if err != nil {
			t.Fatalf("GetDocumentsBatch failed: %v", err)
		}

		t.Logf("Retrieved %d documents", len(docs))
		for id, doc := range docs {
			t.Logf("  - %d: %s", id, doc.Title)
		}

		// Проверяем что все ID найдены
		for _, id := range ids {
			if _, ok := docs[id]; !ok {
				t.Errorf("Document %d not found", id)
			}
		}
	})

	// Тест 4: Пакетный поиск с большим количеством ID
	t.Run("GetDocumentsBatch Large", func(t *testing.T) {
		// Создаем 2500 ID для теста (3 батча по 1000)
		ids := make([]uint64, 2500)
		for i := 0; i < 2500; i++ {
			ids[i] = uint64(6056452479959171000 + uint64(i))
		}

		start := time.Now()
		docs, err := client.GetDocumentsBatch(ctx, ids)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("GetDocumentsBatch large failed: %v", err)
		}

		t.Logf("Retrieved %d documents from %d requested in %v",
			len(docs), len(ids), duration)
		t.Logf("Average per document: %v", duration/time.Duration(len(ids)))
	})

	// Тест 5: Замена документа
	t.Run("ReplaceDocument", func(t *testing.T) {
		// Сначала получаем существующий документ
		testID := uint64(6056452479959171091)

		existing, err := client.GetDocument(ctx, testID)
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
			GeohashesUint64: []uint64{111111, 222222, 333333},
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
		err = client.ReplaceDocument(ctx, existing)
		if err != nil {
			t.Fatalf("ReplaceDocument failed: %v", err)
		}

		// Проверяем что сохранилось
		updated, err := client.GetDocument(ctx, testID)
		if err != nil {
			t.Fatalf("Failed to get updated document: %v", err)
		}

		t.Logf("Updated document geohashes: %s", updated.GeohashesString)

		// Восстанавливаем оригинальные значения
		existing.GeohashesString = originalStrings
		existing.GeohashesUint64 = originalUint64
		err = client.ReplaceDocument(ctx, existing)
		if err != nil {
			t.Errorf("Failed to restore original document: %v", err)
		}
	})

	// Тест 6: Массовая замена документов
	t.Run("BulkReplace", func(t *testing.T) {
		// Получаем несколько документов для теста
		ids := []uint64{
			6056452479959171091,
			6056452479959171088,
		}

		existingDocs, err := client.GetDocumentsBatch(ctx, ids)
		if err != nil {
			t.Fatalf("Failed to get documents: %v", err)
		}

		if len(existingDocs) == 0 {
			t.Skip("No documents found for bulk test")
		}

		// Подготавливаем документы для обновления
		var docsToUpdate []*domain.Document
		originalValues := make(map[uint64]string)

		for id, doc := range existingDocs {
			originalValues[id] = doc.GeohashesString

			updateData := &domain.GeoUpdateData{
				DocID:           id,
				GeohashesString: []string{fmt.Sprintf("bulk_test_%d", id)},
				GeohashesUint64: []uint64{uint64(time.Now().UnixNano())},
			}

			doc.Merge(updateData, domain.ModeMerge)
			docsToUpdate = append(docsToUpdate, doc)
		}

		// Выполняем bulk replace
		result, err := client.BulkReplace(ctx, docsToUpdate)
		if err != nil {
			t.Fatalf("BulkReplace failed: %v", err)
		}

		t.Logf("Bulk result: %s", result.Summary())

		// Проверяем что обновилось
		for id := range existingDocs {
			updated, err := client.GetDocument(ctx, id)
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

		_, err = client.BulkReplace(ctx, restoreDocs)
		if err != nil {
			t.Errorf("Failed to restore documents: %v", err)
		}
	})
}

// Бенчмарки для измерения производительности
func BenchmarkGetDocument(b *testing.B) {
	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	cfg := DefaultConfig()
	cfg.Host = "localhost"
	cfg.Port = 9308
	cfg.TableName = "library2026"

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

func BenchmarkGetDocumentsBatch(b *testing.B) {
	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	cfg := DefaultConfig()
	cfg.Host = "localhost"
	cfg.Port = 9308
	cfg.TableName = "library2026"
	cfg.BatchSize = 1000

	metrics := &mockMetrics{}

	client, err := NewClient(cfg, log, metrics)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Подготавливаем 100 ID для теста
	ids := make([]uint64, 100)
	for i := 0; i < 100; i++ {
		ids[i] = uint64(6056452479959171000 + uint64(i))
	}

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := client.GetDocumentsBatch(ctx, ids)
		if err != nil {
			b.Fatalf("GetDocumentsBatch failed: %v", err)
		}
	}
}

func BenchmarkBulkReplace(b *testing.B) {
	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	cfg := DefaultConfig()
	cfg.Host = "localhost"
	cfg.Port = 9308
	cfg.TableName = "library2026"

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
			ID:              uint64(9000000000000000000 + uint64(i)),
			Source:          "benchmark",
			Genre:           "test",
			Author:          "benchmark",
			Title:           fmt.Sprintf("Test Document %d", i),
			Content:         "Test content for benchmark",
			GeohashesString: "test1, test2, test3",
			GeohashesUint64: []uint64{1, 2, 3},
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
