// internal/adapters/manticore/json_client_test.go - обновленная версия с правильным логированием
package manticore

import (
	"context"
	"testing"
	"time"

	"github.com/terratensor/geoupdater/internal/adapters/logger"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

// Глобальная переменная для управления подробностью логов
var verbose = false

func TestJSONClient(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Настраиваем логгер: только ошибки в консоль, подробно в файл
	logCfg := logger.DefaultConfig()
	logCfg.Console = true
	if verbose {
		logCfg.Level = "debug"
		logCfg.FileOutput = true
		logCfg.OutputPath = "./logs/test_json_client.log"
	} else {
		logCfg.Level = "error" // Только ошибки в консоль
		logCfg.FileOutput = false
	}

	log, err := logger.NewZapLogger(logCfg)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// Создаем JSON клиент
	baseURL := "http://localhost:9308"
	tableName := "library2026"
	timeout := 30 * time.Second
	metrics := &mockMetrics{}

	client := NewJSONClient(baseURL, tableName, timeout, log, metrics)

	ctx := context.Background()

	// Тест Ping
	t.Run("Ping", func(t *testing.T) {
		err := client.Ping(ctx)
		if err != nil {
			t.Errorf("Ping failed: %v", err)
		}
	})

	// Тест SearchByID
	t.Run("SearchByID", func(t *testing.T) {
		testID := uint64(6056452479959171091)

		doc, err := client.SearchByID(ctx, testID)
		if err != nil {
			if err == ports.ErrNotFound {
				t.Logf("Document %d not found - this is expected if document doesn't exist", testID)
				return
			}
			t.Fatalf("SearchByID failed: %v", err)
		}

		if doc.ID != testID {
			t.Errorf("ID mismatch: got %d, want %d", doc.ID, testID)
		}

		t.Logf("Found document: ID=%d, Title=%s", doc.ID, doc.Title)
	})

	// Тест SearchByIDs с несколькими ID
	t.Run("SearchByIDs", func(t *testing.T) {
		ids := []uint64{
			6056452479959171091,
			6056452479959171088,
			6056452479959171104,
		}
		maxMatches := 1000

		docs, err := client.SearchByIDs(ctx, ids, maxMatches)
		if err != nil {
			t.Fatalf("SearchByIDs failed: %v", err)
		}

		t.Logf("Found %d out of %d requested documents", len(docs), len(ids))
		for id, doc := range docs {
			t.Logf("  - %d: %s", id, doc.Title)
		}
	})

	// Тест BulkSearchByIDs
	t.Run("BulkSearchByIDs", func(t *testing.T) {
		// Создаем 2500 ID для теста
		ids := make([]uint64, 2500)
		for i := 0; i < 2500; i++ {
			ids[i] = uint64(6056452479959171000 + uint64(i))
		}
		batchSize := 1000
		maxMatches := 1000

		start := time.Now()
		docs, err := client.BulkSearchByIDs(ctx, ids, batchSize, maxMatches)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("BulkSearchByIDs failed: %v", err)
		}

		t.Logf("Bulk search found %d documents in %v", len(docs), duration)
		t.Logf("Average per batch: %v", duration/3)
	})
}
