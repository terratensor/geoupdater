// internal/adapters/manticore/reality_check_test.go
package manticore

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/terratensor/geoupdater/internal/adapters/logger"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

// TestRealityCheck проверяет что мы действительно можем работать с вашей таблицей
func TestRealityCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping reality check in short mode")
	}

	logCfg := logger.DefaultConfig()
	logCfg.Console = true
	logCfg.Level = "debug"
	log, err := logger.NewZapLogger(logCfg)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	cfg := DefaultConfig()
	cfg.Host = "localhost"
	cfg.Port = 9308
	cfg.TableName = "library2026"
	cfg.Timeout = 10 * time.Second

	t.Logf("Connecting to Manticore at %s:%d", cfg.Host, cfg.Port)

	metrics := &mockMetrics{}

	client, err := NewClient(cfg, log, metrics)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Проверяем что таблица существует
	ctx := context.Background()

	// Используем SQL чтобы убедиться что таблица есть
	t.Log("Executing SHOW TABLES...")
	query := "SHOW TABLES"
	resp, httpResp, err := client.apiClient.UtilsAPI.Sql(ctx).Body(query).Execute()
	if err != nil {
		if httpResp != nil {
			t.Logf("HTTP Response status: %s", httpResp.Status)
		}
		t.Fatalf("Failed to execute SHOW TABLES: %v", err)
	}

	t.Logf("Tables in Manticore: %+v", resp)

	// Проверяем структуру таблицы
	t.Logf("Describing table %s...", cfg.TableName)
	query = fmt.Sprintf("DESCRIBE %s", cfg.TableName)
	resp, httpResp, err = client.apiClient.UtilsAPI.Sql(ctx).Body(query).Execute()
	if err != nil {
		if httpResp != nil {
			t.Logf("HTTP Response status: %s", httpResp.Status)
		}
		t.Fatalf("Failed to describe table: %v", err)
	}

	t.Logf("Table structure: %+v", resp)

	// Проверяем количество записей
	t.Log("Counting records...")
	query = fmt.Sprintf("SELECT COUNT(*) FROM %s", cfg.TableName)
	resp, httpResp, err = client.apiClient.UtilsAPI.Sql(ctx).Body(query).Execute()
	if err != nil {
		if httpResp != nil {
			t.Logf("HTTP Response status: %s", httpResp.Status)
		}
		t.Fatalf("Failed to count records: %v", err)
	}

	t.Logf("Record count: %+v", resp)

	// Проверяем что можем получить документ по ID из примера
	testIDStr := "6056452479959171091"
	testID, err := strconv.ParseUint(testIDStr, 10, 64)
	if err != nil {
		t.Fatalf("Failed to parse test ID: %v", err)
	}

	t.Logf("Attempting to get document with ID: %d", testID)

	doc, err := client.GetDocument(ctx, testID)
	if err != nil {
		if err == ports.ErrNotFound {
			t.Logf("Document %d not found - this is OK if it's not in your test data", testID)
		} else {
			t.Errorf("Error getting document: %v", err)
		}
	} else {
		t.Logf("Successfully retrieved document: ID=%d", doc.ID)
		t.Logf("  Source: %s", doc.Source)
		t.Logf("  Genre: %s", doc.Genre)
		t.Logf("  Author: %s", doc.Author)
		t.Logf("  Title: %s", doc.Title)
		t.Logf("  Geohashes: %s", doc.GeohashesString)
		t.Logf("  GeohashesUint64: %v", doc.GeohashesUint64)
	}
}
