// internal/adapters/manticore/id_precision_test.go
package manticore

import (
	"context"
	"testing"
	"time"

	"github.com/terratensor/geoupdater/internal/adapters/logger"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

func TestIDPrecision(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	logCfg := logger.DefaultConfig()
	logCfg.Console = true
	logCfg.Level = "error"
	log, _ := logger.NewZapLogger(logCfg)

	baseURL := "http://localhost:9308"
	tableName := "library2026"
	timeout := 30 * time.Second
	metrics := &mockMetrics{}

	client := NewJSONClient(baseURL, tableName, timeout, log, metrics)
	ctx := context.Background()

	testIDs := []uint64{
		6056452479959171091,
		6056452479959171088,
		6056452479959171104,
	}

	for _, id := range testIDs {
		t.Run("ID_"+string(rune(id)), func(t *testing.T) {
			doc, err := client.SearchByID(ctx, id)
			if err != nil {
				if err == ports.ErrNotFound {
					t.Skipf("Document %d not found", id)
				}
				t.Fatalf("SearchByID failed: %v", err)
			}

			// Проверяем точность ID
			if doc.ID != id {
				t.Errorf("ID PRECISION LOST!\n  Original: %d\n  Got:      %d\n  Diff:     %d",
					id, doc.ID, int64(id)-int64(doc.ID))
			} else {
				t.Logf("✓ ID %d passed precision test", id)
			}
		})
	}
}
