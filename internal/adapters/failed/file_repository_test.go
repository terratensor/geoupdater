// internal/adapters/failed/file_repository_test.go - исправленная версия
package failed

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/terratensor/geoupdater/internal/adapters/logger"
	"github.com/terratensor/geoupdater/internal/core/domain"
)

func TestFileRepositorySaveAndLoad(t *testing.T) {
	// Создаем временную директорию для тестов
	tmpDir, err := os.MkdirTemp("", "failed_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Создаем логгер
	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	// Создаем конфигурацию
	cfg := DefaultConfig()
	cfg.FailedDir = tmpDir
	cfg.FilePrefix = "test_failed"
	cfg.FlushInterval = 100 * time.Millisecond

	repo, err := NewFileRepository(cfg, log, nil)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// Создаем тестовые записи
	records := []*domain.FailedRecord{
		{
			Data: &domain.GeoUpdateData{
				DocID:           123,
				GeohashesString: []string{"abc", "def"},
				GeohashesUint64: []uint64{123, 456},
			},
			Error:     "test error 1",
			Attempts:  1,
			Timestamp: time.Now().Unix(),
		},
		{
			Data: &domain.GeoUpdateData{
				DocID:           456,
				GeohashesString: []string{"xyz"},
				GeohashesUint64: []uint64{789},
			},
			Error:     "test error 2",
			Attempts:  2,
			Timestamp: time.Now().Unix(),
		},
	}

	// Сохраняем записи
	if err := repo.SaveBatch(ctx, records); err != nil {
		t.Fatalf("Failed to save records: %v", err)
	}

	// Даем время на запись
	time.Sleep(200 * time.Millisecond)

	// Загружаем записи
	loaded, err := repo.LoadAll(ctx)
	if err != nil {
		t.Fatalf("Failed to load records: %v", err)
	}

	if len(loaded) != 2 {
		t.Errorf("Expected 2 records, got %d", len(loaded))
	}

	// Проверяем содержимое
	found := make(map[uint64]bool)
	for _, r := range loaded {
		found[r.Data.DocID] = true
		if r.Attempts == 0 {
			t.Error("Attempts should not be zero")
		}
	}

	if !found[123] || !found[456] {
		t.Error("Not all records found")
	}
}

func TestFileRepositoryCleanup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "failed_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	cfg := DefaultConfig()
	cfg.FailedDir = tmpDir
	cfg.FilePrefix = "test_failed"

	repo, err := NewFileRepository(cfg, log, nil)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// Создаем записи
	records := []*domain.FailedRecord{
		{
			Data:      &domain.GeoUpdateData{DocID: 123},
			Error:     "error1",
			Attempts:  1,
			Timestamp: time.Now().Unix(),
		},
		{
			Data:      &domain.GeoUpdateData{DocID: 456},
			Error:     "error2",
			Attempts:  1,
			Timestamp: time.Now().Unix(),
		},
		{
			Data:      &domain.GeoUpdateData{DocID: 789},
			Error:     "error3",
			Attempts:  1,
			Timestamp: time.Now().Unix(),
		},
	}

	if err := repo.SaveBatch(ctx, records); err != nil {
		t.Fatalf("Failed to save records: %v", err)
	}

	// Даем время на запись
	time.Sleep(200 * time.Millisecond)

	// Очищаем обработанные записи
	processed := []uint64{123, 789}
	if err := repo.Cleanup(ctx, processed); err != nil {
		t.Fatalf("Failed to cleanup: %v", err)
	}

	// Загружаем оставшиеся
	loaded, err := repo.LoadAll(ctx)
	if err != nil {
		t.Fatalf("Failed to load records: %v", err)
	}

	if len(loaded) != 1 {
		t.Errorf("Expected 1 record after cleanup, got %d", len(loaded))
	}

	if len(loaded) > 0 && loaded[0].Data.DocID != 456 {
		t.Errorf("Expected remaining record ID 456, got %d", loaded[0].Data.DocID)
	}
}

func TestFileRepositoryCount(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "failed_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	cfg := DefaultConfig()
	cfg.FailedDir = tmpDir
	cfg.FilePrefix = "test_failed"

	repo, err := NewFileRepository(cfg, log, nil)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	// Проверяем начальное количество
	count, err := repo.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 records, got %d", count)
	}

	// Добавляем записи
	records := make([]*domain.FailedRecord, 5)
	for i := 0; i < 5; i++ {
		records[i] = &domain.FailedRecord{
			Data:      &domain.GeoUpdateData{DocID: uint64(1000 + i)},
			Error:     "error",
			Attempts:  1,
			Timestamp: time.Now().Unix(),
		}
	}

	if err := repo.SaveBatch(ctx, records); err != nil {
		t.Fatalf("Failed to save records: %v", err)
	}

	// Даем время на запись
	time.Sleep(200 * time.Millisecond)

	// Проверяем количество
	count, err = repo.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if count != 5 {
		t.Errorf("Expected 5 records, got %d", count)
	}
}

func TestFileRepositoryLoadByAge(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "failed_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	cfg := DefaultConfig()
	cfg.FailedDir = tmpDir
	cfg.FilePrefix = "test_failed"

	repo, err := NewFileRepository(cfg, log, nil)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer repo.Close()

	ctx := context.Background()

	now := time.Now()

	// Создаем записи разного возраста
	records := []*domain.FailedRecord{
		{
			Data:      &domain.GeoUpdateData{DocID: 1001},
			Error:     "error",
			Attempts:  1,
			Timestamp: now.Unix(),
		},
		{
			Data:      &domain.GeoUpdateData{DocID: 1002},
			Error:     "error",
			Attempts:  1,
			Timestamp: now.Unix(),
		},
		{
			Data:      &domain.GeoUpdateData{DocID: 2001},
			Error:     "error",
			Attempts:  1,
			Timestamp: now.Add(-48 * time.Hour).Unix(),
		},
		{
			Data:      &domain.GeoUpdateData{DocID: 2002},
			Error:     "error",
			Attempts:  1,
			Timestamp: now.Add(-72 * time.Hour).Unix(),
		},
	}

	if err := repo.SaveBatch(ctx, records); err != nil {
		t.Fatalf("Failed to save records: %v", err)
	}

	// Даем время на запись
	time.Sleep(200 * time.Millisecond)

	// Загружаем записи не старше 24 часов
	loaded, err := repo.LoadByAge(ctx, 24)
	if err != nil {
		t.Fatalf("Failed to load by age: %v", err)
	}

	if len(loaded) != 2 {
		t.Errorf("Expected 2 records younger than 24h, got %d", len(loaded))
	}

	// Загружаем записи не старше 48 часов
	loaded, err = repo.LoadByAge(ctx, 48)
	if err != nil {
		t.Fatalf("Failed to load by age: %v", err)
	}

	if len(loaded) != 3 {
		t.Errorf("Expected 3 records younger than 48h, got %d", len(loaded))
	}
}
