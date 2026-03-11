// internal/adapters/ndjson/parser_test.go - исправленная версия
package ndjson

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/terratensor/geoupdater/internal/adapters/logger"
	"github.com/terratensor/geoupdater/internal/core/domain"
)

func TestParseLine(t *testing.T) {
	parser := NewParser(DefaultConfig(), nil, nil)

	tests := []struct {
		name        string
		line        string
		wantDocID   uint64
		wantStrings int
		wantUint64  int
		wantErr     bool
	}{
		{
			name:        "valid line with string doc_id",
			line:        `{"doc_id":"123","geohashes_string":["abc","def"],"geohashes_uint64":[123,456]}`,
			wantDocID:   123,
			wantStrings: 2,
			wantUint64:  2,
			wantErr:     false,
		},
		{
			name:        "valid line with number doc_id",
			line:        `{"doc_id":123,"geohashes_string":["abc","def"],"geohashes_uint64":[123,456]}`,
			wantDocID:   123,
			wantStrings: 2,
			wantUint64:  2,
			wantErr:     false,
		},
		{
			name:        "valid line with large doc_id",
			line:        `{"doc_id":"6056452479959171091","geohashes_string":["abc"],"geohashes_uint64":[123]}`,
			wantDocID:   6056452479959171091,
			wantStrings: 1,
			wantUint64:  1,
			wantErr:     false,
		},
		{
			name:    "missing doc_id",
			line:    `{"geohashes_string":["abc"],"geohashes_uint64":[123]}`,
			wantErr: true,
		},
		{
			name:    "count mismatch",
			line:    `{"doc_id":"123","geohashes_string":["abc"],"geohashes_uint64":[123,456]}`,
			wantErr: true,
		},
		{
			name:    "invalid json",
			line:    `{"doc_id":"123",invalid}`,
			wantErr: true,
		},
		{
			name:    "empty geohashes",
			line:    `{"doc_id":"123","geohashes_string":[],"geohashes_uint64":[]}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := parser.parseLine([]byte(tt.line))

			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if data.DocID != tt.wantDocID {
				t.Errorf("doc_id = %d, want %d", data.DocID, tt.wantDocID)
			}

			if len(data.GeohashesString) != tt.wantStrings {
				t.Errorf("geohashes_string length = %d, want %d",
					len(data.GeohashesString), tt.wantStrings)
			}

			if len(data.GeohashesUint64) != tt.wantUint64 {
				t.Errorf("geohashes_uint64 length = %d, want %d",
					len(data.GeohashesUint64), tt.wantUint64)
			}
		})
	}
}

func TestParseFile(t *testing.T) {
	// Создаем временный файл
	tmpfile, err := os.CreateTemp("", "test*.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	// Пишем тестовые данные с doc_id как в реальном файле (строкой)
	content := `{"doc_id":"1","geohashes_string":["abc"],"geohashes_uint64":[123]}
{"doc_id":"2","geohashes_string":["def","ghi"],"geohashes_uint64":[456,789]}
{"doc_id":"3","geohashes_string":["xyz"],"geohashes_uint64":[101]}`

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	// Создаем логгер для тестов
	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	parser := NewParser(DefaultConfig(), log, nil)

	ctx := context.Background()
	dataChan, errChan := parser.ParseFile(ctx, tmpfile.Name())

	var count int
	var records []*domain.GeoUpdateData

	// Собираем данные
	for data := range dataChan {
		count++
		records = append(records, data)
		if data.DocID == 0 {
			t.Error("received data with empty doc_id")
		}
	}

	// Собираем ошибки
	for err := range errChan {
		t.Errorf("unexpected error: %v", err)
	}

	if count != 3 {
		t.Errorf("expected 3 records, got %d", count)
	}

	// Проверяем конкретные значения
	expectedIDs := []uint64{1, 2, 3}
	for i, record := range records {
		if record.DocID != expectedIDs[i] {
			t.Errorf("record %d: expected ID %d, got %d", i, expectedIDs[i], record.DocID)
		}
	}
}

func TestParseFileWithErrors(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test*.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	// Пишем данные с ошибкой
	content := `{"doc_id":"1","geohashes_string":["abc"],"geohashes_uint64":[123]}
invalid json line
{"doc_id":"2","geohashes_string":["def"],"geohashes_uint64":[456]}`

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	// Тест с пропуском ошибок
	cfg := DefaultConfig()
	cfg.SkipErrors = true

	parser := NewParser(cfg, log, nil)

	ctx := context.Background()
	dataChan, errChan := parser.ParseFile(ctx, tmpfile.Name())

	var count int
	for range dataChan {
		count++
	}

	// Должны получить 2 записи (пропустили ошибочную)
	if count != 2 {
		t.Errorf("expected 2 records with SkipErrors=true, got %d", count)
	}

	// Проверяем что нет ошибок в канале ошибок
	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("unexpected error with SkipErrors=true: %v", err)
		}
	default:
	}

	// Тест без пропуска ошибок
	cfg.SkipErrors = false
	parser = NewParser(cfg, log, nil)

	dataChan, errChan = parser.ParseFile(ctx, tmpfile.Name())

	count = 0
	for range dataChan {
		count++
	}

	// Должна быть только первая запись, потом ошибка
	if count != 1 {
		t.Errorf("expected 1 record with SkipErrors=false, got %d", count)
	}

	// Должна быть ошибка
	select {
	case err := <-errChan:
		if err == nil {
			t.Error("expected error with SkipErrors=false, got nil")
		}
	default:
		t.Error("expected error channel to have an error")
	}
}

func TestParseFiles(t *testing.T) {
	// Создаем несколько тестовых файлов
	var filenames []string

	for i := 0; i < 3; i++ {
		tmpfile, err := os.CreateTemp("", "test*.ndjson")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(tmpfile.Name())

		content := `{"doc_id":"1","geohashes_string":["abc"],"geohashes_uint64":[123]}
{"doc_id":"2","geohashes_string":["def"],"geohashes_uint64":[456]}`

		if _, err := tmpfile.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
		tmpfile.Close()

		filenames = append(filenames, tmpfile.Name())
	}

	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	cfg := DefaultConfig()
	cfg.Workers = 2
	cfg.BatchSize = 10

	parser := NewParser(cfg, log, nil)

	ctx := context.Background()
	dataChan, errChan := parser.ParseFiles(ctx, filenames)

	var count int
	for range dataChan {
		count++
	}

	// Проверяем что получили все записи (3 файла * 2 записи = 6)
	if count != 6 {
		t.Errorf("expected 6 total records, got %d", count)
	}

	// Проверяем ошибки
	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	default:
	}
}

func TestParseReader(t *testing.T) {
	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	parser := NewParser(DefaultConfig(), log, nil)

	content := strings.NewReader(`{"doc_id":"1","geohashes_string":["abc"],"geohashes_uint64":[123]}
{"doc_id":"2","geohashes_string":["def"],"geohashes_uint64":[456]}`)

	ctx := context.Background()
	dataChan, errChan := parser.ParseReader(ctx, content)

	var count int
	var records []*domain.GeoUpdateData

	for data := range dataChan {
		count++
		records = append(records, data)
	}

	if count != 2 {
		t.Errorf("expected 2 records, got %d", count)
	}

	// Проверяем что ID распарсились правильно
	expectedIDs := []uint64{1, 2}
	for i, record := range records {
		if record.DocID != expectedIDs[i] {
			t.Errorf("record %d: expected ID %d, got %d", i, expectedIDs[i], record.DocID)
		}
	}

	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	default:
	}
}

func TestFindFiles(t *testing.T) {
	// Создаем временную директорию с файлами
	tmpDir, err := os.MkdirTemp("", "parser_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Создаем тестовые файлы
	files := []string{
		"data1.ndjson",
		"data2.ndjson",
		"ignore.txt",
		"sub/data3.ndjson",
	}

	for _, file := range files {
		path := filepath.Join(tmpDir, file)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte{}, 0644); err != nil {
			t.Fatal(err)
		}
	}

	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	parser := NewParser(DefaultConfig(), log, nil)

	found, err := parser.FindFiles(tmpDir, "*.ndjson")
	if err != nil {
		t.Fatalf("FindFiles failed: %v", err)
	}

	// Должны найти 3 ndjson файла (включая в поддиректории)
	if len(found) != 3 {
		t.Errorf("expected 3 files, got %d: %v", len(found), found)
	}
}

func TestContextCancellation(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test*.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	// Большой файл для имитации долгой обработки
	var lines []string
	for i := 0; i < 10000; i++ {
		lines = append(lines, `{"doc_id":"1","geohashes_string":["abc"],"geohashes_uint64":[123]}`)
	}
	content := strings.Join(lines, "\n")

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	logCfg := logger.DefaultConfig()
	logCfg.Console = false
	log, _ := logger.NewZapLogger(logCfg)

	parser := NewParser(DefaultConfig(), log, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	dataChan, errChan := parser.ParseFile(ctx, tmpfile.Name())

	var count int
	for range dataChan {
		count++
	}

	// Должны получить не все записи из-за отмены контекста
	t.Logf("Processed %d records before cancellation", count)

	// Проверяем что получили ошибку контекста
	select {
	case err := <-errChan:
		if err == nil {
			t.Error("expected context error, got nil")
		} else {
			t.Logf("Got expected error: %v", err)
		}
	default:
		t.Error("expected error channel to have an error")
	}
}
