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
)

func TestParseLine(t *testing.T) {
	parser := NewParser(DefaultConfig(), nil, nil)

	tests := []struct {
		name        string
		line        string
		wantDocID   uint64 // Меняем тип на uint64
		wantStrings int
		wantUint64  int
		wantErr     bool
	}{
		{
			name:        "valid line",
			line:        `{"doc_id":"123","geohashes_string":["abc","def"],"geohashes_uint64":[123,456]}`,
			wantDocID:   123, // Убираем кавычки
			wantStrings: 2,
			wantUint64:  2,
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
		{
			name:        "doc_id as number",
			line:        `{"doc_id":456,"geohashes_string":["abc"],"geohashes_uint64":[123]}`,
			wantDocID:   456,
			wantStrings: 1,
			wantUint64:  1,
			wantErr:     false,
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

			// Сравниваем uint64 с uint64
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

	// Пишем тестовые данные
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
	var errors []error

	// Собираем данные
	for data := range dataChan {
		count++
		if data.DocID == 0 { // Сравниваем с 0, а не с пустой строкой
			t.Error("received data with empty doc_id")
		}
	}

	// Собираем ошибки
	for err := range errChan {
		errors = append(errors, err)
	}

	if count != 3 {
		t.Errorf("expected 3 records, got %d", count)
	}

	if len(errors) > 0 {
		t.Errorf("unexpected errors: %v", errors)
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
	err = <-errChan
	if err == nil {
		t.Error("expected error with SkipErrors=false, got nil")
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
	for data := range dataChan {
		count++
		if data.DocID == 0 { // Сравниваем с 0
			t.Error("received data with empty doc_id")
		}
	}

	if count != 2 {
		t.Errorf("expected 2 records, got %d", count)
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

// internal/adapters/ndjson/parser_test.go - исправленная версия теста
func TestContextCancellation(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "test*.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	// УВЕЛИЧИВАЕМ РАЗМЕР ФАЙЛА для гарантии, что обработка не завершится до таймаута
	// Создаём файл с 1,000,000 строк вместо 10,000
	var lines []string
	for i := 0; i < 1000000; i++ {
		lines = append(lines, `{"doc_id":123456,"geohashes_string":["abc"],"geohashes_uint64":[123]}`)
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

	// Устанавливаем очень маленький таймаут, чтобы гарантированно не успеть обработать весь файл
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Даем контексту немного времени, чтобы "осознать" таймаут
	time.Sleep(2 * time.Millisecond)

	dataChan, errChan := parser.ParseFile(ctx, tmpfile.Name())

	var count int
	for range dataChan {
		count++
	}

	// Проверяем что получили ошибку контекста
	err = <-errChan
	if err == nil {
		t.Error("expected context error, got nil")
	} else if !strings.Contains(err.Error(), "context deadline exceeded") &&
		!strings.Contains(err.Error(), "context canceled") {
		t.Errorf("expected context error, got: %v", err)
	}

	t.Logf("Processed %d lines before cancellation", count)
}
