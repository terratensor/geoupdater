// internal/adapters/logger/logger_test.go
package logger

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/terratensor/geoupdater/internal/core/ports"
)

func TestZapLoggerBasic(t *testing.T) {
	// Создаем временную директорию для логов
	tmpDir, err := os.MkdirTemp("", "logger_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logFile := filepath.Join(tmpDir, "test.log")

	cfg := &Config{
		Level:      "debug",
		OutputPath: logFile,
		FileOutput: true,
		Console:    false, // Отключаем консоль для тестов
		AddSource:  true,
	}

	logger, err := NewZapLogger(cfg)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync()

	// Тестируем различные уровни логирования
	logger.Debug("debug message", ports.String("key", "value"))
	logger.Info("info message", ports.Int("count", 42))
	logger.Warn("warn message", ports.Error(err))
	logger.Error("error message", ports.Any("data", map[string]string{"foo": "bar"}))

	// Проверяем With
	childLogger := logger.With(ports.String("component", "test"))
	childLogger.Info("child logger message")

	// Проверяем WithContext
	ctx := context.WithValue(context.Background(), "request_id", "req-123")
	ctxLogger := logger.WithContext(ctx)
	ctxLogger.Info("context logger message")
}

func TestZapLoggerLevels(t *testing.T) {
	tests := []struct {
		name      string
		level     string
		logFunc   func(ports.Logger)
		shouldLog bool
	}{
		{
			name:  "debug level logs debug",
			level: "debug",
			logFunc: func(l ports.Logger) {
				l.Debug("debug message")
			},
			shouldLog: true,
		},
		{
			name:  "info level does not log debug",
			level: "info",
			logFunc: func(l ports.Logger) {
				l.Debug("debug message")
			},
			shouldLog: false,
		},
		{
			name:  "error level logs error",
			level: "error",
			logFunc: func(l ports.Logger) {
				l.Error("error message")
			},
			shouldLog: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir, err := os.MkdirTemp("", "logger_test")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tmpDir)

			logFile := filepath.Join(tmpDir, "test.log")

			cfg := &Config{
				Level:      tt.level,
				OutputPath: logFile,
				FileOutput: true,
				Console:    false,
			}

			logger, err := NewZapLogger(cfg)
			if err != nil {
				t.Fatalf("Failed to create logger: %v", err)
			}
			defer logger.Sync()

			tt.logFunc(logger)

			// Даем время на запись
			time.Sleep(100 * time.Millisecond)

			// Проверяем содержимое файла
			content, err := os.ReadFile(logFile)
			if err != nil {
				t.Fatal(err)
			}

			hasLogs := len(content) > 0
			if hasLogs != tt.shouldLog {
				t.Errorf("Expected shouldLog=%v, but hasLogs=%v", tt.shouldLog, hasLogs)
			}
		})
	}
}

func TestZapLoggerFields(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "logger_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logFile := filepath.Join(tmpDir, "test.log")

	cfg := &Config{
		Level:      "debug",
		OutputPath: logFile,
		FileOutput: true,
		Console:    false,
	}

	logger, err := NewZapLogger(cfg)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync()

	// Логируем с разными типами полей
	logger.Info("test with fields",
		ports.String("string", "value"),
		ports.Int("int", 42),
		ports.Error(err),
		ports.Any("any", struct{ Name string }{"test"}),
	)

	// Проверяем что записалось без ошибок
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}

	if len(content) == 0 {
		t.Error("Log file is empty")
	}
}

func TestZapLoggerConcurrency(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "logger_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	logFile := filepath.Join(tmpDir, "test.log")

	cfg := &Config{
		Level:      "debug",
		OutputPath: logFile,
		FileOutput: true,
		Console:    false,
	}

	logger, err := NewZapLogger(cfg)
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync()

	// Запускаем несколько горутин для параллельной записи
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				logger.Info("concurrent log",
					ports.Int("goroutine", id),
					ports.Int("iteration", j),
				)
			}
			done <- true
		}(i)
	}

	// Ждем завершения всех горутин
	for i := 0; i < 10; i++ {
		<-done
	}
}
