// examples/logger_example/main.go
package main

import (
	"context"
	"time"

	"github.com/terratensor/geoupdater/internal/adapters/logger"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

func main() {
	// Создаем логгер с конфигурацией по умолчанию
	log, err := logger.NewZapLogger(logger.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer log.Sync()

	// Базовое логирование
	log.Info("Application started",
		ports.String("version", "1.0.0"),
		ports.String("environment", "development"),
	)

	// Логирование с ошибкой
	err = someOperation()
	if err != nil {
		log.Error("Operation failed",
			ports.Error(err),
			ports.String("operation", "someOperation"),
		)
	}

	// Логирование с контекстом
	ctx := context.WithValue(context.Background(), "request_id", "req-12345")
	ctxLogger := log.WithContext(ctx)

	ctxLogger.Info("Processing request",
		ports.String("path", "/api/update"),
		ports.Int("duration_ms", 150),
	)

	// Создание дочернего логгера
	componentLogger := log.With(
		ports.String("component", "parser"),
		ports.String("file", "results.ndjson"),
	)

	// Имитация обработки
	for i := 0; i < 5; i++ {
		componentLogger.Debug("Processing line",
			ports.Int("line", i),
			ports.String("status", "ok"),
		)
		time.Sleep(100 * time.Millisecond)
	}

	componentLogger.Info("File processing completed",
		ports.Int("lines", 5),
		ports.Int("errors", 0),
	)
}

func someOperation() error {
	// Имитация ошибки
	return ports.ErrorField{}.Value().(error)
}
