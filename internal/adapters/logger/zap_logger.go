// internal/adapters/logger/zap_logger.go
package logger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/terratensor/geoupdater/internal/core/ports"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ZapLogger адаптер для zap логгера
type ZapLogger struct {
	logger *zap.Logger
	config *Config
}

// Config конфигурация для zap логгера
type Config struct {
	Level      string
	OutputPath string
	FileOutput bool
	Console    bool
	AddSource  bool
}

// NewZapLogger создает новый экземпляр ZapLogger
func NewZapLogger(cfg *Config) (*ZapLogger, error) {
	// Настройка уровня логирования
	level := zap.NewAtomicLevel()
	if err := level.UnmarshalText([]byte(cfg.Level)); err != nil {
		return nil, fmt.Errorf("invalid log level %s: %w", cfg.Level, err)
	}

	// Настройка энкодера
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// Создаем cores
	var cores []zapcore.Core

	// Консольный вывод (всегда в JSON формате как требуется)
	if cfg.Console {
		consoleEncoder := zapcore.NewJSONEncoder(encoderConfig)
		consoleWriter := zapcore.Lock(os.Stdout)
		cores = append(cores, zapcore.NewCore(consoleEncoder, consoleWriter, level))
	}

	// Файловый вывод
	if cfg.FileOutput && cfg.OutputPath != "" {
		// Создаем директорию для логов если нужно
		if err := os.MkdirAll(filepath.Dir(cfg.OutputPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}

		file, err := os.OpenFile(cfg.OutputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %w", err)
		}

		fileEncoder := zapcore.NewJSONEncoder(encoderConfig)
		fileWriter := zapcore.AddSync(file)
		cores = append(cores, zapcore.NewCore(fileEncoder, fileWriter, level))
	}

	// Объединяем cores
	core := zapcore.NewTee(cores...)

	// Создаем логгер
	logger := zap.New(core, zap.AddCallerSkip(1), zap.AddCaller())
	if cfg.AddSource {
		logger = logger.WithOptions(zap.AddCaller())
	}

	return &ZapLogger{
		logger: logger,
		config: cfg,
	}, nil
}

// Debug логирует отладочное сообщение
func (l *ZapLogger) Debug(msg string, fields ...ports.Field) {
	l.logger.Debug(msg, l.toZapFields(fields)...)
}

// Info логирует информационное сообщение
func (l *ZapLogger) Info(msg string, fields ...ports.Field) {
	l.logger.Info(msg, l.toZapFields(fields)...)
}

// Warn логирует предупреждение
func (l *ZapLogger) Warn(msg string, fields ...ports.Field) {
	l.logger.Warn(msg, l.toZapFields(fields)...)
}

// Error логирует ошибку
func (l *ZapLogger) Error(msg string, fields ...ports.Field) {
	l.logger.Error(msg, l.toZapFields(fields)...)
}

// Fatal логирует фатальную ошибку и завершает программу
func (l *ZapLogger) Fatal(msg string, fields ...ports.Field) {
	l.logger.Fatal(msg, l.toZapFields(fields)...)
}

// With создает дочерний логгер с дополнительными полями
func (l *ZapLogger) With(fields ...ports.Field) ports.Logger {
	return &ZapLogger{
		logger: l.logger.With(l.toZapFields(fields)...),
		config: l.config,
	}
}

// WithContext создает логгер с контекстом (добавляет request_id и т.д.)
func (l *ZapLogger) WithContext(ctx context.Context) ports.Logger {
	fields := []ports.Field{}

	// Извлекаем request_id из контекста если есть
	if reqID, ok := ctx.Value("request_id").(string); ok {
		fields = append(fields, ports.String("request_id", reqID))
	}

	// Извлекаем trace_id из контекста если есть
	if traceID, ok := ctx.Value("trace_id").(string); ok {
		fields = append(fields, ports.String("trace_id", traceID))
	}

	return l.With(fields...)
}

// Sync сбрасывает буферы логов
func (l *ZapLogger) Sync() error {
	return l.logger.Sync()
}

// toZapFields конвертирует ports.Field в zap.Field
func (l *ZapLogger) toZapFields(fields []ports.Field) []zap.Field {
	zapFields := make([]zap.Field, 0, len(fields))

	for _, f := range fields {
		switch v := f.Value().(type) {
		case string:
			zapFields = append(zapFields, zap.String(f.Key(), v))
		case int:
			zapFields = append(zapFields, zap.Int(f.Key(), v))
		case int64:
			zapFields = append(zapFields, zap.Int64(f.Key(), v))
		case float64:
			zapFields = append(zapFields, zap.Float64(f.Key(), v))
		case bool:
			zapFields = append(zapFields, zap.Bool(f.Key(), v))
		case error:
			zapFields = append(zapFields, zap.Error(v))
		case time.Time:
			zapFields = append(zapFields, zap.Time(f.Key(), v))
		case time.Duration:
			zapFields = append(zapFields, zap.Duration(f.Key(), v))
		default:
			zapFields = append(zapFields, zap.Any(f.Key(), v))
		}
	}

	return zapFields
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() *Config {
	return &Config{
		Level:      "info",
		OutputPath: "./logs/geoupdater.log",
		FileOutput: true,
		Console:    true,
		AddSource:  true,
	}
}
