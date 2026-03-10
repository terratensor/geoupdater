// internal/adapters/logger/factory.go
package logger

import (
	"fmt"

	"github.com/terratensor/geoupdater/internal/core/ports"
)

// Factory создает логгеры на основе конфигурации
type Factory struct{}

// NewFactory создает новую фабрику логгеров
func NewFactory() *Factory {
	return &Factory{}
}

// Create создает логгер на основе конфигурации
func (f *Factory) Create(level, outputPath string, console bool) (ports.Logger, error) {
	cfg := &Config{
		Level:      level,
		OutputPath: outputPath,
		FileOutput: outputPath != "",
		Console:    console,
		AddSource:  true,
	}

	return NewZapLogger(cfg)
}

// CreateFromConfig создает логгер из готовой конфигурации
func (f *Factory) CreateFromConfig(cfg *Config) (ports.Logger, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	return NewZapLogger(cfg)
}

// MustCreate создает логгер или паникует при ошибке
func (f *Factory) MustCreate(level, outputPath string, console bool) ports.Logger {
	logger, err := f.Create(level, outputPath, console)
	if err != nil {
		panic(fmt.Sprintf("failed to create logger: %v", err))
	}
	return logger
}
