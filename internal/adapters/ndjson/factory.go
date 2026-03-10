// internal/adapters/ndjson/factory.go
package ndjson

import (
	"github.com/terratensor/geoupdater/internal/core/ports"
)

// Factory создает парсеры на основе конфигурации
type Factory struct{}

// NewFactory создает новую фабрику парсеров
func NewFactory() *Factory {
	return &Factory{}
}

// Create создает новый парсер
func (f *Factory) Create(cfg *Config, logger ports.Logger, metrics ports.MetricsCollector) *Parser {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	return NewParser(cfg, logger, metrics)
}

// CreateWithWorkers создает парсер с указанным количеством воркеров
func (f *Factory) CreateWithWorkers(workers int, logger ports.Logger, metrics ports.MetricsCollector) *Parser {
	cfg := DefaultConfig()
	cfg.Workers = workers
	return NewParser(cfg, logger, metrics)
}

// CreateWithBatchSize создает парсер с указанным размером батча
func (f *Factory) CreateWithBatchSize(batchSize int, logger ports.Logger, metrics ports.MetricsCollector) *Parser {
	cfg := DefaultConfig()
	cfg.BatchSize = batchSize
	return NewParser(cfg, logger, metrics)
}
