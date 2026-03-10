// internal/adapters/manticore/factory.go - обновляем с учетом новых параметров
package manticore

import (
	"context"
	"fmt"

	"github.com/terratensor/geoupdater/internal/core/ports"
)

// Factory создает Manticore клиенты
type Factory struct{}

// NewFactory создает новую фабрику
func NewFactory() *Factory {
	return &Factory{}
}

// Create создает новый Manticore клиент
func (f *Factory) Create(ctx context.Context, cfg *Config, logger ports.Logger, metrics ports.MetricsCollector) (*Client, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	return NewClient(cfg, logger, metrics)
}

// CreateWithConfig создает клиент с переданной конфигурацией
func (f *Factory) CreateWithConfig(ctx context.Context, cfg *Config, logger ports.Logger, metrics ports.MetricsCollector) (*Client, error) {
	return NewClient(cfg, logger, metrics)
}

// CreateWithBatchSize создает клиент с определенным размером батча
func (f *Factory) CreateWithBatchSize(ctx context.Context, batchSize int, logger ports.Logger, metrics ports.MetricsCollector) (*Client, error) {
	cfg := DefaultConfig()
	cfg.BatchSize = batchSize
	return NewClient(cfg, logger, metrics)
}

// MustCreate создает клиент или паникует при ошибке
func (f *Factory) MustCreate(ctx context.Context, cfg *Config, logger ports.Logger, metrics ports.MetricsCollector) *Client {
	client, err := f.Create(ctx, cfg, logger, metrics)
	if err != nil {
		panic(fmt.Sprintf("failed to create manticore client: %v", err))
	}
	return client
}
