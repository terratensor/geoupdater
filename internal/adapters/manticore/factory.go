// internal/adapters/manticore/factory.go
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

// CreateWithTable создает клиент для конкретной таблицы
func (f *Factory) CreateWithTable(ctx context.Context, tableName string, logger ports.Logger, metrics ports.MetricsCollector) (*Client, error) {
	cfg := DefaultConfig()
	cfg.TableName = tableName
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
