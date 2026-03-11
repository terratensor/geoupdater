// internal/adapters/failed/factory.go
package failed

import (
	"fmt"

	"github.com/terratensor/geoupdater/internal/core/ports"
)

// Factory создает репозитории для failed записей
type Factory struct{}

// NewFactory создает новую фабрику
func NewFactory() *Factory {
	return &Factory{}
}

// Create создает новый файловый репозиторий
func (f *Factory) Create(cfg *Config, logger ports.Logger, metrics ports.MetricsCollector) (*FileRepository, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	return NewFileRepository(cfg, logger, metrics)
}

// CreateWithDir создает репозиторий с указанной директорией
func (f *Factory) CreateWithDir(dir string, logger ports.Logger, metrics ports.MetricsCollector) (*FileRepository, error) {
	cfg := DefaultConfig()
	cfg.FailedDir = dir
	return NewFileRepository(cfg, logger, metrics)
}

// MustCreate создает репозиторий или паникует при ошибке
func (f *Factory) MustCreate(cfg *Config, logger ports.Logger, metrics ports.MetricsCollector) *FileRepository {
	repo, err := f.Create(cfg, logger, metrics)
	if err != nil {
		panic(fmt.Sprintf("failed to create failed repository: %v", err))
	}
	return repo
}
