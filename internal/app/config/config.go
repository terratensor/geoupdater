// internal/app/config/config.go
package config

import (
	"log"
	"time"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	// Manticore configuration
	ManticoreHost     string        `envconfig:"MANTICORE_HOST" default:"localhost"`
	ManticorePort     int           `envconfig:"MANTICORE_PORT" default:"9308"`
	ManticoreTable    string        `envconfig:"MANTICORE_TABLE" default:"library2026"`
	ManticoreTimeout  time.Duration `envconfig:"MANTICORE_TIMEOUT" default:"30s"`
	ManticoreMaxConns int           `envconfig:"MANTICORE_MAX_CONNS" default:"10"`

	// Update configuration
	UpdateMode string        `envconfig:"UPDATE_MODE" default:"merge"` // replace or merge
	BatchSize  int           `envconfig:"BATCH_SIZE" default:"1000"`
	Workers    int           `envconfig:"WORKERS" default:"5"`
	MaxRetries int           `envconfig:"MAX_RETRIES" default:"3"`
	RetryDelay time.Duration `envconfig:"RETRY_DELAY" default:"1s"`

	// File processing
	InputDir    string `envconfig:"INPUT_DIR" default:"./data"`
	FilePattern string `envconfig:"FILE_PATTERN" default:"*.ndjson"`
	FailedDir   string `envconfig:"FAILED_DIR" default:"./failed"`

	// Logging
	LogLevel string `envconfig:"LOG_LEVEL" default:"info"`
	LogFile  string `envconfig:"LOG_FILE" default:"./logs/geoupdater.log"`

	// Failed records reprocessing
	EnableReprocess  bool          `envconfig:"ENABLE_REPROCESS" default:"true"`
	ReprocessWorkers int           `envconfig:"REPROCESS_WORKERS" default:"3"`
	MaxFailedAge     time.Duration `envconfig:"MAX_FAILED_AGE" default:"168h"` // 7 days
}

// Load загружает конфигурацию из переменных окружения и .env файла
func Load() (*Config, error) {
	// Загружаем .env файл если он существует
	_ = godotenv.Load()

	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		return nil, err
	}

	// Валидация конфигурации
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Validate проверяет корректность конфигурации
func (c *Config) Validate() error {
	if c.ManticoreHost == "" {
		log.Println("warning: MANTICORE_HOST is empty, using default localhost")
	}

	if c.ManticorePort <= 0 || c.ManticorePort > 65535 {
		log.Println("warning: invalid MANTICORE_PORT, using default 9308")
		c.ManticorePort = 9308
	}

	if c.UpdateMode != "replace" && c.UpdateMode != "merge" {
		log.Printf("warning: invalid UPDATE_MODE '%s', using default 'merge'", c.UpdateMode)
		c.UpdateMode = "merge"
	}

	if c.BatchSize <= 0 {
		log.Println("warning: invalid BATCH_SIZE, using default 1000")
		c.BatchSize = 1000
	}

	if c.Workers <= 0 {
		log.Println("warning: invalid WORKERS, using default 5")
		c.Workers = 5
	}

	return nil
}

// ManticoreURL возвращает полный URL для подключения к Manticore
func (c *Config) ManticoreURL() string {
	return "http://" + c.ManticoreHost + ":" + string(rune(c.ManticorePort))
}
