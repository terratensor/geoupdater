// internal/app/config/config.go - обновляем с учетом поисковых параметров
package config

import (
	"fmt"
	"log"
	"time"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"github.com/terratensor/geoupdater/internal/adapters/failed"
	"github.com/terratensor/geoupdater/internal/adapters/logger"
	"github.com/terratensor/geoupdater/internal/adapters/manticore"
	"github.com/terratensor/geoupdater/internal/adapters/ndjson"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

type Config struct {
	// Manticore configuration
	ManticoreHost     string        `envconfig:"MANTICORE_HOST" default:"localhost"`
	ManticorePort     int           `envconfig:"MANTICORE_PORT" default:"9308"`
	ManticoreTable    string        `envconfig:"MANTICORE_TABLE" default:"library2026"`
	ManticoreTimeout  time.Duration `envconfig:"MANTICORE_TIMEOUT" default:"30s"`
	ManticoreMaxConns int           `envconfig:"MANTICORE_MAX_CONNS" default:"10"`

	// Search configuration
	SearchBatchSize  int `envconfig:"SEARCH_BATCH_SIZE" default:"1000"`  // размер батча для поиска
	SearchMaxMatches int `envconfig:"SEARCH_MAX_MATCHES" default:"1000"` // max_matches для поиска

	// Update configuration
	UpdateMode string        `envconfig:"UPDATE_MODE" default:"merge"` // replace or merge
	BatchSize  int           `envconfig:"BATCH_SIZE" default:"1000"`   // размер батча для replace
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

	// NER configuration
	NERMode        bool   `envconfig:"NER_MODE" default:"false"`
	NERTableSuffix string `envconfig:"NER_TABLE_SUFFIX" default:"_ner"`
	NERBatchSize   int    `envconfig:"NER_BATCH_SIZE" default:"1000"`
	NERWorkers     int    `envconfig:"NER_WORKERS" default:"5"`
}

// Load загружает конфигурацию из переменных окружения и .env файла
func Load() (*Config, error) {
	_ = godotenv.Load()

	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		return nil, err
	}

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

	if c.SearchBatchSize <= 0 {
		log.Println("warning: invalid SEARCH_BATCH_SIZE, using default 1000")
		c.SearchBatchSize = 1000
	}

	if c.SearchMaxMatches < c.SearchBatchSize {
		log.Printf("warning: SEARCH_MAX_MATCHES (%d) < SEARCH_BATCH_SIZE (%d), adjusting to %d",
			c.SearchMaxMatches, c.SearchBatchSize, c.SearchBatchSize)
		c.SearchMaxMatches = c.SearchBatchSize
	}

	if c.Workers <= 0 {
		log.Println("warning: invalid WORKERS, using default 5")
		c.Workers = 5
	}

	if c.NERBatchSize <= 0 {
		log.Println("warning: invalid NER_BATCH_SIZE, using default 1000")
		c.NERBatchSize = 1000
	}

	if c.NERWorkers <= 0 {
		log.Println("warning: invalid NER_WORKERS, using default 5")
		c.NERWorkers = 5
	}

	return nil
}

// ManticoreURL возвращает полный URL для подключения к Manticore
func (c *Config) ManticoreURL() string {
	return fmt.Sprintf("http://%s:%d", c.ManticoreHost, c.ManticorePort)
}

// CreateManticoreClient создает Manticore клиент из конфигурации
func (c *Config) CreateManticoreClient(logger ports.Logger, metrics ports.MetricsCollector) (*manticore.Client, error) {
	manticoreCfg := &manticore.Config{
		Host:       c.ManticoreHost,
		Port:       c.ManticorePort,
		TableName:  c.ManticoreTable,
		Timeout:    c.ManticoreTimeout,
		MaxConns:   c.ManticoreMaxConns,
		RetryCount: c.MaxRetries,
		RetryDelay: c.RetryDelay,
		BatchSize:  c.SearchBatchSize, // передаем размер батча для поиска
	}

	return manticore.NewClient(manticoreCfg, logger, metrics)
}

// CreateLogger создает логгер из конфигурации
func (c *Config) CreateLogger() (ports.Logger, error) {
	logCfg := &logger.Config{
		Level:      c.LogLevel,
		OutputPath: c.LogFile,
		FileOutput: c.LogFile != "",
		Console:    true,
		AddSource:  true,
	}

	factory := logger.NewFactory()
	return factory.CreateFromConfig(logCfg)
}

// CreateParser создает парсер из конфигурации
func (c *Config) CreateParser(logger ports.Logger, metrics ports.MetricsCollector) *ndjson.Parser {
	parserCfg := &ndjson.Config{
		BatchSize:   c.BatchSize,
		Workers:     c.Workers,
		Validate:    true,
		SkipErrors:  true,
		MaxLineSize: 10 * 1024 * 1024,
	}

	factory := ndjson.NewFactory()
	return factory.Create(parserCfg, logger, metrics)
}

// CreateFailedRepository создает репозиторий для failed записей
func (c *Config) CreateFailedRepository(logger ports.Logger, metrics ports.MetricsCollector) (*failed.FileRepository, error) {
	failedCfg := &failed.Config{
		FailedDir:      c.FailedDir,
		FilePrefix:     "failed",
		MaxFileSize:    100 * 1024 * 1024, // 100MB
		MaxAge:         7 * 24 * time.Hour,
		FlushInterval:  5 * time.Second,
		RotateInterval: 24 * time.Hour,
	}

	factory := failed.NewFactory()
	return factory.Create(failedCfg, logger, metrics)
}
