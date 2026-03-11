// cmd/geoupdater/main.go - финальная чистая версия
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/terratensor/geoupdater/internal/adapters/failed"
	"github.com/terratensor/geoupdater/internal/adapters/logger"
	"github.com/terratensor/geoupdater/internal/adapters/manticore"
	"github.com/terratensor/geoupdater/internal/adapters/ndjson"
	"github.com/terratensor/geoupdater/internal/app/config"
	"github.com/terratensor/geoupdater/internal/app/service"
	"github.com/terratensor/geoupdater/internal/core/domain"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

func main() {
	// Парсим аргументы командной строки
	var (
		files       = flag.String("files", "", "comma-separated list of files to process")
		dir         = flag.String("dir", "", "directory to scan for files")
		pattern     = flag.String("pattern", "*.ndjson", "file pattern to match")
		mode        = flag.String("mode", "", "update mode: replace or merge (overrides config)")
		reprocess   = flag.Bool("reprocess", false, "reprocess failed records")
		showStats   = flag.Bool("stats", false, "show processing stats")
		versionFlag = flag.Bool("version", false, "show version")
	)
	flag.Parse()

	// Версия
	if *versionFlag {
		fmt.Println("GeoUpdater v1.0.0")
		return
	}

	// Загружаем конфигурацию из переменных окружения
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Создаем логгер
	logCfg := &logger.Config{
		Level:      cfg.LogLevel,
		OutputPath: cfg.LogFile,
		FileOutput: cfg.LogFile != "",
		Console:    true,
		AddSource:  true,
	}
	log, err := logger.NewZapLogger(logCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync()

	log.Info("starting GeoUpdater",
		ports.String("version", "1.0.0"),
		ports.String("mode", cfg.UpdateMode),
		ports.Int("workers", cfg.Workers),
		ports.Int("batch_size", cfg.BatchSize))

	// Создаем метрики (пока заглушка)
	metrics := &noopMetrics{}

	// Создаем Manticore клиент
	manticoreCfg := &manticore.Config{
		Host:       cfg.ManticoreHost,
		Port:       cfg.ManticorePort,
		TableName:  cfg.ManticoreTable,
		Timeout:    cfg.ManticoreTimeout,
		MaxConns:   cfg.ManticoreMaxConns,
		RetryCount: cfg.MaxRetries,
		RetryDelay: cfg.RetryDelay,
		BatchSize:  cfg.BatchSize,
	}

	manticoreClient, err := manticore.NewClient(manticoreCfg, log, metrics)
	if err != nil {
		log.Error("failed to create manticore client", ports.Error(err))
		os.Exit(1)
	}
	defer manticoreClient.Close()

	// Создаем парсер
	parserCfg := &ndjson.Config{
		BatchSize:   cfg.BatchSize,
		Workers:     cfg.Workers,
		Validate:    true,
		SkipErrors:  true,
		MaxLineSize: 10 * 1024 * 1024,
	}
	parser := ndjson.NewParser(parserCfg, log, metrics)

	// Создаем failed репозиторий
	failedCfg := &failed.Config{
		FailedDir:      cfg.FailedDir,
		FilePrefix:     "failed",
		MaxFileSize:    100 * 1024 * 1024,
		MaxAge:         7 * 24 * time.Hour,
		FlushInterval:  5 * time.Second,
		RotateInterval: 24 * time.Hour,
	}
	failedRepo, err := failed.NewFileRepository(failedCfg, log, metrics)
	if err != nil {
		log.Error("failed to create failed repository", ports.Error(err))
		os.Exit(1)
	}
	defer failedRepo.Close()

	// Определяем режим обновления
	updateMode := domain.UpdateMode(cfg.UpdateMode)
	if *mode != "" {
		updateMode = domain.UpdateMode(*mode)
	}

	// Валидация режима через метод домена
	if err := updateMode.Validate(); err != nil {
		log.Error("invalid update mode",
			ports.String("mode", string(updateMode)),
			ports.Error(err))
		os.Exit(1)
	}

	// Создаем сервис
	serviceCfg := &service.Config{
		UpdateMode:      updateMode,
		BatchSize:       cfg.BatchSize,
		Workers:         cfg.Workers,
		MaxRetries:      cfg.MaxRetries,
		RetryDelay:      cfg.RetryDelay,
		SaveFailed:      true,
		EnableReprocess: cfg.EnableReprocess,
	}

	processor := service.NewUpdateProcessor(
		manticoreClient,
		parser,
		failedRepo,
		log,
		metrics,
		serviceCfg,
	)

	// Создаем контекст с отменой для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обрабатываем сигналы
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Info("received shutdown signal, initiating graceful shutdown...")

		// Даем время на завершение текущих операций
		time.Sleep(2 * time.Second)
		cancel()

		// Если через 5 секунд все еще работаем, выходим принудительно
		time.AfterFunc(5*time.Second, func() {
			log.Error("forced shutdown due to timeout")
			os.Exit(1)
		})
	}()

	// Показываем статистику если запрошено
	if *showStats {
		stats := processor.GetStats()
		log.Info("current statistics", ports.Any("stats", stats))
		return
	}

	// Репроцессинг failed записей
	if *reprocess {
		log.Info("starting reprocessing of failed records")
		result, err := processor.ReprocessFailed(ctx)
		if err != nil {
			log.Error("reprocessing failed", ports.Error(err))
		} else {
			log.Info("reprocessing completed",
				ports.String("summary", result.Summary()))
		}
		return
	}

	// Обработка файлов
	var filenames []string

	if *files != "" {
		// Разделяем по запятой
		for _, f := range strings.Split(*files, ",") {
			f = strings.TrimSpace(f)
			if f != "" {
				filenames = append(filenames, f)
			}
		}
	} else if *dir != "" {
		// Поиск файлов в директории
		found, err := parser.FindFiles(*dir, *pattern)
		if err != nil {
			log.Error("failed to find files", ports.Error(err))
			os.Exit(1)
		}
		filenames = found
		log.Info("found files", ports.Int("count", len(found)))
	} else {
		log.Error("no input specified: use -files, -dir, or -reprocess")
		os.Exit(1)
	}

	if len(filenames) == 0 {
		log.Info("no files to process")
		return
	}

	// Выводим список файлов
	log.Info("processing files", ports.Any("files", filenames))

	// Обрабатываем файлы
	start := time.Now()
	result, err := processor.ProcessFiles(ctx, filenames)
	if err != nil {
		log.Error("processing failed", ports.Error(err))
	}

	duration := time.Since(start)

	// Выводим итоговую статистику
	log.Info("processing completed",
		ports.String("summary", result.Summary()),
		ports.Int64("duration_ms", duration.Milliseconds()))

	stats := processor.GetStats()
	log.Info("final statistics",
		ports.Any("stats", stats))

	// Если были ошибки, выходим с ненулевым кодом
	if result.Failed > 0 {
		os.Exit(1)
	}
}

// noopMetrics заглушка для метрик
type noopMetrics struct{}

func (m *noopMetrics) RecordDocumentProcessed(duration time.Duration, success bool)                 {}
func (m *noopMetrics) RecordBatchProcessed(size int, duration time.Duration, errors int)            {}
func (m *noopMetrics) RecordFileProcessed(filename string, size int, duration time.Duration)        {}
func (m *noopMetrics) RecordManticoreOperation(operation string, duration time.Duration, err error) {}
func (m *noopMetrics) GetStats(ctx context.Context) (map[string]interface{}, error)                 { return nil, nil }
