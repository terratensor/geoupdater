// cmd/geoupdater/main.go - рефакторинг с вынесением функций
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
	"github.com/terratensor/geoupdater/internal/version"
)

// AppContext содержит общие зависимости приложения
type AppContext struct {
	cfg        *config.Config
	log        ports.Logger
	metrics    ports.MetricsCollector
	manticore  *manticore.Client
	parser     *ndjson.Parser
	failedRepo *failed.FileRepository
	ctx        context.Context
	cancel     context.CancelFunc
	reportsDir string
}

func main() {
	// Парсим аргументы командной строки
	var (
		files       = flag.String("files", "", "comma-separated list of files to process")
		dir         = flag.String("dir", "", "directory to scan for files")
		pattern     = flag.String("pattern", "*.ndjson", "file pattern to match")
		mode        = flag.String("mode", "", "update mode: replace or merge (overrides config)")
		reprocess   = flag.Bool("reprocess", false, "reprocess failed records")
		versionFlag = flag.Bool("version", false, "show version information")
		reportsDir  = flag.String("reports", "reports", "directory to save processing reports")
		nerMode     = flag.Bool("ner", false, "process NER files into library_ner table")
	)
	flag.Parse()

	// Версия
	if *versionFlag {
		fmt.Println(version.Info())
		return
	}

	// Загружаем конфигурацию
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Создаем логгер
	log := createLogger(cfg)
	defer log.Sync()

	// Создаем контекст с отменой
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Настраиваем обработку сигналов
	setupSignalHandler(log, cancel)

	// Создаем общий контекст приложения
	app := &AppContext{
		cfg:        cfg,
		log:        log,
		metrics:    &noopMetrics{},
		ctx:        ctx,
		cancel:     cancel,
		reportsDir: *reportsDir,
	}

	// Инициализируем компоненты
	if err := app.initializeComponents(); err != nil {
		log.Error("failed to initialize components", ports.Error(err))
		os.Exit(1)
	}
	defer app.cleanup()

	// Определяем режим работы
	modeStr := "standard"
	if *nerMode {
		modeStr = "NER"
	}

	log.Info("starting GeoUpdater",
		ports.String("version", version.Short()),
		ports.String("mode", modeStr),
		ports.String("update_mode", cfg.UpdateMode),
		ports.Int("workers", cfg.Workers),
		ports.Int("batch_size", cfg.BatchSize))

	// Репроцессинг failed записей
	if *reprocess {
		runReprocessMode(app, mode)
		return
	}

	// Собираем список файлов для обработки
	filenames, err := collectFiles(app, *files, *dir, *pattern)
	if err != nil {
		log.Error("failed to collect files", ports.Error(err))
		os.Exit(1)
	}

	if len(filenames) == 0 {
		log.Info("no files to process")
		return
	}

	log.Info("processing files", ports.Any("files", filenames))

	// Создаем директорию для отчетов
	if err := os.MkdirAll(app.reportsDir, 0755); err != nil {
		log.Warn("failed to create reports directory",
			ports.String("dir", app.reportsDir),
			ports.Error(err))
	}

	// Запускаем соответствующий режим
	if *nerMode {
		runNERMode(app, filenames, mode)
	} else {
		runStandardMode(app, filenames, mode)
	}
}

// createLogger создает и настраивает логгер
func createLogger(cfg *config.Config) ports.Logger {
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

	return log
}

// setupSignalHandler настраивает обработку сигналов
func setupSignalHandler(log ports.Logger, cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Info("received shutdown signal, initiating graceful shutdown...")
		time.Sleep(2 * time.Second)
		cancel()
		time.AfterFunc(5*time.Second, func() {
			log.Error("forced shutdown due to timeout")
			os.Exit(1)
		})
	}()
}

// initializeComponents инициализирует все компоненты приложения
func (app *AppContext) initializeComponents() error {
	var err error

	// Manticore клиент
	manticoreCfg := &manticore.Config{
		Host:       app.cfg.ManticoreHost,
		Port:       app.cfg.ManticorePort,
		TableName:  app.cfg.ManticoreTable,
		Timeout:    app.cfg.ManticoreTimeout,
		MaxConns:   app.cfg.ManticoreMaxConns,
		RetryCount: app.cfg.MaxRetries,
		RetryDelay: app.cfg.RetryDelay,
		BatchSize:  app.cfg.BatchSize,
	}

	app.manticore, err = manticore.NewClient(manticoreCfg, app.log, app.metrics)
	if err != nil {
		return fmt.Errorf("failed to create manticore client: %w", err)
	}

	// Парсер
	parserCfg := &ndjson.Config{
		BatchSize:   app.cfg.BatchSize,
		Workers:     app.cfg.Workers,
		Validate:    true,
		SkipErrors:  true,
		MaxLineSize: 10 * 1024 * 1024,
	}
	app.parser = ndjson.NewParser(parserCfg, app.log, app.metrics)

	// Failed репозиторий
	failedCfg := &failed.Config{
		FailedDir:      app.cfg.FailedDir,
		FilePrefix:     "failed",
		MaxFileSize:    100 * 1024 * 1024,
		MaxAge:         7 * 24 * time.Hour,
		FlushInterval:  5 * time.Second,
		RotateInterval: 24 * time.Hour,
	}
	app.failedRepo, err = failed.NewFileRepository(failedCfg, app.log, app.metrics)
	if err != nil {
		return fmt.Errorf("failed to create failed repository: %w", err)
	}

	return nil
}

// cleanup закрывает все ресурсы
func (app *AppContext) cleanup() {
	if app.failedRepo != nil {
		app.failedRepo.Close()
	}
	if app.manticore != nil {
		app.manticore.Close()
	}
}

// collectFiles собирает список файлов для обработки
func collectFiles(app *AppContext, filesFlag, dirFlag, patternFlag string) ([]string, error) {
	var filenames []string

	if filesFlag != "" {
		for _, f := range strings.Split(filesFlag, ",") {
			f = strings.TrimSpace(f)
			if f != "" {
				filenames = append(filenames, f)
			}
		}
		return filenames, nil
	}

	if dirFlag != "" {
		found, err := app.parser.FindFiles(dirFlag, patternFlag)
		if err != nil {
			return nil, fmt.Errorf("failed to find files: %w", err)
		}
		return found, nil
	}

	return nil, fmt.Errorf("no input specified: use -files or -dir")
}

// runStandardMode запускает стандартный режим обработки геоданных
func runStandardMode(app *AppContext, filenames []string, modeFlag *string) {
	// Определяем режим обновления
	updateMode := domain.UpdateMode(app.cfg.UpdateMode)
	if *modeFlag != "" {
		updateMode = domain.UpdateMode(*modeFlag)
	}

	if err := updateMode.Validate(); err != nil {
		app.log.Error("invalid update mode",
			ports.String("mode", string(updateMode)),
			ports.Error(err))
		os.Exit(1)
	}

	// Создаем сервис
	serviceCfg := &service.Config{
		UpdateMode:      updateMode,
		BatchSize:       app.cfg.BatchSize,
		Workers:         app.cfg.Workers,
		MaxRetries:      app.cfg.MaxRetries,
		RetryDelay:      app.cfg.RetryDelay,
		SaveFailed:      true,
		EnableReprocess: app.cfg.EnableReprocess,
	}

	processor := service.NewUpdateProcessor(
		app.manticore,
		app.parser,
		app.failedRepo,
		app.log,
		app.metrics,
		serviceCfg,
	)

	// Обрабатываем файлы с отчетом
	start := time.Now()
	report, err := processor.ProcessFilesWithReport(app.ctx, filenames)
	if err != nil {
		app.log.Error("processing failed", ports.Error(err))
	}

	// Сохраняем отчет
	if report != nil {
		if err := report.Save(app.reportsDir); err != nil {
			app.log.Error("failed to save report",
				ports.String("dir", app.reportsDir),
				ports.Error(err))
		} else {
			fmt.Println("\n" + report.Summary())
			app.log.Info("report saved",
				ports.String("dir", app.reportsDir),
				ports.String("file", fmt.Sprintf("report_%s.json", start.Format("20060102_150405"))))
		}
	}

	duration := time.Since(start)
	stats := processor.GetStats()

	app.log.Info("processing completed",
		ports.Int64("duration_ms", duration.Milliseconds()))
	app.log.Info("final statistics", ports.Any("stats", stats))

	if report != nil && report.Stats.TotalFailed > 0 {
		os.Exit(1)
	}
}

// runNERMode запускает режим обработки NER данных
func runNERMode(app *AppContext, filenames []string, modeFlag *string) {
	app.log.Info("running in NER mode")

	// Создаем NER репозиторий
	nerRepo := manticore.NewNERRepository(app.manticore, app.cfg.ManticoreTable, app.log)

	// Проверяем/создаем таблицу
	if err := nerRepo.EnsureTable(app.ctx); err != nil {
		app.log.Error("failed to ensure NER table", ports.Error(err))
		os.Exit(1)
	}

	// Создаем NER процессор
	nerConfig := &service.NERConfig{
		BatchSize:  app.cfg.NERBatchSize,
		Workers:    app.cfg.NERWorkers,
		MaxRetries: app.cfg.MaxRetries,
		SaveFailed: true,
	}

	nerProcessor := service.NewNERProcessor(
		nerRepo,
		app.parser,
		app.failedRepo,
		app.log,
		app.metrics,
		nerConfig,
	)

	// Обрабатываем файлы
	start := time.Now()
	result, err := nerProcessor.ProcessFiles(app.ctx, filenames)
	if err != nil {
		app.log.Error("NER processing failed", ports.Error(err))
	}

	duration := time.Since(start)

	app.log.Info("NER processing completed",
		ports.String("summary", result.Summary()),
		ports.Int64("duration_ms", duration.Milliseconds()))

	stats := nerProcessor.GetStats()
	app.log.Info("NER final statistics", ports.Any("stats", stats))

	if result.Failed > 0 {
		os.Exit(1)
	}
}

// runReprocessMode запускает режим повторной обработки failed записей
func runReprocessMode(app *AppContext, modeFlag *string) {
	app.log.Info("starting reprocessing of failed records")

	updateMode := domain.UpdateMode(app.cfg.UpdateMode)
	if *modeFlag != "" {
		updateMode = domain.UpdateMode(*modeFlag)
	}

	serviceCfg := &service.Config{
		UpdateMode:      updateMode,
		BatchSize:       app.cfg.BatchSize,
		Workers:         app.cfg.Workers,
		MaxRetries:      app.cfg.MaxRetries,
		RetryDelay:      app.cfg.RetryDelay,
		SaveFailed:      true,
		EnableReprocess: app.cfg.EnableReprocess,
	}

	processor := service.NewUpdateProcessor(
		app.manticore,
		app.parser,
		app.failedRepo,
		app.log,
		app.metrics,
		serviceCfg,
	)

	result, err := processor.ReprocessFailed(app.ctx)
	if err != nil {
		app.log.Error("reprocessing failed", ports.Error(err))
	} else {
		app.log.Info("reprocessing completed",
			ports.String("summary", result.Summary()))
	}
}

// noopMetrics заглушка для метрик
type noopMetrics struct{}

func (m *noopMetrics) RecordDocumentProcessed(duration time.Duration, success bool)                 {}
func (m *noopMetrics) RecordBatchProcessed(size int, duration time.Duration, errors int)            {}
func (m *noopMetrics) RecordFileProcessed(filename string, size int, duration time.Duration)        {}
func (m *noopMetrics) RecordManticoreOperation(operation string, duration time.Duration, err error) {}
func (m *noopMetrics) GetStats(ctx context.Context) (map[string]interface{}, error)                 { return nil, nil }
