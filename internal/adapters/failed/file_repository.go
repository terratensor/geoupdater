// internal/adapters/failed/file_repository.go - исправленная версия
package failed

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/terratensor/geoupdater/internal/core/domain"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

// FileRepository реализует ports.FailedRecordsRepository для хранения в файлах
type FileRepository struct {
	config      *Config
	logger      ports.Logger
	metrics     ports.MetricsCollector
	mu          sync.RWMutex
	currentFile *os.File
	writer      *bufio.Writer
}

// Config конфигурация файлового репозитория
type Config struct {
	FailedDir      string        // Директория для хранения failed записей
	FilePrefix     string        // Префикс имени файла
	MaxFileSize    int64         // Максимальный размер файла (в байтах)
	MaxAge         time.Duration // Максимальный возраст записей
	FlushInterval  time.Duration // Интервал сброса буфера
	RotateInterval time.Duration // Интервал ротации файлов
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() *Config {
	return &Config{
		FailedDir:      "./failed",
		FilePrefix:     "failed",
		MaxFileSize:    100 * 1024 * 1024,  // 100MB
		MaxAge:         7 * 24 * time.Hour, // 7 дней
		FlushInterval:  5 * time.Second,
		RotateInterval: 24 * time.Hour, // Каждый день
	}
}

// NewFileRepository создает новый экземпляр FileRepository
func NewFileRepository(cfg *Config, logger ports.Logger, metrics ports.MetricsCollector) (*FileRepository, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	// Создаем директорию если не существует
	if err := os.MkdirAll(cfg.FailedDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create failed dir: %w", err)
	}

	repo := &FileRepository{
		config:  cfg,
		logger:  logger,
		metrics: metrics,
	}

	// Открываем текущий файл для записи
	if err := repo.rotateFile(); err != nil {
		return nil, fmt.Errorf("failed to open initial file: %w", err)
	}

	// Запускаем фоновые задачи
	go repo.flushPeriodically()
	go repo.rotatePeriodically()

	logger.Info("failed records repository initialized",
		ports.String("failed_dir", cfg.FailedDir),
		ports.String("file_prefix", cfg.FilePrefix),
		ports.Int64("max_file_size", cfg.MaxFileSize))

	return repo, nil
}

// Save сохраняет неудачную запись
func (r *FileRepository) Save(ctx context.Context, record *domain.FailedRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.writeRecord(record)
}

// SaveBatch сохраняет пачку неудачных записей
func (r *FileRepository) SaveBatch(ctx context.Context, records []*domain.FailedRecord) error {
	if len(records) == 0 {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, record := range records {
		if err := r.writeRecord(record); err != nil {
			return fmt.Errorf("failed to write record %d: %w", record.Data.DocID, err)
		}
	}

	return nil
}

// writeRecord записывает одну запись в текущий файл
func (r *FileRepository) writeRecord(record *domain.FailedRecord) error {
	// Проверяем размер файла и ротируем если нужно
	if r.currentFile != nil {
		stat, err := r.currentFile.Stat()
		if err == nil && stat.Size() > r.config.MaxFileSize {
			if err := r.rotateFile(); err != nil {
				return fmt.Errorf("failed to rotate file: %w", err)
			}
		}
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	if _, err := r.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	if _, err := r.writer.WriteString("\n"); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}

	return nil
}

// LoadAll загружает все неудачные записи для повторной обработки
func (r *FileRepository) LoadAll(ctx context.Context) ([]*domain.FailedRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Сбрасываем буфер перед чтением
	if err := r.writer.Flush(); err != nil {
		r.logger.Warn("failed to flush writer before loading", ports.Error(err))
	}

	files, err := r.findFailedFiles()
	if err != nil {
		return nil, fmt.Errorf("failed to find failed files: %w", err)
	}

	var allRecords []*domain.FailedRecord
	cutoffTime := time.Now().Add(-r.config.MaxAge).Unix()

	for _, file := range files {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			records, err := r.loadFile(file, cutoffTime)
			if err != nil {
				r.logger.Warn("failed to load file",
					ports.String("file", file),
					ports.Error(err))
				continue
			}
			allRecords = append(allRecords, records...)
		}
	}

	r.logger.Info("loaded failed records",
		ports.Int("count", len(allRecords)),
		ports.Int("files", len(files)))

	return allRecords, nil
}

// LoadByAge загружает записи не старше указанного возраста
func (r *FileRepository) LoadByAge(ctx context.Context, maxAgeHours int) ([]*domain.FailedRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if err := r.writer.Flush(); err != nil {
		r.logger.Warn("failed to flush writer before loading", ports.Error(err))
	}

	files, err := r.findFailedFiles()
	if err != nil {
		return nil, fmt.Errorf("failed to find failed files: %w", err)
	}

	var allRecords []*domain.FailedRecord
	cutoffTime := time.Now().Add(-time.Duration(maxAgeHours) * time.Hour).Unix()

	for _, file := range files {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			records, err := r.loadFile(file, cutoffTime)
			if err != nil {
				r.logger.Warn("failed to load file",
					ports.String("file", file),
					ports.Error(err))
				continue
			}
			allRecords = append(allRecords, records...)
		}
	}

	return allRecords, nil
}

// loadFile загружает записи из одного файла
func (r *FileRepository) loadFile(filename string, cutoffTime int64) ([]*domain.FailedRecord, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var records []*domain.FailedRecord
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var record domain.FailedRecord
		if err := json.Unmarshal(line, &record); err != nil {
			r.logger.Warn("failed to unmarshal record",
				ports.String("file", filename),
				ports.Error(err))
			continue
		}

		// Фильтруем по возрасту
		if record.Timestamp >= cutoffTime {
			records = append(records, &record)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

// Delete удаляет запись после успешной обработки (заглушка)
func (r *FileRepository) Delete(ctx context.Context, key string) error {
	// В файловой реализации удаление отдельных записей сложно,
	// используем Cleanup для массовой очистки
	r.logger.Debug("delete called - operation not supported individually", ports.String("key", key))
	return nil
}

// DeleteBatch удаляет пачку записей (заглушка)
func (r *FileRepository) DeleteBatch(ctx context.Context, keys []string) error {
	r.logger.Debug("delete batch called", ports.Int("count", len(keys)))
	return nil
}

// Cleanup очищает обработанные записи
func (r *FileRepository) Cleanup(ctx context.Context, processedIDs []uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Сбрасываем буфер
	if err := r.writer.Flush(); err != nil {
		r.logger.Warn("failed to flush writer before cleanup", ports.Error(err))
	}

	files, err := r.findFailedFiles()
	if err != nil {
		return fmt.Errorf("failed to find failed files: %w", err)
	}

	// Создаем временную директорию для новых файлов
	tempDir := filepath.Join(r.config.FailedDir, "temp_"+time.Now().Format("20060102150405"))
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Мапа для быстрого поиска обработанных ID
	processedMap := make(map[uint64]bool)
	for _, id := range processedIDs {
		processedMap[id] = true
	}

	// Обрабатываем каждый файл
	for _, file := range files {
		if err := r.cleanupFile(file, tempDir, processedMap); err != nil {
			r.logger.Warn("failed to cleanup file",
				ports.String("file", file),
				ports.Error(err))
		}
	}

	// Перемещаем новые файлы на место старых
	return r.replaceWithCleanedFiles(tempDir)
}

// cleanupFile очищает один файл от обработанных записей
func (r *FileRepository) cleanupFile(filename, tempDir string, processedMap map[uint64]bool) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Создаем новый файл во временной директории
	baseName := filepath.Base(filename)
	newFile, err := os.Create(filepath.Join(tempDir, baseName))
	if err != nil {
		return err
	}
	defer newFile.Close()

	writer := bufio.NewWriter(newFile)
	scanner := bufio.NewScanner(file)
	var keptCount, removedCount int

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var record domain.FailedRecord
		if err := json.Unmarshal(line, &record); err != nil {
			// Если не можем распарсить, сохраняем как есть
			if _, err := writer.Write(line); err != nil {
				return err
			}
			if _, err := writer.WriteString("\n"); err != nil {
				return err
			}
			continue
		}

		// Проверяем, был ли этот ID обработан
		if processedMap[record.Data.DocID] {
			removedCount++
			continue
		}

		// Сохраняем необработанную запись
		if _, err := writer.Write(line); err != nil {
			return err
		}
		if _, err := writer.WriteString("\n"); err != nil {
			return err
		}
		keptCount++
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	r.logger.Debug("cleaned up file",
		ports.String("file", filename),
		ports.Int("kept", keptCount),
		ports.Int("removed", removedCount))

	return nil
}

// replaceWithCleanedFiles заменяет старые файлы очищенными
func (r *FileRepository) replaceWithCleanedFiles(tempDir string) error {
	files, err := filepath.Glob(filepath.Join(tempDir, "*.ndjson"))
	if err != nil {
		return err
	}

	for _, tempFile := range files {
		baseName := filepath.Base(tempFile)
		targetFile := filepath.Join(r.config.FailedDir, baseName)

		// Если оригинальный файл существует, удаляем его
		if _, err := os.Stat(targetFile); err == nil {
			if err := os.Remove(targetFile); err != nil {
				return fmt.Errorf("failed to remove original file: %w", err)
			}
		}

		// Перемещаем новый файл
		if err := os.Rename(tempFile, targetFile); err != nil {
			return fmt.Errorf("failed to rename temp file: %w", err)
		}
	}

	return nil
}

// Count возвращает количество неудачных записей
func (r *FileRepository) Count(ctx context.Context) (int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if err := r.writer.Flush(); err != nil {
		r.logger.Warn("failed to flush writer before counting", ports.Error(err))
	}

	files, err := r.findFailedFiles()
	if err != nil {
		return 0, err
	}

	var total int
	for _, file := range files {
		count, err := r.countLines(file)
		if err != nil {
			r.logger.Warn("failed to count lines in file",
				ports.String("file", file),
				ports.Error(err))
			continue
		}
		total += count
	}

	return total, nil
}

// countLines считает количество строк в файле
func (r *FileRepository) countLines(filename string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		if len(scanner.Bytes()) > 0 {
			count++
		}
	}

	return count, scanner.Err()
}

// findFailedFiles находит все файлы с failed записями
func (r *FileRepository) findFailedFiles() ([]string, error) {
	pattern := filepath.Join(r.config.FailedDir, r.config.FilePrefix+"_*.ndjson")
	return filepath.Glob(pattern)
}

// rotateFile создает новый файл для записи
func (r *FileRepository) rotateFile() error {
	// Закрываем текущий файл если есть
	if r.currentFile != nil {
		if err := r.writer.Flush(); err != nil {
			r.logger.Warn("failed to flush writer during rotation", ports.Error(err))
		}
		if err := r.currentFile.Close(); err != nil {
			r.logger.Warn("failed to close current file", ports.Error(err))
		}
	}

	// Создаем новый файл с временной меткой
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join(r.config.FailedDir,
		fmt.Sprintf("%s_%s.ndjson", r.config.FilePrefix, timestamp))

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create failed file: %w", err)
	}

	r.currentFile = file
	r.writer = bufio.NewWriterSize(file, 64*1024) // 64KB буфер

	r.logger.Debug("rotated failed file",
		ports.String("filename", filename))

	return nil
}

// flushPeriodically периодически сбрасывает буфер на диск
func (r *FileRepository) flushPeriodically() {
	ticker := time.NewTicker(r.config.FlushInterval)
	defer ticker.Stop()

	for range ticker.C {
		r.mu.Lock()
		if r.writer != nil {
			if err := r.writer.Flush(); err != nil {
				r.logger.Warn("failed to flush writer", ports.Error(err))
			}
		}
		r.mu.Unlock()
	}
}

// rotatePeriodically периодически создает новый файл
func (r *FileRepository) rotatePeriodically() {
	ticker := time.NewTicker(r.config.RotateInterval)
	defer ticker.Stop()

	for range ticker.C {
		r.mu.Lock()
		if err := r.rotateFile(); err != nil {
			r.logger.Error("failed to rotate file", ports.Error(err))
		}
		r.mu.Unlock()
	}
}

// Close закрывает репозиторий
func (r *FileRepository) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.writer != nil {
		if err := r.writer.Flush(); err != nil {
			r.logger.Warn("failed to flush writer during close", ports.Error(err))
		}
	}

	if r.currentFile != nil {
		if err := r.currentFile.Close(); err != nil {
			return fmt.Errorf("failed to close failed file: %w", err)
		}
	}

	r.logger.Info("failed records repository closed")
	return nil
}
