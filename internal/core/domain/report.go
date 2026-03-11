// internal/core/domain/report.go
package domain

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ReportStats статистика обработки для отчета
type ReportStats struct {
	TotalProcessed int64 `json:"total_processed"`
	TotalSuccess   int64 `json:"total_success"`
	TotalFailed    int64 `json:"total_failed"`
	TotalSkipped   int64 `json:"total_skipped"`
	TotalFiles     int   `json:"total_files"`
}

// ProcessingReport детальный отчет о выполнении
type ProcessingReport struct {
	// Общая информация
	Version   string    `json:"version"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Duration  string    `json:"duration"`

	// Параметры запуска
	Mode      UpdateMode `json:"mode"`
	Workers   int        `json:"workers"`
	BatchSize int        `json:"batch_size"`

	// Файлы
	Files      []FileReport `json:"files"`
	TotalFiles int          `json:"total_files"`

	// Статистика
	Stats ReportStats `json:"stats"`

	// Диапазоны ID
	MinID   uint64 `json:"min_id,omitempty"`
	MaxID   uint64 `json:"max_id,omitempty"`
	FirstID uint64 `json:"first_id,omitempty"`
	LastID  uint64 `json:"last_id,omitempty"`
}

// FileReport отчет по отдельному файлу
type FileReport struct {
	Filename  string    `json:"filename"`
	Size      int64     `json:"size_bytes"`
	Lines     int       `json:"lines"`
	Valid     int       `json:"valid"`
	Errors    int       `json:"errors"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Duration  string    `json:"duration"`
	Success   int       `json:"success"`
	Failed    int       `json:"failed"`
	Skipped   int       `json:"skipped"`
	FirstID   uint64    `json:"first_id,omitempty"`
	LastID    uint64    `json:"last_id,omitempty"`
}

// Save сохраняет отчет в файл
func (r *ProcessingReport) Save(dir string) error {
	timestamp := r.StartTime.Format("20060102_150405")
	filename := filepath.Join(dir, fmt.Sprintf("report_%s.json", timestamp))

	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create report dir: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write report: %w", err)
	}

	return nil
}

// Summary возвращает краткую сводку
func (r *ProcessingReport) Summary() string {
	return fmt.Sprintf(`=== GeoUpdater Report ===
Version:     %s
Mode:        %s
Duration:    %s
Files:       %d
Processed:   %d
Success:     %d
Failed:      %d
Skipped:     %d
First ID:    %d
Last ID:     %d
Report saved to: reports/...`,
		r.Version, r.Mode, r.Duration, r.TotalFiles,
		r.Stats.TotalProcessed, r.Stats.TotalSuccess,
		r.Stats.TotalFailed, r.Stats.TotalSkipped,
		r.FirstID, r.LastID)
}

// NewProcessingReport создает новый отчет
func NewProcessingReport(version string, mode UpdateMode, workers, batchSize int) *ProcessingReport {
	return &ProcessingReport{
		Version:   version,
		StartTime: time.Now(),
		Mode:      mode,
		Workers:   workers,
		BatchSize: batchSize,
		Files:     make([]FileReport, 0),
	}
}

// AddFile добавляет отчет по файлу
func (r *ProcessingReport) AddFile(file FileReport) {
	r.Files = append(r.Files, file)
	r.TotalFiles++

	// Обновляем общую статистику - используем правильные счетчики
	r.Stats.TotalProcessed += int64(file.Valid) // Все валидные записи
	r.Stats.TotalSuccess += int64(file.Success) // Успешно обновленные
	r.Stats.TotalFailed += int64(file.Failed)   // С ошибками
	r.Stats.TotalSkipped += int64(file.Skipped) // Пропущенные

	// Обновляем диапазоны ID
	if file.FirstID > 0 {
		if r.FirstID == 0 || file.FirstID < r.FirstID {
			r.FirstID = file.FirstID
		}
	}
	if file.LastID > 0 {
		if r.LastID == 0 || file.LastID > r.LastID {
			r.LastID = file.LastID
		}
	}
}

// Complete завершает отчет
func (r *ProcessingReport) Complete() {
	r.EndTime = time.Now()
	r.Duration = r.EndTime.Sub(r.StartTime).String()

	// Вычисляем Min/Max ID если нужно
	if r.FirstID > 0 {
		r.MinID = r.FirstID
	}
	if r.LastID > 0 {
		r.MaxID = r.LastID
	}
}
