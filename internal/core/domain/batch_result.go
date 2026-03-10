// internal/core/domain/batch_result.go
package domain

import (
	"fmt"
	"strings"
	"time"
)

// BatchResult представляет результат обработки батча документов
type BatchResult struct {
	Total     int           `json:"total"`
	Success   int           `json:"success"`
	Failed    int           `json:"failed"`
	Skipped   int           `json:"skipped"`
	Errors    []BatchError  `json:"errors,omitempty"`
	Duration  time.Duration `json:"duration"`
	Timestamp time.Time     `json:"timestamp"`
}

// BatchError представляет ошибку при обработке конкретного документа
type BatchError struct {
	DocID     string `json:"doc_id"`
	Error     string `json:"error"`
	ErrorType string `json:"error_type"`
	Attempts  int    `json:"attempts"`
}

// NewBatchResult создает новый BatchResult
func NewBatchResult() *BatchResult {
	return &BatchResult{
		Errors:    make([]BatchError, 0),
		Timestamp: time.Now(),
	}
}

// AddSuccess добавляет успешную операцию
func (r *BatchResult) AddSuccess() {
	r.Success++
	r.Total++
}

// AddFailed добавляет неудачную операцию
func (r *BatchResult) AddFailed(docID string, err error, errorType ErrorType, attempts int) {
	r.Failed++
	r.Total++

	r.Errors = append(r.Errors, BatchError{
		DocID:     docID,
		Error:     err.Error(),
		ErrorType: string(errorType),
		Attempts:  attempts,
	})
}

// AddSkipped добавляет пропущенную операцию (документ не найден)
func (r *BatchResult) AddSkipped(docID string) {
	r.Skipped++
	r.Total++
}

// Merge объединяет результаты двух батчей
func (r *BatchResult) Merge(other *BatchResult) {
	r.Total += other.Total
	r.Success += other.Success
	r.Failed += other.Failed
	r.Skipped += other.Skipped
	r.Errors = append(r.Errors, other.Errors...)
	r.Duration += other.Duration
}

// Summary возвращает краткую сводку
func (r *BatchResult) Summary() string {
	return fmt.Sprintf("total=%d, success=%d, failed=%d, skipped=%d, errors=%d, duration=%v",
		r.Total, r.Success, r.Failed, r.Skipped, len(r.Errors), r.Duration)
}

// HasErrors проверяет, есть ли ошибки
func (r *BatchResult) HasErrors() bool {
	return len(r.Errors) > 0
}

// Error возвращает строку с ошибками
func (r *BatchResult) Error() string {
	if !r.HasErrors() {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d errors occurred:\n", len(r.Errors)))
	for i, err := range r.Errors {
		if i >= 10 { // Показываем только первые 10 ошибок
			sb.WriteString(fmt.Sprintf("... and %d more", len(r.Errors)-10))
			break
		}
		sb.WriteString(fmt.Sprintf("  - %s: %s (attempts=%d)\n",
			err.DocID, err.Error, err.Attempts))
	}
	return sb.String()
}
