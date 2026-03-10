// internal/core/domain/errors.go
package domain

import "fmt"

// ErrorType определяет тип ошибки
type ErrorType string

const (
	ErrorTypeNotFound   ErrorType = "NOT_FOUND"
	ErrorTypeValidation ErrorType = "VALIDATION"
	ErrorTypeManticore  ErrorType = "MANTICORE"
	ErrorTypeParsing    ErrorType = "PARSING"
	ErrorTypeTimeout    ErrorType = "TIMEOUT"
	ErrorTypeConnection ErrorType = "CONNECTION"
	ErrorTypeUnknown    ErrorType = "UNKNOWN"
)

// DomainError представляет доменную ошибку
type DomainError struct {
	Type    ErrorType
	Message string
	Err     error
	DocID   string
	Details map[string]interface{}
}

func (e *DomainError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v (doc_id: %s)", e.Type, e.Message, e.Err, e.DocID)
	}
	return fmt.Sprintf("[%s] %s (doc_id: %s)", e.Type, e.Message, e.DocID)
}

// Unwrap возвращает вложенную ошибку
func (e *DomainError) Unwrap() error {
	return e.Err
}

// WithDetails добавляет детали к ошибке
func (e *DomainError) WithDetails(details map[string]interface{}) *DomainError {
	e.Details = details
	return e
}

// NewNotFoundError создает ошибку "документ не найден"
func NewNotFoundError(docID string, err error) *DomainError {
	return &DomainError{
		Type:    ErrorTypeNotFound,
		Message: "document not found",
		Err:     err,
		DocID:   docID,
	}
}

// NewValidationError создает ошибку валидации
func NewValidationError(message string, docID string) *DomainError {
	return &DomainError{
		Type:    ErrorTypeValidation,
		Message: message,
		DocID:   docID,
	}
}

// NewManticoreError создает ошибку Manticore
func NewManticoreError(docID string, err error) *DomainError {
	return &DomainError{
		Type:    ErrorTypeManticore,
		Message: "manticore operation failed",
		Err:     err,
		DocID:   docID,
	}
}

// NewParsingError создает ошибку парсинга
func NewParsingError(message string, err error) *DomainError {
	return &DomainError{
		Type:    ErrorTypeParsing,
		Message: message,
		Err:     err,
	}
}

// NewTimeoutError создает ошибку таймаута
func NewTimeoutError(docID string, err error) *DomainError {
	return &DomainError{
		Type:    ErrorTypeTimeout,
		Message: "operation timeout",
		Err:     err,
		DocID:   docID,
	}
}

// IsNotFound проверяет, является ли ошибка "не найдено"
func IsNotFound(err error) bool {
	if e, ok := err.(*DomainError); ok {
		return e.Type == ErrorTypeNotFound
	}
	return false
}

// IsRetryable проверяет, можно ли повторить операцию при этой ошибке
func IsRetryable(err error) bool {
	if e, ok := err.(*DomainError); ok {
		switch e.Type {
		case ErrorTypeTimeout, ErrorTypeConnection, ErrorTypeManticore:
			return true
		}
	}
	return false
}
