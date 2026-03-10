// internal/core/ports/errors.go
package ports

import "errors"

var (
	// ErrNotFound возвращается когда документ не найден
	ErrNotFound = errors.New("document not found")

	// ErrConnection возвращается при проблемах с соединением
	ErrConnection = errors.New("connection error")

	// ErrTimeout возвращается при таймауте операции
	ErrTimeout = errors.New("operation timeout")

	// ErrInvalidData возвращается при невалидных данных
	ErrInvalidData = errors.New("invalid data")

	// ErrBatchTooLarge возвращается когда размер батча превышен
	ErrBatchTooLarge = errors.New("batch too large")

	// ErrManticore возвращается при ошибке Manticore
	ErrManticore = errors.New("manticore error")
)
