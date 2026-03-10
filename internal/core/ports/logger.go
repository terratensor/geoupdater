// internal/core/ports/logger.go
package ports

import (
	"context"
)

// Logger определяет интерфейс для логирования
type Logger interface {
	// Debug логирует отладочное сообщение
	Debug(msg string, fields ...Field)

	// Info логирует информационное сообщение
	Info(msg string, fields ...Field)

	// Warn логирует предупреждение
	Warn(msg string, fields ...Field)

	// Error логирует ошибку
	Error(msg string, fields ...Field)

	// Fatal логирует фатальную ошибку и завершает программу
	Fatal(msg string, fields ...Field)

	// With создает дочерний логгер с дополнительными полями
	With(fields ...Field) Logger

	// WithContext создает логгер с контекстом
	WithContext(ctx context.Context) Logger

	// Sync сбрасывает буферы логов
	Sync() error
}

// Field представляет поле лога
type Field interface {
	Key() string
	Value() interface{}
}

// StringField строковое поле
type StringField struct {
	key   string
	value string
}

func (f StringField) Key() string          { return f.key }
func (f StringField) Value() interface{}   { return f.value }
func String(key, value string) StringField { return StringField{key, value} }

// IntField целочисленное поле
type IntField struct {
	key   string
	value int
}

func (f IntField) Key() string           { return f.key }
func (f IntField) Value() interface{}    { return f.value }
func Int(key string, value int) IntField { return IntField{key, value} }

// ErrorField поле для ошибки
type ErrorField struct {
	key   string
	value error
}

func (f ErrorField) Key() string        { return f.key }
func (f ErrorField) Value() interface{} { return f.value }
func Error(err error) ErrorField        { return ErrorField{"error", err} }

// AnyField любое поле
type AnyField struct {
	key   string
	value interface{}
}

func (f AnyField) Key() string                   { return f.key }
func (f AnyField) Value() interface{}            { return f.value }
func Any(key string, value interface{}) AnyField { return AnyField{key, value} }
