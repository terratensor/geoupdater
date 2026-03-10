// internal/core/ports/logger.go
package ports

import (
	"context"
	"time"
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

// IntField целочисленное поле (int)
type IntField struct {
	key   string
	value int
}

func (f IntField) Key() string           { return f.key }
func (f IntField) Value() interface{}    { return f.value }
func Int(key string, value int) IntField { return IntField{key, value} }

// Int64Field поле для int64
type Int64Field struct {
	key   string
	value int64
}

func (f Int64Field) Key() string               { return f.key }
func (f Int64Field) Value() interface{}        { return f.value }
func Int64(key string, value int64) Int64Field { return Int64Field{key, value} }

// Float64Field поле для float64
type Float64Field struct {
	key   string
	value float64
}

func (f Float64Field) Key() string                   { return f.key }
func (f Float64Field) Value() interface{}            { return f.value }
func Float64(key string, value float64) Float64Field { return Float64Field{key, value} }

// BoolField поле для bool
type BoolField struct {
	key   string
	value bool
}

func (f BoolField) Key() string             { return f.key }
func (f BoolField) Value() interface{}      { return f.value }
func Bool(key string, value bool) BoolField { return BoolField{key, value} }

// ErrorField поле для ошибки
type ErrorField struct {
	key   string
	value error
}

func (f ErrorField) Key() string                    { return f.key }
func (f ErrorField) Value() interface{}             { return f.value }
func Error(err error) ErrorField                    { return ErrorField{"error", err} }
func ErrorWithKey(key string, err error) ErrorField { return ErrorField{key, err} }

// DurationField поле для временного интервала
type DurationField struct {
	key   string
	value time.Duration
}

func (f DurationField) Key() string                          { return f.key }
func (f DurationField) Value() interface{}                   { return f.value }
func Duration(key string, value time.Duration) DurationField { return DurationField{key, value} }

// TimeField поле для временной метки
type TimeField struct {
	key   string
	value time.Time
}

func (f TimeField) Key() string                  { return f.key }
func (f TimeField) Value() interface{}           { return f.value }
func Time(key string, value time.Time) TimeField { return TimeField{key, value} }

// AnyField любое поле
type AnyField struct {
	key   string
	value interface{}
}

func (f AnyField) Key() string                   { return f.key }
func (f AnyField) Value() interface{}            { return f.value }
func Any(key string, value interface{}) AnyField { return AnyField{key, value} }
