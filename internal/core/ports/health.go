// internal/core/ports/health.go
package ports

import (
	"context"
)

// HealthChecker определяет интерфейс для проверки здоровья сервиса
type HealthChecker interface {
	// Health возвращает статус здоровья компонента
	Health(ctx context.Context) (*HealthStatus, error)
}

// HealthStatus представляет статус здоровья
type HealthStatus struct {
	Status    string                 `json:"status"` // "up", "down", "degraded"
	Component string                 `json:"component"`
	Message   string                 `json:"message,omitempty"`
	Details   map[string]interface{} `json:"details,omitempty"`
	Error     string                 `json:"error,omitempty"`
}
