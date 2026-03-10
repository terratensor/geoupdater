// internal/core/ports/failed_records.go
package ports

import (
	"context"

	"github.com/terratensor/geoupdater/internal/core/domain"
)

// FailedRecordsRepository определяет интерфейс для работы с неудачными записями
type FailedRecordsRepository interface {
	// Save сохраняет неудачную запись
	Save(ctx context.Context, record *domain.FailedRecord) error

	// SaveBatch сохраняет пачку неудачных записей
	SaveBatch(ctx context.Context, records []*domain.FailedRecord) error

	// LoadAll загружает все неудачные записи для повторной обработки
	LoadAll(ctx context.Context) ([]*domain.FailedRecord, error)

	// LoadByAge загружает записи не старше указанного возраста
	LoadByAge(ctx context.Context, maxAgeHours int) ([]*domain.FailedRecord, error)

	// Delete удаляет запись после успешной обработки
	Delete(ctx context.Context, key string) error

	// DeleteBatch удаляет пачку записей
	DeleteBatch(ctx context.Context, keys []string) error

	// Count возвращает количество неудачных записей
	Count(ctx context.Context) (int, error)
}
