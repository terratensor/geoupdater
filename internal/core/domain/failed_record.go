// internal/core/domain/failed_record.go
package domain

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// FailedRecord представляет неудачную запись для повторной обработки
type FailedRecord struct {
	Data      *GeoUpdateData `json:"data"`
	Error     string         `json:"error"`
	Attempts  int            `json:"attempts"`
	Timestamp int64          `json:"timestamp"`
	Filename  string         `json:"filename,omitempty"`
}

// NewFailedRecord создает новый экземпляр FailedRecord
func NewFailedRecord(data *GeoUpdateData, err error, filename string) *FailedRecord {
	return &FailedRecord{
		Data:      data,
		Error:     err.Error(),
		Attempts:  1,
		Timestamp: time.Now().Unix(),
		Filename:  filename,
	}
}

// IncrementAttempt увеличивает счетчик попыток
func (f *FailedRecord) IncrementAttempt() {
	f.Attempts++
	f.Timestamp = time.Now().Unix()
}

// CanRetry проверяет, можно ли повторить попытку
func (f *FailedRecord) CanRetry(maxAttempts int) bool {
	return f.Attempts < maxAttempts
}

// ToJSON сериализует запись в JSON
func (f *FailedRecord) ToJSON() ([]byte, error) {
	return json.Marshal(f)
}

// FromJSON десериализует запись из JSON
func FailedRecordFromJSON(data []byte) (*FailedRecord, error) {
	var record FailedRecord
	err := json.Unmarshal(data, &record)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal failed record: %w", err)
	}
	return &record, nil
}

// Key возвращает уникальный ключ для записи
func (f *FailedRecord) Key() string {
	return strconv.FormatUint(f.Data.DocID, 10)
}
