// internal/core/domain/ner.go
package domain

import (
	"time"
)

// NEREntity представляет распознанную сущность
type NEREntity struct {
	Value      string   `json:"value"`
	StartPos   int      `json:"start_pos"`
	EndPos     int      `json:"end_pos"`
	Geohash    []string `json:"geohash"`
	Confidence float64  `json:"confidence"`
}

// NERData представляет все NER данные для документа из входного файла
type NERData struct {
	DocID    uint64      `json:"doc_id"`
	Location []NEREntity `json:"ner_loc"`
	Person   []NEREntity `json:"ner_per"`
	Org      []NEREntity `json:"ner_org"`
}

// NERDocument представляет документ в NER таблице Manticore
type NERDocument struct {
	ID        uint64      `json:"id,omitempty"`      // Автоинкрементный ID Manticore
	DocID     uint64      `json:"doc_id"`            // Оригинальный ID документа
	Location  []NEREntity `json:"ner_loc,omitempty"` // Массив объектов, а не строка!
	Person    []NEREntity `json:"ner_per,omitempty"` // Массив объектов
	Org       []NEREntity `json:"ner_org,omitempty"` // Массив объектов
	CreatedAt int64       `json:"created_at,omitempty"`
	UpdatedAt int64       `json:"updated_at,omitempty"`
}

// ToMap конвертирует NERDocument в map для Manticore
func (d *NERDocument) ToMap() map[string]interface{} {
	result := map[string]interface{}{
		"doc_id":     d.DocID,
		"created_at": d.CreatedAt,
		"updated_at": d.UpdatedAt,
	}

	// JSON marshaling автоматически превратит []NEREntity в правильный JSON массив
	// или в null если слайс пустой
	if d.Location != nil {
		result["ner_loc"] = d.Location
	}
	if d.Person != nil {
		result["ner_per"] = d.Person
	}
	if d.Org != nil {
		result["ner_org"] = d.Org
	}

	// Если есть ID, добавляем его
	if d.ID > 0 {
		result["id"] = d.ID
	}

	return result
}

// NewNERDocumentFromData создает NERDocument из NERData
func NewNERDocumentFromData(data *NERData) *NERDocument {
	now := time.Now().Unix()

	return &NERDocument{
		DocID:     data.DocID,
		Location:  data.Location, // Просто копируем слайс, без маршалинга!
		Person:    data.Person,
		Org:       data.Org,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// Update обновляет NERDocument новыми данными
func (d *NERDocument) Update(data *NERData) {
	d.Location = data.Location
	d.Person = data.Person
	d.Org = data.Org
	d.UpdatedAt = time.Now().Unix()
	// CreatedAt не меняется
}

// IsEmpty проверяет, пуст ли документ
func (d *NERDocument) IsEmpty() bool {
	return len(d.Location) == 0 && len(d.Person) == 0 && len(d.Org) == 0
}
