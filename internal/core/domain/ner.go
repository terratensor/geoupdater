// internal/core/domain/ner.go
package domain

import (
	"encoding/json"
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
	ID        uint64 `json:"id"`      // Автоинкрементный ID Manticore
	DocID     uint64 `json:"doc_id"`  // Оригинальный ID документа
	Location  string `json:"ner_loc"` // JSON строка
	Person    string `json:"ner_per"` // JSON строка
	Org       string `json:"ner_org"` // JSON строка
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

// ToMap конвертирует NERDocument в map для Manticore
func (d *NERDocument) ToMap() map[string]interface{} {
	result := map[string]interface{}{
		"doc_id":     d.DocID,
		"ner_loc":    d.Location,
		"ner_per":    d.Person,
		"ner_org":    d.Org,
		"created_at": d.CreatedAt,
		"updated_at": d.UpdatedAt,
	}

	// Если есть ID, добавляем его (для update/replace)
	if d.ID > 0 {
		result["id"] = d.ID
	}

	return result
}

// NewNERDocumentFromData создает NERDocument из NERData
func NewNERDocumentFromData(data *NERData) (*NERDocument, error) {
	locJSON, err := json.Marshal(data.Location)
	if err != nil {
		return nil, err
	}

	perJSON, err := json.Marshal(data.Person)
	if err != nil {
		return nil, err
	}

	orgJSON, err := json.Marshal(data.Org)
	if err != nil {
		return nil, err
	}

	now := time.Now().Unix()

	return &NERDocument{
		DocID:     data.DocID,
		Location:  string(locJSON),
		Person:    string(perJSON),
		Org:       string(orgJSON),
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// Update обновляет NERDocument новыми данными
func (d *NERDocument) Update(data *NERData) error {
	locJSON, err := json.Marshal(data.Location)
	if err != nil {
		return err
	}

	perJSON, err := json.Marshal(data.Person)
	if err != nil {
		return err
	}

	orgJSON, err := json.Marshal(data.Org)
	if err != nil {
		return err
	}

	d.Location = string(locJSON)
	d.Person = string(perJSON)
	d.Org = string(orgJSON)
	d.UpdatedAt = time.Now().Unix()

	return nil
}

// IsEmpty проверяет, пуст ли документ
func (d *NERDocument) IsEmpty() bool {
	return len(d.Location) == 0 && len(d.Person) == 0 && len(d.Org) == 0
}
