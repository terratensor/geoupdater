// internal/core/domain/ner.go
package domain

import (
	"encoding/json"
	"strconv"
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

// NERData представляет данные из входного файла
type NERData struct {
	DocID    uint64      `json:"doc_id"`
	Location []NEREntity `json:"ner_loc"`
	Person   []NEREntity `json:"ner_per"`
	Org      []NEREntity `json:"ner_org"`
}

// UnmarshalJSON для NERData с поддержкой разных форматов doc_id
func (n *NERData) UnmarshalJSON(data []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Парсим doc_id (может быть строкой или числом)
	if idVal, ok := raw["doc_id"]; ok {
		switch v := idVal.(type) {
		case float64:
			n.DocID = uint64(v)
		case string:
			n.DocID, _ = strconv.ParseUint(v, 10, 64)
		case json.Number:
			n.DocID, _ = strconv.ParseUint(string(v), 10, 64)
		}
	}

	// Парсим массивы NER сущностей
	if loc, ok := raw["ner_loc"]; ok && loc != nil {
		locBytes, _ := json.Marshal(loc)
		json.Unmarshal(locBytes, &n.Location)
	}
	if per, ok := raw["ner_per"]; ok && per != nil {
		perBytes, _ := json.Marshal(per)
		json.Unmarshal(perBytes, &n.Person)
	}
	if org, ok := raw["ner_org"]; ok && org != nil {
		orgBytes, _ := json.Marshal(org)
		json.Unmarshal(orgBytes, &n.Org)
	}

	return nil
}

// NERDocument представляет документ в NER таблице Manticore
type NERDocument struct {
	ID        uint64      `json:"id,omitempty"`
	DocID     uint64      `json:"doc_id"`
	Location  []NEREntity `json:"ner_loc"`
	Person    []NEREntity `json:"ner_per"`
	Org       []NEREntity `json:"ner_org"`
	CreatedAt int64       `json:"created_at"`
	UpdatedAt int64       `json:"updated_at"`
}

// ToMap конвертирует в map для Manticore
func (d *NERDocument) ToMap() map[string]interface{} {
	result := map[string]interface{}{
		"doc_id":     d.DocID,
		"ner_loc":    d.Location,
		"ner_per":    d.Person,
		"ner_org":    d.Org,
		"created_at": d.CreatedAt,
		"updated_at": d.UpdatedAt,
	}
	if d.ID > 0 {
		result["id"] = d.ID
	}
	return result
}

// NewNERDocumentFromData создает из NERData
func NewNERDocumentFromData(data *NERData) *NERDocument {
	now := time.Now().Unix()
	return &NERDocument{
		DocID:     data.DocID,
		Location:  data.Location,
		Person:    data.Person,
		Org:       data.Org,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// Update обновляет документ новыми данными
func (d *NERDocument) Update(data *NERData) {
	d.Location = data.Location
	d.Person = data.Person
	d.Org = data.Org
	d.UpdatedAt = time.Now().Unix()
}

// IsEmpty проверяет, пуст ли документ
func (d *NERDocument) IsEmpty() bool {
	return len(d.Location) == 0 && len(d.Person) == 0 && len(d.Org) == 0
}
