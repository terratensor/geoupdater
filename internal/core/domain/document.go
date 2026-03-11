// internal/core/domain/document.go
package domain

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// UpdateMode режим обновления
type UpdateMode string

const (
	ModeReplace UpdateMode = "replace"
	ModeMerge   UpdateMode = "merge"
)

// GeoUpdateData представляет данные из NDJSON файла для обновления
type GeoUpdateData struct {
	DocID           uint64   `json:"doc_id"`
	GeohashesString []string `json:"geohashes_string"`
	GeohashesUint64 []uint64 `json:"geohashes_uint64"` // Используем uint64 для точности
}

// NewGeoUpdateData создает новый экземпляр GeoUpdateData с валидацией
func NewGeoUpdateData(docID uint64, geohashesString []string, geohashesUint64 []uint64) (*GeoUpdateData, error) {
	if docID == 0 {
		return nil, fmt.Errorf("doc_id cannot be 0")
	}

	if len(geohashesString) != len(geohashesUint64) {
		return nil, fmt.Errorf("geohashes count mismatch: strings=%d, uint64=%d",
			len(geohashesString), len(geohashesUint64))
	}

	for i, g := range geohashesString {
		if g == "" {
			return nil, fmt.Errorf("geohashes_string[%d] is empty", i)
		}
	}

	return &GeoUpdateData{
		DocID:           docID,
		GeohashesString: geohashesString,
		GeohashesUint64: geohashesUint64,
	}, nil
}

// Document представляет полную запись в библиотеке Manticore
type Document struct {
	ID              uint64   `json:"id"`
	Source          string   `json:"source"`
	Genre           string   `json:"genre"`
	Author          string   `json:"author"`
	Title           string   `json:"title"`
	Content         string   `json:"content"`
	GeohashesString string   `json:"geohashes_string"`
	GeohashesUint64 []uint64 `json:"geohashes_uint64"`
	SourceUUID      string   `json:"source_uuid"`
	Language        string   `json:"language"`
	Chunk           uint     `json:"chunk"`
	CharCount       uint     `json:"char_count"`
	WordCount       uint     `json:"word_count"`
	DateTime        int64    `json:"datetime"`
	CreatedAt       int64    `json:"created_at"`
	UpdatedAt       int64    `json:"updated_at"`
}

// UnmarshalJSON кастомный парсинг для Document
func (d *Document) UnmarshalJSON(data []byte) error {
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Парсим ID
	if idVal, ok := raw["id"]; ok {
		switch v := idVal.(type) {
		case float64:
			d.ID = uint64(v)
		case string:
			d.ID, _ = strconv.ParseUint(v, 10, 64)
		case json.Number:
			d.ID, _ = strconv.ParseUint(string(v), 10, 64)
		}
	}

	// Парсим строковые поля
	if val, ok := raw["source"].(string); ok {
		d.Source = val
	}
	if val, ok := raw["genre"].(string); ok {
		d.Genre = val
	}
	if val, ok := raw["author"].(string); ok {
		d.Author = val
	}
	if val, ok := raw["title"].(string); ok {
		d.Title = val
	}
	if val, ok := raw["content"].(string); ok {
		d.Content = val
	}
	if val, ok := raw["geohashes_string"].(string); ok {
		d.GeohashesString = val
	}
	if val, ok := raw["source_uuid"].(string); ok {
		d.SourceUUID = val
	}
	if val, ok := raw["language"].(string); ok {
		d.Language = val
	}

	// Парсим числовые поля
	if val, ok := raw["chunk"].(float64); ok {
		d.Chunk = uint(val)
	}
	if val, ok := raw["char_count"].(float64); ok {
		d.CharCount = uint(val)
	}
	if val, ok := raw["word_count"].(float64); ok {
		d.WordCount = uint(val)
	}
	if val, ok := raw["datetime"].(float64); ok {
		d.DateTime = int64(val)
	}
	if val, ok := raw["created_at"].(float64); ok {
		d.CreatedAt = int64(val)
	}
	if val, ok := raw["updated_at"].(float64); ok {
		d.UpdatedAt = int64(val)
	}

	// Парсим geohashes_uint64 (multi64) с сохранением точности
	if geoVal, ok := raw["geohashes_uint64"]; ok && geoVal != nil {
		switch v := geoVal.(type) {
		case []interface{}:
			d.GeohashesUint64 = make([]uint64, 0, len(v))
			for _, item := range v {
				switch num := item.(type) {
				case float64:
					// Предупреждение о потере точности
					d.GeohashesUint64 = append(d.GeohashesUint64, uint64(num))
				case string:
					if parsed, err := strconv.ParseUint(num, 10, 64); err == nil {
						d.GeohashesUint64 = append(d.GeohashesUint64, parsed)
					}
				case json.Number:
					if parsed, err := strconv.ParseUint(string(num), 10, 64); err == nil {
						d.GeohashesUint64 = append(d.GeohashesUint64, parsed)
					}
				}
			}
		case []uint64:
			d.GeohashesUint64 = v
		case string:
			if v != "" && v != "()" {
				trimmed := strings.Trim(v, "()")
				parts := strings.Split(trimmed, ",")
				d.GeohashesUint64 = make([]uint64, 0, len(parts))
				for _, part := range parts {
					if parsed, err := strconv.ParseUint(strings.TrimSpace(part), 10, 64); err == nil {
						d.GeohashesUint64 = append(d.GeohashesUint64, parsed)
					}
				}
			}
		}
	}

	return nil
}

// ToMap конвертирует документ в map для Manticore API
func (d *Document) ToMap() map[string]interface{} {
	result := map[string]interface{}{
		"id":               d.ID,
		"source":           d.Source,
		"genre":            d.Genre,
		"author":           d.Author,
		"title":            d.Title,
		"content":          d.Content,
		"geohashes_string": d.GeohashesString,
		"source_uuid":      d.SourceUUID,
		"language":         d.Language,
		"chunk":            d.Chunk,
		"char_count":       d.CharCount,
		"word_count":       d.WordCount,
		"datetime":         d.DateTime,
		"created_at":       d.CreatedAt,
		"updated_at":       d.UpdatedAt,
	}

	if len(d.GeohashesUint64) > 0 {
		int64Slice := make([]int64, len(d.GeohashesUint64))
		for i, v := range d.GeohashesUint64 {
			int64Slice[i] = int64(v)
		}
		result["geohashes_uint64"] = int64Slice
	}

	return result
}

// Validate проверяет корректность режима обновления
func (m UpdateMode) Validate() error {
	switch m {
	case ModeReplace, ModeMerge:
		return nil
	default:
		return fmt.Errorf("invalid update mode: %s", m)
	}
}

// Merge объединяет данные из GeoUpdateData
func (d *Document) Merge(data *GeoUpdateData, mode UpdateMode) error {
	if data == nil {
		return fmt.Errorf("update data is nil")
	}

	if d.ID != data.DocID {
		return fmt.Errorf("document ID mismatch: %d != %d", d.ID, data.DocID)
	}

	switch mode {
	case ModeReplace:
		d.replace(data)
	case ModeMerge:
		d.merge(data)
	default:
		return fmt.Errorf("unsupported update mode: %s", mode)
	}

	d.UpdatedAt = time.Now().Unix()
	return nil
}

// replace выполняет полную замену геоданных
func (d *Document) replace(data *GeoUpdateData) {
	sortedStrings := make([]string, len(data.GeohashesString))
	copy(sortedStrings, data.GeohashesString)
	sort.Strings(sortedStrings)
	d.GeohashesString = strings.Join(sortedStrings, ", ")

	d.GeohashesUint64 = make([]uint64, len(data.GeohashesUint64))
	copy(d.GeohashesUint64, data.GeohashesUint64)
	sort.Slice(d.GeohashesUint64, func(i, j int) bool {
		return d.GeohashesUint64[i] < d.GeohashesUint64[j]
	})
}

// merge выполняет слияние геоданных
func (d *Document) merge(data *GeoUpdateData) {
	d.GeohashesString = d.mergeGeohashesStrings(data.GeohashesString)

	existing := make(map[uint64]bool)
	for _, g := range d.GeohashesUint64 {
		existing[g] = true
	}
	for _, g := range data.GeohashesUint64 {
		existing[g] = true
	}

	result := make([]uint64, 0, len(existing))
	for g := range existing {
		result = append(result, g)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })

	d.GeohashesUint64 = result
}

// mergeGeohashesStrings объединяет строковые геохеши
func (d *Document) mergeGeohashesStrings(newStrings []string) string {
	existing := make(map[string]bool)

	if d.GeohashesString != "" {
		for _, g := range strings.Split(d.GeohashesString, ",") {
			g = strings.TrimSpace(g)
			if g != "" {
				existing[g] = true
			}
		}
	}

	for _, g := range newStrings {
		g = strings.TrimSpace(g)
		if g != "" {
			existing[g] = true
		}
	}

	result := make([]string, 0, len(existing))
	for g := range existing {
		result = append(result, g)
	}
	sort.Strings(result)

	return strings.Join(result, ", ")
}

// GetGeohashCount возвращает количество уникальных геохешей
func (d *Document) GetGeohashCount() int {
	count := 0
	if d.GeohashesString != "" {
		count = len(strings.Split(d.GeohashesString, ","))
	}
	return count
}

// IsEmpty проверяет, пуст ли документ
func (d *Document) IsEmpty() bool {
	return d.GeohashesString == "" && len(d.GeohashesUint64) == 0
}
