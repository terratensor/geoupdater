// internal/core/domain/document.go
package domain

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Document представляет полную запись в библиотеке Manticore
type Document struct {
	ID              string  `json:"id"`
	Source          string  `json:"source"`
	Genre           string  `json:"genre"`
	Author          string  `json:"author"`
	Title           string  `json:"title"`
	Content         string  `json:"content"`
	GeohashesString string  `json:"geohashes_string"`
	GeohashesUint64 []int64 `json:"geohashes_uint64"`
	SourceUUID      string  `json:"source_uuid"`
	Language        string  `json:"language"`
	Chunk           uint    `json:"chunk"`
	CharCount       uint    `json:"char_count"`
	WordCount       uint    `json:"word_count"`
	DateTime        int64   `json:"datetime"`
	CreatedAt       int64   `json:"created_at"`
	UpdatedAt       int64   `json:"updated_at"`
}

// GeoUpdateData представляет данные из NDJSON файла для обновления
type GeoUpdateData struct {
	DocID           string   `json:"doc_id"`
	GeohashesString []string `json:"geohashes_string"`
	GeohashesUint64 []int64  `json:"geohashes_uint64"`
}

// NewGeoUpdateData создает новый экземпляр GeoUpdateData с валидацией
func NewGeoUpdateData(docID string, geohashesString []string, geohashesUint64 []int64) (*GeoUpdateData, error) {
	if docID == "" {
		return nil, fmt.Errorf("doc_id cannot be empty")
	}

	// Проверяем соответствие количества геохешей
	if len(geohashesString) != len(geohashesUint64) {
		return nil, fmt.Errorf("geohashes count mismatch: strings=%d, uint64=%d",
			len(geohashesString), len(geohashesUint64))
	}

	// Проверяем, что все геохеши не пустые
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

	// Manticore ожидает []int64 для multi64 поля
	if len(d.GeohashesUint64) > 0 {
		result["geohashes_uint64"] = d.GeohashesUint64
	}

	return result
}

// FromMap создает документ из map (для ответов Manticore)
func FromMap(data map[string]interface{}) (*Document, error) {
	doc := &Document{}

	if id, ok := data["id"]; ok {
		doc.ID = fmt.Sprintf("%v", id)
	}

	if source, ok := data["source"]; ok {
		doc.Source = fmt.Sprintf("%v", source)
	}

	if genre, ok := data["genre"]; ok {
		doc.Genre = fmt.Sprintf("%v", genre)
	}

	if author, ok := data["author"]; ok {
		doc.Author = fmt.Sprintf("%v", author)
	}

	if title, ok := data["title"]; ok {
		doc.Title = fmt.Sprintf("%v", title)
	}

	if content, ok := data["content"]; ok {
		doc.Content = fmt.Sprintf("%v", content)
	}

	if geohashes, ok := data["geohashes_string"]; ok {
		doc.GeohashesString = fmt.Sprintf("%v", geohashes)
	}

	if geohashesUint64, ok := data["geohashes_uint64"]; ok {
		// Manticore может вернуть []interface{}, конвертируем в []int64
		if arr, ok := geohashesUint64.([]interface{}); ok {
			doc.GeohashesUint64 = make([]int64, len(arr))
			for i, v := range arr {
				if num, ok := v.(float64); ok {
					doc.GeohashesUint64[i] = int64(num)
				}
			}
		}
	}

	// Заполняем остальные поля аналогично...

	return doc, nil
}

// UpdateMode определяет режим обновления геоданных
type UpdateMode string

const (
	ModeReplace UpdateMode = "replace"
	ModeMerge   UpdateMode = "merge"
)

// Validate проверяет корректность режима обновления
func (m UpdateMode) Validate() error {
	switch m {
	case ModeReplace, ModeMerge:
		return nil
	default:
		return fmt.Errorf("invalid update mode: %s", m)
	}
}

// Merge объединяет данные из GeoUpdateData с существующим документом
func (d *Document) Merge(data *GeoUpdateData, mode UpdateMode) error {
	if data == nil {
		return fmt.Errorf("update data is nil")
	}

	if d.ID != data.DocID {
		return fmt.Errorf("document ID mismatch: %s != %s", d.ID, data.DocID)
	}

	switch mode {
	case ModeReplace:
		d.replace(data)
	case ModeMerge:
		d.merge(data)
	default:
		return fmt.Errorf("unsupported update mode: %s", mode)
	}

	// Обновляем временную метку
	d.UpdatedAt = time.Now().Unix()

	return nil
}

// replace выполняет полную замену геоданных
func (d *Document) replace(data *GeoUpdateData) {
	// Сортируем для консистентности
	sortedStrings := make([]string, len(data.GeohashesString))
	copy(sortedStrings, data.GeohashesString)
	sort.Strings(sortedStrings)

	d.GeohashesString = strings.Join(sortedStrings, ", ")
	d.GeohashesUint64 = data.GeohashesUint64
}

// merge выполняет слияние геоданных с сохранением уникальности
func (d *Document) merge(data *GeoUpdateData) {
	// Обрабатываем строковые геохеши
	d.GeohashesString = d.mergeGeohashesStrings(data.GeohashesString)

	// Обрабатываем uint64 геохеши
	d.GeohashesUint64 = d.mergeGeohashesUint64(data.GeohashesUint64)
}

// mergeGeohashesStrings объединяет строковые геохеши
func (d *Document) mergeGeohashesStrings(newStrings []string) string {
	existing := make(map[string]bool)

	// Разбираем существующие геохеши
	if d.GeohashesString != "" {
		for _, g := range strings.Split(d.GeohashesString, ",") {
			g = strings.TrimSpace(g)
			if g != "" {
				existing[g] = true
			}
		}
	}

	// Добавляем новые
	for _, g := range newStrings {
		g = strings.TrimSpace(g)
		if g != "" {
			existing[g] = true
		}
	}

	// Сортируем для консистентности
	result := make([]string, 0, len(existing))
	for g := range existing {
		result = append(result, g)
	}
	sort.Strings(result)

	return strings.Join(result, ", ")
}

// mergeGeohashesUint64 объединяет uint64 геохеши
func (d *Document) mergeGeohashesUint64(newUint64 []int64) []int64 {
	existing := make(map[int64]bool)

	// Добавляем существующие
	for _, g := range d.GeohashesUint64 {
		existing[g] = true
	}

	// Добавляем новые
	for _, g := range newUint64 {
		existing[g] = true
	}

	// Сортируем для консистентности
	result := make([]int64, 0, len(existing))
	for g := range existing {
		result = append(result, g)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })

	return result
}

// GetGeohashCount возвращает количество уникальных геохешей
func (d *Document) GetGeohashCount() int {
	count := 0
	if d.GeohashesString != "" {
		count = len(strings.Split(d.GeohashesString, ","))
	}
	return count
}

// IsEmpty проверяет, пуст ли документ (не содержит геоданных)
func (d *Document) IsEmpty() bool {
	return d.GeohashesString == "" && len(d.GeohashesUint64) == 0
}
