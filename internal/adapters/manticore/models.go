// internal/adapters/manticore/models.go
package manticore

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/terratensor/geoupdater/internal/core/domain"
)

// SearchRequest структура запроса к Manticore JSON API
type SearchRequest struct {
	Table      string                 `json:"table"`
	Query      SearchQuery            `json:"query"`
	Limit      int                    `json:"limit,omitempty"`
	MaxMatches *int                   `json:"max_matches,omitempty"`
	Options    map[string]interface{} `json:"options,omitempty"`
}

// SearchQuery структура запроса
type SearchQuery struct {
	Equals map[string]interface{} `json:"equals,omitempty"`
	In     map[string]interface{} `json:"in,omitempty"`
	Bool   *BoolQuery             `json:"bool,omitempty"`
	Range  map[string]interface{} `json:"range,omitempty"`
	Match  map[string]interface{} `json:"match,omitempty"`
}

// BoolQuery для сложных условий
type BoolQuery struct {
	Must    []map[string]interface{} `json:"must,omitempty"`
	Should  []map[string]interface{} `json:"should,omitempty"`
	MustNot []map[string]interface{} `json:"must_not,omitempty"`
}

// SearchResponse структура ответа от Manticore
type SearchResponse struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Hits     Hits `json:"hits"`
}

// Hits обертка для результатов поиска
type Hits struct {
	Total int   `json:"total"`
	Hits  []Hit `json:"hits"`
}

// Hit один найденный документ - используем json.Number для ID
type Hit struct {
	ID     json.Number     `json:"_id"` // json.Number сохраняет точность!
	Score  float64         `json:"_score"`
	Source domain.Document `json:"_source"`
}

// GetUint64ID конвертирует json.Number в uint64
func (h *Hit) GetUint64ID() (uint64, error) {
	if h.ID == "" {
		return 0, fmt.Errorf("ID is empty")
	}
	return strconv.ParseUint(string(h.ID), 10, 64)
}

// MustGetUint64ID возвращает ID или паникует (для тестов)
func (h *Hit) MustGetUint64ID() uint64 {
	id, err := h.GetUint64ID()
	if err != nil {
		panic(fmt.Sprintf("failed to parse ID %s: %v", h.ID, err))
	}
	return id
}

// BulkResponse структура для ответа на bulk запросы
type BulkResponse struct {
	Items  []map[string]BulkItem `json:"items"`
	Errors bool                  `json:"errors"`
}

// BulkItem один элемент в bulk ответе
type BulkItem struct {
	ID     uint64 `json:"_id"`
	Result string `json:"result"`
	Status int    `json:"status"`
	Error  string `json:"error,omitempty"`
}
