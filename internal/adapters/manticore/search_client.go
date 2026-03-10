// internal/adapters/manticore/search_client.go
package manticore

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	Manticoresearch "github.com/manticoresoftware/manticoresearch-go"
	"github.com/terratensor/geoupdater/internal/core/domain"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

// ----------------------------------------------------------------------------
// IDExtractor - безопасное извлечение ID из ответа Manticore
// ----------------------------------------------------------------------------

// IDExtractor помогает извлекать uint64 ID из значения, которое может быть в разных форматах
type IDExtractor struct{}

// ExtractID извлекает uint64 ID из значения
func (e *IDExtractor) ExtractID(idValue interface{}) (uint64, error) {
	if idValue == nil {
		return 0, fmt.Errorf("id is nil")
	}

	switch v := idValue.(type) {
	case float64:
		// В JSON числа приходят как float64
		return uint64(v), nil
	case uint64:
		return v, nil
	case int64:
		return uint64(v), nil
	case int:
		return uint64(v), nil
	case string:
		// Если ID пришел как строка (например, из функции string_id)
		return strconv.ParseUint(v, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected id type: %T", idValue)
	}
}

// ----------------------------------------------------------------------------
// SourceExtractor - извлечение полей из _source
// ----------------------------------------------------------------------------

// SourceExtractor помогает извлекать поля из _source
type SourceExtractor struct{}

// GetString извлекает строковое поле
func (e *SourceExtractor) GetString(source map[string]interface{}, key string) string {
	if val, ok := source[key]; ok && val != nil {
		if str, ok := val.(string); ok {
			return str
		}
		return fmt.Sprintf("%v", val)
	}
	return ""
}

// GetUint извлекает uint поле
func (e *SourceExtractor) GetUint(source map[string]interface{}, key string) uint {
	if val, ok := source[key]; ok && val != nil {
		switch v := val.(type) {
		case float64:
			return uint(v)
		case uint:
			return v
		case int:
			return uint(v)
		}
	}
	return 0
}

// GetInt64 извлекает int64 поле
func (e *SourceExtractor) GetInt64(source map[string]interface{}, key string) int64 {
	if val, ok := source[key]; ok && val != nil {
		switch v := val.(type) {
		case float64:
			return int64(v)
		case int64:
			return v
		case int:
			return int64(v)
		}
	}
	return 0
}

// GetUint64Array извлекает массив uint64 (для MVA полей)
func (e *SourceExtractor) GetUint64Array(source map[string]interface{}, key string) []uint64 {
	if val, ok := source[key]; ok && val != nil {
		if arr, ok := val.([]interface{}); ok {
			result := make([]uint64, len(arr))
			for i, v := range arr {
				if num, ok := v.(float64); ok {
					result[i] = uint64(num)
				}
			}
			return result
		}
	}
	return nil
}

// GetInt64Array извлекает массив int64 (для совместимости с domain.GeohashesUint64)
func (e *SourceExtractor) GetInt64Array(source map[string]interface{}, key string) []int64 {
	if val, ok := source[key]; ok && val != nil {
		if arr, ok := val.([]interface{}); ok {
			result := make([]int64, len(arr))
			for i, v := range arr {
				if num, ok := v.(float64); ok {
					result[i] = int64(num)
				}
			}
			return result
		}
	}
	return nil
}

// ----------------------------------------------------------------------------
// SearchRequestBuilder - построитель запросов для JSON Search API
// ----------------------------------------------------------------------------

// SearchRequestBuilder строит запросы для JSON Search API
type SearchRequestBuilder struct {
	table  string
	query  map[string]interface{}
	limit  int
	offset int
	sort   []map[string]string
	source interface{}
}

// NewSearchRequestBuilder создает новый билдер
func NewSearchRequestBuilder(table string) *SearchRequestBuilder {
	return &SearchRequestBuilder{
		table: table,
		query: make(map[string]interface{}),
		limit: 20,
	}
}

// WithID добавляет поиск по конкретному ID
func (b *SearchRequestBuilder) WithID(id uint64) *SearchRequestBuilder {
	b.query = map[string]interface{}{
		"equals": map[string]interface{}{
			"id": id,
		},
	}
	return b
}

// WithIDs добавляет поиск по нескольким ID
func (b *SearchRequestBuilder) WithIDs(ids []uint64) *SearchRequestBuilder {
	if len(ids) == 0 {
		return b
	}

	if len(ids) == 1 {
		return b.WithID(ids[0])
	}

	// Создаем массив условий "or" для каждого ID
	conditions := make([]map[string]interface{}, len(ids))
	for i, id := range ids {
		conditions[i] = map[string]interface{}{
			"equals": map[string]interface{}{
				"id": id,
			},
		}
	}

	b.query = map[string]interface{}{
		"or": conditions,
	}
	return b
}

// WithLimit устанавливает лимит
func (b *SearchRequestBuilder) WithLimit(limit int) *SearchRequestBuilder {
	if limit > 0 {
		b.limit = limit
	}
	return b
}

// Build собирает запрос в map
func (b *SearchRequestBuilder) Build() map[string]interface{} {
	request := map[string]interface{}{
		"table": b.table,
		"query": b.query,
		"limit": b.limit,
	}

	if b.offset > 0 {
		request["offset"] = b.offset
	}

	if len(b.sort) > 0 {
		request["sort"] = b.sort
	}

	if b.source != nil {
		request["_source"] = b.source
	}

	return request
}

// BuildJSON собирает запрос в JSON строку (для отладки)
func (b *SearchRequestBuilder) BuildJSON() (string, error) {
	request := b.Build()
	data, err := json.MarshalIndent(request, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}
	return string(data), nil
}

// ----------------------------------------------------------------------------
// SearchClient - основной клиент для JSON Search API
// ----------------------------------------------------------------------------

// SearchClient реализует поисковые методы через JSON API
type SearchClient struct {
	apiClient       *Manticoresearch.APIClient
	tableName       string
	logger          ports.Logger
	metrics         ports.MetricsCollector
	idExtractor     *IDExtractor
	sourceExtractor *SourceExtractor
}

// NewSearchClient создает новый поисковый клиент
func NewSearchClient(apiClient *Manticoresearch.APIClient, tableName string,
	logger ports.Logger, metrics ports.MetricsCollector) *SearchClient {

	return &SearchClient{
		apiClient:       apiClient,
		tableName:       tableName,
		logger:          logger,
		metrics:         metrics,
		idExtractor:     &IDExtractor{},
		sourceExtractor: &SourceExtractor{},
	}
}

// SearchByID ищет документ по ID через JSON Search API
func (c *SearchClient) SearchByID(ctx context.Context, id uint64) (*domain.Document, error) {
	start := time.Now()
	defer func() {
		c.metrics.RecordManticoreOperation("search_by_id", time.Since(start), nil)
	}()

	c.logger.Debug("searching by ID",
		ports.Uint64("id", id),
		ports.String("table", c.tableName))

	// Создаем запрос через билдер
	builder := NewSearchRequestBuilder(c.tableName)
	request := builder.WithID(id).WithLimit(1).Build()

	// Выполняем поиск
	resp, httpResp, err := c.apiClient.SearchAPI.Search(ctx).SearchRequest(request).Execute()
	if err != nil {
		if httpResp != nil && httpResp.StatusCode == 404 {
			return nil, ports.ErrNotFound
		}
		return nil, fmt.Errorf("search by id %d failed: %w", id, err)
	}

	// Парсим ответ
	return c.parseSearchResponse(resp, id)
}

// SearchByIDs ищет несколько документов по ID
func (c *SearchClient) SearchByIDs(ctx context.Context, ids []uint64) (map[uint64]*domain.Document, error) {
	start := time.Now()
	defer func() {
		c.metrics.RecordManticoreOperation("search_by_ids", time.Since(start), nil)
	}()

	if len(ids) == 0 {
		return make(map[uint64]*domain.Document), nil
	}

	c.logger.Debug("searching by multiple IDs",
		ports.Int("count", len(ids)),
		ports.String("table", c.tableName))

	// Создаем запрос через билдер
	builder := NewSearchRequestBuilder(c.tableName)
	request := builder.WithIDs(ids).WithLimit(len(ids)).Build()

	// Выполняем поиск
	resp, _, err := c.apiClient.SearchAPI.Search(ctx).SearchRequest(request).Execute()
	if err != nil {
		return nil, fmt.Errorf("search by ids failed: %w", err)
	}

	// Парсим ответ
	return c.parseSearchResponseArray(resp)
}

// parseSearchResponse парсит ответ для одного документа
func (c *SearchClient) parseSearchResponse(resp *Manticoresearch.SearchResponse, expectedID uint64) (*domain.Document, error) {
	if resp.Hits == nil || len(resp.Hits.Hits) == 0 {
		return nil, ports.ErrNotFound
	}

	hit := resp.Hits.Hits[0]

	// Извлекаем ID
	docID, err := c.idExtractor.ExtractID(hit.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to extract ID: %w", err)
	}

	// Проверяем соответствие ожидаемому ID
	if docID != expectedID {
		c.logger.Warn("ID mismatch in search response",
			ports.Uint64("expected", expectedID),
			ports.Uint64("got", docID),
			ports.Any("raw_id", hit.Id))
	}

	// Извлекаем документ из source
	doc, err := c.extractDocumentFromSource(hit.Source)
	if err != nil {
		return nil, err
	}

	doc.ID = docID
	return doc, nil
}

// parseSearchResponseArray парсит ответ в map[uint64]Document
func (c *SearchClient) parseSearchResponseArray(resp *Manticoresearch.SearchResponse) (map[uint64]*domain.Document, error) {
	result := make(map[uint64]*domain.Document)

	if resp.Hits == nil || len(resp.Hits.Hits) == 0 {
		return result, nil
	}

	for _, hit := range resp.Hits.Hits {
		// Извлекаем ID
		docID, err := c.idExtractor.ExtractID(hit.Id)
		if err != nil {
			c.logger.Warn("failed to extract ID from hit",
				ports.Error(err),
				ports.Any("hit", hit))
			continue
		}

		// Извлекаем документ
		doc, err := c.extractDocumentFromSource(hit.Source)
		if err != nil {
			c.logger.Warn("failed to extract document from source",
				ports.Error(err),
				ports.Uint64("id", docID))
			continue
		}

		doc.ID = docID
		result[docID] = doc
	}

	c.logger.Debug("parsed search response",
		ports.Int("expected", len(resp.Hits.Hits)),
		ports.Int("got", len(result)))

	return result, nil
}

// extractDocumentFromSource извлекает поля документа из _source
func (c *SearchClient) extractDocumentFromSource(source interface{}) (*domain.Document, error) {
	if source == nil {
		return nil, fmt.Errorf("source is nil")
	}

	sourceMap, ok := source.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("source is not a map, got %T", source)
	}

	doc := &domain.Document{}

	// Базовые поля
	doc.Source = c.sourceExtractor.GetString(sourceMap, "source")
	doc.Genre = c.sourceExtractor.GetString(sourceMap, "genre")
	doc.Author = c.sourceExtractor.GetString(sourceMap, "author")
	doc.Title = c.sourceExtractor.GetString(sourceMap, "title")
	doc.Content = c.sourceExtractor.GetString(sourceMap, "content")
	doc.GeohashesString = c.sourceExtractor.GetString(sourceMap, "geohashes_string")
	doc.SourceUUID = c.sourceExtractor.GetString(sourceMap, "source_uuid")
	doc.Language = c.sourceExtractor.GetString(sourceMap, "language")

	// Числовые поля
	doc.Chunk = c.sourceExtractor.GetUint(sourceMap, "chunk")
	doc.CharCount = c.sourceExtractor.GetUint(sourceMap, "char_count")
	doc.WordCount = c.sourceExtractor.GetUint(sourceMap, "word_count")
	doc.DateTime = c.sourceExtractor.GetInt64(sourceMap, "datetime")
	doc.CreatedAt = c.sourceExtractor.GetInt64(sourceMap, "created_at")
	doc.UpdatedAt = c.sourceExtractor.GetInt64(sourceMap, "updated_at")

	// MVA поле geohashes_uint64 (в domain оно []int64)
	if uint64Array := c.sourceExtractor.GetUint64Array(sourceMap, "geohashes_uint64"); uint64Array != nil {
		doc.GeohashesUint64 = make([]int64, len(uint64Array))
		for i, v := range uint64Array {
			doc.GeohashesUint64[i] = int64(v)
		}
	}

	return doc, nil
}
