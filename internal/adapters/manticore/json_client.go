// internal/adapters/manticore/json_client.go
package manticore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/terratensor/geoupdater/internal/core/domain"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

// JSONClient простой клиент для Manticore JSON API
type JSONClient struct {
	baseURL    string
	tableName  string
	httpClient *http.Client
	logger     ports.Logger
	metrics    ports.MetricsCollector
}

// NewJSONClient создает новый простой клиент
func NewJSONClient(baseURL, tableName string, timeout time.Duration,
	logger ports.Logger, metrics ports.MetricsCollector) *JSONClient {

	return &JSONClient{
		baseURL:   baseURL,
		tableName: tableName,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger:  logger,
		metrics: metrics,
	}
}

// SearchByID ищет документ по ID
func (c *JSONClient) SearchByID(ctx context.Context, id uint64) (*domain.Document, error) {
	start := time.Now()
	defer func() {
		c.metrics.RecordManticoreOperation("search_by_id", time.Since(start), nil)
	}()

	// Создаем запрос с equals
	request := SearchRequest{
		Table: c.tableName,
		Query: SearchQuery{
			Equals: map[string]interface{}{
				"id": id,
			},
		},
		Limit: 1,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	c.logger.Debug("search request",
		ports.String("json", string(jsonData)),
		ports.Uint64("id", id))

	httpReq, err := http.NewRequestWithContext(ctx, "POST",
		c.baseURL+"/search", bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer httpResp.Body.Close()

	bodyBytes, _ := io.ReadAll(httpResp.Body)

	if httpResp.StatusCode == 404 {
		return nil, ports.ErrNotFound
	}

	if httpResp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected status code: %d, body: %s",
			httpResp.StatusCode, string(bodyBytes))
	}

	// ВАЖНО: используем декодер с UseNumber()
	var searchResp SearchResponse
	decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
	decoder.UseNumber()

	if err := decoder.Decode(&searchResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w, body: %s", err, string(bodyBytes))
	}

	if len(searchResp.Hits.Hits) == 0 {
		return nil, ports.ErrNotFound
	}

	hit := searchResp.Hits.Hits[0]

	// Получаем ID через хелпер
	docID, err := hit.GetUint64ID()
	if err != nil {
		return nil, fmt.Errorf("failed to parse document ID: %w", err)
	}

	doc := &hit.Source
	doc.ID = docID

	c.logger.Debug("search response",
		ports.Uint64("id", doc.ID),
		ports.String("title", doc.Title))

	return doc, nil
}

// SearchByIDs ищет несколько документов по списку ID используя IN
// maxMatches - максимальное количество результатов, которое Manticore сохраняет в памяти
func (c *JSONClient) SearchByIDs(ctx context.Context, ids []uint64, maxMatches int) (map[uint64]*domain.Document, error) {
	start := time.Now()
	defer func() {
		c.metrics.RecordManticoreOperation("search_by_ids", time.Since(start), nil)
	}()

	if len(ids) == 0 {
		return make(map[uint64]*domain.Document), nil
	}

	c.logger.Debug("searching by multiple IDs",
		ports.Int("count", len(ids)),
		ports.Int("max_matches", maxMatches))

	// Конвертируем []uint64 в []interface{} для JSON
	idValues := make([]interface{}, len(ids))
	for i, id := range ids {
		idValues[i] = id
	}

	// Создаем запрос с IN оператором
	request := SearchRequest{
		Table: c.tableName,
		Query: SearchQuery{
			In: map[string]interface{}{
				"id": idValues,
			},
		},
		Limit: len(ids),
	}

	// Устанавливаем max_matches если он больше лимита
	// max_matches должен быть >= limit, иначе получим только max_matches результатов
	if maxMatches > len(ids) {
		request.MaxMatches = &maxMatches
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	c.logger.Debug("batch search request",
		ports.String("json", string(jsonData)))

	httpReq, err := http.NewRequestWithContext(ctx, "POST",
		c.baseURL+"/search", bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer httpResp.Body.Close()

	bodyBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if httpResp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected status code: %d, body: %s",
			httpResp.StatusCode, string(bodyBytes))
	}

	var searchResp SearchResponse
	decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
	decoder.UseNumber() // Критически важно!

	if err := decoder.Decode(&searchResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w, body: %s", err, string(bodyBytes))
	}

	result := make(map[uint64]*domain.Document, len(searchResp.Hits.Hits))
	for _, hit := range searchResp.Hits.Hits {
		docID, err := hit.GetUint64ID()
		if err != nil {
			c.logger.Warn("failed to parse document ID",
				ports.String("raw_id", string(hit.ID)),
				ports.Error(err))
			continue
		}

		doc := &hit.Source
		doc.ID = docID
		result[docID] = doc
	}

	c.logger.Debug("search by IDs completed",
		ports.Int("requested", len(ids)),
		ports.Int("found", len(result)),
		ports.Int("total", searchResp.Hits.Total))

	return result, nil
}

// BulkSearchByIDs выполняет пакетный поиск с разбиением на батчи
// batchSize - размер одного батча (рекомендуется 1000)
// maxMatches - максимальное количество результатов для каждого батча
func (c *JSONClient) BulkSearchByIDs(ctx context.Context, ids []uint64, batchSize int, maxMatches int) (map[uint64]*domain.Document, error) {
	result := make(map[uint64]*domain.Document)

	if len(ids) == 0 {
		return result, nil
	}

	c.logger.Info("starting bulk search",
		ports.Int("total_ids", len(ids)),
		ports.Int("batch_size", batchSize),
		ports.Int("max_matches", maxMatches))

	// Убеждаемся что max_matches не меньше batchSize
	if maxMatches < batchSize {
		maxMatches = batchSize
		c.logger.Debug("adjusted max_matches to batch size", ports.Int("max_matches", maxMatches))
	}

	for i := 0; i < len(ids); i += batchSize {
		end := i + batchSize
		if end > len(ids) {
			end = len(ids)
		}

		batchIDs := ids[i:end]

		// Для последнего батча max_matches может быть меньше
		currentMaxMatches := maxMatches
		if len(batchIDs) < batchSize {
			currentMaxMatches = len(batchIDs)
		}

		c.logger.Debug("processing batch",
			ports.Int("batch_start", i),
			ports.Int("batch_end", end),
			ports.Int("batch_size", len(batchIDs)))

		batchResult, err := c.SearchByIDs(ctx, batchIDs, currentMaxMatches)
		if err != nil {
			c.logger.Error("batch search failed",
				ports.Int("from", i),
				ports.Int("to", end),
				ports.Error(err))
			// Продолжаем со следующим батчем, не прерываем всю операцию
			continue
		}

		for id, doc := range batchResult {
			result[id] = doc
		}

		c.logger.Debug("batch processed",
			ports.Int("batch_size", len(batchIDs)),
			ports.Int("found", len(batchResult)))
	}

	c.logger.Info("bulk search completed",
		ports.Int("total_found", len(result)),
		ports.Int("total_requested", len(ids)))

	return result, nil
}

// Ping проверяет соединение с Manticore
func (c *JSONClient) Ping(ctx context.Context) error {
	request := SearchRequest{
		Table: c.tableName,
		Query: SearchQuery{
			Equals: map[string]interface{}{
				"id": 1,
			},
		},
		Limit: 1,
	}

	jsonData, _ := json.Marshal(request)

	httpReq, err := http.NewRequestWithContext(ctx, "POST",
		c.baseURL+"/search", bytes.NewReader(jsonData))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode >= 500 {
		return fmt.Errorf("manticore returned status %d", httpResp.StatusCode)
	}

	return nil
}
