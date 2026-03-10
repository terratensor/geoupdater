// internal/adapters/manticore/client.go
package manticore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	Manticoresearch "github.com/manticoresoftware/manticoresearch-go"
	"github.com/terratensor/geoupdater/internal/core/domain"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

// Client реализует ports.Repository для Manticore Search
type Client struct {
	apiClient *Manticoresearch.APIClient
	config    *Config
	logger    ports.Logger
	metrics   ports.MetricsCollector
	tableName string
}

// Config конфигурация Manticore клиента
type Config struct {
	Host       string
	Port       int
	TableName  string
	Timeout    time.Duration
	MaxConns   int
	RetryCount int
	RetryDelay time.Duration
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() *Config {
	return &Config{
		Host:       "localhost",
		Port:       9308,
		TableName:  "library2026",
		Timeout:    30 * time.Second,
		MaxConns:   10,
		RetryCount: 3,
		RetryDelay: 1 * time.Second,
	}
}

// NewClient создает новый экземпляр Manticore клиента
func NewClient(cfg *Config, logger ports.Logger, metrics ports.MetricsCollector) (*Client, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	configuration := Manticoresearch.NewConfiguration()
	configuration.Servers[0].URL = fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port)
	configuration.HTTPClient.Timeout = cfg.Timeout

	apiClient := Manticoresearch.NewAPIClient(configuration)

	client := &Client{
		apiClient: apiClient,
		config:    cfg,
		logger:    logger,
		metrics:   metrics,
		tableName: cfg.TableName,
	}

	// Проверяем соединение
	if err := client.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to connect to Manticore: %w", err)
	}

	logger.Info("connected to Manticore",
		ports.String("host", cfg.Host),
		ports.Int("port", cfg.Port),
		ports.String("table", cfg.TableName),
	)

	return client, nil
}

// Ping проверяет соединение с Manticore через SQL запрос
func (c *Client) Ping(ctx context.Context) error {
	start := time.Now()
	defer func() {
		c.metrics.RecordManticoreOperation("ping", time.Since(start), nil)
	}()

	// Используем простой SQL запрос для проверки
	resp, _, err := c.apiClient.UtilsAPI.Sql(ctx).Body("SHOW TABLES").Execute()

	if err != nil {
		c.logger.Error("ping failed", ports.Error(err))
		return fmt.Errorf("ping failed: %w", err)
	}

	// Проверяем что ответ корректен
	if resp == nil {
		return fmt.Errorf("empty response from Manticore")
	}

	return nil
}

// GetDocument получает документ по ID через SQL запрос
func (c *Client) GetDocument(ctx context.Context, id string) (*domain.Document, error) {
	start := time.Now()

	var doc *domain.Document
	var err error

	operation := func() error {
		var getErr error
		doc, getErr = c.getDocument(ctx, id)
		return getErr
	}

	err = c.withRetry(ctx, operation)

	c.metrics.RecordManticoreOperation("get", time.Since(start), err)

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, ports.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get document %s: %w", id, err)
	}

	return doc, nil
}

// getDocument внутренний метод для получения документа через SQL
func (c *Client) getDocument(ctx context.Context, id string) (*domain.Document, error) {
	// Используем SQL запрос для получения документа
	query := fmt.Sprintf("SELECT * FROM %s WHERE id = %s LIMIT 1", c.tableName, id)

	resp, _, err := c.apiClient.UtilsAPI.Sql(ctx).Body(query).RawResponse(false).Execute()
	if err != nil {
		return nil, err
	}

	// Парсим ответ
	return c.parseSQLResponse(resp, id)
}

// GetDocumentsBatch получает пачку документов по списку ID через один SQL запрос
func (c *Client) GetDocumentsBatch(ctx context.Context, ids []string) (map[string]*domain.Document, error) {
	start := time.Now()

	if len(ids) == 0 {
		return make(map[string]*domain.Document), nil
	}

	var result map[string]*domain.Document
	var err error

	operation := func() error {
		var getErr error
		result, getErr = c.getDocumentsBatch(ctx, ids)
		return getErr
	}

	err = c.withRetry(ctx, operation)

	c.metrics.RecordManticoreOperation("get_batch", time.Since(start), err)

	if err != nil {
		return nil, fmt.Errorf("failed to get documents batch: %w", err)
	}

	return result, nil
}

// getDocumentsBatch внутренний метод для получения пачки документов через SQL IN
func (c *Client) getDocumentsBatch(ctx context.Context, ids []string) (map[string]*domain.Document, error) {
	// Формируем список ID для IN запроса
	idList := strings.Join(ids, ",")
	query := fmt.Sprintf("SELECT * FROM %s WHERE id IN (%s)", c.tableName, idList)

	resp, _, err := c.apiClient.UtilsAPI.Sql(ctx).Body(query).RawResponse(false).Execute()
	if err != nil {
		return nil, err
	}

	return c.parseSQLResponseArray(resp)
}

// ReplaceDocument заменяет документ полностью через REPLACE
func (c *Client) ReplaceDocument(ctx context.Context, doc *domain.Document) error {
	start := time.Now()

	operation := func() error {
		return c.replaceDocument(ctx, doc)
	}

	err := c.withRetry(ctx, operation)

	c.metrics.RecordManticoreOperation("replace", time.Since(start), err)

	if err != nil {
		return fmt.Errorf("failed to replace document %s: %w", doc.ID, err)
	}

	return nil
}

// replaceDocument внутренний метод для замены документа через JSON REPLACE
func (c *Client) replaceDocument(ctx context.Context, doc *domain.Document) error {
	// Создаем запрос на replace через JSON API
	replaceRequest := Manticoresearch.NewInsertDocumentRequest(c.tableName, doc.ToMap())

	// Устанавливаем ID если есть
	if doc.ID != "" {
		// ID может быть числом или строкой, конвертируем в int64
		var idInt int64
		fmt.Sscanf(doc.ID, "%d", &idInt)
		if idInt > 0 {
			replaceRequest.SetId(idInt)
		}
	}

	_, _, err := c.apiClient.IndexAPI.Replace(ctx).InsertDocumentRequest(*replaceRequest).Execute()
	if err != nil {
		return fmt.Errorf("replace failed: %w", err)
	}

	c.logger.Debug("document replaced",
		ports.String("id", doc.ID),
		ports.Int("geohashes_count", doc.GetGeohashCount()),
	)

	return nil
}

// BulkReplace выполняет массовую замену документов через /bulk endpoint
func (c *Client) BulkReplace(ctx context.Context, docs []*domain.Document) (*domain.BatchResult, error) {
	start := time.Now()
	result := domain.NewBatchResult()

	if len(docs) == 0 {
		return result, nil
	}

	c.logger.Info("starting bulk replace",
		ports.Int("batch_size", len(docs)),
	)

	// Разбиваем на батчи (Manticore может иметь ограничение)
	batches := c.splitIntoBatches(docs, 1000)

	for _, batch := range batches {
		batchResult, err := c.bulkReplaceBatch(ctx, batch)
		if err != nil {
			c.logger.Error("bulk replace batch failed",
				ports.Int("batch_size", len(batch)),
				ports.Error(err),
			)
			// Добавляем все документы как failed
			for _, doc := range batch {
				result.AddFailed(doc.ID, err, domain.ErrorTypeManticore, 1)
			}
		} else {
			result.Merge(batchResult)
		}
	}

	result.Duration = time.Since(start)

	c.metrics.RecordBatchProcessed(len(docs), result.Duration, result.Failed)

	c.logger.Info("bulk replace completed",
		ports.String("summary", result.Summary()),
		ports.Int64("duration_ms", result.Duration.Milliseconds()),
	)

	return result, nil
}

// bulkReplaceBatch выполняет bulk replace для одного батча через /bulk endpoint
func (c *Client) bulkReplaceBatch(ctx context.Context, docs []*domain.Document) (*domain.BatchResult, error) {
	result := domain.NewBatchResult()

	// Формируем NDJSON для bulk запроса
	var bulkLines []string

	for _, doc := range docs {
		// Каждая операция replace в формате NDJSON
		// { "replace" : { "table" : "table_name", "id" : id, "doc": { ... } } }
		docMap := doc.ToMap()

		// Конвертируем ID в int64
		var idInt int64
		if doc.ID != "" {
			fmt.Sscanf(doc.ID, "%d", &idInt)
		}

		// Убираем ID из doc, так как он передается отдельно
		delete(docMap, "id")

		replaceOp := map[string]interface{}{
			"replace": map[string]interface{}{
				"table": c.tableName,
				"id":    idInt,
				"doc":   docMap,
			},
		}

		jsonLine, err := json.Marshal(replaceOp)
		if err != nil {
			result.AddFailed(doc.ID, fmt.Errorf("failed to marshal: %w", err), domain.ErrorTypeValidation, 1)
			continue
		}

		bulkLines = append(bulkLines, string(jsonLine))
	}

	// Отправляем bulk запрос
	bulkBody := strings.Join(bulkLines, "\n") + "\n"

	bulkResp, _, err := c.apiClient.IndexAPI.Bulk(ctx).Body(bulkBody).Execute()
	if err != nil {
		return nil, fmt.Errorf("bulk request failed: %w", err)
	}

	// Анализируем ответ
	if bulkResp.Items != nil {
		for _, item := range bulkResp.Items {
			if replace, ok := item["replace"]; ok {
				// Парсим результат операции replace
				if replaceMap, ok := replace.(map[string]interface{}); ok {
					if resultStr, ok := replaceMap["result"]; ok {
						if resultStr == "updated" || resultStr == "created" {
							result.AddSuccess()
						} else {
							// Извлекаем ID
							var docID string
							if id, ok := replaceMap["_id"]; ok {
								docID = fmt.Sprintf("%v", id)
							}
							result.AddFailed(docID,
								fmt.Errorf("replace failed: %v", resultStr),
								domain.ErrorTypeManticore, 1)
						}
					}
				}
			}
		}
	}

	// Проверяем общую ошибку
	if bulkResp.Errors != nil && *bulkResp.Errors {
		c.logger.Warn("bulk operation had errors",
			ports.Any("error", bulkResp.Error),
		)
	}

	return result, nil
}

// parseSQLResponse парсит SQL ответ в один документ
func (c *Client) parseSQLResponse(resp Manticoresearch.SqlResponse, id string) (*domain.Document, error) {
	// SQL ответ может быть массивом объектов
	actual := resp.GetActualInstance()

	// Пробуем получить как массив
	if arr, ok := actual.([]map[string]interface{}); ok {
		if len(arr) == 0 {
			return nil, ports.ErrNotFound
		}
		if len(arr) > 0 {
			return c.mapToDocument(arr[0]), nil
		}
	}

	return nil, ports.ErrNotFound
}

// parseSQLResponseArray парсит SQL ответ в мапу документов
func (c *Client) parseSQLResponseArray(resp Manticoresearch.SqlResponse) (map[string]*domain.Document, error) {
	result := make(map[string]*domain.Document)

	actual := resp.GetActualInstance()

	if arr, ok := actual.([]map[string]interface{}); ok {
		for _, row := range arr {
			doc := c.mapToDocument(row)
			if doc != nil && doc.ID != "" {
				result[doc.ID] = doc
			}
		}
	}

	return result, nil
}

// mapToDocument конвертирует map из SQL ответа в Document
func (c *Client) mapToDocument(data map[string]interface{}) *domain.Document {
	doc := &domain.Document{}

	// ID может быть в разных форматах
	if id, ok := data["id"]; ok {
		doc.ID = fmt.Sprintf("%v", id)
	}

	// Основные поля
	if val, ok := data["source"]; ok && val != nil {
		doc.Source = fmt.Sprintf("%v", val)
	}
	if val, ok := data["genre"]; ok && val != nil {
		doc.Genre = fmt.Sprintf("%v", val)
	}
	if val, ok := data["author"]; ok && val != nil {
		doc.Author = fmt.Sprintf("%v", val)
	}
	if val, ok := data["title"]; ok && val != nil {
		doc.Title = fmt.Sprintf("%v", val)
	}
	if val, ok := data["content"]; ok && val != nil {
		doc.Content = fmt.Sprintf("%v", val)
	}
	if val, ok := data["geohashes_string"]; ok && val != nil {
		doc.GeohashesString = fmt.Sprintf("%v", val)
	}
	if val, ok := data["source_uuid"]; ok && val != nil {
		doc.SourceUUID = fmt.Sprintf("%v", val)
	}
	if val, ok := data["language"]; ok && val != nil {
		doc.Language = fmt.Sprintf("%v", val)
	}

	// Числовые поля
	if val, ok := data["chunk"].(float64); ok {
		doc.Chunk = uint(val)
	}
	if val, ok := data["char_count"].(float64); ok {
		doc.CharCount = uint(val)
	}
	if val, ok := data["word_count"].(float64); ok {
		doc.WordCount = uint(val)
	}
	if val, ok := data["datetime"].(float64); ok {
		doc.DateTime = int64(val)
	}
	if val, ok := data["created_at"].(float64); ok {
		doc.CreatedAt = int64(val)
	}
	if val, ok := data["updated_at"].(float64); ok {
		doc.UpdatedAt = int64(val)
	}

	// MVA поле geohashes_uint64 - может быть в разных форматах
	if val, ok := data["geohashes_uint64"]; ok && val != nil {
		switch v := val.(type) {
		case []interface{}:
			doc.GeohashesUint64 = make([]int64, len(v))
			for i, item := range v {
				if num, ok := item.(float64); ok {
					doc.GeohashesUint64[i] = int64(num)
				}
			}
		case string:
			// Иногда MVA может приходить как строка
			// TODO: парсить строку
		}
	}

	return doc
}

// splitIntoBatches разбивает слайс документов на батчи указанного размера
func (c *Client) splitIntoBatches(docs []*domain.Document, batchSize int) [][]*domain.Document {
	var batches [][]*domain.Document

	for i := 0; i < len(docs); i += batchSize {
		end := i + batchSize
		if end > len(docs) {
			end = len(docs)
		}
		batches = append(batches, docs[i:end])
	}

	return batches
}

// withRetry выполняет операцию с повторными попытками
func (c *Client) withRetry(ctx context.Context, operation func() error) error {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = c.config.RetryDelay
	expBackoff.MaxInterval = c.config.RetryDelay * 5
	expBackoff.MaxElapsedTime = c.config.Timeout

	return backoff.Retry(func() error {
		select {
		case <-ctx.Done():
			return backoff.Permanent(ctx.Err())
		default:
			return operation()
		}
	}, backoff.WithContext(expBackoff, ctx))
}

// Close закрывает соединение с Manticore
func (c *Client) Close() error {
	c.logger.Info("closing manticore connection")
	return nil
}
