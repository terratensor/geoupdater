// internal/adapters/manticore/client.go
package manticore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	Manticoresearch "github.com/manticoresoftware/manticoresearch-go"
	"github.com/terratensor/geoupdater/internal/core/domain"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

// Client реализует ports.Repository для Manticore Search
type Client struct {
	apiClient  *Manticoresearch.APIClient
	config     *Config
	logger     ports.Logger
	metrics    ports.MetricsCollector
	tableName  string
	httpClient *http.Client
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

	// Создаем HTTP клиент с таймаутом
	httpClient := &http.Client{
		Timeout: cfg.Timeout,
	}

	// Создаем конфигурацию для Manticore клиента
	configuration := Manticoresearch.NewConfiguration()
	configuration.HTTPClient = httpClient
	configuration.Servers[0].URL = fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port)
	configuration.UserAgent = "GeoUpdater/1.0"

	apiClient := Manticoresearch.NewAPIClient(configuration)

	client := &Client{
		apiClient:  apiClient,
		config:     cfg,
		logger:     logger,
		metrics:    metrics,
		tableName:  cfg.TableName,
		httpClient: httpClient,
	}

	// Проверяем соединение
	if err := client.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to connect to Manticore: %w", err)
	}

	logger.Info("connected to Manticore",
		ports.String("host", cfg.Host),
		ports.Int("port", cfg.Port),
		ports.String("table", cfg.TableName),
		ports.String("url", configuration.Servers[0].URL),
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

// ReplaceDocument заменяет документ полностью через REPLACE
func (c *Client) ReplaceDocument(ctx context.Context, doc *domain.Document) error {
	start := time.Now()

	operation := func() error {
		return c.replaceDocument(ctx, doc)
	}

	err := c.withRetry(ctx, operation)

	c.metrics.RecordManticoreOperation("replace", time.Since(start), err)

	if err != nil {
		return fmt.Errorf("failed to replace document %d: %w", doc.ID, err)
	}

	return nil
}

// replaceDocument внутренний метод для замены документа через JSON REPLACE
func (c *Client) replaceDocument(ctx context.Context, doc *domain.Document) error {
	// Создаем запрос на replace через JSON API
	docMap := doc.ToMap()

	// Для JSON API ID должен быть в поле id верхнего уровня, не в doc
	delete(docMap, "id") // убираем id из doc

	replaceRequest := Manticoresearch.NewInsertDocumentRequest(c.tableName, docMap)
	replaceRequest.SetId(doc.ID) // ID передаем отдельно как uint64

	_, _, err := c.apiClient.IndexAPI.Replace(ctx).InsertDocumentRequest(*replaceRequest).Execute()
	if err != nil {
		return fmt.Errorf("replace failed: %w", err)
	}

	c.logger.Debug("document replaced",
		ports.Uint64("id", doc.ID),
		ports.Int("geohashes_count", doc.GetGeohashCount()),
	)

	return nil
}

// internal/adapters/manticore/client.go - исправляем использование AddFailed
func (c *Client) BulkReplace(ctx context.Context, docs []*domain.Document) (*domain.BatchResult, error) {
	start := time.Now()
	result := domain.NewBatchResult()

	if len(docs) == 0 {
		return result, nil
	}

	c.logger.Info("starting bulk replace",
		ports.Int("batch_size", len(docs)),
	)

	// Разбиваем на батчи
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

// bulkReplaceBatch исправляем
func (c *Client) bulkReplaceBatch(ctx context.Context, docs []*domain.Document) (*domain.BatchResult, error) {
	result := domain.NewBatchResult()

	var bulkLines []string

	for _, doc := range docs {
		docMap := doc.ToMap()
		delete(docMap, "id")

		replaceOp := map[string]interface{}{
			"replace": map[string]interface{}{
				"index": c.tableName,
				"id":    doc.ID,
				"doc":   docMap,
			},
		}

		jsonLine, err := json.Marshal(replaceOp)
		if err != nil {
			result.AddFailed(doc.ID, fmt.Errorf("failed to marshal: %w", err),
				domain.ErrorTypeValidation, 1)
			continue
		}

		bulkLines = append(bulkLines, string(jsonLine))
	}

	if len(bulkLines) == 0 {
		return result, nil
	}

	bulkBody := strings.Join(bulkLines, "\n") + "\n"

	bulkResp, _, err := c.apiClient.IndexAPI.Bulk(ctx).Body(bulkBody).Execute()
	if err != nil {
		return nil, fmt.Errorf("bulk request failed: %w", err)
	}

	if bulkResp.Items != nil {
		for _, item := range bulkResp.Items {
			if replace, ok := item["replace"]; ok {
				if replaceMap, ok := replace.(map[string]interface{}); ok {
					if resultStr, ok := replaceMap["result"]; ok {
						if resultStr == "updated" || resultStr == "created" {
							result.AddSuccess()
						} else {
							// Извлекаем ID из ответа
							var docID uint64
							if id, ok := replaceMap["_id"]; ok {
								switch v := id.(type) {
								case float64:
									docID = uint64(v)
								case uint64:
									docID = v
								}
							}
							if docID == 0 && len(docs) > len(result.Errors) {
								// fallback - используем ID из оригинального документа
								docID = docs[len(result.Errors)].ID
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

	if bulkResp.Errors != nil && *bulkResp.Errors {
		c.logger.Warn("bulk operation had errors",
			ports.Any("error", bulkResp.Error),
		)
	}

	return result, nil
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
