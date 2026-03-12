// internal/adapters/manticore/ner_repository.go
package manticore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	Manticoresearch "github.com/manticoresoftware/manticoresearch-go"
	"github.com/terratensor/geoupdater/internal/core/domain"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

// ManticoreNERRepository реализует ports.NERRepository для Manticore
type ManticoreNERRepository struct {
	client    *Client
	tableName string
	logger    ports.Logger
	apiClient *Manticoresearch.APIClient
}

// NewNERRepository создает NER репозиторий
func NewNERRepository(client *Client, baseTableName string, logger ports.Logger) *ManticoreNERRepository {
	return &ManticoreNERRepository{
		client:    client,
		tableName: baseTableName + "_ner",
		logger:    logger,
		apiClient: client.apiClient,
	}
}

// tableExists проверяет существование таблицы через SHOW CREATE TABLE
func (r *ManticoreNERRepository) tableExists(ctx context.Context) (bool, error) {
	showCreateTableQuery := fmt.Sprintf("SHOW CREATE TABLE %s", r.tableName)
	req := r.apiClient.UtilsAPI.Sql(ctx).Body(showCreateTableQuery)
	req = req.RawResponse(true)

	_, _, err := r.apiClient.UtilsAPI.SqlExecute(req)

	if err == nil {
		return true, nil
	}

	// Если ошибка - таблицы нет
	return false, nil
}

// EnsureTable создает таблицу если не существует
func (r *ManticoreNERRepository) EnsureTable(ctx context.Context) error {
	// Проверяем существование таблицы
	exists, err := r.tableExists(ctx)
	if err != nil {
		r.logger.Debug("error checking table existence", ports.Error(err))
	}

	if exists {
		r.logger.Info("NER table already exists", ports.String("table", r.tableName))
		return nil
	}

	// Создаем таблицу
	createQuery := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            doc_id bigint,
            ner_loc json,
            ner_per json,
            ner_org json,
            created_at timestamp,
            updated_at timestamp
        ) engine='rowwise'`, r.tableName)

	r.logger.Info("creating NER table", ports.String("table", r.tableName))

	req := r.apiClient.UtilsAPI.Sql(ctx).Body(createQuery)
	req = req.RawResponse(true)

	_, _, err = r.apiClient.UtilsAPI.SqlExecute(req)
	if err != nil {
		return fmt.Errorf("failed to create NER table: %w", err)
	}

	r.logger.Info("created NER table", ports.String("table", r.tableName))
	return nil
}

// GetDocument получает NER документ по doc_id через JSON API
func (r *ManticoreNERRepository) GetDocument(ctx context.Context, docID uint64) (*domain.NERDocument, error) {
	request := map[string]interface{}{
		"table": r.tableName,
		"query": map[string]interface{}{
			"equals": map[string]interface{}{
				"doc_id": docID,
			},
		},
		"limit": 1,
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("http://%s:%d/search", r.client.config.Host, r.client.config.Port),
		bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := r.client.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode == 404 {
		return nil, ports.ErrNotFound
	}

	if httpResp.StatusCode != 200 {
		body, _ := io.ReadAll(httpResp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s",
			httpResp.StatusCode, string(body))
	}

	var searchResp struct {
		Hits struct {
			Hits []struct {
				ID     json.Number        `json:"_id"`
				Source domain.NERDocument `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	decoder := json.NewDecoder(httpResp.Body)
	decoder.UseNumber()

	if err := decoder.Decode(&searchResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if len(searchResp.Hits.Hits) == 0 {
		return nil, ports.ErrNotFound
	}

	hit := searchResp.Hits.Hits[0]
	doc := &hit.Source

	// Устанавливаем ID документа из _id
	if id, err := strconv.ParseUint(string(hit.ID), 10, 64); err == nil {
		doc.ID = id
	}

	r.logger.Debug("got NER document",
		ports.Uint64("doc_id", doc.DocID),
		ports.Uint64("id", doc.ID))

	return doc, nil
}

// UpdateDocument обновляет или вставляет NER документ
func (r *ManticoreNERRepository) UpdateDocument(ctx context.Context, doc *domain.NERDocument) error {
	// Проверяем существование документа
	existing, err := r.GetDocument(ctx, doc.DocID)
	if err != nil && err != ports.ErrNotFound {
		return err
	}

	now := time.Now().Unix()

	if existing != nil {
		// UPDATE существующего документа
		doc.ID = existing.ID
		doc.CreatedAt = existing.CreatedAt
		doc.UpdatedAt = now

		updateDoc := map[string]interface{}{
			"ner_loc":    doc.Location,
			"ner_per":    doc.Person,
			"ner_org":    doc.Org,
			"updated_at": doc.UpdatedAt,
		}

		updateRequest := Manticoresearch.NewUpdateDocumentRequest(r.tableName, updateDoc)
		updateRequest.SetId(doc.ID)

		_, _, err = r.apiClient.IndexAPI.Update(ctx).UpdateDocumentRequest(*updateRequest).Execute()
		return err
	}

	// INSERT нового документа
	doc.CreatedAt = now
	doc.UpdatedAt = now

	insertRequest := Manticoresearch.NewInsertDocumentRequest(r.tableName, doc.ToMap())
	_, _, err = r.apiClient.IndexAPI.Insert(ctx).InsertDocumentRequest(*insertRequest).Execute()
	return err
}

// getExistingDocs получает мапу существующих документов через JSON API
func (r *ManticoreNERRepository) getExistingDocs(ctx context.Context, docs []*domain.NERDocument) (map[uint64]uint64, error) {
	if len(docs) == 0 {
		return make(map[uint64]uint64), nil
	}

	// Формируем список doc_id для поиска
	docIDs := make([]interface{}, len(docs))
	for i, doc := range docs {
		docIDs[i] = doc.DocID
	}

	request := map[string]interface{}{
		"table": r.tableName,
		"query": map[string]interface{}{
			"in": map[string]interface{}{
				"doc_id": docIDs,
			},
		},
		"limit": len(docs),
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("http://%s:%d/search", r.client.config.Host, r.client.config.Port),
		bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := r.client.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != 200 {
		body, _ := io.ReadAll(httpResp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s",
			httpResp.StatusCode, string(body))
	}

	var searchResp struct {
		Hits struct {
			Hits []struct {
				ID     json.Number `json:"_id"`
				Source struct {
					DocID uint64 `json:"doc_id"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	decoder := json.NewDecoder(httpResp.Body)
	decoder.UseNumber()

	if err := decoder.Decode(&searchResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	existingDocs := make(map[uint64]uint64)
	for _, hit := range searchResp.Hits.Hits {
		if id, err := strconv.ParseUint(string(hit.ID), 10, 64); err == nil {
			existingDocs[hit.Source.DocID] = id
			r.logger.Debug("found existing doc",
				ports.Uint64("doc_id", hit.Source.DocID),
				ports.Uint64("id", id))
		}
	}

	r.logger.Debug("found existing NER docs",
		ports.Int("total", len(existingDocs)))

	return existingDocs, nil
}

// BulkUpdate массовое обновление NER документов
func (r *ManticoreNERRepository) BulkUpdate(ctx context.Context, docs []*domain.NERDocument) (*domain.BatchResult, error) {
	result := domain.NewBatchResult()

	if len(docs) == 0 {
		return result, nil
	}

	r.logger.Info("starting NER bulk update",
		ports.Int("batch_size", len(docs)))

	// Получаем существующие документы
	existingDocs, err := r.getExistingDocs(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing docs: %w", err)
	}

	// Формируем bulk запрос
	var bulkLines []string
	now := time.Now().Unix()

	for _, doc := range docs {
		doc.UpdatedAt = now
		if doc.CreatedAt == 0 {
			doc.CreatedAt = now
		}

		if existingID, ok := existingDocs[doc.DocID]; ok {
			// UPDATE существующего документа
			doc.ID = existingID

			updateOp := map[string]interface{}{
				"update": map[string]interface{}{
					"index": r.tableName,
					"id":    existingID,
					"doc": map[string]interface{}{
						"ner_loc":    doc.Location,
						"ner_per":    doc.Person,
						"ner_org":    doc.Org,
						"updated_at": doc.UpdatedAt,
					},
				},
			}

			jsonLine, err := json.Marshal(updateOp)
			if err != nil {
				result.AddFailed(doc.DocID, fmt.Errorf("failed to marshal update: %w", err),
					domain.ErrorTypeValidation, 1)
				continue
			}
			bulkLines = append(bulkLines, string(jsonLine))

			r.logger.Debug("preparing update",
				ports.Uint64("doc_id", doc.DocID),
				ports.Uint64("id", existingID))

		} else {
			// INSERT нового документа
			insertOp := map[string]interface{}{
				"insert": map[string]interface{}{
					"index": r.tableName,
					"doc":   doc.ToMap(),
				},
			}

			jsonLine, err := json.Marshal(insertOp)
			if err != nil {
				result.AddFailed(doc.DocID, fmt.Errorf("failed to marshal insert: %w", err),
					domain.ErrorTypeValidation, 1)
				continue
			}
			bulkLines = append(bulkLines, string(jsonLine))

			r.logger.Debug("preparing insert",
				ports.Uint64("doc_id", doc.DocID))
		}
	}

	if len(bulkLines) == 0 {
		return result, nil
	}

	bulkBody := strings.Join(bulkLines, "\n") + "\n"

	// Логируем пример запроса
	if len(bulkLines) > 0 {
		r.logger.Debug("NER bulk request sample",
			ports.String("sample", bulkLines[0]))
	}

	// Отправляем bulk запрос
	bulkResp, httpResp, err := r.apiClient.IndexAPI.Bulk(ctx).Body(bulkBody).Execute()

	if err != nil {
		if httpResp != nil {
			body, _ := io.ReadAll(httpResp.Body)
			return nil, fmt.Errorf("bulk request failed (status %d): %s",
				httpResp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("bulk request failed: %w", err)
	}

	// Анализируем ответ
	if bulkResp.Items != nil {
		for _, item := range bulkResp.Items {
			// Проверяем update операции
			if update, ok := item["update"]; ok {
				if updateMap, ok := update.(map[string]interface{}); ok {
					if resultStr, ok := updateMap["result"]; ok {
						if resultStr == "updated" {
							result.AddSuccess()
						}
					}
				}
			}
			// Проверяем insert операции
			if insert, ok := item["insert"]; ok {
				if insertMap, ok := insert.(map[string]interface{}); ok {
					if resultStr, ok := insertMap["result"]; ok {
						if resultStr == "created" {
							result.AddSuccess()
						}
					}
				}
			}
		}
	}

	if bulkResp.Errors != nil && *bulkResp.Errors {
		r.logger.Warn("NER bulk operation had errors",
			ports.Any("error", bulkResp.Error))
	}

	r.logger.Info("NER bulk update completed",
		ports.Int("total", result.Total),
		ports.Int("success", result.Success),
		ports.Int("failed", result.Failed))

	return result, nil
}
