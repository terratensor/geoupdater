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

// internal/adapters/manticore/ner_repository.go - исправляем парсинг ID

// getExistingDocs получает мапу существующих документов через JSON API
func (r *ManticoreNERRepository) getExistingDocs(ctx context.Context, docs []*domain.NERDocument) (map[uint64]uint64, error) {
	if len(docs) == 0 {
		return make(map[uint64]uint64), nil
	}

	// Формируем список doc_id для поиска
	docIDs := make([]uint64, len(docs))
	for i, doc := range docs {
		docIDs[i] = doc.DocID
	}

	r.logger.Debug("searching for existing NER docs",
		ports.Int("count", len(docIDs)),
		ports.Any("first_few", docIDs[:min(5, len(docIDs))]))

	// Конвертируем в []interface{} для JSON
	idValues := make([]interface{}, len(docIDs))
	for i, id := range docIDs {
		idValues[i] = id
	}

	request := map[string]interface{}{
		"table": r.tableName,
		"query": map[string]interface{}{
			"in": map[string]interface{}{
				"doc_id": idValues,
			},
		},
		"_source": []string{"doc_id"},
		"limit":   len(docIDs),
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

	bodyBytes, _ := io.ReadAll(httpResp.Body)

	if httpResp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected status code: %d, body: %s",
			httpResp.StatusCode, string(bodyBytes))
	}

	var searchResp struct {
		Hits struct {
			Hits []struct {
				ID     json.Number `json:"_id"` // Важно: используем json.Number!
				Source struct {
					DocID uint64 `json:"doc_id"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
	decoder.UseNumber() // Критически важно для сохранения точности!

	if err := decoder.Decode(&searchResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w, body: %s", err, string(bodyBytes))
	}

	existingDocs := make(map[uint64]uint64)
	for _, hit := range searchResp.Hits.Hits {
		// Парсим json.Number в uint64 с сохранением точности
		id, err := strconv.ParseUint(string(hit.ID), 10, 64)
		if err != nil {
			r.logger.Warn("failed to parse Manticore ID",
				ports.String("raw_id", string(hit.ID)),
				ports.Error(err))
			continue
		}
		existingDocs[hit.Source.DocID] = id
		r.logger.Debug("found existing doc",
			ports.Uint64("doc_id", hit.Source.DocID),
			ports.Uint64("id", id))
	}

	r.logger.Info("found existing NER docs",
		ports.Int("total", len(existingDocs)),
		ports.Int("requested", len(docIDs)))

	return existingDocs, nil
}

// internal/adapters/manticore/ner_repository.go - добавляем логирование данных

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

	r.logger.Info("existing docs found",
		ports.Int("found", len(existingDocs)),
		ports.Int("total", len(docs)))

	// Формируем bulk запрос
	var bulkLines []string
	now := time.Now().Unix()
	var updateCount, insertCount int

	for i, doc := range docs {
		doc.UpdatedAt = now
		if doc.CreatedAt == 0 {
			doc.CreatedAt = now
		}

		// Логируем содержимое документа для отладки
		r.logger.Debug("document content",
			ports.Int("index", i),
			ports.Uint64("doc_id", doc.DocID),
			ports.Any("location", doc.Location),
			ports.Any("person", doc.Person),
			ports.Any("org", doc.Org))

		// internal/adapters/manticore/ner_repository.go - исправляем формирование UPDATE

		if existingID, ok := existingDocs[doc.DocID]; ok {
			updateCount++

			// Формируем doc с JSON полями как строки
			updateDoc := map[string]interface{}{}

			// Преобразуем массивы в JSON строки
			if doc.Location != nil {
				locBytes, _ := json.Marshal(doc.Location)
				updateDoc["ner_loc"] = string(locBytes)
			}
			if doc.Person != nil {
				perBytes, _ := json.Marshal(doc.Person)
				updateDoc["ner_per"] = string(perBytes)
			}
			if doc.Org != nil {
				orgBytes, _ := json.Marshal(doc.Org)
				updateDoc["ner_org"] = string(orgBytes)
			}
			updateDoc["updated_at"] = doc.UpdatedAt

			updateOp := map[string]interface{}{
				"update": map[string]interface{}{
					"table": r.tableName,
					"id":    existingID,
					"doc":   updateDoc,
				},
			}

			jsonLine, err := json.Marshal(updateOp)
			if err != nil {
				result.AddFailed(doc.DocID, fmt.Errorf("failed to marshal update: %w", err),
					domain.ErrorTypeValidation, 1)
				continue
			}

			r.logger.Debug("update request with stringified JSON",
				ports.Int("index", i),
				ports.String("json", string(jsonLine)))

			bulkLines = append(bulkLines, string(jsonLine))
		} else {
			insertCount++

			// INSERT нового документа
			docMap := doc.ToMap()
			delete(docMap, "id")

			insertOp := map[string]interface{}{
				"insert": map[string]interface{}{
					"table": r.tableName,
					"id":    doc.DocID,
					"doc":   docMap,
				},
			}

			jsonLine, err := json.Marshal(insertOp)
			if err != nil {
				result.AddFailed(doc.DocID, fmt.Errorf("failed to marshal insert: %w", err),
					domain.ErrorTypeValidation, 1)
				continue
			}

			r.logger.Debug("insert request",
				ports.Int("index", i),
				ports.String("json", string(jsonLine)))

			bulkLines = append(bulkLines, string(jsonLine))
		}
	}

	r.logger.Info("bulk operations prepared",
		ports.Int("updates", updateCount),
		ports.Int("inserts", insertCount))

	if len(bulkLines) == 0 {
		return result, nil
	}

	bulkBody := strings.Join(bulkLines, "\n") + "\n"

	// Логируем полное тело запроса
	r.logger.Debug("full bulk request", ports.String("body", bulkBody))

	// Отправляем bulk запрос
	bulkResp, httpResp, err := r.apiClient.IndexAPI.Bulk(ctx).Body(bulkBody).Execute()

	if err != nil {
		if httpResp != nil {
			body, _ := io.ReadAll(httpResp.Body)
			r.logger.Error("bulk error response",
				ports.Int("status", httpResp.StatusCode),
				ports.String("body", string(body)))
			return nil, fmt.Errorf("bulk request failed (status %d): %s",
				httpResp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("bulk request failed: %w", err)
	}

	// Логируем ответ
	respJSON, _ := json.Marshal(bulkResp)
	r.logger.Debug("bulk response", ports.String("response", string(respJSON)))

	// Анализируем ответ
	if bulkResp.Items != nil {
		for _, item := range bulkResp.Items {
			if update, ok := item["update"]; ok {
				if updateMap, ok := update.(map[string]interface{}); ok {
					if resultStr, ok := updateMap["result"]; ok {
						if resultStr == "updated" {
							result.AddSuccess()
						}
					}
				}
			}
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

	return result, nil
}
