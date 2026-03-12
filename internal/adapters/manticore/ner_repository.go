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
	// Проверяем существование таблицы через SHOW CREATE TABLE
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

// GetDocument получает NER документ по doc_id
func (r *ManticoreNERRepository) GetDocument(ctx context.Context, docID uint64) (*domain.NERDocument, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE doc_id = %d LIMIT 1", r.tableName, docID)

	req := r.apiClient.UtilsAPI.Sql(ctx).Body(query)
	req = req.RawResponse(false)

	resp, _, err := r.apiClient.UtilsAPI.SqlExecute(req)
	if err != nil {
		return nil, err
	}

	// Парсим ответ как массив
	if resp != nil {
		actual := resp.GetActualInstance()
		if arr, ok := actual.([]map[string]interface{}); ok && len(arr) > 0 {
			row := arr[0]
			doc := &domain.NERDocument{}

			if id, ok := row["id"].(float64); ok {
				doc.ID = uint64(id)
			}
			if d, ok := row["doc_id"].(float64); ok {
				doc.DocID = uint64(d)
			}

			// Парсим JSON строки обратно в []NEREntity
			if locStr, ok := row["ner_loc"].(string); ok && locStr != "" && locStr != "null" {
				var entities []domain.NEREntity
				if err := json.Unmarshal([]byte(locStr), &entities); err == nil {
					doc.Location = entities
				}
			}

			if perStr, ok := row["ner_per"].(string); ok && perStr != "" && perStr != "null" {
				var entities []domain.NEREntity
				if err := json.Unmarshal([]byte(perStr), &entities); err == nil {
					doc.Person = entities
				}
			}

			if orgStr, ok := row["ner_org"].(string); ok && orgStr != "" && orgStr != "null" {
				var entities []domain.NEREntity
				if err := json.Unmarshal([]byte(orgStr), &entities); err == nil {
					doc.Org = entities
				}
			}

			if createdAt, ok := row["created_at"].(float64); ok {
				doc.CreatedAt = int64(createdAt)
			}
			if updatedAt, ok := row["updated_at"].(float64); ok {
				doc.UpdatedAt = int64(updatedAt)
			}

			return doc, nil
		}
	}

	return nil, ports.ErrNotFound
}

// UpdateDocument обновляет или вставляет NER документ
func (r *ManticoreNERRepository) UpdateDocument(ctx context.Context, doc *domain.NERDocument) error {
	// Сначала пробуем найти существующий документ
	existing, err := r.GetDocument(ctx, doc.DocID)
	if err != nil && err != ports.ErrNotFound {
		return err
	}

	now := time.Now().Unix()

	if existing != nil {
		// Обновляем существующий
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

	// Вставляем новый
	doc.CreatedAt = now
	doc.UpdatedAt = now

	insertRequest := Manticoresearch.NewInsertDocumentRequest(r.tableName, doc.ToMap())
	_, _, err = r.apiClient.IndexAPI.Insert(ctx).InsertDocumentRequest(*insertRequest).Execute()
	return err
}

// BulkUpdate массовое обновление NER документов
func (r *ManticoreNERRepository) BulkUpdate(ctx context.Context, docs []*domain.NERDocument) (*domain.BatchResult, error) {
	result := domain.NewBatchResult()

	if len(docs) == 0 {
		return result, nil
	}

	r.logger.Info("starting NER bulk update",
		ports.Int("batch_size", len(docs)))

	// Сначала получаем все существующие документы по doc_id
	existingDocs, err := r.getExistingDocs(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing docs: %w", err)
	}

	// Формируем NDJSON для bulk запроса
	var bulkLines []string
	now := time.Now().Unix()

	for _, doc := range docs {
		// Устанавливаем временные метки
		doc.UpdatedAt = now
		if doc.CreatedAt == 0 {
			doc.CreatedAt = now
		}

		if existingID, ok := existingDocs[doc.DocID]; ok {
			// UPDATE существующего документа
			doc.ID = existingID
			// Для update нужны только изменяемые поля
			updateDoc := map[string]interface{}{
				"ner_loc":    doc.Location, // Прямая передача слайса!
				"ner_per":    doc.Person,
				"ner_org":    doc.Org,
				"updated_at": doc.UpdatedAt,
			}

			updateOp := map[string]interface{}{
				"update": map[string]interface{}{
					"index": r.tableName,
					"id":    existingID,
					"doc":   updateDoc,
				},
			}
			jsonLine, _ := json.Marshal(updateOp)
			bulkLines = append(bulkLines, string(jsonLine))
		} else {
			// INSERT нового документа - используем ToMap()
			insertOp := map[string]interface{}{
				"insert": map[string]interface{}{
					"index": r.tableName,
					"doc":   doc.ToMap(), // ToMap теперь возвращает правильную структуру
				},
			}
			jsonLine, _ := json.Marshal(insertOp)
			bulkLines = append(bulkLines, string(jsonLine))
		}
	}

	bulkBody := strings.Join(bulkLines, "\n") + "\n"

	// Логируем пример запроса для отладки
	if len(bulkLines) > 0 {
		r.logger.Debug("NER bulk request sample",
			ports.String("sample", bulkLines[0]))
	}

	// Отправляем через официальный клиент
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
			if update, ok := item["update"]; ok {
				if updateMap, ok := update.(map[string]interface{}); ok {
					if resultStr, ok := updateMap["result"]; ok {
						if resultStr == "updated" {
							result.AddSuccess()
						}
					}
				}
			} else if insert, ok := item["insert"]; ok {
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

// getExistingDocs получает существующие документы
func (r *ManticoreNERRepository) getExistingDocs(ctx context.Context, docs []*domain.NERDocument) (map[uint64]uint64, error) {
	if len(docs) == 0 {
		return make(map[uint64]uint64), nil
	}

	// Формируем список doc_id
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

	jsonData, _ := json.Marshal(request)

	httpReq, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("http://%s:%d/search", r.client.config.Host, r.client.config.Port),
		bytes.NewReader(jsonData))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := r.client.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

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

	if err := json.NewDecoder(httpResp.Body).Decode(&searchResp); err != nil {
		return nil, err
	}

	existingDocs := make(map[uint64]uint64)
	for _, hit := range searchResp.Hits.Hits {
		id, _ := strconv.ParseUint(string(hit.ID), 10, 64)
		existingDocs[hit.Source.DocID] = id
	}

	return existingDocs, nil
}
