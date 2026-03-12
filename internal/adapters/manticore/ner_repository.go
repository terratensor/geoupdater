// internal/adapters/manticore/ner_repository.go
package manticore

import (
	"context"
	"encoding/json"
	"fmt"
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

// EnsureTable создает таблицу если не существует
func (r *ManticoreNERRepository) EnsureTable(ctx context.Context) error {
	// Проверяем существование таблицы
	query := fmt.Sprintf("SHOW TABLES LIKE '%s'", r.tableName)
	resp, _, err := r.apiClient.UtilsAPI.Sql(ctx).Body(query).Execute()
	if err == nil && resp != nil {
		// Таблица существует
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

	_, _, err = r.apiClient.UtilsAPI.Sql(ctx).Body(createQuery).Execute()
	if err != nil {
		return fmt.Errorf("failed to create NER table: %w", err)
	}

	r.logger.Info("created NER table", ports.String("table", r.tableName))
	return nil
}

// GetDocument получает NER документ по doc_id
func (r *ManticoreNERRepository) GetDocument(ctx context.Context, docID uint64) (*domain.NERDocument, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE doc_id = %d LIMIT 1", r.tableName, docID)

	resp, _, err := r.apiClient.UtilsAPI.Sql(ctx).Body(query).RawResponse(false).Execute()
	if err != nil {
		return nil, err
	}

	return r.parseNERResponse(*resp)
}

// UpdateDocument обновляет или вставляет NER документ
func (r *ManticoreNERRepository) UpdateDocument(ctx context.Context, doc *domain.NERDocument) error {
	// Сначала пробуем найти существующий документ
	existing, err := r.GetDocument(ctx, doc.DocID)
	if err != nil && err != ports.ErrNotFound {
		return err
	}

	if existing != nil {
		// Обновляем существующий
		doc.ID = existing.ID
		doc.CreatedAt = existing.CreatedAt
		doc.UpdatedAt = time.Now().Unix()

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
	doc.CreatedAt = time.Now().Unix()
	doc.UpdatedAt = doc.CreatedAt

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

	// Формируем bulk запрос
	var bulkLines []string
	now := time.Now().Unix()

	for _, doc := range docs {
		if existingID, ok := existingDocs[doc.DocID]; ok {
			// UPDATE существующего документа
			doc.ID = existingID
			doc.UpdatedAt = now
			// created_at не меняем

			updateOp := map[string]interface{}{
				"update": map[string]interface{}{
					"index": r.tableName,
					"id":    doc.ID,
					"doc": map[string]interface{}{
						"ner_loc":    doc.Location,
						"ner_per":    doc.Person,
						"ner_org":    doc.Org,
						"updated_at": doc.UpdatedAt,
					},
				},
			}
			jsonLine, _ := json.Marshal(updateOp)
			bulkLines = append(bulkLines, string(jsonLine))
		} else {
			// INSERT нового документа
			doc.CreatedAt = now
			doc.UpdatedAt = now

			insertOp := map[string]interface{}{
				"insert": map[string]interface{}{
					"index": r.tableName,
					"doc":   doc.ToMap(),
				},
			}
			jsonLine, _ := json.Marshal(insertOp)
			bulkLines = append(bulkLines, string(jsonLine))
		}
	}

	if len(bulkLines) == 0 {
		return result, nil
	}

	bulkBody := strings.Join(bulkLines, "\n") + "\n"

	// Логируем пример запроса для отладки
	if len(bulkLines) > 0 {
		r.logger.Debug("NER bulk request sample",
			ports.String("sample", bulkLines[0]))
	}

	bulkResp, _, err := r.apiClient.IndexAPI.Bulk(ctx).Body(bulkBody).Execute()
	if err != nil {
		return nil, fmt.Errorf("bulk request failed: %w", err)
	}

	// Анализируем ответ
	if bulkResp.Items != nil {
		for _, item := range bulkResp.Items {
			// Проверяем оба типа операций
			for _, op := range []string{"update", "insert"} {
				if data, ok := item[op]; ok {
					if dataMap, ok := data.(map[string]interface{}); ok {
						if resultStr, ok := dataMap["result"]; ok {
							if resultStr == "updated" || resultStr == "created" {
								result.AddSuccess()
							} else {
								// Извлекаем ID для failed записи
								var docID uint64
								if id, ok := dataMap["_id"]; ok {
									switch v := id.(type) {
									case float64:
										docID = uint64(v)
									}
								}
								result.AddFailed(docID,
									fmt.Errorf("%s failed: %v", op, resultStr),
									domain.ErrorTypeManticore, 1)
							}
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

// getExistingDocs получает мапу существующих документов по doc_id
func (r *ManticoreNERRepository) getExistingDocs(ctx context.Context, docs []*domain.NERDocument) (map[uint64]uint64, error) {
	if len(docs) == 0 {
		return make(map[uint64]uint64), nil
	}

	// Формируем список doc_id для запроса
	docIDs := make([]string, len(docs))
	for i, doc := range docs {
		docIDs[i] = fmt.Sprintf("%d", doc.DocID)
	}

	query := fmt.Sprintf("SELECT id, doc_id FROM %s WHERE doc_id IN (%s)",
		r.tableName, strings.Join(docIDs, ","))

	resp, _, err := r.apiClient.UtilsAPI.Sql(ctx).Body(query).RawResponse(false).Execute()
	if err != nil {
		return nil, err
	}

	existingDocs := make(map[uint64]uint64)
	actual := resp.GetActualInstance()

	if arr, ok := actual.([]map[string]interface{}); ok {
		for _, row := range arr {
			var id, docID uint64
			if idVal, ok := row["id"].(float64); ok {
				id = uint64(idVal)
			}
			if docIDVal, ok := row["doc_id"].(float64); ok {
				docID = uint64(docIDVal)
			}
			if id > 0 && docID > 0 {
				existingDocs[docID] = id
			}
		}
	}

	r.logger.Debug("found existing NER docs",
		ports.Int("total", len(existingDocs)))

	return existingDocs, nil
}

// parseNERResponse парсит SQL ответ в NERDocument
func (r *ManticoreNERRepository) parseNERResponse(resp Manticoresearch.SqlResponse) (*domain.NERDocument, error) {
	actual := resp.GetActualInstance()

	if arr, ok := actual.([]map[string]interface{}); ok && len(arr) > 0 {
		row := arr[0]
		doc := &domain.NERDocument{}

		if id, ok := row["id"].(float64); ok {
			doc.ID = uint64(id)
		}
		if docID, ok := row["doc_id"].(float64); ok {
			doc.DocID = uint64(docID)
		}
		if loc, ok := row["ner_loc"].(string); ok {
			doc.Location = loc
		}
		if per, ok := row["ner_per"].(string); ok {
			doc.Person = per
		}
		if org, ok := row["ner_org"].(string); ok {
			doc.Org = org
		}
		if createdAt, ok := row["created_at"].(float64); ok {
			doc.CreatedAt = int64(createdAt)
		}
		if updatedAt, ok := row["updated_at"].(float64); ok {
			doc.UpdatedAt = int64(updatedAt)
		}

		return doc, nil
	}

	return nil, ports.ErrNotFound
}
