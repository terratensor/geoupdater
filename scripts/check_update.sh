#!/bin/bash
# scripts/check_update.sh

echo "Проверка обновления документов..."

# Получаем документ до обновления
echo "До обновления:"
curl -s -X POST http://localhost:9308/search \
  -H "Content-Type: application/json" \
  -d '{
    "table": "library2026",
    "query": { "equals": { "id": 6056452479959171091 } },
    "limit": 1
  }' | jq '.hits.hits[0]._source.geohashes_string'

# Запускаем обновление
echo "Запуск обновления..."
./build/geoupdater -dir ./data -mode merge

# Получаем документ после обновления
echo "После обновления:"
curl -s -X POST http://localhost:9308/search \
  -H "Content-Type: application/json" \
  -d '{
    "table": "library2026",
    "query": { "equals": { "id": 6056452479959171091 } },
    "limit": 1
  }' | jq '.hits.hits[0]._source.geohashes_string'