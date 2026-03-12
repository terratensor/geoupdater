# GeoUpdater

Сервис для массового обновления геоданных в Manticore Search.

## 🚀 Возможности

- **Массовая обработка** NDJSON файлов с геоданными
- **Два режима обновления геоданных**: REPLACE (полная замена) и MERGE (слияние)
- **Режим NER** для обработки именованных сущностей в отдельную таблицу
- **Пакетная обработка** с настраиваемым размером батча
- **Параллельная обработка** нескольких файлов
- **Graceful shutdown** с сохранением прогресса
- **Детальные отчеты** о каждой обработке
- **Хранение failed записей** для повторной обработки
- **Поддержка больших чисел** (uint64) без потери точности

## 📋 Требования

- Go 1.21+
- Manticore Search 5.0.0+
- NDJSON файлы с геоданными

## 🔧 Установка

### Из исходников
```bash
git clone https://github.com/terratensor/geoupdater.git
cd geoupdater
make build
```

### Docker
```bash
docker build -t geoupdater:latest .
```

## Архитектура

Проект построен на гексагональной архитектуре (ports & adapters):

- **core/domain** - бизнес-логика и модели данных
- **core/ports** - интерфейсы для взаимодействия с внешним миром
- **adapters/** - реализации портов (Manticore, NDJSON, логгер, failed storage)
- **app/service** - координация всех компонентов

## Особенности работы с ID

**Критически важно:** В Manticore Search ID документов хранятся как 64-битные целые числа.
В нашем сервисе мы используем `uint64` для всех ID, чтобы избежать потери точности при парсинге JSON.

```go
type Document struct {
    ID uint64 `json:"id"` // ВАЖНО: всегда uint64, не string!
}

type GeoUpdateData struct {
    DocID uint64 `json:"doc_id"` // В JSON может приходить как строка или число
}
```

При парсинге NDJSON файлов мы используем `json.Number` для сохранения точности:
```go
decoder := json.NewDecoder(bytes.NewReader(line))
decoder.UseNumber() // Критически важно для больших чисел!
```

## Режимы работы

### 1. REPLACE (полная замена) - ПО УМОЛЧАНИЮ
```bash
./geoupdater -dir ./data -mode replace
```
Старые геоданные полностью заменяются новыми.

### 2. MERGE (слияние)
```bash
./geoupdater -dir ./data -mode merge
```
Новые геохеши добавляются к существующим, дубликаты удаляются.

### 3. NER режим (обработка именованных сущностей)
```bash
./geoupdater -ner -dir ./data -pattern "*.ndjson"
```
Обрабатывает NER данные в отдельную таблицу `{основная_таблица}_ner` (например, `library2026_ner`).

Формат входного файла:
```json
{
  "doc_id": "6056452479959192749",
  "ner_loc": [{"value": "Египет", "start_pos": 769, "end_pos": 775, "geohash": ["1443f4"], "confidence": 1}],
  "ner_per": [{"value": "Плутарх", "start_pos": 839, "end_pos": 846, "geohash": [], "confidence": 0.6273}],
  "ner_org": []
}
```

Структура создаваемой таблицы:
```sql
CREATE TABLE library2026_ner (
    doc_id bigint,
    ner_loc json,
    ner_per json,
    ner_org json,
    created_at timestamp,
    updated_at timestamp
) engine='rowwise'
```

## Конфигурация

### Переменные окружения (.env)

```env
# Manticore connection
MANTICORE_HOST=localhost
MANTICORE_PORT=9308
MANTICORE_TABLE=library2026
MANTICORE_TIMEOUT=30s
MANTICORE_MAX_CONNS=10

# Processing
UPDATE_MODE=merge              # replace или merge
BATCH_SIZE=1000                # документов в батче
WORKERS=5                      # параллельных воркеров
MAX_RETRIES=3                   # попыток при ошибке
RETRY_DELAY=1s                  # задержка между попытками

# NER specific
NER_BATCH_SIZE=1000             # размер батча для NER
NER_WORKERS=5                   # воркеров для NER

# Files
INPUT_DIR=./data
FILE_PATTERN=*.ndjson
FAILED_DIR=./failed
REPORTS_DIR=./reports

# Logging
LOG_LEVEL=info                  # debug, info, warn, error
LOG_FILE=./logs/geoupdater.log
```

### Флаги командной строки

| Флаг | Описание | Пример |
|------|----------|--------|
| `-dir` | Директория с файлами | `-dir ./data` |
| `-files` | Конкретные файлы (через запятую) | `-files file1.ndjson,file2.ndjson` |
| `-pattern` | Маска файлов | `-pattern "*.ndjson"` |
| `-mode` | Режим обновления геоданных | `-mode merge` |
| `-ner` | Режим обработки NER | `-ner` |
| `-reprocess` | Повторная обработка failed записей | `-reprocess` |
| `-reports` | Директория для отчетов | `-reports ./reports` |
| `-version` | Версия | `-version` |

## Детали реализации

### Парсер NDJSON

- Использует потоковое чтение (`bufio.Reader`) для работы с большими файлами
- Поддерживает параллельную обработку нескольких файлов (`workers`)
- Валидирует геохеши (длина 3-12 символов)
- Сохраняет точность ID через `json.Number`

### Manticore клиент

Два способа взаимодействия:

1. **JSON Search API** - для поиска документов
   ```json
   {
     "table": "library2026",
     "query": { "in": { "id": [123, 456, 789] } }
   }
   ```

2. **Bulk API** - для массового обновления
   ```json
   { "replace": { "index": "library2026", "id": 123, "doc": {...} } }
   { "replace": { "index": "library2026", "id": 456, "doc": {...} } }
   ```

**Важно:** В ответе на bulk запрос ключ называется `"bulk"`, а не `"replace"`:
```json
{
  "items": [{
    "bulk": {           // <-- ВНИМАНИЕ: не "replace"!
      "_id": 123,
      "result": "updated"
    }
  }]
}
```

## ⚠️ Важные особенности и грабли

### 1. Работа с ID
```go
// ВАЖНО: Все ID должны быть uint64!
type Document struct {
    ID uint64 `json:"id"`  // Никогда не используйте string!
}
```

### 2. JSON парсинг
```go
// Всегда используйте decoder.UseNumber() для больших чисел
decoder := json.NewDecoder(bytes.NewReader(line))
decoder.UseNumber() // Критически важно!
```

### 3. Bulk ответ Manticore
```json
// В ответе на bulk запрос ключ называется "bulk", а не "replace"
{
  "items": [{
    "bulk": {           // <-- ВНИМАНИЕ!
      "_id": 123,
      "result": "updated"
    }
  }]
}
```

### 4. 🚨 Особенность UPDATE для JSON полей в Manticore

**Важное открытие!** Manticore некорректно обрабатывает прямые JSON массивы в UPDATE запросах, воспринимая их как MVA (multi-value attributes). 

**Неправильно (вызывает ошибку):**
```json
{
  "update": {
    "doc": {
      "ner_loc": [{"value": "Египет", "start_pos": 769}]
    }
  }
}
// Ошибка: MVA elements should be integers
```

**Правильно (работает):**
```json
{
  "update": {
    "doc": {
      "ner_loc": "[{\"value\":\"Египет\",\"start_pos\":769}]"
    }
  }
}
```

Причина: Manticore путает JSON массивы с MVA. Решение - всегда передавать JSON поля как **строки** в UPDATE запросах, даже если в таблице они определены как `json`. При этом INSERT работает с обычными массивами без проблем.

### 5. Merge режим для геоданных
- Сохраняет уникальность геохешей
- Автоматически сортирует для консистентности
- Обновляет `updated_at` timestamp

### 6. Graceful shutdown
- При получении SIGINT/SIGTERM дает 2 секунды на завершение
- Принудительное завершение через 5 секунд
- Сохраняет статистику и отчеты

### 7. Точность ID
При парсинге ответов от Manticore всегда используйте `json.Number` и `decoder.UseNumber()` для сохранения точности 64-битных ID.

## Failed Records

Неудачные записи сохраняются в `./failed/failed_YYYYMMDD_HHMMSS.ndjson`:
```json
{
  "data": {
    "doc_id": 6056452479959171091,
    "geohashes_string": ["test1", "test2"],
    "geohashes_uint64": [111111, 222222]
  },
  "error": "document not found",
  "attempts": 1,
  "timestamp": 1741712807,
  "filename": "data/results.ndjson"
}
```

Автоматическая ротация:
- По размеру (по умолчанию 100MB)
- По времени (каждый день)
- Очистка старых записей (по умолчанию 7 дней)

## Примеры использования

### 1. Обработка всех файлов с геоданными
```bash
./geoupdater -dir ./data -mode merge
```

### 2. Обработка NER файлов
```bash
./geoupdater -ner -dir ./data -pattern "ner_*.ndjson"
```

### 3. Обработка конкретных файлов
```bash
./geoupdater -files ./data/file1.ndjson,./data/file2.ndjson
```

### 4. Повторная обработка failed записей
```bash
./geoupdater -reprocess
```

## 🐳 Docker

### Запуск с docker-compose
```bash
# Создаем структуру директорий
mkdir -p data failed logs reports

# Копируем файлы для обработки
cp results.ndjson data/
cp ner.ndjson data/

# Запускаем
docker-compose up
```

### Запуск отдельного контейнера
```bash
# Для геоданных
docker run --rm \
  --network host \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/failed:/app/failed \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/reports:/app/reports \
  geoupdater:latest -dir /app/data -mode merge

# Для NER
docker run --rm \
  --network host \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/failed:/app/failed \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/reports:/app/reports \
  geoupdater:latest -ner -dir /app/data -pattern "ner_*.ndjson"
```

## Отчеты

После каждой обработки создается детальный отчет в `./reports/`:

- `report_YYYYMMDD_HHMMSS.json` - для геоданных
- `ner_report_YYYYMMDD_HHMMSS.json` - для NER

Пример отчета для NER:
```json
{
  "version": "v1.1.0",
  "start_time": "2026-03-13T01:32:16.102+03:00",
  "end_time": "2026-03-13T01:32:22.076+03:00",
  "duration": "5.974244674s",
  "mode": "ner",
  "workers": 5,
  "batch_size": 1000,
  "files": [
    {
      "filename": "data/ner.ndjson",
      "size_bytes": 51300925,
      "lines": 68672,
      "valid": 68672,
      "errors": 0,
      "duration": "5.974244674s",
      "success": 68672,
      "failed": 0,
      "skipped": 0,
      "first_id": 6056452479959192576,
      "last_id": 6056452479959171072
    }
  ],
  "total_files": 1,
  "stats": {
    "total_processed": 68672,
    "total_success": 68672,
    "total_failed": 0,
    "total_skipped": 0
  }
}
```

## Производительность

### Геоданные (library2026)
- **37,820 документов** обработано за **5.09 секунды**
- Скорость: **~7,430 документов/сек**
- Время на документ: ~0.13 мс

### NER данные (library2026_ner)
- **68,672 документа** обработано за **5.97 секунды**
- Скорость: **~11,500 документов/сек**
- Время на документ: ~0.087 мс

При увеличении `WORKERS` и `BATCH_SIZE` производительность растет линейно.

## 🛠️ Команды Makefile

```bash
make build          # Сборка проекта
make run ARGS="-h"  # Запуск с аргументами
make test           # Запуск тестов
make clean          # Очистка
make docker-build   # Сборка Docker образа
make docker-run     # Запуск в Docker
make version        # Показать версию
```

## Версионирование

Проект использует семантическое версионирование. Версия внедряется в бинарный файл при сборке:

```bash
./geoupdater -version
# GeoUpdater v1.1.0 (commit: abc1234, built: 2026-03-13_01:30:45)
```

Подробнее в [VERSIONING.md](VERSIONING.md)
