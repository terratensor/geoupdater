#!/bin/bash
# scripts/prepare_test.sh

echo "Подготовка тестовой среды..."

# Создаем необходимые директории
mkdir -p ./data
mkdir -p ./failed
mkdir -p ./logs
mkdir -p ./results

# Копируем тестовый файл (предполагаем что results.ndjson лежит в текущей директории)
if [ -f "./results.ndjson" ]; then
    cp ./results.ndjson ./data/
    echo "✓ Файл results.ndjson скопирован в ./data/"
else
    echo "⚠ Файл results.ndjson не найден в текущей директории"
    echo "  Пожалуйста, поместите файл в ./data/ вручную"
fi

# Создаем тестовый .env файл если его нет
if [ ! -f "./.env" ]; then
    cat > ./.env << EOF
# Manticore configuration
MANTICORE_HOST=localhost
MANTICORE_PORT=9308
MANTICORE_TABLE=library2026
MANTICORE_TIMEOUT=30s
MANTICORE_MAX_CONNS=10

# Update configuration
UPDATE_MODE=merge
BATCH_SIZE=1000
WORKERS=5
MAX_RETRIES=3
RETRY_DELAY=1s

# File processing
INPUT_DIR=./data
FILE_PATTERN=*.ndjson
FAILED_DIR=./failed

# Logging
LOG_LEVEL=info
LOG_FILE=./logs/geoupdater.log

# Failed records reprocessing
ENABLE_REPROCESS=true
REPROCESS_WORKERS=3
MAX_FAILED_AGE=168h
EOF
    echo "✓ Создан файл .env с конфигурацией по умолчанию"
fi

echo ""
echo "✅ Подготовка завершена!"
echo "Для запуска теста выполните:"
echo "  make build"
echo "  ./build/geoupdater -dir ./data -mode merge"