#!/bin/bash
# scripts/benchmark.sh

echo "Запуск бенчмарка..."

# Замер времени обработки
time (
    ./build/geoupdater -dir ./data -mode merge
)

# Показать статистику после обработки
./build/geoupdater -stats