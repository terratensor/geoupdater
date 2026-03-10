// internal/core/ports/ports.go
package ports

// Этот файл просто для документирования пакета ports

// Пакет ports определяет интерфейсы (порты) для взаимодействия
// между ядром приложения и внешними адаптерами в соответствии с
// гексагональной архитектурой.
//
// Основные порты:
// - Repository: работа с Manticore Search
// - Parser: парсинг NDJSON файлов
// - Logger: логирование
// - FailedRecordsRepository: хранение неудачных записей
// - UpdateProcessor: бизнес-логика обновления
// - FileWatcher: отслеживание новых файлов
// - MetricsCollector: сбор метрик
// - HealthChecker: проверка здоровья
