// internal/adapters/ndjson/parser.go - исправляем парсинг
package ndjson

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/terratensor/geoupdater/internal/core/domain"
	"github.com/terratensor/geoupdater/internal/core/ports"
)

// Parser реализует ports.Parser для NDJSON файлов
type Parser struct {
	config  *Config
	logger  ports.Logger
	metrics ports.MetricsCollector
}

// Config конфигурация парсера
type Config struct {
	BatchSize   int  // Размер буфера для канала
	Workers     int  // Количество воркеров для параллельного парсинга
	Validate    bool // Валидировать данные
	SkipErrors  bool // Пропускать ошибочные строки
	MaxLineSize int  // Максимальный размер строки (в байтах)
}

// DefaultConfig возвращает конфигурацию по умолчанию
func DefaultConfig() *Config {
	return &Config{
		BatchSize:   1000,
		Workers:     4,
		Validate:    true,
		SkipErrors:  true,
		MaxLineSize: 10 * 1024 * 1024, // 10MB
	}
}

// NewParser создает новый экземпляр парсера
func NewParser(cfg *Config, logger ports.Logger, metrics ports.MetricsCollector) *Parser {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	return &Parser{
		config:  cfg,
		logger:  logger,
		metrics: metrics,
	}
}

// ParseFile читает один файл и возвращает канал с данными
func (p *Parser) ParseFile(ctx context.Context, filename string) (<-chan *domain.GeoUpdateData, <-chan error) {
	dataChan := make(chan *domain.GeoUpdateData, p.config.BatchSize)
	errChan := make(chan error, 1)

	go func() {
		defer close(dataChan)
		defer close(errChan)

		file, err := os.Open(filename)
		if err != nil {
			errChan <- fmt.Errorf("failed to open file %s: %w", filename, err)
			return
		}
		defer file.Close()

		p.logger.Info("start parsing file",
			ports.String("filename", filename),
			ports.Int("batch_size", p.config.BatchSize))

		// Используем буферизованное чтение
		reader := bufio.NewReaderSize(file, p.config.MaxLineSize)

		var lineNum int64
		var validCount, errorCount int

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				line, err := p.readLine(reader)
				if err == io.EOF {
					p.logger.Info("finished parsing file",
						ports.String("filename", filename),
						ports.Int64("lines", lineNum),
						ports.Int("valid", validCount),
						ports.Int("errors", errorCount))
					return
				}
				if err != nil {
					errorCount++
					if !p.config.SkipErrors {
						errChan <- fmt.Errorf("error reading file %s at line %d: %w",
							filename, lineNum, err)
						return
					}
					p.logger.Warn("error reading line",
						ports.String("filename", filename),
						ports.Int64("line", lineNum),
						ports.Error(err))
					continue
				}

				lineNum++

				// Пропускаем пустые строки
				if len(line) == 0 {
					continue
				}

				// Парсим строку
				data, err := p.parseLine(line)
				if err != nil {
					errorCount++
					if !p.config.SkipErrors {
						errChan <- fmt.Errorf("error parsing line %d: %w", lineNum, err)
						return
					}
					p.logger.Warn("error parsing line",
						ports.String("filename", filename),
						ports.Int64("line", lineNum),
						ports.Error(err),
						ports.String("line_preview", p.preview(line, 100)))
					continue
				}

				validCount++

				// Отправляем данные
				select {
				case <-ctx.Done():
					errChan <- ctx.Err()
					return
				case dataChan <- data:
				}
			}
		}
	}()

	return dataChan, errChan
}

// ParseFiles параллельно читает несколько файлов
func (p *Parser) ParseFiles(ctx context.Context, filenames []string) (<-chan *domain.GeoUpdateData, <-chan error) {
	dataChan := make(chan *domain.GeoUpdateData, p.config.BatchSize)
	errChan := make(chan error, len(filenames))

	go func() {
		defer close(dataChan)
		defer close(errChan)

		var wg sync.WaitGroup
		workers := p.config.Workers

		// Создаем канал для распределения файлов по воркерам
		fileChan := make(chan string, len(filenames))

		// Запускаем воркеров
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go p.fileWorker(ctx, i, fileChan, dataChan, errChan, &wg)
		}

		// Отправляем файлы воркерам
		for _, filename := range filenames {
			fileChan <- filename
		}
		close(fileChan)

		// Ждем завершения всех воркеров
		wg.Wait()

		p.logger.Info("finished parsing all files",
			ports.Int("files", len(filenames)),
			ports.Int("workers", workers))
	}()

	return dataChan, errChan
}

// ParseReader читает данные из io.Reader (для тестирования)
func (p *Parser) ParseReader(ctx context.Context, reader io.Reader) (<-chan *domain.GeoUpdateData, <-chan error) {
	dataChan := make(chan *domain.GeoUpdateData, p.config.BatchSize)
	errChan := make(chan error, 1)

	go func() {
		defer close(dataChan)
		defer close(errChan)

		bufReader := bufio.NewReaderSize(reader, p.config.MaxLineSize)

		var lineNum int

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				line, err := p.readLine(bufReader)
				if err == io.EOF {
					return
				}
				if err != nil {
					if !p.config.SkipErrors {
						errChan <- fmt.Errorf("error reading at line %d: %w", lineNum, err)
						return
					}
					continue
				}

				lineNum++

				if len(line) == 0 {
					continue
				}

				data, err := p.parseLine(line)
				if err != nil {
					if !p.config.SkipErrors {
						errChan <- fmt.Errorf("error parsing line %d: %w", lineNum, err)
						return
					}
					continue
				}

				select {
				case <-ctx.Done():
					errChan <- ctx.Err()
					return
				case dataChan <- data:
				}
			}
		}
	}()

	return dataChan, errChan
}

// ParseNERLine парсит строку в NERData
func (p *Parser) ParseNERLine(line []byte) (*domain.NERData, error) {
	// Сначала парсим в map для гибкой обработки doc_id
	var raw map[string]interface{}

	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()

	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("failed to unmarshal NER JSON: %w", err)
	}

	// Парсим doc_id (может быть строкой или числом)
	var docID uint64
	if idVal, ok := raw["doc_id"]; ok {
		switch v := idVal.(type) {
		case string:
			docID, _ = strconv.ParseUint(v, 10, 64)
			p.logger.Debug("parsed doc_id from string",
				ports.String("raw", v),
				ports.Uint64("parsed", docID))
		case json.Number:
			docID, _ = strconv.ParseUint(string(v), 10, 64)
			p.logger.Debug("parsed doc_id from json.Number",
				ports.String("raw", string(v)),
				ports.Uint64("parsed", docID))
		case float64:
			docID = uint64(v)
			p.logger.Debug("parsed doc_id from float64",
				ports.Float64("raw", v),
				ports.Uint64("parsed", docID))
		default:
			return nil, fmt.Errorf("doc_id has unexpected type: %T", v)
		}
	} else {
		return nil, fmt.Errorf("doc_id is required")
	}

	// Парсим NER массивы
	var location, person, org []domain.NEREntity

	if loc, ok := raw["ner_loc"]; ok && loc != nil {
		locBytes, _ := json.Marshal(loc)
		json.Unmarshal(locBytes, &location)
	}

	if per, ok := raw["ner_per"]; ok && per != nil {
		perBytes, _ := json.Marshal(per)
		json.Unmarshal(perBytes, &person)
	}

	if or, ok := raw["ner_org"]; ok && or != nil {
		orgBytes, _ := json.Marshal(or)
		json.Unmarshal(orgBytes, &org)
	}

	data := &domain.NERData{
		DocID:    docID,
		Location: location,
		Person:   person,
		Org:      org,
	}

	return data, nil
}

// ParseNERFile читает NER файл и возвращает канал с данными
func (p *Parser) ParseNERFile(ctx context.Context, filename string) (<-chan *domain.NERData, <-chan error) {
	dataChan := make(chan *domain.NERData, p.config.BatchSize)
	errChan := make(chan error, 1)

	go func() {
		defer close(dataChan)
		defer close(errChan)

		file, err := os.Open(filename)
		if err != nil {
			errChan <- fmt.Errorf("failed to open file %s: %w", filename, err)
			return
		}
		defer file.Close()

		p.logger.Info("start parsing NER file",
			ports.String("filename", filename),
			ports.Int("batch_size", p.config.BatchSize))

		reader := bufio.NewReaderSize(file, p.config.MaxLineSize)

		var lineNum int64
		var validCount, errorCount int

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				line, err := p.readLine(reader)
				if err == io.EOF {
					p.logger.Info("finished parsing NER file",
						ports.String("filename", filename),
						ports.Int64("lines", lineNum),
						ports.Int("valid", validCount),
						ports.Int("errors", errorCount))
					return
				}
				if err != nil {
					errorCount++
					if !p.config.SkipErrors {
						errChan <- fmt.Errorf("error reading file %s at line %d: %w",
							filename, lineNum, err)
						return
					}
					continue
				}

				lineNum++

				if len(line) == 0 {
					continue
				}

				data, err := p.ParseNERLine(line)
				if err != nil {
					errorCount++
					if !p.config.SkipErrors {
						errChan <- fmt.Errorf("error parsing line %d: %w", lineNum, err)
						return
					}
					p.logger.Warn("error parsing NER line",
						ports.String("filename", filename),
						ports.Int64("line", lineNum),
						ports.Error(err),
						ports.String("line_preview", p.preview(line, 100)))
					continue
				}

				validCount++

				select {
				case <-ctx.Done():
					errChan <- ctx.Err()
					return
				case dataChan <- data:
				}
			}
		}
	}()

	return dataChan, errChan
}

// ParseNERReader читает NER данные из io.Reader (для тестирования)
func (p *Parser) ParseNERReader(ctx context.Context, reader io.Reader) (<-chan *domain.NERData, <-chan error) {
	dataChan := make(chan *domain.NERData, p.config.BatchSize)
	errChan := make(chan error, 1)

	go func() {
		defer close(dataChan)
		defer close(errChan)

		bufReader := bufio.NewReaderSize(reader, p.config.MaxLineSize)

		var lineNum int

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				line, err := p.readLine(bufReader)
				if err == io.EOF {
					return
				}
				if err != nil {
					if !p.config.SkipErrors {
						errChan <- fmt.Errorf("error reading at line %d: %w", lineNum, err)
						return
					}
					continue
				}

				lineNum++

				if len(line) == 0 {
					continue
				}

				data, err := p.ParseNERLine(line)
				if err != nil {
					if !p.config.SkipErrors {
						errChan <- fmt.Errorf("error parsing NER line %d: %w", lineNum, err)
						return
					}
					continue
				}

				select {
				case <-ctx.Done():
					errChan <- ctx.Err()
					return
				case dataChan <- data:
				}
			}
		}
	}()

	return dataChan, errChan
}

// fileWorker обрабатывает файлы из канала
func (p *Parser) fileWorker(ctx context.Context, id int, fileChan <-chan string,
	dataChan chan<- *domain.GeoUpdateData, errChan chan<- error, wg *sync.WaitGroup) {

	defer wg.Done()

	p.logger.Debug("worker started", ports.Int("worker_id", id))

	for filename := range fileChan {
		select {
		case <-ctx.Done():
			return
		default:
			p.logger.Debug("worker processing file",
				ports.Int("worker_id", id),
				ports.String("filename", filename))

			fileDataChan, fileErrChan := p.ParseFile(ctx, filename)

			// Передаем данные
			for data := range fileDataChan {
				select {
				case <-ctx.Done():
					return
				case dataChan <- data:
				}
			}

			// Проверяем ошибки
			select {
			case err := <-fileErrChan:
				if err != nil {
					errChan <- fmt.Errorf("worker %d: %w", id, err)
				}
			default:
			}
		}
	}

	p.logger.Debug("worker finished", ports.Int("worker_id", id))
}

// readLine читает одну строку с учетом максимального размера
func (p *Parser) readLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	var isPrefix bool
	var err error

	for {
		var part []byte
		part, isPrefix, err = reader.ReadLine()

		if err != nil {
			return nil, err
		}

		line = append(line, part...)

		if !isPrefix {
			break
		}

		// Проверяем не превышен ли максимальный размер
		if len(line) > p.config.MaxLineSize {
			return nil, fmt.Errorf("line exceeds maximum size of %d bytes", p.config.MaxLineSize)
		}
	}

	return line, nil
}

// parseLine парсит JSON строку в GeoUpdateData
func (p *Parser) parseLine(line []byte) (*domain.GeoUpdateData, error) {
	// Используем map для гибкого парсинга
	var raw map[string]interface{}

	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()

	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	// Парсим doc_id (может быть строкой или числом)
	var docID uint64
	if idVal, ok := raw["doc_id"]; ok {
		switch v := idVal.(type) {
		case string:
			docID, _ = strconv.ParseUint(v, 10, 64)
		case json.Number:
			docID, _ = strconv.ParseUint(string(v), 10, 64)
		case float64:
			docID = uint64(v)
		default:
			return nil, fmt.Errorf("doc_id has unexpected type: %T", v)
		}
	} else {
		return nil, fmt.Errorf("doc_id is required")
	}

	// Парсим geohashes_string
	var geohashesString []string
	if gs, ok := raw["geohashes_string"]; ok {
		if gsArr, ok := gs.([]interface{}); ok {
			geohashesString = make([]string, len(gsArr))
			for i, v := range gsArr {
				geohashesString[i] = fmt.Sprintf("%v", v)
			}
		}
	}

	// Парсим geohashes_uint64
	var geohashesUint64 []uint64
	if gu, ok := raw["geohashes_uint64"]; ok {
		if guArr, ok := gu.([]interface{}); ok {
			geohashesUint64 = make([]uint64, len(guArr))
			for i, v := range guArr {
				switch val := v.(type) {
				case json.Number:
					num, _ := strconv.ParseUint(string(val), 10, 64)
					geohashesUint64[i] = num
				case float64:
					geohashesUint64[i] = uint64(val)
				default:
					return nil, fmt.Errorf("geohashes_uint64[%d] has unexpected type: %T", i, v)
				}
			}
		}
	}

	// Валидация
	if p.config.Validate {
		if len(geohashesString) != len(geohashesUint64) {
			return nil, fmt.Errorf("geohashes count mismatch: strings=%d, uint64=%d",
				len(geohashesString), len(geohashesUint64))
		}

		if len(geohashesString) == 0 && len(geohashesUint64) == 0 {
			return nil, fmt.Errorf("at least one geohash array must be non-empty")
		}

		for i, g := range geohashesString {
			if g == "" {
				return nil, fmt.Errorf("geohashes_string[%d] is empty", i)
			}
			if len(g) < 3 || len(g) > 12 {
				return nil, fmt.Errorf("geohashes_string[%d] has invalid length: %d", i, len(g))
			}
		}
	}

	// Создаем доменную модель
	data, err := domain.NewGeoUpdateData(docID, geohashesString, geohashesUint64)
	if err != nil {
		return nil, fmt.Errorf("failed to create GeoUpdateData: %w", err)
	}

	return data, nil
}

// preview возвращает превью строки для логирования
func (p *Parser) preview(line []byte, maxLen int) string {
	if len(line) <= maxLen {
		return string(line)
	}
	return string(line[:maxLen]) + "..."
}

// FindFiles находит все файлы по паттерну в директории
func (p *Parser) FindFiles(dir, pattern string) ([]string, error) {
	var files []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		matched, err := filepath.Match(pattern, info.Name())
		if err != nil {
			return err
		}

		if matched {
			files = append(files, path)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk directory %s: %w", dir, err)
	}

	return files, nil
}

// GetStats возвращает статистику парсера
func (p *Parser) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"batch_size":    p.config.BatchSize,
		"workers":       p.config.Workers,
		"validate":      p.config.Validate,
		"skip_errors":   p.config.SkipErrors,
		"max_line_size": p.config.MaxLineSize,
	}
}
