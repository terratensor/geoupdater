# Makefile
.PHONY: build run test clean docker-build docker-run version

BINARY_NAME=geoupdater
BUILD_DIR=build

# Версия из git tag или dev
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
BUILD_TIME ?= $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS = -ldflags "-X github.com/terratensor/geoupdater/internal/version.Version=$(VERSION) \
                    -X github.com/terratensor/geoupdater/internal/version.Commit=$(COMMIT) \
                    -X github.com/terratensor/geoupdater/internal/version.BuildTime=$(BUILD_TIME)"

build:
	@echo "Building $(BINARY_NAME) version $(VERSION)..."
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) cmd/geoupdater/main.go
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

run: build
	./$(BUILD_DIR)/$(BINARY_NAME) $(ARGS)

test:
	go test -v ./...

test-integration:
	go test -v -tags=integration ./...

clean:
	rm -rf $(BUILD_DIR)
	rm -rf logs/*.log
	find . -name "*.log" -delete
	find . -name "report_*.json" -delete

docker-build:
	docker build -t $(BINARY_NAME):$(VERSION) .
	@echo "Docker image built: $(BINARY_NAME):$(VERSION)"

docker-run:
	docker run --rm \
		--network host \
		-v $(PWD)/data:/app/data \
		-v $(PWD)/failed:/app/failed \
		-v $(PWD)/logs:/app/logs \
		-v $(PWD)/reports:/app/reports \
		$(BINARY_NAME):$(VERSION) $(ARGS)

version:
	@echo "GeoUpdater version information:"
	@echo "  Version:    $(VERSION)"
	@echo "  Commit:     $(COMMIT)"
	@echo "  Build time: $(BUILD_TIME)"

.PHONY: example
example:
	@echo "Running example..."
	go run cmd/geoupdater/main.go -dir ./data -pattern "*.ndjson" -mode merge