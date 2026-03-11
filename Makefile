# Makefile
.PHONY: build run test clean docker

BINARY_NAME=geoupdater
VERSION=1.0.0
BUILD_DIR=build

build:
	@echo "Building $(BINARY_NAME)..."
	go build -o $(BUILD_DIR)/$(BINARY_NAME) cmd/geoupdater/main.go

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

docker-build:
	docker build -t $(BINARY_NAME):$(VERSION) .

docker-run:
	docker run --rm \
		--network host \
		-v $(PWD)/data:/data \
		-v $(PWD)/failed:/failed \
		-v $(PWD)/logs:/logs \
		$(BINARY_NAME):$(VERSION) $(ARGS)

.PHONY: example
example:
	@echo "Running example..."
	go run cmd/geoupdater/main.go -dir ./data -pattern "*.ndjson" -mode merge