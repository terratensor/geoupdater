# Dockerfile
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o /app/geoupdater cmd/geoupdater/main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Создаем необходимые директории
RUN mkdir -p /app/data /app/failed /app/logs /app/reports

# Копируем бинарник
COPY --from=builder /app/geoupdater /app/geoupdater
COPY --from=builder /app/configs/.env.example /app/.env.example

# Добавляем скрипт для инициализации
RUN echo '#!/bin/sh' > /app/entrypoint.sh && \
    echo 'if [ ! -f /app/.env ] && [ -f /app/.env.example ]; then' >> /app/entrypoint.sh && \
    echo '  cp /app/.env.example /app/.env' >> /app/entrypoint.sh && \
    echo '  echo "Created default .env file from example"' >> /app/entrypoint.sh && \
    echo 'fi' >> /app/entrypoint.sh && \
    echo 'exec /app/geoupdater "$@"' >> /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

VOLUME ["/app/data", "/app/failed", "/app/logs", "/app/reports"]

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["-h"]