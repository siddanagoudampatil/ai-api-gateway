# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Leverage Docker layer caching for dependency metadata
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Compile static, stripped binary without CGO dependencies for minimal attack surface
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -trimpath \
    -o /bin/gateway \
    ./cmd/gateway

# Production runtime stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata \
    && addgroup -S appgroup \
    && adduser -S appuser -G appgroup

WORKDIR /app
COPY --from=builder /bin/gateway /app/gateway

USER appuser:appgroup

EXPOSE 8080

ENTRYPOINT ["/app/gateway"]
