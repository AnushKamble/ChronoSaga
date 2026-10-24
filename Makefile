.PHONY: all fmt lint test test-race test-integration test-chaos migrate-up demo demo-crash demo-compensation bench down clean build

all: build test

build:
	go build -o bin/chronosaga ./cmd/chronosaga

fmt:
	go fmt ./...

lint:
	go vet ./...

test:
	go test -p 1 -v -count=1 ./...

test-race:
	go test -p 1 -race -v -count=1 ./...

test-integration:
	go test -v -tags=integration -count=1 ./tests/integration/...

test-chaos:
	go test -v -tags=chaos -count=1 ./tests/chaos/...

migrate-up:
	go run ./cmd/chronosaga migrate up

demo:
	go run ./cmd/chronosaga demo full

demo-crash:
	go run ./cmd/chronosaga demo crash-recovery

demo-compensation:
	go run ./cmd/chronosaga demo saga-failure

bench:
	go run ./cmd/chronosaga benchmark

down:
	@echo "Cleaning up local test database or compose containers..."
	@docker compose down -v 2>nul || true

clean:
	@rmdir /s /q bin 2>nul || rm -rf bin 2>/dev/null || true
	@rmdir /s /q tmp 2>nul || rm -rf tmp 2>/dev/null || true
