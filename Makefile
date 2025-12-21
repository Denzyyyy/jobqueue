.PHONY: help setup run test docker-up docker-down migrate-up migrate-down

help:
	@echo "Available commands:"
	@echo "  make setup       - Install dependencies"
	@echo "  make run         - Run the server"
	@echo "  make test        - Run tests"
	@echo "  make docker-up   - Start Docker services"
	@echo "  make docker-down - Stop Docker services"
	@echo "  make migrate-up  - Run migrations"

setup:
	go mod download
	go mod tidy

run:
	go run cmd/server/main.go

test:
	go test -v -race ./...

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

migrate-up:
	migrate -path migrations -database "postgresql://jobqueue:devpass@localhost:5432/jobqueue?sslmode=disable" up

migrate-down:
	migrate -path migrations -database "postgresql://jobqueue:devpass@localhost:5432/jobqueue?sslmode=disable" down 1

migrate-create:
	migrate create -ext sql -dir migrations -seq $(NAME)
