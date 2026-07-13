COMPOSE := docker compose

.PHONY: proto build lint test test-short certs dev down logs migrate clean help

help: ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "%-12s %s\n", $$1, $$2}'

proto: ## Regenerate gen/ from proto/ (buf remote plugins; needs network on first run)
	buf lint
	buf generate

build: ## Compile all packages
	go build ./...

lint: ## golangci-lint over the module
	golangci-lint run

test: ## All tests (migration test needs Docker)
	go test ./...

test-short: ## Tests that need no Docker
	go test -short ./...

certs: certs/ca.pem ## Generate mTLS material (prerequisite — nothing connects without it)

certs/ca.pem:
	./scripts/gen-certs.sh

dev: certs ## Boot the full cluster via compose
	$(COMPOSE) up --build -d
	@echo "cluster starting — try: curl -s localhost:8080/health"

down: ## Stop the cluster and drop volumes
	$(COMPOSE) down -v

logs: ## Tail cluster logs
	$(COMPOSE) logs -f

migrate: ## Apply migrations to localhost:5432 (outside compose)
	go run ./cmd/migrate

clean: down ## down + remove generated certs
	rm -rf certs
