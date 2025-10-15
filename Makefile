# CMS Pricing API Makefile

.PHONY: help install install-dev test test-golden test-cov lint format clean
.PHONY: docker-build docker-up docker-down docker-logs migrate migrate-create migrate-downgrade
.PHONY: dev worker shell docs check
.PHONY: audit audit-with-tests audit-quick audit-companion audit-catalog audit-links audit-cross-refs audit-makefile audit-makefile-fix
.PHONY: pre-commit pre-commit-run setup

help: ## Show this help message
	@echo "CMS Pricing API - Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	poetry install

install-dev: ## Install development dependencies
	poetry install --with dev

test: ## Run tests
	poetry run pytest

test-golden: ## Run golden tests for parity validation
	poetry run pytest tests/test_golden.py -v

test-cov: ## Run tests with coverage
	poetry run pytest --cov=cms_pricing --cov-report=html --cov-report=term

lint: ## Run linting
	poetry run flake8 cms_pricing/
	poetry run mypy cms_pricing/

format: ## Format code
	poetry run black cms_pricing/
	poetry run isort cms_pricing/

clean: ## Clean up temporary files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf htmlcov/
	rm -rf dist/
	rm -rf build/

docker-build: ## Build Docker image
	docker build -t cms-pricing-api .

docker-up: ## Start services with Docker Compose
	docker-compose up -d

docker-down: ## Stop services with Docker Compose
	docker-compose down

docker-logs: ## Show Docker Compose logs
	docker-compose logs -f

migrate: ## Run database migrations
	poetry run alembic upgrade head

migrate-create: ## Create new migration
	@read -p "Enter migration message: " msg; \
	poetry run alembic revision --autogenerate -m "$$msg"

migrate-downgrade: ## Rollback last migration
	poetry run alembic downgrade -1

dev: ## Start development server
	poetry run uvicorn cms_pricing.main:app --host 0.0.0.0 --port 8000 --reload

worker: ## Start background worker
	poetry run python -m cms_pricing.worker

shell: ## Start Python shell with app context
	poetry run python -c "from cms_pricing.main import app; import IPython; IPython.embed()"

docs: ## Generate API documentation
	@echo "API documentation available at: http://localhost:8000/docs"

check: lint test ## Run all checks (lint + test)

audit: ## Run all documentation audits
	python tools/run_all_audits.py

audit-with-tests: ## Run audits with documentation tests
	python tools/run_all_audits.py --with-tests

audit-quick: ## Run quick audits with fast tests
	python tools/run_all_audits.py --with-tests --quick

audit-companion: ## Audit companion documents only
	python tools/audit_companion_docs.py

audit-catalog: ## Audit documentation catalog only
	python tools/audit_doc_catalog.py

audit-links: ## Audit documentation links only
	python tools/audit_doc_links.py

audit-cross-refs: ## Audit cross-references only
	python tools/audit_cross_references.py

audit-makefile: ## Audit Makefile .PHONY declarations
	python tools/audit_makefile_phony.py

audit-makefile-fix: ## Auto-fix missing .PHONY declarations
	python tools/audit_makefile_phony.py --fix

pre-commit: ## Install pre-commit hooks
	poetry run pre-commit install

pre-commit-run: ## Run pre-commit on all files
	poetry run pre-commit run --all-files

setup: install-dev pre-commit ## Complete development setup
	@echo "Development environment setup complete!"
	@echo "Run 'make dev' to start the development server"
	@echo "Run 'make docker-up' to start with Docker Compose"
	@echo "Run 'make audit' to run documentation audits"
