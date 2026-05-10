.PHONY: help build up down logs clean test query

# Default target
help:
	@echo "E-Commerce Clickstream System - Makefile Commands"
	@echo "=================================================="
	@echo ""
	@echo "Development Commands:"
	@echo "  make build              - Build all Docker images"
	@echo "  make up                 - Start all services"
	@echo "  make down               - Stop all services"
	@echo "  make clean              - Remove containers and volumes"
	@echo ""
	@echo "Monitoring Commands:"
	@echo "  make logs-producer      - Show producer logs"
	@echo "  make logs-processor     - Show stream processor logs"
	@echo "  make logs-kafka         - Show Kafka logs"
	@echo "  make logs-postgres      - Show PostgreSQL logs"
	@echo "  make logs-airflow       - Show Airflow logs"
	@echo "  make ps                 - Show running containers"
	@echo ""
	@echo "Verification Commands:"
	@echo "  make verify-kafka       - Test Kafka connectivity"
	@echo "  make verify-postgres    - Test PostgreSQL connectivity"
	@echo "  make query-metrics      - Query product metrics"
	@echo "  make query-segments     - Query user segments"
	@echo "  make query-flash-sale   - Query Flash Sale triggers"
	@echo ""
	@echo "Utility Commands:"
	@echo "  make shell-producer     - SSH into producer container"
	@echo "  make shell-kafka        - SSH into Kafka container"
	@echo "  make shell-postgres     - Connect to PostgreSQL"
	@echo ""

# Build
build:
	docker-compose build

# Container management
up:
	docker-compose up

up-d:
	docker-compose up -d

down:
	docker-compose down

clean:
	docker-compose down -v
	rm -rf checkpoint logs producer_logs processor_logs postgres_data

restart:
	docker-compose restart

ps:
	docker-compose ps

# Logs
logs:
	docker-compose logs -f

logs-producer:
	docker-compose logs -f python-producer

logs-processor:
	docker logs -f --tail 100 stream-processor 2>&1

logs-kafka:
	docker-compose logs -f kafka

logs-postgres:
	docker-compose logs -f postgres-db

logs-airflow:
	docker-compose logs -f airflow-webserver airflow-scheduler

# Verification
verify-kafka:
	docker exec -it kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092
	@echo "Kafka broker is responsive!"

verify-postgres:
	docker exec postgres-db pg_isready -U airflow

query-metrics:
	docker exec -it postgres-db psql -U airflow -d clickstream_db -c \
		"SELECT product_id, view_count, purchase_count, conversion_rate FROM product_metrics LIMIT 10;"

query-segments:
	docker exec -it postgres-db psql -U airflow -d clickstream_db -c \
		"SELECT segment_type, COUNT(*) FROM user_segments GROUP BY segment_type;"

query-flash-sale:
	docker exec -it postgres-db psql -U airflow -d clickstream_db -c \
		"SELECT product_id, view_count, purchase_count FROM product_metrics WHERE flash_sale_suggested = true LIMIT 10;"

# Shell access
shell-producer:
	docker exec -it python-producer /bin/sh

shell-kafka:
	docker exec -it kafka /bin/bash

shell-postgres:
	docker exec -it postgres-db psql -U airflow -d clickstream_db

# Full setup
setup: build up
	@echo "System is now starting. Please wait a few seconds..."

setup-dev: build up-d
	@echo "Development environment is running in background"
	@docker-compose ps

# Testing
test-kafka-messages:
	docker exec -it kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic clickstream_topic \
		--max-messages 10

test-connectivity:
	@echo "Testing Kafka..."
	docker exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092
	@echo "Testing PostgreSQL..."
	docker exec postgres-db pg_isready -U airflow
	@echo "All tests passed!"

# Report
report:
	python query_helper.py

health:
	@echo "=== Service Status ===" && \
	docker-compose ps && \
	echo "" && \
	echo "=== Kafka Health ===" && \
	docker exec -it kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>/dev/null && echo "✓ Kafka OK" || echo "✗ Kafka Down" && \
	echo "" && \
	echo "=== PostgreSQL Health ===" && \
	docker exec postgres-db pg_isready -U airflow 2>/dev/null | grep "accepting" && echo "✓ PostgreSQL OK" || echo "✗ PostgreSQL Down"
