# E-Commerce Clickstream Inventory Watch

A containerized big data pipeline for simulating e-commerce clickstream activity, processing real-time product metrics, detecting flash-sale opportunities, and generating daily analytics reports.

## Overview

This project streams synthetic user events through Kafka, processes them with a Python/PySpark stream processor, stores analytics in PostgreSQL, and uses Airflow for scheduled segmentation and reporting.

```text
Producer -> Kafka -> Stream Processor -> PostgreSQL -> Airflow Reports
```

## Tech Stack

- Python
- Kafka and Zookeeper
- PostgreSQL
- Apache Airflow
- Docker Compose
- Kafka UI

## Features

- Real-time clickstream event generation
- Kafka topic-based ingestion
- Product-level streaming metrics
- Flash-sale recommendation logic
- Daily user segmentation
- TXT/CSV report generation
- Optional email reporting through SMTP

## Project Structure

```text
.
├── producer.py              # Generates clickstream events
├── stream_processor.py      # Processes Kafka events and writes metrics
├── query_helper.py          # Helper queries and reports
├── config.py                # Centralized environment configuration
├── dags/                    # Airflow DAGs
├── init_db.sql              # PostgreSQL schema
├── docker-compose.yaml      # Local container stack
└── Dockerfile.*             # Service images
```

## Quick Start

Create or update `.env` from `.env.example`, then start the full stack:

```bash
docker compose up -d --build
```

Check running services:

```bash
docker compose ps
```

Open the main tools:

- Airflow: `http://localhost:18080`
- Kafka UI: `http://localhost:9000`
- PostgreSQL: `localhost:5432`

Default Airflow login:

```text
Username: admin
Password: admin
```

## Useful Commands

```bash
docker compose logs -f
docker compose logs -f python-producer
docker compose logs -f stream-processor
docker compose exec postgres-db psql -U airflow -d clickstream_db
docker compose down
docker compose down -v
```

Query sample metrics:

```bash
docker compose exec postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT product_id, total_views, total_purchases, conversion_rate
   FROM daily_product_summary
   ORDER BY total_views DESC
   LIMIT 10;"
```

## Data Tables

- `clickstream_events` - raw event data
- `product_metrics` - streaming product metrics
- `daily_product_summary` - daily product analytics
- `user_segments` - daily user classifications

## Reports

Airflow writes generated reports inside the container at:

```text
/airflow/logs/reports
```

Reports are retained based on the configured retention period.

## License

This project is for learning and demonstration purposes.
