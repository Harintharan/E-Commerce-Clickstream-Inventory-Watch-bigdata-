# E-Commerce Clickstream Inventory Watch

A containerized data engineering project for real-time e-commerce clickstream ingestion, product behavior analytics, flash-sale detection, user segmentation, and daily reporting.

## Overview

The system simulates e-commerce user activity, streams events through Kafka, processes product metrics, stores analytics in PostgreSQL, and uses Airflow to generate daily business reports.

Core capabilities:

- Real-time clickstream event generation
- Kafka-based event ingestion
- Stream processing for product metrics
- Flash-sale candidate detection
- Daily user segmentation
- Daily product summary reports in TXT, CSV, and HTML email formats
- Automatic cleanup of report files older than 30 days

## Architecture

```text
Python Producer
    -> Kafka topic: clickstream_topic
    -> Stream Processor
    -> PostgreSQL
    -> Airflow DAG
    -> TXT/CSV reports and email summary
```

## Services

| Service | Purpose | URL / Port |
| --- | --- | --- |
| Kafka | Event broker | `localhost:9092` |
| Kafka UI | Topic/message inspection | `http://localhost:9000` |
| PostgreSQL | Analytics database | `localhost:5432` |
| Airflow Webserver | DAG UI | `http://localhost:18080` |
| Airflow Scheduler | Batch orchestration | Internal |
| Python Producer | Simulated clickstream source | Internal |
| Stream Processor | Kafka consumer and analytics processor | Internal |

Airflow login:

```text
Username: admin
Password: admin
```

## Project Structure

```text
.
├── dags/
│   └── dag_segmentation.py       # Airflow DAG for daily batch analytics and reporting
├── Dockerfile.airflow            # Airflow image
├── Dockerfile.processor          # Stream processor image
├── Dockerfile.producer           # Producer image
├── docker-compose.yaml           # Full local stack
├── init_db.sql                   # PostgreSQL schema
├── producer.py                   # Clickstream event producer
├── stream_processor.py           # Stream processor and fallback processor
├── query_helper.py               # Local database query helper
├── requirements.txt              # Python dependencies
├── Makefile                      # Common commands
└── README.md                     # Project guide
```

## Data Model

Main tables:

| Table | Description |
| --- | --- |
| `clickstream_events` | Raw event-level clickstream data |
| `product_metrics` | Streaming product metrics by time window |
| `daily_product_summary` | Daily product totals used for reports |
| `user_segments` | Daily user classification as Buyer or Window Shopper |

Event types:

```text
view
add_to_cart
purchase
```

Flash-sale rule:

```text
total_views > 100 AND total_purchases < 5
```

## Getting Started

### Prerequisites

- Docker
- Docker Compose
- At least 8 GB RAM recommended

### Start the stack

```bash
docker compose up -d --build
```

Check status:

```bash
docker compose ps
```

Wait until the main services are running and healthy:

```text
kafka
postgres-db
redis
airflow-webserver
airflow-scheduler
python-producer
stream-processor
```

### Open Airflow

Open:

```text
http://localhost:18080
```

Enable and trigger the DAG:

```text
clickstream_daily_batch
```

## Airflow DAG

DAG name:

```text
clickstream_daily_batch
```

Task flow:

```text
start
  -> check_data_exist
  -> [segment_users, generate_daily_summary]
  -> generate_summary_report
  -> send_summary_email
  -> cleanup_old_reports
  -> end
```

Task summary:

| Task | Description |
| --- | --- |
| `check_data_exist` | Validates source tables and available clickstream data |
| `segment_users` | Classifies users as Buyers or Window Shoppers |
| `generate_daily_summary` | Builds daily product totals from raw events |
| `generate_summary_report` | Writes TXT and CSV report files |
| `send_summary_email` | Sends the styled HTML report email |
| `cleanup_old_reports` | Deletes `.txt` and `.csv` reports older than 30 days |

## Reports

Reports are written inside the Airflow container:

```text
/airflow/logs/reports
```

File naming:

```text
summary_YYYY-MM-DD_YYYYMMDD_HHMMSS.txt
summary_YYYY-MM-DD_YYYYMMDD_HHMMSS.csv
```

Example:

```text
summary_2026-05-10_20260510_170756.txt
summary_2026-05-10_20260510_170756.csv
```

List reports:

```bash
docker compose exec airflow-webserver ls -lah /airflow/logs/reports
```

Read a TXT report:

```bash
docker compose exec airflow-webserver cat /airflow/logs/reports/summary_2026-05-10_20260510_170756.txt
```

Read a CSV report:

```bash
docker compose exec airflow-webserver cat /airflow/logs/reports/summary_2026-05-10_20260510_170756.csv
```

Copy reports to the local project directory:

```bash
docker compose cp airflow-webserver:/airflow/logs/reports ./reports
```

## Useful Commands

Start services:

```bash
docker compose up -d
```

Stop services:

```bash
docker compose down
```

Rebuild services:

```bash
docker compose up -d --build
```

View logs:

```bash
docker compose logs -f
```

View producer logs:

```bash
docker compose logs -f python-producer
```

View stream processor logs:

```bash
docker compose logs -f stream-processor
```

Connect to PostgreSQL:

```bash
docker compose exec postgres-db psql -U airflow -d clickstream_db
```

Query daily product summary:

```bash
docker compose exec postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT product_id, total_views, total_purchases, conversion_rate, flash_sale_recommended
   FROM daily_product_summary
   ORDER BY total_views DESC
   LIMIT 10;"
```

Query user segments:

```bash
docker compose exec postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT segment_type, COUNT(*)
   FROM user_segments
   GROUP BY segment_type;"
```

## Configuration

Common environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `DB_USER` | `airflow` | PostgreSQL user |
| `DB_PASSWORD` | `airflow` | PostgreSQL password |
| `DB_NAME` | `clickstream_db` | PostgreSQL database |
| `KAFKA_TOPIC` | `clickstream_topic` | Kafka event topic |
| `LOG_LEVEL` | `INFO` | Application log level |
| `SMTP_HOST` | `smtp.gmail.com` | Email server |
| `SMTP_PORT` | `587` | Email server port |
| `SMTP_USER` | unset | Sender email account |
| `SMTP_PASSWORD` | unset | Email app password |
| `REPORT_RECIPIENT` | unset | Report recipient |

For production or shared environments, keep email credentials outside source control and inject them through environment variables or a secret manager.

## Cleanup

Remove containers:

```bash
docker compose down
```

Remove containers and named volumes:

```bash
docker compose down -v
```

The Airflow DAG also runs `cleanup_old_reports` after email delivery. It deletes generated `.txt` and `.csv` report files older than 30 days from:

```text
/airflow/logs/reports
```

## Notes

- Daily summary totals are calculated from `clickstream_events`, the raw source of truth.
- The top 5 product table is a ranking view only.
- Email summary cards use totals across all daily product rows.
- CSV reports contain product-level daily summary rows only.
- TXT reports include the top product ranking and user segmentation summary.
