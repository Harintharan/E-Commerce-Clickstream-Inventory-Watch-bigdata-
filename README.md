# E-Commerce Clickstream & Inventory Watch System

A production-ready, fully containerized big data engineering project for processing e-commerce clickstream events, detecting anomalies, and generating business intelligence.

## Architecture Overview

```
┌─────────────┐
│   Kafka     │ (Message Broker)
│  Producer   │
└──────┬──────┘
       │
       ├──────────┐
       │          │
  ┌────▼──┐   ┌──▼──────────┐
  │ Kafka │   │     DAG      │
  │Topic  │   │ (Airflow)    │
  └────┬──┘   └──────────────┘
       │
  ┌────▼──────────────┐
  │  PySpark Stream   │
  │   Processor       │
  └────┬──────────────┘
       │
  ┌────▼──────────────┐
  │   PostgreSQL      │
  │   Database        │
  └───────────────────┘
```

## System Components

### 1. **Kafka** (Message Broker)
- Receives clickstream events from the producer
- Topic: `clickstream_topic`
- Enables real-time data ingestion

### 2. **Python Producer**
- Generates simulated e-commerce events
- Event schema: `{user_id, product_id, event_type, timestamp, session_id, device}`
- **Feature**: Injects anomalous patterns (high views, low purchases) to trigger Flash Sale detection
- Runs continuously in Docker container

### 3. **PySpark Structured Streaming Processor**
- Consumes events from Kafka
- Performs 10-minute sliding window aggregation
- **Flash Sale Trigger Logic**:
  - Condition: Views > 100 AND Purchases < 5
  - Suggests promotional discount when met
- Writes processed metrics to PostgreSQL

### 4. **Apache Airflow**
- Schedules daily batch processing jobs
- **Tasks**:
  1. User Segmentation (Window Shoppers vs Buyers)
  2. Daily Product Summary (Top 5 products)
  3. Data Quality Validation
- Dashboard available at `http://localhost:8080`

### 5. **PostgreSQL Database**
- Stores all processed metrics and insights
- Tables: `product_metrics`, `user_segments`, `daily_product_summary`

## Prerequisites

- Docker & Docker Compose (v20.10+)
- 8GB+ RAM available
- 10GB+ disk space
- Windows, macOS, or Linux

## Quick Start

### Step 1: Clone/Setup Project

```bash
# Navigate to project directory
cd big_data_mini_project
```

### Step 2: Build and Start Services

```bash
# Build all container images
docker-compose build

# Start all services (in foreground with logs)
docker-compose up

# OR start in background
docker-compose up -d

# Check status of all services
docker-compose ps
```

**Expected Output**:
```
NAME                             STATUS
zookeeper                        Up (healthy)
kafka                            Up (healthy)
postgres-db                      Up (healthy)
redis                            Up (healthy)
airflow-webserver                Up (healthy)
airflow-scheduler                Up (healthy)
spark-master                     Up (healthy)
spark-worker                     Up
python-producer                  Up (restarting)
stream-processor                 Up (restarting)
```

### Step 3: Verify Kafka Producer

```bash
# Attach to Kafka container and verify messages are flowing
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic clickstream_topic \
  --from-beginning \
  --max-messages 10

# Expected output: JSON clickstream events
# Example: {"user_id": 42, "product_id": 15, "event_type": "view", "timestamp": "2024-04-18T..."}
```

### Step 4: Monitor PostgreSQL Data

```bash
# Connect to PostgreSQL
docker exec -it postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT * FROM product_metrics LIMIT 5;"

# Monitor real-time processed metrics
docker exec -it postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT product_id, view_count, purchase_count, flash_sale_suggested 
   FROM product_metrics 
   WHERE flash_sale_suggested = true 
   ORDER BY view_count DESC LIMIT 5;"
```

### Step 5: Access Airflow Web UI

1. Open browser: **`http://localhost:8080`**
2. Login with:
   - Username: `admin`
   - Password: `admin`
3. Enable and trigger the `clickstream_daily_batch` DAG
4. Monitor task execution in real-time

### Step 6: View Spark Master UI

1. Open browser: **`http://localhost:8081`**
2. Monitor streaming application performance
3. Check worker status and memory/CPU utilization

## Configuration

### Environment Variables (`.env` file)

```bash
# Database
DB_USER=airflow
DB_PASSWORD=airflow
DB_NAME=clickstream_db

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC=clickstream_topic

# Producer
BATCH_SIZE=10              # Events per batch
BATCH_INTERVAL_SECONDS=5   # Time between batches

# Logging
LOG_LEVEL=INFO
```

### Tuning for Production

#### Producer Throughput
```bash
# Increase batch size in .env
BATCH_SIZE=100
BATCH_INTERVAL_SECONDS=2
```

#### Spark Streaming
Edit `stream_processor.py`:
```python
WINDOW_DURATION = "5 minutes"      # Smaller window
SLIDING_INTERVAL = "30 seconds"    # More frequent updates
WATERMARK_DELAY = "10 minutes"     # Handle late data
```

#### Airflow Concurrency
Edit `docker-compose.yaml` for airflow-scheduler:
```yaml
environment:
  AIRFLOW__CORE__PARALLELISM: 16
  AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 4
```

## Verification Checklist

- [ ] All containers are healthy: `docker-compose ps`
- [ ] Producer sending events: `docker logs python-producer`
- [ ] Kafka topic has messages: kafka-console-consumer
- [ ] PostgreSQL tables populated: `SELECT COUNT(*) FROM product_metrics;`
- [ ] PySpark processing: `docker logs stream-processor | grep "Flash Sale"`
- [ ] Airflow DAG visible in UI: `http://localhost:8080`

## Monitoring & Debugging

### Check Producer Logs
```bash
docker logs python-producer --follow
```

### Check Stream Processor Logs
```bash
docker logs stream-processor --follow
```

### Monitor Kafka Broker
```bash
docker logs kafka --follow
docker exec -it kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

### Query Anomalies (Flash Sale Triggers)
```bash
docker exec -it postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT product_id, view_count, purchase_count, 
          ROUND(conversion_rate, 2) as conv_rate
   FROM product_metrics 
   WHERE flash_sale_suggested = true 
   ORDER BY view_count DESC LIMIT 10;"
```

### View User Segments
```bash
docker exec -it postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT segment_type, COUNT(*) as count 
   FROM user_segments 
   WHERE segment_date = CURRENT_DATE - 1 
   GROUP BY segment_type;"
```

## Data Schema

### `product_metrics` (Real-time)
```
product_id         INTEGER
window_start       TIMESTAMP
window_end         TIMESTAMP
view_count         INTEGER
cart_count         INTEGER
purchase_count     INTEGER
conversion_rate    DECIMAL(5,2)
flash_sale_suggested BOOLEAN
```

### `user_segments` (Daily)
```
user_id            INTEGER
segment_type       VARCHAR (Window Shopper / Buyer)
segment_date       DATE
view_count         INTEGER
purchase_count     INTEGER
products_viewed    INTEGER
```

### `daily_product_summary` (Daily)
```
product_id         INTEGER
summary_date       DATE
total_views        INTEGER
total_purchases    INTEGER
conversion_rate    DECIMAL(5,2)
flash_sale_recommended BOOLEAN
```

## File Structure

```
big_data_mini_project/
├── docker-compose.yaml           # Service orchestration
├── Dockerfile.producer           # Producer service
├── Dockerfile.processor          # Stream processor service
├── Dockerfile.airflow            # Airflow service
├── producer.py                   # Kafka data generator
├── stream_processor.py           # PySpark streaming logic
├── dags/
│   └── dag_segmentation.py       # Airflow DAG
├── init_db.sql                   # Database initialization
├── requirements.txt              # Python dependencies
├── .env                          # Environment variables
├── producer_logs/                # Producer logs
├── processor_logs/               # Processor logs
├── logs/                         # Airflow logs
└── README.md                     # This file
```

## Stopping Services

```bash
# Stop all services and remove containers
docker-compose down

# Stop services but keep volumes (database data retained)
docker-compose down -v

# Stop without removing
docker-compose stop

# Restart services
docker-compose restart
```

## Performance Notes

### Expected Throughput
- Producer: ~100-1000 events/second (configurable)
- Spark Processing: <2s latency with 10-min window
- Database: Can handle 10M+ records with proper indexing

### Resource Requirements
- Docker Memory: 6-8GB
- CPU: 4+ cores recommended
- Disk: 5GB for database and logs

## Troubleshooting

### Issue: Producer not sending events
```bash
# Check connectivity
docker exec python-producer python -c "from kafka import KafkaProducer; print('OK')"
# Check logs
docker logs python-producer
```

### Issue: Stream processor not receiving data
```bash
# Verify Kafka topic exists
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
# Check consumer lag
docker exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

### Issue: Airflow DAG not showing
```bash
# Restart scheduler
docker-compose restart airflow-scheduler
# Check DAG parsing errors
docker logs airflow-scheduler
```

### Issue: PostgreSQL connection refused
```bash
# Verify database is ready
docker exec postgres-db pg_isready -U airflow
# Check logs
docker logs postgres-db
```

## Production Deployment

### Security Hardening
1. Change default credentials in `.env`
2. Enable Kafka authentication (SASL/SSL)
3. Use secrets management (AWS Secrets Manager, Vault)
4. Restrict network access with firewall rules

### High Availability
1. Add Kafka brokers (multi-broker cluster)
2. Replicate PostgreSQL (primary-replica setup)
3. Run multiple Airflow worker instances
4. Use external Celery backend instead of Redis

### Monitoring
1. Add Prometheus exporters for metrics
2. Configure Grafana dashboards
3. Set up alert rules for anomalies
4. Enable central logging with ELK stack

## Contributors

- Data Engineering Team
- Academic Project (Semester 8)

## License

This project is for educational purposes.

---

## Support

For issues or questions:
1. Check logs: `docker logs <service_name>`
2. Review data in PostgreSQL
3. Verify Kafka topic messages
4. Check Airflow task execution history

**Last Updated**: April 2024
