# QUICK START GUIDE
# E-Commerce Clickstream & Inventory Watch System

## Prerequisites Check
- [ ] Docker installed: `docker --version`
- [ ] Docker Compose installed: `docker-compose --version`
- [ ] 8GB+ RAM available
- [ ] 10GB+ disk space

## Installation Steps (Windows/Mac/Linux)

### Step 1: Navigate to Project Directory
```
cd big_data_mini_project
```

### Step 2: Build All Containers (First Time Only)
```
docker-compose build
```

### Step 3: Start All Services
```
docker-compose up
```
Wait until you see messages like:
- "Kafka broker is ready"
- "airflow-webserver is running"
- "spark-master is running"

### Step 4: Verify in New Terminal (Keep previous running)

#### Check Kafka messages
```
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic clickstream_topic \
  --from-beginning \
  --max-messages 5
```

#### Check PostgreSQL tables
```
docker exec -it postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT COUNT(*) FROM product_metrics;"
```

#### Check Flash Sale Triggers
```
docker exec -it postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT product_id, view_count, purchase_count 
   FROM product_metrics 
   WHERE flash_sale_suggested = true;"
```

### Step 5: Access Web Interfaces

- **Airflow**: http://localhost:8080
  - Username: admin | Password: admin
  - Click "clickstream_daily_batch" DAG to see tasks

- **Spark**: http://localhost:8081
  - Monitor streaming application performance

- **PostgreSQL**: localhost:5432
  - User: airflow | Password: airflow
  - Database: clickstream_db

## Key Processes Explained

### Producer (python-producer)
- Continuously generates 10 events/batch
- Sends every 5 seconds
- Creates anomalous patterns for Flash Sale detection

### Stream Processor (stream-processor)
- Reads from Kafka
- Performs 10-minute sliding windows
- Detects: Views > 100 AND Purchases < 5 → Flash Sale signal
- Writes metrics to PostgreSQL

### Airflow DAG (clickstream_daily_batch)
- Runs daily at 11 PM UTC
- Segments users: Window Shoppers vs Buyers
- Generates top 5 products report
- Validates data quality

## Troubleshooting

### Producer not sending?
```
docker logs python-producer
```

### No Kafka messages?
```
docker logs kafka
docker exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

### Database connection error?
```
docker logs postgres-db
docker exec postgres-db pg_isready -U airflow
```

### Airflow not showing DAG?
```
docker logs airflow-scheduler
docker-compose restart airflow-scheduler
```

## Stopping Everything
```
docker-compose down
```

## Important Notes

1. **First Run**: Services take 20-30 seconds to fully initialize
2. **Logs**: Check `docker-compose logs <service_name>` for debugging
3. **Database**: Data persists in postgres_data volume
4. **Reset**: Run `docker-compose down -v` to remove all data

## Production Tips

- Increase `BATCH_SIZE` in .env for higher throughput
- Adjust window duration in stream_processor.py
- Enable authentication for Kafka and PostgreSQL
- Add monitoring with Prometheus/Grafana

## Next Steps

1. Generate sample data: Wait 2-3 minutes for events to accumulate
2. Check Flash Sale detection: Query product_metrics table
3. Trigger Airflow DAG: Enable in UI, click "Trigger DAG"
4. Monitor results: Check user_segments and daily_product_summary

## Contact
For issues, check README.md for detailed documentation.
