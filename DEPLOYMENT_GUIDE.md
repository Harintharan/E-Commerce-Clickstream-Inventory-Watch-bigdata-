# Complete Docker Compose Down & Up Guide

## Step 1: Prepare Before Shutdown
```bash
# Check current status
docker-compose ps

# View logs (save if needed)
docker-compose logs > backup_logs.txt
```

## Step 2: Full Clean Shutdown
```bash
# Option A: Safe shutdown (keeps volumes/data)
docker-compose down

# Option B: Complete reset (removes everything including data)
docker-compose down -v

# CAUTION: Only use -v if you want to reset all data
```

---

## Step 3: Startup & Potential Issues

### ISSUE 1: PostgreSQL Won't Start (Exit Code 3)
**Symptom:** `postgres-db exited with code 3`

**Cause:** Corrupted database volume

**Fix:**
```bash
# Nuclear option - remove volume and restart
docker-compose down -v
docker-compose up -d

# The init_db.sql will auto-run and create fresh schema
```

**Verification:**
```bash
# Wait 10 seconds for DB to initialize
Start-Sleep -Seconds 10

# Check it's healthy
docker-compose ps | Select-String postgres-db
# Should show: (healthy)

# Verify schema was created
docker exec postgres-db psql -U airflow -d clickstream_db -c "\dt"
# Should show: product_metrics, user_segments, daily_product_summary
```

---

### ISSUE 2: Kafka Won't Connect (Service_Unhealthy)
**Symptom:** Kafka shows unhealthy, or services can't connect

**Cause:** Kafka startup is slow or Zookeeper not ready

**Fix:**
```bash
# Wait longer for Kafka to initialize
Start-Sleep -Seconds 30

# Check both services are healthy
docker-compose ps | Select-String "zookeeper|kafka"

# If still failing, restart just Kafka
docker-compose restart kafka

# Wait another 15 seconds
Start-Sleep -Seconds 15

# Verify Kafka can be accessed
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
# Should show broker info without errors
```

---

### ISSUE 3: Airflow Services Crash
**Symptom:** airflow-scheduler or airflow-webserver in crashed state

**Cause:** Database not ready when Airflow tries to connect, or corrupted metadata

**Fix:**
```bash
# Verify PostgreSQL is fully healthy first
docker exec postgres-db psql -U airflow -d clickstream_db -c "SELECT 1"

# Wait 20 seconds to ensure DB is fully ready
Start-Sleep -Seconds 20

# Then restart Airflow services
docker-compose restart airflow-webserver airflow-scheduler

# Wait for them to start
Start-Sleep -Seconds 15

# Check if they're running
docker-compose ps | Select-String airflow

# View logs for errors
docker-compose logs airflow-scheduler --tail 50
docker-compose logs airflow-webserver --tail 50
```

---

### ISSUE 4: Stream Processor Connection Error
**Symptom:** Stream processor exits or can't connect to Kafka/PostgreSQL

**Cause:** Services not ready when processor tries to connect

**Fix:**
```bash
# Check Kafka is accessible
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Check PostgreSQL is accessible
docker exec postgres-db psql -U airflow -d clickstream_db -c "SELECT 1"

# Restart stream processor
docker-compose restart stream-processor

# Monitor logs
docker-compose logs stream-processor --tail 50 -f
# Wait for: "Successfully wrote X records to product_metrics"
```

---

## Step 4: Startup Sequence (Do in This Order)

### Phase 1: Infrastructure (60 seconds)
```bash
docker-compose up -d zookeeper kafka postgres-db redis

Start-Sleep -Seconds 60

# Verify all are healthy
docker-compose ps
# All should show: healthy or Up
```

### Phase 2: UI Services (45 seconds)
```bash
docker-compose up -d pgadmin airflow-webserver airflow-scheduler

Start-Sleep -Seconds 45

# Verify running
docker-compose ps | Select-String "pgadmin|airflow"
```

### Phase 3: Data Services (30 seconds)
```bash
docker-compose up -d python-producer stream-processor

Start-Sleep -Seconds 30

# Check logs
docker-compose logs python-producer --tail 10
docker-compose logs stream-processor --tail 10
```

---

## Step 5: Comprehensive Verification

### Check 1: All Services Running
```bash
docker-compose ps
# EXPECTED: All 9 services showing "Up" (with health checks running or healthy)
```

### Check 2: Database Schema
```bash
docker exec postgres-db psql -U airflow -d clickstream_db -c "\dt"
# EXPECTED OUTPUT:
#                List of relations
# Schema |           Name           | Type  | Owner
#--------+--------------------------+-------+--------
# public | daily_product_summary    | table | airflow
# public | product_metrics          | table | airflow
# public | user_segments            | table | airflow
```

### Check 3: Kafka Topic
```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
# EXPECTED: clickstream_topic (and possibly __consumer_offsets)
```

### Check 4: Data Flowing
```bash
# Wait 30 seconds for producer to generate events
Start-Sleep -Seconds 30

# Check product_metrics has data
docker exec postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT COUNT(*) as record_count FROM product_metrics;"
# EXPECTED: count > 0 (should show 20-100 after 30 seconds)

# Check user_segments (batch job, may be empty until DAG runs)
docker exec postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT COUNT(*) FROM user_segments;"
# EXPECTED: 0 initially (populated by Airflow DAG)
```

### Check 5: Airflow UI
```bash
# Open browser and go to: http://localhost:18080
# Login: admin / admin
# Should see: clickstream_daily_batch DAG listed
```

---

## Step 6: If Something Still Fails

### Full Diagnostic Report
```bash
# Get complete status
$report = @"
=== Docker Compose Status ===
$(docker-compose ps)

=== Airflow Scheduler Logs ===
$(docker-compose logs airflow-scheduler --tail 30)

=== Stream Processor Logs ===
$(docker-compose logs stream-processor --tail 30)

=== Database Connection Test ===
$(docker exec postgres-db psql -U airflow -d clickstream_db -c "SELECT 1" 2>&1)

=== Kafka Test ===
$(docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>&1)
"@

$report | Out-File -FilePath diagnostic_report.txt
Write-Host "Report saved to diagnostic_report.txt"
```

### Nuclear Reset (Last Resort)
```bash
# Stop everything
docker-compose down

# Remove ALL volumes (data will be lost!)
docker volume rm $(docker volume ls -q | Select-String big_data)

# Remove images to force rebuild
docker-compose down --rmi all

# Start fresh
docker-compose up -d

# Wait for full initialization
Start-Sleep -Seconds 90
```

---

## Step 7: Common Error Messages & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `connect: connection refused` | Service not ready | Wait longer, use `Start-Sleep -Seconds 30` |
| `duplicate key value violates` | DAG ran twice on same date | Use ON CONFLICT in SQL (already done) |
| `FATAL: Ident authentication failed` | DB credentials wrong | Check `.env` file: `DB_USER`, `DB_PASSWORD` |
| `No module named 'confluent_kafka'` | Dependencies not installed | Rebuild image: `docker-compose build` |
| `port already in use` | Another service using port | Kill process or use different port |
| `OutOfMemory` | Container running out of RAM | Increase Docker memory limit |

---

## Step 8: Post-Startup Checklist

- [ ] All 9 services running: `docker-compose ps`
- [ ] Database schema exists: Check `\dt` output
- [ ] Kafka topic created: `clickstream_topic` exists
- [ ] Data flowing: `product_metrics` has records
- [ ] Airflow UI accessible: http://localhost:18080
- [ ] Producer generating events: Check logs
- [ ] Stream processor writing: Check logs for "Successfully wrote"
- [ ] No error logs in services: `docker-compose logs`

---

## Emergency Recovery Commands

```bash
# View real-time logs for all services
docker-compose logs -f

# View logs for specific service with last 100 lines
docker-compose logs SERVICE_NAME --tail 100

# Restart single service
docker-compose restart SERVICE_NAME

# Kill and restart all
docker-compose down && docker-compose up -d && Start-Sleep -Seconds 90

# Enter service shell for debugging
docker exec -it SERVICE_NAME bash

# Check resource usage
docker stats
```

---

## When to Use Each Option

### `docker-compose down`
- Use when: Updating code, changing configs
- Keeps: All data, volumes, previous state
- Risk: Low

### `docker-compose down -v`
- Use when: Testing from scratch, database corrupted
- Removes: All data, volumes, everything
- Risk: Data loss

### `docker-compose restart SERVICE`
- Use when: Single service acting weird
- Keeps: All data, running state
- Risk: Very low

### Full rebuild: `docker-compose up -d --build`
- Use when: Changed Dockerfiles
- Rebuilds: All images
- Time: 5-10 minutes

---

## Quick Reference Commands

```bash
# FULL RESTART SEQUENCE
docker-compose down
Start-Sleep -Seconds 5
docker-compose up -d
Start-Sleep -Seconds 90
docker-compose ps

# HEALTH CHECK
docker-compose ps | Select-String "unhealthy|Exit"

# DATA CHECK
docker exec postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT COUNT(*) FROM product_metrics; SELECT COUNT(*) FROM user_segments;"

# LOG MONITORING
docker-compose logs -f --tail 50

# AIRFLOW UI
# Visit: http://localhost:18080
# User: admin
# Pass: admin
```

