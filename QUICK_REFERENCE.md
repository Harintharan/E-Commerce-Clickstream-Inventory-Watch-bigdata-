# Quick Reference: Most Common Scenarios

## SCENARIO 1: Normal Start (Preserve Data)
```powershell
docker-compose down
docker-compose up -d
Start-Sleep -Seconds 90
docker-compose ps
```
**Expected Time:** 2-3 minutes
**Data:** ✅ Preserved

---

## SCENARIO 2: Fresh Start (Clear Everything)
```powershell
docker-compose down -v
docker-compose up -d
Start-Sleep -Seconds 90
docker-compose ps
```
**Expected Time:** 2-3 minutes  
**Data:** ❌ Lost (fresh init)

---

## SCENARIO 3: Use Automated Startup Script
```powershell
# Safe mode (preserves data)
./startup.ps1 -Mode safe

# Clean mode (removes volumes)
./startup.ps1 -Mode clean
```
**Expected Time:** 3-5 minutes (includes health checks)
**Features:** Auto-verification, detailed status

---

## SCENARIO 4: One Service Is Broken
```powershell
# Restart just that service
docker-compose restart SERVICE_NAME

# Examples:
docker-compose restart airflow-scheduler
docker-compose restart stream-processor
docker-compose restart postgres-db
```
**Expected Time:** 10-30 seconds
**Data:** ✅ Safe, no data loss

---

## SCENARIO 5: Database Won't Start
```powershell
# Check the error
docker-compose logs postgres-db --tail 50

# If corrupted, nuke the volume
docker-compose down -v

# Start again (fresh init will run)
docker-compose up -d postgres-db redis
Start-Sleep -Seconds 60
docker-compose up -d
```
**Expected Time:** 2-3 minutes
**Data:** ❌ Lost

---

## SCENARIO 6: Kafka Connection Issues
```powershell
# Restart Kafka + Zookeeper
docker-compose restart zookeeper kafka

# Wait for them to be ready
Start-Sleep -Seconds 30

# Verify connection
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```
**Expected Time:** 1-2 minutes
**Data:** ✅ Safe

---

## SCENARIO 7: Stream Processor Not Writing Data
```powershell
# Check logs
docker-compose logs stream-processor --tail 50

# Check if Kafka has data
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic clickstream_topic --from-beginning --max-messages 5

# Check if DB is reachable
docker exec stream-processor psql -h postgres-db -U airflow -d clickstream_db -c "SELECT 1"

# Restart if needed
docker-compose restart stream-processor
Start-Sleep -Seconds 10
docker-compose logs stream-processor --tail 20
```
**Expected Time:** 1-2 minutes

---

## SCENARIO 8: Run Troubleshooting Diagnostics
```powershell
# Quick check of everything
./troubleshoot.ps1 -Service all

# Check specific service
./troubleshoot.ps1 -Service postgres
./troubleshoot.ps1 -Service kafka
./troubleshoot.ps1 -Service airflow
./troubleshoot.ps1 -Service processor

# Generate full diagnostic report
./troubleshoot.ps1 -Service report
```
**Output:** Creates `diagnostic_report_*.txt`

---

## SCENARIO 9: Check What's Running & Status
```powershell
# All services status
docker-compose ps

# Service health
docker-compose ps | Select-String "healthy|Up"

# Row counts in database
docker exec postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT COUNT(*) FROM product_metrics;
   SELECT COUNT(*) FROM user_segments;
   SELECT COUNT(*) FROM daily_product_summary;"
```

---

## SCENARIO 10: Verify Data is Flowing
```powershell
# 1. Producer generating events?
docker-compose logs python-producer --tail 10 | Select-String "event"

# 2. Kafka has events?
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic clickstream_topic --from-beginning --max-messages 1

# 3. Stream processor reading?
docker-compose logs stream-processor --tail 10 | Select-String "Successfully wrote"

# 4. Database has records?
docker exec postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT COUNT(*), MAX(processed_timestamp) FROM product_metrics;"
```

---

## MONITORING CHECKLIST

### Before Restart
- [ ] Backup important logs: `docker-compose logs > backup_$(date).txt`
- [ ] Note current data counts
- [ ] Check available disk space

### During Startup
- [ ] Monitor logs: `docker-compose logs -f`
- [ ] All services should be `Up` or `healthy`
- [ ] No `ERROR` or `Exception` messages

### After Startup (Verification)
- [ ] `docker-compose ps` shows all 9 services
- [ ] All services have "Up" status
- [ ] Database: 3 tables exist
- [ ] Kafka: `clickstream_topic` exists
- [ ] Data flow: `product_metrics` has records
- [ ] Airflow UI: Accessible at http://localhost:18080

### If Something Goes Wrong
1. Run: `./troubleshoot.ps1 -Service all`
2. Check: `docker-compose logs SERVICE_NAME --tail 100`
3. Identify: Error messages or connection failures
4. Fix: Based on error type (see DEPLOYMENT_GUIDE.md)

---

## EMERGENCY COMMANDS

```powershell
# Kill everything and start clean
docker-compose down -v && docker-compose up -d && Start-Sleep -Seconds 90

# View real-time logs from all services
docker-compose logs -f --tail 50

# Restart everything
docker-compose restart

# Enter a service shell for debugging
docker exec -it CONTAINER_NAME bash

# Force rebuild all images
docker-compose build --no-cache

# Remove stopped containers
docker container prune -f

# Free up disk space (CAUTION!)
docker system prune -a --volumes
```

---

## EXPECTED STARTUP TIMES

| Phase | Service(s) | Time | Status |
|-------|-----------|------|--------|
| 1 | Infrastructure (Zookeeper, Kafka, Postgres, Redis) | 60s | Should see "healthy" |
| 2 | Airflow (WebServer, Scheduler) | 45s | Should see "Up" |
| 3 | Data (Producer, Processor) | 30s | Ready to start processing |
| **Total** | All 9 services | ~2-3 min | Full system ready |

---

## COMMON PORT MAPPINGS

| Service | Internal | External | Purpose |
|---------|----------|----------|---------|
| Airflow | 8080 | 18080 | Web UI (http://localhost:18080) |
| PostgreSQL | 5432 | 5432 | Database access |
| Kafka | 9092 | 9092 | Broker access |
| Zookeeper | 2181 | - | Kafka coordination |
| PgAdmin | 80 | 5050 | Database UI (http://localhost:5050) |
| Redis | 6379 | 6379 | Caching/Message broker |

---

## CREDENTIALS & ACCESS

| Service | User | Password | Where |
|---------|------|----------|-------|
| Airflow | admin | admin | http://localhost:18080 |
| PostgreSQL | airflow | airflow | localhost:5432 |
| PgAdmin | pgadmin@pgadmin.com | admin | http://localhost:5050 |
| Kafka | N/A | N/A | localhost:9092 |

---

## IF YOU'RE STUCK

1. **Read the error message carefully** - Most issues have clear error text
2. **Check logs**: `./troubleshoot.ps1 -Service all`
3. **Create diagnostic report**: `./troubleshoot.ps1 -Service report`
4. **Try restart**: `docker-compose restart SERVICE_NAME`
5. **Last resort**: `docker-compose down -v && docker-compose up -d`

Good luck! 🚀
