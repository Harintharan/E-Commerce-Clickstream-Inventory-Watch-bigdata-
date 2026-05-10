# 📖 Complete Documentation Index

## 🎯 START HERE

**New to the project?** Read this section first!

1. **README_DEPLOYMENT.md** ← Start here for overview
2. **QUICK_REFERENCE.md** ← Day-to-day commands
3. **DEPLOYMENT_GUIDE.md** ← Detailed explanations

---

## 🚀 GETTING STARTED (5 minutes)

### Quick Start
```powershell
# Option 1: Automated (Recommended)
./startup.ps1 -Mode safe

# Option 2: Manual
docker-compose down
docker-compose up -d
Start-Sleep -Seconds 90
docker-compose ps
```

### Verify Everything Works
```powershell
./troubleshoot.ps1 -Service all
```

---

## 📚 Documentation Map

### For Deployment & Startup
| Document | Purpose | Best For |
|----------|---------|----------|
| **README_DEPLOYMENT.md** | Overview & summary | New users, big picture |
| **DEPLOYMENT_GUIDE.md** | Step-by-step guide | Detailed walkthrough |
| **QUICK_REFERENCE.md** | Common scenarios | Day-to-day use |

### For Troubleshooting
| Document | Purpose | Best For |
|----------|---------|----------|
| **README_DEPLOYMENT.md** | "Help! Nothing Works!" section | Emergency situations |
| **QUICK_REFERENCE.md** | Common Issues section | Quick fixes |
| **DEPLOYMENT_GUIDE.md** | Issue-by-issue solutions | Understanding the problem |

### For Scripts
| Script | Purpose | When to Use |
|--------|---------|------------|
| **startup.ps1** | Automated deployment | Normal startups |
| **troubleshoot.ps1** | Diagnostics & debugging | When something's broken |

---

## 🔥 MOST COMMON TASKS

### Task 1: Start Everything Fresh (Preserve Data)
```powershell
./startup.ps1 -Mode safe
```
**Time:** 2-3 minutes  
**Result:** System fully operational

### Task 2: Start Fresh (Clear Everything)
```powershell
./startup.ps1 -Mode clean
```
**Time:** 2-3 minutes  
**Result:** Fresh start, all data reset

### Task 3: Something's Wrong - Diagnose It
```powershell
./troubleshoot.ps1 -Service all
```
**Result:** Full diagnostic report with issues highlighted

### Task 4: Check Specific Service
```powershell
./troubleshoot.ps1 -Service postgres    # Database
./troubleshoot.ps1 -Service kafka       # Kafka
./troubleshoot.ps1 -Service airflow     # Airflow
./troubleshoot.ps1 -Service processor   # Stream processor
```

### Task 5: Monitor Real-Time Activity
```powershell
docker-compose logs -f --tail 50
```

### Task 6: Access Airflow Dashboard
```
URL: http://localhost:18080
User: admin
Password: admin
```

### Task 7: Check If Data Is Flowing
```powershell
# Is producer generating events?
docker-compose logs python-producer --tail 5

# Is stream processor writing to DB?
docker-compose logs stream-processor --tail 5 | Select-String "Successfully wrote"

# How many records in database?
docker exec postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT COUNT(*) FROM product_metrics;"
```

---

## 🆘 NEED HELP? FOLLOW THIS

### Step 1: Identify Your Issue
- ❓ **Don't know what to do?** → Read README_DEPLOYMENT.md
- 🔧 **Something is broken?** → Run `./troubleshoot.ps1 -Service all`
- 📖 **Want detailed help?** → Read DEPLOYMENT_GUIDE.md
- ⚡ **Need quick command?** → Check QUICK_REFERENCE.md

### Step 2: Find Your Scenario
In **QUICK_REFERENCE.md**, find your scenario (1-10):
- Scenario 1: Normal start
- Scenario 2: Fresh start
- Scenario 3: Restart one service
- Scenario 4: Database won't start
- Scenario 5: Kafka issues
- Scenario 6: Stream processor not writing
- Scenario 7: Check status
- Scenario 8: Verify data flow
- etc.

### Step 3: Follow the Instructions
Each scenario has exact commands to run

### Step 4: Still Stuck?
1. Run diagnostics: `./troubleshoot.ps1 -Service report`
2. Check generated file: `diagnostic_report_*.txt`
3. Look for errors (marked with 🔴)
4. Cross-reference with DEPLOYMENT_GUIDE.md

---

## 📋 FILE STRUCTURE REFERENCE

```
big_data_mini_project/
├── README_DEPLOYMENT.md          ← START HERE
├── QUICK_REFERENCE.md            ← Most common tasks
├── DEPLOYMENT_GUIDE.md           ← Detailed guide
├── INDEX.md                       ← This file
├── startup.ps1                    ← Automated startup
├── troubleshoot.ps1              ← Diagnostics
│
├── docker-compose.yaml           ← Service definitions
├── init_db.sql                   ← Database schema
├── .env                          ← Configuration
│
├── dags/
│   └── dag_segmentation.py       ← Airflow DAG
├── Dockerfile.airflow            ← Airflow image
├── Dockerfile.producer           ← Producer image
├── Dockerfile.processor          ← Stream processor image
├── producer.py                   ← Event producer
├── stream_processor.py           ← Stream processor
│
└── logs/                         ← Log files
```

---

## 🔗 QUICK LINKS

### Access Services
- **Airflow UI:** http://localhost:18080 (admin/admin)
- **PgAdmin:** http://localhost:5050 (pgadmin@pgadmin.com / admin)
- **Database:** localhost:5432 (airflow/airflow)
- **Kafka:** localhost:9092

### Check Status
```powershell
docker-compose ps                           # All services
./troubleshoot.ps1 -Service all            # Full diagnostics
docker-compose logs -f --tail 50           # Real-time logs
```

### Database Queries
```powershell
# Count records
docker exec postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT COUNT(*) FROM product_metrics; SELECT COUNT(*) FROM user_segments;"

# View top 5 products
docker exec postgres-db psql -U airflow -d clickstream_db -c \
  "SELECT * FROM daily_product_summary ORDER BY total_views DESC LIMIT 5;"

# Check table schema
docker exec postgres-db psql -U airflow -d clickstream_db -c "\dt"
```

---

## 💾 IMPORTANT FILES

### Configuration
- `.env` - Environment variables (DB credentials, SMTP, etc.)
- `docker-compose.yaml` - Service definitions and networking

### Data
- `init_db.sql` - Database schema (runs on first start)
- `logs/` - Application logs

### Source Code
- `producer.py` - Kafka event producer
- `stream_processor.py` - Real-time stream processing
- `dags/dag_segmentation.py` - Batch analytics DAG

### Dockerfiles
- `Dockerfile.airflow` - Airflow webserver & scheduler
- `Dockerfile.producer` - Python producer
- `Dockerfile.processor` - Stream processor

---

## 🎓 LEARNING PATHS

### If You're New to This Stack

1. **Day 1: Understanding**
   - Read: README_DEPLOYMENT.md
   - Understand the data flow
   - See what each service does

2. **Day 2: Operation**
   - Run: `./startup.ps1 -Mode safe`
   - Check: `./troubleshoot.ps1 -Service all`
   - Browse: http://localhost:18080

3. **Day 3: Troubleshooting**
   - Read: QUICK_REFERENCE.md scenarios
   - Learn: Common error patterns
   - Practice: Running diagnostic scripts

4. **Day 4+: Deep Dive**
   - Read: DEPLOYMENT_GUIDE.md
   - Explore: Source code (producer.py, stream_processor.py)
   - Modify: Configuration in .env and docker-compose.yaml

---

## ⚙️ COMMON CONFIGURATIONS

### Edit Environment Variables
```powershell
# File: .env
DB_USER=airflow              # PostgreSQL user
DB_PASSWORD=airflow          # PostgreSQL password
DB_NAME=clickstream_db       # Database name
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
# ... more variables
```

### Modify Compose Services
```yaml
# File: docker-compose.yaml
services:
  postgres-db:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: ${DB_NAME}
      # ... other config
  # ... other services
```

### Adjust DAG Schedule
In `dags/dag_segmentation.py`, find:
```python
schedule_interval='0 23 * * *'  # Daily at 23:00 UTC
```

---

## 📊 SYSTEM ARCHITECTURE

```
                    Producer (generates events)
                           ↓
                    Kafka (message broker)
                           ↓
                 Stream Processor (1-min windows)
                           ↓
                 PostgreSQL (product_metrics)
                           ↓
                    Airflow DAG (batch)
                           ↓
        ┌─────────────────┬─────────────────┐
        ↓                 ↓                 ↓
   user_segments    daily_summary    send_email
```

---

## 🆘 EMERGENCY PROCEDURES

### If Everything Is Broken
```powershell
# Option 1: Restart everything
docker-compose restart

# Option 2: Stop and start
docker-compose down
Start-Sleep -Seconds 10
docker-compose up -d
Start-Sleep -Seconds 90

# Option 3: Clean slate
docker-compose down -v
docker-compose up -d
Start-Sleep -Seconds 90
```

### Create Full Report
```powershell
./troubleshoot.ps1 -Service report
# Generates: diagnostic_report_YYYYMMDD_HHMMSS.txt
```

---

## 📞 HOW TO READ OUTPUT

### docker-compose ps Output
```
NAME                 SERVICE           STATUS
postgres-db          postgres-db       Up 2 minutes (healthy)
kafka                kafka             Up 2 minutes (healthy)
airflow-scheduler    airflow-scheduler Up 2 minutes
```
- 🟢 `Up X minutes` = Running
- 🟢 `(healthy)` = Passed health check
- 🔴 `Exit code X` = Crashed
- 🟡 `health: starting` = Still initializing

### Logs
```
2026-05-10 07:43:41,303 INFO - Segmenting products for date: 2026-05-10
2026-05-10 07:43:41,558 INFO - Product segmentation completed
```
- 🟢 `INFO` = Normal operation
- 🟡 `WARNING` = Something unusual but not critical
- 🔴 `ERROR` = Something failed

---

## 🎯 SUCCESS CHECKLIST

After running `./startup.ps1 -Mode safe`:

- [ ] All 9 services show "Up" status
- [ ] No services show "Exit code"
- [ ] `./troubleshoot.ps1 -Service all` shows mostly ✅
- [ ] Airflow UI loads at http://localhost:18080
- [ ] Can log in with admin/admin
- [ ] Database has records in product_metrics
- [ ] No error messages in logs

---

## 📞 STILL NEED HELP?

1. **Check this index** - Find your scenario
2. **Run diagnostics** - `./troubleshoot.ps1 -Service all`
3. **Read DEPLOYMENT_GUIDE.md** - Find the error section
4. **Check logs** - `docker-compose logs SERVICE_NAME --tail 50`
5. **Try restart** - `docker-compose restart SERVICE_NAME`
6. **Last resort** - `docker-compose down -v && docker-compose up -d`

---

## Version Info
- Docker Compose: v3.8
- PostgreSQL: 15-alpine
- Kafka: 7.5.0
- Airflow: 2.7.2
- Python: 3.11
- Last Updated: 2026-05-10

**You've got this! 🚀**
