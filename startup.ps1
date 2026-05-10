#!/usr/bin/env pwsh
# Automated Docker Compose Startup Script with Health Checks

param(
    [string]$Mode = "safe",  # safe or clean
    [int]$Timeout = 120      # seconds to wait for services
)

Write-Host "================================" -ForegroundColor Cyan
Write-Host "Docker Compose Startup Script" -ForegroundColor Cyan
Write-Host "Mode: $Mode | Timeout: ${Timeout}s" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan

# Function to check service health
function Test-ServiceHealth {
    param([string]$ServiceName, [int]$MaxAttempts = 10)
    
    $attempts = 0
    while ($attempts -lt $MaxAttempts) {
        $status = docker-compose ps $ServiceName | Select-String "Up|healthy"
        if ($status) {
            return $true
        }
        $attempts++
        Write-Host "  ⏳ $ServiceName: Waiting... ($attempts/$MaxAttempts)" -ForegroundColor Yellow
        Start-Sleep -Seconds 3
    }
    return $false
}

# Step 1: Shutdown
Write-Host "`n[STEP 1] Shutting Down..." -ForegroundColor Cyan
if ($Mode -eq "clean") {
    Write-Host "🧹 Removing volumes (CLEAN MODE - DATA WILL BE LOST)" -ForegroundColor Red
    docker-compose down -v
} else {
    Write-Host "💾 Keeping volumes (SAFE MODE - Data preserved)" -ForegroundColor Green
    docker-compose down
}
Start-Sleep -Seconds 5

# Step 2: Startup Infrastructure
Write-Host "`n[STEP 2] Starting Infrastructure (Zookeeper, Kafka, PostgreSQL)..." -ForegroundColor Cyan
docker-compose up -d zookeeper kafka postgres-db redis

Write-Host "⏳ Waiting for infrastructure to be ready (60 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 60

# Verify infrastructure
Write-Host "`n[VERIFY] Infrastructure Status:" -ForegroundColor Cyan
$infra_ok = $true
@("zookeeper", "kafka", "postgres-db", "redis") | ForEach-Object {
    if (Test-ServiceHealth $_) {
        Write-Host "  ✅ $_" -ForegroundColor Green
    } else {
        Write-Host "  ❌ $_ - FAILED TO START" -ForegroundColor Red
        $infra_ok = $false
    }
}

if (-not $infra_ok) {
    Write-Host "`n⚠️  Infrastructure failed. Checking logs..." -ForegroundColor Red
    docker-compose logs postgres-db --tail 30
    exit 1
}

# Step 3: Startup Airflow
Write-Host "`n[STEP 3] Starting Airflow Services..." -ForegroundColor Cyan
docker-compose up -d pgadmin airflow-webserver airflow-scheduler
Start-Sleep -Seconds 45

# Verify Airflow
Write-Host "`n[VERIFY] Airflow Status:" -ForegroundColor Cyan
@("airflow-webserver", "airflow-scheduler") | ForEach-Object {
    if (Test-ServiceHealth $_) {
        Write-Host "  ✅ $_" -ForegroundColor Green
    } else {
        Write-Host "  ❌ $_ - FAILED TO START" -ForegroundColor Red
    }
}

# Step 4: Startup Data Services
Write-Host "`n[STEP 4] Starting Data Services (Producer, Stream Processor)..." -ForegroundColor Cyan
docker-compose up -d python-producer stream-processor
Start-Sleep -Seconds 30

# Step 5: Comprehensive Verification
Write-Host "`n[STEP 5] Running Comprehensive Checks..." -ForegroundColor Cyan

# Check 5.1: All Services Running
Write-Host "`n  📊 Service Status:" -ForegroundColor Cyan
$all_running = $true
$services = @("zookeeper", "kafka", "postgres-db", "redis", "pgadmin", "airflow-webserver", "airflow-scheduler", "python-producer", "stream-processor")
$services | ForEach-Object {
    $status = docker-compose ps $_ | Select-String "Up"
    if ($status) {
        Write-Host "    ✅ $_" -ForegroundColor Green
    } else {
        Write-Host "    ❌ $_" -ForegroundColor Red
        $all_running = $false
    }
}

# Check 5.2: Database Schema
Write-Host "`n  🗄️  Database Schema:" -ForegroundColor Cyan
try {
    $tables = docker exec postgres-db psql -U airflow -d clickstream_db -c "\dt" 2>&1 | Select-String "product_metrics|user_segments|daily_product_summary"
    if ($tables.Count -eq 3) {
        Write-Host "    ✅ All 3 tables exist" -ForegroundColor Green
    } else {
        Write-Host "    ⚠️  Only $($tables.Count) tables found" -ForegroundColor Yellow
    }
} catch {
    Write-Host "    ❌ Database check failed: $_" -ForegroundColor Red
}

# Check 5.3: Kafka Topic
Write-Host "`n  🔗 Kafka Topic:" -ForegroundColor Cyan
try {
    $topic = docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 2>&1 | Select-String "clickstream_topic"
    if ($topic) {
        Write-Host "    ✅ clickstream_topic exists" -ForegroundColor Green
    } else {
        Write-Host "    ❌ clickstream_topic not found" -ForegroundColor Red
    }
} catch {
    Write-Host "    ⚠️  Kafka check failed" -ForegroundColor Yellow
}

# Check 5.4: Data Flow
Write-Host "`n  📈 Data Flow (checking product_metrics):" -ForegroundColor Cyan
Write-Host "    ⏳ Waiting 30 seconds for data..." -ForegroundColor Yellow
Start-Sleep -Seconds 30
try {
    $count = docker exec postgres-db psql -U airflow -d clickstream_db -c "SELECT COUNT(*) as count FROM product_metrics;" 2>&1 | Select-String -Pattern "[0-9]+" | Select-Object -First 1
    if ($count -match "\d+") {
        $record_count = [int]($matches[0])
        if ($record_count -gt 0) {
            Write-Host "    ✅ $record_count records in product_metrics" -ForegroundColor Green
        } else {
            Write-Host "    ⚠️  No data yet in product_metrics (may take 60 seconds)" -ForegroundColor Yellow
        }
    }
} catch {
    Write-Host "    ⚠️  Data check failed" -ForegroundColor Yellow
}

# Final Summary
Write-Host "`n================================" -ForegroundColor Cyan
if ($all_running) {
    Write-Host "✅ STARTUP SUCCESSFUL" -ForegroundColor Green
    Write-Host "`nNext Steps:" -ForegroundColor Cyan
    Write-Host "  1. Open Airflow UI: http://localhost:18080 (admin/admin)" -ForegroundColor White
    Write-Host "  2. Check producer logs: docker-compose logs python-producer -f" -ForegroundColor White
    Write-Host "  3. Check stream processor: docker-compose logs stream-processor -f" -ForegroundColor White
    Write-Host "  4. Trigger DAG manually in Airflow UI" -ForegroundColor White
    Write-Host "`nMonitor all services:" -ForegroundColor White
    Write-Host "  docker-compose logs -f --tail 50" -ForegroundColor White
} else {
    Write-Host "❌ STARTUP INCOMPLETE - SOME SERVICES FAILED" -ForegroundColor Red
    Write-Host "`nRun for details:" -ForegroundColor Yellow
    Write-Host "  docker-compose logs" -ForegroundColor White
}
Write-Host "================================" -ForegroundColor Cyan
