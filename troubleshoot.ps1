#!/usr/bin/env pwsh
# Troubleshooting & Diagnostics Script

param(
    [string]$Service = "all",  # Service to check: all, postgres, kafka, airflow, processor
    [int]$TailLines = 50       # Number of log lines to show
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Troubleshooting & Diagnostics" -ForegroundColor Cyan
Write-Host "Service: $Service | Log Lines: $TailLines" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Function: Check service status
function Show-ServiceStatus {
    Write-Host "`n[SERVICE STATUS]" -ForegroundColor Cyan
    docker-compose ps
}

# Function: Check Docker resource usage
function Show-ResourceUsage {
    Write-Host "`n[RESOURCE USAGE]" -ForegroundColor Cyan
    docker stats --no-stream | Select-Object -First 10
}

# Function: Check specific service logs
function Show-ServiceLogs {
    param([string]$ServiceName)
    Write-Host "`n[LOGS: $ServiceName]" -ForegroundColor Cyan
    docker-compose logs $ServiceName --tail $TailLines
}

# Function: Check for errors in logs
function Find-Errors {
    param([string]$ServiceName)
    Write-Host "`n[ERRORS IN: $ServiceName]" -ForegroundColor Red
    $errors = docker-compose logs $ServiceName 2>&1 | Select-String -Pattern "ERROR|Exception|FAILED|Traceback|Segmentation"
    if ($errors) {
        Write-Host $errors
    } else {
        Write-Host "  ✅ No errors found" -ForegroundColor Green
    }
}

# Function: Database connectivity check
function Test-DatabaseConnection {
    Write-Host "`n[DATABASE CONNECTIVITY]" -ForegroundColor Cyan
    try {
        $result = docker exec postgres-db psql -U airflow -d clickstream_db -c "SELECT 1"
        Write-Host "  ✅ Database is reachable" -ForegroundColor Green
        
        # Check tables
        Write-Host "`n  Tables:" -ForegroundColor White
        docker exec postgres-db psql -U airflow -d clickstream_db -c "\dt"
        
        # Check table row counts
        Write-Host "`n  Row Counts:" -ForegroundColor White
        docker exec postgres-db psql -U airflow -d clickstream_db -c \
            "SELECT 'product_metrics' as table_name, COUNT(*) as row_count FROM product_metrics UNION ALL
             SELECT 'user_segments', COUNT(*) FROM user_segments UNION ALL
             SELECT 'daily_product_summary', COUNT(*) FROM daily_product_summary;"
    } catch {
        Write-Host "  ❌ Cannot connect to database: $_" -ForegroundColor Red
    }
}

# Function: Kafka connectivity check
function Test-KafkaConnection {
    Write-Host "`n[KAFKA CONNECTIVITY]" -ForegroundColor Cyan
    try {
        $brokers = docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
        Write-Host "  ✅ Kafka broker is reachable" -ForegroundColor Green
        
        # Check topics
        Write-Host "`n  Topics:" -ForegroundColor White
        docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
        
        # Check topic details
        Write-Host "`n  Topic Details (clickstream_topic):" -ForegroundColor White
        docker exec kafka kafka-topics --describe --topic clickstream_topic --bootstrap-server localhost:9092
    } catch {
        Write-Host "  ❌ Cannot connect to Kafka: $_" -ForegroundColor Red
    }
}

# Function: Airflow DAG check
function Test-AirflowDAG {
    Write-Host "`n[AIRFLOW DAG STATUS]" -ForegroundColor Cyan
    try {
        $dags = docker-compose exec -T -u airflow airflow-scheduler airflow dags list 2>&1
        Write-Host $dags
        
        # Check DAG details
        Write-Host "`n[DAG: clickstream_daily_batch]" -ForegroundColor White
        $dag_info = docker-compose exec -T -u airflow airflow-scheduler airflow dags info clickstream_daily_batch 2>&1
        Write-Host $dag_info
    } catch {
        Write-Host "  ⚠️  Could not connect to Airflow: $_" -ForegroundColor Yellow
    }
}

# Function: Port availability check
function Test-PortAvailability {
    Write-Host "`n[PORT AVAILABILITY]" -ForegroundColor Cyan
    $ports = @(
        @{Port=5432; Service="PostgreSQL"; Expected="postgres-db"},
        @{Port=9092; Service="Kafka"; Expected="kafka"},
        @{Port=2181; Service="Zookeeper"; Expected="zookeeper"},
        @{Port=18080; Service="Airflow UI"; Expected="airflow-webserver"},
        @{Port=5050; Service="PgAdmin"; Expected="pgadmin"},
        @{Port=6379; Service="Redis"; Expected="redis"}
    )
    
    $ports | ForEach-Object {
        try {
            $socket = New-Object System.Net.Sockets.TcpClient
            $connection = $socket.BeginConnect("localhost", $_.Port, $null, $null)
            $connection.AsyncWaitHandle.WaitOne(1000, $false) | Out-Null
            if ($socket.Connected) {
                Write-Host "  ✅ Port $($_.Port) ($($_.Service)) - OPEN" -ForegroundColor Green
                $socket.Close()
            } else {
                Write-Host "  ❌ Port $($_.Port) ($($_.Service)) - CLOSED" -ForegroundColor Red
            }
        } catch {
            Write-Host "  ❌ Port $($_.Port) ($($_.Service)) - UNREACHABLE" -ForegroundColor Red
        }
    }
}

# Function: Data flow check
function Test-DataFlow {
    Write-Host "`n[DATA FLOW CHECK]" -ForegroundColor Cyan
    
    # Check producer
    Write-Host "`n  Producer Status:" -ForegroundColor White
    $producer_logs = docker-compose logs python-producer --tail 5 | Select-String -Pattern "event|Traceback"
    if ($producer_logs) {
        Write-Host $producer_logs
    }
    
    # Check stream processor
    Write-Host "`n  Stream Processor Status:" -ForegroundColor White
    $processor_logs = docker-compose logs stream-processor --tail 5 | Select-String -Pattern "wrote|ERROR"
    if ($processor_logs) {
        Write-Host $processor_logs
    } else {
        Write-Host "    ⏳ No data written yet" -ForegroundColor Yellow
    }
    
    # Check database receives data
    Write-Host "`n  Database Record Count:" -ForegroundColor White
    docker exec postgres-db psql -U airflow -d clickstream_db -c \
        "SELECT COUNT(*) as product_metrics_count FROM product_metrics;"
}

# Function: Full diagnostic report
function Create-DiagnosticReport {
    Write-Host "`n[GENERATING DIAGNOSTIC REPORT]" -ForegroundColor Cyan
    
    $report = @"
=================================================================
DIAGNOSTIC REPORT - $(Get-Date)
=================================================================

1. SERVICE STATUS
-----------------------------------------------------------------
$(docker-compose ps)

2. RESOURCE USAGE
-----------------------------------------------------------------
$(docker stats --no-stream | Select-Object -First 10)

3. NETWORK CONNECTIVITY
-----------------------------------------------------------------
Postgres: $(docker exec postgres-db ping -c 1 localhost 2>&1 | Select-String "bytes from")
Kafka: $(docker exec kafka ping -c 1 localhost 2>&1 | Select-String "bytes from")

4. DATABASE STATUS
-----------------------------------------------------------------
$(docker exec postgres-db psql -U airflow -d clickstream_db -c "SELECT version();")

Tables:
$(docker exec postgres-db psql -U airflow -d clickstream_db -c "\dt")

Row Counts:
$(docker exec postgres-db psql -U airflow -d clickstream_db -c \
    "SELECT 'product_metrics', COUNT(*) FROM product_metrics UNION ALL
     SELECT 'user_segments', COUNT(*) FROM user_segments UNION ALL
     SELECT 'daily_product_summary', COUNT(*) FROM daily_product_summary;")

5. KAFKA STATUS
-----------------------------------------------------------------
Topics: $(docker exec kafka kafka-topics --list --bootstrap-server localhost:9092)

6. AIRFLOW STATUS
-----------------------------------------------------------------
$(docker-compose exec -T -u airflow airflow-scheduler airflow dags list 2>&1)

7. ERROR LOG (Last 20 errors)
-----------------------------------------------------------------
$(docker-compose logs 2>&1 | Select-String "ERROR|Exception|FAILED" | Select-Object -Last 20)

8. RECENT LOGS (Last 30 lines per service)
-----------------------------------------------------------------
POSTGRES:
$(docker-compose logs postgres-db --tail 10)

KAFKA:
$(docker-compose logs kafka --tail 10)

PRODUCER:
$(docker-compose logs python-producer --tail 10)

PROCESSOR:
$(docker-compose logs stream-processor --tail 10)

AIRFLOW SCHEDULER:
$(docker-compose logs airflow-scheduler --tail 10)

=================================================================
"@

    $report | Out-File -FilePath "diagnostic_report_$(Get-Date -Format 'yyyyMMdd_HHmmss').txt"
    Write-Host "✅ Report saved!" -ForegroundColor Green
}

# Main execution
switch ($Service) {
    "all" {
        Show-ServiceStatus
        Show-ResourceUsage
        Test-PortAvailability
        Test-DatabaseConnection
        Test-KafkaConnection
        Test-AirflowDAG
        Test-DataFlow
        Create-DiagnosticReport
    }
    "postgres" {
        Show-ServiceStatus
        Find-Errors "postgres-db"
        Test-DatabaseConnection
    }
    "kafka" {
        Show-ServiceStatus
        Find-Errors "kafka"
        Test-KafkaConnection
    }
    "airflow" {
        Show-ServiceStatus
        Find-Errors "airflow-scheduler"
        Find-Errors "airflow-webserver"
        Test-AirflowDAG
    }
    "processor" {
        Show-ServiceStatus
        Find-Errors "stream-processor"
        Test-DataFlow
    }
    "report" {
        Create-DiagnosticReport
    }
    default {
        Write-Host "Unknown service: $Service" -ForegroundColor Red
        Write-Host "Usage: .\troubleshoot.ps1 -Service <all|postgres|kafka|airflow|processor|report>" -ForegroundColor Yellow
    }
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Diagnostics Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
