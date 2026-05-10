@echo off
REM E-Commerce Clickstream System - Deployment Helper for Windows

setlocal enabledelayedexpansion

color 0A

:menu
cls
echo.
echo =====================================
echo E-Commerce Clickstream System
echo Deployment Helper for Windows
echo =====================================
echo.
echo 1. Check Docker installation
echo 2. Build containers
echo 3. Start services
echo 4. Full setup (build + start)
echo 5. Run diagnostics
echo 6. Check service health
echo 7. Verify Kafka
echo 8. Verify PostgreSQL
echo 9. View producer logs
echo 10. View stream processor logs
echo 11. Stop services
echo 12. Show access URLs
echo 13. Exit
echo.

set /p choice="Enter your choice (1-13): "

if "%choice%"=="1" goto check_docker
if "%choice%"=="2" goto build_containers
if "%choice%"=="3" goto start_services
if "%choice%"=="4" goto full_setup
if "%choice%"=="5" goto diagnostics
if "%choice%"=="6" goto check_health
if "%choice%"=="7" goto verify_kafka
if "%choice%"=="8" goto verify_postgres
if "%choice%"=="9" goto producer_logs
if "%choice%"=="10" goto processor_logs
if "%choice%"=="11" goto stop_services
if "%choice%"=="12" goto access_urls
if "%choice%"=="13" goto end
goto menu

:check_docker
cls
echo.
echo Checking Docker Installation...
docker --version
if %errorlevel% neq 0 (
    echo.
    echo X Docker is not installed
    goto error
)
echo.
echo Docker is installed!
docker ps >nul 2>&1
if %errorlevel% neq 0 (
    echo.
    echo X Docker daemon is not running
    goto error
)
echo + Docker daemon is running
echo.
pause
goto menu

:build_containers
cls
echo.
echo Building Docker Containers...
docker-compose build
if %errorlevel% neq 0 goto error
echo.
echo + All containers built successfully
echo.
pause
goto menu

:start_services
cls
echo.
echo Starting Services...
docker-compose up -d
if %errorlevel% neq 0 goto error
echo.
echo + Services started successfully
echo Waiting 10 seconds for services to initialize...
timeout /t 10 /nobreak
echo.
pause
goto menu

:full_setup
cls
echo.
echo Running Full Setup...
echo Building containers...
docker-compose build
if %errorlevel% neq 0 goto error
echo.
echo Starting services...
docker-compose up -d
if %errorlevel% neq 0 goto error
echo.
echo Waiting 10 seconds for services to initialize...
timeout /t 10 /nobreak
echo.
echo Checking service health...
docker-compose ps
echo.
echo + Full setup completed!
echo.
pause
goto menu

:diagnostics
cls
echo.
echo Running Full Diagnostics...
echo.
echo 1. Checking service health...
docker-compose ps
echo.
echo 2. Verifying Kafka...
docker exec -it kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >nul 2>&1
if %errorlevel% equ 0 (
    echo + Kafka broker is responsive
) else (
    echo X Kafka broker is not responsive
)
echo.
echo 3. Verifying PostgreSQL...
docker exec postgres-db pg_isready -U airflow >nul 2>&1
if %errorlevel% equ 0 (
    echo + PostgreSQL is responsive
) else (
    echo X PostgreSQL is not responsive
)
echo.
echo 4. Producer status:
docker-compose logs python-producer 2>nul | find /i "events sent" >nul
if %errorlevel% equ 0 (
    echo + Producer is sending events
) else (
    echo ! Producer may still be initializing
)
echo.
echo 5. Accessing services:
echo.
echo   Airflow Web UI:    http://localhost:8080
echo   Spark Master UI:   http://localhost:8081
echo   PostgreSQL:        localhost:5432
echo.
pause
goto menu

:check_health
cls
echo.
echo Checking Service Health...
docker-compose ps
echo.
pause
goto menu

:verify_kafka
cls
echo.
echo Verifying Kafka...
docker exec -it kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >nul 2>&1
if %errorlevel% equ 0 (
    echo + Kafka broker is responsive
    echo.
    echo Checking for clickstream_topic...
    docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092 2>nul | find "clickstream_topic" >nul
    if %errorlevel% equ 0 (
        echo + clickstream_topic exists
    ) else (
        echo ! clickstream_topic not found yet
    )
) else (
    echo X Kafka broker is not responsive
)
echo.
pause
goto menu

:verify_postgres
cls
echo.
echo Verifying PostgreSQL...
docker exec postgres-db pg_isready -U airflow >nul 2>&1
if %errorlevel% equ 0 (
    echo + PostgreSQL is responsive
    echo.
    echo Checking for required tables...
    for /f %%i in ('docker exec -it postgres-db psql -U airflow -d clickstream_db -c "SELECT COUNT(*) FROM product_metrics;" 2^>nul') do (
        if "%%i" NEQ "" (
            echo + product_metrics table exists with records
        )
    )
) else (
    echo X PostgreSQL is not responsive
)
echo.
pause
goto menu

:producer_logs
cls
echo.
echo Producer Logs (Press Ctrl+C to stop):
docker-compose logs python-producer --follow
goto menu

:processor_logs
cls
echo.
echo Stream Processor Logs (Press Ctrl+C to stop):
docker-compose logs stream-processor --follow
goto menu

:stop_services
cls
echo.
echo Stopping Services...
docker-compose down
echo + Services stopped
echo.
pause
goto menu

:access_urls
cls
echo.
echo =====================================
echo Access Points
echo =====================================
echo.
echo Web Interfaces:
echo   Airflow Web UI:    http://localhost:8080
echo     - Username: admin
echo     - Password: admin
echo.
echo   Spark Master UI:   http://localhost:8081
echo.
echo Database Access:
echo   PostgreSQL:        localhost:5432
echo     - User: airflow
echo     - Password: airflow
echo.
echo Message Broker:
echo   Kafka:             localhost:9092
echo.
pause
goto menu

:error
echo.
echo =====================================
echo ERROR
echo =====================================
echo.
pause
goto menu

:end
echo.
echo Exiting...
exit /b 0
