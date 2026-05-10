#!/bin/bash

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Utility functions
print_header() {
    echo -e "${BLUE}================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Check if Docker is running
check_docker() {
    print_header "Checking Docker Installation"
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed"
        return 1
    fi
    print_success "Docker is installed"
    
    if ! docker ps &> /dev/null; then
        print_error "Docker daemon is not running"
        return 1
    fi
    print_success "Docker daemon is running"
    
    return 0
}

# Build containers
build_containers() {
    print_header "Building Docker Containers"
    
    if docker-compose build; then
        print_success "All containers built successfully"
        return 0
    else
        print_error "Failed to build containers"
        return 1
    fi
}

# Start services
start_services() {
    print_header "Starting Services"
    
    if docker-compose up -d; then
        print_success "Services started successfully"
        sleep 5
        return 0
    else
        print_error "Failed to start services"
        return 1
    fi
}

# Check service health
check_service_health() {
    print_header "Checking Service Health"
    
    local services=("zookeeper" "kafka" "postgres-db" "redis" "spark-master" "airflow-webserver" "airflow-scheduler")
    
    for service in "${services[@]}"; do
        if docker-compose ps $service | grep -q "Up"; then
            print_success "$service is running"
        else
            print_warning "$service is not healthy"
        fi
    done
}

# Verify Kafka
verify_kafka() {
    print_header "Verifying Kafka"
    
    echo "Checking if Kafka is accepting connections..."
    if docker exec -it kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092 &> /dev/null; then
        print_success "Kafka broker is responsive"
        
        echo "Checking for clickstream_topic..."
        if docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092 | grep -q "clickstream_topic"; then
            print_success "clickstream_topic exists"
        else
            print_warning "clickstream_topic not found yet"
        fi
    else
        print_error "Kafka broker is not responsive"
    fi
}

# Verify PostgreSQL
verify_postgres() {
    print_header "Verifying PostgreSQL"
    
    echo "Checking database connectivity..."
    if docker exec postgres-db pg_isready -U airflow &> /dev/null; then
        print_success "PostgreSQL is responsive"
        
        echo "Checking for required tables..."
        if docker exec -it postgres-db psql -U airflow -d clickstream_db -c "SELECT 1 FROM product_metrics LIMIT 1;" &> /dev/null; then
            print_success "product_metrics table exists"
        else
            print_warning "product_metrics table not found yet"
        fi
    else
        print_error "PostgreSQL is not responsive"
    fi
}

# Check producer logs
check_producer() {
    print_header "Producer Status"
    
    if docker-compose logs python-producer 2>/dev/null | grep -q "Total events sent"; then
        print_success "Producer is sending events"
        echo "Recent events:"
        docker-compose logs python-producer 2>/dev/null | tail -5
    else
        print_warning "Producer may not be running yet"
        echo "Full logs:"
        docker-compose logs python-producer --tail=10 2>/dev/null
    fi
}

# Check stream processor
check_processor() {
    print_header "Stream Processor Status"
    
    if docker-compose logs stream-processor 2>/dev/null | grep -q "awaitAnyTermination"; then
        print_success "Stream processor is running"
    else
        print_warning "Stream processor may still be initializing"
    fi
    
    if docker-compose logs stream-processor 2>/dev/null | grep -q "Flash Sale"; then
        print_success "Flash Sale triggers detected!"
        echo "Recent triggers:"
        docker-compose logs stream-processor 2>/dev/null | grep "Flash Sale" | tail -5
    fi
}

# Show access URLs
show_access_urls() {
    print_header "Access Points"
    
    echo -e "${GREEN}Web Interfaces:${NC}"
    echo "  Airflow Web UI:    http://localhost:8080"
    echo "    - Username: admin"
    echo "    - Password: admin"
    echo ""
    echo "  Spark Master UI:   http://localhost:8081"
    echo ""
    echo -e "${GREEN}Database Access:${NC}"
    echo "  PostgreSQL: localhost:5432"
    echo "    - User: airflow"
    echo "    - Password: airflow"
    echo ""
    echo -e "${GREEN}Message Broker:${NC}"
    echo "  Kafka: kafka:29092 (inside containers)"
    echo "  Kafka: localhost:9092 (from host)"
}

# Full diagnostic
run_diagnostics() {
    check_docker || exit 1
    check_service_health
    verify_kafka
    verify_postgres
    check_producer
    check_processor
    show_access_urls
}

# Main menu
show_menu() {
    echo ""
    print_header "E-Commerce Clickstream System - Deployment Helper"
    echo "1. Check Docker installation"
    echo "2. Build containers"
    echo "3. Start services"
    echo "4. Full setup (build + start)"
    echo "5. Run diagnostics"
    echo "6. Check service health"
    echo "7. Verify Kafka"
    echo "8. Verify PostgreSQL"
    echo "9. View producer logs"
    echo "10. View stream processor logs"
    echo "11. Stop services"
    echo "12. Show access URLs"
    echo "13. Exit"
    echo ""
}

# Handle menu selection
case "${1:-}" in
    "1")
        check_docker
        ;;
    "2")
        build_containers
        ;;
    "3")
        start_services
        ;;
    "4")
        check_docker && build_containers && start_services && check_service_health
        ;;
    "5")
        run_diagnostics
        ;;
    "6")
        check_service_health
        ;;
    "7")
        verify_kafka
        ;;
    "8")
        verify_postgres
        ;;
    "9")
        docker-compose logs python-producer --follow
        ;;
    "10")
        docker-compose logs stream-processor --follow
        ;;
    "11")
        docker-compose down
        ;;
    "12")
        show_access_urls
        ;;
    "13")
        echo "Exiting..."
        exit 0
        ;;
    *)
        show_menu
        ;;
esac
