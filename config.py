"""
Configuration module for the Clickstream project
Centralized configuration for all components
Loads all settings from .env file
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Environment
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'

# Kafka Configuration
KAFKA_CONFIG: Dict[str, Any] = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092').split(','),
    'topic': os.getenv('KAFKA_TOPIC', 'clickstream_topic'),
    'consumer_group': os.getenv('KAFKA_CONSUMER_GROUP', 'clickstream_consumer_group'),
    'auto_offset_reset': 'latest',
    'enable_auto_commit': True,
    'session_timeout_ms': 30000,
}

# Database Configuration
DATABASE_CONFIG: Dict[str, Any] = {
    'host': os.getenv('DB_HOST', 'postgres-db'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'user': os.getenv('DB_USER', 'airflow'),
    'password': os.getenv('DB_PASSWORD', 'airflow'),
    'database': os.getenv('DB_NAME', 'clickstream_db'),
    'connection_timeout': int(os.getenv('DB_CONNECTION_TIMEOUT', '10')),
}

# Producer Configuration
PRODUCER_CONFIG: Dict[str, Any] = {
    'batch_size': int(os.getenv('BATCH_SIZE', '25')),
    'batch_interval_seconds': int(os.getenv('BATCH_INTERVAL_SECONDS', '1')),
    'num_users': int(os.getenv('NUM_USERS', '50')),
    'num_products': int(os.getenv('NUM_PRODUCTS', '20')),
    'event_types': os.getenv('EVENT_TYPES', 'view,add_to_cart,purchase').split(','),
    'anomaly_view_threshold': int(os.getenv('ANOMALY_VIEW_THRESHOLD', '100')),
    'anomaly_purchase_threshold': int(os.getenv('ANOMALY_PURCHASE_THRESHOLD', '5')),
}

# Stream Processor Configuration
PROCESSOR_CONFIG: Dict[str, Any] = {
    'window_duration': os.getenv('WINDOW_DURATION', '10 minutes'),
    'sliding_interval': os.getenv('SLIDING_INTERVAL', '1 minute'),
    'watermark_delay': os.getenv('WATERMARK_DELAY', '30 seconds'),
    'flash_sale_view_threshold': int(os.getenv('FLASH_SALE_VIEW_THRESHOLD', '100')),
    'flash_sale_purchase_threshold': int(os.getenv('FLASH_SALE_PURCHASE_THRESHOLD', '5')),
    'checkpoint_dir': os.getenv('CHECKPOINT_DIR', '/app/checkpoint'),
    'fallback_flush_batch_size': int(os.getenv('FALLBACK_FLUSH_BATCH_SIZE', '100')),
    'fallback_evidence_interval': int(os.getenv('FALLBACK_EVIDENCE_INTERVAL', '25')),
    'window_duration_minutes': int(os.getenv('WINDOW_DURATION_MINUTES', '10')),
    'sliding_interval_minutes': int(os.getenv('SLIDING_INTERVAL_MINUTES', '1')),
}

# Airflow Configuration
AIRFLOW_CONFIG: Dict[str, Any] = {
    'home': os.getenv('AIRFLOW_HOME', '/airflow'),
    'dags_folder': os.getenv('AIRFLOW_DAGS_FOLDER', '/airflow/dags'),
    'base_log_folder': os.getenv('AIRFLOW_BASE_LOG_FOLDER', '/airflow/logs'),
    'parallelism': int(os.getenv('AIRFLOW_PARALLELISM', '4')),
    'max_active_runs_per_dag': int(os.getenv('AIRFLOW_MAX_ACTIVE_RUNS_PER_DAG', '2')),
    'dag_schedule_interval': os.getenv('AIRFLOW_DAG_SCHEDULE_INTERVAL', '0 23 * * *'),
    'dag_start_date': os.getenv('AIRFLOW_DAG_START_DATE', '2024-01-01'),
}

# Logging Configuration
LOGGING_CONFIG: Dict[str, Any] = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'format': os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
    'file': os.getenv('LOG_FILE', '/app/logs/clickstream.log'),
}

# Feature Flags
FEATURES: Dict[str, bool] = {
    'enable_flash_sale_detection': os.getenv('ENABLE_FLASH_SALE_DETECTION', 'True').lower() == 'true',
    'enable_anomaly_injection': os.getenv('ENABLE_ANOMALY_INJECTION', 'True').lower() == 'true',
    'enable_data_quality_checks': os.getenv('ENABLE_DATA_QUALITY_CHECKS', 'True').lower() == 'true',
}

# Monitoring
MONITORING_CONFIG: Dict[str, Any] = {
    'metrics_enabled': os.getenv('METRICS_ENABLED', 'True').lower() == 'true',
    'health_check_interval': int(os.getenv('HEALTH_CHECK_INTERVAL', '30')),
}

# Report Configuration
REPORT_CONFIG: Dict[str, Any] = {
    'report_dir': os.getenv('REPORT_DIR', '/airflow/logs/reports'),
    'report_retention_days': int(os.getenv('REPORT_RETENTION_DAYS', '30')),
    'smtp_host': os.getenv('SMTP_HOST', 'smtp.gmail.com'),
    'smtp_port': int(os.getenv('SMTP_PORT', '587')),
    'smtp_user': os.getenv('SMTP_USER', 'your-email@gmail.com'),
    'smtp_password': os.getenv('SMTP_PASSWORD', 'your-app-password'),
    'report_recipient': os.getenv('REPORT_RECIPIENT', 'admin@example.com'),
    'report_sender': os.getenv('REPORT_SENDER', os.getenv('SMTP_USER', 'your-email@gmail.com')),
}


def get_config(section: str) -> Dict[str, Any]:
    """Get configuration section"""
    sections = {
        'kafka': KAFKA_CONFIG,
        'database': DATABASE_CONFIG,
        'producer': PRODUCER_CONFIG,
        'processor': PROCESSOR_CONFIG,
        'airflow': AIRFLOW_CONFIG,
        'logging': LOGGING_CONFIG,
        'features': FEATURES,
        'monitoring': MONITORING_CONFIG,
    }
    return sections.get(section, {})


def print_config():
    """Print all configuration"""
    print("\n" + "="*70)
    print("CLICKSTREAM PROJECT CONFIGURATION")
    print("="*70)
    print(f"\nEnvironment: {ENVIRONMENT}")
    print(f"Debug Mode: {DEBUG}")
    
    print("\n[KAFKA]")
    for k, v in KAFKA_CONFIG.items():
        print(f"  {k}: {v}")
    
    print("\n[DATABASE]")
    for k, v in DATABASE_CONFIG.items():
        if k != 'password':
            print(f"  {k}: {v}")
        else:
            print(f"  {k}: {'*' * len(v)}")
    
    print("\n[PRODUCER]")
    for k, v in PRODUCER_CONFIG.items():
        print(f"  {k}: {v}")
    
    print("\n[STREAMING]")
    for k, v in PROCESSOR_CONFIG.items():
        print(f"  {k}: {v}")
    
    print("\n[FEATURES]")
    for k, v in FEATURES.items():
        print(f"  {k}: {v}")
    
    print("\n" + "="*70 + "\n")


if __name__ == '__main__':
    print_config()
