"""
Configuration module for the Clickstream project
Centralized configuration for all components
"""

import os
from typing import Dict, Any

# Environment
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'

# Kafka Configuration
KAFKA_CONFIG: Dict[str, Any] = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092').split(','),
    'topic': os.getenv('KAFKA_TOPIC', 'clickstream_topic'),
    'consumer_group': 'clickstream_consumer_group',
    'auto_offset_reset': 'latest',
    'enable_auto_commit': True,
    'session_timeout_ms': 30000,
}

# Database Configuration
DATABASE_CONFIG: Dict[str, Any] = {
    'host': os.getenv('DB_HOST', 'postgres-db'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'user': os.getenv('DB_USER', 'airflow'),
    'password': os.getenv('DB_PASSWORD', 'airflow'),
    'database': os.getenv('DB_NAME', 'clickstream_db'),
    'connection_timeout': 10,
}

# Producer Configuration
PRODUCER_CONFIG: Dict[str, Any] = {
    'batch_size': int(os.getenv('BATCH_SIZE', 10)),
    'batch_interval_seconds': int(os.getenv('BATCH_INTERVAL_SECONDS', 5)),
    'num_users': 100,
    'num_products': 50,
    'event_types': ['view', 'add_to_cart', 'purchase'],
}

# Stream Processor Configuration
PROCESSOR_CONFIG: Dict[str, Any] = {
    'window_duration': '10 minutes',
    'sliding_interval': '1 minute',
    'watermark_delay': '5 minutes',
    'flash_sale_view_threshold': 100,
    'flash_sale_purchase_threshold': 5,
    'checkpoint_dir': '/app/checkpoint',
}

# Airflow Configuration
AIRFLOW_CONFIG: Dict[str, Any] = {
    'home': '/airflow',
    'dags_folder': '/airflow/dags',
    'base_log_folder': '/airflow/logs',
    'parallelism': 4,
    'max_active_runs_per_dag': 2,
}

# Logging Configuration
LOGGING_CONFIG: Dict[str, Any] = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': '/app/logs/clickstream.log',
}

# Feature Flags
FEATURES: Dict[str, bool] = {
    'enable_flash_sale_detection': True,
    'enable_anomaly_injection': True,
    'enable_data_quality_checks': True,
}

# Monitoring
MONITORING_CONFIG: Dict[str, Any] = {
    'metrics_enabled': True,
    'health_check_interval': 30,  # seconds
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
