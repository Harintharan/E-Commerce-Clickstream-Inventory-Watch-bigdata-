"""
Airflow DAG for E-Commerce Clickstream Batch Processing
Categorizes users and generates daily product summaries
"""

from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

import psycopg2
from psycopg2.extras import RealDictCursor


# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for DAG
default_args = {
    'owner': 'data_engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# DAG definition
dag = DAG(
    'clickstream_daily_batch',
    default_args=default_args,
    description='Daily batch processing for user segmentation and product analytics',
    schedule_interval='0 23 * * *',  # Daily at 11 PM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['clickstream', 'batch', 'analytics'],
)

# Configuration
DB_HOST = 'postgres-db'
DB_PORT = 5432
DB_USER = 'airflow'
DB_PASSWORD = 'airflow'
DB_NAME = 'clickstream_db'


class DatabaseConnector:
    """Database connection manager with error handling"""

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self) -> None:
        """Establish database connection with retry logic"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                connect_timeout=10
            )
            logger.info(f"Connected to PostgreSQL: {self.host}:{self.port}/{self.database}")
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def execute_query(self, query: str, params: tuple = None, fetch: bool = False) -> Any:
        """Execute SQL query with error handling"""
        try:
            if not self.connection:
                self.connect()

            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                self.connection.commit()
                if fetch:
                    return cursor.fetchall()
                return None
        except psycopg2.Error as e:
            logger.error(f"Database error: {e}")
            if self.connection:
                self.connection.rollback()
            raise

    def close(self) -> None:
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")


def create_tables(**context) -> None:
    """Create necessary tables if they don't exist"""
    try:
        db = DatabaseConnector(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
        db.connect()

        # Create product_metrics table
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS product_metrics (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                window_start TIMESTAMP NOT NULL,
                window_end TIMESTAMP NOT NULL,
                view_count INTEGER DEFAULT 0,
                cart_count INTEGER DEFAULT 0,
                purchase_count INTEGER DEFAULT 0,
                total_events INTEGER DEFAULT 0,
                conversion_rate DECIMAL(5, 2) DEFAULT 0.0,
                flash_sale_suggested BOOLEAN DEFAULT FALSE,
                processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(product_id, window_start, window_end)
            );
        """)

        # Create user_segments table
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS user_segments (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                segment_type VARCHAR(50) NOT NULL,
                segment_date DATE NOT NULL,
                view_count INTEGER DEFAULT 0,
                purchase_count INTEGER DEFAULT 0,
                products_viewed INTEGER DEFAULT 0,
                products_purchased INTEGER DEFAULT 0,
                processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, segment_date)
            );
        """)

        # Create daily_product_summary table
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS daily_product_summary (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                summary_date DATE NOT NULL,
                total_views INTEGER DEFAULT 0,
                total_purchases INTEGER DEFAULT 0,
                total_carts INTEGER DEFAULT 0,
                unique_visitors INTEGER DEFAULT 0,
                conversion_rate DECIMAL(5, 2) DEFAULT 0.0,
                flash_sale_recommended BOOLEAN DEFAULT FALSE,
                processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(product_id, summary_date)
            );
        """)

        # Create indexes
        db.execute_query("""
            CREATE INDEX IF NOT EXISTS idx_product_metrics_product_id 
            ON product_metrics(product_id);
        """)
        db.execute_query("""
            CREATE INDEX IF NOT EXISTS idx_user_segments_user_id 
            ON user_segments(user_id);
        """)
        db.execute_query("""
            CREATE INDEX IF NOT EXISTS idx_daily_product_summary_product_id 
            ON daily_product_summary(product_id);
        """)

        logger.info("All tables created successfully")
        db.close()

    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        raise AirflowException(f"Table creation failed: {e}")


def segment_users(**context) -> Dict[str, Any]:
    """
    Segment users into 'Window Shoppers' and 'Buyers'
    Window Shoppers: Users who only viewed products
    Buyers: Users who made at least one purchase
    """
    try:
        execution_date = context['execution_date']
        previous_date = execution_date - timedelta(days=1)
        
        db = DatabaseConnector(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
        db.connect()

        logger.info(f"Segmenting users for date: {previous_date.date()}")

        # Clear previous segment data for the date
        db.execute_query("""
            DELETE FROM user_segments 
            WHERE segment_date = %s;
        """, (previous_date.date(),))

        # Segment users as 'Window Shoppers' (only views, no purchases)
        db.execute_query("""
            INSERT INTO user_segments 
            (user_id, segment_type, segment_date, view_count, purchase_count, 
             products_viewed, products_purchased)
            SELECT 
                user_id,
                'Window Shopper' as segment_type,
                %s as segment_date,
                COUNT(CASE WHEN event_type = 'view' THEN 1 END) as view_count,
                0 as purchase_count,
                COUNT(DISTINCT CASE WHEN event_type = 'view' THEN product_id END) as products_viewed,
                0 as products_purchased
            FROM (
                SELECT user_id, event_type, product_id, timestamp
                FROM product_metrics
                WHERE CAST(window_start AS DATE) = %s
                  AND event_type IN ('view', 'add_to_cart')
                  AND user_id NOT IN (
                    SELECT DISTINCT user_id
                    FROM product_metrics
                    WHERE CAST(window_start AS DATE) = %s
                      AND event_type = 'purchase'
                  )
            ) AS window_shoppers
            GROUP BY user_id
            ON CONFLICT (user_id, segment_date) DO UPDATE SET
                segment_type = EXCLUDED.segment_type,
                view_count = EXCLUDED.view_count,
                products_viewed = EXCLUDED.products_viewed;
        """, (previous_date.date(), previous_date.date(), previous_date.date()))

        # Segment users as 'Buyers' (made at least one purchase)
        db.execute_query("""
            INSERT INTO user_segments 
            (user_id, segment_type, segment_date, view_count, purchase_count, 
             products_viewed, products_purchased)
            SELECT 
                user_id,
                'Buyer' as segment_type,
                %s as segment_date,
                COUNT(CASE WHEN event_type = 'view' THEN 1 END) as view_count,
                COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchase_count,
                COUNT(DISTINCT CASE WHEN event_type = 'view' THEN product_id END) as products_viewed,
                COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN product_id END) as products_purchased
            FROM (
                SELECT user_id, event_type, product_id, timestamp
                FROM product_metrics
                WHERE CAST(window_start AS DATE) = %s
            ) AS buyer_events
            WHERE user_id IN (
                SELECT DISTINCT user_id
                FROM product_metrics
                WHERE CAST(window_start AS DATE) = %s
                  AND event_type = 'purchase'
            )
            GROUP BY user_id
            ON CONFLICT (user_id, segment_date) DO UPDATE SET
                segment_type = EXCLUDED.segment_type,
                view_count = EXCLUDED.view_count,
                purchase_count = EXCLUDED.purchase_count,
                products_viewed = EXCLUDED.products_viewed,
                products_purchased = EXCLUDED.products_purchased;
        """, (previous_date.date(), previous_date.date(), previous_date.date()))

        db.close()

        logger.info(f"User segmentation completed for {previous_date.date()}")

        return {
            'segmentation_date': previous_date.date().isoformat(),
            'status': 'completed'
        }

    except Exception as e:
        logger.error(f"Failed to segment users: {e}")
        raise AirflowException(f"User segmentation failed: {e}")


def generate_daily_summary(**context) -> Dict[str, Any]:
    """
    Generate daily product summary with top products
    """
    try:
        execution_date = context['execution_date']
        previous_date = execution_date - timedelta(days=1)

        db = DatabaseConnector(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
        db.connect()

        logger.info(f"Generating daily summary for {previous_date.date()}")

        # Clear previous summary data for the date
        db.execute_query("""
            DELETE FROM daily_product_summary 
            WHERE summary_date = %s;
        """, (previous_date.date(),))

        # Generate daily summary
        db.execute_query("""
            INSERT INTO daily_product_summary 
            (product_id, summary_date, total_views, total_purchases, total_carts, 
             unique_visitors, conversion_rate, flash_sale_recommended)
            SELECT 
                product_id,
                %s as summary_date,
                SUM(view_count) as total_views,
                SUM(purchase_count) as total_purchases,
                SUM(cart_count) as total_carts,
                COUNT(DISTINCT product_id) as unique_visitors,
                CASE 
                    WHEN SUM(view_count) > 0 
                    THEN (SUM(purchase_count) * 100.0) / SUM(view_count)
                    ELSE 0
                END as conversion_rate,
                CASE 
                    WHEN SUM(view_count) > 100 AND SUM(purchase_count) < 5 
                    THEN TRUE
                    ELSE FALSE
                END as flash_sale_recommended
            FROM product_metrics
            WHERE CAST(window_start AS DATE) = %s
            GROUP BY product_id
            ORDER BY total_views DESC;
        """, (previous_date.date(), previous_date.date()))

        # Get top 5 most viewed products
        top_products = db.execute_query("""
            SELECT 
                product_id, 
                total_views, 
                total_purchases,
                conversion_rate,
                flash_sale_recommended
            FROM daily_product_summary
            WHERE summary_date = %s
            ORDER BY total_views DESC
            LIMIT 5;
        """, (previous_date.date(),), fetch=True)

        logger.info("Daily summary generated successfully")
        logger.info(f"Top 5 products by views: {top_products}")

        db.close()

        return {
            'summary_date': previous_date.date().isoformat(),
            'top_products_count': len(top_products) if top_products else 0,
            'status': 'completed'
        }

    except Exception as e:
        logger.error(f"Failed to generate daily summary: {e}")
        raise AirflowException(f"Daily summary generation failed: {e}")


def validate_data_quality(**context) -> None:
    """
    Validate data quality of processing
    """
    try:
        execution_date = context['execution_date']
        previous_date = execution_date - timedelta(days=1)

        db = DatabaseConnector(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
        db.connect()

        # Check for data completeness
        metrics_count = db.execute_query("""
            SELECT COUNT(*) as count
            FROM product_metrics
            WHERE CAST(window_start AS DATE) = %s;
        """, (previous_date.date(),), fetch=True)[0]['count']

        segments_count = db.execute_query("""
            SELECT COUNT(*) as count
            FROM user_segments
            WHERE segment_date = %s;
        """, (previous_date.date(),), fetch=True)[0]['count']

        summary_count = db.execute_query("""
            SELECT COUNT(*) as count
            FROM daily_product_summary
            WHERE summary_date = %s;
        """, (previous_date.date(),), fetch=True)[0]['count']

        logger.info(f"Data Quality Check for {previous_date.date()}:")
        logger.info(f"  Product metrics records: {metrics_count}")
        logger.info(f"  User segments records: {segments_count}")
        logger.info(f"  Daily summaries records: {summary_count}")

        if metrics_count == 0:
            logger.warning("No product metrics data found for the date")

        db.close()

    except Exception as e:
        logger.error(f"Data quality validation failed: {e}")
        raise AirflowException(f"Data quality check failed: {e}")


# Task 1: Create tables
create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag,
)

# Task 2: Segment users
segment_users_task = PythonOperator(
    task_id='segment_users',
    python_callable=segment_users,
    provide_context=True,
    dag=dag,
)

# Task 3: Generate daily summary
generate_summary_task = PythonOperator(
    task_id='generate_daily_summary',
    python_callable=generate_daily_summary,
    provide_context=True,
    dag=dag,
)

# Task 4: Validate data quality
data_quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
create_tables_task >> segment_users_task
create_tables_task >> generate_summary_task
segment_users_task >> data_quality_task
generate_summary_task >> data_quality_task
