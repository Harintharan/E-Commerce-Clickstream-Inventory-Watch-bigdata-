"""
Airflow DAG for E-Commerce Clickstream Batch Processing
Categorizes users and generates daily product summaries
"""

from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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

# Report configuration
REPORT_DIR = '/airflow/logs/reports'
SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USER = os.getenv('SMTP_USER', 'your-email@gmail.com')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', 'your-app-password')
REPORT_RECIPIENT = os.getenv('REPORT_RECIPIENT', 'admin@example.com')
REPORT_SENDER = os.getenv('REPORT_SENDER', SMTP_USER)


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


def generate_summary_report(**context) -> Dict[str, Any]:
    """
    Generate formatted text report with top 5 most viewed products
    Exports report to file for archival and email delivery
    """
    try:
        execution_date = context['execution_date']
        previous_date = execution_date - timedelta(days=1)
        summary_date = previous_date.date()

        db = DatabaseConnector(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
        db.connect()

        logger.info(f"Generating summary report for {summary_date}")

        # Create reports directory if it doesn't exist
        os.makedirs(REPORT_DIR, exist_ok=True)

        # Query top 5 most viewed products
        top_products = db.execute_query("""
            SELECT 
                product_id, 
                total_views, 
                total_purchases,
                total_carts,
                conversion_rate,
                flash_sale_recommended
            FROM daily_product_summary
            WHERE summary_date = %s
            ORDER BY total_views DESC
            LIMIT 5;
        """, (summary_date,), fetch=True)

        # Query user segments statistics
        user_stats = db.execute_query("""
            SELECT 
                segment_type,
                COUNT(*) as user_count,
                ROUND(AVG(purchase_count), 2) as avg_purchases,
                ROUND(AVG(view_count), 2) as avg_views
            FROM user_segments
            WHERE segment_date = %s
            GROUP BY segment_type;
        """, (summary_date,), fetch=True)

        db.close()

        # Generate formatted report
        report_filename = f"daily_summary_{summary_date}.txt"
        report_path = os.path.join(REPORT_DIR, report_filename)

        with open(report_path, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write(f"E-COMMERCE CLICKSTREAM - DAILY SUMMARY REPORT\n")
            f.write(f"Date: {summary_date}\n")
            f.write(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
            f.write("=" * 80 + "\n\n")

            # Top 5 Products Section
            f.write("TOP 5 MOST VIEWED PRODUCTS\n")
            f.write("-" * 80 + "\n")
            f.write(f"{'Rank':<6} {'Product ID':<12} {'Views':<10} {'Purchases':<12} {'Conv. Rate':<12} {'Flash Sale':<12}\n")
            f.write("-" * 80 + "\n")

            if top_products:
                for idx, product in enumerate(top_products, 1):
                    flash_sale = "YES" if product['flash_sale_recommended'] else "NO"
                    f.write(
                        f"{idx:<6} "
                        f"{product['product_id']:<12} "
                        f"{product['total_views']:<10} "
                        f"{product['total_purchases']:<12} "
                        f"{product['conversion_rate']:.2f}%{'':<8} "
                        f"{flash_sale:<12}\n"
                    )
            else:
                f.write("No product data available for this period\n")

            f.write("\n")
            f.write("USER SEGMENTATION SUMMARY\n")
            f.write("-" * 80 + "\n")
            f.write(f"{'Segment Type':<20} {'User Count':<15} {'Avg Purchases':<15} {'Avg Views':<15}\n")
            f.write("-" * 80 + "\n")

            if user_stats:
                for stat in user_stats:
                    f.write(
                        f"{stat['segment_type']:<20} "
                        f"{stat['user_count']:<15} "
                        f"{stat['avg_purchases']:<15} "
                        f"{stat['avg_views']:<15}\n"
                    )
            else:
                f.write("No user segmentation data available for this period\n")

            f.write("\n")
            f.write("=" * 80 + "\n")
            f.write("End of Report\n")
            f.write("=" * 80 + "\n")

        logger.info(f"Summary report generated successfully: {report_path}")

        return {
            'report_date': summary_date.isoformat(),
            'report_path': report_path,
            'top_products_count': len(top_products) if top_products else 0,
            'status': 'completed'
        }

    except Exception as e:
        logger.error(f"Failed to generate summary report: {e}")
        raise AirflowException(f"Summary report generation failed: {e}")


def send_summary_email(**context) -> Dict[str, Any]:
    """
    Send formatted email with daily product summary
    """
    try:
        # Get report path from previous task
        task_instance = context['task_instance']
        previous_task_data = task_instance.xcom_pull(task_ids='generate_summary_report')
        
        if not previous_task_data or 'report_path' not in previous_task_data:
            logger.warning("No report path found from previous task")
            return {'status': 'skipped', 'reason': 'No report generated'}

        report_path = previous_task_data['report_path']
        summary_date = previous_task_data['report_date']

        # Read report content
        if not os.path.exists(report_path):
            logger.error(f"Report file not found: {report_path}")
            raise AirflowException(f"Report file not found: {report_path}")

        with open(report_path, 'r') as f:
            report_content = f.read()

        # Create email
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Daily E-Commerce Summary Report - {summary_date}"
        msg['From'] = REPORT_SENDER
        msg['To'] = REPORT_RECIPIENT

        # Create plain text part
        text_part = MIMEText(report_content, 'plain')
        msg.attach(text_part)

        # Create HTML part for better formatting
        html_content = f"""
        <html>
          <body style="font-family: Arial, sans-serif;">
            <h2>E-Commerce Clickstream - Daily Summary Report</h2>
            <p><strong>Report Date:</strong> {summary_date}</p>
            <p><strong>Generated:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
            <hr>
            <pre style="background-color: #f4f4f4; padding: 10px; overflow-x: auto;">
{report_content}
            </pre>
            <hr>
            <p><em>This is an automated report. Please do not reply to this email.</em></p>
          </body>
        </html>
        """
        html_part = MIMEText(html_content, 'html')
        msg.attach(html_part)

        # Send email
        try:
            logger.info(f"Connecting to SMTP server: {SMTP_HOST}:{SMTP_PORT}")
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.send_message(msg)
            logger.info(f"Email sent successfully to {REPORT_RECIPIENT}")
        except smtplib.SMTPAuthenticationError:
            logger.warning(f"SMTP authentication failed. Check SMTP_USER and SMTP_PASSWORD credentials.")
            logger.info(f"Report available at: {report_path}")
            return {'status': 'warning', 'reason': 'SMTP auth failed', 'report_path': report_path}
        except smtplib.SMTPException as e:
            logger.warning(f"SMTP error occurred: {e}. Report saved at: {report_path}")
            return {'status': 'warning', 'reason': f'SMTP error: {str(e)}', 'report_path': report_path}

        return {
            'report_date': summary_date,
            'recipient': REPORT_RECIPIENT,
            'report_path': report_path,
            'status': 'sent'
        }

    except Exception as e:
        logger.error(f"Failed to send summary email: {e}")
        raise AirflowException(f"Email sending failed: {e}")


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

# Task 5: Generate summary report file
generate_report_task = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    provide_context=True,
    dag=dag,
)

# Task 6: Send summary email
send_email_task = PythonOperator(
    task_id='send_summary_email',
    python_callable=send_summary_email,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
create_tables_task >> segment_users_task
create_tables_task >> generate_summary_task
segment_users_task >> data_quality_task
generate_summary_task >> data_quality_task
data_quality_task >> generate_report_task
generate_report_task >> send_email_task
