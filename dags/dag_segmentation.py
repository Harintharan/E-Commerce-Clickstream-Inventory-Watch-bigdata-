"""
Airflow DAG for E-Commerce Clickstream Batch Processing
Categorizes users and generates daily product summaries
"""

from datetime import date, datetime, timedelta
import csv
import logging
from typing import Dict, Any
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

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
REPORT_RETENTION_DAYS = 30
SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USER = os.getenv('SMTP_USER', 'your-email@gmail.com')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', 'your-app-password')
REPORT_RECIPIENT = os.getenv('REPORT_RECIPIENT', 'admin@example.com')
REPORT_SENDER = os.getenv('REPORT_SENDER', SMTP_USER)


def get_process_date(context) -> date:
    """
    Return the business date this DAG run should process.
    Optional manual override: trigger DAG with {"process_date": "YYYY-MM-DD"}.
    """
    dag_run = context.get('dag_run')
    if dag_run and dag_run.conf and dag_run.conf.get('process_date'):
        return datetime.strptime(dag_run.conf['process_date'], '%Y-%m-%d').date()

    logical_date = context.get('logical_date') or context['execution_date']
    return logical_date.date()


def resolve_data_date(db: "DatabaseConnector", context) -> date:
    """
    Use the requested process date when data exists; otherwise use the latest
    clickstream date. This keeps manual/demo runs from failing on stale Airflow
    logical dates.
    """
    requested_date = get_process_date(context)
    requested_count = db.execute_query("""
        SELECT COUNT(*) as count
        FROM clickstream_events
        WHERE CAST(event_timestamp AS DATE) = %s;
    """, (requested_date,), fetch=True)[0]['count']
    if requested_count > 0:
        return requested_date

    latest_row = db.execute_query("""
        SELECT CAST(MAX(event_timestamp) AS DATE) as latest_date
        FROM clickstream_events;
    """, fetch=True)[0]
    if latest_row['latest_date']:
        latest_date = latest_row['latest_date']
        logger.warning(
            f"No clickstream data for {requested_date}; using latest available date {latest_date}"
        )
        return latest_date

    raise AirflowException("No clickstream_events data found")


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


def check_data_exist(**context) -> Dict[str, Any]:
    """
    Ensure required tables and source data exist before running daily batch tasks.
    Tables are expected to be created by DB migration/init scripts.
    """
    try:
        db = DatabaseConnector(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
        db.connect()
        process_date = resolve_data_date(db, context)

        required_tables = [
            'clickstream_events',
            'product_metrics',
            'user_segments',
            'daily_product_summary',
        ]
        existing_tables = db.execute_query("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = ANY(%s);
        """, (required_tables,), fetch=True)
        existing_table_names = {row['table_name'] for row in existing_tables}
        missing_tables = sorted(set(required_tables) - existing_table_names)
        if missing_tables:
            raise AirflowException(
                "Required table(s) missing. Run DB migration/init scripts first: "
                + ", ".join(missing_tables)
            )

        raw_event_count = db.execute_query("""
            SELECT COUNT(*) as count
            FROM clickstream_events
            WHERE CAST(event_timestamp AS DATE) = %s;
        """, (process_date,), fetch=True)[0]['count']

        metrics_count = db.execute_query("""
            SELECT COUNT(*) as count
            FROM product_metrics
            WHERE CAST(window_start AS DATE) = %s;
        """, (process_date,), fetch=True)[0]['count']

        logger.info(
            f"Data check for {process_date}: "
            f"clickstream_events={raw_event_count}, product_metrics={metrics_count}"
        )

        if raw_event_count == 0:
            raise AirflowException(f"No clickstream_events found for {process_date}")
        if metrics_count == 0:
            logger.warning(
                f"No product_metrics found for {process_date}; "
                "daily summary will be generated from raw clickstream_events"
            )

        db.close()
        return {
            'process_date': process_date.isoformat(),
            'clickstream_events': raw_event_count,
            'product_metrics': metrics_count,
            'status': 'ready',
        }

    except Exception as e:
        logger.error(f"Data existence check failed: {e}")
        raise AirflowException(f"Data existence check failed: {e}")


def segment_users(**context) -> Dict[str, Any]:
    """
    Segment users into 'Window Shoppers' and 'Buyers'
    Window Shoppers: Users who only viewed products
    Buyers: Users who made at least one purchase
    """
    try:
        db = DatabaseConnector(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
        db.connect()
        process_date = resolve_data_date(db, context)

        logger.info(f"Segmenting users for date: {process_date}")

        # Clear previous segment data for the date
        db.execute_query("""
            DELETE FROM user_segments
            WHERE segment_date = %s;
        """, (process_date,))

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
                SELECT user_id, event_type, product_id, event_timestamp
                FROM clickstream_events
                WHERE CAST(event_timestamp AS DATE) = %s
                  AND event_type IN ('view', 'add_to_cart')
                  AND user_id NOT IN (
                    SELECT DISTINCT user_id
                    FROM clickstream_events
                    WHERE CAST(event_timestamp AS DATE) = %s
                      AND event_type = 'purchase'
                  )
            ) AS window_shoppers
            GROUP BY user_id
            ON CONFLICT (user_id, segment_date) DO UPDATE SET
                segment_type = EXCLUDED.segment_type,
                view_count = EXCLUDED.view_count,
                products_viewed = EXCLUDED.products_viewed;
        """, (process_date, process_date, process_date))

        # Segment users as 'Buyers' (made at least one purchase)
        db.execute_query("""
            INSERT INTO user_segments
            (user_id, segment_type, segment_date, view_count, purchase_count,
             products_viewed, products_purchased)
            SELECT
                pm.user_id,
                'Buyer' as segment_type,
                %s as segment_date,
                COUNT(CASE WHEN pm.event_type = 'view' THEN 1 END) as view_count,
                COUNT(CASE WHEN pm.event_type = 'purchase' THEN 1 END) as purchase_count,
                COUNT(DISTINCT CASE WHEN pm.event_type = 'view' THEN pm.product_id END) as products_viewed,
                COUNT(DISTINCT CASE WHEN pm.event_type = 'purchase' THEN pm.product_id END) as products_purchased
            FROM clickstream_events pm
            WHERE CAST(pm.event_timestamp AS DATE) = %s
              AND pm.user_id IN (
                SELECT DISTINCT user_id
                FROM clickstream_events
                WHERE CAST(event_timestamp AS DATE) = %s
                  AND event_type = 'purchase'
              )
            GROUP BY pm.user_id
            ON CONFLICT (user_id, segment_date) DO UPDATE SET
                segment_type = EXCLUDED.segment_type,
                view_count = EXCLUDED.view_count,
                purchase_count = EXCLUDED.purchase_count,
                products_viewed = EXCLUDED.products_viewed,
                products_purchased = EXCLUDED.products_purchased;
        """, (process_date, process_date, process_date))

        db.close()

        logger.info(f"User segmentation completed for {process_date}")

        return {
            'segmentation_date': process_date.isoformat(),
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
        db = DatabaseConnector(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
        db.connect()
        process_date = resolve_data_date(db, context)

        logger.info(f"Generating daily summary for {process_date}")

        # Clear previous summary data for the date
        db.execute_query("""
            DELETE FROM daily_product_summary
            WHERE summary_date = %s;
        """, (process_date,))

        # Use raw events as the daily source of truth. product_metrics contains
        # streaming windows, which may be incomplete during a run and may
        # double-count daily totals if sliding windows are summed.
        summary_source_query = """
            SELECT
                product_id,
                COUNT(CASE WHEN event_type = 'view' THEN 1 END) as total_views,
                COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as total_purchases,
                COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) as total_carts
            FROM clickstream_events
            WHERE CAST(event_timestamp AS DATE) = %s
            GROUP BY product_id
        """

        db.execute_query("""
            WITH product_totals AS (
                {summary_source_query}
            ),
            product_visitors AS (
                SELECT
                    product_id,
                    COUNT(DISTINCT user_id) as unique_visitors
                FROM clickstream_events
                WHERE CAST(event_timestamp AS DATE) = %s
                GROUP BY product_id
            )
            INSERT INTO daily_product_summary
            (product_id, summary_date, total_views, total_purchases, total_carts,
             unique_visitors, conversion_rate, flash_sale_recommended)
            SELECT
                pt.product_id,
                %s as summary_date,
                pt.total_views,
                pt.total_purchases,
                pt.total_carts,
                COALESCE(pv.unique_visitors, 0) as unique_visitors,
                CASE
                    WHEN pt.total_views > 0
                    THEN (pt.total_purchases * 100.0) / pt.total_views
                    ELSE 0
                END as conversion_rate,
                CASE
                    WHEN pt.total_views > 100 AND pt.total_purchases < 5
                    THEN TRUE
                    ELSE FALSE
                END as flash_sale_recommended
            FROM product_totals pt
            LEFT JOIN product_visitors pv ON pt.product_id = pv.product_id
            ORDER BY pt.total_views DESC;
        """.format(summary_source_query=summary_source_query), (process_date, process_date, process_date))

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
        """, (process_date,), fetch=True)

        logger.info("Daily summary generated successfully")
        logger.info(f"Top 5 products by views: {top_products}")

        db.close()

        return {
            'summary_date': process_date.isoformat(),
            'top_products_count': len(top_products) if top_products else 0,
            'status': 'completed'
        }

    except Exception as e:
        logger.error(f"Failed to generate daily summary: {e}")
        raise AirflowException(f"Daily summary generation failed: {e}")


def generate_summary_report(**context) -> Dict[str, Any]:
    """
    Generate formatted text report with top 5 most viewed products
    Exports report to file for archival and email delivery
    """
    try:
        db = DatabaseConnector(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
        db.connect()
        summary_date = resolve_data_date(db, context)

        logger.info(f"Generating summary report for {summary_date}")

        # Create reports directory if it doesn't exist
        os.makedirs(REPORT_DIR, exist_ok=True)

        # Query product summaries. TXT highlights the top five; CSV keeps the
        # full daily detail for downstream analysis.
        product_summaries = db.execute_query("""
            SELECT
                product_id,
                total_views,
                total_purchases,
                total_carts,
                unique_visitors,
                conversion_rate,
                flash_sale_recommended
            FROM daily_product_summary
            WHERE summary_date = %s
            ORDER BY total_views DESC, product_id ASC;
        """, (summary_date,), fetch=True)
        top_products = product_summaries[:5] if product_summaries else []

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

        generated_at = datetime.utcnow()
        generated_at_display = generated_at.strftime('%Y-%m-%d %H:%M:%S UTC')
        generated_at_token = generated_at.strftime('%Y%m%d_%H%M%S')
        report_basename = f"summary_{summary_date}_{generated_at_token}"
        txt_report_path = os.path.join(REPORT_DIR, f"{report_basename}.txt")
        csv_report_path = os.path.join(REPORT_DIR, f"{report_basename}.csv")

        with open(txt_report_path, 'w') as f:
            f.write("E-Commerce Clickstream Daily Summary\n")
            f.write("------------------------------------\n")
            f.write(f"Report Date : {summary_date}\n")
            f.write(f"Generated   : {generated_at_display}\n\n")

            # Top 5 Products Section
            f.write("Top 5 Most Viewed Products\n")
            f.write("--------------------------\n")
            f.write(f"{'#':<4} {'Product':<10} {'Views':>8} {'Purchases':>10} {'Conv.':>8} {'Flash Sale':>11}\n")
            f.write(f"{'-' * 4} {'-' * 10} {'-' * 8} {'-' * 10} {'-' * 8} {'-' * 11}\n")

            if top_products:
                for idx, product in enumerate(top_products, 1):
                    flash_sale = "YES" if product['flash_sale_recommended'] else "NO"
                    f.write(
                        f"{idx:<4} "
                        f"{product['product_id']:<10} "
                        f"{product['total_views']:>8} "
                        f"{product['total_purchases']:>10} "
                        f"{product['conversion_rate']:>7.2f}% "
                        f"{flash_sale:>11}\n"
                    )
            else:
                f.write("No product data available for this period\n")

            f.write("\n")
            f.write("User Segmentation Summary\n")
            f.write("-------------------------\n")
            f.write(f"{'Segment':<18} {'Users':>8} {'Avg Purchases':>15} {'Avg Views':>12}\n")
            f.write(f"{'-' * 18} {'-' * 8} {'-' * 15} {'-' * 12}\n")

            if user_stats:
                for stat in user_stats:
                    f.write(
                        f"{stat['segment_type']:<18} "
                        f"{stat['user_count']:>8} "
                        f"{stat['avg_purchases']:>15} "
                        f"{stat['avg_views']:>12}\n"
                    )
            else:
                f.write("No user segmentation data available for this period\n")

        with open(csv_report_path, 'w', newline='') as csv_file:
            fieldnames = [
                'report_date',
                'generated_at_utc',
                'rank',
                'product_id',
                'total_views',
                'total_carts',
                'total_purchases',
                'unique_visitors',
                'conversion_rate_percent',
                'flash_sale_recommended',
            ]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for idx, product in enumerate(product_summaries or [], 1):
                writer.writerow({
                    'report_date': summary_date,
                    'generated_at_utc': generated_at_display,
                    'rank': idx,
                    'product_id': product['product_id'],
                    'total_views': product['total_views'],
                    'total_carts': product['total_carts'],
                    'total_purchases': product['total_purchases'],
                    'unique_visitors': product['unique_visitors'],
                    'conversion_rate_percent': f"{product['conversion_rate']:.2f}",
                    'flash_sale_recommended': 'YES' if product['flash_sale_recommended'] else 'NO',
                })

        logger.info(f"Text summary report generated successfully: {txt_report_path}")
        logger.info(f"CSV summary report generated successfully: {csv_report_path}")

        return {
            'report_date': summary_date.isoformat(),
            'report_path': txt_report_path,
            'text_report_path': txt_report_path,
            'csv_report_path': csv_report_path,
            'generated_at_utc': generated_at_display,
            'top_products_count': len(top_products) if top_products else 0,
            'product_rows_count': len(product_summaries) if product_summaries else 0,
            'segment_rows_count': len(user_stats) if user_stats else 0,
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

        db = DatabaseConnector(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
        db.connect()
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
        overall_product_stats = db.execute_query("""
            SELECT
                COALESCE(SUM(total_views), 0) as total_views,
                COALESCE(SUM(total_purchases), 0) as total_purchases,
                COUNT(*) FILTER (WHERE flash_sale_recommended = TRUE) as flash_sale_count
            FROM daily_product_summary
            WHERE summary_date = %s;
        """, (summary_date,), fetch=True)[0]
        user_stats = db.execute_query("""
            SELECT
                segment_type,
                COUNT(*) as user_count,
                ROUND(AVG(purchase_count), 2) as avg_purchases,
                ROUND(AVG(view_count), 2) as avg_views
            FROM user_segments
            WHERE segment_date = %s
            GROUP BY segment_type
            ORDER BY segment_type;
        """, (summary_date,), fetch=True)
        db.close()

        total_views = overall_product_stats['total_views']
        total_purchases = overall_product_stats['total_purchases']
        total_users = sum(stat['user_count'] for stat in user_stats) if user_stats else 0
        flash_sale_count = overall_product_stats['flash_sale_count']

        product_rows = ""
        if top_products:
            for idx, product in enumerate(top_products, 1):
                badge_color = "#dcfce7" if product['flash_sale_recommended'] else "#f1f5f9"
                badge_text = "#166534" if product['flash_sale_recommended'] else "#475569"
                badge_label = "YES" if product['flash_sale_recommended'] else "NO"
                product_rows += f"""
                  <tr>
                    <td style="padding:12px 10px;border-bottom:1px solid #e2e8f0;color:#64748b;">#{idx}</td>
                    <td style="padding:12px 10px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#0f172a;">{product['product_id']}</td>
                    <td style="padding:12px 10px;border-bottom:1px solid #e2e8f0;color:#0f172a;">{product['total_views']}</td>
                    <td style="padding:12px 10px;border-bottom:1px solid #e2e8f0;color:#0f172a;">{product['total_purchases']}</td>
                    <td style="padding:12px 10px;border-bottom:1px solid #e2e8f0;color:#0f172a;">{product['conversion_rate']:.2f}%</td>
                    <td style="padding:12px 10px;border-bottom:1px solid #e2e8f0;">
                      <span style="display:inline-block;padding:4px 10px;border-radius:999px;background:{badge_color};color:{badge_text};font-size:12px;font-weight:700;">{badge_label}</span>
                    </td>
                  </tr>
                """
        else:
            product_rows = """
              <tr>
                <td colspan="6" style="padding:18px 10px;text-align:center;color:#64748b;border-bottom:1px solid #e2e8f0;">No product data available</td>
              </tr>
            """

        segment_rows = ""
        if user_stats:
            for stat in user_stats:
                segment_rows += f"""
                  <tr>
                    <td style="padding:12px 10px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#0f172a;">{stat['segment_type']}</td>
                    <td style="padding:12px 10px;border-bottom:1px solid #e2e8f0;color:#0f172a;">{stat['user_count']}</td>
                    <td style="padding:12px 10px;border-bottom:1px solid #e2e8f0;color:#0f172a;">{stat['avg_purchases']}</td>
                    <td style="padding:12px 10px;border-bottom:1px solid #e2e8f0;color:#0f172a;">{stat['avg_views']}</td>
                  </tr>
                """
        else:
            segment_rows = """
              <tr>
                <td colspan="4" style="padding:18px 10px;text-align:center;color:#64748b;border-bottom:1px solid #e2e8f0;">No user segmentation data available</td>
              </tr>
            """

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
        <html lang="en">
          <body style="margin:0;padding:0;background:#f8fafc;font-family:Arial,Helvetica,sans-serif;color:#0f172a;">
            <div style="max-width:760px;margin:0 auto;padding:28px 16px;">
              <div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;">
                <div style="padding:28px 28px 22px;background:#0f172a;color:#ffffff;">
                  <div style="font-size:13px;font-weight:700;letter-spacing:.4px;text-transform:uppercase;color:#93c5fd;">Daily Summary</div>
                  <h1 style="margin:8px 0 8px;font-size:26px;line-height:1.25;font-weight:800;">E-Commerce Clickstream Report</h1>
                  <div style="font-size:14px;color:#cbd5e1;">Report Date: {summary_date} &nbsp;|&nbsp; Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</div>
                </div>

                <div style="padding:22px 28px 8px;">
                  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                    <tr>
                      <td style="width:25%;padding:0 8px 14px 0;">
                        <div style="border:1px solid #e2e8f0;border-radius:10px;padding:14px;background:#f8fafc;">
                          <div style="font-size:12px;color:#64748b;font-weight:700;text-transform:uppercase;">Total Views</div>
                          <div style="font-size:24px;font-weight:800;margin-top:6px;color:#0f172a;">{total_views}</div>
                        </div>
                      </td>
                      <td style="width:25%;padding:0 8px 14px 0;">
                        <div style="border:1px solid #e2e8f0;border-radius:10px;padding:14px;background:#f8fafc;">
                          <div style="font-size:12px;color:#64748b;font-weight:700;text-transform:uppercase;">Total Purchases</div>
                          <div style="font-size:24px;font-weight:800;margin-top:6px;color:#0f172a;">{total_purchases}</div>
                        </div>
                      </td>
                      <td style="width:25%;padding:0 8px 14px 0;">
                        <div style="border:1px solid #e2e8f0;border-radius:10px;padding:14px;background:#f8fafc;">
                          <div style="font-size:12px;color:#64748b;font-weight:700;text-transform:uppercase;">Users</div>
                          <div style="font-size:24px;font-weight:800;margin-top:6px;color:#0f172a;">{total_users}</div>
                        </div>
                      </td>
                      <td style="width:25%;padding:0 0 14px 0;">
                        <div style="border:1px solid #e2e8f0;border-radius:10px;padding:14px;background:#f8fafc;">
                          <div style="font-size:12px;color:#64748b;font-weight:700;text-transform:uppercase;">Flash Sale</div>
                          <div style="font-size:24px;font-weight:800;margin-top:6px;color:#0f172a;">{flash_sale_count}</div>
                        </div>
                      </td>
                    </tr>
                  </table>
                </div>

                <div style="padding:8px 28px 22px;">
                  <h2 style="margin:10px 0 12px;font-size:18px;line-height:1.3;color:#0f172a;">Top 5 Most Viewed Products</h2>
                  <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;">
                    <thead>
                      <tr style="background:#f1f5f9;">
                        <th align="left" style="padding:11px 10px;font-size:12px;color:#475569;text-transform:uppercase;">Rank</th>
                        <th align="left" style="padding:11px 10px;font-size:12px;color:#475569;text-transform:uppercase;">Product</th>
                        <th align="left" style="padding:11px 10px;font-size:12px;color:#475569;text-transform:uppercase;">Views</th>
                        <th align="left" style="padding:11px 10px;font-size:12px;color:#475569;text-transform:uppercase;">Purchases</th>
                        <th align="left" style="padding:11px 10px;font-size:12px;color:#475569;text-transform:uppercase;">Conv.</th>
                        <th align="left" style="padding:11px 10px;font-size:12px;color:#475569;text-transform:uppercase;">Flash Sale</th>
                      </tr>
                    </thead>
                    <tbody>{product_rows}</tbody>
                  </table>
                </div>

                <div style="padding:0 28px 28px;">
                  <h2 style="margin:10px 0 12px;font-size:18px;line-height:1.3;color:#0f172a;">User Segmentation Summary</h2>
                  <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;">
                    <thead>
                      <tr style="background:#f1f5f9;">
                        <th align="left" style="padding:11px 10px;font-size:12px;color:#475569;text-transform:uppercase;">Segment</th>
                        <th align="left" style="padding:11px 10px;font-size:12px;color:#475569;text-transform:uppercase;">Users</th>
                        <th align="left" style="padding:11px 10px;font-size:12px;color:#475569;text-transform:uppercase;">Avg Purchases</th>
                        <th align="left" style="padding:11px 10px;font-size:12px;color:#475569;text-transform:uppercase;">Avg Views</th>
                      </tr>
                    </thead>
                    <tbody>{segment_rows}</tbody>
                  </table>
                </div>

                <div style="padding:16px 28px;background:#f8fafc;border-top:1px solid #e2e8f0;color:#64748b;font-size:13px;">
                  This is an automated report. Please do not reply to this email.
                </div>
              </div>
            </div>
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


def cleanup_old_reports(**context) -> Dict[str, Any]:
    """
    Delete generated report files older than the configured retention period.
    Only report .txt and .csv files in REPORT_DIR are removed.
    """
    try:
        os.makedirs(REPORT_DIR, exist_ok=True)
        cutoff_time = datetime.utcnow() - timedelta(days=REPORT_RETENTION_DAYS)
        deleted_files = []
        skipped_files = []

        for entry in os.scandir(REPORT_DIR):
            if not entry.is_file():
                continue
            if not entry.name.lower().endswith(('.txt', '.csv')):
                skipped_files.append(entry.name)
                continue

            modified_at = datetime.utcfromtimestamp(entry.stat().st_mtime)
            if modified_at < cutoff_time:
                os.remove(entry.path)
                deleted_files.append(entry.name)

        logger.info(
            f"Report cleanup completed: deleted={len(deleted_files)}, "
            f"retention_days={REPORT_RETENTION_DAYS}, skipped={len(skipped_files)}"
        )

        return {
            'status': 'completed',
            'retention_days': REPORT_RETENTION_DAYS,
            'deleted_count': len(deleted_files),
            'deleted_files': deleted_files,
            'skipped_count': len(skipped_files),
        }

    except Exception as e:
        logger.error(f"Failed to cleanup old reports: {e}")
        raise AirflowException(f"Report cleanup failed: {e}")


# Start marker
start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Task 1: Check source data exists
check_data_exist_task = PythonOperator(
    task_id='check_data_exist',
    python_callable=check_data_exist,
    provide_context=True,
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

# Task 4: Generate summary report file
generate_report_task = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    provide_context=True,
    dag=dag,
)

# Task 5: Send summary email
send_email_task = PythonOperator(
    task_id='send_summary_email',
    python_callable=send_summary_email,
    provide_context=True,
    dag=dag,
)

# Task 6: Delete report files older than the retention period
cleanup_reports_task = PythonOperator(
    task_id='cleanup_old_reports',
    python_callable=cleanup_old_reports,
    provide_context=True,
    dag=dag,
)

# End marker
end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Define task dependencies
start_task >> check_data_exist_task
check_data_exist_task >> [segment_users_task, generate_summary_task]
[segment_users_task, generate_summary_task] >> generate_report_task
generate_report_task >> send_email_task >> cleanup_reports_task >> end_task
