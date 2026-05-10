"""
Database Query Helper for Clickstream Project
Provides utilities for querying and monitoring the PostgreSQL database
Loads all configuration from config module
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import os

from config import DATABASE_CONFIG

class ClickstreamDB:
    """Helper class for database operations"""

    def __init__(self):
        self.host = DATABASE_CONFIG['host']
        self.port = DATABASE_CONFIG['port']
        self.user = DATABASE_CONFIG['user']
        self.password = DATABASE_CONFIG['password']
        self.database = DATABASE_CONFIG['database']
        self.connection = None

    def connect(self):
        """Connect to database"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            print(f"✓ Connected to {self.host}:{self.port}/{self.database}")
        except Exception as e:
            print(f"✗ Connection failed: {e}")

    def query(self, sql, fetch_all=True):
        """Execute query"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql)
                if fetch_all:
                    return cursor.fetchall()
                return cursor.fetchone()
        except Exception as e:
            print(f"✗ Query failed: {e}")
            return None

    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()

    def get_flash_sale_triggers(self, limit=10):
        """Get products with Flash Sale triggers"""
        sql = f"""
        SELECT 
            product_id,
            SUM(view_count) as total_views,
            SUM(purchase_count) as total_purchases,
            ROUND(AVG(conversion_rate), 2) as avg_conversion_rate,
            COUNT(*) as windows_detected
        FROM product_metrics
        WHERE flash_sale_suggested = true
        GROUP BY product_id
        ORDER BY total_views DESC
        LIMIT {limit};
        """
        return self.query(sql)

    def get_user_segments(self, date=None):
        """Get user segmentation for a specific date"""
        if date is None:
            date = (datetime.now() - timedelta(days=1)).date()
        
        sql = f"""
        SELECT 
            segment_type,
            COUNT(*) as count,
            AVG(view_count) as avg_views,
            AVG(purchase_count) as avg_purchases,
            SUM(products_purchased) as total_products_purchased
        FROM user_segments
        WHERE segment_date = '{date}'
        GROUP BY segment_type;
        """
        return self.query(sql)

    def get_top_products(self, date=None, limit=5):
        """Get top products by views for a specific date"""
        if date is None:
            date = (datetime.now() - timedelta(days=1)).date()
        
        sql = f"""
        SELECT 
            product_id,
            total_views,
            total_purchases,
            ROUND(conversion_rate, 2) as conversion_rate,
            flash_sale_recommended
        FROM daily_product_summary
        WHERE summary_date = '{date}'
        ORDER BY total_views DESC
        LIMIT {limit};
        """
        return self.query(sql)

    def get_metrics_summary(self):
        """Get overall system metrics"""
        sql = """
        SELECT 
            COUNT(DISTINCT product_id) as distinct_products,
            SUM(view_count) as total_views,
            SUM(purchase_count) as total_purchases,
            SUM(cart_count) as total_carts,
            ROUND(SUM(purchase_count)::float / NULLIF(SUM(view_count), 0) * 100, 2) as overall_conversion_rate,
            COUNT(*) as total_window_records
        FROM product_metrics;
        """
        return self.query(sql, fetch_all=False)

    def get_recent_metrics(self, minutes=10):
        """Get recent metrics from last N minutes"""
        sql = f"""
        SELECT 
            product_id,
            window_start,
            window_end,
            view_count,
            purchase_count,
            conversion_rate,
            flash_sale_suggested
        FROM product_metrics
        WHERE window_end >= NOW() - INTERVAL '{minutes} minutes'
        ORDER BY window_end DESC
        LIMIT 20;
        """
        return self.query(sql)

    def print_report(self):
        """Print comprehensive report"""
        print("\n" + "="*80)
        print("CLICKSTREAM ANALYTICS REPORT")
        print("="*80)

        # Overall Metrics
        print("\n[OVERALL METRICS]")
        metrics = self.get_metrics_summary()
        if metrics:
            for key, value in metrics.items():
                print(f"  {key}: {value}")

        # Flash Sale Triggers
        print("\n[FLASH SALE TRIGGERS (Top 5)]")
        triggers = self.get_flash_sale_triggers(limit=5)
        if triggers:
            print(f"{'Product ID':<12} {'Views':<10} {'Purchases':<12} {'Conv Rate':<12} {'Windows':<8}")
            print("-" * 54)
            for row in triggers:
                print(f"{row['product_id']:<12} {row['total_views']:<10} "
                      f"{row['total_purchases']:<12} {row['avg_conversion_rate']:<12} "
                      f"{row['windows_detected']:<8}")

        # User Segments
        print("\n[USER SEGMENTS (Latest)]")
        segments = self.get_user_segments()
        if segments:
            print(f"{'Segment Type':<15} {'Count':<8} {'Avg Views':<12} {'Avg Purchases':<15}")
            print("-" * 50)
            for row in segments:
                print(f"{row['segment_type']:<15} {row['count']:<8} "
                      f"{row['avg_views']:<12.1f} {row['avg_purchases']:<15.1f}")

        # Top Products
        print("\n[TOP 5 PRODUCTS]")
        top = self.get_top_products(limit=5)
        if top:
            print(f"{'Product ID':<12} {'Views':<10} {'Purchases':<12} {'Conv Rate':<12} {'Flash Sale':<12}")
            print("-" * 58)
            for row in top:
                flash_sale = "✓ YES" if row['flash_sale_recommended'] else "NO"
                print(f"{row['product_id']:<12} {row['total_views']:<10} "
                      f"{row['total_purchases']:<12} {row['conversion_rate']:<12.2f}% "
                      f"{flash_sale:<12}")

        # Recent Activity
        print("\n[RECENT ACTIVITY (Last 10 Minutes)]")
        recent = self.get_recent_metrics(minutes=10)
        if recent:
            print(f"{'Product':<10} {'Window Start':<20} {'Views':<8} {'Purchases':<10} {'Flash Sale':<12}")
            print("-" * 60)
            for row in recent[:10]:
                flash_sale = "✓" if row['flash_sale_suggested'] else ""
                window_start = row['window_start'].strftime("%Y-%m-%d %H:%M")
                print(f"{row['product_id']:<10} {window_start:<20} {row['view_count']:<8} "
                      f"{row['purchase_count']:<10} {flash_sale:<12}")

        print("\n" + "="*80 + "\n")


def main():
    """Main function"""
    db = ClickstreamDB()
    db.connect()

    if db.connection:
        db.print_report()
        db.close()
    else:
        print("Failed to connect to database")


if __name__ == '__main__':
    main()
