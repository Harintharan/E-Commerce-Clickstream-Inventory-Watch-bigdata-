#!/usr/bin/env python3
"""
PySpark Structured Streaming for E-Commerce Clickstream Processing
Performs sliding window aggregation and Flash Sale detection
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
from kafka import KafkaConsumer
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, window, count, when, sum as spark_sum,
    col as spark_col, current_timestamp, lit
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


# Configure logging
log_level = os.getenv('LOG_LEVEL', 'INFO')
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'clickstream_topic')
DB_HOST = os.getenv('DB_HOST', 'postgres-db')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER', 'airflow')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
DB_NAME = os.getenv('DB_NAME', 'clickstream_db')
CHECKPOINT_DIR = '/app/checkpoint'

# Flash Sale Trigger Configuration
FLASH_SALE_VIEW_THRESHOLD = 15
FLASH_SALE_PURCHASE_THRESHOLD = 2
WINDOW_DURATION = "1 minute"
SLIDING_INTERVAL = "30 seconds"
WATERMARK_DELAY = "30 seconds"


# Define schema for incoming Kafka events
EVENT_SCHEMA = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("device", StringType(), True),
])


class StreamProcessor:
    """Spark Structured Streaming processor for clickstream analytics"""

    def __init__(self):
        """Initialize Spark session"""
        self.spark = None
        self.jdbc_url = None
        self.connection_properties = None
        self._initialize_spark()

    def _initialize_spark(self) -> None:
        """Initialize Spark session with necessary packages"""
        try:
            self.spark = SparkSession.builder \
                .appName("ClickstreamProcessor") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
                .config("spark.sql.streaming.schemaInference", "true") \
                .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()

            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session initialized successfully")

            # Setup PostgreSQL connection properties
            self.jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
            self.connection_properties = {
                "user": DB_USER,
                "password": DB_PASSWORD,
                "driver": "org.postgresql.Driver"
            }
            logger.info(f"PostgreSQL connection configured for {DB_HOST}:{DB_PORT}/{DB_NAME}")

        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            raise

    def read_kafka_stream(self) -> DataFrame:
        """
        Read clickstream events from Kafka topic
        """
        try:
            logger.info(f"Connecting to Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", KAFKA_TOPIC) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()

            # Parse JSON value
            df = df.select(
                from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"),
                col("timestamp").alias("kafka_timestamp")
            ).select("data.*", "kafka_timestamp")

            # Convert string timestamp to timestamp type
            df = df.withColumn(
                "event_time",
                col("timestamp").cast("timestamp")
            )

            logger.info("Kafka stream reader configured successfully")
            return df

        except Exception as e:
            logger.error(f"Failed to read Kafka stream: {e}")
            raise

    def apply_watermark_and_window(self, df: DataFrame) -> DataFrame:
        """
        Apply watermark and sliding window aggregation
        """
        try:
            df_with_watermark = df.withWatermark("event_time", WATERMARK_DELAY)

            # Aggregate by product and time window
            windowed_df = df_with_watermark.groupBy(
                window(col("event_time"), WINDOW_DURATION, SLIDING_INTERVAL),
                col("product_id")
            ).agg(
                count(when(col("event_type") == "view", 1)).alias("view_count"),
                count(when(col("event_type") == "add_to_cart", 1)).alias("cart_count"),
                count(when(col("event_type") == "purchase", 1)).alias("purchase_count"),
                count(col("*")).alias("total_events")
            )

            logger.info("Watermark and windowing applied successfully")
            return windowed_df

        except Exception as e:
            logger.error(f"Failed to apply watermark and window: {e}")
            raise

    def detect_flash_sale_trigger(self, windowed_df: DataFrame) -> DataFrame:
        """
        Detect products meeting Flash Sale criteria
        Criteria: Views > 100 AND Purchases < 5
        """
        try:
            flash_sale_df = windowed_df.withColumn(
                "flash_sale_suggested",
                when(
                    (col("view_count") > FLASH_SALE_VIEW_THRESHOLD) & 
                    (col("purchase_count") < FLASH_SALE_PURCHASE_THRESHOLD), 
                    True
                ).otherwise(False)
            ).withColumn(
                "conversion_rate",
                when(col("view_count") > 0, (col("purchase_count") / col("view_count")) * 100.0).otherwise(0.0)
            )

            logger.info("Flash Sale trigger logic applied")
            return flash_sale_df

        except Exception as e:
            logger.error(f"Failed to detect Flash Sale triggers: {e}")
            raise

    def add_metadata(self, df: DataFrame) -> DataFrame:
        """Add processing metadata"""
        return df.withColumn(
            "processed_timestamp",
            current_timestamp()
        ).withColumn(
            "processing_batch_id",
            lit(int(datetime.utcnow().timestamp()))
        )

    def write_to_console(self, df: DataFrame, mode: str = "complete") -> None:
        """
        Write output to console for debugging and monitoring
        """
        try:
            query = df \
                .select("window", "product_id", "view_count", "cart_count",
                       "purchase_count", "conversion_rate", "flash_sale_suggested") \
                .writeStream \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", 50) \
                .outputMode(mode) \
                .trigger(processingTime="30 seconds") \
                .option("checkpointLocation", f"{CHECKPOINT_DIR}/console") \
                .start()

            logger.info("Console output stream started")
            return query

        except Exception as e:
            logger.error(f"Failed to write to console: {e}")
            raise

    def write_to_postgresql(self, df: DataFrame, table_name: str = "product_metrics", mode: str = "append") -> None:
        """
        Write aggregated metrics to PostgreSQL
        """
        try:
            output_df = df.select(
                col("product_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("view_count"),
                col("cart_count"),
                col("purchase_count"),
                col("total_events"),
                col("conversion_rate"),
                col("flash_sale_suggested"),
                col("processed_timestamp")
            )

            def write_to_jdbc(batch_df: DataFrame, batch_id: int):
                batch_df.write \
                    .format("jdbc") \
                    .option("url", self.jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", DB_USER) \
                    .option("password", DB_PASSWORD) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode(mode) \
                    .save()

            query = output_df \
                .writeStream \
                .foreachBatch(write_to_jdbc) \
                .trigger(processingTime="30 seconds") \
                .option("checkpointLocation", f"{CHECKPOINT_DIR}/postgres") \
                .start()

            logger.info(f"PostgreSQL output stream started for table: {table_name}")
            return query

        except Exception as e:
            logger.error(f"Failed to write to PostgreSQL: {e}")
            raise

    def write_raw_events_to_postgresql(self, df: DataFrame, table_name: str = "clickstream_events") -> None:
        """
        Write raw clickstream events to PostgreSQL for user-level batch analytics.
        """
        try:
            output_df = df.select(
                col("user_id"),
                col("product_id"),
                col("event_type"),
                col("event_time").alias("event_timestamp"),
                col("session_id"),
                col("device"),
                col("kafka_timestamp"),
                current_timestamp().alias("processed_timestamp")
            )

            def write_to_jdbc(batch_df: DataFrame, batch_id: int):
                batch_df.write \
                    .format("jdbc") \
                    .option("url", self.jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", DB_USER) \
                    .option("password", DB_PASSWORD) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

            query = output_df \
                .writeStream \
                .foreachBatch(write_to_jdbc) \
                .trigger(processingTime="30 seconds") \
                .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw_events") \
                .start()

            logger.info(f"Raw event PostgreSQL output stream started for table: {table_name}")
            return query

        except Exception as e:
            logger.error(f"Failed to write raw events to PostgreSQL: {e}")
            raise

    def run(self) -> None:
        """
        Main execution pipeline
        """
        try:
            logger.info("=" * 80)
            logger.info("Starting PySpark Clickstream Stream Processor")
            logger.info(f"Configuration: Window={WINDOW_DURATION}, Slide={SLIDING_INTERVAL}, Watermark={WATERMARK_DELAY}")
            logger.info(f"Flash Sale Trigger: Views > {FLASH_SALE_VIEW_THRESHOLD} AND Purchases < {FLASH_SALE_PURCHASE_THRESHOLD}")
            logger.info("=" * 80)

            # Read from Kafka
            df = self.read_kafka_stream()

            # Apply windowing and aggregation
            windowed_df = self.apply_watermark_and_window(df)

            # Detect Flash Sale triggers
            flash_sale_df = self.detect_flash_sale_trigger(windowed_df)

            # Add metadata
            output_df = self.add_metadata(flash_sale_df)

            # Write to console and PostgreSQL
            console_query = self.write_to_console(output_df)
            raw_events_query = self.write_raw_events_to_postgresql(df)
            db_query = self.write_to_postgresql(output_df)

            # Keep streaming
            self.spark.streams.awaitAnyTermination()

        except Exception as e:
            logger.error(f"Error in stream processing pipeline: {e}")
            raise
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Gracefully shutdown the stream processor"""
        try:
            if self.spark:
                logger.info("Stopping all streaming queries...")
                for query in self.spark.streams.active:
                    query.stop()
                logger.info("All streaming queries stopped")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")


class PythonFallbackProcessor:
    """Kafka/PostgreSQL processor used when Spark connector JARs are unavailable."""

    def __init__(self):
        self.consumer = None
        self.connection = None

    def connect(self) -> None:
        self.connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            connect_timeout=10,
        )
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="python_fallback_stream_processor",
        )
        logger.info("Python fallback processor connected to Kafka and PostgreSQL")

    def process_event(self, event: dict) -> None:
        event_time = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00")).replace(tzinfo=None)
        window_start = event_time.replace(second=0, microsecond=0)
        window_end = window_start + timedelta(minutes=1)

        view_count = 1 if event["event_type"] == "view" else 0
        cart_count = 1 if event["event_type"] == "add_to_cart" else 0
        purchase_count = 1 if event["event_type"] == "purchase" else 0

        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO clickstream_events
                (user_id, product_id, event_type, event_timestamp, session_id, device, kafka_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);
                """,
                (
                    event["user_id"],
                    event["product_id"],
                    event["event_type"],
                    event_time,
                    event.get("session_id"),
                    event.get("device"),
                ),
            )
            cursor.execute(
                """
                INSERT INTO product_metrics
                (product_id, window_start, window_end, view_count, cart_count, purchase_count,
                 total_events, conversion_rate, flash_sale_suggested, processed_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, 1, %s, FALSE, CURRENT_TIMESTAMP)
                ON CONFLICT (product_id, window_start, window_end) DO UPDATE SET
                    view_count = product_metrics.view_count + EXCLUDED.view_count,
                    cart_count = product_metrics.cart_count + EXCLUDED.cart_count,
                    purchase_count = product_metrics.purchase_count + EXCLUDED.purchase_count,
                    total_events = product_metrics.total_events + 1,
                    conversion_rate = CASE
                        WHEN product_metrics.view_count + EXCLUDED.view_count > 0
                        THEN ((product_metrics.purchase_count + EXCLUDED.purchase_count) * 100.0)
                             / (product_metrics.view_count + EXCLUDED.view_count)
                        ELSE 0
                    END,
                    flash_sale_suggested =
                        (product_metrics.view_count + EXCLUDED.view_count > %s)
                        AND (product_metrics.purchase_count + EXCLUDED.purchase_count < %s),
                    processed_timestamp = CURRENT_TIMESTAMP;
                """,
                (
                    event["product_id"],
                    window_start,
                    window_end,
                    view_count,
                    cart_count,
                    purchase_count,
                    (purchase_count * 100.0 / view_count) if view_count else 0.0,
                    FLASH_SALE_VIEW_THRESHOLD,
                    FLASH_SALE_PURCHASE_THRESHOLD,
                ),
            )
        self.connection.commit()

    def run(self) -> None:
        self.connect()
        logger.info("Starting Python fallback stream processor")
        while True:
            try:
                for message in self.consumer:
                    self.process_event(message.value)
            except Exception as e:
                logger.error(f"Python fallback processor error: {e}")
                if self.connection:
                    self.connection.rollback()
                time.sleep(5)


def main():
    """Entry point"""
    try:
        processor = StreamProcessor()
        processor.run()
    except Exception as e:
        logger.warning(f"Spark processor unavailable, falling back to Python processor: {e}")
        fallback = PythonFallbackProcessor()
        fallback.run()


if __name__ == '__main__':
    main()
