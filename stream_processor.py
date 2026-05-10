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
    format='%(asctime)s [%(levelname)-8s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Suppress verbose Spark/JVM logs
logging.getLogger('py4j').setLevel(logging.WARNING)
logging.getLogger('py4j.java_gateway').setLevel(logging.WARNING)
logging.getLogger('py4j.clientserver').setLevel(logging.WARNING)

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
# Requirement: High Interest (>100 views) + Low Conversion (<5 purchases)
FLASH_SALE_VIEW_THRESHOLD = 100  # PRODUCTION: >100 views
FLASH_SALE_PURCHASE_THRESHOLD = 5  # PRODUCTION: <5 purchases
# Window Configuration
WINDOW_DURATION_MINUTES = 10  # Must match WINDOW_DURATION
SLIDING_INTERVAL_MINUTES = 1   # Must match SLIDING_INTERVAL
WINDOW_DURATION = "10 minutes"  # Production requirement
SLIDING_INTERVAL = "1 minute"   # Check every minute
WATERMARK_DELAY = "30 seconds"
# Fallback processor batch configuration
FALLBACK_FLUSH_BATCH_SIZE = 100  # Flush aggregates to DB every N events
FALLBACK_EVIDENCE_INTERVAL = int(os.getenv('FALLBACK_EVIDENCE_INTERVAL', '25'))


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
                .getOrCreate()

            self.spark.sparkContext.setLogLevel("ERROR")
            # Suppress verbose Spark warnings
            logging.getLogger('org.apache.spark.sql.streaming').setLevel(logging.ERROR)
            logging.getLogger('org.apache.kafka.common.config').setLevel(logging.ERROR)
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

    def log_raw_event_evidence(self, batch_df: DataFrame, batch_id: int, table_name: str) -> int:
        """Log a short proof point that Kafka events are being consumed."""
        record_count = batch_df.count()
        if record_count == 0:
            logger.info(f"STREAM: batch={batch_id} no new Kafka events")
            return record_count

        type_counts = [
            f"{row.event_type}={row['count']}"
            for row in batch_df.groupBy("event_type").count().orderBy("event_type").collect()
        ]
        logger.info(
            f"STREAM: received {record_count} Kafka events "
            f"(batch={batch_id}, {', '.join(type_counts)})"
        )
        return record_count

    def log_metric_evidence(self, batch_df: DataFrame, batch_id: int, table_name: str) -> int:
        """Log a short proof point that streaming aggregation is running."""
        record_count = batch_df.count()
        if record_count == 0:
            logger.info(f"STREAM: processed batch={batch_id} no completed windows yet")
            return record_count

        totals = batch_df.agg(
            spark_sum("view_count").alias("views"),
            spark_sum("cart_count").alias("carts"),
            spark_sum("purchase_count").alias("purchases"),
            spark_sum("total_events").alias("events"),
        ).first()
        alert_count = batch_df.filter(col("flash_sale_suggested") == True).count()
        logger.info(
            f"STREAM: processed batch={batch_id} windows={record_count} "
            f"events={totals.events} views={totals.views} carts={totals.carts} "
            f"purchases={totals.purchases} alerts={alert_count} "
            f"-> PostgreSQL table '{table_name}'"
        )
        return record_count

    def write_flash_sale_alerts(self, df: DataFrame) -> None:
        """
        Write ONLY Flash Sale alerts to console in formatted TABLE with recommendation
        """
        try:
            # Filter only products that trigger flash sale alert
            alerts_df = df.filter(col("flash_sale_suggested") == True).select(
                col("window.start").alias("window_start"),
                col("product_id"),
                col("view_count"),
                col("purchase_count"),
                col("conversion_rate")
            )

            def log_flash_sale_table(batch_df: DataFrame, batch_id: int):
                """Log flash sale alerts as formatted table"""
                if batch_df.count() > 0:
                    rows = batch_df.collect()
                    
                    logger.info("")
                    logger.info("="*110)
                    logger.info("🔥 FLASH SALE ALERTS - HIGH INTEREST + LOW CONVERSION")
                    logger.info("="*110)
                    logger.info(
                        f"{'Window Start':<20} | {'Product':<8} | {'Views':<8} | {'Purchases':<12} | "
                        f"{'Conversion%':<13} | {'Recommendation':<25}"
                    )
                    logger.info("-"*110)
                    
                    for row in rows:
                        window_str = row.window_start.strftime('%Y-%m-%d %H:%M:%S')
                        recommendation = "✅ YES - APPLY FLASH SALE"
                        logger.info(
                            f"{window_str:<20} | {row.product_id:<8} | {row.view_count:<8} | "
                            f"{row.purchase_count:<12} | {row.conversion_rate:<13.2f}% | {recommendation:<25}"
                        )
                    
                    logger.info("="*110)
                    logger.info("")

            query = alerts_df \
                .writeStream \
                .foreachBatch(log_flash_sale_table) \
                .trigger(processingTime="1 minute") \
                .option("checkpointLocation", f"{CHECKPOINT_DIR}/flash_sale_alerts") \
                .start()

            logger.info("Flash Sale alert stream started - TABLE FORMAT")
            return query

        except Exception as e:
            logger.error(f"Failed to write Flash Sale alerts: {e}")
            raise

    def write_to_console(self, df: DataFrame, mode: str = "complete") -> None:
        """
        Write aggregated metrics to console (all products) for debugging
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
                .trigger(processingTime="1 minute") \
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
                row_count = self.log_metric_evidence(batch_df, batch_id, table_name)
                batch_df.write \
                    .format("jdbc") \
                    .option("url", self.jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", DB_USER) \
                    .option("password", DB_PASSWORD) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode(mode) \
                    .save()
                if row_count > 0:
                    logger.info(f"STREAM: saved batch={batch_id} rows={row_count} to '{table_name}'")

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
                row_count = self.log_raw_event_evidence(batch_df, batch_id, table_name)
                batch_df.write \
                    .format("jdbc") \
                    .option("url", self.jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", DB_USER) \
                    .option("password", DB_PASSWORD) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()
                if row_count > 0:
                    logger.info(f"STREAM: saved batch={batch_id} rows={row_count} to '{table_name}'")

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
            logger.info("="*80)
            logger.info("Starting PySpark Clickstream Stream Processor")
            logger.info(f"Configuration: Window={WINDOW_DURATION}, Slide={SLIDING_INTERVAL}, Watermark={WATERMARK_DELAY}")
            logger.info(f"Flash Sale Trigger: Views > {FLASH_SALE_VIEW_THRESHOLD} AND Purchases < {FLASH_SALE_PURCHASE_THRESHOLD}")
            logger.info("="*80)

            # Read from Kafka
            df = self.read_kafka_stream()

            # Apply windowing and aggregation
            windowed_df = self.apply_watermark_and_window(df)

            # Detect Flash Sale triggers
            flash_sale_df = self.detect_flash_sale_trigger(windowed_df)

            # Add metadata
            output_df = self.add_metadata(flash_sale_df)

            # Write to console, alerts, and PostgreSQL
            flash_sale_query = self.write_flash_sale_alerts(output_df)
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
        # Running aggregates: {(product_id, window_start): {view: count, cart: count, purchase: count}}
        self.running_aggregates = {}
        self.event_count = 0
        self.processed_offsets = set()  # For idempotency tracking
        self.last_event_summary = None

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

    def log_event_evidence(self, event: dict, offset: int, window_start: datetime, window_end: datetime) -> None:
        """Log compact evidence that the fallback consumer is actively processing Kafka events."""
        if self.event_count <= 3 or self.event_count % FALLBACK_EVIDENCE_INTERVAL == 0:
            logger.info(
                f"STREAM: consumed event={self.event_count} offset={offset} "
                f"product={event['product_id']} type={event['event_type']} "
                f"-> PostgreSQL table 'clickstream_events'"
            )

    def process_event(self, event: dict, offset: int) -> None:
        """Process a single event and accumulate metrics."""
        # Idempotency check
        if offset in self.processed_offsets:
            logger.debug(f"Skipping already-processed offset {offset}")
            return
        
        # Parse event timestamp
        event_time = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00")).replace(tzinfo=None)
        
        # Calculate 10-minute window bucket (FIX #1)
        minute_bucket = (event_time.minute // WINDOW_DURATION_MINUTES) * WINDOW_DURATION_MINUTES
        window_start = event_time.replace(minute=minute_bucket, second=0, microsecond=0)
        window_end = window_start + timedelta(minutes=WINDOW_DURATION_MINUTES)
        
        # Insert raw event (immediate write)
        try:
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
            self.connection.commit()
        except Exception as e:
            logger.error(f"Failed to insert raw event: {e}")
            self.connection.rollback()
            return
        
        # Accumulate metrics in memory (FIX #2)
        agg_key = (event["product_id"], window_start)
        if agg_key not in self.running_aggregates:
            self.running_aggregates[agg_key] = {
                "view": 0,
                "cart": 0,
                "purchase": 0,
                "window_end": window_end,
            }
        
        # Increment appropriate counter
        if event["event_type"] == "view":
            self.running_aggregates[agg_key]["view"] += 1
        elif event["event_type"] == "add_to_cart":
            self.running_aggregates[agg_key]["cart"] += 1
        elif event["event_type"] == "purchase":
            self.running_aggregates[agg_key]["purchase"] += 1

        self.event_count += 1
        self.processed_offsets.add(offset)
        self.last_event_summary = {
            "offset": offset,
            "product_id": event["product_id"],
            "event_type": event["event_type"],
            "window_start": window_start,
            "window_end": window_end,
        }
        self.log_event_evidence(event, offset, window_start, window_end)

        # Flush to DB periodically
        if self.event_count % FALLBACK_FLUSH_BATCH_SIZE == 0:
            self.flush_aggregates()
    
    def flush_aggregates(self) -> None:
        """Flush accumulated metrics to PostgreSQL."""
        if not self.running_aggregates:
            return
        
        try:
            with self.connection.cursor() as cursor:
                for (product_id, window_start), agg in self.running_aggregates.items():
                    view_count = agg["view"]
                    cart_count = agg["cart"]
                    purchase_count = agg["purchase"]
                    total_events = view_count + cart_count + purchase_count
                    window_end = agg["window_end"]
                    
                    # Calculate conversion rate per window (FIX #2)
                    conversion_rate = 0.0
                    if view_count > 0:
                        conversion_rate = (purchase_count * 100.0) / view_count
                    
                    # Check flash sale trigger
                    flash_sale_suggested = (view_count > FLASH_SALE_VIEW_THRESHOLD and
                                           purchase_count < FLASH_SALE_PURCHASE_THRESHOLD)

                    # UPSERT metrics
                    cursor.execute(
                        """
                        INSERT INTO product_metrics
                        (product_id, window_start, window_end, view_count, cart_count, purchase_count,
                         total_events, conversion_rate, flash_sale_suggested, processed_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (product_id, window_start, window_end) DO UPDATE SET
                            view_count = product_metrics.view_count + EXCLUDED.view_count,
                            cart_count = product_metrics.cart_count + EXCLUDED.cart_count,
                            purchase_count = product_metrics.purchase_count + EXCLUDED.purchase_count,
                            total_events = product_metrics.total_events + EXCLUDED.total_events,
                            conversion_rate = CASE
                                WHEN (product_metrics.view_count + EXCLUDED.view_count) > 0
                                THEN ((product_metrics.purchase_count + EXCLUDED.purchase_count) * 100.0)
                                     / (product_metrics.view_count + EXCLUDED.view_count)
                                ELSE 0
                            END,
                            flash_sale_suggested =
                                ((product_metrics.view_count + EXCLUDED.view_count) > %s)
                                AND ((product_metrics.purchase_count + EXCLUDED.purchase_count) < %s),
                            processed_timestamp = CURRENT_TIMESTAMP;
                        """,
                        (
                            product_id,
                            window_start,
                            window_end,
                            view_count,
                            cart_count,
                            purchase_count,
                            total_events,
                            conversion_rate,
                            flash_sale_suggested,
                            FLASH_SALE_VIEW_THRESHOLD,
                            FLASH_SALE_PURCHASE_THRESHOLD,
                        ),
                    )

            self.connection.commit()
            logger.info(
                f"STREAM: saved windows={len(self.running_aggregates)} "
                "to PostgreSQL table 'product_metrics'"
            )
            self.running_aggregates.clear()
        
        except Exception as e:
            logger.error(f"Failed to flush aggregates: {e}")
            self.connection.rollback()

    def run(self) -> None:
        self.connect()
        logger.info("Starting Python fallback stream processor")
        logger.info(f"Configuration: Window={WINDOW_DURATION_MINUTES} min, Flush batch size={FALLBACK_FLUSH_BATCH_SIZE}")
        while True:
            try:
                for message in self.consumer:
                    self.process_event(message.value, message.offset)
                
                # Periodic flush even if batch not full
                if self.running_aggregates:
                    self.flush_aggregates()
            
            except Exception as e:
                logger.error(f"Python fallback processor error: {e}")
                if self.connection:
                    self.connection.rollback()
                time.sleep(5)
            
            finally:
                # Final flush on shutdown
                if self.running_aggregates:
                    try:
                        self.flush_aggregates()
                    except Exception as e:
                        logger.error(f"Error during final flush: {e}")


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
