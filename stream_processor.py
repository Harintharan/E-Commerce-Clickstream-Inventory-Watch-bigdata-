#!/usr/bin/env python3
"""
PySpark Structured Streaming for E-Commerce Clickstream Processing
Performs sliding window aggregation and Flash Sale detection
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, window, count, when, sum as spark_sum,
    col as spark_col, current_timestamp, lit
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

import pandas as pd
from sqlalchemy import create_engine


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
        Write aggregated metrics to PostgreSQL using SQLAlchemy
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

            def write_to_db(batch_df: DataFrame, batch_id: int):
                """Write batch to PostgreSQL using SQLAlchemy"""
                try:
                    if batch_df.count() == 0:
                        logger.debug(f"Batch {batch_id}: No data to write")
                        return
                    
                    # Convert to pandas
                    pdf = batch_df.toPandas()
                    
                    # Create SQLAlchemy engine
                    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
                    engine = create_engine(db_url)
                    
                    # Write to database
                    pdf.to_sql(table_name, engine, if_exists=mode, index=False)
                    logger.info(f"Batch {batch_id}: Successfully wrote {len(pdf)} records to {table_name}")
                    
                except Exception as e:
                    logger.error(f"Batch {batch_id}: Failed to write to database: {e}")
                    raise

            query = output_df \
                .writeStream \
                .foreachBatch(write_to_db) \
                .trigger(processingTime="30 seconds") \
                .option("checkpointLocation", f"{CHECKPOINT_DIR}/postgres") \
                .start()

            logger.info(f"PostgreSQL output stream started for table: {table_name}")
            return query

        except Exception as e:
            logger.error(f"Failed to write to PostgreSQL: {e}")
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


def main():
    """Entry point"""
    processor = StreamProcessor()
    processor.run()


if __name__ == '__main__':
    main()
