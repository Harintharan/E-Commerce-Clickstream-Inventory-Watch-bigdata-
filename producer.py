#!/usr/bin/env python3
"""
Kafka Producer for E-Commerce Clickstream Data
Generates simulated user events with optional anomalous patterns
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import Dict, Any

from kafka import KafkaProducer
from kafka.errors import KafkaError


# Configure logging
log_level = os.getenv('LOG_LEVEL', 'INFO')
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092').split(',')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'clickstream_topic')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
BATCH_INTERVAL_SECONDS = int(os.getenv('BATCH_INTERVAL_SECONDS', '5'))

# Product and user configuration
NUM_PRODUCTS = 50
NUM_USERS = 100
EVENT_TYPES = ['view', 'add_to_cart', 'purchase']

# Anomaly configuration - Flash Sale trigger threshold
ANOMALY_PRODUCTS = set(random.sample(range(1, NUM_PRODUCTS + 1), k=5))
ANOMALY_VIEW_THRESHOLD = 100
ANOMALY_PURCHASE_THRESHOLD = 5


class ClickstreamProducer:
    """Kafka producer for clickstream events with error handling and retry logic"""

    def __init__(self):
        """Initialize the Kafka producer"""
        self.producer = None
        self.event_counter = 0
        self._initialize_producer()

    def _initialize_producer(self) -> None:
        """Initialize Kafka producer with retry logic"""
        retry_attempts = 5
        retry_delay = 2

        for attempt in range(retry_attempts):
            try:
                logger.info(f"Attempting to connect to Kafka (attempt {attempt + 1}/{retry_attempts})")
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                )
                logger.info(f"Successfully connected to Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
                break
            except Exception as e:
                logger.error(f"Failed to connect to Kafka: {e}")
                if attempt < retry_attempts - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to Kafka after all attempts")
                    raise

    def generate_event(self) -> Dict[str, Any]:
        """
        Generate a single clickstream event
        With probability, generates anomalous patterns
        """
        user_id = random.randint(1, NUM_USERS)
        product_id = random.randint(1, NUM_PRODUCTS)

        # Decide event type based on probability distribution
        event_probabilities = [0.70, 0.20, 0.10]  # view, add_to_cart, purchase
        event_type = random.choices(EVENT_TYPES, weights=event_probabilities)[0]

        # Generate anomalous pattern for specific products
        # These products will have high views but low purchases to trigger Flash Sale
        if product_id in ANOMALY_PRODUCTS and random.random() < 0.85:
            if random.random() < 0.95:
                event_type = 'view'  # High view rate
            else:
                event_type = random.choice(['add_to_cart', 'purchase'])

        event = {
            'user_id': user_id,
            'product_id': product_id,
            'event_type': event_type,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'session_id': f"session_{user_id}_{int(time.time())}",
            'device': random.choice(['mobile', 'desktop', 'tablet']),
        }

        return event

    def send_event(self, event: Dict[str, Any]) -> None:
        """
        Send event to Kafka topic with error handling
        """
        try:
            future = self.producer.send(KAFKA_TOPIC, value=event)
            record_metadata = future.get(timeout=10)

            logger.debug(
                f"Event sent: user_id={event['user_id']}, "
                f"product_id={event['product_id']}, "
                f"type={event['event_type']}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}"
            )
            self.event_counter += 1

            if self.event_counter % 100 == 0:
                logger.info(f"Total events sent: {self.event_counter}")

        except KafkaError as e:
            logger.error(f"Kafka error while sending event: {e}")
        except Exception as e:
            logger.error(f"Unexpected error while sending event: {e}")

    def send_batch(self, batch_size: int = BATCH_SIZE) -> None:
        """Generate and send a batch of events"""
        try:
            for _ in range(batch_size):
                event = self.generate_event()
                self.send_event(event)
            self.producer.flush()
        except Exception as e:
            logger.error(f"Error during batch send: {e}")

    def run(self) -> None:
        """Main loop to continuously generate and send events"""
        logger.info(f"Starting Clickstream Producer")
        logger.info(f"Anomaly Products (Flash Sale candidates): {sorted(ANOMALY_PRODUCTS)}")
        logger.info(f"Batch size: {BATCH_SIZE}, Interval: {BATCH_INTERVAL_SECONDS}s")

        try:
            while True:
                self.send_batch()
                time.sleep(BATCH_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Gracefully shutdown the producer"""
        try:
            if self.producer:
                self.producer.flush()
                self.producer.close(timeout_ms=30000)
                logger.info(f"Producer shut down gracefully. Total events sent: {self.event_counter}")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")


def main():
    """Entry point"""
    producer = ClickstreamProducer()
    producer.run()


if __name__ == '__main__':
    main()
