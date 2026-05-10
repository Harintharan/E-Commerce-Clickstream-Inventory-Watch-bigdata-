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
from config import PRODUCER_CONFIG, KAFKA_CONFIG, LOGGING_CONFIG

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG['level']),
    format='%(asctime)s [%(levelname)-8s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Suppress verbose Kafka library logs
logging.getLogger('kafka').setLevel(logging.WARNING)
logging.getLogger('kafka.producer').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.WARNING)
logging.getLogger('kafka.protocol').setLevel(logging.WARNING)

# Configuration from config.py
KAFKA_BOOTSTRAP_SERVERS = KAFKA_CONFIG['bootstrap_servers']
KAFKA_TOPIC = KAFKA_CONFIG['topic']
BATCH_SIZE = PRODUCER_CONFIG['batch_size']
BATCH_INTERVAL_SECONDS = PRODUCER_CONFIG['batch_interval_seconds']
NUM_PRODUCTS = PRODUCER_CONFIG['num_products']
NUM_USERS = PRODUCER_CONFIG['num_users']
EVENT_TYPES = PRODUCER_CONFIG['event_types']
ANOMALY_VIEW_THRESHOLD = PRODUCER_CONFIG['anomaly_view_threshold']
ANOMALY_PURCHASE_THRESHOLD = PRODUCER_CONFIG['anomaly_purchase_threshold']

# Retry configuration
KAFKA_CONNECT_RETRY_ATTEMPTS = int(os.getenv('KAFKA_CONNECT_RETRY_ATTEMPTS', '30'))
KAFKA_CONNECT_RETRY_DELAY = int(os.getenv('KAFKA_CONNECT_RETRY_DELAY', '5'))

# Anomaly configuration - Flash Sale trigger threshold
ANOMALY_PRODUCTS = set(random.sample(range(1, NUM_PRODUCTS + 1), k=3))


class ClickstreamProducer:
    """Kafka producer for clickstream events with error handling and retry logic"""

    def __init__(self):
        """Initialize the Kafka producer"""
        self.producer = None
        self.event_counter = 0
        self._initialize_producer()

    def _initialize_producer(self) -> None:
        """Initialize Kafka producer with retry logic"""
        for attempt in range(KAFKA_CONNECT_RETRY_ATTEMPTS):
            try:
                logger.info(f"Connecting to Kafka (attempt {attempt + 1}/{KAFKA_CONNECT_RETRY_ATTEMPTS})...")
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                )
                logger.info(f"✓ Connected to Kafka: {', '.join(KAFKA_BOOTSTRAP_SERVERS)}")
                break
            except Exception as e:
                logger.warning(f"Connection failed: {e}")
                if attempt < KAFKA_CONNECT_RETRY_ATTEMPTS - 1:
                    logger.info(f"Retrying in {KAFKA_CONNECT_RETRY_DELAY}s...")
                    time.sleep(KAFKA_CONNECT_RETRY_DELAY)
                else:
                    logger.error("Failed to connect to Kafka after all retry attempts")
                    raise

    def generate_event(self) -> Dict[str, Any]:
        """
        Generate a single clickstream event
        With probability, generates anomalous patterns on specific products
        """
        user_id = random.randint(1, NUM_USERS)
        
        # Bias: 80% chance to pick anomaly product, 20% normal product (EXTREME for fast demo)
        if random.random() < 0.80:
            product_id = random.choice(list(ANOMALY_PRODUCTS))
        else:
            product_id = random.randint(1, NUM_PRODUCTS)

        # Decide event type based on probability distribution
        event_probabilities = [0.85, 0.10, 0.05]  # view, add_to_cart, purchase (reduced to 5%)
        event_type = random.choices(EVENT_TYPES, weights=event_probabilities)[0]

        # Generate anomalous pattern for specific products
        # These products will have high views but low purchases to trigger Flash Sale
        if product_id in ANOMALY_PRODUCTS and random.random() < 0.95:
            if random.random() < 0.99:  # 99% views for anomaly products
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

            self.event_counter += 1
            
            # Log only essential data
            logger.info(
                f"user_id: {event['user_id']:3d} | "
                f"product_id: {event['product_id']:2d} | "
                f"event_type: {event['event_type']:12s} | "
                f"timestamp: {event['timestamp']}"
            )

        except KafkaError as e:
            logger.error(f"✗ Kafka error: {e}")
        except Exception as e:
            logger.error(f"✗ Unexpected error: {e}")

    def send_batch(self, batch_size: int = None) -> None:
        """Generate and send a batch of events"""
        if batch_size is None:
            batch_size = BATCH_SIZE
        try:
            for _ in range(batch_size):
                event = self.generate_event()
                self.send_event(event)
            self.producer.flush()
        except Exception as e:
            logger.error(f"Error during batch send: {e}")

    def run(self) -> None:
        """Main loop to continuously generate and send events"""
        logger.info("=" * 80)
        logger.info("CLICKSTREAM EVENT PRODUCER STARTED")
        logger.info("=" * 80)
        logger.info(f"Anomaly Products: {sorted(ANOMALY_PRODUCTS)}")
        logger.info("=" * 80)

        try:
            while True:
                self.send_batch()
                time.sleep(BATCH_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("\n" + "=" * 80)
            logger.info("PRODUCER STOPPED")
            logger.info(f"Total events sent: {self.event_counter}")
            logger.info("=" * 80)
        except Exception as e:
            logger.error(f"Error: {e}")
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


class ClickstreamProducer:
    """Kafka producer for clickstream events with error handling and retry logic"""

    def __init__(self):
        """Initialize the Kafka producer"""
        self.producer = None
        self.event_counter = 0
        self._initialize_producer()

    def _initialize_producer(self) -> None:
        """Initialize Kafka producer with retry logic"""
        retry_attempts = int(os.getenv('KAFKA_CONNECT_RETRY_ATTEMPTS', '30'))
        retry_delay = int(os.getenv('KAFKA_CONNECT_RETRY_DELAY', '5'))

        for attempt in range(retry_attempts):
            try:
                logger.info(f"Connecting to Kafka (attempt {attempt + 1}/{retry_attempts})...")
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                )
                logger.info(f"✓ Connected to Kafka: {', '.join(KAFKA_BOOTSTRAP_SERVERS)}")
                break
            except Exception as e:
                logger.warning(f"Connection failed: {e}")
                if attempt < retry_attempts - 1:
                    logger.info(f"Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to Kafka after all retry attempts")
                    raise

    def generate_event(self) -> Dict[str, Any]:
        """
        Generate a single clickstream event
        With probability, generates anomalous patterns on specific products
        """
        user_id = random.randint(1, NUM_USERS)
        
        # Bias: 80% chance to pick anomaly product, 20% normal product (EXTREME for fast demo)
        if random.random() < 0.80:
            product_id = random.choice(list(ANOMALY_PRODUCTS))
        else:
            product_id = random.randint(1, NUM_PRODUCTS)

        # Decide event type based on probability distribution
        event_probabilities = [0.85, 0.10, 0.05]  # view, add_to_cart, purchase (reduced to 5%)
        event_type = random.choices(EVENT_TYPES, weights=event_probabilities)[0]

        # Generate anomalous pattern for specific products
        # These products will have high views but low purchases to trigger Flash Sale
        if product_id in ANOMALY_PRODUCTS and random.random() < 0.95:
            if random.random() < 0.99:  # 99% views for anomaly products
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

            self.event_counter += 1
            
            # Log only essential data
            logger.info(
                f"user_id: {event['user_id']:3d} | "
                f"product_id: {event['product_id']:2d} | "
                f"event_type: {event['event_type']:12s} | "
                f"timestamp: {event['timestamp']}"
            )

        except KafkaError as e:
            logger.error(f"✗ Kafka error: {e}")
        except Exception as e:
            logger.error(f"✗ Unexpected error: {e}")

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
        logger.info("=" * 80)
        logger.info("CLICKSTREAM EVENT PRODUCER STARTED")
        logger.info("=" * 80)
        logger.info(f"Anomaly Products: {sorted(ANOMALY_PRODUCTS)}")
        logger.info("=" * 80)

        try:
            while True:
                self.send_batch()
                time.sleep(BATCH_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("\n" + "=" * 80)
            logger.info("PRODUCER STOPPED")
            logger.info(f"Total events sent: {self.event_counter}")
            logger.info("=" * 80)
        except Exception as e:
            logger.error(f"Error: {e}")
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
