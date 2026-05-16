import json
import logging
from confluent_kafka import Producer
from typing import Dict, Any

logger = logging.getLogger(__name__)

class PaymentProducer:
    def __init__(self, bootstrap_servers: str):
        # We use acks=all to ensure all in-sync replicas acknowledge the message.
        # This is CRITICAL for financial transactions to avoid data loss.
        # enable.idempotence=True ensures exactly-once semantics from producer to broker.
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'payment-api-producer',
            'acks': 'all',
            'enable.idempotence': True,
            'compression.type': 'snappy', # Good balance of CPU and compression ratio
            'linger.ms': 5, # Small artificial delay to allow batching for high throughput
            'retries': 5
        }
        self.producer = Producer(conf)

    def _delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def publish_event(self, topic: str, key: str, value: Dict[str, Any]):
        """
        Publish an event to Kafka.
        The 'key' is crucial for partitioning. All events with the same key
        (e.g., user_id) will go to the same partition, ensuring order.
        """
        try:
            # Trigger any available delivery report callbacks from previous produce() calls
            self.producer.poll(0)
            
            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8'),
                value=json.dumps(value).encode('utf-8'),
                callback=self._delivery_report
            )
        except Exception as e:
            logger.error(f"Failed to publish to Kafka: {e}")
            raise e

    def flush(self):
        """Wait for any outstanding messages to be delivered."""
        self.producer.flush()
