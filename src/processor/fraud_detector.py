import os
import json
import logging
from confluent_kafka import Consumer, Producer
from prometheus_client import start_http_server, Counter, Histogram
from src.processor.redis_client import RedisCache

# Prometheus Metrics
EVENTS_PROCESSED = Counter('events_processed_total', 'Total number of events processed', ['status'])
PROCESSING_TIME = Histogram('event_processing_seconds', 'Time spent processing an event')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_INITIATED = "payments.initiated"
TOPIC_VALIDATED = "payments.validated"
TOPIC_FRAUD = "payments.fraud_detected"

# Fraud Rules Configuration
MAX_TRANSACTIONS_PER_MINUTE = 3
MAX_AMOUNT_PER_TRANSACTION = 5000.00

def _delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")

def start_stream_processor():
    # 1. Initialize Consumer (Reading from API)
    # enable.auto.commit=False is crucial. If the processor crashes mid-event, 
    # we don't want Kafka to think we finished it. We only commit AFTER processing.
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'fraud-detection-group', # Consumer group allows horizontal scaling
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False 
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_INITIATED])

    # 2. Initialize Producer (Writing results)
    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'fraud-processor-producer',
        'acks': 'all',
        'enable.idempotence': True
    }
    producer = Producer(producer_conf)

    # 3. Initialize Distributed Cache
    cache = RedisCache()

    # 4. Start Prometheus Metrics Server
    start_http_server(8001)

    logger.info("Stream Processor started. Waiting for events... (Metrics on port 8001)")

    try:
        while True:
            # Poll for new messages (batching is possible here for high throughput)
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            # Parse event
            try:
                event = json.loads(msg.value().decode('utf-8'))
                user_id = event['user_id']
                amount = float(event['amount'])
                transaction_id = event['transaction_id']
                idempotency_key = event.get('idempotency_key', transaction_id)
                
                logger.info(f"Processing transaction {transaction_id} for {user_id}")

                # --- IDEMPOTENCY CHECK ---
                # This fixes the duplicate velocity inflation issue.
                if not cache.check_and_set_idempotency(idempotency_key):
                    logger.warning(f"🛡️ DUPLICATE DETECTED: {transaction_id} (key: {idempotency_key}) already processed. Skipping.")
                    consumer.commit(message=msg)
                    continue

                is_fraud = False
                fraud_reason = ""

                # Measure processing time
                with PROCESSING_TIME.time():
                    # --- FRAUD DETECTION LOGIC ---
                    
                    # Rule 1: High Dollar Value
                    if amount > MAX_AMOUNT_PER_TRANSACTION:
                        is_fraud = True
                        fraud_reason = "Amount exceeds maximum threshold"

                    # Rule 2: High Velocity (Stateful Check)
                    velocity = cache.increment_velocity(user_id=user_id, window_seconds=60)
                    if velocity > MAX_TRANSACTIONS_PER_MINUTE:
                        is_fraud = True
                        fraud_reason = f"Velocity exceeded: {velocity} transactions in 60s"
                # --- ROUTE EVENT ---
                event['status'] = "FRAUD" if is_fraud else "VALIDATED"
                if is_fraud:
                    event['fraud_reason'] = fraud_reason

                target_topic = TOPIC_FRAUD if is_fraud else TOPIC_VALIDATED

                # Publish decision
                producer.produce(
                    topic=target_topic,
                    key=user_id.encode('utf-8'),
                    value=json.dumps(event).encode('utf-8'),
                    callback=_delivery_report
                )
                
                # Update metrics
                EVENTS_PROCESSED.labels(status=event['status']).inc()
                producer.poll(0) # Trigger delivery callbacks

                # EXACTLY-ONCE / AT-LEAST-ONCE SEMANTICS:
                # We manually commit the offset ONLY after successfully pushing the result.
                # If this crashes right here, Kafka will re-deliver the message to another worker.
                consumer.commit(message=msg)
                
                if is_fraud:
                    logger.warning(f"🚫 FRAUD DETECTED: {user_id} - {fraud_reason}")
                else:
                    logger.info(f"✅ VALIDATED: {user_id} - ${amount}")

            except json.JSONDecodeError:
                logger.error("Failed to decode message.")
                consumer.commit(message=msg) # Commit bad messages so we don't get stuck in a loop
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # We DO NOT commit here. The message will be re-processed.

    except KeyboardInterrupt:
        logger.info("Shutting down stream processor...")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    start_stream_processor()
