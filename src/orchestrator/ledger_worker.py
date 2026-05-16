import os
import json
import logging
import time
import random
from confluent_kafka import Consumer
from prometheus_client import start_http_server, Counter, Histogram
from src.orchestrator.db import LedgerDB

# Prometheus Metrics
LEDGER_WRITES = Counter('ledger_writes_total', 'Total number of transactions written to ledger', ['status'])
BANK_API_CALLS = Counter('bank_api_calls_total', 'Total number of simulated bank API calls', ['result'])
DB_WRITE_TIME = Histogram('db_write_seconds', 'Time spent writing to the PostgreSQL ledger')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_VALIDATED = "payments.validated"

# Simulated Bank API
def call_acquiring_bank(amount: float) -> bool:
    """
    Simulates a network call to Visa/Mastercard.
    Introduces a 20% random failure rate to demonstrate retries.
    """
    time.sleep(random.uniform(0.1, 0.3)) # Simulate network latency
    if random.random() < 0.20:
        BANK_API_CALLS.labels(result='failure').inc()
        raise ConnectionError("503 Service Unavailable: Acquiring Bank is down")
    BANK_API_CALLS.labels(result='success').inc()
    return True

def process_with_retry(event: dict, max_retries: int = 3):
    """
    Temporal-inspired retry loop with exponential backoff.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            call_acquiring_bank(event['amount'])
            logger.info(f"🏦 Bank approved transaction {event['transaction_id']}")
            return True
        except ConnectionError as e:
            attempt += 1
            wait_time = (2 ** attempt) + random.uniform(0, 1) # Exponential backoff + Jitter
            logger.warning(f"Bank API failed ({e}). Retrying in {wait_time:.2f}s (Attempt {attempt}/{max_retries})")
            time.sleep(wait_time)
            
    logger.error(f"❌ Bank API failed permanently for {event['transaction_id']}")
    return False

def start_ledger_worker():
    db = LedgerDB()
    
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'ledger-worker-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False # Manual offset commits are mandatory for financial ledgers
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_VALIDATED])

    # Start Prometheus Metrics Server
    start_http_server(8002)

    logger.info("Ledger Worker started. Waiting for validated payments... (Metrics on port 8002)")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): continue

            try:
                event = json.loads(msg.value().decode('utf-8'))
                tx_id = event['transaction_id']
                
                # 1. Orchestrate the Bank Call
                success = process_with_retry(event)
                final_status = "COMPLETED" if success else "FAILED_BANK_REJECT"

                # 2. Write to Source of Truth (Postgres Ledger)
                with DB_WRITE_TIME.time():
                    inserted = db.insert_transaction(event, status=final_status)
                
                LEDGER_WRITES.labels(status=final_status).inc()
                
                if inserted:
                    logger.info(f"💾 Saved to Ledger: {tx_id} [{final_status}]")
                else:
                    # Idempotency caught it! This means we crashed after calling the bank
                    # but before committing to Kafka, and Kafka redelivered it.
                    logger.warning(f"🛡️ IDEMPOTENCY TRIGGERED: Ignored duplicate write for {tx_id}")

                # 3. Commit the Kafka offset ONLY after the DB write succeeds
                consumer.commit(message=msg)

            except Exception as e:
                logger.error(f"Worker crashed processing message: {e}")
                # Notice we do NOT commit the offset here. 
                # Kafka will re-deliver this message to the next available worker.
                
    except KeyboardInterrupt:
        logger.info("Shutting down Ledger Worker...")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_ledger_worker()
