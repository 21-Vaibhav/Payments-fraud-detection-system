import time
import json
import logging
import os
from datetime import datetime
from confluent_kafka import Producer
from src.orchestrator.db import LedgerDB

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_CDC = "payments.cdc"
WATERMARK_FILE = "cdc_watermark.txt"

def _delivery_report(err, msg):
    if err is not None:
        logger.error(f"CDC delivery failed: {err}")

class CDCPoller:
    """
    Simulates a Debezium-style Change Data Capture (CDC) worker.
    In a real production environment, Debezium tails the Postgres WAL (Write-Ahead Log)
    to get instantaneous, low-level binary replication events.
    Here, we simulate it via a polling pattern (often called the 'Outbox Pattern'
    if querying a specific outbox table, or a 'Watermark Poller' otherwise).
    """
    def __init__(self):
        self.db = LedgerDB()
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BROKER,
            'client.id': 'cdc-poller',
            'acks': 'all',
            'enable.idempotence': True
        })
        self.watermark = self._load_watermark()

    def _load_watermark(self) -> str:
        """Loads the last processed timestamp. If none, start from the beginning of time."""
        if os.path.exists(WATERMARK_FILE):
            with open(WATERMARK_FILE, "r") as f:
                ts = f.read().strip()
                if ts:
                    return ts
        return "1970-01-01 00:00:00"

    def _save_watermark(self, timestamp: datetime):
        """Saves the high-watermark so we don't process the same rows if we crash."""
        with open(WATERMARK_FILE, "w") as f:
            f.write(str(timestamp))

    def poll_database(self):
        query = """
            SELECT transaction_id, user_id, merchant_id, amount, currency, status, idempotency_key, created_at 
            FROM ledger 
            WHERE created_at > %s 
            ORDER BY created_at ASC 
            LIMIT 100;
        """
        try:
            with self.db.get_connection() as conn:
                # Use RealDictCursor to get results as dictionaries instead of tuples
                from psycopg2.extras import RealDictCursor
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, (self.watermark,))
                    rows = cur.fetchall()
                    return rows
        except Exception as e:
            logger.error(f"Error querying database: {e}")
            return []

    def start(self):
        logger.info(f"Starting CDC Poller. Current Watermark: {self.watermark}")
        
        while True:
            new_rows = self.poll_database()
            
            if new_rows:
                logger.info(f"CDC detected {len(new_rows)} new ledger entries.")
                
                last_ts = None
                for row in new_rows:
                    # Convert datetime to ISO string for JSON serialization
                    row['created_at'] = row['created_at'].isoformat()
                    
                    # Convert Decimal to float for JSON
                    row['amount'] = float(row['amount'])
                    
                    # Publish to Kafka
                    self.producer.produce(
                        topic=TOPIC_CDC,
                        key=row['transaction_id'].encode('utf-8'),
                        value=json.dumps(row).encode('utf-8'),
                        callback=_delivery_report
                    )
                    last_ts = row['created_at']

                # Wait for all messages in this batch to be delivered
                self.producer.flush()
                
                # Update watermark ONLY AFTER successful Kafka delivery
                if last_ts:
                    self.watermark = last_ts
                    self._save_watermark(last_ts)
                    logger.info(f"Watermark advanced to {self.watermark}")
            
            # Polling interval. Real Debezium has near-zero latency because it's push-based.
            # Polling introduces latency.
            time.sleep(2)

if __name__ == "__main__":
    poller = CDCPoller()
    try:
        poller.start()
    except KeyboardInterrupt:
        logger.info("Shutting down CDC Poller...")
