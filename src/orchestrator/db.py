import psycopg2
from psycopg2.extras import RealDictCursor
import os
import logging

logger = logging.getLogger(__name__)

class LedgerDB:
    def __init__(self):
        self.conn_str = f"dbname='payments_db' user='middleware_user' password='middleware_password' host='localhost' port='5432'"
        self._init_db()

    def get_connection(self):
        return psycopg2.connect(self.conn_str)

    def _init_db(self):
        """Creates the ledger table if it doesn't exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS ledger (
            transaction_id VARCHAR(50) PRIMARY KEY,
            user_id VARCHAR(50) NOT NULL,
            merchant_id VARCHAR(50) NOT NULL,
            amount DECIMAL(10, 2) NOT NULL,
            currency VARCHAR(3) DEFAULT 'USD',
            status VARCHAR(20) NOT NULL,
            idempotency_key VARCHAR(50) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        # The idempotency_key UNIQUE constraint is our ultimate safeguard against double-charging.
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_table_query)
                conn.commit()
            logger.info("PostgreSQL ledger table initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")

    def insert_transaction(self, event: dict, status: str) -> bool:
        """
        Inserts the transaction into the ledger.
        Returns True if inserted, False if it was a duplicate (idempotency caught it).
        """
        insert_query = """
        INSERT INTO ledger (transaction_id, user_id, merchant_id, amount, currency, status, idempotency_key)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (idempotency_key) DO NOTHING;
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(insert_query, (
                        event['transaction_id'],
                        event['user_id'],
                        event['merchant_id'],
                        event['amount'],
                        event.get('currency', 'USD'),
                        status,
                        event['idempotency_key']
                    ))
                    # rowcount is 0 if ON CONFLICT DO NOTHING was triggered
                    inserted = cur.rowcount > 0 
                conn.commit()
            return inserted
        except Exception as e:
            logger.error(f"Database error inserting {event['transaction_id']}: {e}")
            raise e
