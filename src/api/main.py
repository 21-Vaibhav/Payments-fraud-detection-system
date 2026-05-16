from fastapi import FastAPI, HTTPException, status
from src.api.models import PaymentRequest, PaymentEvent
from src.api.kafka_producer import PaymentProducer
import uuid
import datetime
import os
import logging
import asyncio
from prometheus_fastapi_instrumentator import Instrumentator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Payment Ingestion API")
Instrumentator().instrument(app).expose(app)

# Configure Kafka Bootstrap Server (defaulting to local docker-compose setup)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "payments.initiated"

producer = None

@app.on_event("startup")
async def startup_event():
    global producer
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    producer = PaymentProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

@app.on_event("shutdown")
async def shutdown_event():
    global producer
    if producer:
        logger.info("Flushing Kafka producer...")
        producer.flush()

@app.post("/api/v1/payments", status_code=status.HTTP_202_ACCEPTED)
async def create_payment(request: PaymentRequest):
    """
    Ingest a payment request.
    This endpoint is designed to be highly available. It does NOT wait for
    fraud checks or ledger updates. It simply validates the schema, 
    generates a transaction ID, and publishes to Kafka.
    """
    try:
        # Generate a unique transaction ID
        transaction_id = str(uuid.uuid4())
        timestamp = datetime.datetime.utcnow().isoformat() + "Z"

        # Construct the event payload
        event = PaymentEvent(
            transaction_id=transaction_id,
            user_id=request.user_id,
            merchant_id=request.merchant_id,
            amount=request.amount,
            currency=request.currency,
            status="INITIATED",
            timestamp=timestamp,
            idempotency_key=request.idempotency_key
        )

        # Publish to Kafka. 
        # We use user_id as the partition key. This guarantees that all transactions 
        # for a single user are processed in order by the same Kafka partition consumer.
        # This is vital for stateful fraud rules (e.g., sliding window limits).
        if producer:
            producer.publish_event(
                topic=TOPIC_NAME,
                key=request.user_id,
                value=event.model_dump()
            )
            # Yield control briefly to ensure delivery reports are processed 
            # (optional, depends on sync vs async strictness)
            producer.producer.poll(0)

        # We return 202 ACCEPTED. The processing is asynchronous.
        return {
            "status": "accepted",
            "transaction_id": transaction_id,
            "message": "Payment processing initiated"
        }

    except Exception as e:
        logger.error(f"Failed to process payment request: {e}")
        # If Kafka is completely unreachable, we return a 503.
        # In a real system, you might have a fallback mechanism like a local WAL
        # or writing to a temporary file, but failing fast is often safer.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Payment Gateway is temporarily unavailable."
        )

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
