from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import uuid

class PaymentRequest(BaseModel):
    user_id: str = Field(..., description="Unique identifier for the user")
    merchant_id: str = Field(..., description="Unique identifier for the merchant")
    amount: float = Field(..., gt=0, description="Transaction amount in USD")
    currency: str = Field(default="USD")
    idempotency_key: str = Field(..., description="Client-provided key to prevent duplicate processing")

class PaymentEvent(BaseModel):
    transaction_id: str
    user_id: str
    merchant_id: str
    amount: float
    currency: str
    status: str
    timestamp: str
    idempotency_key: str
