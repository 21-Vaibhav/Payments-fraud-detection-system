# pyrefly: ignore [missing-import]
import redis
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class RedisCache:
    def __init__(self, host: str = "localhost", port: int = 6379):
        # In a real Visa/Stripe environment, you'd use a Redis Cluster 
        # or a distributed cache like Hazelcast for high availability.
        self.client = redis.Redis(
            host=os.getenv("REDIS_HOST", host),
            port=int(os.getenv("REDIS_PORT", port)),
            decode_responses=True # Returns strings instead of bytes
        )

    def increment_velocity(self, user_id: str, window_seconds: int = 60) -> int:
        """
        Implements a simple sliding window using Redis keys with TTL.
        Returns the number of transactions this user has made in the current window.
        """
        key = f"velocity:user:{user_id}"
        
        try:
            # We use a pipeline to execute multiple commands atomically
            pipe = self.client.pipeline()
            pipe.incr(key) # Increment the counter
            pipe.expire(key, window_seconds) # Reset the Time-To-Live (TTL)
            results = pipe.execute()
            
            # The first command in the pipeline (incr) returns the new value
            current_count = results[0]
            return current_count
        except Exception as e:
            logger.error(f"Redis error checking velocity for {user_id}: {e}")
            # In a distributed system, if the cache is down, we typically
            # "fail open" (allow the transaction) to prioritize availability, 
            # or "fail closed" (block it) to prioritize security. 
            # We will fail open here and assume 1 transaction.
            return 1

    def check_and_set_idempotency(self, idempotency_key: str, ttl: int = 86400) -> bool:
        """
        Returns True if the key was successfully set (this is a NEW transaction).
        Returns False if the key already exists (this is a DUPLICATE transaction).
        ttl default is 24 hours (86400 seconds).
        """
        key = f"idempotency:{idempotency_key}"
        try:
            # SETNX (set if not exists). In redis-py, set(nx=True) does this atomically.
            is_new = self.client.set(key, "1", nx=True, ex=ttl)
            return bool(is_new)
        except Exception as e:
            logger.error(f"Redis error checking idempotency key {idempotency_key}: {e}")
            # Fail open for availability
            return True
