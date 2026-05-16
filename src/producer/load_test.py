import requests
import time
import random
import uuid
import threading

API_URL = "http://127.0.0.1:8000/api/v1/payments"

# Some mocked users and merchants
USERS = [f"user_{i}" for i in range(1, 100)]
MERCHANTS = ["amazon", "netflix", "uber", "doordash", "apple"]

def simulate_traffic():
    while True:
        user = random.choice(USERS)
        merchant = random.choice(MERCHANTS)
        amount = round(random.uniform(5.0, 500.0), 2)
        
        # Idempotency key prevents double charging if a client retries due to network failure
        idempotency_key = str(uuid.uuid4())

        payload = {
            "user_id": user,
            "merchant_id": merchant,
            "amount": amount,
            "currency": "USD",
            "idempotency_key": idempotency_key
        }

        try:
            response = requests.post(API_URL, json=payload, timeout=2)
            if response.status_code == 202:
                print(f"[{response.status_code}] Sent ${amount} from {user} to {merchant}")
            else:
                print(f"Error: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Connection failed: {e}")

        # Sleep to simulate realistic traffic.
        # If you remove this sleep, you'll simulate a traffic spike (e.g., Black Friday)
        time.sleep(random.uniform(0.1, 0.5))

if __name__ == "__main__":
    print("Starting Payment Event Generator...")
    # Spin up a few threads to simulate concurrent clients
    threads = []
    for _ in range(3):
        t = threading.Thread(target=simulate_traffic)
        t.daemon = True
        t.start()
        threads.append(t)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping load generator.")
