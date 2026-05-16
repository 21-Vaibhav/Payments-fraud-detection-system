# Real-Time Payment Event Processing & Fraud Detection Middleware Platform

A distributed, event-driven middleware platform simulating enterprise-grade payment processing systems (similar to Visa, Stripe, or PayPal). This project demonstrates advanced distributed systems concepts, stream processing, high availability, and platform engineering best practices.

## 🚀 Architecture Overview

The platform decoupled into highly specialized microservices communicating asynchronously over Apache Kafka.

1. **Ingestion API (FastAPI)**: Highly available gateway that accepts payment requests, validates schema, attaches idempotency keys, and instantly pushes to Kafka.
2. **Stream Processor (Fraud Detector)**: A Flink-inspired stateful stream processor that consumes raw payments, checks velocity using a distributed cache (Redis), and routes transactions.
3. **Orchestrator (Ledger Worker)**: A Temporal-inspired worker that handles the "Saga". It manages simulated Bank API calls, exponential backoff retries, and strictly ordered writes to the PostgreSQL source-of-truth.
4. **Data Integration (CDC Poller)**: Simulates a Debezium-style outbox pattern by polling the database for finalized state changes and broadcasting them downstream.

Architecture diagram:
```
flowchart TD
    User(("User (Load Tester)")) -->|HTTP POST| API["API Gateway (FastAPI)"]
    
    subgraph Kafka ["Event Bus (Apache Kafka)"]
        K1[/"payments.initiated"/]
        K2[/"payments.validated"/]
        K3[/"payments.fraud_detected"/]
        K4[/"payments.cdc"/]
    end
    
    API -->|Produce Event| K1
    
    K1 -->|Consume| FraudProc["Fraud Detector (Stream Processor)"]
    
    subgraph Cache ["Distributed Cache"]
        Redis[("Redis (Velocity Tracking)")]
    end
    
    FraudProc <-->|Check 60s Sliding Window| Redis
    
    FraudProc -->|If Safe| K2
    FraudProc -->|If Fraud| K3
    
    K2 -->|Consume| LedgerWorker["Ledger Worker (Orchestrator)"]
    
    LedgerWorker <-->|Exponential Backoff Retries| BankAPI(("External Bank API (Visa)"))
    
    subgraph Database ["Source of Truth"]
        Postgres[("PostgreSQL (Ledger DB)")]
    end
    
    LedgerWorker -->|Write Final State and Idempotency Check| Postgres
    
    CDC["CDC Poller (Outbox Pattern)"] -->|Poll for new rows| Postgres
    CDC -->|Produce Event| K4
    
    K4 -->|Consume| Downstream(("Downstream Services - Email or Analytics"))
    
    subgraph Observability ["Observability Stack"]
        Prom{{"Prometheus (RED Metrics)"}}
        Graf{{"Grafana (Dashboards)"}}
        Prom -.->|Scrape Metrics| API
        Prom -.->|Scrape Metrics| FraudProc
        Prom -.->|Scrape Metrics| LedgerWorker
        Graf -.->|Query| Prom
    end
```


## 🛠 Tech Stack
* **Language:** Python 3.10+
* **Broker:** Apache Kafka & Zookeeper
* **Cache:** Redis (Stateful velocity checking)
* **Database:** PostgreSQL (Source of truth)
* **Observability:** Prometheus & Grafana (RED Metrics)
* **Orchestration:** Docker Compose & Kubernetes (Manifests included)
* **CI/CD:** GitHub Actions

## 🧠 Core Distributed Systems Concepts Demonstrated

* **Idempotency & Exactly-Once Semantics**: Prevents double-charging users during network retries using database constraints and manual Kafka offset management (`enable.auto.commit=False`).
* **Decoupling & Backpressure**: Using Kafka to buffer massive traffic spikes without crashing the downstream database.
* **Eventual Consistency**: Ensuring that the final settled state is asynchronously broadcasted via CDC.
* **Chaos Engineering Resilience**: The system can survive complete database outages, network partitions, and pod crashes without data loss.
* **Observability**: Prometheus RED metrics (Rate, Errors, Duration) for deep visibility.

## 🏃‍♂️ How to Run Locally

### 1. Start Infrastructure
```bash
docker-compose up -d
```
*(Wait 30-60s for Kafka, Postgres, Redis, Prometheus, and Grafana to initialize).*

### 2. Install Dependencies
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Start Microservices (In separate terminals)
```bash
# Terminal 1: API Gateway
uvicorn src.api.main:app --reload --port 8000

# Terminal 2: Stream Processor
python -m src.processor.fraud_detector

# Terminal 3: Orchestrator
python -m src.orchestrator.ledger_worker

# Terminal 4: CDC Worker
python -m src.cdc.cdc_poller
```

### 4. Generate Load
```bash
# Terminal 5: Traffic Simulator
python src/producer/load_test.py
```

## 📊 Observability Dashboards
* **Kafka UI:** [http://localhost:8090](http://localhost:8090)
* **Prometheus:** [http://localhost:9090](http://localhost:9090)
* **Grafana:** [http://localhost:3000](http://localhost:3000)

## 📁 Project Structure
```text
├── k8s/                    # Kubernetes declarative manifests
├── config/                 # Prometheus configurations
├── src/
│   ├── api/                # FastAPI Gateway
│   ├── processor/          # Kafka Consumer & Redis Fraud Logic
│   ├── orchestrator/       # DB integration & Retry Logic
│   ├── cdc/                # Change Data Capture worker
│   └── producer/           # Load testing scripts
├── docker-compose.yml      # Local infra orchestration
└── .github/workflows/      # CI/CD Pipeline
```


