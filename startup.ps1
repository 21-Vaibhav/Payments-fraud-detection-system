Write-Host "=========================================================="
Write-Host "Starting Payment Fraud Detection Middleware Infrastructure"
Write-Host "=========================================================="

Write-Host "`n[1/3] Spinning up Docker containers (Kafka, DB, Redis, Grafana, etc.)..."
docker compose up -d

Write-Host "`nWaiting 15 seconds for infrastructure to initialize..."
Start-Sleep -Seconds 15

Write-Host "`n[2/3] Starting Python Microservices in new windows..."

# Start the FastAPI Server
Write-Host " -> Starting FastAPI Server"
Start-Process -FilePath "uvicorn" -ArgumentList "src.api.main:app", "--host", "127.0.0.1", "--port", "8000" -WindowStyle Normal

# Start the CDC Poller Worker
Write-Host " -> Starting CDC Poller"
Start-Process -FilePath "python" -ArgumentList "-m", "src.cdc.cdc_poller" -WindowStyle Normal

# Start the Fraud Detector Worker
Write-Host " -> Starting Fraud Detector"
Start-Process -FilePath "python" -ArgumentList "-m", "src.processor.fraud_detector" -WindowStyle Normal

# Start the Ledger Worker
Write-Host " -> Starting Ledger Worker"
Start-Process -FilePath "python" -ArgumentList "-m", "src.orchestrator.ledger_worker" -WindowStyle Normal

Write-Host "`n[3/3] System is up and running!"
Write-Host "=========================================================="
Write-Host "IMPORTANT URLS AND PORTS"
Write-Host "=========================================================="
Write-Host "Grafana Dashboard     : http://localhost:3000  (user: admin, pass: admin)"
Write-Host "Prometheus UI         : http://localhost:9090"
Write-Host "Kafka UI              : http://localhost:8090"
Write-Host "FastAPI Swagger UI    : http://localhost:8000/docs"
Write-Host "----------------------------------------------------------"
Write-Host "PostgreSQL            : localhost:5432"
Write-Host "Redis                 : localhost:6379"
Write-Host "Kafka                 : localhost:9092"
Write-Host "=========================================================="
Write-Host "`nNote: Python services (FastAPI, CDC, Processor, Ledger) have been opened in separate console windows."
