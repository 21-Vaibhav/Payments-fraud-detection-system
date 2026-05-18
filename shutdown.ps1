Write-Host "=========================================================="
Write-Host "Stopping Payment Fraud Detection Middleware Infrastructure"
Write-Host "=========================================================="

Write-Host "`n[1/2] Stopping Docker containers..."
docker compose down

Write-Host "`n[2/2] Stopping Python Microservices..."

# Find and stop our specific Python processes
$processes = Get-WmiObject Win32_Process | Where-Object { 
    $_.CommandLine -match "src.api.main:app" -or 
    $_.CommandLine -match "src.cdc.cdc_poller" -or 
    $_.CommandLine -match "src.processor.fraud_detector" -or 
    $_.CommandLine -match "src.orchestrator.ledger_worker" -or
    $_.CommandLine -match "src/producer/load_test.py"
}

if ($processes) {
    foreach ($proc in $processes) {
        Write-Host " -> Killing Process ID: $($proc.ProcessId)"
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
    }
} else {
    Write-Host " -> No running Python microservices found."
}

Write-Host "`nAll processes have been stopped successfully! 👋"
Write-Host "=========================================================="
