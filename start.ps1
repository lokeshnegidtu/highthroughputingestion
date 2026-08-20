# AegisIngest Quickstart Script (PowerShell)
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Starting AegisIngest Pipeline Stack" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Build and start containers
docker compose up -d --build

Write-Host "`nWaiting 8 seconds for services to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 8

# 2. Run Health Check
python scripts/health_check.py

Write-Host "`nAccess Endpoints:" -ForegroundColor Green
Write-Host "  - Ingestion API:         http://localhost:8010/docs" -ForegroundColor White
Write-Host "  - Real-Time Dashboard:   http://localhost:8090" -ForegroundColor White
Write-Host "  - Grafana Dashboards:    http://localhost:3010" -ForegroundColor White
Write-Host "  - Prometheus Metrics:    http://localhost:9095" -ForegroundColor White
Write-Host "`nRun Load Test:" -ForegroundColor Cyan
Write-Host "  python scripts/load_test.py --total-requests 5000 --duration 30 --concurrency 2500" -ForegroundColor White
