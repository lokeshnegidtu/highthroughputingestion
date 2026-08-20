#!/usr/bin/env bash
set -e

echo "=========================================================="
echo "  Starting AegisIngest Pipeline Stack"
echo "=========================================================="

# Build and start services
docker compose up -d --build

echo ""
echo "Waiting 8 seconds for services to initialize..."
sleep 8

# Run health check
python3 scripts/health_check.py || python scripts/health_check.py

echo ""
echo "Access Endpoints:"
echo "  - Ingestion API:         http://localhost:8010/docs"
echo "  - Real-Time Dashboard:   http://localhost:8090"
echo "  - Grafana Dashboards:    http://localhost:3010"
echo "  - Prometheus Metrics:    http://localhost:9095"
echo ""
echo "Run Load Test:"
echo "  python3 scripts/load_test.py --total-requests 5000 --duration 30 --concurrency 2500"
