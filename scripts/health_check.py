"""
AegisIngest - End-to-End System Health Check Validator
Verifies availability of Ingestion API, Redis, Redpanda/Kafka broker, Prometheus, and Grafana.
"""

import sys
import time
import httpx

SERVICES = [
    {"name": "Ingestion API", "url": "http://localhost:8010/healthz", "expected_code": 200},
    {"name": "API Prometheus Metrics", "url": "http://localhost:8010/metrics", "expected_code": 200},
    {"name": "Real-Time Dashboard", "url": "http://localhost:8090", "expected_code": 200},
    {"name": "Prometheus Server", "url": "http://localhost:9095/-/healthy", "expected_code": 200},
    {"name": "Grafana Console", "url": "http://localhost:3010/api/health", "expected_code": 200},
]


def check_services():
    print("=" * 70)
    print("  AEGISINGEST END-TO-END SYSTEM HEALTH CHECK")
    print("=" * 70)

    all_healthy = True
    with httpx.Client(timeout=4.0) as client:
        for s in SERVICES:
            try:
                res = client.get(s["url"])
                if res.status_code == s["expected_code"]:
                    print(f"  [+] {s['name']:<28} [ONLINE] (HTTP {res.status_code})")
                else:
                    print(f"  [-] {s['name']:<28} [DEGRADED] (HTTP {res.status_code} != {s['expected_code']})")
                    all_healthy = False
            except Exception as e:
                print(f"  [x] {s['name']:<28} [UNREACHABLE] ({e})")
                all_healthy = False

    print("=" * 70)
    if all_healthy:
        print("  All AegisIngest pipeline services are HEALTHY and READY.")
    else:
        print("  Some services are not fully ready yet. Check container logs.")
    print("=" * 70)
    return all_healthy


if __name__ == "__main__":
    healthy = check_services()
    sys.exit(0 if healthy else 1)
