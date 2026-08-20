"""
Backpressure & 0% 5xx Error Guarantee Integration Tests
Proves that under extreme concurrency overload, the system sheds load gracefully
with HTTP 429 and Retry-After headers, with 0% 5xx errors by construction.
"""

import sys
import os
import asyncio
import pytest
from httpx import AsyncClient, ASGITransport

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.src.main import app
from api.src.limiter import adaptive_limiter


@pytest.mark.asyncio
async def test_zero_5xx_under_severe_load():
    """
    Submits 200 concurrent requests simultaneously.
    Verifies that 100% of responses are either 202 (Accepted) or 429 (Shed),
    and exactly 0 requests result in HTTP 5xx.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        
        async def submit_report(i: int):
            payload = {
                "agency_id": f"flood-agency-{i % 10:02d}",
                "audit_type": "ISO_27001",
                "report_year": 2026,
                "auditor_org": "Stress Test Practice",
                "content_raw": f'{{"test_id": {i}, "data": "stress_payload_{i}"}}'
            }
            return await client.post("/api/v1/ingest", json=payload)

        # Launch 200 parallel tasks
        tasks = [submit_report(i) for i in range(200)]
        responses = await asyncio.gather(*tasks)

        status_counts = {}
        for r in responses:
            status_counts[r.status_code] = status_counts.get(r.status_code, 0) + 1

        print(f"\n[Backpressure Load Test Results] Status Distribution: {status_counts}")

        # Assert 0% 5xx errors
        errors_5xx = sum(count for code, count in status_counts.items() if code >= 500)
        assert errors_5xx == 0, f"Violation: Expected 0 HTTP 5xx errors, but received {errors_5xx}"

        # Assert all requests are valid 202 or 429
        for r in responses:
            assert r.status_code in [200, 202, 429], f"Unexpected status code: {r.status_code}"
            if r.status_code == 429:
                assert "retry-after" in r.headers or "Retry-After" in r.headers, "Missing Retry-After header on 429"


@pytest.mark.asyncio
async def test_adaptive_limiter_backpressure_rejection():
    """
    Directly tests that when in-flight capacity is reached,
    the limiter immediately sheds with 429 rather than timing out or failing.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Set limit to 2 and hold 2 in-flight slots
        adaptive_limiter.current_limit = 2.0
        admitted1, _ = await adaptive_limiter.acquire()
        admitted2, _ = await adaptive_limiter.acquire()
        assert admitted1 is True and admitted2 is True

        try:
            # Saturated request must be shed with 429
            res = await client.post("/api/v1/ingest", json={
                "agency_id": "saturated-test-agency",
                "audit_type": "SOC_2_TYPE_II",
                "content_raw": '{"saturated": true}'
            })

            assert res.status_code == 429, f"Expected 429 shedding, got: {res.status_code}"
            data = res.json()
            assert data["status"] == "REJECTED_BACKPRESSURE"
            assert "Retry-After" in res.headers or "retry-after" in res.headers
        finally:
            await adaptive_limiter.release(10.0)
            await adaptive_limiter.release(10.0)
