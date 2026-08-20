"""
Idempotency Integration Tests
Proves that identical documents submitted multiple times return the existing job record
without creating duplicate processing tasks or data corruption.
"""

import sys
import os
import pytest
from httpx import AsyncClient, ASGITransport

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.src.main import app


@pytest.mark.asyncio
async def test_idempotent_duplicate_submission():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "agency_id": "agency-idemp-01",
            "audit_type": "FEDRAMP_HIGH",
            "report_year": 2026,
            "auditor_org": "GovCloud Audit Corp",
            "content_raw": "Fixed Content Payload For Idempotency Testing 12345"
        }

        # 1. Initial Submission
        res1 = await client.post("/api/v1/ingest", json=payload)
        assert res1.status_code == 202
        data1 = res1.json()
        job_id_1 = data1["job_id"]
        sha_1 = data1["sha256_checksum"]

        # 2. Duplicate Submission with identical content
        res2 = await client.post("/api/v1/ingest", json=payload)
        assert res2.status_code in [200, 202]
        data2 = res2.json()

        # Must match original job_id and sha256 checksum
        assert data2["job_id"] == job_id_1
        assert data2["sha256_checksum"] == sha_1
        assert data2.get("is_idempotent_duplicate") is True
