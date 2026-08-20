"""
Partitioning Contract & Sharding Invariants Tests
Proves that documents are routed strictly according to key hierarchy (agency_id)
with deterministic partition assignment.
"""

import sys
import os
import pytest
from httpx import AsyncClient, ASGITransport

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.src.main import app
from api.src.producer import hash_key_to_partition


@pytest.mark.asyncio
async def test_per_agency_partition_consistency():
    """
    Invariant 1: Consecutive submissions from the same agency must always return the same partition.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        agency = "agency-critical-infra-99"
        expected_partition = hash_key_to_partition(agency, 16)

        for i in range(5):
            payload = {
                "agency_id": agency,
                "audit_type": "NIST_CSF",
                "content_raw": f"Report iteration {i}"
            }
            res = await client.post("/api/v1/ingest", json=payload)
            if res.status_code == 202:
                data = res.json()
                assert data["partition"] == expected_partition, (
                    f"Partition mismatch for {agency}: got {data['partition']}, expected {expected_partition}"
                )


@pytest.mark.asyncio
async def test_cross_agency_sharding_distribution():
    """
    Invariant 2: Multiple agencies should be distributed across available partitions.
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        partitions = set()
        for i in range(20):
            agency = f"agency-dist-test-{i:02d}"
            payload = {
                "agency_id": agency,
                "audit_type": "ISO_27001",
                "content_raw": f"Distribution test {i}"
            }
            res = await client.post("/api/v1/ingest", json=payload)
            if res.status_code == 202:
                partitions.add(res.json()["partition"])

        # Expect distribution over at least 5 different partitions
        assert len(partitions) >= 5
