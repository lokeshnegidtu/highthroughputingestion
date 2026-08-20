"""
Unit Tests for AegisIngest Pipeline Components
Tests partition hashing, limiter mathematics, storage integrity, and cybersecurity analyzer.
"""

import os
import sys
import json
import pytest
import asyncio

# Add root to pythonpath
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from api.src.producer import hash_key_to_partition
from api.src.limiter import AdaptiveConcurrencyLimiter, TokenBucketRateLimiter
from api.src.storage import ContentAddressableStorage
from worker.src.analyzer import AuditBenchmarkAnalyzer


class TestPartitioningMath:
    def test_deterministic_partition_mapping(self):
        """Invariant 1: Identical agency_id must always map to the exact same partition."""
        agency = "agency-us-cert-042"
        p1 = hash_key_to_partition(agency, 16)
        p2 = hash_key_to_partition(agency, 16)
        p3 = hash_key_to_partition(agency, 16)
        
        assert 0 <= p1 < 16
        assert p1 == p2 == p3

    def test_partition_distribution_coverage(self):
        """Invariant 2: Hash distribution must spread across multiple partitions."""
        partitions_hit = set()
        for i in range(100):
            p = hash_key_to_partition(f"agency-enterprise-{i:03d}", 16)
            partitions_hit.add(p)
        
        # Across 100 random agencies, we should hit nearly all 16 partitions
        assert len(partitions_hit) >= 12


class TestAdaptiveLimiterMath:
    @pytest.mark.asyncio
    async def test_aimd_additive_increase_on_low_rtt(self):
        limiter = AdaptiveConcurrencyLimiter(min_limit=10, max_limit=100, target_rtt_ms=100.0, alpha=2.0, beta=0.8)
        initial_limit = limiter.current_limit

        # Acquire and release with healthy low RTT (20ms < 100ms)
        admitted, _ = await limiter.acquire()
        assert admitted is True
        await limiter.release(rtt_ms=20.0)

        assert limiter.current_limit > initial_limit

    @pytest.mark.asyncio
    async def test_aimd_multiplicative_decrease_on_high_rtt(self):
        limiter = AdaptiveConcurrencyLimiter(min_limit=10, max_limit=100, target_rtt_ms=100.0, alpha=2.0, beta=0.8)
        
        # Release with high RTT (300ms > 100ms) multiple times to degrade
        for _ in range(5):
            admitted, _ = await limiter.acquire()
            if admitted:
                await limiter.release(rtt_ms=350.0)

        # Limit should contract towards min_limit
        assert limiter.current_limit < 20.0
        assert limiter.current_limit >= limiter.min_limit


class TestTokenBucketLimiter:
    @pytest.mark.asyncio
    async def test_token_bucket_exhaustion_and_replenishment(self):
        # 5 token burst, 10 tokens/sec replenishment
        bucket = TokenBucketRateLimiter(burst_capacity=5, refill_rate=10.0)
        agency = "test-agency-burst"

        # First 5 should succeed
        for _ in range(5):
            allowed, _ = await bucket.check_agency_limit(agency)
            assert allowed is True

        # 6th should be rejected with positive retry_after
        allowed, retry_after = await bucket.check_agency_limit(agency)
        assert allowed is False
        assert retry_after > 0.0

        # Wait for replenishment (0.15s = ~1.5 tokens)
        await asyncio.sleep(0.15)
        allowed_after_wait, _ = await bucket.check_agency_limit(agency)
        assert allowed_after_wait is True


class TestContentAddressableStorage:
    @pytest.mark.asyncio
    async def test_cas_write_and_sha256_verification(self, tmp_path):
        cas = ContentAddressableStorage(base_dir=str(tmp_path))
        sample_data = b"Cybersecurity Audit Report Payload Content 2026"
        
        sha256, size, path = await cas.save_document(sample_data)
        assert len(sha256) == 64
        assert size == len(sample_data)
        assert os.path.exists(path)

        # Read back
        read_bytes = await cas.read_document(sha256)
        assert read_bytes == sample_data


class TestAuditBenchmarkAnalyzer:
    def test_analyzer_scoring_with_critical_findings(self):
        analyzer = AuditBenchmarkAnalyzer()
        payload = json.dumps({
            "agency_id": "agency-99",
            "findings": [
                {"id": "F-01", "severity": "CRITICAL", "description": "Unauthenticated RCE in gateway"},
                {"id": "F-02", "severity": "HIGH", "description": "Hardcoded admin credentials"}
            ],
            "controls_evaluated": 114
        })

        result = analyzer.analyze(payload, "ISO_27001", "agency-99")
        assert result["compliance_benchmark_score"] < 80.0
        assert result["findings_summary"]["critical"] == 1
        assert result["findings_summary"]["high"] == 1
        assert result["benchmark_status"] == "COMPLETED"

    def test_analyzer_clean_report_scoring(self):
        analyzer = AuditBenchmarkAnalyzer()
        payload = json.dumps({
            "agency_id": "agency-clean",
            "findings": [],
            "controls_evaluated": 114,
            "executive_summary": "Thorough audit completed with zero non-conformances identified."
        })

        result = analyzer.analyze(payload, "ISO_27001", "agency-clean")
        assert result["compliance_benchmark_score"] >= 95.0
        assert "A+" in result["posture_grade"]
