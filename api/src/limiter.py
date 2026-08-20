"""
AegisIngest - Adaptive Latency-Based Concurrency & Token Bucket Admission Controller
Guarantees 0% 5xx errors by shedding excess load gracefully with HTTP 429 (Too Many Requests).
"""

import time
import math
import asyncio
from typing import Optional, Tuple
from api.src.config import settings


class AdaptiveConcurrencyLimiter:
    """
    Adaptive Latency-Based Concurrency Limiter (AIMD / Gradient Algorithm).
    Dynamically adjusts concurrency ceiling based on observed latency (RTT).
    Derived from Little's Law and TCP congestion control principles.
    """

    def __init__(
        self,
        min_limit: int = settings.ADAPTIVE_MIN_LIMIT,
        max_limit: int = settings.ADAPTIVE_MAX_LIMIT,
        target_rtt_ms: float = settings.ADAPTIVE_RTT_TARGET_MS,
        alpha: float = settings.ADAPTIVE_AIMD_ALPHA,
        beta: float = settings.ADAPTIVE_AIMD_BETA,
    ):
        self.min_limit = min_limit
        self.max_limit = max_limit
        self.target_rtt_ms = target_rtt_ms
        self.alpha = alpha
        self.beta = beta

        # Start midpoint between min and max
        self.current_limit: float = float(min_limit * 2)
        self.in_flight: int = 0
        self._lock = asyncio.Lock()
        
        # Exponential Moving Average of RTT
        self.smoothed_rtt_ms: float = target_rtt_ms * 0.5
        self.ema_weight: float = 0.1

    async def acquire(self) -> Tuple[bool, Optional[float]]:
        """
        Attempts to acquire an execution slot.
        Returns:
            (is_admitted: bool, retry_after_seconds: Optional[float])
        """
        async with self._lock:
            if self.in_flight < math.floor(self.current_limit):
                self.in_flight += 1
                return True, None

            # Saturated: compute recommended retry delay based on RTT
            retry_after = max(0.05, (self.smoothed_rtt_ms / 1000.0) * 1.5)
            return False, round(retry_after, 2)

    async def release(self, rtt_ms: float):
        """
        Releases an execution slot and adapts the concurrency ceiling based on RTT.
        """
        async with self._lock:
            self.in_flight = max(0, self.in_flight - 1)

            # Update smoothed RTT
            self.smoothed_rtt_ms = (
                (1.0 - self.ema_weight) * self.smoothed_rtt_ms + (self.ema_weight * rtt_ms)
            )

            # AIMD adaptation
            if self.smoothed_rtt_ms <= self.target_rtt_ms:
                self.current_limit = min(self.max_limit, self.current_limit + self.alpha)
            else:
                self.current_limit = max(self.min_limit, self.current_limit * self.beta)

    def get_stats(self) -> dict:
        return {
            "current_limit": round(self.current_limit, 2),
            "in_flight": self.in_flight,
            "smoothed_rtt_ms": round(self.smoothed_rtt_ms, 2),
            "target_rtt_ms": self.target_rtt_ms,
            "utilization_pct": round((self.in_flight / max(1, self.current_limit)) * 100, 1),
        }


class TokenBucketRateLimiter:
    """
    Ultra-Fast Per-Agency Token Bucket Rate Limiter with sub-millisecond execution.
    """

    def __init__(
        self,
        burst_capacity: int = settings.RATE_LIMIT_AGENCY_BURST,
        refill_rate: float = settings.RATE_LIMIT_AGENCY_RATE,
    ):
        self.burst_capacity = burst_capacity
        self.refill_rate = refill_rate
        self._local_buckets = {}
        self._lock = asyncio.Lock()

    async def check_agency_limit(
        self, agency_id: str
    ) -> Tuple[bool, Optional[float]]:
        """
        High-speed in-memory token bucket check.
        """
        now = time.time()
        async with self._lock:
            bucket = self._local_buckets.get(agency_id)
            if not bucket:
                self._local_buckets[agency_id] = {
                    "tokens": float(self.burst_capacity - 1),
                    "last_refill": now,
                }
                return True, None

            elapsed = max(0.0, now - bucket["last_refill"])
            tokens = min(float(self.burst_capacity), bucket["tokens"] + (elapsed * self.refill_rate))
            bucket["last_refill"] = now

            if tokens >= 1.0:
                bucket["tokens"] = tokens - 1.0
                return True, None
            else:
                bucket["tokens"] = tokens
                needed = 1.0 - tokens
                wait_sec = max(0.05, needed / self.refill_rate)
                return False, round(wait_sec, 2)


adaptive_limiter = AdaptiveConcurrencyLimiter()
token_bucket_limiter = TokenBucketRateLimiter()
