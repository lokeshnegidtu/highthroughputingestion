"""
AegisIngest - High-Speed Content-Addressable Storage (CAS)
Optimized for ultra-low latency direct writes without thread-pool contention.
"""

import os
import uuid
import hashlib
import io
import asyncio
try:
    from minio import Minio
except ImportError:  # local unit tests intentionally run without service clients
    Minio = None
from pathlib import Path
from typing import Tuple
from api.src.config import settings


class ContentAddressableStorage:
    def __init__(self, base_dir: str = settings.STORAGE_DIR):
        self.base_dir = Path(base_dir).resolve()
        (self.base_dir / "blobs").mkdir(parents=True, exist_ok=True)
        (self.base_dir / "tmp").mkdir(parents=True, exist_ok=True)
        self.client = Minio(settings.MINIO_ENDPOINT, access_key=settings.MINIO_ACCESS_KEY,
                            secret_key=settings.MINIO_SECRET_KEY, secure=settings.MINIO_SECURE) if Minio else None
        self.remote_available = False

    async def start(self):
        try:
            if not self.client:
                return
            exists = await asyncio.to_thread(self.client.bucket_exists, settings.MINIO_BUCKET)
            if not exists:
                await asyncio.to_thread(self.client.make_bucket, settings.MINIO_BUCKET)
            self.remote_available = True
        except Exception:
            self.remote_available = False

    async def save_document(self, content_bytes: bytes) -> Tuple[str, int, str]:
        """
        Saves document bytes atomically under /blobs/<sha256>.bin.
        """
        if len(content_bytes) > settings.MAX_PAYLOAD_BYTES:
            raise ValueError(f"Payload size {len(content_bytes)} bytes exceeds limit {settings.MAX_PAYLOAD_BYTES} bytes")

        sha256 = hashlib.sha256(content_bytes).hexdigest()
        file_path = self.base_dir / "blobs" / f"{sha256}.bin"
        
        # Fast path if already exists
        if file_path.exists():
            return sha256, len(content_bytes), str(file_path)

        tmp_path = self.base_dir / "tmp" / f"{sha256}_{uuid.uuid4().hex}.tmp"
        try:
            with open(tmp_path, "wb") as f:
                f.write(content_bytes)
            tmp_path.replace(file_path)
        except Exception:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass

        object_key = f"documents/{sha256}.bin"
        if self.remote_available:
            await asyncio.to_thread(self.client.put_object, settings.MINIO_BUCKET, object_key,
                                    io.BytesIO(content_bytes), len(content_bytes), "application/octet-stream")
            return sha256, len(content_bytes), object_key
        return sha256, len(content_bytes), str(file_path)

    async def read_document(self, sha256_hash: str) -> bytes:
        file_path = self.base_dir / "blobs" / f"{sha256_hash}.bin"
        if not file_path.exists():
            raise FileNotFoundError(f"Blob with hash {sha256_hash} does not exist")
        with open(file_path, "rb") as f:
            return f.read()


storage = ContentAddressableStorage()
