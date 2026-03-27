from __future__ import annotations

import asyncio
import logging
import os

logger = logging.getLogger(__name__)


class S3Store:
    """Optional S3/MinIO image storage.

    Requires env vars: S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY.
    Uses boto3 if available; otherwise falls back to no-op (returns None).
    """

    def __init__(self, endpoint: str, bucket: str, access_key: str, secret_key: str) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._bucket = bucket
        self._access_key = access_key
        self._secret_key = secret_key

    async def upload_file(self, local_path: str) -> str | None:
        """Upload *local_path* to S3 and return the object URL, or None on failure."""
        try:
            import boto3
            from botocore.config import Config

            key = os.path.basename(local_path)

            def _upload() -> str:
                s3 = boto3.client(
                    "s3",
                    endpoint_url=self._endpoint,
                    aws_access_key_id=self._access_key,
                    aws_secret_access_key=self._secret_key,
                    config=Config(signature_version="s3v4"),
                )
                s3.upload_file(local_path, self._bucket, key)
                return f"{self._endpoint}/{self._bucket}/{key}"

            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, _upload)
        except ImportError:
            logger.warning("boto3 not installed; S3 upload skipped for %s", local_path)
            return None
        except Exception as exc:
            logger.warning("S3 upload failed for %s: %s", local_path, exc)
            return None

    @classmethod
    def from_env(cls) -> S3Store | None:
        """Create from environment variables, or return None if not configured."""
        endpoint = os.environ.get("S3_ENDPOINT", "")
        bucket = os.environ.get("S3_BUCKET", "")
        access_key = os.environ.get("S3_ACCESS_KEY", "")
        secret_key = os.environ.get("S3_SECRET_KEY", "")
        if endpoint and bucket and access_key and secret_key:
            return cls(endpoint, bucket, access_key, secret_key)
        return None
