from __future__ import annotations

import asyncio
import logging
import os
import urllib.request
import uuid
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

# Presigned GET URL lifetime. Must outlive the moderation/publish delay; 7 days
# is the SigV4 maximum.
PRESIGNED_TTL_SEC = 7 * 24 * 3600


def _unique_key(source: str) -> str:
    """Build a collision-free S3 object key from a source path/URL basename.

    Provider results frequently share generic basenames ("output.png", "image.png"),
    so keying on the basename alone made two generations write the SAME object — an
    existing presigned URL would then serve a later run's image (cross-run overwrite /
    exposure). Prefix a uuid4 so every upload gets a distinct key while keeping a
    readable, correctly-suffixed name (#862 review).
    """
    base = os.path.basename(source) or "image"
    ext = os.path.splitext(base)[1]
    if not ext or len(ext) > 10:
        ext = ".png"
    return f"images/{uuid.uuid4().hex}{ext}"


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

    def owns_url(self, url: str) -> bool:
        """True if *url* already points at this S3 endpoint (avoid re-mirroring)."""
        return bool(url) and url.startswith(self._endpoint)

    def _client(self):
        import boto3
        from botocore.config import Config

        return boto3.client(
            "s3",
            endpoint_url=self._endpoint,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            config=Config(signature_version="s3v4"),
        )

    def _presigned_get(self, s3, key: str) -> str:
        # Presigned URL so a private (default-ACL) object is still readable by the
        # moderation UI and Telegram, instead of a bare object URL that 403s
        # (audit #836/3).
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self._bucket, "Key": key},
            ExpiresIn=PRESIGNED_TTL_SEC,
        )

    async def upload_file(self, local_path: str) -> str | None:
        """Upload *local_path* to S3 and return a presigned GET URL, or None."""
        try:
            key = _unique_key(local_path)

            def _upload() -> str:
                s3 = self._client()
                s3.upload_file(local_path, self._bucket, key)
                return self._presigned_get(s3, key)

            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, _upload)
        except ImportError:
            logger.warning("boto3 not installed; S3 upload skipped for %s", local_path)
            return None
        except Exception as exc:
            logger.warning("S3 upload failed for %s: %s", local_path, exc)
            return None

    async def upload_url(self, url: str, key: str | None = None) -> str | None:
        """Mirror a remote image URL into S3 and return a durable presigned URL.

        Used for provider results that are ephemeral host URLs (e.g. Replicate,
        which expire ~24h) so a saved run does not 404 later (audit #836/4).
        """
        if not url or urlsplit(url).scheme not in ("http", "https"):
            return None
        try:
            # Honor an explicit caller key; otherwise build a collision-free unique key
            # (provider URLs often share a generic basename like output.png).
            object_key = key or _unique_key(urlsplit(url).path)

            def _upload() -> str:
                with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310 - provider URL
                    data = resp.read()
                s3 = self._client()
                s3.put_object(Bucket=self._bucket, Key=object_key, Body=data)
                return self._presigned_get(s3, object_key)

            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, _upload)
        except ImportError:
            logger.warning("boto3 not installed; S3 mirror skipped for %s", url)
            return None
        except Exception as exc:
            logger.warning("S3 mirror failed for %s: %s", url, exc)
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
