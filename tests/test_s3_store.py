"""Tests for S3Store."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.services.s3_store import S3Store


def test_from_env_configured(monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT", "https://s3.example.com")
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.setenv("S3_ACCESS_KEY", "access")
    monkeypatch.setenv("S3_SECRET_KEY", "secret")

    store = S3Store.from_env()
    assert store is not None
    assert store._endpoint == "https://s3.example.com"
    assert store._bucket == "test-bucket"


def test_from_env_missing_vars(monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT", "")
    monkeypatch.setenv("S3_BUCKET", "")
    monkeypatch.setenv("S3_ACCESS_KEY", "")
    monkeypatch.setenv("S3_SECRET_KEY", "")

    store = S3Store.from_env()
    assert store is None


def test_from_env_partial(monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT", "https://s3.example.com")
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.setenv("S3_ACCESS_KEY", "")
    monkeypatch.setenv("S3_SECRET_KEY", "")

    store = S3Store.from_env()
    assert store is None


def test_owns_url_matches_on_host_and_bucket():
    """Regression (#862 review): owns_url must match on host AND bucket path, not a string
    prefix or host alone. Path-style S3/MinIO share one host across buckets, so a same-host
    foreign-bucket URL must NOT be treated as ours, otherwise generate() skips mirroring and
    persists an ephemeral URL that 404s later."""
    store = S3Store("https://s3.example.com", "bucket", "ak", "sk")
    # Our endpoint + our bucket path → owned (this is the shape _presigned_get produces).
    assert store.owns_url("https://s3.example.com/bucket/images/abc.png?sig=x") is True
    assert store.owns_url("https://s3.example.com/bucket/k?sig=abc") is True
    # Same host, DIFFERENT bucket → NOT owned (host-only check would wrongly return True).
    assert store.owns_url("https://s3.example.com/other-bucket/k?sig=abc") is False
    # Bare endpoint with no bucket path → NOT owned (this store never emits such URLs).
    assert store.owns_url("https://s3.example.com") is False
    # Look-alike suffix host → NOT owned (the prefix-match bug would return True here).
    assert store.owns_url("https://s3.example.com.evil.com/bucket/x.png") is False
    # Unrelated host and empty input → NOT owned.
    assert store.owns_url("https://provider.test/output.png") is False
    assert store.owns_url("") is False


@pytest.mark.anyio
async def test_upload_file_success():
    mock_s3 = MagicMock()
    mock_client = MagicMock()
    mock_s3.client.return_value = mock_client
    mock_client.generate_presigned_url.return_value = "https://s3.test/bucket/test_image.png?sig=abc"

    with patch.dict("sys.modules", {"boto3": mock_s3, "botocore": MagicMock(), "botocore.config": MagicMock()}):
        store = S3Store("https://s3.test", "bucket", "ak", "sk")
        url = await store.upload_file("/tmp/test_image.png")
        # Returns a presigned GET URL so a private object stays readable (audit #836/3).
        assert url == "https://s3.test/bucket/test_image.png?sig=abc"
        mock_client.upload_file.assert_called_once()
        mock_client.generate_presigned_url.assert_called_once()


def test_unique_key_distinct_for_same_basename():
    """Regression (#862 review): two sources with the same basename must produce DISTINCT
    S3 keys, otherwise a later upload overwrites an earlier object that a presigned URL still
    points at (cross-run image overwrite/exposure). Extension is preserved."""
    from src.services.s3_store import _unique_key

    k1 = _unique_key("/a/output.png")
    k2 = _unique_key("/b/output.png")
    assert k1 != k2
    assert k1.endswith(".png") and k2.endswith(".png")
    # No suffix / weird suffix falls back to .png
    assert _unique_key("/x/output").endswith(".png")
    assert _unique_key("").endswith(".png")


@pytest.mark.anyio
async def test_upload_file_uses_unique_key_not_basename():
    """upload_file must write a unique object key (not the bare basename), so two uploads
    of files with the same name don't collide in S3 (#862 review)."""
    mock_s3 = MagicMock()
    mock_client = MagicMock()
    mock_s3.client.return_value = mock_client
    mock_client.generate_presigned_url.return_value = "https://s3.test/bucket/k?sig=abc"

    with patch.dict("sys.modules", {"boto3": mock_s3, "botocore": MagicMock(), "botocore.config": MagicMock()}):
        store = S3Store("https://s3.test", "bucket", "ak", "sk")
        await store.upload_file("/tmp/output.png")
        await store.upload_file("/other/output.png")

    keys = [c.args[2] for c in mock_client.upload_file.call_args_list]  # (local, bucket, key)
    assert keys[0] != keys[1], "same basename must not produce the same S3 key"
    assert all(k != "output.png" for k in keys)
    assert all(k.endswith(".png") for k in keys)


@pytest.mark.anyio
async def test_upload_file_no_boto3():
    with patch.dict("sys.modules", {"boto3": None}):
        store = S3Store("https://s3.test", "bucket", "ak", "sk")
        result = await store.upload_file("/tmp/test_image.png")
        assert result is None


@pytest.mark.anyio
async def test_upload_file_failure():
    mock_s3 = MagicMock()
    mock_client = MagicMock()
    mock_client.upload_file.side_effect = Exception("upload failed")
    mock_s3.client.return_value = mock_client

    with patch.dict("sys.modules", {"boto3": mock_s3, "botocore": MagicMock(), "botocore.config": MagicMock()}):
        store = S3Store("https://s3.test", "bucket", "ak", "sk")
        result = await store.upload_file("/tmp/test_image.png")
        assert result is None


# ── upload_url: bounded-read mirroring (#862 review) ──


class _FakeResp:
    """Minimal urlopen() context-manager stand-in with a chunked .read(n)."""

    def __init__(self, body: bytes, content_length: str | None = None) -> None:
        self._buf = body
        self._pos = 0
        self.headers = {} if content_length is None else {"Content-Length": content_length}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            chunk = self._buf[self._pos:]
            self._pos = len(self._buf)
            return chunk
        chunk = self._buf[self._pos:self._pos + n]
        self._pos += len(chunk)
        return chunk


def _mock_s3_modules():
    mock_s3 = MagicMock()
    mock_client = MagicMock()
    mock_s3.client.return_value = mock_client
    mock_client.generate_presigned_url.return_value = "https://s3.test/bucket/k?sig=abc"
    return mock_s3, mock_client


@pytest.mark.anyio
async def test_upload_url_mirrors_to_s3():
    mock_s3, mock_client = _mock_s3_modules()
    with patch.dict("sys.modules", {"boto3": mock_s3, "botocore": MagicMock(), "botocore.config": MagicMock()}), \
         patch("urllib.request.urlopen", return_value=_FakeResp(b"PNGDATA", "7")):
        store = S3Store("https://s3.test", "bucket", "ak", "sk")
        url = await store.upload_url("https://provider.test/output.png")
    assert url == "https://s3.test/bucket/k?sig=abc"
    body = mock_client.put_object.call_args.kwargs["Body"]
    assert body == b"PNGDATA"
    # put_object must set ContentType (unlike upload_file it does not auto-derive it),
    # else the mirrored object is stored as octet-stream and browsers download it (#862 review).
    assert mock_client.put_object.call_args.kwargs["ContentType"] == "image/png"


@pytest.mark.anyio
async def test_upload_url_rejects_oversize_content_length():
    """A Content-Length over the cap is rejected up front — body is never read into RAM."""
    from src.services.s3_store import MAX_MIRROR_BYTES

    mock_s3, mock_client = _mock_s3_modules()
    resp = _FakeResp(b"x", content_length=str(MAX_MIRROR_BYTES + 1))
    with patch.dict("sys.modules", {"boto3": mock_s3, "botocore": MagicMock(), "botocore.config": MagicMock()}), \
         patch("urllib.request.urlopen", return_value=resp):
        store = S3Store("https://s3.test", "bucket", "ak", "sk")
        url = await store.upload_url("https://provider.test/huge.png")
    assert url is None
    mock_client.put_object.assert_not_called()


@pytest.mark.anyio
async def test_upload_url_rejects_oversize_body_despite_lying_header():
    """A missing/understated Content-Length cannot bypass the cap — the streaming read
    still aborts once the body exceeds MAX_MIRROR_BYTES."""
    from src.services.s3_store import MAX_MIRROR_BYTES

    mock_s3, mock_client = _mock_s3_modules()
    oversized = b"a" * (MAX_MIRROR_BYTES + 1024)
    # No Content-Length header at all → must still be caught while reading.
    with patch.dict("sys.modules", {"boto3": mock_s3, "botocore": MagicMock(), "botocore.config": MagicMock()}), \
         patch("urllib.request.urlopen", return_value=_FakeResp(oversized, content_length=None)):
        store = S3Store("https://s3.test", "bucket", "ak", "sk")
        url = await store.upload_url("https://provider.test/lying.png")
    assert url is None
    mock_client.put_object.assert_not_called()


@pytest.mark.anyio
async def test_upload_url_rejects_non_http_scheme():
    store = S3Store("https://s3.test", "bucket", "ak", "sk")
    assert await store.upload_url("file:///etc/passwd") is None
    assert await store.upload_url("") is None
