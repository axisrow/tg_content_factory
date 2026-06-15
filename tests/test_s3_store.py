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
