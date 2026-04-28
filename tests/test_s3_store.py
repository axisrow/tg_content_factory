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

    with patch.dict("sys.modules", {"boto3": mock_s3, "botocore": MagicMock(), "botocore.config": MagicMock()}):
        store = S3Store("https://s3.test", "bucket", "ak", "sk")
        url = await store.upload_file("/tmp/test_image.png")
        assert url == "https://s3.test/bucket/test_image.png"
        mock_client.upload_file.assert_called_once()


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
