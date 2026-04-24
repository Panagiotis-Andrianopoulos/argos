"""S3-compatible object storage client.

A thin wrapper γύρω από το boto3 S3 client που:
- Configures authentication from the settings
- Supports MinIO (local), Cloudflare R2, AWS S3
- Exposes minimal interface: put, get, exists, list

Doesn't know anything about domain entities (FRED data, etc.) — that's
the job of the higher layers.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Self

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from argos.config import settings

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


class ObjectStore:
    """Thin wrapper around from S3-compatible storage.

    Use:
        with ObjectStore.from_settings() as store:
            store.put(b'{"hello": "world"}', key="test/data.json")
            data = store.get("test/data.json")
    """

    def __init__(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        region: str = "auto",
    ) -> None:
        """Args:
        endpoint_url: URL of the S3-compatible service (e.g., http://localhost:9000).
        access_key: Access key credential.
        secret_key: Secret key credential.
        bucket: Name of the default bucket.
        region: S3 region (for MinIO/R2: "auto" is fine).
        """
        self._bucket = bucket
        self._client: S3Client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    @classmethod
    def from_settings(cls) -> Self:
        """Factory that creates client from the global settings."""
        return cls(
            endpoint_url=settings.s3_endpoint_url,
            access_key=settings.s3_access_key.get_secret_value(),
            secret_key=settings.s3_secret_key.get_secret_value(),
            bucket=settings.s3_bucket,
            region=settings.s3_region,
        )

    # ============================================================
    # Context manager support
    # ============================================================

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying client.

        The boto3 don't have explicit close, but keeps the method
        for consistency with others clients in ARGOS.
        """
        self._client.close()

    # ============================================================
    # Core operations
    # ============================================================

    def put(
        self,
        data: bytes,
        *,
        key: str,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Upload bytes in specific key.

        Args:
            data: The bytes to upload.
            key: The S3 key (path within the bucket).
            content_type: MIME type (e.g., "application/json").

        Raises:
            ClientError: If the upload fails.
        """
        logger.debug("PUT %s/%s (%d bytes)", self._bucket, key, len(data))
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )

    def get(self, key: str) -> bytes:
        """Download bytes from specific key.

        Args:
            key: The S3 key to download.

        Returns:
            The downloaded bytes.

        Raises:
            ClientError: If the key doesn't exist or download fails.
        """
        logger.debug("GET %s/%s", self._bucket, key)
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        data: bytes = response["Body"].read()
        return data

    def exists(self, key: str) -> bool:
        """Ckeck if the key exists, without downloading it."""
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
                return False
            raise
        return True

    def list_keys(self, prefix: str = "") -> Iterator[str]:
        """Yields keys that start with the given prefix.

        Args:
            prefix: The key prefix (e.g., "raw/fred/series/").

        Yields:
            Keys one by one.
        """
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                yield item["Key"]
