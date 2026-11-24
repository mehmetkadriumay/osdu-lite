"""EPIC-006 private Blob files, scoped SDK SAS and bounded publication."""
from datetime import datetime, timezone
import hashlib
import math
import os
from urllib.parse import quote, urlsplit

from azure.core import MatchConditions
from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobSasPermissions, BlobServiceClient, generate_blob_sas

from search_backend import BackendError
from storage_backend import AZURITE_CONNECTION_STRING

CHUNK_BYTES = 1024 * 1024
MAX_FILE_BYTES = 256 * CHUNK_BYTES
CONTAINER = "osdu-files"


def validate_key(key):
    try:
        encoded_size = len(key.encode("utf-8")) if isinstance(key, str) else 0
    except UnicodeError:
        raise BackendError(400, "invalid_file_source") from None
    if (not isinstance(key, str) or not key or encoded_size > 1024
            or any(c in key for c in "\\:?#%")
            or any(ord(c) < 32 or ord(c) == 127 for c in key)
            or any(part in ("", ".", "..") for part in key.split("/"))):
        raise BackendError(400, "invalid_file_source")
    return key


def validate_endpoint(endpoint):
    try:
        parsed = urlsplit(endpoint)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or "?" in endpoint or "#" in endpoint or "\\" in endpoint
                or any(c.isspace() or ord(c) < 32 for c in endpoint)
                or any(part in (".", "..") for part in parsed.path.split("/"))
                or "%" in parsed.path):
            raise ValueError()
        parsed.port
        return endpoint.rstrip("/")
    except (ValueError, TypeError, AttributeError):
        raise BackendError(503, "invalid_file_endpoint") from None


def input_chunks(stream, length):
    if (type(length) is not int or not 0 <= length <= MAX_FILE_BYTES
            or not callable(getattr(stream, "read", None))):
        raise BackendError(400, "invalid_file_source")
    remaining = length
    while remaining:
        try:
            data = stream.read(min(CHUNK_BYTES, remaining))
        except (OSError, ValueError, TypeError):
            raise BackendError(400, "invalid_file_source") from None
        if not isinstance(data, bytes) or not data or len(data) > min(CHUNK_BYTES, remaining):
            raise BackendError(400, "invalid_file_source")
        remaining -= len(data)
        yield data


class AzureFiles:
    def __init__(self, connection_string, public_endpoint):
        try:
            self.client = BlobServiceClient.from_connection_string(
                connection_string, api_version="2023-11-03", retry_total=0,
                connection_timeout=5, read_timeout=30,
                max_single_get_size=CHUNK_BYTES, max_chunk_get_size=CHUNK_BYTES,
                max_block_size=CHUNK_BYTES, max_single_put_size=CHUNK_BYTES)
            self.public_endpoint = validate_endpoint(
                self.client.url if public_endpoint is None else public_endpoint)
            self.container = self.client.get_container_client(CONTAINER)
        except (AzureError, ValueError, TypeError):
            raise BackendError(503, "file_backend_unavailable") from None

    @classmethod
    def from_env(cls, require_public=False):
        public = os.getenv("AZURE_STORAGE_PUBLIC_BLOB_ENDPOINT")
        if require_public and not public:
            raise BackendError(503, "invalid_file_endpoint")
        return cls(os.getenv("AZURE_STORAGE_CONNECTION_STRING", AZURITE_CONNECTION_STRING), public)

    def initialize(self):
        try:
            try:
                self.container.create_container()
            except ResourceExistsError:
                # Fail closed if a pre-existing container was configured public.
                if self.container.get_container_access_policy().get("public_access"):
                    raise BackendError(503, "file_backend_unavailable")
        except AzureError:
            raise BackendError(503, "file_backend_unavailable") from None

    def signed_url(self, key, expires_at, *, write=False):
        validate_key(key)
        try:
            if (isinstance(expires_at, bool) or not isinstance(expires_at, (int, float))
                    or not math.isfinite(expires_at)):
                raise ValueError()
            token = generate_blob_sas(
                account_name=self.client.account_name, container_name=CONTAINER,
                blob_name=key, account_key=self.client.credential.account_key,
                permission=BlobSasPermissions(create=True, write=True) if write else BlobSasPermissions(read=True),
                expiry=datetime.fromtimestamp(expires_at, timezone.utc))
            return self.public_endpoint + "/" + CONTAINER + "/" + quote(key, safe="/") + "?" + token
        except (AzureError, ValueError, TypeError, AttributeError, OverflowError, OSError):
            raise BackendError(503, "file_backend_unavailable") from None

    def upload(self, key, stream, length):
        validate_key(key)
        # Validate even zero-byte uploads before entering the SDK.
        if (type(length) is not int or not 0 <= length <= MAX_FILE_BYTES
                or not callable(getattr(stream, "read", None))):
            raise BackendError(400, "invalid_file_source")
        try:
            self.container.get_blob_client(key).upload_blob(
                input_chunks(stream, length), length=length, overwrite=True, max_concurrency=1)
        except AzureError:
            raise BackendError(503, "file_backend_unavailable") from None

    def probe(self, key):
        validate_key(key)
        try:
            props = self.container.get_blob_client(key).get_blob_properties()
            return {"size": props.size, "etag": props.etag}
        except ResourceNotFoundError:
            raise BackendError(503, "file_unavailable") from None
        except AzureError:
            raise BackendError(503, "file_backend_unavailable") from None

    def publish(self, staging_key, final_key):
        validate_key(staging_key)
        validate_key(final_key)
        if staging_key == final_key:
            raise BackendError(409, "file_already_exists")
        source = self.container.get_blob_client(staging_key)
        final = self.container.get_blob_client(final_key)
        try:
            try:
                props = source.get_blob_properties()
            except ResourceNotFoundError:
                raise BackendError(400, "invalid_file_source") from None
            if type(props.size) is not int or not 0 <= props.size <= MAX_FILE_BYTES:
                raise BackendError(400, "invalid_file_source")
            digest = hashlib.sha256()
            count = 0
            try:
                download = source.download_blob(
                    etag=props.etag, match_condition=MatchConditions.IfNotModified, max_concurrency=1)
            except ResourceNotFoundError:
                raise BackendError(400, "invalid_file_source") from None

            def chunks():
                nonlocal count
                for data in download.chunks():
                    if not isinstance(data, bytes) or len(data) > CHUNK_BYTES:
                        raise BackendError(503, "file_backend_unavailable")
                    count += len(data)
                    if count > props.size or count > MAX_FILE_BYTES:
                        raise BackendError(400, "invalid_file_source")
                    digest.update(data)
                    yield data
                if count != props.size:
                    raise BackendError(400, "invalid_file_source")

            uploaded = final.upload_blob(chunks(), length=props.size, overwrite=False, max_concurrency=1)
            if count != props.size or source.get_blob_properties().etag != props.etag:
                raise BackendError(400, "invalid_file_source")
            sha256 = digest.hexdigest()
            final.set_blob_metadata({"sha256": sha256}, etag=uploaded["etag"],
                                    match_condition=MatchConditions.IfNotModified)
            verified = final.get_blob_properties()
            if verified.size != count or verified.metadata.get("sha256") != sha256:
                raise BackendError(503, "file_backend_unavailable")
            return {"finalKey": final_key, "size": count, "sha256": sha256}
        except ResourceExistsError:
            raise BackendError(409, "file_already_exists") from None
        except ResourceNotFoundError:
            raise BackendError(503, "file_unavailable") from None
        except AzureError:
            raise BackendError(503, "file_backend_unavailable") from None

    def read(self, key):
        validate_key(key)
        try:
            download = self.container.get_blob_client(key).download_blob(max_concurrency=1)
            if download.properties.size > MAX_FILE_BYTES:
                raise BackendError(503, "file_unavailable")
            count = 0
            for chunk in download.chunks():
                count += len(chunk)
                if len(chunk) > CHUNK_BYTES or count > MAX_FILE_BYTES:
                    raise BackendError(503, "file_unavailable")
                yield chunk
            if count != download.properties.size:
                raise BackendError(503, "file_unavailable")
        except ResourceNotFoundError:
            raise BackendError(503, "file_unavailable") from None
        except AzureError:
            raise BackendError(503, "file_backend_unavailable") from None
