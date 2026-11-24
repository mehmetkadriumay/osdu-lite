"""Atomic, partition-scoped Blob Storage snapshots for the bounded emulator."""
import hashlib
import json
import os

from azure.core import MatchConditions
from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContentSettings

from search_backend import BackendError

# Public Azurite emulator credentials, not an Azure account key.
AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)
MAX_SNAPSHOT_BYTES = 144 * 1024 * 1024


def parse_json(raw):
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON key")
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique_object)


def snapshot_bytes(state):
    try:
        raw = json.dumps(state, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
        if len(raw) > MAX_SNAPSHOT_BYTES:
            raise ValueError("Snapshot byte limit")
        return raw
    except (ValueError, TypeError, RecursionError, UnicodeError) as exc:
        raise BackendError(503, "Invalid or oversized storage snapshot") from exc


def validate_envelope(state):
    base = {"format", "records", "versions", "fences", "timestamps", "deleted", "suppressed"}
    control = {"schemas", "legalTags", "groups", "recordMetadata", "fileBindings", "uploadGrants", "revision"}
    if (not isinstance(state, dict) or type(state.get("format")) is not int
            or state["format"] not in (1, 2)
            or set(state) != base | (control if state["format"] == 2 else set())):
        raise BackendError(503, "Unsupported or malformed storage snapshot")
    snapshot_bytes(state)
    return state


class AzureBlobStorage:
    def __init__(self, connection_string, partition, container="osdu-records"):
        self.client = BlobServiceClient.from_connection_string(
            connection_string, api_version="2023-11-03", retry_total=0,
            connection_timeout=5, read_timeout=5)
        self.container = self.client.get_container_client(container)
        self.partition_hash = hashlib.sha256(partition.encode()).hexdigest()
        self.blob = self.container.get_blob_client(self.partition_hash + ".json")
        self.etag = None
        self.original_raw = None

    @classmethod
    def from_env(cls, partition):
        connection = os.getenv("AZURE_STORAGE_CONNECTION_STRING", AZURITE_CONNECTION_STRING)
        return cls(connection, partition, os.getenv("AZURE_STORAGE_CONTAINER", "osdu-records"))

    def load(self, *, initialize=True):
        try:
            if initialize:
                try:
                    self.container.create_container()
                except ResourceExistsError:
                    pass
            try:
                download = self.blob.download_blob()
            except ResourceNotFoundError as exc:
                if exc.error_code != "BlobNotFound":
                    raise
                self.etag = None
                self.original_raw = None
                return None
            if download.properties.size > MAX_SNAPSHOT_BYTES:
                raise ValueError("Snapshot byte limit")
            raw = download.readall()
            if len(raw) > MAX_SNAPSHOT_BYTES:
                raise ValueError("Snapshot byte limit")
            state = validate_envelope(parse_json(raw))
            self.etag = download.properties.etag
            self.original_raw = raw if state["format"] == 1 else None
            return state
        except (AzureError, ValueError, RecursionError, UnicodeError) as exc:
            raise BackendError(503, "Blob Storage unavailable or invalid snapshot") from exc

    def backup_format1(self):
        if self.original_raw is None:
            raise BackendError(503, "Original format1 snapshot unavailable")
        digest = hashlib.sha256(self.original_raw).hexdigest()
        name = self.partition_hash + "." + digest + ".format1.json"
        backup = self.container.get_blob_client(name)
        try:
            try:
                backup.upload_blob(self.original_raw, overwrite=False,
                                   content_settings=ContentSettings(content_type="application/json"))
            except ResourceExistsError:
                pass
            download = backup.download_blob()
            if download.properties.size > MAX_SNAPSHOT_BYTES:
                raise ValueError("Backup byte limit")
            if hashlib.sha256(download.readall()).hexdigest() != digest:
                raise ValueError("Backup digest mismatch")
        except (AzureError, ValueError) as exc:
            raise BackendError(503, "Format1 backup unavailable or digest mismatch") from exc

    def save(self, state):
        data = snapshot_bytes(validate_envelope(state))
        conditions = {"etag": self.etag, "match_condition": MatchConditions.IfNotModified} if self.etag else {}
        try:
            result = self.blob.upload_blob(
                data, overwrite=self.etag is not None, **conditions,
                content_settings=ContentSettings(content_type="application/json"))
            self.etag = result["etag"]
        except AzureError as exc:
            raise BackendError(503, "Blob Storage write failed; restart after a concurrent writer conflict") from exc
