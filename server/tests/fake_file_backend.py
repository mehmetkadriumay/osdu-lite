"""EPIC-006 injectable in-memory files; never selected by normal runtime."""
import hashlib
import os
from urllib.parse import quote

from file_backend import CHUNK_BYTES, CONTAINER, MAX_FILE_BYTES, input_chunks, validate_endpoint, validate_key
from search_backend import BackendError


class FakeFiles:
    def __init__(self, connection_string=None, public_endpoint="http://files.invalid/devstoreaccount1"):
        self.public_endpoint = validate_endpoint(public_endpoint)
        self.blobs = {}
        self.failures = {}
        self.chunks_read = []
        self.chunks_uploaded = []
        self.on_publish_chunk = None

    @classmethod
    def from_env(cls, require_public=False):
        public = os.getenv("AZURE_STORAGE_PUBLIC_BLOB_ENDPOINT")
        if require_public and not public:
            raise BackendError(503, "invalid_file_endpoint")
        return cls(public_endpoint=public or "http://files.invalid/devstoreaccount1")

    def _check(self, operation):
        if operation in self.failures:
            raise self.failures[operation]

    def initialize(self):
        self._check("initialize")

    def signed_url(self, key, expires_at, *, write=False):
        validate_key(key)
        self._check("signed_url")
        return (self.public_endpoint + "/" + CONTAINER + "/" + quote(key, safe="/")
                + "?sp=" + ("cw" if write else "r") + "&se=" + str(expires_at))

    def upload(self, key, stream, length):
        validate_key(key)
        self._check("upload")
        chunks = []
        for chunk in input_chunks(stream, length):
            self.chunks_uploaded.append(len(chunk))
            chunks.append(chunk)
        self.blobs[key] = b"".join(chunks)

    def probe(self, key):
        validate_key(key)
        self._check("probe")
        if key not in self.blobs:
            raise BackendError(503, "file_unavailable")
        data = self.blobs[key]
        return {"size": len(data), "etag": hashlib.sha256(data).hexdigest()}

    def publish(self, staging_key, final_key):
        validate_key(staging_key)
        validate_key(final_key)
        self._check("publish")
        if staging_key not in self.blobs:
            raise BackendError(400, "invalid_file_source")
        if final_key in self.blobs or staging_key == final_key:
            raise BackendError(409, "file_already_exists")
        data = self.blobs[staging_key]
        if len(data) > MAX_FILE_BYTES:
            raise BackendError(400, "invalid_file_source")
        digest = hashlib.sha256()
        for offset in range(0, len(data), CHUNK_BYTES):
            chunk = data[offset:offset + CHUNK_BYTES]
            self.chunks_read.append(len(chunk))
            digest.update(chunk)
            if self.on_publish_chunk:
                self.on_publish_chunk()
        if self.blobs.get(staging_key) != data:
            raise BackendError(400, "invalid_file_source")
        self.blobs[final_key] = memoryview(data).tobytes()
        return {"finalKey": final_key, "size": len(data), "sha256": digest.hexdigest()}

    def read(self, key):
        validate_key(key)
        self._check("read")
        if key not in self.blobs:
            raise BackendError(503, "file_unavailable")
        data = self.blobs[key]
        if len(data) > MAX_FILE_BYTES:
            raise BackendError(503, "file_unavailable")
        for offset in range(0, len(data), CHUNK_BYTES):
            chunk = data[offset:offset + CHUNK_BYTES]
            self.chunks_read.append(len(chunk))
            yield chunk
