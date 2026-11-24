"""EPIC-006 bounded file backend and opt-in real SAS checks."""
import hashlib
import io
import os
import sys
import time
import unittest
import uuid
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from azure.core import MatchConditions
from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError
from file_backend import AzureFiles, CHUNK_BYTES, MAX_FILE_BYTES
from fake_file_backend import FakeFiles
from search_backend import BackendError
from storage_backend import AZURITE_CONNECTION_STRING


class BoundedStream(io.BytesIO):
    def read(self, size=-1):
        if not 0 < size <= CHUNK_BYTES:
            raise AssertionError("unbounded read")
        return super().read(size)


class FakeFilesTests(unittest.TestCase):
    def setUp(self):
        self.files = FakeFiles()

    def test_roundtrip_and_immutable_publication(self):
        data = b"x" * (CHUNK_BYTES + 17)
        self.files.upload("stage/a", BoundedStream(data), len(data))
        result = self.files.publish("stage/a", "final/a")
        self.assertEqual(result, {"finalKey": "final/a", "size": len(data),
                                  "sha256": hashlib.sha256(data).hexdigest()})
        self.files.upload("stage/a", BoundedStream(b"changed"), 7)
        chunks = list(self.files.read("final/a"))
        self.assertTrue(all(len(chunk) <= CHUNK_BYTES for chunk in chunks))
        self.assertEqual(b"".join(chunks), data)
        with self.assertRaises(BackendError) as caught:
            self.files.publish("stage/a", "final/a")
        self.assertEqual(caught.exception.status, 409)

    def test_invalid_uploads(self):
        for length, stream in ((None, io.BytesIO()), (-1, io.BytesIO()),
                               (True, io.BytesIO()), (MAX_FILE_BYTES + 1, io.BytesIO()),
                               (2, BoundedStream(b"x")), (1, None)):
            with self.subTest(length=length), self.assertRaises(BackendError):
                self.files.upload("stage/a", stream, length)

    def test_invalid_keys(self):
        for key in ("", "/a", "../a", "a/../b", "a//b", "a\\b", "a?x", "a#x",
                    "http:a", "a/%2e%2e/b", "a/.", "a\x00"):
            with self.subTest(key=key), self.assertRaises(BackendError):
                self.files.probe(key)

    def test_missing_errors(self):
        for action, status, message in (
                (lambda: self.files.probe("absent"), 503, "file_unavailable"),
                (lambda: list(self.files.read("absent")), 503, "file_unavailable"),
                (lambda: self.files.publish("absent", "final/a"), 400, "invalid_file_source")):
            with self.assertRaises(BackendError) as caught:
                action()
            self.assertEqual((caught.exception.status, str(caught.exception)), (status, message))

    def test_source_change_and_fault_injection(self):
        self.files.blobs["stage/a"] = b"before"
        self.files.on_publish_chunk = lambda: self.files.blobs.update({"stage/a": b"after"})
        with self.assertRaises(BackendError):
            self.files.publish("stage/a", "final/a")
        self.assertNotIn("final/a", self.files.blobs)
        self.files.failures["probe"] = BackendError(503, "file_backend_unavailable")
        with self.assertRaises(BackendError):
            self.files.probe("stage/a")


class AzureFilesTests(unittest.TestCase):
    def setUp(self):
        self.files = AzureFiles(AZURITE_CONNECTION_STRING, "http://external:10000/devstoreaccount1")

    def test_endpoint_and_sas_permissions(self):
        for write, permission in ((True, "cw"), (False, "r")):
            url = self.files.signed_url("stage/a b", time.time() + 300, write=write)
            self.assertTrue(url.startswith("http://external:10000/devstoreaccount1/osdu-files/stage/a%20b?"))
            query = parse_qs(urlsplit(url).query)
            self.assertEqual(query["sp"], [permission])
            self.assertTrue(query["sv"])  # SAS version belongs to installed SDK.
            self.assertEqual(query["sr"], ["b"])

    def test_environment_and_invalid_endpoints(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(BackendError):
                AzureFiles.from_env(require_public=True)
            self.assertTrue(AzureFiles.from_env().public_endpoint.startswith("http://127.0.0.1:10000"))
        for root in ("ftp://a", "https://u:p@a", "https://a?x", "https://a#x",
                     "/relative", "https://a/../b", "https://a\\b"):
            with self.subTest(root=root), self.assertRaises(BackendError):
                AzureFiles(AZURITE_CONNECTION_STRING, root)

    def test_private_initialize_and_sanitized_errors(self):
        self.files.container = MagicMock()
        self.files.initialize()
        self.files.container.create_container.assert_called_once_with()
        self.files.container.create_container.side_effect = AzureError("secret SAS connection")
        with self.assertRaises(BackendError) as caught:
            self.files.initialize()
        self.assertEqual(str(caught.exception), "file_backend_unavailable")

    def configure_publish(self, data=b"payload"):
        source, final = MagicMock(), MagicMock()
        source.get_blob_properties.return_value = MagicMock(size=len(data), etag='"stage"')
        source.download_blob.return_value.chunks.return_value = iter([data])
        final.get_blob_properties.return_value = MagicMock(
            size=len(data), metadata={"sha256": hashlib.sha256(data).hexdigest()})
        def upload(chunks, **kwargs):
            list(chunks)
            return {"etag": '"final"'}
        final.upload_blob.side_effect = upload
        self.files.container = MagicMock()
        self.files.container.get_blob_client.side_effect = lambda key: source if key == "stage/a" else final
        return source, final

    def test_publish_conditional_bounded_create_only(self):
        source, final = self.configure_publish()
        result = self.files.publish("stage/a", "final/a")
        self.assertEqual(result["sha256"], hashlib.sha256(b"payload").hexdigest())
        source.download_blob.assert_called_once_with(
            etag='"stage"', match_condition=MatchConditions.IfNotModified, max_concurrency=1)
        self.assertFalse(final.upload_blob.call_args.kwargs["overwrite"])
        self.assertEqual(final.upload_blob.call_args.kwargs["max_concurrency"], 1)

    def test_source_change_never_succeeds(self):
        source, _ = self.configure_publish()
        source.download_blob.side_effect = AzureError("condition failed; secret")
        with self.assertRaises(BackendError):
            self.files.publish("stage/a", "final/a")

    def test_source_change_after_stream_and_truncation(self):
        source, _ = self.configure_publish()
        source.get_blob_properties.side_effect = [
            MagicMock(size=7, etag="before"), MagicMock(size=7, etag="after")]
        with self.assertRaises(BackendError):
            self.files.publish("stage/a", "final/a")
        source, _ = self.configure_publish()
        source.download_blob.return_value.chunks.return_value = iter([b"short"])
        with self.assertRaises(BackendError):
            self.files.publish("stage/a", "final/a")

    def test_empty_publish(self):
        self.configure_publish(b"")
        self.assertEqual(self.files.publish("stage/a", "final/a")["size"], 0)

    def test_sdk_chunk_configuration_and_internal_endpoint(self):
        self.assertIn("127.0.0.1:10000", self.files.client.url)
        self.assertEqual(self.files.client.api_version, "2023-11-03")
        for name in ("max_single_get_size", "max_chunk_get_size", "max_single_put_size", "max_block_size"):
            self.assertEqual(getattr(self.files.client._config, name), CHUNK_BYTES)

    def test_azure_upload_and_read_streaming(self):
        blob = MagicMock()
        self.files.container = MagicMock()
        self.files.container.get_blob_client.return_value = blob
        data = b"x" * (CHUNK_BYTES + 1)
        uploaded = []
        def capture(chunks, **kwargs):
            uploaded.extend(chunks)
        blob.upload_blob.side_effect = capture
        self.files.upload("stage/a", BoundedStream(data), len(data))
        self.assertEqual([len(c) for c in uploaded], [CHUNK_BYTES, 1])
        self.assertEqual(b"".join(uploaded), data)
        with self.assertRaises(BackendError):
            self.files.upload("stage/a", BoundedStream(b"short"), 20)
        blob.download_blob.return_value.properties.size = len(data)
        blob.download_blob.return_value.chunks.return_value = iter([data[:CHUNK_BYTES], data[CHUNK_BYTES:]])
        self.assertEqual(b"".join(self.files.read("stage/a")), data)
        blob.download_blob.side_effect = ResourceNotFoundError("private secret")
        with self.assertRaises(BackendError) as caught:
            list(self.files.read("missing"))
        self.assertEqual(str(caught.exception), "file_unavailable")

    def test_oversize_source_and_final_verification(self):
        source, final = self.configure_publish()
        source.get_blob_properties.return_value.size = MAX_FILE_BYTES + 1
        with self.assertRaises(BackendError):
            self.files.publish("stage/a", "final/a")
        final.upload_blob.assert_not_called()
        self.configure_publish()
        self.files.container.get_blob_client("final/a").get_blob_properties.return_value.metadata = {}
        with self.assertRaises(BackendError):
            self.files.publish("stage/a", "final/a")

    def test_missing_and_exists_sdk_errors(self):
        source, final = self.configure_publish()
        source.get_blob_properties.side_effect = ResourceNotFoundError("missing")
        with self.assertRaises(BackendError) as caught:
            self.files.publish("stage/a", "final/a")
        self.assertEqual(caught.exception.status, 400)
        source, final = self.configure_publish()
        final.upload_blob.side_effect = ResourceExistsError("exists")
        with self.assertRaises(BackendError) as caught:
            self.files.publish("stage/a", "final/a")
        self.assertEqual(caught.exception.status, 409)


@unittest.skipUnless(os.getenv("AZURITE_TEST_CONNECTION_STRING"), "opt-in real Azurite")
class RealFilesTests(unittest.TestCase):
    def test_real_sas_rest_and_immutable_publish(self):
        connection = os.environ["AZURITE_TEST_CONNECTION_STRING"]
        # Test-only inference is allowed; runtime core must explicitly configure its root.
        files = AzureFiles(connection, None)
        public = os.getenv("AZURE_STORAGE_PUBLIC_BLOB_ENDPOINT")
        if public:
            files = AzureFiles(connection, public)
        files.initialize()
        stage, final = "test-stage/" + uuid.uuid4().hex, "test-final/" + uuid.uuid4().hex
        payload = b"real-file-payload"
        def rest(url, method="GET", data=None, headers=None):
            try:
                with urlopen(Request(url, data=data, method=method, headers=headers or {}), timeout=10) as response:
                    return response.status, response.read()
            except HTTPError as error:
                return error.code, error.read()
        write = files.signed_url(stage, time.time() + 300, write=True)
        headers = {"x-ms-blob-type": "BlockBlob", "x-ms-version": "2023-11-03"}
        try:
            self.assertEqual(rest(write, "PUT", payload, headers)[0], 201)
            self.assertEqual(rest(write)[0], 403)
            self.assertEqual(files.publish(stage, final)["sha256"], hashlib.sha256(payload).hexdigest())
            read = files.signed_url(final, time.time() + 300)
            self.assertEqual(rest(read), (200, payload))
            self.assertEqual(rest(read, headers={"Range": "bytes=0-3"}), (206, payload[:4]))
            self.assertEqual(rest(read.replace(final, stage))[0], 403)
            self.assertEqual(rest(read + "x")[0], 403)
            self.assertEqual(rest(files.signed_url(final, time.time() - 300))[0], 403)
            listing = files.public_endpoint + "/osdu-files?restype=container&comp=list&" + urlsplit(write).query
            self.assertEqual(rest(listing)[0], 403)
            self.assertEqual(rest(write, "PUT", b"changed", headers)[0], 201)
            self.assertEqual(rest(read), (200, payload))
        finally:
            for key in (stage, final):
                files.container.get_blob_client(key).delete_blob()
