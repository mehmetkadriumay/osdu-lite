"""EPIC-001: offline public contracts and the deliberately bounded core shell."""
import datetime
import hashlib
import importlib.util
import io
import json
import os
import socket
import tempfile
import threading
import unittest
from email.message import Message
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from test_server import call, make_handler, server
from fake_search_backend import FakeSearch
from fake_storage_backend import FakeStorage


FIXTURE = Path(__file__).parent / "fixtures" / "osdu-core-contracts.json"
SOURCES = {
    "storage": ("storage/v2", "2.0.0", "4be71b3396aee5e560ddbff4a4540de9ca6c4e12f31f332938055371807a95b9"),
    "search": ("search/v2", "2.0", "2351885238f6334580e9220da9038cb8aa20edc38ffce08253f8330a01192f31"),
    "schema": ("schema-service/v1", "1.0", "2ec44ba7d30e3e98076c83df65975e4ef074b2c83dcfa63683091be9c1384b34"),
    "file": ("file/v2", "2.0.0", "0a40ec132cafb6f585d95cab661e4e3e4ee0aed068c5a7d4bca641b9ad0fba32"),
    "dataset": ("dataset/v1", "1.0.0", "d6599ee8736971630933517f2f50b11fc303a53b2d02a7dadc408943eefd695b"),
}
SELECTED = {
    "storage": {
        "/records": ["get", "put", "patch"],
        "/records/{id}": ["get", "delete"],
        "/records/{id}:delete": ["post"],
        "/records/{id}/{version}": ["get"],
        "/records/versions/{id}": ["get"],
        "/query/records:batch": ["post"],
    },
    "search": {"/query": ["post"], "/query_with_cursor": ["post"],
               "/query_with_cursor/{cursor}": ["delete"]},
    "schema": {"/schema": ["get", "post", "put"], "/schema/{id}": ["get"],
               "/schemas/system": ["put"]},
    "file": {"/v2/files/uploadURL": ["get"], "/v2/files/metadata": ["post"],
             "/v2/files/{id}/metadata": ["get", "delete"],
             "/v2/files/{id}/downloadURL": ["get"], "/v2/files/revokeURL": ["post"]},
    "dataset": {"/storageInstructions": ["post"], "/registerDataset": ["put"],
                "/retrievalInstructions": ["get", "post"], "/revokeURL": ["post"]},
}
DEVIATIONS = {
    "generated-id", "storage-batch", "storage-list", "bounded-patch",
    "schema-local-vocabulary", "schema-list-total", "file-version-prefix",
    "file-expiry", "dataset-file-only", "search-subset", "exact-total-count",
    "cursor-local-snapshot", "sas-revocation", "legacy-extensions",
}


def normalize(document, selected):
    """Keep exact selected wire definitions, not service descriptions/examples."""
    return {
        "operations": {
            method.upper() + " " + path: {
                key: operation[key] for key in ("parameters", "requestBody", "responses")
                if key in operation
            }
            for path, methods in selected.items()
            for method in methods
            for operation in [document["paths"][path][method]]
        },
        "schemas": document["components"]["schemas"],
    }


class PublicContractTests(unittest.TestCase):
    def setUp(self):
        self.bundle = json.loads(FIXTURE.read_text(encoding="utf-8"))

    def test_exact_five_bodies_and_provenance(self):
        self.assertEqual(set(self.bundle["sources"]), set(SOURCES))
        for name, (service, version, original) in SOURCES.items():
            with self.subTest(service=name):
                source = self.bundle["sources"][name]
                for body_key, manifest_key, endpoint in (
                    ("yaml", "manifest", "api-docs.yaml"), ("json", "jsonManifest", "api-docs")
                ):
                    raw = source[body_key].encode("utf-8")
                    manifest = source[manifest_key]
                    self.assertEqual(manifest["url"],
                                     "https://dev.osdu-main.osdu-cimpl.opengroup.org/api/"
                                     + service + "/" + endpoint)
                    self.assertEqual(manifest["httpStatus"], 200)
                    self.assertEqual(manifest["byteCount"], len(raw))
                    self.assertEqual(manifest["sha256"], hashlib.sha256(raw).hexdigest())
                    self.assertEqual(manifest["serviceVersion"], version)
                    self.assertEqual(manifest["openapiVersion"], "3.1.0")
                    instant = datetime.datetime.fromisoformat(manifest["retrievedAt"].replace("Z", "+00:00"))
                    self.assertEqual(instant.utcoffset(), datetime.timedelta(0))
                self.assertTrue(source["yaml"].startswith("openapi: 3.1.0\n"))
                self.assertNotIn("<html", source["yaml"][:512].lower())
                self.assertEqual(source["planningSha256"], original)
                self.assertEqual(source["operationDifferences"], [])
                self.assertEqual(source["manifest"]["sha256"], original)
                document = json.loads(source["json"])
                self.assertEqual(source["normalized"], normalize(document, SELECTED[name]))
        self.assertFalse(self.bundle["upstreamEntitySchemasImported"])

    def schema(self, service, name):
        return self.bundle["sources"][service]["normalized"]["schemas"][name]

    def operation(self, service, method, path):
        return self.bundle["sources"][service]["normalized"]["operations"][method + " " + path]

    def test_selected_operations_reference_the_exact_public_envelopes(self):
        for service, method, path, status, envelope in (
            ("storage", "PUT", "/records", "201", "CreateUpdateRecordsResponse"),
            ("storage", "POST", "/query/records:batch", "200", "MultiRecordResponse"),
            ("storage", "GET", "/records", "200", "RecordInfoQueryResult"),
            ("storage", "GET", "/records/versions/{id}", "200", "RecordVersions"),
            ("search", "POST", "/query", "200", "QueryResponse"),
            ("search", "POST", "/query_with_cursor", "200", "CursorQueryResponse"),
            ("schema", "GET", "/schema", "200", "SchemaInfoResponse"),
            ("schema", "POST", "/schema", "201", "SchemaInfo"),
            ("file", "GET", "/v2/files/uploadURL", "200", "LocationResponse"),
            ("file", "GET", "/v2/files/{id}/downloadURL", "200", "DownloadUrlResponse"),
            ("file", "POST", "/v2/files/metadata", "201", "FileMetadataResponse"),
            ("dataset", "POST", "/storageInstructions", "200", "GetDatasetStorageInstructionsResponse"),
            ("dataset", "PUT", "/registerDataset", "201", "GetCreateUpdateDatasetRegistryResponse"),
            ("dataset", "GET", "/retrievalInstructions", "200", "RetrievalInstructionsResponse"),
            ("dataset", "POST", "/retrievalInstructions", "200", "RetrievalInstructionsResponse"),
        ):
            with self.subTest(service=service, operation=method + " " + path):
                schema = self.operation(service, method, path)["responses"][status]["content"]["application/json"]["schema"]
                self.assertEqual(schema, {"$ref": "#/components/schemas/" + envelope})
        raw_get = self.operation("schema", "GET", "/schema/{id}")["responses"]["200"]["content"]
        self.assertEqual(raw_get["application/json"]["schema"], {"type": "object"})
        patch_operation = self.operation("storage", "PATCH", "/records")
        for status in ("200", "206"):
            schemas = patch_operation["responses"][status]["content"]["application/json"]["schema"]["oneOf"]
            self.assertEqual({schema["$ref"] for schema in schemas}, {
                "#/components/schemas/PatchRecordsResponse", "#/components/schemas/BulkUpdateRecordsResponse"})
        self.assertEqual(set(patch_operation["requestBody"]["content"]),
                         {"application/json", "application/json-patch+json"})

    def test_storage_exact_status_casing_and_envelopes(self):
        response = self.schema("storage", "CreateUpdateRecordsResponse")["properties"]
        self.assertEqual(set(response), {"recordCount", "recordIds", "recordIdVersions", "skippedRecordIds"})
        for name in ("recordIds", "recordIdVersions", "skippedRecordIds"):
            self.assertEqual(response[name]["items"]["type"], "string")
        self.assertIn("201", self.operation("storage", "PUT", "/records")["responses"])
        self.assertIn("204", self.operation("storage", "POST", "/records/{id}:delete")["responses"])
        self.assertIn("204", self.operation("storage", "DELETE", "/records/{id}")["responses"])
        self.assertEqual(set(self.schema("storage", "RecordVersions")["properties"]), {"recordId", "versions"})
        self.assertEqual(set(self.schema("storage", "PatchRecordsResponse")["properties"]),
                         {"recordCount", "recordIds", "notFoundRecordIds", "failedRecordIds", "errors"})
        self.assertIn("unAuthorizedRecordIds", self.schema("storage", "BulkUpdateRecordsResponse")["properties"])
        self.assertIn("id", self.schema("storage", "Record")["required"])  # resolved contradiction, not adopted
        batch = self.schema("storage", "MultiRecordResponse")["properties"]
        self.assertEqual(set(batch), {"records", "notFound", "conversionStatuses"})

    def test_search_exact_camel_case_and_envelopes(self):
        request = self.schema("search", "QueryRequest")["properties"]
        self.assertTrue({"queryAsOwner", "returnedFields", "excludedFields", "aggregateBy",
                         "spatialFilter", "trackTotalCount"} <= set(request))
        self.assertNotIn("query_as_owner", request)
        self.assertEqual(set(self.schema("search", "AggregationResponse")["properties"]), {"key", "count"})
        self.assertEqual(set(self.schema("search", "QueryResponse")["properties"]),
                         {"results", "aggregations", "phraseSuggestions", "totalCount"})
        self.assertEqual(set(self.schema("search", "CursorQueryResponse")["properties"]),
                         {"cursor", "results", "totalCount"})
        self.assertIn("200", self.operation("search", "POST", "/query")["responses"])

    def test_schema_file_dataset_exact_casing_status_and_envelopes(self):
        self.assertEqual(set(self.schema("schema", "SchemaRequest")["properties"]), {"schemaInfo", "schema"})
        self.assertEqual(set(self.schema("schema", "SchemaInfoResponse")["properties"]),
                         {"schemaInfos", "count", "totalCount", "offset"})
        self.assertEqual(set(self.schema("schema", "SchemaInfo")["properties"]["status"]["enum"]),
                         {"DEVELOPMENT", "PUBLISHED", "OBSOLETE"})
        self.assertIn("201", self.operation("schema", "POST", "/schema")["responses"])
        self.assertEqual(set(self.schema("file", "LocationResponse")["properties"]), {"FileID", "Location"})
        self.assertEqual(set(self.schema("file", "DownloadUrlResponse")["properties"]), {"SignedUrl"})
        self.assertIn("200", self.operation("file", "GET", "/v2/files/uploadURL")["responses"])
        self.assertIn("201", self.operation("file", "POST", "/v2/files/metadata")["responses"])
        self.assertEqual(set(self.schema("dataset", "GetDatasetStorageInstructionsResponse")["properties"]),
                         {"storageLocation", "providerKey"})
        self.assertEqual(set(self.schema("dataset", "RetrievalInstructionsResponse")["properties"]), {"datasets"})
        self.assertEqual(set(self.schema("dataset", "GetCreateUpdateDatasetRegistryResponse")["properties"]),
                         {"datasetRegistries"})
        self.assertIn("201", self.operation("dataset", "PUT", "/registerDataset")["responses"])

    def test_all_deliberate_deviations_have_explicit_local_decisions(self):
        deviations = {entry["id"]: entry for entry in self.bundle["deviations"]}
        self.assertEqual(set(deviations), DEVIATIONS)
        for entry in deviations.values():
            self.assertTrue(entry["sourceOperations"])
            self.assertGreater(len(entry["observed"]), 20)
            self.assertGreater(len(entry["localDecision"]), 20)
            for service, operation in entry["sourceOperations"]:
                self.assertIn(operation, self.bundle["sources"][service]["normalized"]["operations"])


class ProfileSelectionTests(unittest.TestCase):
    def test_default_explicit_and_invalid_selection(self):
        import core_profile
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(core_profile.select_profile(), "legacy")
        for value in ("legacy", "core"):
            with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": value}):
                self.assertEqual(core_profile.select_profile(), value)
        for value in ("", "CORE", " core", "other"):
            with self.subTest(value=value), patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": value}):
                with self.assertRaises(ValueError):
                    core_profile.select_profile()

    def test_startup_selects_profile_and_invalid_fails_before_backend_access(self):
        files = tempfile.TemporaryDirectory()
        self.addCleanup(files.cleanup)
        patcher = patch.object(server, "FILES_DIR", Path(files.name))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(server.start_runtime, FakeSearch(), storage=FakeStorage())
        for value in (None, "core", "legacy"):
            with patch.dict(os.environ):
                if value is None:
                    os.environ.pop("OSDU_COMPAT_PROFILE", None)
                else:
                    os.environ["OSDU_COMPAT_PROFILE"] = value
                server.start_runtime(FakeSearch(), storage=FakeStorage())
                self.assertEqual(server.COMPAT_PROFILE, value or "legacy")
        storage = FakeStorage()
        with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "invalid"}), patch.object(storage, "load") as load:
            with self.assertRaises(ValueError):
                server.start_runtime(FakeSearch(), storage=storage)
            load.assert_not_called()


class CoreRequestTests(unittest.TestCase):
    def test_container_packages_imported_core_modules(self):
        dockerfile = (Path(__file__).resolve().parents[1] / "Dockerfile").read_text()
        for module in ("core_profile", "core_auth"):
            self.assertIn(f"COPY {module}.py /app/{module}.py", dockerfile)

    def setUp(self):
        self.previous = getattr(server, "COMPAT_PROFILE", "legacy")
        server.COMPAT_PROFILE = "core"
        self.addCleanup(setattr, server, "COMPAT_PROFILE", self.previous)

    def request(self, method="POST", path="/api/search/v2/query", raw=b'{"kind":"*"}', headers=None):
        message = Message()
        for key, value in [("Authorization", "Bearer " + server.TOKEN),
                           ("data-partition-id", server.PARTITION)]:
            message[key] = value
        for key, value in (headers if headers is not None else [
            ("Content-Type", "application/json"), ("Content-Length", str(len(raw)))
        ]):
            message[key] = value
        handler = make_handler(path, message, raw)
        getattr(handler, "do_" + method)()
        body = handler.wfile.buf.getvalue()
        return handler, json.loads(body) if body else None

    def test_info_reports_only_implemented_core_features_and_actual_limits(self):
        status, result = call("get", "/info")
        self.assertEqual(status, 200)
        info = result["compatibility"]
        self.assertEqual(info["profile"], "core")
        self.assertEqual(info["snapshotFormat"], 2)
        self.assertEqual(info["supportedOperations"], [
            "GET /health", "GET /info",
            "PUT /api/storage/v2/records", "PATCH /api/storage/v2/records",
            "GET /api/storage/v2/records/{id}", "GET /api/storage/v2/records/{id}/{version}",
            "GET /api/storage/v2/records/versions/{id}",
            "POST /api/storage/v2/query/records:batch",
            "POST /api/storage/v2/records/{id}:delete", "DELETE /api/storage/v2/records/{id}",
            "GET /api/schema-service/v1/schema/{kind}", "GET /api/schema-service/v1/schema",
            "POST /api/schema-service/v1/schema", "PUT /api/schema-service/v1/schema",
            "GET /api/storage/v2/records", "POST /api/search/v2/query",
            "POST /api/search/v2/query_with_cursor", "DELETE /api/search/v2/query_with_cursor/{cursor}",
        ])
        self.assertEqual(info["supportedFeatures"],
                         ["profile-selection", "bounded-json-requests", "storage-writes",
                          "storage-retained-versions", "storage-patch",
                          "storage-list-selection", "schema-core", "search-core",
                          "storage-list-cursors", "durable-core-state"])
        self.assertTrue({"storage-core", "file-sas",
                         "dataset-files", "live-acl"} <= set(info["deferredFeatures"]))
        self.assertNotIn("durable-core-state", info["deferredFeatures"])
        self.assertFalse(info["localSchemas"]["upstreamVerified"])
        self.assertEqual(info["localSchemas"]["builtinKinds"], server.SUPPORTED_KINDS)
        self.assertEqual(info["limits"]["jsonBodyBytes"], 8 * 1024 * 1024)
        self.assertEqual(info["limits"]["retainedRecordBytes"], server.MAX_RECORD_BYTES)
        self.assertEqual(info["limits"]["retainedVersionsPerId"], server.MAX_RETAINED_VERSIONS)
        self.assertEqual(info["limits"]["groups"], 256)
        self.assertEqual(info["limits"]["membersPerGroup"], 256)
        self.assertEqual(info["limits"]["legalTags"], 256)
        self.assertEqual(info["limits"]["pendingUploadGrants"], 256)
        self.assertEqual(info["limits"]["fileBindings"], 5000)
        self.assertEqual(info["limits"]["controlStateBytes"], 8 * 1024 * 1024)
        self.assertEqual(info["limits"]["snapshotBytes"], 144 * 1024 * 1024)
        self.assertNotIn(server.TOKEN, json.dumps(result))

    def test_legacy_info_and_serializers_remain_separate(self):
        server.COMPAT_PROFILE = "legacy"
        status, result = call("get", "/info")
        self.assertEqual(status, 200)
        self.assertEqual(result["compatibility"]["profile"], "legacy")
        self.assertEqual(result["service"], "osdu-well360-local")
        self.assertEqual(call("post", "/api/search/v2/query", {"kind": "*"})[0], 200)

    def test_core_requests_never_fall_through_to_legacy_serializers(self):
        for method, suffix, raw, expected in (
            ("PUT", "storage/v2/records", b"[]", 400),
            ("GET", "storage/v2/records/id", b"", 400),
            ("POST", "search/v2/query", b'{"kind":"*"}', 200),
            ("POST", "schema-service/v1/schema", b"{}", 400),
            ("GET", "file/v2/files/uploadURL", b"", 503),
            ("PUT", "dataset/v1/registerDataset", b'{"datasetRegistries":[]}', 400),
            ("DELETE", "storage/v2/records/id", b"", 400),
        ):
            for prefix in ("/api/", "/"):
                with self.subTest(method=method, path=prefix + suffix):
                    handler, result = self.request(method, prefix + suffix, raw)
                    self.assertEqual(handler._status, expected, result)
                    if expected == 501:
                        self.assertEqual(result["reason"], "core_operation_not_implemented")

    def test_legacy_storage_aliases_are_rejected_with_guidance(self):
        for method, raw, expected in (("POST", b"[]", 405), ("PATCH", b'{"id":"x"}', 400)):
            handler, result = self.request(method, "/api/storage/v2/records", raw)
            self.assertEqual(handler._status, expected, result)
            self.assertIn("core", result["message"])

    def test_recognized_unsupported_and_unknown_search_keys(self):
        for key, value, status in (
            ("spatialFilter", {}, 501), ("suggestPhrase", "", 501),
            ("highlightedFields", [], 501), ("search_after", True, 501),
            ("sort", {"field": ["id"], "order": ["ASC"], "filter": {}}, 501),
            ("query_as_owner", True, 400), ("aggregations", [], 400), ("typo", 1, 400),
        ):
            for suffix in ("query", "query_with_cursor"):
                raw = json.dumps({"kind": "*", key: value}).encode()
                handler, result = self.request(path="/search/v2/" + suffix, raw=raw)
                self.assertEqual(handler._status, status, (key, result))
        handler, _ = self.request("GET", "/api/storage/v2/records?modifyAfterDate=bad", b"")
        self.assertEqual(handler._status, 400)

    def test_malformed_framing_is_rejected_without_reading(self):
        for headers in (
            [("Content-Type", "application/json")],
            [("Content-Length", "1"), ("Content-Length", "1")],
            [("Content-Length", "1"), ("Transfer-Encoding", "chunked")],
            [("Content-Length", "1"), ("Transfer-Encoding", "")],
            [("Content-Length", "1, 1")],
            *[[("Content-Length", text)] for text in ("-1", "+1", "NaN", "1.0", "1e2", "inf", "١")],
        ):
            with self.subTest(headers=headers):
                handler, result = self.request(headers=headers)
                self.assertEqual(handler._status, 400, result)
                self.assertEqual(handler.rfile.tell(), 0)
                self.assertTrue(handler.close_connection)
                self.assertIn(("Connection", "close"), handler._response_headers)

    def test_oversize_is_rejected_before_body_read_on_all_touched_services(self):
        for method, path in (
            ("POST", "/api/search/v2/query"), ("PUT", "/storage/v2/records"),
            ("POST", "/schema-service/v1/schema"), ("POST", "/api/file/v2/files/metadata"),
            ("PUT", "/api/dataset/v1/registerDataset"), ("PATCH", "/storage/v2/records")
        ):
            handler, result = self.request(method, path, headers=[
                ("Content-Length", str(8 * 1024 * 1024 + 1)), ("Content-Type", "application/json")])
            self.assertEqual(handler._status, 413, result)
            self.assertEqual(handler.rfile.tell(), 0)
            self.assertTrue(handler.close_connection)

    def test_media_type_validation(self):
        for headers in (
            [("Content-Length", "2")],
            [("Content-Length", "2"), ("Content-Type", "text/plain")],
            [("Content-Length", "2"), ("Content-Type", "application/json; charset=utf-16")],
            [("Content-Length", "2"), ("Content-Type", "application/json"),
             ("Content-Type", "application/json")],
            [("Content-Length", "2"), ("Content-Type", "application/json-patch+json")],
        ):
            handler, result = self.request(raw=b"{}", headers=headers)
            self.assertEqual(handler._status, 415, result)
            self.assertEqual(handler.rfile.tell(), 0)
        raw = b'{"query":{"ids":["x"]},"ops":[]}'
        handler, result = self.request("PATCH", "/storage/v2/records", raw, [
            ("Content-Length", str(len(raw))), ("Content-Type", "application/json-patch+json")])
        self.assertEqual(handler._status, 400, result)

    def test_malformed_nonfinite_and_nonobject_search_json(self):
        for raw in (b"", b"{", b'{"kind":NaN}', b'{"kind":Infinity}', b'{"kind":-Infinity}',
                    b'{"kind":1e9999}', b'{"kind":"\xff"}', b'{"kind":1} trailing',
                    b"null", b"[]", b"true", b"42", b"[" * 1500 + b"]" * 1500):
            with self.subTest(raw=raw[:64]):
                handler, result = self.request(raw=raw)
                self.assertEqual(handler._status, 400, result)
                self.assertTrue(handler.close_connection)

    def test_truncated_body_is_rejected_and_get_body_cannot_be_ignored(self):
        handler, result = self.request(raw=b"{}", headers=[
            ("Content-Length", "3"), ("Content-Type", "application/json")])
        self.assertEqual(handler._status, 400, result)
        handler, result = self.request("GET", "/storage/v2/records", b"{}")
        self.assertEqual(handler._status, 400, result)

    def test_bounded_parser_accepts_utf8_and_exact_limit(self):
        import core_profile
        for raw in ('{"name":"é"}'.encode(), b"{}" + b" " * (8 * 1024 * 1024 - 2)):
            headers = Message()
            headers["Content-Length"] = str(len(raw))
            headers["Content-Type"] = "application/json; charset=UTF-8"
            self.assertIsInstance(core_profile.read_json(io.BytesIO(raw), headers), dict)

    def test_core_profile_import_has_no_network_or_runtime_side_effects(self):
        spec = importlib.util.spec_from_file_location("isolated_core_profile",
                                                    Path(server.__file__).with_name("core_profile.py"))
        isolated = importlib.util.module_from_spec(spec)
        with patch("urllib.request.urlopen", side_effect=AssertionError("network at import")):
            spec.loader.exec_module(isolated)
        self.assertEqual(isolated.MAX_JSON_BYTES, 8 * 1024 * 1024)

    def test_sas_revocation_does_not_claim_success(self):
        for path in ("/api/file/v2/files/revokeURL", "/dataset/v1/revokeURL"):
            handler, result = self.request(path=path, raw=b"{}")
            self.assertEqual(handler._status, 501)
            self.assertEqual(result["reason"], "unsupported_immediate_sas_revocation")

    def test_actual_http_framing_rejects_without_waiting_for_a_body(self):
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
        worker = threading.Thread(target=httpd.serve_forever, daemon=True)
        worker.start()
        try:
            for framing, expected in (
                ("Content-Length: 8388609\r\n", b"413"),
                ("Content-Length: 2\r\nContent-Length: 2\r\n", b"400"),
                ("Content-Length: 2\r\nTransfer-Encoding: chunked\r\n", b"400"),
            ):
                with self.subTest(framing=framing), socket.create_connection(httpd.server_address, timeout=3) as client:
                    request = (
                        "POST /api/search/v2/query HTTP/1.1\r\nHost: localhost\r\n"
                        f"Authorization: Bearer {server.TOKEN}\r\ndata-partition-id: {server.PARTITION}\r\n"
                        "Content-Type: application/json\r\n" + framing + "\r\n"
                    )
                    with patch.object(server.Handler, "log_message"):
                        client.sendall(request.encode("ascii"))
                        response = b""
                        while True:
                            chunk = client.recv(4096)
                            if not chunk:
                                break
                            response += chunk
                    self.assertIn(b" " + expected + b" ", response.split(b"\r\n", 1)[0])
                    self.assertIn(b"Connection: close\r\n", response)
        finally:
            httpd.shutdown()
            httpd.server_close()
            worker.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
