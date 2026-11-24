import hashlib
import io
import json
import os
import sys
import tempfile
import time
import unittest
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("PRELOAD_FULL_SAMPLE", "false")
os.environ.setdefault("REQUIRE_AUTH", "true")
os.environ.setdefault("OSDU_TOKEN", "demo-token")
os.environ.setdefault("OSDU_PARTITION", "company-prod")
os.environ.setdefault("INDEX_DELAY_SECONDS", "0")

import server  # noqa: E402
from fake_search_backend import FakeSearch
from fake_storage_backend import FakeStorage
from fake_file_backend import FakeFiles

server.start_runtime(FakeSearch(), storage=FakeStorage())

METHODS = {"get": "do_GET", "post": "do_POST", "put": "do_PUT", "delete": "do_DELETE",
           "patch": "do_PATCH", "options": "do_OPTIONS"}


class _Headers:
    def __init__(self, values):
        self._d = {str(k).lower(): v for k, v in values.items()}

    def get(self, key, default=None):
        return self._d.get(str(key).lower(), default)


class _FakeWriter:
    def __init__(self):
        self.buf = io.BytesIO()

    def write(self, data):
        self.buf.write(data)


class _FakeHandler:
    def __init__(self, path, headers, body):
        self.path = path
        self.headers = headers
        self.rfile = io.BytesIO((body if isinstance(body, bytes) else body.encode("utf-8")))
        self.wfile = _FakeWriter()
        self.client_address = ("127.0.0.1", 0)
        self._status = None
        self._response_headers = []

    def send_response(self, code):
        self._status = code

    def send_header(self, key, value):
        self._response_headers.append((key, value))

    def end_headers(self):
        pass


def make_handler(path, headers, body):
    handler = object.__new__(server.Handler)
    handler.path = path
    handler.headers = headers
    handler.rfile = io.BytesIO((body if isinstance(body, bytes) else body.encode("utf-8")))
    handler.wfile = _FakeWriter()
    handler.client_address = ("127.0.0.1", 0)
    handler.requestline = "HTTP/1.1 200 OK"
    handler.request_version = "HTTP/1.1"
    handler._status = None
    handler._response_headers = []
    handler.send_response = lambda code: setattr(handler, "_status", code)
    handler.send_response_only = lambda *a, **k: None
    handler.send_header = lambda k, v: handler._response_headers.append((k, v))
    handler.end_headers = lambda: None
    handler.log_request = lambda *a, **k: None
    return handler


def call(method, path, body=None, token="demo-token", partition="company-prod", raw=False, headers=None):
    headers = {
        "Authorization": f"Bearer {token}",
        "data-partition-id": partition,
        "Host": "localhost:TEST",
        **(headers or {}),
    }
    payload = None
    body_text = ""
    if body is not None:
        if raw:
            payload = body if isinstance(body, bytes) else body.encode("utf-8")
        else:
            payload = body if isinstance(body, str) else json.dumps(body)
        headers["Content-Type"] = "application/json"
        headers["Content-Length"] = str(len(payload))
        body_text = payload
    fake = make_handler(path, _Headers(headers), body_text)
    getattr(server.Handler, METHODS[method])(fake)
    data = fake.wfile.buf.getvalue()
    try:
        parsed = json.loads(data.decode("utf-8")) if data else None
    except Exception:
        parsed = None
    return fake._status, parsed


def wait_until(fn, timeout=3.0, interval=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return True
        time.sleep(interval)
    return bool(fn())


def valid_well(rid="company-prod:master-data--Well:WT-1", name="EAGLE-01"):
    return {
        "id": rid,
        "kind": server.WELL_KIND,
        "acl": {"viewers": [server.ACL_VIEWERS], "owners": [server.ACL_OWNERS]},
        "legal": {"legaltags": [server.LEGAL_TAG], "otherRelevantDataCountries": ["SA"]},
        "data": {"FacilityName": name, "FieldName": "EAGLE-A", "Status": "DW"},
    }


def valid_log(rid="company-prod:work-product-component--WellLog:WL-1",
              name="EAGLE-01", start_m=1500.0):
    return {
        "id": rid,
        "kind": server.WELLLOG_KIND,
        "acl": {"viewers": [server.ACL_VIEWERS], "owners": [server.ACL_OWNERS]},
        "legal": {"legaltags": [server.LEGAL_TAG], "otherRelevantDataCountries": ["SA"]},
        "data": {"FacilityName": name, "StartM": start_m, "StopM": start_m + 500},
    }


class ServerTestCase(unittest.TestCase):

    def setUp(self):
        server.clear_state()
        server.FILE_BACKEND = FakeFiles()
        server.FILE_RUNTIME_EPOCH = "legacy-test"
        server.INDEX_DELAY_SECONDS = 0.0
        self._tmp_files = tempfile.TemporaryDirectory()
        server.FILES_DIR = Path(self._tmp_files.name)

    def tearDown(self):
        server.clear_state()
        server.INDEX_DELAY_SECONDS = 0.0
        server.INDEX_DELAY_SECONDS = float(os.environ.get("INDEX_DELAY_SECONDS", "0"))
        self._tmp_files.cleanup()

    def put_records(self, records, **kw):
        return call("put", "/api/storage/v2/records", records, **kw)


class TestHealth(ServerTestCase):

    def test_health_empty(self):
        status, payload = call("get", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "UP")
        self.assertEqual(payload["storedRecords"], 0)
        self.assertEqual(payload["currentDataProfile"], "empty")

    def test_health_public_no_auth(self):
        status, _ = call("get", "/health", token="")
        self.assertEqual(status, 200)


class TestStorageValidation(ServerTestCase):

    def test_put_get_roundtrip(self):
        status, payload = self.put_records([valid_well()])
        self.assertEqual(status, 200)
        self.assertEqual(payload["recordCount"], 1)
        status, rec = call("get", "/api/storage/v2/records/company-prod:master-data--Well:WT-1")
        self.assertEqual(status, 200)
        self.assertEqual(rec["version"], 1)
        self.assertEqual(rec["data"]["FacilityName"], "EAGLE-01")

    def test_put_rejects_without_acl(self):
        rec = valid_well()
        del rec["acl"]
        status, payload = self.put_records([rec])
        self.assertEqual(status, 400)
        self.assertIn("acl", payload["message"])

    def test_put_rejects_without_legal(self):
        rec = valid_well()
        del rec["legal"]
        status, payload = self.put_records([rec])
        self.assertEqual(status, 400)
        self.assertIn("legal.legaltags", payload["message"])

    def test_put_rejects_unknown_kind(self):
        rec = valid_well()
        rec["kind"] = "osdu:wks:master-data--Foo:1.0.0"
        status, payload = self.put_records([rec])
        self.assertEqual(status, 400)
        self.assertIn("Unsupported kind", payload["message"])

    def test_put_rejects_unknown_legaltag(self):
        rec = valid_well()
        rec["legal"]["legaltags"] = ["company-prod-nope"]
        status, payload = self.put_records([rec])
        self.assertEqual(status, 400)
        self.assertIn("unknown LegalTag", payload["message"])

    def test_put_version_increments(self):
        self.assertEqual(self.put_records([valid_well()])[0], 200)
        status, payload = self.put_records([valid_well()])
        self.assertEqual(status, 200)
        self.assertEqual(payload["recordIds"][0]["version"], 2)

    def test_invalid_json_body(self):
        status, payload = call("post", "/api/search/v2/query", body="{ not json", raw=True)
        self.assertEqual(status, 400)
        self.assertIn("Invalid JSON body", payload["message"])


class TestSearch(ServerTestCase):

    def search(self, payload, **kw):
        return call("post", "/api/search/v2/query", payload, **kw)

    def test_search_requires_kind(self):
        self.put_records([valid_well()])
        status, payload = self.search({})
        self.assertEqual(status, 200)
        self.assertEqual(payload["totalCount"], 0)

    def test_search_with_index_delay(self):
        server.INDEX_DELAY_SECONDS = 2.0
        self.put_records([valid_well()])
        _, payload = self.search({"kind": server.WELL_KIND})
        self.assertEqual(payload["totalCount"], 0)
        self.assertTrue(wait_until(
            lambda: self.search({"kind": server.WELL_KIND})[1]["totalCount"] == 1,
            timeout=10.0))

    def test_search_field_query(self):
        self.put_records([valid_well(name="EAGLE-01"), valid_well("company-prod:master-data--Well:WT-2", "OSPREY-02")])
        wait_until(lambda: self.search({"kind": server.WELL_KIND})[1]["totalCount"] == 2)
        _, payload = self.search({"kind": server.WELL_KIND, "query": 'data.FacilityName:"EAGLE-01"'})
        self.assertEqual(payload["totalCount"], 1)
        self.assertEqual(payload["results"][0]["data"]["FacilityName"], "EAGLE-01")
        _, payload = self.search({"kind": server.WELL_KIND, "query": 'data.FacilityName:"NO-SUCH"'})
        self.assertEqual(payload["totalCount"], 0)

    def test_search_and_or_range(self):
        self.put_records([
            valid_log("company-prod:work-product-component--WellLog:L1", "EAGLE-01", 1000.0),
            valid_log("company-prod:work-product-component--WellLog:L2", "OSPREY-02", 3000.0),
        ])
        wait_until(lambda: self.search({"kind": server.WELLLOG_KIND})[1]["totalCount"] == 2)
        _, payload = self.search({
            "kind": server.WELLLOG_KIND,
            "query": 'data.FacilityName:"EAGLE-01" AND data.StartM:[500 TO 2000]',
        })
        self.assertEqual(payload["totalCount"], 1)
        _, payload = self.search({
            "kind": server.WELLLOG_KIND,
            "query": 'data.FacilityName:"EAGLE-01" OR data.FacilityName:"OSPREY-02"',
        })
        self.assertEqual(payload["totalCount"], 2)
        _, payload = self.search({
            "kind": server.WELLLOG_KIND,
            "query": 'data.StartM:[2500 TO *]',
        })
        self.assertEqual(payload["totalCount"], 1)
        _, payload = self.search({
            "kind": server.WELLLOG_KIND,
            "query": 'NOT data.FacilityName:"OSPREY-02"',
        })
        self.assertEqual(payload["totalCount"], 1)

    def test_cursor_paging(self):
        for i in range(3):
            self.put_records([valid_well(f"company-prod:master-data--Well:WT-{i}")])
        wait_until(lambda: self.search({"kind": server.WELL_KIND})[1]["totalCount"] == 3)
        _, first = call("post", "/api/search/v2/query_with_cursor",
                        {"kind": server.WELL_KIND, "limit": 2})
        self.assertEqual(len(first["results"]), 2)
        self.assertEqual(first["totalCount"], 3)
        cursor = first["cursor"]
        _, second = call("post", "/api/search/v2/query_with_cursor", {"cursor": cursor})
        self.assertEqual(len(second["results"]), 1)
        self.assertEqual(second["totalCount"], 3)
        cursor = second["cursor"]
        _, third = call("post", "/api/search/v2/query_with_cursor", {"cursor": cursor})
        self.assertEqual(len(third["results"]), 0)

    def test_bad_cursor(self):
        status, payload = call("post", "/api/search/v2/query_with_cursor", {"cursor": "stale"})
        self.assertEqual(status, 400)
        self.assertIn("cursor", payload["message"].lower())


class TestDataset(ServerTestCase):

    def test_dataset_lifecycle_with_checksums(self):
        status, payload = call("post", "/api/dataset/v1/storageInstructions", {})
        self.assertEqual(status, 200)
        url = payload["storageLocation"]["signedUrl"]
        token = url.rsplit("/", 1)[1]
        body = b"LAS-001 sample curve data"
        status, upload = call("put", f"/upload/{token}", body=body, raw=True,
                              headers={"x-file-name": "up-1.las"})
        self.assertEqual(status, 200)
        self.assertEqual(upload["size"], len(body))
        self.assertEqual(upload["sha256"], hashlib.sha256(body).hexdigest())

        did = "company-prod:dataset--File.Generic:UP-1-LAS"
        ds = valid_well(did)
        ds["kind"] = server.DATASET_KIND
        ds["data"] = {"Name": "up-1.las", "FileName": "up-1.las", "Format": "LAS",
                      "FileSource": payload["storageLocation"]["FileSource"]}
        self.assertEqual(call("put", "/api/dataset/v1/registerDataset",
                              {"datasetRegistries": [ds]})[0], 200)

        status, payload = call("post", "/api/dataset/v1/retrievalInstructions",
                               {"datasetRegistryIds": [did]})
        self.assertEqual(status, 200)
        props = payload["datasets"][0]["retrievalProperties"]
        self.assertEqual(props["size"], len(body))
        self.assertEqual(props["sha256"], hashlib.sha256(body).hexdigest())

    def test_upload_requires_valid_token(self):
        status, _ = call("put", "/upload/bad-token", body=b"data", raw=True, token="")
        self.assertEqual(status, 401)

    def test_storage_instructions_expiry_reported(self):
        server.UPLOAD_TOKEN_TTL = 120
        _, payload = call("post", "/api/dataset/v1/storageInstructions", {})
        self.assertEqual(payload["storageLocation"]["expiresInSeconds"], 120)
        server.UPLOAD_TOKEN_TTL = 3600

    def test_upload_token_ttl_sweep(self):
        server.UPLOAD_TOKEN_TTL = 10
        with server.STORE_LOCK:
            server.UPLOAD_TOKENS["stale-token"] = {"created": time.time() - 1000, "kindSubType": "x"}
        _, payload = call("post", "/api/dataset/v1/storageInstructions", {})
        with server.STORE_LOCK:
            self.assertNotIn("stale-token", server.UPLOAD_TOKENS)
            digest = hashlib.sha256(payload["storageLocation"]["signedUrl"].rsplit("/", 1)[1].encode()).hexdigest()
            self.assertIn(digest, server.UPLOAD_GRANTS)
            self.assertFalse(server.UPLOAD_TOKENS)
        server.UPLOAD_TOKEN_TTL = 3600

    def test_full_sample_retrieval_has_checksums(self):
        call("post", "/poc/load-full-sample")
        with server.STORE_LOCK:
            did = next(rid for rid, r in server.RECORD_BY_ID.items() if r["kind"] == server.DATASET_KIND)
        _, payload = call("post", "/api/dataset/v1/retrievalInstructions", {"datasetRegistryIds": [did]})
        props = payload["datasets"][0]["retrievalProperties"]
        self.assertIn("size", props)
        self.assertIn("sha256", props)
        self.assertEqual(len(props["sha256"]), 64)


class TestWorkflow(ServerTestCase):

    def manifest(self, items, acl=None, legal=None):
        return {
            "executionContext": {
                "acl": acl or {"viewers": [server.ACL_VIEWERS], "owners": [server.ACL_OWNERS]},
                "legal": legal or {"legaltags": [server.LEGAL_TAG], "otherRelevantDataCountries": ["SA"]},
                "manifest": {"MasterData": items, "Data": {}},
            }
        }

    def test_submit_and_poll_success(self):
        server.INDEX_DELAY_SECONDS = 0.3
        status, payload = call("post", "/api/workflow/v1/workflow/Osdu_ingest/workflowRun",
                               self.manifest([valid_well()]))
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "submitted")
        run_id = payload["workflowRunId"]
        status, run = call("get", f"/api/workflow/v1/workflow/{run_id}")
        self.assertEqual(status, 200)
        self.assertIn(run["status"], {"submitted", "running", "succeeded"})
        status, _ = call("get", "/api/storage/v2/records/company-prod:master-data--Well:WT-1")
        self.assertEqual(status, 200)
        time.sleep(0.6)
        _, run = call("get", f"/api/workflow/v1/workflow/{run_id}")
        self.assertEqual(run["status"], "succeeded")

    def test_submit_rejects_invalid_manifest(self):
        rec = valid_well()
        del rec["acl"]
        body = {"executionContext": {"manifest": {"MasterData": [rec], "Data": {}}}}
        status, payload = call("post", "/api/workflow/v1/workflow/Osdu_ingest/workflowRun", body)
        self.assertEqual(status, 400)
        self.assertIn("acl", payload["message"])

    def test_unknown_run(self):
        status, _ = call("get", "/api/workflow/v1/workflow/wf-nope")
        self.assertEqual(status, 404)


class TestLegal(ServerTestCase):

    def test_create_and_use_tag(self):
        status, payload = call("post", "/api/legal/v1/legaltags", {"name": "company-prod-extra-tag"})
        self.assertEqual(status, 201)
        self.assertEqual(payload["name"], "company-prod-extra-tag")
        self.assertTrue(payload["isValid"])
        status, _ = call("get", "/api/legal/v1/legaltags/company-prod-extra-tag")
        self.assertEqual(status, 200)
        rec = valid_well()
        rec["legal"]["legaltags"] = ["company-prod-extra-tag"]
        self.assertEqual(self.put_records([rec])[0], 200)
        _, payload = call("get", "/api/legal/v1/legaltags")
        names = [t["name"] for t in payload["legalTags"]]
        self.assertIn("company-prod-extra-tag", names)
        self.assertIn(server.LEGAL_TAG, names)

    def test_unknown_tag_404(self):
        status, _ = call("get", "/api/legal/v1/legaltags/company-prod-nope")
        self.assertEqual(status, 404)


class TestRbac(ServerTestCase):

    def test_write_requires_editor_group(self):
        saved = server.GROUPS_BY_TOKEN[server.TOKEN]
        server.GROUPS_BY_TOKEN[server.TOKEN] = {"users.datalake.viewers"}
        try:
            status, payload = self.put_records([valid_well()])
            self.assertEqual(status, 403)
            self.assertIn("Insufficient entitlements", payload["message"])
        finally:
            server.GROUPS_BY_TOKEN[server.TOKEN] = saved

    def test_search_accepts_datalake_editor_role(self):
        saved = server.GROUPS_BY_TOKEN[server.TOKEN]
        server.GROUPS_BY_TOKEN[server.TOKEN] = {"users.datalake.editors"}
        try:
            status, _ = call("post", "/api/search/v2/query", {"kind": server.WELL_KIND})
            self.assertEqual(status, 200)  # datalake editors are an explicit Search role.
        finally:
            server.GROUPS_BY_TOKEN[server.TOKEN] = saved

    def test_full_token_passes_all(self):
        self.assertEqual(self.put_records([valid_well()])[0], 200)
        status, _ = call("post", "/api/search/v2/query", {"kind": server.WELL_KIND})
        self.assertEqual(status, 200)


class TestPoc(ServerTestCase):

    def test_load_and_reset(self):
        status, payload = call("post", "/poc/load-full-sample")
        self.assertEqual(status, 200)
        self.assertEqual(payload["storedRecords"], len(server.FULL_SAMPLE))
        self.assertEqual(payload["currentDataProfile"], "full-sample")
        status, payload = call("post", "/poc/reset")
        self.assertEqual(status, 200)
        self.assertEqual(payload["storedRecords"], 0)
        self.assertEqual(payload["currentDataProfile"], "empty")
        self.assertEqual(payload["recordsByKind"]["wells"], 0)


class TestStorageDeleteAndPaging(ServerTestCase):

    def test_batch_soft_delete(self):
        self.put_records([valid_well(), valid_well("company-prod:master-data--Well:WT-2", "OSPREY-02")])
        self.put_records([valid_log("company-prod:work-product-component--WellLog:L1")])
        status, payload = call("delete", "/api/storage/v2/records",
                               {"records": ["company-prod:master-data--Well:WT-1"]})
        self.assertEqual(status, 200)
        self.assertEqual(payload["recordCount"], 1)
        self.assertEqual(payload["skippedRecordIds"], [])
        status, _ = call("get", "/api/storage/v2/records/company-prod:master-data--Well:WT-1")
        self.assertEqual(status, 404)
        status, payload = call("get", "/api/storage/v2/records?kind=" + server.WELL_KIND)
        self.assertEqual(status, 200)
        self.assertEqual([r["id"] for r in payload["records"]], ["company-prod:master-data--Well:WT-2"])
        status, payload = call("get", "/api/storage/v2/records?kind=" + server.WELL_KIND + "&deleted=true")
        self.assertEqual(status, 200)
        self.assertEqual(len(payload["records"]), 1)
        self.assertEqual(payload["records"][0]["id"], "company-prod:master-data--Well:WT-1")

    def test_soft_deleted_record_leaves_search(self):
        self.put_records([valid_well()])
        wait_until(lambda: call("post", "/api/search/v2/query",
                                {"kind": server.WELL_KIND})[1]["totalCount"] == 1)
        call("delete", "/api/storage/v2/records", {"records": ["company-prod:master-data--Well:WT-1"]})
        time.sleep(0.1)
        _, payload = call("post", "/api/search/v2/query", {"kind": server.WELL_KIND})
        self.assertEqual(payload["totalCount"], 0)

    def test_reupsert_revives_deleted(self):
        self.put_records([valid_well()])
        call("delete", "/api/storage/v2/records", {"records": ["company-prod:master-data--Well:WT-1"]})
        status, _ = call("get", "/api/storage/v2/records/company-prod:master-data--Well:WT-1")
        self.assertEqual(status, 404)
        self.put_records([valid_well()])
        status, rec = call("get", "/api/storage/v2/records/company-prod:master-data--Well:WT-1")
        self.assertEqual(status, 200)
        # Deletion consumes a generation, not a visible data version.
        self.assertEqual(rec["version"], 2)

    def test_single_delete_and_hard_delete(self):
        self.put_records([valid_well()])
        status, _ = call("delete", "/api/storage/v2/records/company-prod:master-data--Well:WT-1")
        self.assertEqual(status, 204)
        self.put_records([valid_well()])
        status, _ = call("delete", "/api/storage/v2/records/company-prod:master-data--Well:WT-1?hardDelete=true")
        self.assertEqual(status, 204)
        with server.STORE_LOCK:
            self.assertNotIn("company-prod:master-data--Well:WT-1", server.RECORD_BY_ID)
        status, _ = call("get", "/api/storage/v2/records/company-prod:master-data--Well:WT-1")
        self.assertEqual(status, 404)
        status, _ = call("delete", "/api/storage/v2/records/company-prod:master-data--Well:WT-1")
        self.assertEqual(status, 404)

    def test_list_cursor_paging(self):
        for i in range(3):
            self.put_records([valid_well(f"company-prod:master-data--Well:WT-{i}")])
        _, first = call("get", "/api/storage/v2/records?limit=2")
        self.assertEqual(len(first["records"]), 2)
        self.assertIsNotNone(first["cursor"])
        _, second = call("get",
                         "/api/storage/v2/records?cursor=" + first["cursor"])
        self.assertEqual(len(second["records"]), 1)
        self.assertIsNone(second["cursor"])

    def test_bad_list_cursor(self):
        status, _ = call("get", "/api/storage/v2/records?cursor=stale")
        self.assertEqual(status, 400)

    def test_sort_order_desc(self):
        self.put_records([valid_well("company-prod:master-data--Well:WT-1"),
                          valid_well("company-prod:master-data--Well:WT-5", "Z"),
                          valid_well("company-prod:master-data--Well:WT-3", "M")])
        _, payload = call("get", "/api/storage/v2/records?sortOrder=desc")
        ids = [r["id"] for r in payload["records"]]
        self.assertEqual(ids, sorted(ids, reverse=True))

    def test_modify_after_date(self):
        self.put_records([valid_well()])
        with server.STORE_LOCK:
            server.RECORD_TS["company-prod:master-data--Well:WT-1"]["modify"] = 1_700_000_000
        _, payload = call("get", "/api/storage/v2/records?modifyAfterDate=2023-11-14T22:13:20Z")
        self.assertEqual(len(payload["records"]), 1)
        _, payload = call("get", "/api/storage/v2/records?modifyAfterDate=2023-11-16T22:13:20Z")
        self.assertEqual(len(payload["records"]), 0)


class TestLegalExtended(ServerTestCase):

    def expired_tag(self):
        return {"name": "company-prod-expired",
                "properties": {"expirationDate": "2000-01-01"}}

    def test_create_expired_is_invalid_and_listable(self):
        status, payload = call("post", "/api/legal/v1/legaltags", self.expired_tag())
        self.assertEqual(status, 201)
        self.assertFalse(payload["isValid"])
        _, payload = call("get", "/api/legal/v1/legaltags")
        names = [t["name"] for t in payload["legalTags"]]
        self.assertNotIn("company-prod-expired", names)
        _, payload = call("get", "/api/legal/v1/legaltags?valid=false")
        names = [t["name"] for t in payload["legalTags"]]
        self.assertIn("company-prod-expired", names)

    def test_put_update_restores_validity(self):
        call("post", "/api/legal/v1/legaltags", self.expired_tag())
        status, payload = call("put", "/api/legal/v1/legaltags",
                               {"name": "company-prod-expired",
                                "properties": {"expirationDate": "2099-01-01"}})
        self.assertEqual(status, 200)
        self.assertTrue(payload["isValid"])
        rec = valid_well()
        rec["legal"]["legaltags"] = ["company-prod-expired"]
        self.assertEqual(self.put_records([rec])[0], 200)

    def test_ingest_rejects_expired_tag(self):
        call("post", "/api/legal/v1/legaltags", self.expired_tag())
        rec = valid_well()
        rec["legal"]["legaltags"] = ["company-prod-expired"]
        status, payload = self.put_records([rec])
        self.assertEqual(status, 400)
        self.assertIn("not valid", payload["message"])

    def test_delete_tag(self):
        call("post", "/api/legal/v1/legaltags", {"name": "company-prod-temp"})
        status, _ = call("delete", "/api/legal/v1/legaltags/company-prod-temp")
        self.assertEqual(status, 204)
        status, _ = call("get", "/api/legal/v1/legaltags/company-prod-temp")
        self.assertEqual(status, 404)

    def test_validate_endpoint(self):
        _, payload = call("post", "/api/legal/v1/legaltags:validate",
                          {"legaltags": [server.LEGAL_TAG, "company-prod-missing"]})
        self.assertEqual(payload["invalidLegalTags"], [{"name": "company-prod-missing",
                                                        "reason": "LegalTag not found"}])

    def test_batch_retrieve(self):
        _, payload = call("post", "/api/legal/v1/legaltags:batchRetrieve",
                          {"name": [server.LEGAL_TAG]})
        self.assertEqual(len(payload["legalTags"]), 1)
        self.assertEqual(payload["legalTags"][0]["name"], server.LEGAL_TAG)

    def test_properties_endpoint(self):
        _, payload = call("get", "/api/legal/v1/legaltags:properties")
        names = {p["propertyName"] for p in payload["legaltagProperties"]}
        self.assertIn("countryOfOrigin", names)
        self.assertIn("expirationDate", names)

    def test_duplicate_create_409(self):
        status, _ = call("post", "/api/legal/v1/legaltags", {"name": server.LEGAL_TAG})
        self.assertEqual(status, 409)


class TestSearchEnhanced(ServerTestCase):

    def test_track_total_count_false_omits(self):
        self.put_records([valid_well()])
        def search_no_total():
            _, p = call("post", "/api/search/v2/query",
                        {"kind": server.WELL_KIND, "trackTotalCount": False})
            return p
        wait_until(lambda: len(search_no_total()["results"]) == 1)
        payload = search_no_total()
        self.assertNotIn("totalCount", payload)
        self.assertEqual(len(payload["results"]), 1)

    def test_query_as_owner_restricts(self):
        rec = valid_well()
        rec["acl"]["owners"] = ["data.other.owners@company-prod.company.com"]
        call("post", "/api/entitlements/v2/groups", {"name": "data.other.owners"})
        # Trusted fixture: demo is a viewer, not an owner, so HTTP creation must be denied.
        self.assertEqual(self.put_records([rec])[0], 403)
        server.authorized_upsert([rec], index_immediately=True, principal=server.core_auth.SYSTEM)
        self.assertEqual(call("post", "/api/search/v2/query", {"kind": server.WELL_KIND})[1]["totalCount"], 1)
        _, payload = call("post", "/api/search/v2/query",
                          {"kind": server.WELL_KIND, "query_as_owner": True})
        self.assertEqual(payload["totalCount"], 0)

    def test_aggregate_count_and_terms(self):
        for i in range(3):
            self.put_records([valid_well(f"company-prod:master-data--Well:WT-{i}")])
        wait_until(lambda: call("post", "/api/search/v2/query",
                                {"kind": server.WELL_KIND})[1]["totalCount"] == 3)
        _, payload = call("post", "/api/search/v2/query", {
            "kind": server.WELL_KIND,
            "aggregateBy": [
                {"aggregateByType": "count", "field": "kind"},
                {"aggregateByType": "filteredTerms", "field": "kind", "bucketSize": 10},
            ],
        })
        self.assertEqual(payload["aggregates"][0]["Count"]["count"], 3)
        terms = payload["aggregates"][1]["FilteredTerms"]["terms"]
        self.assertEqual(terms[0]["key"], server.WELL_KIND)
        self.assertEqual(terms[0]["count"], 3)

    def test_aggregate_histogram(self):
        self.put_records([valid_log("company-prod:work-product-component--WellLog:L1", "A", 1000.0),
                          valid_log("company-prod:work-product-component--WellLog:L2", "B", 3000.0)])
        wait_until(lambda: call("post", "/api/search/v2/query",
                                {"kind": server.WELLLOG_KIND})[1]["totalCount"] == 2)
        _, payload = call("post", "/api/search/v2/query", {
            "kind": server.WELLLOG_KIND,
            "aggregateBy": [{"aggregateByType": "histogram", "field": "data.StartM", "interval": 1000}],
        })
        buckets = payload["aggregates"][0]["Histogram"]["buckets"]
        self.assertEqual(len(buckets), 2)
        self.assertEqual(buckets[0]["start"], 1000)
        self.assertEqual(buckets[1]["count"], 1)


class TestFileService(ServerTestCase):

    def _register_file(self, body):
        status, payload = call("put", f"/upload/{body[0]}", body=body[1], raw=True,
                               headers={"x-file-name": "f-1.las"})
        self.assertEqual(status, 200)
        return payload

    def test_post_get_download(self):
        status, instructions = call("post", "/api/dataset/v1/storageInstructions", {})
        token = instructions["storageLocation"]["signedUrl"].rsplit("/", 1)[1]
        content = b"f-1 curve data"
        upload = self._register_file((token, content))
        rec = valid_well("company-prod:dataset--File.Generic:F-1-LAS")
        rec["kind"] = server.DATASET_KIND
        rec["data"] = {"Name": "f-1.las", "FileName": "f-1.las", "Format": "LAS",
                       "FileSource": instructions["storageLocation"]["FileSource"]}
        status, payload = call("post", "/api/file/v2/files", rec)
        self.assertEqual(status, 200)
        self.assertEqual(payload["fileID"], "company-prod:dataset--File.Generic:F-1-LAS")
        self.assertEqual(payload["fileSourceDatasWithLocation"][0]["fileSource"], "f-1.las")

        status, payload = call("get", "/api/file/v2/files/company-prod:dataset--File.Generic:F-1-LAS")
        self.assertEqual(status, 200)
        loc = payload["fileSourceDatasWithLocation"][0]["location"]
        self.assertIn("?sp=r", loc["signedUrl"])
        self.assertEqual(loc["sha256"], hashlib.sha256(content).hexdigest())

        status, payload = call("get", "/api/file/v2/files/company-prod:dataset--File.Generic:F-1-LAS/downloadURL")
        self.assertEqual(status, 200)
        self.assertEqual(payload["size"], len(content))
        self.assertEqual(payload["sha256"], hashlib.sha256(content).hexdigest())

    def test_get_full_sample_file(self):
        call("post", "/poc/load-full-sample")
        with server.STORE_LOCK:
            did = next(rid for rid, r in server.RECORD_BY_ID.items() if r["kind"] == server.DATASET_KIND)
        status, payload = call("get", f"/api/file/v2/files/{did}/downloadURL")
        self.assertEqual(status, 200)
        self.assertEqual(len(payload["sha256"]), 64)

    def test_post_requires_dataset_record(self):
        status, _ = call("post", "/api/file/v2/files", {"name": "not-a-record"})
        self.assertEqual(status, 400)

    def test_get_non_dataset_404(self):
        self.put_records([valid_well()])
        status, _ = call("get", "/api/file/v2/files/company-prod:master-data--Well:WT-1")
        self.assertEqual(status, 404)


class TestEntitlements(ServerTestCase):

    GROUP = "data.demo.viewers"

    def test_v2_groups_lists_seeded(self):
        status, payload = call("get", "/api/entitlements/v2/groups")
        self.assertEqual(status, 200)
        names = {g["name"] for g in payload["groups"]}
        self.assertIn("service.storage.viewer", names)
        self.assertIn("users.datalake.viewers", names)

    def test_create_group_and_members(self):
        email = f"{self.GROUP}@company-prod.company.com"
        status, payload = call("post", "/api/entitlements/v2/groups",
                               {"name": self.GROUP, "description": "demo viewers"})
        self.assertEqual(status, 201)
        self.assertEqual(payload["email"], email)
        status, _ = call("post", "/api/entitlements/v2/groups", {"name": self.GROUP})
        self.assertEqual(status, 409)

        status, _ = call("post", f"/api/entitlements/v2/groups/{email}/members",
                         {"email": "u1@acme.com", "role": "OWNER"})
        self.assertEqual(status, 200)
        call("post", f"/api/entitlements/v2/groups/{email}/members",
             {"email": "u2@acme.com", "role": "MEMBER"})

        status, payload = call("get", f"/api/entitlements/v2/groups/{email}/members")
        self.assertEqual(status, 200)
        self.assertEqual(len(payload["members"]), 2)
        _, payload = call("get", f"/api/entitlements/v2/groups/{email}/members?role=OWNER")
        self.assertEqual([m["email"] for m in payload["members"]], ["u1@acme.com"])
        _, payload = call("get", f"/api/entitlements/v2/groups/{email}/members?includeType=true")
        self.assertIn("type", payload["members"][0])

        status, payload = call("get", "/api/entitlements/v2/members/u1@acme.com/groups")
        self.assertEqual(status, 200)
        self.assertEqual([g["email"] for g in payload["groups"]], [email])

        status, _ = call("delete", f"/api/entitlements/v2/groups/{email}/members/u1@acme.com")
        self.assertEqual(status, 204)
        status, _ = call("delete", f"/api/entitlements/v2/groups/{email}/members/u1@acme.com")
        self.assertEqual(status, 404)

    def test_patch_and_delete_group(self):
        email = f"{self.GROUP}@company-prod.company.com"
        call("post", "/api/entitlements/v2/groups", {"name": self.GROUP})
        status, _ = call("patch", f"/api/entitlements/v2/groups/{email}",
                         {"description": "updated"})
        self.assertEqual(status, 204)
        status, _ = call("delete", f"/api/entitlements/v2/groups/{email}")
        self.assertEqual(status, 204)
        status, _ = call("get", f"/api/entitlements/v2/groups/{email}/members")
        self.assertEqual(status, 404)

    def test_group_rbac(self):
        saved = server.GROUPS_BY_TOKEN[server.TOKEN]
        server.GROUPS_BY_TOKEN[server.TOKEN] = {"users.datalake.viewers"}
        try:
            status, _ = call("post", "/api/entitlements/v2/groups", {"name": self.GROUP})
            self.assertEqual(status, 403)
        finally:
            server.GROUPS_BY_TOKEN[server.TOKEN] = saved


class TestStorageVersionsPatchCopy(ServerTestCase):

    RID = "company-prod:master-data--Well:WT-V1"

    def test_versions_patch_copy_query(self):
        self.put_records([valid_well(self.RID)])
        self.put_records([valid_well(self.RID)])

        status, payload = call("get", f"/api/storage/v2/records/versions/{self.RID}")
        self.assertEqual(status, 200)
        self.assertEqual(payload["recordId"], self.RID)
        self.assertEqual(payload["versions"], [1, 2])

        _, rec = call("get", f"/api/storage/v2/records/{self.RID}?version=1")
        self.assertEqual(rec["version"], 1)
        _, rec = call("get", f"/api/storage/v2/records/{self.RID}/2")
        self.assertEqual(rec["version"], 2)
        status, _ = call("get", f"/api/storage/v2/records/{self.RID}/99")
        self.assertEqual(status, 404)

        status, payload = call("patch", f"/api/storage/v2/records/{self.RID}",
                               {"data": {"Status": "PX"}, "tags": {"stage": "demo"}})
        self.assertEqual(status, 200)
        self.assertEqual(payload["recordIds"], [self.RID])
        _, rec = call("get", f"/api/storage/v2/records/{self.RID}")
        self.assertEqual(rec["version"], 3)
        self.assertEqual(rec["data"]["Status"], "PX")

        status, payload = call("patch", "/api/storage/v2/records",
                               [{"id": self.RID, "data": {"Status": "PD"}}])
        self.assertEqual(status, 200)
        self.assertEqual(payload["recordCount"], 1)
        _, rec = call("get", f"/api/storage/v2/records/{self.RID}")
        self.assertEqual(rec["data"]["Status"], "PD")

        status, payload = call("post", f"/api/records/{self.RID}:copy", {})
        self.assertEqual(status, 200)
        self.assertEqual(payload["recordCount"], 1)
        new_id = payload["newRecordId"]
        self.assertNotEqual(new_id, self.RID)
        _, rec = call("get", f"/api/storage/v2/records/{new_id}")
        self.assertEqual(rec["data"]["FacilityName"], "EAGLE-01")
        self.assertEqual(rec["version"], 1)

        _, payload = call("get", f"/api/storage/v2/query/records?kind={server.WELL_KIND}")
        self.assertIn(self.RID, payload["records"])
        self.assertIn(new_id, payload["records"])

        _, rec = call("get", f"/api/storage/v2/records/{self.RID}?attribute=data.FacilityName")
        self.assertIn("FacilityName", rec["data"])
        self.assertNotIn("Status", rec["data"])

    def test_batch_and_single_delete_and_purge(self):
        rec_a = valid_well("company-prod:master-data--Well:WT-A")
        rec_b = valid_well("company-prod:master-data--Well:WT-B")
        rec_c = valid_well("company-prod:master-data--Well:WT-C")
        self.put_records([rec_a, rec_b, rec_c])

        status, _ = call("post", "/api/storage/v2/records/delete",
                         {"records": [rec_a["id"], rec_b["id"]]})
        self.assertEqual(status, 204)
        _, payload = call("get", "/api/storage/v2/records?deleted=true")
        self.assertEqual({r["id"] for r in payload["records"]}, {rec_a["id"], rec_b["id"]})
        _, payload = call("get", "/api/storage/v2/records")
        self.assertEqual([r["id"] for r in payload["records"]], [rec_c["id"]])

        status, _ = call("post", f"/api/records/{rec_c['id']}:delete", {})
        self.assertEqual(status, 204)
        _, payload = call("get", "/api/storage/v2/records?deleted=true")
        self.assertEqual({r["id"] for r in payload["records"]},
                         {rec_a["id"], rec_b["id"], rec_c["id"]})

        status, _ = call("delete", f"/api/storage/v2/records/versions/{rec_a['id']}")
        self.assertEqual(status, 204)
        _, payload = call("get", "/api/storage/v2/records?deleted=true")
        self.assertNotIn(rec_a["id"], {r["id"] for r in payload["records"]})
        status, _ = call("get", f"/api/storage/v2/records/versions/{rec_a['id']}")
        self.assertEqual(status, 404)


class TestSearchCursorAndHealth(ServerTestCase):

    def test_cursor_delete_and_liveness(self):
        for i in range(3):
            self.put_records([valid_well(f"company-prod:master-data--Well:WT-{i}")])
        wait_until(lambda: call("post", "/api/search/v2/query",
                                {"kind": server.WELL_KIND})[1]["totalCount"] == 3)

        _, payload = call("post", "/api/search/v2/query_with_cursor",
                          {"kind": server.WELL_KIND, "limit": 1})
        self.assertEqual(len(payload["results"]), 1)
        cursor = payload["cursor"]
        self.assertTrue(cursor)

        status, _ = call("delete", f"/api/search/v2/query_with_cursor/{cursor}")
        self.assertEqual(status, 204)
        status, _ = call("post", "/api/search/v2/query_with_cursor", {"cursor": cursor})
        self.assertEqual(status, 400)

        status, payload = call("get", "/api/search/v2/liveness_check")
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "UP")
        status, payload = call("get", "/api/search/v2/readiness_check")
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "UP")


class TestLegalQuery(ServerTestCase):

    def seed_tags(self):
        tags = {
            "company-prod-q1": {
                "description": "North Sea well operations",
                "properties": {"contractId": "C-100", "originator": "acme",
                               "expirationDate": "2027-06-15"},
            },
            "company-prod-q2": {
                "description": "south field development",
                "properties": {"contractId": "C-200", "originator": "acme",
                               "expirationDate": "2028-06-15"},
            },
            "company-prod-q3": {
                "description": "old archive",
                "properties": {"contractId": "C-900", "originator": "other",
                               "expirationDate": "2000-01-01"},
            },
        }
        for name, tag in tags.items():
            call("post", "/api/legal/v1/legaltags", {"name": name, **tag})

    def names(self, payload):
        return [t["name"] for t in payload["legalTags"]]

    def test_query_operators(self):
        self.seed_tags()
        _, payload = call("post", "/api/legal/v1/legaltags:query",
                          {"queryList": ["any=acme"], "operatorList": ["union"]})
        self.assertEqual(self.names(payload), ["company-prod-q1", "company-prod-q2"])

        _, payload = call("post", "/api/legal/v1/legaltags:query",
                          {"queryList": ["any=acme", "any=north"],
                           "operatorList": ["intersection"]})
        self.assertEqual(self.names(payload), ["company-prod-q1"])

        _, payload = call("post", "/api/legal/v1/legaltags:query",
                          {"queryList": ["any=acme", "any=acme"], "operatorList": ["add"]})
        self.assertEqual(len(payload["legalTags"]), 4)

    def test_query_attr_between_and_valid(self):
        self.seed_tags()
        _, payload = call("post", "/api/legal/v1/legaltags:query",
                          {"queryList": ["contractId=C-200"], "operatorList": ["union"]})
        self.assertEqual(self.names(payload), ["company-prod-q2"])

        _, payload = call("post", "/api/legal/v1/legaltags:query",
                          {"queryList": ["expirationDate between (2022-01-01, 2029-12-31)"],
                           "operatorList": ["union"]})
        self.assertEqual(self.names(payload), ["company-prod-q1", "company-prod-q2"])

        _, payload = call("post", "/api/legal/v1/legaltags:query",
                          {"queryList": ["any=acme"], "operatorList": ["union"],
                           "valid": False})
        self.assertEqual(self.names(payload), [])

    def test_query_valid_false_catches_expired(self):
        call("post", "/api/legal/v1/legaltags",
             {"name": "company-prod-q3", "properties": {"expirationDate": "2000-01-01"}})
        _, payload = call("post", "/api/legal/v1/legaltags:query",
                          {"queryList": ["any=q3"], "operatorList": ["union"]})
        self.assertEqual(self.names(payload), [])
        _, payload = call("post", "/api/legal/v1/legaltags:query",
                          {"queryList": ["any=q3"], "operatorList": ["union"],
                           "valid": False})
        self.assertEqual(self.names(payload), ["company-prod-q3"])


class TestDatasetBatchEndpoints(ServerTestCase):

    DID = "company-prod:dataset--File.Generic:UP-2-LAS"

    def upload(self):
        _, instructions = call("post", "/api/dataset/v1/storageInstructions", {})
        self.source = instructions["storageLocation"]["FileSource"]
        token = instructions["storageLocation"]["signedUrl"].rsplit("/", 1)[1]
        body = b"D1 batch curve data"
        status, _ = call("put", f"/upload/{token}", body=body, raw=True,
                         headers={"x-file-name": "f-2.las"})
        self.assertEqual(status, 200)

    def register(self):
        ds = valid_well(self.DID)
        ds["kind"] = server.DATASET_KIND
        ds["data"] = {"Name": "f-2.las", "FileName": "f-2.las", "Format": "LAS",
                      "FileSource": self.source}
        status, payload = call("put", "/api/dataset/v1/registerDataset",
                               {"datasetRegistries": [ds]})
        self.assertEqual(status, 200)
        self.assertEqual(payload["datasetRegistries"][0]["id"], self.DID)

    def test_batch_register_and_get(self):
        self.upload()
        self.register()
        status, payload = call("post", "/api/dataset/v1/getDatasetRegistry",
                               {"datasetRegistryIds": [self.DID]})
        self.assertEqual(status, 200)
        self.assertEqual(len(payload["datasetRegistries"]), 1)
        self.assertEqual(payload["datasetRegistries"][0]["id"], self.DID)

        status, payload = call("get", f"/api/dataset/v1/getDatasetRegistry/?id={self.DID}")
        self.assertEqual(status, 200)
        self.assertEqual(payload["datasetRegistries"][0]["id"], self.DID)

        status, payload = call("get", f"/api/dataset/v1/retrievalInstructions?id={self.DID}")
        self.assertEqual(status, 200)
        self.assertEqual(payload["datasets"][0]["datasetRegistryId"], self.DID)

    def test_revoke_and_soft_delete(self):
        self.upload()
        self.register()
        status, _ = call("post", "/api/dataset/v1/revokeURL",
                         {"datasetRegistryIds": [self.DID]})
        self.assertEqual(status, 200)
        _, payload = call("get", f"/api/dataset/v1/retrievalInstructions?id={self.DID}")
        self.assertEqual(payload["datasets"], [])

        status, _ = call("post", f"/api/dataset/v1/metadataRecord/{self.DID}/softDelete", {})
        self.assertEqual(status, 204)
        status, _ = call("get", f"/api/dataset/v1/getDatasetRegistry/?id={self.DID}")
        self.assertEqual(status, 404)


class TestFileV2Extended(ServerTestCase):

    DID = "company-prod:dataset--File.Generic:F-M1"

    def upload(self):
        _, instructions = call("post", "/api/dataset/v1/storageInstructions", {})
        self.source = instructions["storageLocation"]["FileSource"]
        token = instructions["storageLocation"]["signedUrl"].rsplit("/", 1)[1]
        return call("put", f"/upload/{token}", body=b"F2 metadata contents", raw=True,
                    headers={"x-file-name": "f-3.las"})

    def test_upload_url_metadata_delivery(self):
        self.assertEqual(self.upload()[0], 200)
        status, payload = call("get", "/api/file/v2/files/uploadURL")
        self.assertEqual(status, 200)
        self.assertIn("FileSource", payload["Location"])
        self.assertIn("SignedUrl", payload["Location"])

        status, payload = call("post", "/api/file/v2/files/metadata", {
            "id": self.DID,
            "kindId": server.DATASET_KIND,
            "FileName": "f-3.las",
            "data": {"FileSource": self.source},
        })
        self.assertEqual(status, 201)
        self.assertEqual(payload["id"], self.DID)

        status, payload = call("get", f"/api/file/v2/files/{self.DID}/metadata")
        self.assertEqual(status, 200)
        self.assertEqual(payload["id"], self.DID)

        status, payload = call("get", f"/api/file/v2/files/{self.DID}/DownloadURL")
        self.assertEqual(status, 200)
        self.assertIn("signedURL", payload)
        self.assertIn("?sp=r", payload["signedURL"])

        status, payload = call("post", "/api/file/v2/getLocation", {"FileID": self.DID})
        self.assertEqual(status, 200)
        self.assertEqual(payload["fileID"], self.DID)
        self.assertIn("SignedURL", payload["Location"])

        status, payload = call("post", "/api/file/v2/delivery/getFileSignedUrl",
                               {"srn": [self.DID]})
        self.assertEqual(status, 200)
        self.assertIn(self.DID, payload["processed"])
        self.assertEqual(payload["unprocessed"], [])

        _, payload = call("post", "/api/file/v2/delivery/getFileSignedUrl",
                          {"srn": ["missing-file-1", self.DID]})
        self.assertIn("missing-file-1", payload["unprocessed"])
        self.assertIn(self.DID, payload["processed"])

        status, _ = call("delete", f"/api/file/v2/files/{self.DID}/metadata")
        self.assertEqual(status, 204)
        status, _ = call("get", f"/api/file/v2/files/{self.DID}/DownloadURL")
        self.assertEqual(status, 404)

    def test_metadata_requires_file(self):
        status, _ = call("post", "/api/file/v2/files/metadata",
                         {"id": self.DID, "data": {"FileSource": "never-uploaded.bin"}})
        self.assertEqual(status, 400)


class TestWorkflowDeployment(ServerTestCase):

    def test_deploy_list_run_update_delete(self):
        status, payload = call("post", "/api/workflow/v1/workflow",
                               {"workflowName": "Demo_workflow", "description": "demo run"})
        self.assertEqual(status, 200)
        self.assertEqual(payload["workflowName"], "Demo_workflow")

        _, payload = call("get", "/api/workflow/v1/workflow")
        names = [w["workflowName"] for w in payload]
        self.assertIn("Osdu_ingest", names)
        self.assertIn("Demo_workflow", names)

        status, _ = call("post", "/api/workflow/v1/workflow",
                         {"workflowName": "Demo_workflow", "description": "v2"})
        self.assertEqual(status, 200)
        _, payload = call("get", "/api/workflow/v1/workflow/Demo_workflow")
        self.assertEqual(payload["description"], "v2")

        status, payload = call("post", "/api/workflow/v1/workflow/Demo_workflow/workflowRun", {})
        self.assertEqual(status, 501)
        self.assertIn("unsupported_workflow", payload["message"])

        _, payload = call("get", "/api/workflow/v1/workflow/Demo_workflow/workflowRun")
        self.assertEqual(payload, [])
        status, _ = call("delete", "/api/workflow/v1/workflow/Demo_workflow")
        self.assertEqual(status, 204)
        status, _ = call("get", "/api/workflow/v1/workflow/Demo_workflow")
        self.assertEqual(status, 404)

    def test_unknown_workflow_run_404(self):
        status, _ = call("post", "/api/workflow/v1/workflow/Nope/workflowRun", {})
        self.assertEqual(status, 404)


class TestSchemaService(ServerTestCase):

    CUSTOM = "osdu:wks:master-data--DemoCustom:1.0.0"

    def test_registry_lifecycle_storage_and_builtin_protection(self):
        body = {"kind": self.CUSTOM, "schema": {"type": "object",
                "properties": {"Value": {"type": "number"}}}}
        for method, expected in (("post", 201), ("post", 200), ("put", 200)):
            status, response = call(method, "/api/schema-service/v1/schema", body)
            self.assertEqual(status, expected)
            self.assertEqual(response["schemaInfo"]["schemaIdentity"]["schemaVersionPatch"], 0)
        rec = valid_well()
        rec.update(kind=self.CUSTOM, data={"Value": 1})
        self.assertEqual(call("post", "/api/storage/v2/records", [rec])[0], 200)
        self.assertTrue(wait_until(lambda: bool(server.search_records({"kind": self.CUSTOM}))))
        bad = {**rec, "id": "bad", "data": {"Value": "wrong"}}
        self.assertEqual(call("post", "/api/storage/v2/records", [bad])[0], 400)
        self.assertNotIn("bad", server.RECORD_BY_ID)
        self.assertEqual(call("put", "/api/schema-service/v1/schema",
                              {**body, "status": "DEPRECATED"})[0], 200)
        self.assertEqual(call("post", "/api/storage/v2/records", [rec])[0], 409)
        self.assertEqual(call("get", f"/api/schema-service/v1/schema/{self.CUSTOM}")[0], 200)
        _, listing = call("get", "/api/schema-service/v1/schema")
        self.assertEqual(sum(s["kind"] == self.CUSTOM for s in listing), 1)
        _, builtin = call("get", f"/api/schema-service/v1/schema/{server.WELL_KIND}")
        self.assertEqual(call("post", "/api/schema-service/v1/schema", builtin)[0], 200)
        builtin["schema"]["properties"]["FacilityName"] = {"type": "integer"}
        self.assertEqual(call("put", "/api/schema-service/v1/schema", builtin)[0], 409)

    def test_registry_rejects_bad_identity_and_preserves_service_roles(self):
        for payload in ({"schemaInfo": {"schemaIdentity": {"entityType": "Only"}},
                         "schema": {"type": "object"}},
                        {"kind": self.CUSTOM, "schema": {"type": "object"},
                         "schemaInfo": {"schemaIdentity": {"authority": "wrong"}}}):
            self.assertEqual(call("post", "/api/schema-service/v1/schema", payload)[0], 400)
        saved = server.GROUPS_BY_TOKEN[server.TOKEN]
        server.GROUPS_BY_TOKEN[server.TOKEN] = {"service.storage.viewer"}
        try:
            self.assertEqual(call("post", "/api/schema-service/v1/schema",
                                  {"kind": self.CUSTOM, "schema": {"type": "object"}})[0], 403)
        finally:
            server.GROUPS_BY_TOKEN[server.TOKEN] = saved

    def test_all_csv_samples_and_manifest_records_validate(self):
        root = Path(__file__).resolve().parents[2]
        for data_dir in (root / "server/data", root / "examples/well360/reference"):
            original = server.DATA_DIR
            try:
                server.DATA_DIR = data_dir
                records = server.build_full_sample_records()
            finally:
                server.DATA_DIR = original
            self.assertEqual(len(records), 15)
            self.assertEqual(call("post", "/api/storage/v2/records", records)[0], 200)
        for path in (root / "examples").rglob("manifest_example.json"):
            # Render the reference guide's explicitly administrator-supplied LegalTag.
            payload = json.loads(path.read_text().replace("<approved-legal-tag-name>", server.LEGAL_TAG))
            records = server.manifest_records(payload)
            self.assertEqual(len(records), 3)
            for rec in records:
                server.validate_record(rec)
            self.assertEqual(call("post", "/api/storage/v2/records", records)[0], 200)
        paths = list((root / "examples").rglob("sample_*_record.json"))
        self.assertEqual(len(paths), 7)
        for path in paths:
            rec = json.loads(path.read_text().replace("<approved-legal-tag-name>", server.LEGAL_TAG))
            status, response = call("post", "/api/storage/v2/records", rec if isinstance(rec, list) else [rec])
            self.assertEqual(status, 200, (str(path), response))

    def test_registry_capacity_returns_retry_after_without_mutation(self):
        for i in range(28):
            server.SCHEMA_REGISTRY.register({"kind": f"test:local:K{i}:1.0.0",
                                            "schema": {"type": "object"}}, "POST")
        body = json.dumps({"kind": self.CUSTOM, "schema": {"type": "object"}})
        handler = make_handler("/api/schema-service/v1/schema", _Headers({
            "Authorization": f"Bearer {server.TOKEN}", "data-partition-id": server.PARTITION,
            "Content-Length": str(len(body)), "Content-Type": "application/json"}), body)
        server.Handler.do_POST(handler)
        self.assertEqual(handler._status, 429)
        self.assertIn(("Retry-After", "1"), handler._response_headers)
        self.assertIsNone(server.schema_info_for(self.CUSTOM))

    def test_envelope_and_known_fields_reject_before_individual_mutation(self):
        for key, value in (("id", []), ("kind", {}), ("data", []), ("acl", []),
                           ("data", {"FacilityName": 123}), ("legal", {"legaltags": [123]})):
            rec = valid_well()
            rec[key] = value
            self.assertEqual(call("post", "/api/storage/v2/records", [rec])[0], 400)
            self.assertEqual(server.RECORD_BY_ID, {})
            self.assertEqual(server.RECORD_VERSIONS, {})
        rec = valid_well()
        del rec["legal"]["otherRelevantDataCountries"]
        self.assertEqual(call("post", "/api/storage/v2/records", [rec])[0], 400)

    def test_list_and_execute(self):
        status, payload = call("get", "/api/schema-service/v1/schema")
        self.assertEqual(status, 200)
        self.assertTrue(any(s.get("kind") == server.WELL_KIND for s in payload))
        self.assertTrue(any(s.get("kind") == server.DATASET_KIND for s in payload))

        status, _ = call("get", f"/api/schema-service/v1/schema/{server.WELL_KIND}")
        self.assertEqual(status, 200)

        body = {
            "kind": self.CUSTOM,
            "schemaInfo": {
                "schemaIdentity": {
                    "authority": "osdu", "source": "wks",
                    "entityType": "master-data--DemoCustom",
                    "schemaVersionMajor": 1, "schemaVersionMinor": 0,
                }
            },
            "schema": {"type": "object", "properties": {}},
        }
        status, payload = call("post", "/api/schema-service/v1/schema", body)
        self.assertEqual(status, 201)
        _, payload = call("get", f"/api/schema-service/v1/schema/{self.CUSTOM}")
        self.assertEqual(payload["kind"], self.CUSTOM)

        status, payload = call("put", "/api/schema-service/v1/schema", body)
        self.assertEqual(status, 200)
        self.assertEqual(payload["kind"], self.CUSTOM)

        status, _ = call("put", "/api/schema-service/v1/schemas/system",
                         {"schema": {"title": "system", "type": "object"}})
        self.assertEqual(status, 200)
        _, listing = call("get", "/api/schema-service/v1/schema")
        self.assertEqual({s.get("kind") for s in listing}, {*server.SUPPORTED_KINDS, self.CUSTOM})

        status, _ = call("get", f"/api/schema-service/v1/schema/{self.CUSTOM}?x=1")
        self.assertEqual(status, 200)


class TestEntitlementsExtended(ServerTestCase):

    GROUP = "data.demo.extended"
    EMAIL = f"{GROUP}@company-prod.company.com"

    def setUp(self):
        super().setUp()
        call("post", "/api/entitlements/v2/groups", {"name": self.GROUP})
        call("post", f"/api/entitlements/v2/groups/{self.EMAIL}/members",
             {"email": "u1@acme.com", "role": "OWNER"})
        call("post", f"/api/entitlements/v2/groups/{self.EMAIL}/members",
             {"email": "u2@acme.com", "role": "MEMBER"})

    def test_members_count_with_role_filter(self):
        _, payload = call("get", f"/api/entitlements/v2/groups/{self.EMAIL}/membersCount")
        self.assertEqual(payload["count"], 2)
        _, payload = call("get", f"/api/entitlements/v2/groups/{self.EMAIL}/membersCount?role=owner")
        self.assertEqual(payload["count"], 1)

    def test_v1_role_required(self):
        _, payload = call("get", "/api/entitlements/v1/groups?roleRequired=true")
        for group in payload["groups"]:
            self.assertIn("role", group)
            self.assertIn("email", group)

    def test_app_ids_patch_and_create(self):
        status, payload = call("patch", f"/api/entitlements/v2/groups/{self.EMAIL}",
                               {"op": "replace", "path": "/appIds", "value": ["app-a", "app-b"]})
        self.assertEqual(status, 200)
        self.assertEqual(payload["appIds"], ["app-a", "app-b"])

        status, _ = call("post", "/api/entitlements/v2/groups",
                         {"name": "data.demo.apps", "appIds": ["app-x"]})
        self.assertEqual(status, 201)


class TestLegacyDurableFiles(ServerTestCase):
    def test_adapter_rechecks_expiry_after_checksum_read(self):
        from unittest.mock import patch
        _, issued = call("get", "/file/v2/files/uploadURL")
        token = issued["Location"]["SignedUrl"].rsplit("/", 1)[1]
        digest = hashlib.sha256(token.encode()).hexdigest()
        grant = deepcopy(server.UPLOAD_GRANTS[digest])
        read = server.FILE_BACKEND.read
        def expire_after_read(key):
            yield from read(key)
            clock.return_value = grant["expiresAt"] + 1
        with server.STORE_LOCK, \
                patch.object(server.time, "time", return_value=grant["expiresAt"] - 1) as clock, \
                patch.object(server.FILE_BACKEND, "read", side_effect=expire_after_read):
            self.assertEqual(call("put", "/upload/" + token, b"abc", raw=True)[0], 404)
            self.assertEqual(server.UPLOAD_GRANTS[digest], grant)

    def test_adapter_hash_only_bound_bytes_retry(self):
        from unittest.mock import patch
        _, issued = call("get", "/file/v2/files/uploadURL")
        token = issued["Location"]["SignedUrl"].rsplit("/", 1)[1]
        digest = hashlib.sha256(token.encode()).hexdigest()
        self.assertIn(digest, server.UPLOAD_GRANTS)
        self.assertNotIn(token, json.dumps(server.storage_state()))
        self.assertNotIn(token, server.UPLOAD_GRANTS[digest]["stagingKey"])
        self.assertFalse(server.UPLOAD_TOKENS)
        data = b"x" * (1024 * 1024 + 7)
        status, upload = call("put", "/upload/" + token, data, raw=True,
                              headers={"x-file-name": "private.bin"})
        self.assertEqual(status, 200)
        record = valid_well("company-prod:dataset--File.Generic:legacy-bound")
        record.update(kind=server.DATASET_KIND, data={
            "FileName": "private.bin", "FileSource": issued["Location"]["FileSource"]})
        path = "/file/v2/files/metadata"
        self.assertEqual(call("post", path, record)[0], 201)
        self.assertEqual(call("post", path, record)[0], 201)
        self.assertEqual(server.RECORD_BY_ID[record["id"]]["version"], 1)
        _, result = call("get", "/file/v2/files/" + record["id"] + "/DownloadURL")
        self.assertIn("?sp=r", result["signedURL"])
        self.assertEqual(upload["sha256"], server.FILE_BINDINGS[record["id"]]["sha256"])
        self.assertFalse(list(server.FILES_DIR.iterdir()))
        self.assertEqual(call("get", "/files/private.bin", token="")[0], 401)
        handler = make_handler("/files/private.bin", _Headers({
            "Authorization": "Bearer demo-token", "data-partition-id": server.PARTITION}), "")
        server.Handler.do_GET(handler)
        self.assertEqual(handler._status, 200)
        self.assertEqual(handler.wfile.buf.getvalue(), data)
        self.assertLessEqual(max(server.FILE_BACKEND.chunks_uploaded), 1024 * 1024)
        binding = deepcopy(server.FILE_BINDINGS[record["id"]])
        profile = server.COMPAT_PROFILE
        try:
            with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "core"}), patch.object(
                    server, "PRELOAD_FULL_SAMPLE", False), patch.object(
                    server, "immutable_sample", return_value=False):
                server.start_runtime(FakeSearch(), storage=server.STORAGE_BACKEND,
                                     files=server.FILE_BACKEND)
                self.assertTrue(server.RUNTIME_ACCEPTING)
                self.assertEqual(server.FILE_BINDINGS[record["id"]], binding)
        finally:
            with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": profile}):
                server.start_runtime(FakeSearch(), storage=server.STORAGE_BACKEND,
                                     files=server.FILE_BACKEND)

    def test_legacy_metadata_only_requires_migration_for_byte_reads(self):
        record = valid_well("company-prod:dataset--File.Generic:unbound")
        record.update(kind=server.DATASET_KIND, data={"FileName": "user.bin"})
        self.assertEqual(self.put_records([record])[0], 200)
        self.assertEqual(call("post", "/file/v2/files/metadata", record)[0], 400)
        self.assertNotIn(record["id"], server.FILE_BINDINGS)
        (server.FILES_DIR / "user.bin").write_bytes(b"not imported")
        self.assertEqual(call("get", "/file/v2/files/" + record["id"] + "/downloadURL")[0], 503)


class TestSampleFileStartup(unittest.TestCase):
    def test_already_bound_samples_are_verified_before_restart_readiness(self):
        from unittest.mock import patch
        storage, files = FakeStorage(), FakeFiles()
        with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "core"}), patch.object(
                server, "PRELOAD_FULL_SAMPLE", True):
            server.start_runtime(FakeSearch(), storage=storage, files=files)
            before = deepcopy(storage.state)
            with patch.object(server, "immutable_sample", return_value=False):
                with self.assertRaises(server.BackendError):
                    server.start_runtime(FakeSearch(), storage=storage, files=files)
            self.assertFalse(server.RUNTIME_ACCEPTING)
            self.assertEqual(storage.state, before)

    def test_import_commit_failure_and_modified_sample_do_not_publish_readiness(self):
        from unittest.mock import patch
        storage, files = FakeStorage(), FakeFiles()
        with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "legacy"}):
            server.start_runtime(FakeSearch(), storage=storage)
            server.upsert_records(server.FULL_SAMPLE)
        before = deepcopy(storage.state)
        with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "core"}), patch.object(
                storage, "save", side_effect=server.BackendError(503, "commit_failed")):
            with self.assertRaisesRegex(Exception, "commit_failed"):
                server.start_runtime(FakeSearch(), storage=storage, files=files)
        self.assertFalse(server.RUNTIME_ACCEPTING)
        self.assertEqual(storage.state, before)
        self.assertFalse(storage.state["fileBindings"])
        with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "core"}), patch.dict(
                server.SAMPLE_HASHES, {"eagle01_gr_cali.las": "0" * 64}):
            with self.assertRaisesRegex(Exception, "unmigrated"):
                server.start_runtime(FakeSearch(), storage=storage, files=files)
        self.assertFalse(server.RUNTIME_ACCEPTING)
        self.assertEqual(storage.state, before)

    def test_default_samples_atomic_restart_and_fail_closed(self):
        from unittest.mock import patch
        storage, files = FakeStorage(), FakeFiles()
        with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "core"}), patch.object(
                server, "PRELOAD_FULL_SAMPLE", True):
            server.start_runtime(FakeSearch(), storage=storage, files=files)
            self.assertTrue(server.RUNTIME_ACCEPTING)
            bindings = deepcopy(server.FILE_BINDINGS)
            self.assertEqual(len(bindings), 4)
            for rid, binding in bindings.items():
                self.assertEqual(binding["sha256"], server.SAMPLE_HASHES[
                    server.record_file_name(server.RECORD_BY_ID[rid])])
            revision = server.REVISION
            server.start_runtime(FakeSearch(), storage=storage, files=files)
            self.assertEqual(server.FILE_BINDINGS, bindings)
            self.assertEqual(server.REVISION, revision)
            bad = valid_well("company-prod:dataset--File.Generic:user")
            bad.update(kind=server.DATASET_KIND, data={"FileName": "user.bin"})
            server.upsert_records([bad])
            with self.assertRaisesRegex(Exception, "unmigrated"):
                server.start_runtime(FakeSearch(), storage=storage, files=files)
            self.assertFalse(server.RUNTIME_ACCEPTING)

    def tearDown(self):
        server.start_runtime(FakeSearch(), storage=FakeStorage(), files=FakeFiles())


class TestCoreFilePublication(unittest.TestCase):
    def setUp(self):
        from fake_file_backend import FakeFiles
        from unittest.mock import patch
        self.patch = patch
        self.tmp = tempfile.TemporaryDirectory()
        self.old_files = server.FILES_DIR
        server.FILES_DIR = Path(self.tmp.name)
        self.env = patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "core"})
        self.env.start()
        self.storage, self.files = FakeStorage(), FakeFiles()
        server.start_runtime(FakeSearch(), storage=self.storage, files=self.files)

    def tearDown(self):
        server.stop_runtime()
        self.env.stop()
        server.start_runtime(FakeSearch(), storage=FakeStorage())
        server.FILES_DIR = self.old_files
        self.tmp.cleanup()

    def grant(self):
        status, body = call("get", "/api/file/v2/files/uploadURL?expiryTime=1M")
        self.assertEqual(status, 200, body)
        return body

    def test_legacy_unbound_dataset_metadata_restore_does_not_publish_bytes(self):
        record = valid_well(f"{server.PARTITION}:dataset--File.Generic:legacy-metadata-only")
        record.update(kind=server.DATASET_KIND, data={
            "FileName": "pending-migration.bin", "FileSource": "pending-migration.bin"})
        self.assertEqual(call("put", "/api/storage/v2/records", [record])[0], 400)
        self.assertNotIn(record["id"], server.RECORD_BY_ID)
        server.COMPAT_PROFILE = "legacy"
        self.assertEqual(call("post", "/api/storage/v2/records", [record])[0], 200)
        route = "/api/dataset/v1/metadataRecord/" + record["id"]
        self.assertEqual(call("post", route + "/softDelete")[0], 204)
        self.assertEqual(call("post", route + "/undelete")[0], 200)
        self.assertNotIn(record["id"], server.FILE_BINDINGS)
        self.assertEqual(self.files.blobs, {})
        self.assertEqual(call("get", "/api/file/v2/files/" + record["id"] + "/DownloadURL")[0], 503)
        self.assertEqual(call("get", "/files/pending-migration.bin", token="")[0], 401)

    def test_legacy_dataset_metadata_only_registration_preserves_core_publication_boundary(self):
        record = valid_well(f"{server.PARTITION}:dataset--File.Generic:metadata-only-registration")
        record.update(kind=server.DATASET_KIND, data={"Name": "metadata-only"})
        body = {"datasetRegistries": [record]}
        self.assertEqual(call("put", "/api/dataset/v1/registerDataset", body)[0], 400)
        server.COMPAT_PROFILE = "legacy"
        status, result = call("put", "/api/dataset/v1/registerDataset", body)
        self.assertEqual(status, 200)
        self.assertEqual(result["datasetRegistries"][0]["id"], record["id"])
        self.assertNotIn(record["id"], server.FILE_BINDINGS)
        self.assertEqual(self.files.blobs, {})
        before = deepcopy(server.storage_state())
        for malformed in ([], [None], [{}], [record, None]):
            with self.subTest(malformed=malformed):
                self.assertEqual(call("put", "/api/dataset/v1/registerDataset", malformed)[0], 400)
                self.assertEqual(server.storage_state(), before)
                self.assertEqual(self.files.blobs, {})

    def record(self, grant, suffix="one"):
        record = valid_well(f"{server.PARTITION}:dataset--File.Generic:{suffix}")
        record.update(kind=server.DATASET_KIND, data={
            "FileName": "same.bin", "FileSource": grant["Location"]["FileSource"]})
        return record

    def test_grant_publication_immutable_bytes_retry_and_restart(self):
        grant = self.grant()
        source = grant["Location"]["FileSource"]
        self.files.upload(source, io.BytesIO(b"published bytes"), 15)
        record = self.record(grant)
        route = "/api/file/v2/files/metadata"
        self.assertEqual(call("post", route, record), (201, {"id": record["id"]}))
        before = json.loads(json.dumps(server.storage_state()))
        self.assertEqual(call("post", route, record), (201, {"id": record["id"]}))
        self.assertEqual(server.storage_state(), before)
        altered = json.loads(json.dumps(record))
        altered["data"]["FileName"] = "changed"
        self.assertEqual(call("post", route, altered)[0], 409)
        binding = dict(server.FILE_BINDINGS[record["id"]])
        self.files.upload(source, io.BytesIO(b"overwrite"), 9)
        self.assertEqual(b"".join(self.files.read(binding["finalKey"])), b"published bytes")
        server.start_runtime(FakeSearch(), storage=self.storage, files=self.files)
        status, body = call("get", f'/api/file/v2/files/{record["id"]}/downloadURL')
        self.assertEqual(status, 200, body)
        self.assertIn("SignedUrl", body)
        self.assertEqual(server.FILE_BINDINGS[record["id"]], binding)
        self.assertEqual(call("get", "/files/same.bin", token="")[0], 401)
        self.assertFalse((server.FILES_DIR / "same.bin").exists())
        self.assertNotIn(grant["Location"]["SignedUrl"], json.dumps(self.storage.state))

    def test_dataset_roundtrip_and_same_name_isolation(self):
        records = []
        for suffix, payload in (("one", b"first"), ("two", b"second")):
            status, instructions = call("post",
                "/api/dataset/v1/storageInstructions?kindSubType=dataset--File.Generic", {})
            self.assertEqual(status, 200, instructions)
            self.assertEqual(instructions["providerKey"], "AZURE")
            source = instructions["storageLocation"]["fileSource"]
            self.files.upload(source, io.BytesIO(payload), len(payload))
            records.append(self.record({"Location": {"FileSource": source}}, suffix))
        status, result = call("put", "/api/dataset/v1/registerDataset",
                              {"datasetRegistries": records})
        self.assertEqual(status, 201, result)
        self.assertEqual(len(result["datasetRegistries"]), 2)
        keys = [server.FILE_BINDINGS[r["id"]]["finalKey"] for r in records]
        self.assertNotEqual(*keys)
        status, result = call("post", "/api/dataset/v1/retrievalInstructions",
                              {"datasetRegistryIds": [r["id"] for r in records]})
        self.assertEqual(status, 200, result)
        self.assertEqual(len(result["datasets"]), 2)
        self.assertEqual(call("post", "/api/dataset/v1/revokeURL", {})[0], 501)

    def test_missing_unsafe_source_expiry_and_failed_commit(self):
        for expiry in ("0M", "2H", "1D", "nonsense"):
            self.assertEqual(call("get", "/api/file/v2/files/uploadURL?expiryTime=" + expiry)[0], 400)
        grant = self.grant()
        record = self.record(grant)
        self.assertEqual(call("post", "/api/file/v2/files/metadata", record)[0], 400)
        source = grant["Location"]["FileSource"]
        self.files.upload(source, io.BytesIO(b"abc"), 3)
        before = json.loads(json.dumps(server.storage_state()))
        with self.patch.object(self.storage, "save", side_effect=server.BackendError(503, "offline")):
            self.assertEqual(call("post", "/api/file/v2/files/metadata", record)[0], 503)
        self.assertEqual(server.storage_state(), before)
        self.assertNotIn(record["id"], server.FILE_BINDINGS)
        self.assertNotIn(record["id"], server.RECORD_BY_ID)
        record["data"]["FileSource"] = "https://foreign/account/blob"
        self.assertEqual(call("post", "/api/file/v2/files/metadata", record)[0], 400)
        self.assertEqual(call("post", "/api/dataset/v1/storageInstructions?kindSubType=other", {})[0], 501)

    def test_concurrent_retry_creates_one_version_and_enforces_two_slots(self):
        import threading
        grant = self.grant()
        self.files.upload(grant["Location"]["FileSource"], io.BytesIO(b"abc"), 3)
        record = self.record(grant)
        barrier = threading.Barrier(2)
        publish = self.files.publish
        def delayed(*args):
            barrier.wait(timeout=5)
            return publish(*args)
        results = []
        with self.patch.object(self.files, "publish", side_effect=delayed):
            workers = [threading.Thread(target=lambda: results.append(
                call("post", "/api/file/v2/files/metadata", record))) for _ in range(2)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(10)
                self.assertFalse(worker.is_alive())
        self.assertEqual([status for status, _ in results], [201, 201])
        self.assertEqual(len(server.RECORD_VERSIONS[record["id"]]), 1)
        self.assertTrue(server.FILE_FINALIZATIONS.acquire(blocking=False))
        self.assertTrue(server.FILE_FINALIZATIONS.acquire(blocking=False))
        try:
            self.assertEqual(call("post", "/api/file/v2/files/metadata", record)[0], 429)
        finally:
            server.FILE_FINALIZATIONS.release()
            server.FILE_FINALIZATIONS.release()

    def test_expired_grant_reclamation_and_authority_recheck_after_stream(self):
        grant = self.grant()
        source = grant["Location"]["FileSource"]
        digest = hashlib.sha256(source.encode()).hexdigest()
        with server.STORE_LOCK, server.storage_transaction():
            server.UPLOAD_GRANTS[digest]["expiresAt"] = 0
        self.grant()
        self.assertNotIn(digest, server.UPLOAD_GRANTS)
        grant = self.grant()
        source = grant["Location"]["FileSource"]
        self.files.upload(source, io.BytesIO(b"abc"), 3)
        publish = self.files.publish
        def revoke(*args):
            result = publish(*args)
            with server.STORE_LOCK, server.storage_transaction():
                server.GROUPS[server.ACL_OWNERS]["members"].clear()
            return result
        record = self.record(grant)
        with self.patch.object(self.files, "publish", side_effect=revoke):
            self.assertEqual(call("post", "/api/file/v2/files/metadata", record)[0], 403)
        self.assertNotIn(record["id"], server.FILE_BINDINGS)
        self.assertNotIn(record["id"], server.RECORD_BY_ID)


@unittest.skipUnless(os.getenv("AZURITE_TEST_CONNECTION_STRING"), "Real Azurite not configured")
class TestRealLegacySampleHTTP(unittest.TestCase):
    def test_real_legacy_adapter_default_samples_and_restart(self):
        import threading
        import urllib.request
        import urllib.error
        import uuid
        from urllib.parse import urlparse
        from http.server import ThreadingHTTPServer
        from unittest.mock import patch
        from file_backend import AzureFiles
        from storage_backend import AzureBlobStorage
        connection = os.environ["AZURITE_TEST_CONNECTION_STRING"]
        public = os.environ["AZURE_STORAGE_PUBLIC_BLOB_ENDPOINT"]
        storage = AzureBlobStorage(connection, server.PARTITION,
                                   container="legacysamples-" + uuid.uuid4().hex)
        files = AzureFiles(connection, public)
        httpd = None
        with tempfile.TemporaryDirectory() as directory, patch.object(server, "FILES_DIR", Path(directory)), \
                patch.object(server, "PRELOAD_FULL_SAMPLE", True):
            try:
                (server.FILES_DIR / "arbitrary.bin").write_bytes(b"never import")
                with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "core"}):
                    server.start_runtime(FakeSearch(), storage=storage, files=files)
                samples = deepcopy(server.FILE_BINDINGS)
                self.assertEqual(len(samples), 4)
                httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
                worker = threading.Thread(target=httpd.serve_forever, daemon=True)
                worker.start()
                base = f"http://127.0.0.1:{httpd.server_port}"
                def api(method, path, body=None, token=server.TOKEN, raw=False):
                    data = body if raw else json.dumps(body).encode() if body is not None else None
                    request = urllib.request.Request(base + path, data=data, method=method, headers={
                        "Authorization": "Bearer " + token, "data-partition-id": server.PARTITION,
                        "Content-Type": "application/json", "Host": "forged.invalid",
                        "x-file-name": "legacy-private.bin"})
                    try:
                        with urllib.request.urlopen(request, timeout=30) as response:
                            content = response.read()
                            return response.status, content if raw else json.loads(content) if content else None
                    except urllib.error.HTTPError as error:
                        return error.code, error.read()
                for rid, binding in samples.items():
                    _, response = api("GET", "/api/file/v2/files/" + rid + "/downloadURL")
                    with urllib.request.urlopen(response["SignedUrl"], timeout=30) as stream:
                        self.assertEqual(hashlib.sha256(stream.read()).hexdigest(), binding["sha256"])
                revision = server.REVISION
                with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "legacy"}):
                    server.start_runtime(FakeSearch(), storage=storage, files=AzureFiles(connection, public))
                    self.assertEqual(server.FILE_BINDINGS, samples)
                    self.assertEqual(server.REVISION, revision)
                    _, issued = api("GET", "/file/v2/files/uploadURL")
                    upload_path = urlparse(issued["Location"]["SignedUrl"]).path
                    token = upload_path.rsplit("/", 1)[1]
                    self.assertNotIn(token, json.dumps(storage.load()))
                    self.assertFalse(server.UPLOAD_TOKENS)
                    server.start_runtime(FakeSearch(), storage=storage, files=AzureFiles(connection, public))
                    content = b"adapter-real:" + b"0123456789" * 150000
                    self.assertEqual(api("PUT", upload_path, content, raw=True)[0], 200)
                    record = valid_well(f"{server.PARTITION}:dataset--File.Generic:{uuid.uuid4().hex}")
                    record.update(kind=server.DATASET_KIND, data={
                        "FileName": "legacy-private.bin", "FileSource": issued["Location"]["FileSource"]})
                    self.assertEqual(api("POST", "/file/v2/files/metadata", record)[0], 201)
                    self.assertEqual(api("POST", "/file/v2/files/metadata", record)[0], 201)
                    _, download = api("GET", "/file/v2/files/" + record["id"] + "/DownloadURL")
                    self.assertTrue(download["signedURL"].startswith(public + "/"))
                    with urllib.request.urlopen(download["signedURL"], timeout=30) as stream:
                        self.assertEqual(stream.read(), content)
                    self.assertEqual(api("GET", "/files/legacy-private.bin", raw=True), (200, content))
                    self.assertEqual(api("GET", "/files/legacy-private.bin", token="", raw=True)[0], 401)
                    self.assertEqual([p.name for p in server.FILES_DIR.iterdir()], ["arbitrary.bin"])
                    server.start_runtime(FakeSearch(), storage=storage, files=AzureFiles(connection, public))
                    self.assertEqual(server.RECORD_BY_ID[record["id"]]["version"], 1)
                    self.assertEqual(api("GET", "/files/legacy-private.bin", raw=True), (200, content))
                with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "core"}):
                    server.start_runtime(FakeSearch(), storage=storage, files=AzureFiles(connection, public))
                    self.assertEqual({rid: server.FILE_BINDINGS[rid] for rid in samples}, samples)
                    bad = valid_well(f"{server.PARTITION}:dataset--File.Generic:unmigrated")
                    bad.update(kind=server.DATASET_KIND, data={"FileName": "arbitrary.bin"})
                    server.upsert_records([bad])
                    with self.assertRaisesRegex(Exception, "unmigrated"):
                        server.start_runtime(FakeSearch(), storage=storage, files=AzureFiles(connection, public))
                    self.assertFalse(server.RUNTIME_ACCEPTING)
            finally:
                if httpd is not None:
                    httpd.shutdown()
                    httpd.server_close()
                    worker.join(5)
                server.stop_runtime()
                storage.container.delete_container()
                with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "legacy"}):
                    server.start_runtime(FakeSearch(), storage=FakeStorage(), files=FakeFiles())


@unittest.skipUnless(os.getenv("AZURITE_TEST_CONNECTION_STRING"), "Real Azurite not configured")
class TestRealCoreFilesHTTP(unittest.TestCase):
    def test_api_sas_bytes_policy_and_runtime_restart(self):
        import threading
        import urllib.request
        import urllib.error
        import uuid
        from http.server import ThreadingHTTPServer
        from unittest.mock import patch
        from file_backend import AzureFiles
        from storage_backend import AzureBlobStorage
        connection = os.environ["AZURITE_TEST_CONNECTION_STRING"]
        public = os.environ["AZURE_STORAGE_PUBLIC_BLOB_ENDPOINT"]
        storage = AzureBlobStorage(connection, server.PARTITION, container="corefiles-" + uuid.uuid4().hex)
        files = AzureFiles(connection, public)
        httpd = None
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "core"}), \
                patch.object(server, "FILES_DIR", Path(directory)):
            try:
                server.start_runtime(FakeSearch(), storage=storage, files=files)
                httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
                worker = threading.Thread(target=httpd.serve_forever, daemon=True)
                worker.start()
                base = f"http://127.0.0.1:{httpd.server_port}"
                def api(method, path, body=None, token=server.TOKEN):
                    raw = json.dumps(body).encode() if body is not None else None
                    request = urllib.request.Request(base + path, data=raw, method=method, headers={
                        "Authorization": "Bearer " + token, "data-partition-id": server.PARTITION,
                        "Content-Type": "application/json", "Host": "forged.invalid"})
                    try:
                        with urllib.request.urlopen(request, timeout=20) as response:
                            content = response.read()
                            return response.status, json.loads(content) if content else None
                    except urllib.error.HTTPError as error:
                        return error.code, json.loads(error.read())
                status, grant = api("GET", "/api/file/v2/files/uploadURL?expiryTime=1M")
                self.assertEqual(status, 200, grant)
                self.assertTrue(grant["Location"]["SignedUrl"].startswith(public + "/"))
                payload = b"real-file-bytes:" + b"0123456789" * 150000
                write = urllib.request.Request(grant["Location"]["SignedUrl"], data=payload, method="PUT",
                    headers={"x-ms-blob-type": "BlockBlob", "x-ms-version": "2023-11-03"})
                with urllib.request.urlopen(write, timeout=30) as response:
                    self.assertEqual(response.status, 201)
                record = valid_well(f"{server.PARTITION}:dataset--File.Generic:{uuid.uuid4().hex}")
                record.update(kind=server.DATASET_KIND, data={
                    "FileName": "private.bin", "FileSource": grant["Location"]["FileSource"]})
                self.assertEqual(api("POST", "/api/file/v2/files/metadata", record), (201, {"id": record["id"]}))
                revision = server.REVISION
                self.assertEqual(api("POST", "/api/file/v2/files/metadata", record)[0], 201)
                self.assertEqual(server.REVISION, revision)
                path = "/api/file/v2/files/" + record["id"] + "/downloadURL"
                status, download = api("GET", path)
                self.assertEqual(status, 200, download)
                signed = download["SignedUrl"]
                with urllib.request.urlopen(signed, timeout=30) as response:
                    self.assertEqual(hashlib.sha256(response.read()).hexdigest(), hashlib.sha256(payload).hexdigest())
                ranged = urllib.request.Request(signed, headers={"Range": "bytes=3-12"})
                with urllib.request.urlopen(ranged, timeout=20) as response:
                    self.assertEqual(response.status, 206)
                    self.assertEqual(response.read(), payload[3:13])
                with self.assertRaises(urllib.error.HTTPError) as denied:
                    urllib.request.urlopen(signed.split("?")[0], timeout=20)
                self.assertIn(denied.exception.code, (403, 404))
                overwrite = urllib.request.Request(grant["Location"]["SignedUrl"], data=b"overwritten", method="PUT",
                    headers={"x-ms-blob-type": "BlockBlob", "x-ms-version": "2023-11-03"})
                urllib.request.urlopen(overwrite, timeout=20).close()
                with urllib.request.urlopen(signed, timeout=30) as response:
                    self.assertEqual(response.read(), payload)
                server.start_runtime(FakeSearch(), storage=storage, files=AzureFiles(connection, public))
                self.assertEqual(api("GET", path)[0], 200)
                self.assertEqual(api("GET", "/files/private.bin", token="")[0], 401)
                self.assertFalse((server.FILES_DIR / "private.bin").exists())
                with server.STORE_LOCK, server.storage_transaction():
                    server.LEGAL_TAGS[server.LEGAL_TAG]["properties"]["expirationDate"] = "2000-01-01"
                self.assertEqual(api("GET", path)[0], 404)
                self.assertEqual(api("GET", "/api/dataset/v1/retrievalInstructions?id=" + record["id"])[1],
                                 {"datasets": []})
            finally:
                if httpd is not None:
                    httpd.shutdown()
                    httpd.server_close()
                    worker.join(5)
                server.stop_runtime()
                storage.container.delete_container()
                with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "legacy"}):
                    server.start_runtime(FakeSearch(), storage=FakeStorage())


if __name__ == "__main__":
    unittest.main()