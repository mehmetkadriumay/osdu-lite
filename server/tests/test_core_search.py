"""ITEM-018/019: executable core query and reference-cursor contract."""
import json
import os
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from test_server import call, server, valid_well
from fake_search_backend import FakeSearch
from fake_storage_backend import FakeStorage
from fake_file_backend import FakeFiles


class CoreSearchTests(unittest.TestCase):
    def backend(self):
        return FakeSearch()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_files = server.FILES_DIR
        server.FILES_DIR = Path(self.tmp.name)
        self.a, self.b, self.v = [server.group_email("data." + s) for s in ("a", "b", "reader")]
        roles = sorted(server.GROUPS_ALL | {"users.datalake.admins", "users.datalake.ops",
                                           "service.storage.admin", "service.storage.creator"})
        path = Path(self.tmp.name) / "identities.json"
        path.write_text(json.dumps([
            {"principal": who, "token": who + "-secret", "groups": roles + [group]}
            for who, group in (("alice", self.a), ("bob", self.b), ("reader", self.v))
        ]))
        self.env = patch.dict(os.environ, {
            "OSDU_IDENTITIES_FILE": str(path), "OSDU_COMPAT_PROFILE": "core",
            "AZURE_STORAGE_PUBLIC_BLOB_ENDPOINT": "http://localhost:10000/devstoreaccount1"})
        self.env.start()
        self.engine = self.backend()
        server.start_runtime(self.engine, storage=FakeStorage(), files=FakeFiles())
        self.records = []
        for name, start, owner in (("beta", 2, self.a), ("alpha", 2, self.a),
                                    ("alpha", 1, self.a), ("foreign", 9, self.b)):
            record = valid_well("company-prod:master-data--Well:" + str(len(self.records)), name)
            record["data"]["StartM"] = start
            record["acl"] = {"owners": [owner], "viewers": [self.v]}
            self.records.append(record)
        server.authorized_upsert(self.records, index_immediately=True, principal=server.core_auth.SYSTEM)

    def tearDown(self):
        server.stop_runtime()
        if hasattr(self.engine, "request"):
            self.engine.reset()
        self.env.stop()
        server.COMPAT_PROFILE = "legacy"
        server.start_runtime(FakeSearch(), storage=FakeStorage())
        server.FILES_DIR = self.old_files
        self.tmp.cleanup()

    def request(self, body, who="alice", cursor=False):
        return call("post", "/api/search/v2/query" + ("_with_cursor" if cursor else ""),
                    body, token=who + "-secret")

    def query(self, who="alice", **extra):
        status, body = self.request({"kind": server.WELL_KIND, **extra}, who)
        self.assertEqual(status, 200, body)
        return body

    def test_exact_totals_limit_zero_owner_and_pre_page_terms(self):
        self.assertEqual(self.query(limit=0, trackTotalCount=False), {"results": [], "totalCount": 3})
        self.assertEqual(self.query(who="bob")["totalCount"], 1)
        self.assertEqual(self.query(who="reader")["totalCount"], 4)
        self.assertEqual(self.query(who="reader", queryAsOwner=True)["totalCount"], 0)
        body = self.query(limit=1, aggregateBy="data.FacilityName")
        self.assertEqual(body["aggregations"], [{"key": "alpha", "count": 2}, {"key": "beta", "count": 1}])

    def test_sort_projection_exclusion_and_metadata_overlay(self):
        body = self.query(sort={"field": ["data.FacilityName", "data.StartM"],
                                "order": ["ASC", "DESC"]}, returnedFields=["id", "data.StartM"])
        self.assertEqual([r["id"] for r in body["results"]], [self.records[i]["id"] for i in (1, 2, 0)])
        self.assertEqual(set(body["results"][0]), {"id", "data"})
        body = self.query(excludedFields=["data.FacilityName", "acl.viewers"])
        self.assertNotIn("FacilityName", body["results"][0]["data"])
        self.assertNotIn("viewers", body["results"][0]["acl"])
        with server.STORE_LOCK:
            server.RECORD_METADATA[self.records[0]["id"]] = {"tags": {"live": "yes"}}
        body = self.query(returnedFields=["id", "tags"])
        self.assertEqual(body["results"][0]["tags"], {"live": "yes"})

    def test_dotted_array_projections_preserve_elements_and_exclude_each_secret(self):
        kind = "local:test:Items:1.0.0"
        server.SCHEMA_REGISTRY.register({"kind": kind, "schema": {
            "type": "object", "properties": {"Items": {"type": "array", "items": {
                "type": "object", "properties": {"Name": {"type": "string"},
                    "Secret": {"type": "string"}, "Code": {"type": "integer"}}}}}}}, "POST")
        record = deepcopy(self.records[0])
        record.update(id="company-prod:Items:array", kind=kind, data={"Items": [
            {"Name": "alpha", "Secret": "x", "Code": 1},
            {"Name": "beta", "Secret": "y", "Code": 2}, {"Code": 3}]})
        foreign = deepcopy(record)
        foreign.update(id="company-prod:Items:foreign", acl=deepcopy(self.records[3]["acl"]),
                       data={"Items": [{"Name": "foreign", "Secret": "not allowed", "Code": 9}]})
        server.authorized_upsert([record, foreign], index_immediately=True,
                                 principal=server.core_auth.SYSTEM)
        body = self.query(kind=kind, returnedFields=["id", "data.Items.Name", "data.Items.Code"])
        self.assertEqual(body["totalCount"], 1)
        self.assertEqual(body["results"][0], {"id": record["id"], "data": {"Items": [
            {"Name": "alpha", "Code": 1}, {"Name": "beta", "Code": 2}, {"Code": 3}]}})
        body = self.query(kind=kind, returnedFields=["data.Items.Name", "data.Items.Secret"])
        self.assertEqual(body["results"], [{"data": {"Items": [
            {"Name": "alpha", "Secret": "x"}, {"Name": "beta", "Secret": "y"}, {}]}}])
        body = self.query(kind=kind, excludedFields=["data.Items.Secret", "acl.viewers"])
        self.assertTrue(all("Secret" not in item for item in body["results"][0]["data"]["Items"]))
        self.assertNotIn("viewers", body["results"][0]["acl"])
        body = self.query(kind=kind, returnedFields=["data.Items", "data.Items.Name"])
        self.assertEqual(body["results"], [{"data": record["data"]}])

    def test_legacy_cursors_reject_cross_service_reuse_without_advancing(self):
        with patch.object(server, "COMPAT_PROFILE", "legacy"):
            status, search = self.request({"kind": server.WELL_KIND, "limit": 1}, cursor=True)
            self.assertEqual(status, 200, search)
            status, storage = call("get", "/api/storage/v2/records?limit=1",
                                   token="alice-secret")
            self.assertEqual(status, 200, storage)
            search_token, storage_token = search["cursor"], storage["cursor"]
            for service, token in (("storage", search_token), ("search", storage_token)):
                with self.subTest(service=service):
                    before = deepcopy(server.CURSORS[token])
                    if service == "storage":
                        status, body = call("get", "/api/storage/v2/records?cursor=" + token,
                                            token="alice-secret")
                    else:
                        status, body = self.request({"cursor": token}, cursor=True)
                    self.assertEqual(status, 400, body)
                    self.assertEqual(server.CURSORS[token], before)
            self.assertEqual(self.request({"cursor": search_token}, cursor=True)[0], 200)
            self.assertEqual(call("get", "/api/storage/v2/records?cursor=" + storage_token +
                                 "&limit=1", token="alice-secret")[0], 200)

    def test_invalid_unknown_legacy_and_unsupported_requests(self):
        for extra in ({"kind": []}, {"kind": ["*"] * 33}, {"limit": True}, {"limit": 1001},
                      {"offset": 1000, "limit": 1}, {"queryAsOwner": "true"},
                      {"query_as_owner": True}, {"wat": 1}, {"aggregateBy": []},
                      {"returnedFields": ["id"], "excludedFields": ["kind"]},
                      {"returnedFields": ["data.*"]},
                      {"sort": {"field": ["data.Missing"], "order": ["ASC"]}}):
            with self.subTest(extra=extra):
                self.assertEqual(self.request({"kind": "*", **extra})[0], 400)
        for extra in ({"spatialFilter": {}}, {"suggestPhrase": ""}, {"highlightedFields": []},
                      {"search_after": True}, {"sort": {"filter": {}}}):
            with self.subTest(extra=extra):
                self.assertEqual(self.request({"kind": "*", **extra})[0], 501)

    def test_reference_cursor_ownership_exhaustion_and_delete(self):
        status, body = self.request({"kind": server.WELL_KIND, "limit": 1,
                                    "returnedFields": ["id"]}, cursor=True)
        self.assertEqual(status, 200, body)
        token = body["cursor"]
        self.assertRegex(token, r"^[0-9A-F]{32}$")
        self.assertNotIn("FacilityName", json.dumps(server.CURSORS))
        self.assertEqual(self.request({"cursor": token}, "bob", True)[0], 403)
        self.assertEqual(self.request({"cursor": token, "limit": 2}, cursor=True)[0], 400)
        status, second = self.request({"cursor": token}, cursor=True)
        self.assertEqual((status, second["totalCount"]), (200, 3))
        status, last = self.request({"cursor": token}, cursor=True)
        self.assertEqual((status, last["cursor"]), (200, None))
        self.assertNotIn(token, server.CURSORS)
        self.assertEqual(call("delete", "/api/search/v2/query_with_cursor/" + token,
                              token="alice-secret")[0], 204)

    def test_cursor_mutation_groups_legal_expiry_and_capacity(self):
        for mode in ("revision", "groups", "legal"):
            status, body = self.request({"kind": server.WELL_KIND, "limit": 1}, cursor=True)
            self.assertEqual(status, 200, body)
            token = body["cursor"]
            with server.STORE_LOCK:
                if mode == "revision":
                    server.REVISION += 1
                elif mode == "groups":
                    group = server.GROUPS[self.a]
                    members = deepcopy(group["members"])
                    group["members"] = []
                else:
                    tag = server.LEGAL_TAGS[self.records[0]["legal"]["legaltags"][0]]
                    properties = deepcopy(tag["properties"])
                    tag["properties"]["expirationDate"] = "2000-01-01"
            self.assertEqual(self.request({"cursor": token}, cursor=True)[0], 400)
            if mode == "groups":
                group["members"] = members
            elif mode == "legal":
                tag["properties"] = properties
        with patch.object(server, "MAX_CURSORS", 1):
            self.assertEqual(self.request({"kind": "*", "limit": 1}, cursor=True)[0], 200)
            self.assertEqual(self.request({"kind": "*", "limit": 1}, cursor=True)[0], 429)

    def test_cursor_expiry_byte_capacity_and_foreign_delete(self):
        status, body = self.request({"kind": "*", "limit": 1}, cursor=True)
        self.assertEqual(status, 200, body)
        token = body["cursor"]
        self.assertEqual(call("delete", "/api/search/v2/query_with_cursor/" + token,
                              token="bob-secret")[0], 403)
        with patch.object(server.time, "time", return_value=server.time.time() + 301):
            self.assertEqual(self.request({"cursor": token}, cursor=True)[0], 400)
        with patch.object(server, "MAX_CURSOR_BYTES", 1):
            self.assertEqual(self.request({"kind": "*", "limit": 1}, cursor=True)[0], 429)

    def test_storage_reference_cursor_and_strict_selection(self):
        status, body = call("get", "/api/storage/v2/records?limit=1&sortOrder=ASC",
                            token="alice-secret")
        self.assertEqual(status, 200, body)
        self.assertRegex(body["cursor"], r"^[0-9A-F]{32}$")
        token = body["cursor"]
        self.assertEqual(call("get", "/api/storage/v2/records?cursor=" + token,
                              token="bob-secret")[0], 403)
        status, body = call("get", "/api/storage/v2/records?cursor=" + token,
                            token="alice-secret")
        self.assertEqual(status, 200, body)
        server.REVISION += 1
        self.assertEqual(call("get", "/api/storage/v2/records?cursor=" + token,
                              token="alice-secret")[0], 400)
        self.assertEqual(call("get", "/api/storage/v2/records?modifiedAfterDate=bad",
                              token="alice-secret")[0], 400)

    def test_query_policy_race_retries_once_and_never_returns_stale_counts(self):
        original = self.engine.page
        attempts = []
        def mutate(*args, **kwargs):
            result = original(*args, **kwargs)
            attempts.append(1)
            server.REVISION += 1
            return result
        with patch.object(self.engine, "page", side_effect=mutate):
            self.assertEqual(self.request({"kind": "*", "limit": 0})[0], 503)
        self.assertEqual(len(attempts), 2)

    def test_missing_sort_values_are_last_and_id_breaks_ties(self):
        record = deepcopy(self.records[0])
        record["data"].pop("StartM")
        server.authorized_upsert([record], index_immediately=True, principal=server.core_auth.SYSTEM)
        for order in ("ASC", "DESC"):
            body = self.query(sort={"field": ["data.StartM"], "order": [order]})
            self.assertEqual(body["results"][-1]["id"], record["id"])
        body = self.query(sort={"field": ["data.FacilityName"], "order": ["ASC"]})
        self.assertEqual([r["id"] for r in body["results"][:2]], [self.records[i]["id"] for i in (1, 2)])

    def test_terms_unique_arrays_cardinality_and_mixed_scalar_capabilities(self):
        kind = "local:test:Terms:1.0.0"
        server.SCHEMA_REGISTRY.register({"kind": kind, "schema": {"type": "object", "properties": {
            "Values": {"type": "array", "items": {"type": "string"}},
            "StartM": {"type": "string"}}}}, "POST")
        record = deepcopy(self.records[0])
        record.update(id="company-prod:Terms:values", kind=kind,
                      data={"Values": ["same", "same", "other"], "StartM": "text"})
        server.authorized_upsert([record], index_immediately=True, principal=server.core_auth.SYSTEM)
        self.assertEqual(self.query(kind=kind, aggregateBy="data.Values")["aggregations"],
                         [{"key": "other", "count": 1}, {"key": "same", "count": 1}])
        self.assertEqual(self.request({"kind": kind,
            "sort": {"field": ["data.Values"], "order": ["ASC"]}})[0], 400)
        self.assertEqual(self.request({"kind": [kind, server.WELL_KIND],
            "sort": {"field": ["data.StartM"], "order": ["ASC"]}})[0], 400)
        record["data"]["Values"] = [f"k{i:03d}" for i in range(100)]
        server.authorized_upsert([record], index_immediately=True, principal=server.core_auth.SYSTEM)
        self.assertEqual(len(self.query(kind=kind, aggregateBy="data.Values")["aggregations"]), 100)
        record["data"]["Values"].append("excess")
        server.authorized_upsert([record], index_immediately=True, principal=server.core_auth.SYSTEM)
        status, body = self.request({"kind": kind, "aggregateBy": "data.Values", "limit": 0})
        self.assertEqual((status, body["reason"]), (400, "aggregation_cardinality_limit"))

    def test_one_policy_mutation_retries_with_new_authorized_totals(self):
        original = self.engine.page
        attempts = []
        def mutate(*args, **kwargs):
            result = original(*args, **kwargs)
            if not attempts:
                with server.STORE_LOCK, server.storage_transaction():
                    del server.GROUPS[self.a]["members"]["alice"]
            attempts.append(1)
            return result
        with patch.object(self.engine, "page", side_effect=mutate):
            status, body = self.request({"kind": "*", "limit": 0, "aggregateBy": "kind"})
        self.assertEqual((status, body["totalCount"], body["results"], body["aggregations"]),
                         (200, 0, [], []))
        self.assertEqual(len(attempts), 2)

    def test_storage_deleted_owner_cursor_rechecks_all_references(self):
        server.authorized_delete([r["id"] for r in self.records[:3]],
                                 principal=server.core_auth.SYSTEM)
        status, body = call("get", "/api/storage/v2/records?deleted=true&limit=1",
                            token="alice-secret")
        self.assertEqual((status, len(body["results"])), (200, 1))
        token = body["cursor"]
        self.assertEqual(call("get", "/api/storage/v2/records?deleted=true",
                              token="reader-secret")[1]["results"], [])
        with patch.object(server, "is_legal_tag_valid", return_value=False):
            self.assertEqual(call("get", "/api/storage/v2/records?cursor=" + token,
                                  token="alice-secret")[0], 400)
