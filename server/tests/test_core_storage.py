"""EPIC-004 ITEM-008..010: bounded core Storage acceptance checks."""
import json
import os
import tempfile
import threading
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote

from test_server import call, make_handler, _Headers, server, valid_well, wait_until
from fake_search_backend import FakeSearch
from fake_storage_backend import FakeStorage


ROOT = "/api/storage/v2/records"


class CoreStorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_files = server.FILES_DIR
        server.FILES_DIR = Path(self.tmp.name)
        self.env = patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "core"})
        self.env.start()
        self.storage = FakeStorage()
        server.start_runtime(FakeSearch(), storage=self.storage)
        server.INDEX_DELAY_SECONDS = 0
        self.rec = valid_well()

    def tearDown(self):
        server.stop_runtime()
        self.env.stop()
        server.COMPAT_PROFILE = "legacy"
        server.start_runtime(FakeSearch(), storage=FakeStorage())
        server.FILES_DIR = self.old_files
        self.tmp.cleanup()

    def put(self, records=None, query=""):
        return call("put", ROOT + query, records if records is not None else [self.rec])

    def get(self, suffix=""):
        return call("get", ROOT + suffix)

    def settled(self):
        self.assertTrue(wait_until(lambda: server.INDEX_SCHEDULER.pending("index") == 0))

    def patch_records(self, ops, ids=None, media="application/json", body=None, token=None):
        body = body if body is not None else {"query": {"ids": ids or [self.rec["id"]]}, "ops": ops}
        raw = json.dumps(body).encode("utf-8")
        handler = make_handler(ROOT, _Headers({
            "Authorization": "Bearer " + (token or server.TOKEN),
            "data-partition-id": server.PARTITION,
            "Content-Type": media, "Content-Length": str(len(raw)),
        }), raw)
        handler.do_PATCH()
        return handler._status, json.loads(handler.wfile.buf.getvalue())

    def test_create_generated_ids_audit_and_readonly_spoofing(self):
        rec = deepcopy(self.rec)
        rec.pop("id")
        rec.update(version=-10, createUser="spoof", createTime="spoof",
                   modifyUser="spoof", modifyTime="spoof")
        status, body = self.put([rec])
        self.assertEqual(status, 201, body)
        self.assertEqual(body["recordCount"], 1)
        rid = body["recordIds"][0]
        self.assertRegex(rid, r"^company-prod:master-data--Well:[0-9a-f]{32}$")
        self.assertEqual(body["recordIdVersions"], [rid + ":1"])
        self.assertEqual(body["skippedRecordIds"], [])
        status, stored = self.get("/" + rid)
        self.assertEqual(status, 200)
        self.assertEqual(stored["createUser"], "demo@osdu-lite.local")
        self.assertTrue(stored["createTime"].endswith("Z"))
        self.assertNotIn("modifyTime", stored)
        stored["data"]["FacilityName"] = "changed"
        self.put([stored])
        updated = self.get("/" + rid)[1]
        self.assertEqual(updated["version"], 2)
        self.assertEqual(updated["createTime"], stored["createTime"])
        self.assertEqual(updated["modifyUser"], stored["createUser"])

    def test_skipdupes_no_save_queue_and_ordered_mixed_result(self):
        self.assertEqual(self.put()[0], 201)
        self.settled()
        before = deepcopy(server.storage_state())
        with patch.object(self.storage, "save", wraps=self.storage.save) as save, \
                patch.object(server.INDEX_SCHEDULER, "reserve",
                             wraps=server.INDEX_SCHEDULER.reserve) as reserve:
            same = deepcopy(self.rec)
            same.update(ancestry={}, meta=[], tags={}, version=900, createUser="ignored")
            status, body = self.put([same], "?skipdupes=true")
            self.assertEqual((status, body["recordCount"]), (201, 0), body)
            self.assertEqual(body["skippedRecordIds"], [self.rec["id"]])
            self.assertEqual(server.storage_state(), before)
            save.assert_not_called()
            reserve.assert_not_called()
        other = valid_well("company-prod:master-data--Well:OTHER")
        status, body = self.put([self.rec, other], "?skipdupes=true")
        self.assertEqual((status, body["recordIds"], body["skippedRecordIds"]),
                         (201, [other["id"]], [self.rec["id"]]))
        self.assertEqual(self.put()[1]["recordIdVersions"], [self.rec["id"] + ":2"])

    def test_skipdupes_first_version_at_real_byte_limit_and_full_retention(self):
        now = 1800000000
        self.rec["data"]["Custom"] = ""
        rendered = dict(deepcopy(self.rec), version=1, createUser="demo@osdu-lite.local",
                        createTime=server.epoch_to_iso(now))
        self.rec["data"]["Custom"] = "x" * (65536 - len(server.snapshot_bytes(rendered)))
        with patch.object(server.time, "time", return_value=now):
            self.assertEqual(self.put()[0], 201)
        self.settled()
        stored = self.get("/" + self.rec["id"])[1]
        self.assertEqual(len(server.snapshot_bytes(stored)), 65536)
        self.assertEqual(stored["version"], 1)
        self.assertNotIn("modifyTime", stored)
        before = deepcopy(server.storage_state())
        with patch.object(server.time, "time", return_value=now), \
                patch.object(server, "MAX_RETAINED_VERSIONS", 1), \
                patch.object(server, "MAX_RECORD_BYTES", server._retained_bytes()), \
                patch.object(server, "MAX_RECORD_IDS", len(server.RECORD_FENCES)), \
                patch.object(self.storage, "save", wraps=self.storage.save) as save, \
                patch.object(server.INDEX_SCHEDULER, "reserve",
                             wraps=server.INDEX_SCHEDULER.reserve) as reserve:
            status, body = self.put(query="?skipdupes=true")
            self.assertEqual((status, body), (201, {
                "recordCount": 0, "recordIds": [], "recordIdVersions": [],
                "skippedRecordIds": [self.rec["id"]],
            }))
            self.assertEqual(server.storage_state(), before)
            save.assert_not_called()
            reserve.assert_not_called()
        first = valid_well("company-prod:master-data--Well:NEW-A")
        second = valid_well("company-prod:master-data--Well:NEW-B")
        with patch.object(server.time, "time", return_value=now):
            status, body = self.put([first, self.rec, second], "?skipdupes=true")
        self.assertEqual((status, body["recordIds"], body["recordIdVersions"],
                          body["skippedRecordIds"]),
                         (201, [first["id"], second["id"]],
                          [first["id"] + ":1", second["id"] + ":1"], [self.rec["id"]]))
        self.assertEqual(self.get("/" + self.rec["id"])[1], stored)

    def test_skipdupes_still_checks_current_schema_policy_and_unknown_fields(self):
        self.assertEqual(self.put()[0], 201)
        self.settled()
        before = deepcopy(server.storage_state())
        real_get = server.SCHEMA_REGISTRY.get
        def development(kind):
            entry = real_get(kind)
            entry["status"] = "DEVELOPMENT"
            return entry
        with patch.object(self.storage, "save", wraps=self.storage.save) as save, \
                patch.object(server.INDEX_SCHEDULER, "reserve",
                             wraps=server.INDEX_SCHEDULER.reserve) as reserve:
            with patch.object(server.SCHEMA_REGISTRY, "get", side_effect=development):
                self.assertEqual(self.put(query="?skipdupes=true")[0], 409)
            with patch.object(server, "is_legal_tag_valid", return_value=False):
                self.assertEqual(self.put(query="?skipdupes=true")[0], 404)
            real_readable = server.readable
            def not_owner(record, **kwargs):
                return False if kwargs.get("owner") else real_readable(record, **kwargs)
            with patch.object(server, "readable", side_effect=not_owner):
                self.assertEqual(self.put(query="?skipdupes=true")[0], 403)
            with patch.object(server.core_auth, "can_write", return_value=False):
                self.assertEqual(self.put(query="?skipdupes=true")[0], 403)
            with patch.object(server, "readable", wraps=server.readable) as readable:
                status, _ = self.put(query="?skipdupes=true")
                self.assertEqual(status, 201)
                self.assertTrue(any(c.kwargs.get("owner") for c in readable.call_args_list))
            unknown = dict(self.rec, unsupported="must not be ignored")
            self.assertEqual(self.put([unknown], "?skipdupes=true")[0], 400)
            invalid = deepcopy(self.rec)
            invalid["meta"] = {}
            self.assertEqual(self.put([invalid], "?skipdupes=true")[0], 400)
            self.assertEqual(server.storage_state(), before)
            save.assert_not_called()
            reserve.assert_not_called()

    def test_ten_retained_versions_and_watermark_survives_purge(self):
        for n in range(10):
            self.rec["data"]["FacilityName"] = str(n)
            self.assertEqual(self.put()[0], 201)
        rid = self.rec["id"]
        self.settled()
        self.assertEqual(self.get("/versions/" + rid),
                         (200, {"recordId": rid, "versions": list(range(1, 11))}))
        self.assertEqual(self.get("/" + rid + "/1")[0], 200)
        self.assertEqual(call("delete", ROOT + "/" + rid)[0], 204)
        self.assertEqual(self.get("/" + rid)[0], 404)
        self.assertNotIn(rid, server.RECORD_VERSIONS)
        self.assertEqual(server.RECORD_FENCES[rid][2], 10)
        self.assertEqual(self.put()[1]["recordIdVersions"], [rid + ":11"])
        conflict = deepcopy(self.rec)
        call("delete", ROOT + "/" + rid)
        conflict["kind"] = server.WELLBORE_KIND
        conflict["data"] = {}
        self.assertEqual(self.put([conflict])[0], 409)

    def test_soft_delete_hides_history_and_repeated_delete_is_noop(self):
        self.put()
        rid = self.rec["id"]
        route = ROOT + "/" + rid + ":delete"
        self.assertEqual(call("post", route, {})[0], 204)
        before = deepcopy(server.storage_state())
        self.assertEqual(call("post", route, {})[0], 204)
        self.assertEqual(server.storage_state(), before)
        for suffix in ("/" + rid, "/" + rid + "/1", "/versions/" + rid):
            self.assertEqual(self.get(suffix)[0], 404)
        self.assertEqual(self.get("?deleted=true")[1]["results"][0]["id"], rid)
        self.assertEqual(self.put(query="?skipdupes=true")[1]["recordIdVersions"], [rid + ":2"])

    def test_strict_shapes_filters_and_batch(self):
        for records, query in [([], ""), ([self.rec] * 101, ""), ([self.rec] * 2, ""),
                               ([self.rec], "?skipdupes=1"), ([self.rec], "?typo=true")]:
            with self.subTest(query=query, length=len(records)):
                self.assertEqual(self.put(records, query)[0], 400)
        for rid in ("other:master-data--Well:1", "company-prod:x:" + "é" * 250):
            record = dict(self.rec, id=rid)
            self.assertEqual(self.put([record])[0], 400)
        self.put()
        for query in ("?limit=0", "?limit=101", "?limit=1.5", "?deleted=1",
                      "?sortOrder=desc", "?modifyAfterDate=2020-01-01",
                      "?modifiedAfterDate=2020-01-01", "?kind=*", "?limit=1&limit=2",
                      "?foo=bar"):
            with self.subTest(query=query):
                self.assertEqual(self.get(query)[0], 400)
        rec = self.get("/" + self.rec["id"])[1]
        self.assertEqual(self.get("?modifiedAfterDate=" + rec["createTime"])[1]["results"], [])
        status, body = call("post", "/api/storage/v2/query/records:batch",
                            {"records": ["company-prod:master-data--Well:missing", self.rec["id"]]})
        self.assertEqual((status, [r["id"] for r in body["records"]]),
                         (200, [self.rec["id"]]))
        for body in ({"ids": [self.rec["id"]]}, {"records": []}, {"records": [12]}):
            self.assertEqual(call("post", "/api/storage/v2/query/records:batch", body)[0], 400)

    def test_exact_kind_timestamp_id_order_and_alias(self):
        first = valid_well("company-prod:master-data--Well:A")
        second = valid_well("company-prod:master-data--Well:B")
        self.put([second, first])
        status, body = call("get", "/storage/v2/records?sortOrder=ASC&kind=" + server.WELL_KIND)
        self.assertEqual(status, 200)
        self.assertEqual([r["id"] for r in body["results"]], [first["id"], second["id"]])
        self.assertIsNone(body["cursor"])

    def test_retention_capacity_rejects_whole_batch_before_reservation(self):
        self.put()
        self.settled()
        for name, value in (("MAX_RETAINED_VERSIONS", 1), ("MAX_RECORD_BYTES", 1),
                            ("MAX_RECORD_IDS", 1)):
            before = deepcopy(server.storage_state())
            batch = [self.rec, valid_well("company-prod:master-data--Well:OTHER")]
            with self.subTest(limit=name), patch.object(server, name, value), \
                    patch.object(server.INDEX_SCHEDULER, "reserve") as reserve:
                status, _ = self.put(batch)
                self.assertEqual(status, 429)
                reserve.assert_not_called()
                self.assertEqual(server.storage_state(), before)

    def test_oversized_rendered_record_is_413_and_save_failure_rolls_back(self):
        big = deepcopy(self.rec)
        big["data"]["Description"] = "x" * 65536
        self.assertEqual(self.put([big])[0], 413)
        before = deepcopy(server.storage_state())
        with patch.object(self.storage, "save", side_effect=server.BackendError(503, "failed")):
            self.assertEqual(self.put()[0], 503)
        self.assertEqual(server.storage_state(), before)
        self.assertEqual(server.INDEX_SCHEDULER.pending("index"), 0)

    def test_legacy_retention_also_has_no_eviction(self):
        server.COMPAT_PROFILE = "legacy"
        for _ in range(10):
            server.upsert_records([self.rec], index_immediately=True)
        self.assertEqual(len(server.RECORD_VERSIONS[self.rec["id"]]), 10)

    def test_patch_metadata_overlays_all_history_without_audit_changes(self):
        self.put()
        self.put()
        self.settled()
        rid = self.rec["id"]
        before = self.get("/" + rid)[1]
        old_history = deepcopy(server.RECORD_VERSIONS[rid])
        revision = server.REVISION
        generation = server.RECORD_FENCES[rid][1]
        status, body = self.patch_records([{"op": "add", "path": "/tags", "value": {"x": "y"}}])
        self.assertEqual((status, body["recordCount"], body["recordIds"]), (200, 1, [rid]))
        self.assertEqual(body["unAuthorizedRecordIds"], [])
        self.assertEqual(body["lockedRecordIds"], [])
        self.settled()
        after = self.get("/" + rid)[1]
        self.assertEqual({k: v for k, v in after.items() if k != "tags"}, before)
        self.assertEqual(self.get("/" + rid + "/1")[1]["tags"], {"x": "y"})
        self.assertEqual(server.RECORD_VERSIONS[rid], old_history)
        self.assertEqual(server.REVISION, revision + 1)
        self.assertEqual(server.RECORD_FENCES[rid], (self.rec["kind"], generation + 1, 2))
        self.settled()
        snapshot = deepcopy(server.storage_state())
        with patch.object(self.storage, "save") as save, \
                patch.object(server.INDEX_SCHEDULER, "reserve") as reserve:
            status, body = self.patch_records([{"op": "add", "path": "/tags", "value": {"x": "y"}}])
            self.assertEqual((status, body["recordCount"]), (200, 0))
            self.assertEqual(body["recordIds"], [])
            self.assertEqual(server.storage_state(), snapshot)
            save.assert_not_called()
            reserve.assert_not_called()
        self.assertEqual(self.get("?modifiedAfterDate=" + before["modifyTime"])[1]["results"], [])

    def test_patch_json_pointer_arrays_and_mixed_data_metadata(self):
        self.rec["data"]["Custom"] = {"a/b": {"~key": [1, 2]}}
        self.put()
        rid = self.rec["id"]
        status, body = self.patch_records([
            {"op": "add", "path": "/data/Custom/a~1b/~0key/-", "value": 3},
            {"op": "remove", "path": "/data/Custom/a~1b/~0key/0"},
            {"op": "replace", "path": "/data/Custom/a~1b/~0key/0", "value": 5},
            {"op": "add", "path": "/tags", "value": {"patched": "yes"}},
        ], media="application/json-patch+json")
        self.assertEqual((status, body["recordCount"]), (200, 1), body)
        after = self.get("/" + rid)[1]
        self.assertEqual(after["version"], 2)
        self.assertEqual(after["data"]["Custom"]["a/b"]["~key"], [5, 3])
        self.assertEqual(self.get("/" + rid + "/1")[1]["tags"], {"patched": "yes"})
        self.assertNotIn("unAuthorizedRecordIds", body)

    def test_patch_shape_rejection_precedes_mutation(self):
        self.put()
        before = deepcopy(server.storage_state())
        for ops in ([], [{"op": "test", "path": "/data"}],
                    [{"op": "add", "path": "/id", "value": "bad"}],
                    [{"op": "replace", "path": "/data", "value": {}}],
                    [{"op": "add", "path": "/acl/owners", "value": "wrong"}],
                    [{"op": "add", "path": "/tags/x", "value": "no"}],
                    [{"op": "remove", "path": "/tags", "value": {}}],
                    [{"op": "add", "path": "/tags"}],
                    [{"op": "remove", "path": "/data/~2"}]):
            with self.subTest(ops=ops):
                self.assertEqual(self.patch_records(ops)[0], 400)
                self.assertEqual(server.storage_state(), before)
        for query, code in (({"kind": "*"}, 501), ({"ids": [self.rec["id"]] * 2}, 400)):
            self.assertEqual(self.patch_records([], body={"query": query, "ops": [
                {"op": "add", "path": "/tags", "value": {}}]})[0], code)

    def test_patch_ordered_partial_success_and_one_commit(self):
        first = self.rec["id"]
        second = valid_well("company-prod:master-data--Well:SECOND")
        second["data"]["Custom"] = {"flag": 1}
        self.put([self.rec, second])
        self.settled()
        missing = "company-prod:master-data--Well:MISSING"
        with patch.object(self.storage, "save", wraps=self.storage.save) as save:
            status, body = self.patch_records([
                {"op": "remove", "path": "/data/Custom/flag"}
            ], ids=[missing, first, second["id"]], media="application/json-patch+json")
            self.assertEqual(status, 206, body)
            self.assertEqual(body["recordIds"], [second["id"]])
            self.assertEqual(body["notFoundRecordIds"], [missing])
            self.assertEqual(body["failedRecordIds"], [first])
            self.assertTrue(all(isinstance(e, str) for e in body["errors"]))
            self.assertEqual(body["recordCount"], 1)
            self.assertEqual(save.call_count, 1)
        self.assertEqual(self.get("/" + first)[1]["version"], 1)
        self.assertEqual(self.get("/" + second["id"])[1]["version"], 2)

    def test_patch_commit_failure_restores_every_map_and_reservation(self):
        other = valid_well("company-prod:master-data--Well:OTHER")
        self.put([self.rec, other])
        self.settled()
        before = deepcopy(server.storage_state())
        with patch.object(self.storage, "save", side_effect=server.BackendError(503, "failed")):
            self.assertEqual(self.patch_records([
                {"op": "add", "path": "/tags", "value": {"x": "y"}},
                {"op": "replace", "path": "/data/FacilityName", "value": "changed"},
            ], ids=[self.rec["id"], other["id"]], media="application/json-patch+json")[0], 503)
        self.assertEqual(server.storage_state(), before)
        self.assertEqual(server.INDEX_SCHEDULER.pending("index"), 0)

    def test_patch_invalid_final_record_is_failed_and_noop_data_no_save(self):
        self.put()
        self.settled()
        before = deepcopy(server.storage_state())
        for ops in ([
            {"op": "replace", "path": "/data/FacilityName", "value": 12}], [
            {"op": "replace", "path": "/acl/owners", "value": []}]):
            status, body = self.patch_records(ops, media="application/json-patch+json")
            self.assertEqual(status, 206, body)
            self.assertEqual(body["failedRecordIds"], [self.rec["id"]])
            self.assertEqual(server.storage_state(), before)
        with patch.object(self.storage, "save") as save:
            status, body = self.patch_records([
                {"op": "replace", "path": "/data/FacilityName", "value": "EAGLE-01"}
            ], media="application/json-patch+json")
            self.assertEqual((status, body["recordCount"]), (200, 0))
            save.assert_not_called()

    def test_patch_stale_same_version_job_cannot_restore_policy(self):
        self.put()
        self.settled()
        rid = self.rec["id"]
        with server.STORE_LOCK:
            old_job, _, _ = server._index_job(server.effective_record(server.RECORD_BY_ID[rid]))
        self.assertEqual(self.patch_records([{"op": "add", "path": "/tags",
                                             "value": {"latest": "yes"}}])[0], 200)
        self.settled()
        with patch.object(server.SEARCH_BACKEND, "bulk", wraps=server.SEARCH_BACKEND.bulk) as bulk:
            outcome = server._execute_index(old_job)
            self.assertEqual(outcome.error, "superseded")
            bulk.assert_not_called()
        self.assertEqual(self.get("/" + rid)[1]["version"], 1)

    def test_patch_readable_nonowner_and_foreign_are_distinguished_only_for_metadata(self):
        self.put()
        self.settled()
        reader = server.core_auth.Principal("reader")
        roles = ["users.datalake.editors", "service.storage.viewer"]
        with server.STORE_LOCK, server.storage_transaction():
            for role in roles:
                server.GROUPS[server.group_email(role)]["members"][reader.name] = "MEMBER"
            server.GROUPS[server.ACL_VIEWERS]["members"][reader.name] = "MEMBER"
        with patch.dict(server.IDENTITIES, {"reader-token": reader}):
            status, body = self.patch_records([{"op": "add", "path": "/tags", "value": {}}],
                                               token="reader-token")
            self.assertEqual(status, 206, body)
            self.assertEqual(body["unAuthorizedRecordIds"], [self.rec["id"]])
            with server.STORE_LOCK, server.storage_transaction():
                del server.GROUPS[server.ACL_VIEWERS]["members"][reader.name]
            status, body = self.patch_records([{"op": "add", "path": "/tags", "value": {}}],
                                               token="reader-token")
            self.assertEqual(body["notFoundRecordIds"], [self.rec["id"]])
            self.assertEqual(body["unAuthorizedRecordIds"], [])

    def test_inventory_reports_effective_retention_and_enabled_storage_subset(self):
        status, body = call("get", "/info")
        self.assertEqual(status, 200)
        info = body["compatibility"]
        self.assertEqual(info["limits"]["retainedVersionsPerId"], 1000)
        self.assertIn("PUT /api/storage/v2/records", info["supportedOperations"])
        self.assertIn("storage-list-cursors", info["supportedFeatures"])

    def test_encoded_ids_empty_delete_and_legacy_action_rejection(self):
        self.put()
        rid = self.rec["id"]
        self.assertEqual(self.get("/" + quote(rid, safe=""))[0], 200)
        status, _ = call("post", "/api/records/" + rid + ":delete", {})
        self.assertEqual(status, 405)
        status, _ = call("post", ROOT + "/" + rid + ":delete",
                         headers={"Content-Length": "0"})
        self.assertEqual(status, 204)

    def test_duplicate_comparison_is_canonical_but_array_order_matters(self):
        self.rec.update(meta=[{"x": 1, "y": 2}], ancestry={"parents": ["a", "b"]})
        self.put()
        reordered = deepcopy(self.rec)
        reordered["data"] = dict(reversed(list(reordered["data"].items())))
        reordered["meta"] = [{"y": 2, "x": 1}]
        self.assertEqual(self.put([reordered], "?skipdupes=true")[1]["recordCount"], 0)
        reordered["ancestry"]["parents"].reverse()
        self.assertEqual(self.put([reordered], "?skipdupes=true")[1]["recordCount"], 1)

    def test_patch_acl_overlay_controls_old_versions_and_batch(self):
        self.put()
        self.put()
        reader = server.core_auth.Principal("reader")
        with server.STORE_LOCK, server.storage_transaction():
            server.GROUPS[server.group_email("service.storage.viewer")]["members"][reader.name] = "MEMBER"
            server.GROUPS[server.ACL_VIEWERS]["members"][reader.name] = "MEMBER"
        with patch.dict(server.IDENTITIES, {"reader-token": reader}):
            route = ROOT + "/" + self.rec["id"] + "/1"
            self.assertEqual(call("get", route, token="reader-token")[0], 200)
            self.assertEqual(self.patch_records([
                {"op": "replace", "path": "/acl/viewers", "value": []}])[0], 200)
            self.assertEqual(call("get", route, token="reader-token")[0], 404)
            self.assertEqual(call("post", "/api/storage/v2/query/records:batch",
                                  {"records": [self.rec["id"]]}, token="reader-token")[1], {"records": []})
        self.assertEqual(self.get("/" + self.rec["id"] + "/1")[1]["acl"]["viewers"], [])

    def test_patch_preserves_subset_if_later_capacity_or_final_validation_fails(self):
        other = valid_well("company-prod:master-data--Well:OTHER")
        self.put([self.rec, other])
        self.put([other])
        self.settled()
        with patch.object(server, "MAX_RETAINED_VERSIONS", 2):
            status, body = self.patch_records([
                {"op": "replace", "path": "/data/FacilityName", "value": "new"}
            ], ids=[self.rec["id"], other["id"]], media="application/json-patch+json")
        self.assertEqual(status, 206)
        self.assertEqual(body["recordIds"], [self.rec["id"]])
        self.assertEqual(body["failedRecordIds"], [other["id"]])
        self.assertEqual(self.get("/" + other["id"])[1]["version"], 2)

    def test_json_patch_array_rules_and_whole_data_replacement(self):
        self.rec["data"]["Custom"] = ["a", "b"]
        self.put()
        for path in ("/data/Custom/01", "/data/Custom/-1", "/data/Custom/3",
                     "/data/absent/key", "/data/Custom/-/x"):
            before = deepcopy(server.storage_state())
            status, body = self.patch_records([{"op": "add", "path": path, "value": "c"}],
                                               media="application/json-patch+json")
            self.assertEqual(status, 206)
            self.assertEqual(body["failedRecordIds"], [self.rec["id"]])
            self.assertEqual(server.storage_state(), before)
        data = {"FacilityName": "whole"}
        self.assertEqual(self.patch_records([{"op": "replace", "path": "/data", "value": data}],
                                            media="application/json-patch+json")[0], 200)
        self.assertEqual(self.get("/" + self.rec["id"])[1]["data"], data)

    def test_list_reference_cursor_does_not_silently_truncate(self):
        self.put([self.rec, valid_well("company-prod:master-data--Well:OTHER")])
        status, body = self.get("?limit=1")
        self.assertEqual((status, len(body["results"])), (200, 1))
        self.assertRegex(body["cursor"], r"^[0-9A-F]{32}$")
        status, last = self.get("?cursor=" + body["cursor"])
        self.assertEqual((status, len(last["results"]), last["cursor"]), (200, 1, None))
        self.assertEqual(self.get("?cursor=anything")[0], 400)

    def test_patch_history_byte_limit_and_all_failed_request_do_not_save(self):
        self.rec["data"]["Custom"] = "x" * 64000
        self.put()
        # The latest record is small, but retained version one is nearly full.
        self.rec["data"].pop("Custom")
        self.put()
        self.settled()
        before = deepcopy(server.storage_state())
        with patch.object(self.storage, "save") as save:
            status, body = self.patch_records([
                {"op": "add", "path": "/tags", "value": {"x": "y" * 2000}}])
            self.assertEqual(status, 206)
            self.assertEqual(body["failedRecordIds"], [self.rec["id"]])
            self.assertEqual(server.storage_state(), before)
            save.assert_not_called()

    def test_put_after_patch_replaces_current_metadata_for_all_versions(self):
        self.put()
        self.patch_records([{"op": "add", "path": "/tags", "value": {"old": "value"}}])
        updated = deepcopy(self.rec)
        updated["tags"] = {"new": "value"}
        self.put([updated])
        self.assertEqual(self.get("/" + self.rec["id"] + "/1")[1]["tags"], {"new": "value"})

    def test_equal_service_roles_do_not_bypass_core_storage_acl(self):
        self.put()
        rid = self.rec["id"]
        stranger = server.core_auth.Principal("stranger")
        with server.STORE_LOCK, server.storage_transaction():
            for role in ("service.storage.viewer", "service.storage.creator",
                         "service.storage.admin", "users.datalake.editors"):
                server.GROUPS[server.group_email(role)]["members"][stranger.name] = "MEMBER"
        before = deepcopy(server.storage_state())
        with patch.dict(server.IDENTITIES, {"stranger-token": stranger}):
            for suffix in ("/" + rid, "/" + rid + "/1", "/versions/" + rid):
                self.assertEqual(call("get", ROOT + suffix, token="stranger-token")[0], 404)
            self.assertEqual(call("get", ROOT, token="stranger-token")[1]["results"], [])
            self.assertEqual(call("post", "/api/storage/v2/query/records:batch",
                                  {"records": [rid]}, token="stranger-token")[1]["records"], [])
            for method, route, body in (("put", ROOT, [self.rec]),
                                        ("post", ROOT + "/" + rid + ":delete", {}),
                                        ("delete", ROOT + "/" + rid, None)):
                self.assertEqual(call(method, route, body, token="stranger-token")[0], 404)
            changed_kind = dict(self.rec, kind=server.WELLBORE_KIND, data={})
            self.assertEqual(call("put", ROOT, [changed_kind], token="stranger-token")[0], 404)
        self.assertEqual(server.storage_state(), before)

    def test_restart_after_overlay_and_purge_preserves_generation_and_watermark(self):
        self.put()
        self.patch_records([{"op": "add", "path": "/tags", "value": {"x": "y"}}])
        self.settled()
        server.start_runtime(FakeSearch(), storage=self.storage)
        rid = self.rec["id"]
        self.assertEqual(self.get("/" + rid + "/1")[1]["tags"], {"x": "y"})
        self.assertEqual(server.RECORD_FENCES[rid], (self.rec["kind"], 2, 1))
        call("delete", ROOT + "/" + rid)
        server.start_runtime(FakeSearch(), storage=self.storage)
        self.assertEqual(server.RECORD_FENCES[rid], (self.rec["kind"], 3, 1))
        self.assertEqual(self.put()[1]["recordIdVersions"], [rid + ":2"])
        self.settled()
        self.assertEqual(server.RECORD_FENCES[rid], (self.rec["kind"], 4, 2))
        self.assertEqual(server.SEARCH_BACKEND.generations[rid], 4)
        self.assertEqual(self.get("/" + rid)[1]["version"], 2)

    def test_delete_commit_failure_restores_history_metadata_and_fence(self):
        self.put()
        self.patch_records([{"op": "add", "path": "/tags", "value": {"x": "y"}}])
        self.settled()
        before = deepcopy(server.storage_state())
        with patch.object(self.storage, "save", side_effect=server.BackendError(503, "failed")):
            self.assertEqual(call("delete", ROOT + "/" + self.rec["id"])[0], 503)
        self.assertEqual(server.storage_state(), before)
        self.assertEqual(server.INDEX_SCHEDULER.pending("index"), 0)

    def test_delayed_same_version_job_finishes_before_new_policy_publication(self):
        self.put()
        self.settled()
        entered, release = threading.Event(), threading.Event()
        rid = self.rec["id"]
        with server.STORE_LOCK:
            old, _, _ = server._index_job(server.effective_record(server.RECORD_BY_ID[rid]))
        original_bulk = server.SEARCH_BACKEND.bulk
        outcomes = []
        def delayed(records):
            if not entered.is_set():
                entered.set()
                release.wait(5)
            return original_bulk(records)
        worker = threading.Thread(target=lambda: outcomes.append(server._execute_index(old)))
        with patch.object(server.SEARCH_BACKEND, "bulk", side_effect=delayed):
            try:
                worker.start()
                self.assertTrue(entered.wait(2))
                self.assertEqual(self.patch_records([
                    {"op": "add", "path": "/tags", "value": {"latest": "yes"}}])[0], 200)
                release.set()
                worker.join(5)
                self.assertFalse(worker.is_alive())
                self.settled()
            finally:
                release.set()
                worker.join(5)
        self.assertEqual(outcomes[0].error, "superseded")
        self.assertEqual(server.SEARCH_BACKEND.documents[rid]["tags"], {"latest": "yes"})
        self.assertEqual(self.get("/" + rid)[1]["version"], 1)

    def test_core_admission_requires_published_schema(self):
        real_get = server.SCHEMA_REGISTRY.get
        def development(kind):
            entry = real_get(kind)
            if entry:
                entry["status"] = "DEVELOPMENT"
            return entry
        with patch.object(server.SCHEMA_REGISTRY, "get", side_effect=development):
            self.assertEqual(self.put()[0], 409)
        self.assertEqual(server.RECORD_BY_ID, {})

    def test_index_failure_after_commit_is_accepted_but_delete_reports_pending(self):
        from runtime_jobs import Outcome
        with patch.object(server.SEARCH_BACKEND, "bulk", return_value=[
                Outcome(False, False, "engine_unavailable")]):
            self.assertEqual(self.put()[0], 201)
            self.settled()
            self.assertIn(self.rec["id"], self.storage.state["records"])
            with patch.object(server, "DELETE_ACK_TIMEOUT", .05):
                status, body = call("post", ROOT + "/" + self.rec["id"] + ":delete", {})
                self.assertEqual((status, body["reason"]), (503, "deletion_pending"))
            self.assertIn(self.rec["id"], self.storage.state["deleted"])

    def test_put_checks_rendered_history_limit_before_commit(self):
        self.rec["data"]["Custom"] = "x" * 64000
        self.put()
        self.rec["data"].pop("Custom")
        self.put()
        self.settled()
        before = deepcopy(server.storage_state())
        self.rec["tags"] = {"x": "y" * 2000}
        self.assertEqual(self.put()[0], 413)
        self.assertEqual(server.storage_state(), before)

    def test_malformed_kind_and_operation_values_are_400_not_exceptions(self):
        self.put()
        before = deepcopy(server.storage_state())
        for kind in (None, [], {}):
            malformed = dict(self.rec, kind=kind)
            self.assertEqual(self.put([malformed])[0], 400)
        malformed = deepcopy(self.rec)
        del malformed["kind"]
        self.assertEqual(self.put([malformed])[0], 400)
        for operation in ([], {}, 12, None):
            self.assertEqual(self.patch_records([
                {"op": operation, "path": "/tags", "value": {}}])[0], 400)
        for path, value in (("/acl/owners/0", 12), ("/acl/owners/0/foo", "bad"),
                            ("/tags/key/nested", "bad"), ("/tags/key", [])):
            self.assertEqual(self.patch_records([{"op": "add", "path": path, "value": value}],
                                               media="application/json-patch+json")[0], 400)
        self.assertEqual(server.storage_state(), before)

    def test_storage_cannot_create_unbound_core_files_or_change_bound_identity(self):
        record = deepcopy(self.rec)
        record.update(id="company-prod:dataset--File.Generic:BOUND", kind=server.DATASET_KIND,
                      data={"FileName": "display.bin", "FileSource": "source.bin",
                            "FileSize": 1, "sha256": "0" * 64})
        self.assertEqual(self.put([record])[0], 400)
        server.authorized_upsert([record], principal=server.core_auth.SYSTEM, index_immediately=True)
        rid = record["id"]
        binding = {"finalKey": "final/bound", "size": 1, "sha256": "0" * 64,
                   "grantDigest": "1" * 64, "principal": "demo@osdu-lite.local",
                   "recordId": rid, "metadataDigest": "2" * 64, "version": 1}
        with server.STORE_LOCK, server.storage_transaction():
            server.FILE_BINDINGS[rid] = binding
        before = deepcopy(server.storage_state())
        for field, value in (("FileSource", "elsewhere"), ("FileSize", 2), ("sha256", "9" * 64)):
            changed = deepcopy(record)
            changed["data"][field] = value
            self.assertEqual(self.put([changed])[0], 409)
            self.assertEqual(server.storage_state(), before)
            status, body = self.patch_records([
                {"op": "replace", "path": "/data/" + field, "value": value}
            ], ids=[rid], media="application/json-patch+json")
            self.assertEqual((status, body["failedRecordIds"]), (206, [rid]))
            self.assertEqual(server.storage_state(), before)
        changed = deepcopy(record)
        changed["data"]["FileName"] = "new-display.bin"
        self.assertEqual(self.put([changed])[0], 201)
        self.settled()
        before = deepcopy(server.storage_state())
        with patch.object(self.storage, "save", side_effect=server.BackendError(503, "failed")):
            self.assertEqual(call("delete", ROOT + "/" + rid)[0], 503)
        self.assertEqual(server.storage_state(), before)
        self.assertEqual(call("delete", ROOT + "/" + rid)[0], 204)
        self.assertNotIn(rid, server.FILE_BINDINGS)

    def test_preexisting_auditless_format2_reads_use_stored_timestamps_without_save(self):
        self.put()
        self.put()
        self.settled()
        state = deepcopy(self.storage.state)
        rid = self.rec["id"]
        for record in [state["records"][rid]] + [
                entry["record"] for entry in state["versions"][rid]]:
            for key in ("createUser", "createTime", "modifyUser", "modifyTime"):
                record.pop(key, None)
        for entry in state["versions"][rid]:
            entry["bytes"] = len(server.core_profile.canonical(entry["record"]).encode("utf-8"))
        self.storage.state = state
        server.start_runtime(FakeSearch(), storage=self.storage)
        with patch.object(self.storage, "save") as save:
            status, body = self.get("/" + rid)
            self.assertEqual(status, 200)
            self.assertEqual(body["createUser"], "osdu-lite:migrated")
            self.assertEqual(body["createTime"], server.epoch_to_iso(state["timestamps"][rid]["createTime"]))
            self.assertEqual(body["modifyTime"], server.epoch_to_iso(state["versions"][rid][-1]["timestamp"]))
            self.assertNotIn("modifyTime", self.get("/" + rid + "/1")[1])
            self.assertEqual(self.get()[0], 200)
            save.assert_not_called()
