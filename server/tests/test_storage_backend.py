"""Persistence acceptance checks, with an opt-in real Azurite transport."""
import os
import hashlib
import json
import tempfile
import unittest
import uuid
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from test_server import ServerTestCase, call, server, valid_well
from fake_search_backend import FakeSearch
from fake_storage_backend import FakeStorage
from search_backend import BackendError
from storage_backend import AzureBlobStorage


class SnapshotV2Tests(ServerTestCase):
    def setUp(self):
        super().setUp()
        self.storage = FakeStorage()
        server.start_runtime(FakeSearch(), storage=self.storage)

    def tearDown(self):
        server.start_runtime(FakeSearch(), storage=FakeStorage())
        super().tearDown()

    def restart(self):
        server.start_runtime(FakeSearch(), storage=self.storage)

    def format1(self):
        server.upsert_records([valid_well()], index_immediately=True)
        state = self.storage.load()
        old = {key: deepcopy(state[key]) for key in
                ("records", "versions", "fences", "timestamps", "deleted", "suppressed")} | {"format": 1}
        old["fences"] = {rid: fence[:2] for rid, fence in old["fences"].items()}
        for record in list(old["records"].values()) + [
                entry["record"] for entries in old["versions"].values() for entry in entries]:
            for key in ("createUser", "createTime", "modifyUser", "modifyTime"):
                record.pop(key, None)
        return old

    def custom_schema(self):
        return {"kind": "local:test:Thing:1.0.0",
                "schema": {"type": "object", "properties": {"Value": {"type": "number"}}}}

    def test_referenced_policy_definitions_cannot_be_deleted(self):
        record = valid_well()
        server.upsert_records([record], index_immediately=True)
        server.upsert_records([valid_well(name="second")], index_immediately=True)
        self.assertEqual(call("post", "/api/legal/v1/legaltags", {"name": "overlay"})[0], 201)
        self.assertEqual(call("post", "/api/entitlements/v2/groups", {"name": "data.overlay"})[0], 201)
        overlay_group = server.group_email("data.overlay")
        with server.STORE_LOCK, server.storage_transaction():
            server.RECORD_METADATA[record["id"]] = {
                "acl": {"owners": [overlay_group], "viewers": []},
                "legal": {**record["legal"], "legaltags": ["overlay"]},
            }
        before = deepcopy(server.storage_state())
        for path in (
            "/api/legal/v1/legaltags/" + server.LEGAL_TAG,
            "/api/entitlements/v2/groups/" + server.ACL_OWNERS,
            "/api/legal/v1/legaltags/overlay",
            "/api/entitlements/v2/groups/" + overlay_group,
        ):
            with self.subTest(path=path):
                self.assertEqual(call("delete", path)[0], 409)
                self.assertEqual(server.storage_state(), before)
        self.restart()
        self.assertEqual(server.storage_state(), before)
        for create, body, delete in (
            ("/api/legal/v1/legaltags", {"name": "unused"}, "/api/legal/v1/legaltags/unused"),
            ("/api/entitlements/v2/groups", {"name": "data.unused"},
             "/api/entitlements/v2/groups/" + server.group_email("data.unused")),
        ):
            self.assertEqual(call("post", create, body)[0], 201)
            self.assertEqual(call("delete", delete)[0], 204)
        self.restart()

    def test_near_limit_registration_can_restore_its_larger_audited_export(self):
        payload = self.custom_schema()
        payload["schema"]["description"] = ""
        encoded = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        payload["schema"]["description"] = "x" * (128 * 1024 - len(encoded(payload)) - 1)
        self.assertEqual(call("post", "/api/schema-service/v1/schema", payload)[0], 201)
        expected = deepcopy(server.storage_state())
        self.assertGreater(len(encoded(expected["schemas"][0])), 128 * 1024)
        self.restart()
        self.assertEqual(server.storage_state(), expected)

    def test_format1_migration_preserves_maps_backup_and_commits_once(self):
        old = self.format1()
        old["suppressed"] = [valid_well()["id"]]
        self.storage.state = deepcopy(old)
        saves = self.storage.saves
        self.restart()
        state = self.storage.load()
        self.assertEqual(state["format"], 2)
        self.assertEqual(state["revision"], 1)
        self.assertEqual(self.storage.saves, saves + 1)
        for key in ("timestamps", "deleted", "suppressed"):
            self.assertEqual(state[key], old[key])
        for rid, fence in old["fences"].items():
            self.assertEqual(state["fences"][rid][:2], fence[:2])
            self.assertGreaterEqual(state["fences"][rid][2], fence[1])
        for rid, rec in old["records"].items():
            self.assertEqual(state["records"][rid]["data"], rec["data"])
            self.assertEqual(state["records"][rid]["version"], rec["version"])
            self.assertEqual(state["records"][rid]["createUser"], "osdu-lite:migrated")
            self.assertEqual(state["records"][rid]["createTime"],
                             server.epoch_to_iso(old["timestamps"][rid]["createTime"]))
        raw = self.storage.loaded_raw
        digest = hashlib.sha256(raw).hexdigest()
        name = hashlib.sha256(server.PARTITION.encode()).hexdigest() + "." + digest + ".format1.json"
        self.assertEqual(self.storage.backups[name], raw)
        self.restart()
        self.assertEqual(self.storage.load(), state)
        self.assertEqual(self.storage.saves, saves + 1)

    def test_format2_control_roundtrip_and_revision_atomic_rollback(self):
        before = deepcopy(self.storage.state)
        with self.assertRaises(BackendError):
            with server.STORE_LOCK, server.storage_transaction():
                server.SCHEMA_REGISTRY.register(self.custom_schema(), "POST")
                server.LEGAL_TAGS["custom"] = server.default_legal_tag("custom")
                server.GROUPS[server.ACL_OWNERS]["members"]["member@local"] = "OWNER"
                server.RECORD_METADATA["unallocated"] = {}
        self.assertEqual(server.storage_state(), before)

    def test_control_restart_reset_and_all_maps_rollback(self):
        schema_path = "/api/schema-service/v1/schema"
        self.assertEqual(call("post", schema_path, self.custom_schema())[0], 201)
        tag_path = "/api/legal/v1/legaltags"
        self.assertEqual(call("post", tag_path, {"name": "custom"})[0], 201)
        group_path = "/api/entitlements/v2/groups"
        self.assertEqual(call("post", group_path, {"name": "data.custom"})[0], 201)
        email = server.group_email("data.custom")
        self.assertEqual(call("post", group_path + "/" + email + "/members",
                              {"members": [{"email": "member@local", "role": "OWNER"}]})[0], 200)
        self.assertEqual(call("patch", group_path + "/" + email, {"description": "changed"})[0], 204)
        self.assertEqual(call("put", tag_path, {"name": "custom", "description": "changed"})[0], 200)
        with server.STORE_LOCK, server.storage_transaction():
            server.GROUPS[server.ACL_VIEWERS]["members"].clear()
        expected = deepcopy(self.storage.state)
        self.restart()
        self.assertEqual(self.storage.state, expected)
        self.assertEqual(server.SCHEMA_REGISTRY.export_custom(), expected["schemas"])
        self.assertEqual(server.GROUPS[email]["members"], {"member@local": "OWNER"})
        self.assertEqual(server.GROUPS[server.ACL_VIEWERS]["members"], {})
        with patch.object(self.storage, "save", side_effect=BackendError(503, "injected")):
            for method, path, body in (
                ("delete", tag_path + "/custom", None),
                ("delete", group_path + "/" + email + "/members/member@local", None),
                ("delete", group_path + "/" + email, None),
                ("put", schema_path, dict(self.custom_schema(), status="DEPRECATED")),
                ("post", group_path, {"name": "data.failed"}),
                ("patch", group_path + "/" + email, {"name": "failed"}),
                ("put", tag_path, {"name": "custom", "description": "failed"}),
            ):
                self.assertEqual(call(method, path, body)[0], 503, (method, path))
                self.assertEqual(server.storage_state(), expected)
            with self.assertRaises(BackendError):
                with server.STORE_LOCK, server.storage_transaction():
                    server.FILE_BINDINGS["missing"] = {"blob": "invalid"}
                    server.UPLOAD_GRANTS["bad"] = {}
                    server.REVISION += 10
            self.assertEqual(server.storage_state(), expected)
            with self.assertRaises(BackendError):
                server.clear_state()
            self.assertEqual(server.storage_state(), expected)
        server.clear_state()
        reset = deepcopy(self.storage.state)
        self.assertEqual(reset["revision"], expected["revision"] + 1)
        self.assertEqual(reset["schemas"], [])
        self.assertEqual(reset["legalTags"], {server.LEGAL_TAG: server.default_legal_tag()})
        self.assertEqual(reset["groups"], server.seed_groups())
        for key in ("records", "versions", "fences", "timestamps", "recordMetadata", "fileBindings", "uploadGrants"):
            self.assertEqual(reset[key], {})
        self.restart()
        self.assertEqual(server.storage_state(), reset)

    def test_unknown_malformed_and_oversized_states_fail_before_index(self):
        good = deepcopy(self.storage.state)
        bad_states = [dict(good, format=3), dict(good, revision=True),
                      dict(good, revision=-1), dict(good, groups=[]),
                      dict(good, records={"x": {}}), dict(good, schemas=[{}]),
                      dict(good, legalTags={"x": {}}),
                      dict(good, recordMetadata={"missing": {}}),
                      dict(good, fileBindings={"missing": {}}),
                      dict(good, uploadGrants={"raw-token": {}}),
                      dict(good, cursors={}), dict(good, groups={str(n): {} for n in range(257)})]
        for state in bad_states:
            with self.subTest(state=list(state)):
                self.storage.state = state
                engine = FakeSearch()
                with patch.object(engine, "start_epoch", wraps=engine.start_epoch) as start:
                    with self.assertRaises(BackendError):
                        server.start_runtime(engine, storage=self.storage)
                    start.assert_not_called()
                self.assertFalse(server.RUNTIME_ACCEPTING)
                self.assertEqual(server.RECORD_BY_ID, {})
                self.assertEqual(self.storage.state, state)
        self.storage.state = good
        with patch.object(server, "MAX_CONTROL_BYTES", 1):
            with self.assertRaises(BackendError):
                self.restart()

    def test_import_only_initialization_and_missing_dependencies_fail_closed(self):
        old = self.format1()
        rec = old["records"][valid_well()["id"]]
        rec["kind"] = "local:test:Thing:1.0.0"
        rec["data"] = {"Value": 1}
        rec["legal"]["legaltags"] = ["imported"]
        imported_group = server.group_email("data.imported")
        rec["acl"]["owners"] = [imported_group]
        old["versions"][rec["id"]][0]["record"] = deepcopy(rec)
        old["fences"][rec["id"]] = [rec["kind"], 1]
        self.storage.state = deepcopy(old)
        with self.assertRaisesRegex(BackendError, "local:test:Thing:1.0.0|imported"):
            self.restart()
        self.assertEqual(self.storage.state, old)
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "private.json"
            path.write_text(json.dumps({"schemas": [self.custom_schema()],
                                        "legalTags": {"imported": server.default_legal_tag("imported")},
                                        "groups": {imported_group: {
                                            "name": "data.imported", "email": imported_group,
                                            "description": "Imported", "appIds": [],
                                            "members": {"imported@local": "OWNER"}}}}))
            with patch.dict(os.environ, {"OSDU_CORE_IMPORT_FILE": str(path)}):
                self.restart()
                state = deepcopy(self.storage.state)
                path.write_text("invalid now and must not be read")
                self.restart()
                self.assertEqual(self.storage.state, state)

    def test_backup_conflict_and_migration_save_failure_are_not_ready(self):
        old = self.format1()
        self.storage.state = deepcopy(old)
        with patch.object(self.storage, "backup_format1", side_effect=BackendError(503, "backup digest mismatch")):
            with self.assertRaises(BackendError):
                self.restart()
        self.assertFalse(server.RUNTIME_ACCEPTING)
        self.assertEqual(self.storage.state, old)
        with patch.object(self.storage, "save", side_effect=BackendError(503, "injected")):
            with self.assertRaises(BackendError):
                self.restart()
        self.assertFalse(server.RUNTIME_ACCEPTING)
        self.assertEqual(server.RECORD_BY_ID, {})
        self.assertEqual(self.storage.state, old)

    def test_snapshot_excludes_volatile_credentials_and_failed_member_batch(self):
        server.UPLOAD_TOKENS["raw-token"] = {"url": "?sig=secret"}
        server.CURSORS["cursor-secret"] = {}
        server.WORKFLOW_RUNS["workflow-secret"] = {}
        with server.STORE_LOCK, server.storage_transaction():
            server.LEGAL_TAGS["safe"] = server.default_legal_tag("safe")
        raw = json.dumps(self.storage.state)
        for secret in ("raw-token", "?sig=", "cursor-secret", "workflow-secret", server.TOKEN):
            self.assertNotIn(secret, raw)
        before = deepcopy(self.storage.state)
        email = server.ACL_OWNERS
        self.assertEqual(call("post", "/api/entitlements/v2/groups/" + email + "/members",
                              {"members": [{"email": "valid@local"}, {}]})[0], 400)
        self.assertEqual(server.storage_state(), before)

    def test_nonempty_bindings_grants_metadata_roundtrip_and_failed_commit(self):
        rid = valid_well()["id"]
        server.upsert_records([valid_well()], index_immediately=True)
        digest = hashlib.sha256(b"opaque").hexdigest()
        with server.STORE_LOCK, server.storage_transaction():
            server.RECORD_METADATA[rid] = {"tags": {"source": "override"}}
            server.UPLOAD_GRANTS[digest] = {"owner": "demo@osdu-lite.local",
                                           "stagingKey": "staging/private", "expiresAt": 2000000000}
            server.FILE_BINDINGS[rid] = {
                "finalKey": "published/private", "size": 6, "sha256": digest,
                "grantDigest": digest, "principal": "demo@osdu-lite.local",
                "recordId": rid, "metadataDigest": digest, "version": 1}
        expected = deepcopy(self.storage.state)
        self.restart()
        self.assertEqual(server.storage_state(), expected)
        with patch.object(self.storage, "save", side_effect=BackendError(503, "injected")) as save:
            with self.assertRaises(BackendError):
                with server.STORE_LOCK, server.storage_transaction():
                    server.RECORD_METADATA[rid]["tags"]["source"] = "lost"
                    server.FILE_BINDINGS[rid]["size"] = 7
                    server.UPLOAD_GRANTS[digest]["expiresAt"] = 2000000001
                    server.LEGAL_TAGS[server.LEGAL_TAG]["description"] = "lost"
                    server.GROUPS[server.ACL_OWNERS]["members"].clear()
                    server.SCHEMA_REGISTRY.register(self.custom_schema(), "POST")
            save.assert_called_once()
        self.assertEqual(server.storage_state(), expected)
        self.assertEqual(self.storage.state, expected)
        with self.assertRaises(BackendError):
            with server.STORE_LOCK, server.storage_transaction():
                server.RECORD_METADATA[rid] = {"acl": None}
        self.assertEqual(server.storage_state(), expected)

    def test_control_count_byte_limits_and_snapshot_limit_rollback(self):
        from runtime_jobs import CapacityError
        from storage_backend import MAX_SNAPSHOT_BYTES
        before = deepcopy(self.storage.state)
        for key, count in (("GROUPS", 257), ("LEGAL_TAGS", 257),
                           ("UPLOAD_GRANTS", 257), ("FILE_BINDINGS", 5001)):
            with self.subTest(key=key):
                with self.assertRaises(CapacityError) as caught:
                    with server.STORE_LOCK, server.storage_transaction():
                        getattr(server, key).update({str(n): {} for n in range(count)})
                self.assertEqual(caught.exception.status, 429)
                self.assertEqual(server.storage_state(), before)
        with patch.object(server, "MAX_CONTROL_BYTES", 1):
            with self.assertRaises(CapacityError):
                with server.STORE_LOCK, server.storage_transaction():
                    server.LEGAL_TAGS["overflow"] = server.default_legal_tag("overflow")
        self.assertEqual(server.storage_state(), before)
        with patch("storage_backend.MAX_SNAPSHOT_BYTES", 1):
            with self.assertRaises(BackendError):
                with server.STORE_LOCK, server.storage_transaction():
                    server.LEGAL_TAGS["overflow"] = server.default_legal_tag("overflow")
        self.assertEqual(server.storage_state(), before)
        self.assertEqual(MAX_SNAPSHOT_BYTES, 144 * 1024 * 1024)
        with self.assertRaises(CapacityError) as caught:
            with server.STORE_LOCK, server.storage_transaction():
                server.GROUPS[server.ACL_OWNERS]["members"] = {
                    f"member{n}@local": "MEMBER" for n in range(257)}
        self.assertEqual(caught.exception.status, 429)
        self.assertEqual(server.storage_state(), before)

    def test_registry_import_is_detached_preserves_audit_and_resolves_order(self):
        from schema_registry import SchemaRegistry, SchemaError
        catalog = SchemaRegistry()
        target = self.custom_schema()
        catalog.register(target, "POST")
        dependent = {"kind": "local:test:Dependent:1.0.0",
                     "schema": {"type": "object", "properties": {
                         "Child": {"$ref": "urn:osdu-lite:schema:" + target["kind"]}}}}
        catalog.register(dependent, "POST")
        exported = catalog.export_custom()
        clone = SchemaRegistry()
        clone.import_custom(list(reversed(exported)))
        self.assertEqual({e["kind"]: e for e in clone.export_custom()},
                         {e["kind"]: e for e in exported})
        before = clone.export_custom()
        for invalid in ([{}], [exported[0], exported[0]], [dict(target, schema={"type": "object", "$ref": "https://invalid"})]):
            with self.assertRaises(SchemaError):
                clone.import_custom(invalid)
            self.assertEqual(clone.export_custom(), before)
        catalog.register(dict(target, status="DEPRECATED"), "PUT")
        self.assertEqual(catalog.export_custom()[0]["status"], "OBSOLETE")

    def test_nested_record_history_rollback_preserves_scheduler_entry_identity(self):
        rid = valid_well()["id"]
        server.upsert_records([valid_well()], index_immediately=True)
        expected = deepcopy(self.storage.state)
        entry = server.RECORD_VERSIONS[rid][0]
        payload = entry["record"]
        with patch.object(self.storage, "save", side_effect=BackendError(503, "injected")):
            with self.assertRaises(BackendError):
                with server.STORE_LOCK, server.storage_transaction():
                    server.RECORD_BY_ID[rid]["data"]["FacilityName"] = "lost"
                    entry["bytes"] += 123
                    entry["record"]["tags"] = {"lost": "value"}
                    server.RECORD_TS[rid]["modify"] += 1
        self.assertEqual(server.storage_state(), expected)
        self.assertIs(server.RECORD_VERSIONS[rid][0], entry)
        self.assertIs(entry["record"], payload)

    def test_format2_rejects_underreported_history_and_watermark(self):
        server.upsert_records([valid_well()], index_immediately=True)
        server.upsert_records([valid_well(name="v2")], index_immediately=True)
        good = self.storage.load()
        for field in ("bytes", "watermark"):
            bad = deepcopy(good)
            if field == "bytes":
                bad["versions"][valid_well()["id"]][0]["bytes"] = 0
            else:
                bad["fences"][valid_well()["id"]][2] = 1
            self.storage.state = bad
            with self.assertRaises(BackendError):
                self.restart()
            self.assertFalse(server.RUNTIME_ACCEPTING)


class BlobBackupTests(unittest.TestCase):
    def test_create_only_backup_verifies_exact_original_digest(self):
        from unittest.mock import MagicMock
        from azure.core.exceptions import ResourceExistsError
        from storage_backend import AzureBlobStorage
        backend = AzureBlobStorage.__new__(AzureBlobStorage)
        backend.partition_hash = "partition-hash"
        backend.original_raw = b'{ "format": 1, "original": true }\n'
        backend.container = MagicMock()
        blob = backend.container.get_blob_client.return_value
        blob.download_blob.return_value.properties.size = len(backend.original_raw)
        blob.download_blob.return_value.readall.return_value = backend.original_raw
        backend.backup_format1()
        blob.upload_blob.assert_called_once()
        self.assertFalse(blob.upload_blob.call_args.kwargs["overwrite"])
        blob.upload_blob.side_effect = ResourceExistsError("exists")
        backend.backup_format1()
        blob.download_blob.return_value.readall.return_value = b"wrong bytes"
        with self.assertRaises(BackendError):
            backend.backup_format1()

    def test_duplicate_snapshot_json_keys_are_rejected(self):
        from unittest.mock import MagicMock
        backend = AzureBlobStorage.__new__(AzureBlobStorage)
        backend.container = MagicMock()
        backend.blob = MagicMock()
        raw = (b'{"format":99,"format":1,"records":{},"versions":{},"fences":{},'
               b'"timestamps":{},"deleted":[],"suppressed":[]}')
        download = backend.blob.download_blob.return_value
        download.properties.size = len(raw)
        download.readall.return_value = raw
        with self.assertRaises(BackendError):
            backend.load()


class StoragePersistenceTests(ServerTestCase):
    def setUp(self):
        super().setUp()
        self.storage = FakeStorage()
        server.start_runtime(FakeSearch(), storage=self.storage)

    def restart(self):
        server.start_runtime(FakeSearch(), storage=self.storage)

    def tearDown(self):
        server.start_runtime(FakeSearch(), storage=FakeStorage())
        super().tearDown()

    def test_restart_restores_history_timestamps_and_search_without_reseeding(self):
        record = valid_well()
        server.upsert_records([record], index_immediately=True)
        server.upsert_records([valid_well(name="updated")], index_immediately=True)
        timestamps = dict(server.RECORD_TS[record["id"]])
        with patch.object(server, "PRELOAD_FULL_SAMPLE", True):
            self.restart()
        self.assertEqual(server.RECORD_BY_ID[record["id"]]["data"]["FacilityName"], "updated")
        self.assertEqual(server.RECORD_TS[record["id"]], timestamps)
        self.assertEqual(server.record_version(record["id"], 1)["data"]["FacilityName"], "EAGLE-01")
        self.assertEqual(server.SEARCH_BACKEND.count(), 1)
        self.assertTrue(all(e["pins"] == 0 for e in server.RECORD_VERSIONS[record["id"]]))

    def test_soft_and_hard_deletes_and_generation_fences_survive_restart(self):
        soft, hard = valid_well(), valid_well("company-prod:master-data--Well:hard")
        server.upsert_records([soft, hard], index_immediately=True)
        server.delete_records([soft["id"]])
        server.delete_records([hard["id"]], hard=True)
        self.restart()
        self.assertIn(soft["id"], server.DELETED)
        self.assertNotIn(hard["id"], server.RECORD_BY_ID)
        self.assertNotIn(hard["id"], server.RECORD_VERSIONS)
        self.assertEqual(server.SEARCH_BACKEND.count(), 0)
        self.assertEqual(server.upsert_records([hard], index_immediately=True)[0]["version"], 2)
        self.assertEqual(server.RECORD_FENCES[hard["id"]][1:], (3, 2))

    def test_failed_write_leaves_cache_history_jobs_and_blob_unchanged(self):
        record = valid_well()
        server.upsert_records([record], index_immediately=True)
        before = self.storage.load()
        with patch.object(self.storage, "save", side_effect=BackendError(503, "storage_unavailable")):
            self.assertEqual(call("put", "/api/storage/v2/records", [valid_well(name="lost")])[0], 503)
            self.assertEqual(call("delete", "/api/storage/v2/records/" + record["id"])[0], 503)
            self.assertEqual(call("post", "/api/workflow/v1/workflow/Osdu_ingest/workflowRun",
                                  {"executionContext": {"manifest": {"MasterData": [valid_well()]}}})[0], 503)
        self.assertEqual(server.RECORD_BY_ID[record["id"]]["version"], 1)
        self.assertNotIn(record["id"], server.DELETED)
        self.assertEqual(len(server.RECORD_VERSIONS[record["id"]]), 1)
        self.assertEqual(server.INDEX_SCHEDULER.pending("index"), 0)
        self.assertEqual(server.WORKFLOW_RUNS, {})
        self.assertEqual(self.storage.load(), before)

    def test_purge_and_explicit_reset_persist(self):
        record = valid_well()
        for n in range(2):
            server.upsert_records([valid_well(name=str(n))], index_immediately=True)
        path = "/api/storage/v2/records/" + record["id"] + "/versions?versionIds=1"
        self.assertEqual(call("delete", path)[0], 204)
        self.restart()
        self.assertIsNone(server.record_version(record["id"], 1))
        server.clear_state()
        with patch.object(server, "PRELOAD_FULL_SAMPLE", True):
            self.restart()
        self.assertEqual(server.RECORD_BY_ID, {})
        self.assertEqual(server.SEARCH_BACKEND.count(), 0)

    def test_storage_startup_failure_does_not_publish_readiness_or_erase_data(self):
        with patch.object(self.storage, "load", side_effect=BackendError(503, "storage_unavailable")):
            with self.assertRaises(BackendError):
                self.restart()
        self.assertFalse(server.RUNTIME_ACCEPTING)
        self.assertEqual(call("put", "/api/storage/v2/records", [valid_well()])[0], 503)

    def test_restart_recovers_a_write_that_was_not_yet_indexed(self):
        with patch.object(server, "INDEX_DELAY_SECONDS", 5):
            server.upsert_records([valid_well()])
        self.restart()
        self.assertEqual(server.SEARCH_BACKEND.count(), 1)
        self.assertEqual(server.RECORD_VERSIONS[valid_well()["id"]][0]["pins"], 0)

    def test_index_kind_suppression_survives_restart_and_can_be_reindexed(self):
        server.upsert_records([valid_well()], index_immediately=True)
        server.delete_index_kind(server.WELL_KIND)
        self.restart()
        self.assertEqual(server.SEARCH_BACKEND.count(), 0)
        self.assertIn(valid_well()["id"], server.RECORD_BY_ID)
        server.index_current_records()
        self.assertEqual(server.SEARCH_BACKEND.count(), 1)

    def test_failed_reset_and_purge_leave_persisted_records_intact(self):
        for n in range(2):
            server.upsert_records([valid_well(name=str(n))], index_immediately=True)
        with patch.object(self.storage, "save", side_effect=BackendError(503, "storage_unavailable")):
            path = "/api/storage/v2/records/" + valid_well()["id"] + "/versions?versionIds=1"
            self.assertEqual(call("delete", path)[0], 503)
            with self.assertRaises(BackendError):
                server.clear_state()
        self.assertEqual(len(server.RECORD_VERSIONS[valid_well()["id"]]), 2)
        self.restart()
        self.assertEqual(server.SEARCH_BACKEND.count(), 1)
        self.assertEqual(len(server.RECORD_VERSIONS[valid_well()["id"]]), 2)


@unittest.skipUnless(os.getenv("AZURITE_TEST_CONNECTION_STRING"), "requires real Azurite")
class AzuriteIntegrationTests(StoragePersistenceTests):
    def setUp(self):
        from storage_backend import AzureBlobStorage
        ServerTestCase.setUp(self)
        self.container = "osdu-test-" + uuid.uuid4().hex
        self.storage = AzureBlobStorage(os.environ["AZURITE_TEST_CONNECTION_STRING"],
                                        server.PARTITION, container=self.container)
        server.start_runtime(FakeSearch(), storage=self.storage)

    def tearDown(self):
        super().tearDown()
        self.storage.client.delete_container(self.container)
        self.storage.client.close()

    def test_stale_writer_rejected_without_overwriting_partition(self):
        from storage_backend import AzureBlobStorage
        stale = AzureBlobStorage(os.environ["AZURITE_TEST_CONNECTION_STRING"],
                                 server.PARTITION, container=self.container)
        try:
            old = stale.load()
            server.upsert_records([valid_well()], index_immediately=True)
            with self.assertRaises(BackendError):
                stale.save(old)
            self.restart()
            self.assertIn(valid_well()["id"], server.RECORD_BY_ID)
        finally:
            stale.client.close()

    def test_reset_does_not_touch_another_partition_blob(self):
        from storage_backend import AzureBlobStorage
        other = AzureBlobStorage(os.environ["AZURITE_TEST_CONNECTION_STRING"],
                                 "another-partition", container=self.container)
        try:
            self.assertIsNone(other.load())
            expected = deepcopy(dict(server.storage_state(), records={"other": {"id": "other"}}))
            other.save(expected)
            server.clear_state()
            self.assertEqual(other.load(), expected)
        finally:
            other.client.close()

    def test_invalid_blob_fails_startup_without_reseeding(self):
        data = b"not-json"
        self.storage.blob.upload_blob(data, overwrite=True)
        with self.assertRaises(BackendError):
            self.restart()
        self.assertFalse(server.RUNTIME_ACCEPTING)
        self.assertEqual(self.storage.blob.download_blob().readall(), data)

    def test_real_format1_backup_migration_and_new_sdk_restart(self):
        server.upsert_records([valid_well()], index_immediately=True)
        state = self.storage.load()
        old = {key: state[key] for key in
               ("records", "versions", "fences", "timestamps", "deleted", "suppressed")}
        old["format"] = 1
        old["fences"] = {rid: fence[:2] for rid, fence in old["fences"].items()}
        raw = json.dumps(old, indent=2).encode() + b"\n"
        self.storage.blob.upload_blob(raw, overwrite=True)
        self.restart()
        migrated = deepcopy(server.storage_state())
        digest = hashlib.sha256(raw).hexdigest()
        backup = self.storage.container.get_blob_client(
            self.storage.partition_hash + "." + digest + ".format1.json")
        self.assertEqual(backup.download_blob().readall(), raw)
        fresh = AzureBlobStorage(os.environ["AZURITE_TEST_CONNECTION_STRING"],
                                 server.PARTITION, container=self.container)
        try:
            server.start_runtime(FakeSearch(), storage=fresh)
            self.assertEqual(server.storage_state(), migrated)
        finally:
            server.stop_runtime()
            fresh.client.close()
        # A conflicting existing backup must prevent migration and preserve format1.
        backup.upload_blob(b"wrong backup", overwrite=True)
        self.storage.blob.upload_blob(raw, overwrite=True)
        with self.assertRaises(BackendError):
            self.restart()
        self.assertFalse(server.RUNTIME_ACCEPTING)
        self.assertEqual(self.storage.blob.download_blob().readall(), raw)


if __name__ == "__main__":
    unittest.main()
