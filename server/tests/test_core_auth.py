"""ITEM-006: live authorization at HTTP boundaries, not just policy helpers."""
import contextlib
import io
import json
import os
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from test_server import call, server, valid_well, wait_until
from fake_search_backend import FakeSearch
from fake_storage_backend import FakeStorage
from fake_file_backend import FakeFiles


class LiveAuthorizationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_files = server.FILES_DIR
        server.FILES_DIR = Path(self.tmp.name)
        self.a = server.group_email("data.a")
        self.b = server.group_email("data.b")
        self.v = server.group_email("data.reader")
        roles = sorted(server.GROUPS_ALL | {
            "service.storage.creator", "service.storage.admin",
            "service.file.viewers", "service.file.editors",
            "service.dataset.viewers", "service.dataset.editors",
            "service.schema-service.viewers", "service.schema-service.editors",
            "users.datalake.admins", "users.datalake.ops",
        })
        self.identities = [
            {"principal": "alice", "token": "alice-secret", "groups": roles + [self.a]},
            {"principal": "bob", "token": "bob-secret", "groups": roles + [self.b]},
            {"principal": "reader", "token": "reader-secret", "groups": roles + [self.v]},
            {"principal": "limited", "token": "limited-secret",
             "groups": ["service.entitlements.user", "service.storage.viewer", self.a]},
            {"principal": "demo@osdu-lite.local", "token": server.TOKEN, "groups": roles},
        ]
        self.identity_path = Path(self.tmp.name) / "identities.json"
        self.identity_path.write_text(json.dumps(self.identities))
        self.env = patch.dict(os.environ, {"OSDU_IDENTITIES_FILE": str(self.identity_path),
                                          "OSDU_COMPAT_PROFILE": "legacy"})
        self.env.start()
        self.files = FakeFiles()
        server.start_runtime(FakeSearch(), storage=FakeStorage(), files=self.files)
        self.rec = valid_well()
        self.rec["acl"] = {"owners": [self.a], "viewers": [self.v]}
        server.authorized_upsert([self.rec], index_immediately=True, principal=server.core_auth.SYSTEM)
        changed = deepcopy(self.rec)
        changed["data"]["FacilityName"] = "new"
        server.authorized_upsert([changed], index_immediately=True, principal=server.core_auth.SYSTEM)
        self.did = "company-prod:dataset--File.Generic:PRIVATE"
        self.dataset = deepcopy(self.rec)
        grant = server.issue_file_grant(server.core_auth.Principal("alice"), 3600)
        source = grant["Location"]["FileSource"]
        self.dataset.update(id=self.did, kind=server.DATASET_KIND,
                            data={"FileName": "private.bin", "FileSource": source})
        self.files.upload(source, io.BytesIO(b"private-data"), 12)
        server.publish_file_records([self.dataset], principal=server.core_auth.Principal("alice"))

    def tearDown(self):
        server.stop_runtime()
        self.env.stop()
        server.COMPAT_PROFILE = "legacy"
        server.start_runtime(FakeSearch(), storage=FakeStorage())
        server.FILES_DIR = self.old_files
        self.tmp.cleanup()

    def request(self, method, path, body=None, who="bob"):
        return call(method, path, body, token=who + "-secret")

    def test_equal_role_foreign_direct_history_file_matrix(self):
        rid = self.rec["id"]
        paths = [
            f"/api/storage/v2/records/{rid}",
            f"/api/storage/v2/records/{rid}/1",
            f"/api/storage/v2/records/{rid}?version=1",
            f"/api/storage/v2/records/versions/{rid}",
            f"/api/file/v2/files/{self.did}",
            f"/api/file/v2/files/{self.did}/metadata",
            f"/api/file/v2/files/{self.did}/downloadURL",
            f"/api/file/v2/files/{self.did}/DownloadURL",
            f"/api/dataset/v1/getDatasetRegistry/?id={self.did}",
            f"/api/dataset/v1/getDatasetRegistry?id={self.did}",
        ]
        for path in paths:
            with self.subTest(path=path):
                self.assertEqual(self.request("get", path)[0], 404)
                self.assertEqual(self.request("get", path, who="alice")[0], 200)

    def test_foreign_lists_batches_instructions_counts_and_aggregations(self):
        for path in ("/api/storage/v2/records", "/api/storage/v2/query/records"):
            status, body = self.request("get", path)
            self.assertEqual((status, body.get("records")), (200, []))
        for path, body, key in [
            ("/api/storage/v2/query/records:batch", {"records": [self.rec["id"]]}, "records"),
            ("/api/dataset/v1/getDatasetRegistry", {"datasetRegistryIds": [self.did]}, "datasetRegistries"),
            ("/api/dataset/v1/retrievalInstructions", {"datasetRegistryIds": [self.did]}, "datasets"),
            ("/api/file/v2/delivery/getFileSignedUrl", {"srns": [self.did]}, "processed"),
        ]:
            with self.subTest(path=path):
                status, result = self.request("post", path, body)
                self.assertEqual(status, 200)
                self.assertFalse(result[key])
        self.assertEqual(self.request("get", f"/api/dataset/v1/retrievalInstructions?id={self.did}")[1]["datasets"], [])
        status, result = self.request("post", "/api/search/v2/query", {
            "kind": "*", "aggregateBy": [{"field": "kind", "aggType": "terms"}]})
        self.assertEqual((status, result["totalCount"], result["results"]), (200, 0, []))
        self.assertNotIn("private-data", str(self.request("get", "/info")[1]))
        self.assertEqual(self.request("get", "/info")[1]["storedRecords"], 0)

    def test_foreign_mutation_matrix_is_atomic(self):
        rid = self.rec["id"]
        before = deepcopy(server.storage_state())
        for method, path, body in [
            ("put", "/api/storage/v2/records", [self.rec]),
            ("patch", f"/api/storage/v2/records/{rid}", {"data": {"FacilityName": "stolen"}}),
            ("post", f"/api/records/{rid}:copy", {}),
            ("post", f"/api/records/{rid}:delete", {}),
            ("delete", f"/api/storage/v2/records/{rid}", None),
            ("delete", f"/api/storage/v2/records/versions/{rid}", None),
            ("delete", f"/api/file/v2/files/{self.did}/metadata", None),
            ("put", "/api/dataset/v1/registerDataset", {"datasetRegistries": [self.dataset]}),
            ("post", "/api/file/v2/files", [self.dataset]),
            ("post", "/api/file/v2/files/metadata", self.dataset),
            ("post", "/api/dataset/v1/revokeURL", {"datasetRegistryIds": [self.did]}),
            ("post", "/api/workflow/v1/workflow/Osdu_ingest/workflowRun",
             {"executionContext": {"manifest": {"MasterData": [self.rec]}}}),
        ]:
            with self.subTest(path=path):
                self.assertEqual(self.request(method, path, body)[0], 404)
                self.assertEqual(server.storage_state(), before)
        self.assertEqual(server.WORKFLOW_RUNS, {})

    def test_viewer_reads_but_cannot_write_or_transfer_ownership(self):
        rid = self.rec["id"]
        self.assertEqual(self.request("get", f"/api/storage/v2/records/{rid}/1", who="reader")[0], 200)
        self.assertEqual(self.request("put", "/api/storage/v2/records", [self.rec], who="reader")[0], 403)
        moved = deepcopy(self.rec)
        moved["acl"]["owners"] = [self.b]
        self.assertEqual(self.request("put", "/api/storage/v2/records", [moved], who="alice")[0], 403)
        invalid = deepcopy(self.rec)
        invalid["acl"]["owners"] = ["data.unknown@company-prod.company.com"]
        self.assertEqual(self.request("put", "/api/storage/v2/records", [invalid], who="alice")[0], 400)

    def test_revocation_and_legal_expiry_ignore_delayed_index_acl(self):
        server.index_current_records()
        status, cursor = self.request("post", "/api/search/v2/query_with_cursor",
                                      {"kind": "*", "limit": 1}, who="alice")
        self.assertEqual(status, 200)
        with server.STORE_LOCK, server.storage_transaction():
            del server.GROUPS[self.a]["members"]["alice"]
        self.assertEqual(self.request("get", f"/api/storage/v2/records/{self.rec['id']}", who="alice")[0], 404)
        self.assertEqual(self.request("post", "/api/search/v2/query", {"kind": "*"}, who="alice")[1]["totalCount"], 0)
        self.assertIn(self.request("post", "/api/search/v2/query_with_cursor",
                                   {"cursor": cursor["cursor"]}, who="alice")[0], (400, 403))
        with server.STORE_LOCK, server.storage_transaction():
            server.GROUPS[self.a]["members"]["alice"] = "MEMBER"
            server.LEGAL_TAGS[server.LEGAL_TAG]["properties"]["expirationDate"] = "not-a-date"
        self.assertEqual(self.request("get", f"/api/file/v2/files/{self.did}/downloadURL", who="alice")[0], 404)
        self.assertEqual(self.request("post", "/api/search/v2/query", {"kind": "*"}, who="alice")[1]["totalCount"], 0)

    def test_storage_and_search_cursors_recheck_live_legal_and_owner(self):
        for route, method in [("/api/storage/v2/records?limit=1", "get"),
                              ("/api/search/v2/query_with_cursor", "post")]:
            with self.subTest(route=route):
                status, result = self.request(method, route, {"kind": "*", "limit": 1} if method == "post" else None, who="alice")
                self.assertEqual(status, 200)
                cursor = result["cursor"]
                path = "/api/storage/v2/records?cursor=" + cursor if method == "get" else route
                body = {"cursor": cursor} if method == "post" else None
                self.assertEqual(self.request(method, path, body)[0], 403)
                # Wall-clock policy change without revision must still invalidate.
                with patch.object(server, "is_legal_tag_valid", return_value=False):
                    self.assertEqual(self.request(method, path, body, who="alice")[0], 400)

    def test_entitlement_mutations_require_admin_in_both_profiles(self):
        for profile in ("legacy", "core"):
            server.COMPAT_PROFILE = profile
            for method, path, body in [
                ("post", f"/api/entitlements/v2/groups/{self.a}/members", {"email": "bob"}),
                ("delete", f"/api/entitlements/v2/groups/{self.a}/members/alice", None),
                ("patch", f"/api/entitlements/v2/groups/{self.a}", {"description": "changed"}),
            ]:
                with self.subTest(profile=profile, method=method):
                    self.assertEqual(self.request(method, path, body, who="limited")[0], 403)

    def test_unknown_token_redaction_and_safe_logging(self):
        status, result = call("get", "/api/storage/v2/records", token="unknown-private")
        self.assertEqual(status, 401)
        self.assertNotIn(server.TOKEN, json.dumps(result))
        self.assertNotIn("unknown-private", json.dumps(result))
        handler = object.__new__(server.Handler)
        handler.client_address = ("127.0.0.1", 1)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            handler.log_message('"%s" %s', "PUT /upload/opaque-private?sig=sas-private HTTP/1.1", 200)
        self.assertNotIn("opaque-private", output.getvalue())
        self.assertNotIn("sas-private", output.getvalue())

    def test_core_extension_paths_do_not_bypass_acl(self):
        server.COMPAT_PROFILE = "core"
        rid = self.rec["id"]
        for method, path, body in [
            ("post", f"/api/records/{rid}:copy", {}),
            ("post", "/api/workflow/v1/workflow/Osdu_ingest/workflowRun",
             {"executionContext": {"manifest": {"MasterData": [self.rec]}}}),
            ("post", "/api/storage/v2/query/records/headers", {"records": [rid]}),
        ]:
            with self.subTest(path=path):
                status, result = self.request(method, path, body)
                if path.endswith("/headers"):
                    self.assertEqual((status, result["records"]), (200, []))
                else:
                    self.assertEqual(status, 404)

    def test_unsafe_core_startup_and_identity_file_fail_before_backend(self):
        with patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "core"}), patch.object(server, "REQUIRE_AUTH", False):
            with self.assertRaises((ValueError, server.BackendError)):
                server.start_runtime(FakeSearch(), storage=FakeStorage())
        for identities in ([self.identities[0], self.identities[0]],
                           [{"principal": "x", "token": "", "groups": []}]):
            self.identity_path.write_text(json.dumps(identities))
            with self.assertRaises((ValueError, server.BackendError)):
                server.start_runtime(FakeSearch(), storage=FakeStorage())

    def test_uploaded_bytes_are_not_public_and_upload_is_owner_bound(self):
        self.assertEqual(call("get", "/files/private.bin", token="")[0], 401)
        status, grant = self.request("get", "/api/file/v2/files/uploadURL", who="alice")
        self.assertEqual(status, 200)
        path = "/" + grant["Location"]["SignedUrl"].split("/", 3)[-1]
        self.assertEqual(self.request("put", path, who="bob")[0], 404)

    def test_identity_seeds_are_not_reapplied_after_restart(self):
        with server.STORE_LOCK, server.storage_transaction():
            del server.GROUPS[self.a]["members"]["alice"]
        storage = server.STORAGE_BACKEND
        server.start_runtime(FakeSearch(), storage=storage)
        self.assertNotIn("alice", server.GROUPS[self.a]["members"])

    def test_file_name_and_publication_cannot_steal_or_overwrite_foreign_bytes(self):
        stolen = deepcopy(self.dataset)
        stolen["id"] += "-stolen"
        stolen["acl"] = {"owners": [self.b], "viewers": []}
        # A readable path name is not authority over the bytes of another record.
        for path, body in [
            ("/api/file/v2/files/metadata", stolen),
            ("/api/file/v2/files", [stolen]),
        ]:
            with self.subTest(path=path):
                self.assertIn(self.request("post", path, body)[0], (400, 404))
        _, grant = self.request("get", "/api/file/v2/files/uploadURL")
        path = "/" + grant["Location"]["SignedUrl"].split("/", 3)[-1]
        self.assertEqual(call("put", path, body=b"overwrite", raw=True, token="bob-secret",
                              headers={"x-file-name": "private.bin"})[0], 200)
        self.assertEqual(b"".join(self.files.read(server.FILE_BINDINGS[self.did]["finalKey"])),
                         b"private-data")
        self.assertFalse((server.FILES_DIR / "private.bin").exists())

    def test_community_history_restore_replay_and_indexer_cannot_bypass(self):
        rid = self.rec["id"]
        before = deepcopy(server.storage_state())
        routes = [
            ("delete", f"/api/storage/v2/records/{rid}/versions?versionIds=1", None),
            ("post", "/api/storage/v2/query/records/headers", {"records": [rid]}),
            ("post", "/api/indexer/v2/reindex/records", {"recordIds": [rid]}),
            ("post", "/api/dataset/v1/metadataRecord/" + self.did + "/undelete", {}),
        ]
        for profile in ("legacy", "core"):
            server.COMPAT_PROFILE = profile
            for method, path, body in routes:
                with self.subTest(profile=profile, path=path):
                    status, response = self.request(method, path, body)
                    if path.endswith("/headers"):
                        self.assertEqual((status, response["records"]), (200, []))
                    else:
                        self.assertEqual(status, 404)
                    self.assertEqual(server.storage_state(), before)

    def test_workflow_runs_are_principal_owned_and_live_acl_checked(self):
        route = "/api/workflow/v1/workflow/Osdu_ingest/workflowRun"
        status, response = self.request("post", route, {
            "executionContext": {"manifest": {"MasterData": [self.rec]}}}, who="alice")
        self.assertEqual(status, 200, response)
        run = route + "/" + response["workflowRunId"]
        self.assertEqual(self.request("get", run)[0], 404)
        self.assertEqual(self.request("put", run, {})[0], 404)
        self.assertEqual(self.request("delete", run)[0], 404)
        self.assertEqual(self.request("get", run, who="alice")[0], 200)
        with server.STORE_LOCK, server.storage_transaction():
            del server.GROUPS[self.a]["members"]["alice"]
        self.assertEqual(self.request("get", run, who="alice")[0], 404)

    def test_role_name_edit_is_not_role_grant_and_core_aliases_are_legacy_only(self):
        with server.STORE_LOCK, server.storage_transaction():
            server.GROUPS[self.a]["name"] = "service.storage.admin"
        server.COMPAT_PROFILE = "core"
        self.assertEqual(self.request("delete", "/api/storage/v2/records/" + self.rec["id"],
                                      who="limited")[0], 403)
        for role, method, path, body in [
            ("service.storage.editor", "put", "/api/storage/v2/records", [self.rec]),
            ("service.search.user", "post", "/api/search/v2/query", {"kind": "*"}),
        ]:
            with self.subTest(role=role):
                with server.STORE_LOCK, server.storage_transaction():
                    for group in server.GROUPS.values():
                        group["members"].pop("limited", None)
                    server.GROUPS[server.group_email(role)]["members"]["limited"] = "MEMBER"
                self.assertEqual(self.request(method, path, body, who="limited")[0], 403)

    def test_data_policy_mutation_during_engine_query_is_retried_without_stale_counts(self):
        backend = server.SEARCH_BACKEND
        search = backend.search
        calls = []

        def revoke(payload, groups=None, owner=False):
            calls.append(1)
            result = search(payload, groups, owner)
            if len(calls) == 1:
                with server.STORE_LOCK, server.storage_transaction():
                    del server.GROUPS[self.a]["members"]["alice"]
            return result

        with patch.object(backend, "search", side_effect=revoke):
            status, response = self.request("post", "/api/search/v2/query", {"kind": "*"}, who="alice")
        self.assertEqual((status, response["totalCount"], response["results"]), (200, 0, []))
        self.assertEqual(len(calls), 2)

    def test_reset_and_replay_status_do_not_expose_foreign_records(self):
        self.assertEqual(self.request("post", "/poc/reset", {})[0], 404)
        status, response = self.request("post", "/api/storage/v2/replay",
                                       {"operation": "reindex"}, who="alice")
        self.assertEqual(status, 202)
        path = "/api/storage/v2/replay/status/" + response["replayId"]
        self.assertEqual(self.request("get", path)[0], 404)
        self.assertEqual(self.request("get", path, who="alice")[0], 200)

    def test_default_upload_source_is_not_the_upload_capability(self):
        status, response = self.request("get", "/api/file/v2/files/uploadURL", who="alice")
        self.assertEqual(status, 200)
        token = response["Location"]["SignedUrl"].rsplit("/", 1)[1]
        self.assertNotIn(token, response["Location"]["FileSource"])

    def test_direct_and_list_wire_errors_never_return_bearer_values(self):
        # A supplied path is not a reason to echo a configured identity token.
        status, response = self.request("get", "/api/storage/v2/records/alice-secret")
        self.assertEqual(status, 404)
        self.assertNotIn("alice-secret", json.dumps(response))
        invalid = deepcopy(self.rec)
        invalid["id"] = "alice-secret"
        invalid["data"]["FacilityName"] = 2
        status, response = self.request("post", "/api/workflow/v1/workflow/Osdu_ingest/workflowRun",
                                       {"executionContext": {"manifest": {"MasterData": [invalid]}}},
                                       who="alice")
        self.assertEqual(status, 400)
        self.assertNotIn("alice-secret", json.dumps(response))

    def test_workflow_and_cursor_paths_never_acquire_store_after_workflow(self):
        store = server.STORE_LOCK

        class OrderedStore:
            def __enter__(self):
                if server.WORKFLOW_LOCK._is_owned() and not store._is_owned():
                    raise AssertionError("workflow -> store inversion")
                if server.GROUPS_LOCK._is_owned() and not store._is_owned():
                    raise AssertionError("groups -> store inversion")
                store.acquire()
                return self

            def __exit__(self, *exc):
                store.release()

        route = "/api/workflow/v1/workflow/Osdu_ingest/workflowRun"
        _, response = self.request("post", route, {
            "executionContext": {"manifest": {"MasterData": [self.rec]}}}, who="alice")
        wait_until(lambda: server.WORKFLOW_RUNS[response["workflowRunId"]]["status"] == "succeeded")
        with patch.object(server, "STORE_LOCK", OrderedStore()):
            for profile in ("legacy", "core"):
                server.COMPAT_PROFILE = profile
                for path in (route, route + "/" + response["workflowRunId"],
                             route + "/" + response["workflowRunId"] + "/latestInfo",
                             "/api/storage/v2/records", "/api/entitlements/v2/groups"):
                    with self.subTest(profile=profile, path=path):
                        self.assertEqual(self.request("get", path, who="alice")[0], 200)

    def test_shipped_immutable_samples_only_are_public(self):
        with patch.object(server, "FILES_DIR", server.ROOT / "files"):
            status, _ = call("get", "/files/eagle01_gr_cali.las", token="")
            self.assertEqual(status, 200)
        self.assertEqual(call("get", "/files/private.bin", token="")[0], 401)

    def test_policy_change_immediately_before_serialization_is_fail_closed(self):
        render = server.Handler._json
        changed = []

        def revoke(handler, status, body, headers=None):
            if status == 200 and not changed:
                changed.append(True)
                with server.STORE_LOCK, server.storage_transaction():
                    del server.GROUPS[self.a]["members"]["alice"]
            return render(handler, status, body, headers)

        with patch.object(server.Handler, "_json", revoke):
            status, response = self.request("get", f"/api/file/v2/files/{self.did}/downloadURL",
                                             who="alice")
        self.assertIn(status, (404, 503))
        self.assertNotIn("signedUrl", response)

    def test_delayed_index_work_does_not_restore_revoked_viewer_or_historical_acl(self):
        changed = deepcopy(self.rec)
        changed["acl"]["viewers"] = []
        with patch.object(server, "INDEX_DELAY_SECONDS", 5):
            self.assertEqual(self.request("put", "/api/storage/v2/records", [changed], who="alice")[0], 200)
            # The engine still contains the old viewer ACL while the admitted version is queued.
            self.assertIn(self.v, server.SEARCH_BACKEND.documents[self.rec["id"]]["acl"]["viewers"])
            self.assertEqual(self.request("post", "/api/search/v2/query",
                                          {"kind": server.WELL_KIND}, who="reader")[1]["totalCount"], 0)
            self.assertEqual(self.request("get", "/api/storage/v2/records/" + self.rec["id"] + "/1",
                                          who="reader")[0], 404)

    def test_failed_membership_commit_restores_live_authority_and_snapshot_has_no_credentials(self):
        before = deepcopy(server.storage_state())
        with patch.object(server.STORAGE_BACKEND, "save", side_effect=server.BackendError(503, "unavailable")):
            path = f"/api/entitlements/v2/groups/{self.a}/members/alice"
            self.assertEqual(self.request("delete", path, who="bob")[0], 503)
        self.assertEqual(server.storage_state(), before)
        self.assertEqual(self.request("get", "/api/storage/v2/records/" + self.rec["id"], who="alice")[0], 200)
        serialized = json.dumps(server.STORAGE_BACKEND.state)
        for identity in self.identities:
            self.assertNotIn(identity["token"], serialized)
        self.assertNotIn("Bearer ", serialized)

    def test_core_storage_denies_foreign_reads_and_unimplemented_services_stay_closed(self):
        server.COMPAT_PROFILE = "core"
        self.assertEqual(self.request("get", "/files/private.bin", who="limited")[0], 403)
        for method, path, body, expected in [
            ("get", "/api/storage/v2/records/" + self.rec["id"], None, 404),
            ("get", f"/api/file/v2/files/{self.did}/downloadURL", None, 404),
            ("post", "/api/search/v2/query", {"kind": "*"}, 200),
            ("post", "/api/dataset/v1/retrievalInstructions", {"datasetRegistryIds": [self.did]}, 200),
        ]:
            with self.subTest(path=path):
                self.assertEqual(self.request(method, path, body)[0], expected)
                self.assertEqual(call(method, path, body, token="unknown")[0], 401)
                self.assertEqual(call(method, path, body, token="alice-secret", partition="wrong")[0], 400)

    def test_core_storage_multi_principal_read_matrix(self):
        server.COMPAT_PROFILE = "core"
        rid = self.rec["id"]
        foreign = deepcopy(self.rec)
        foreign.update(id=rid + "-bob", acl={"owners": [self.b], "viewers": []})
        self.assertEqual(self.request("put", "/api/storage/v2/records", [foreign])[0], 201)
        for who, visible in (("alice", {rid, self.did}), ("reader", {rid, self.did}),
                             ("bob", {foreign["id"]})):
            with self.subTest(who=who):
                for suffix in ("", "/1", "/2"):
                    self.assertEqual(self.request("get", "/api/storage/v2/records/" + rid + suffix,
                                                  who=who)[0], 404 if who == "bob" else 200)
                status, versions = self.request("get", "/api/storage/v2/records/versions/" + rid,
                                                 who=who)
                self.assertEqual(status, 404 if who == "bob" else 200)
                if status == 200:
                    self.assertEqual(versions["versions"], [1, 2])
                status, listing = self.request("get", "/api/storage/v2/records", who=who)
                self.assertEqual(status, 200, listing)
                self.assertEqual({r["id"] for r in listing["results"]}, visible)
                self.assertIsNone(listing["cursor"])
                status, batch = self.request("post", "/api/storage/v2/query/records:batch",
                                             {"records": [foreign["id"], rid, self.did]}, who=who)
                self.assertEqual(status, 200)
                self.assertEqual([r["id"] for r in batch["records"]],
                                 [i for i in [foreign["id"], rid, self.did] if i in visible])

    def test_core_metadata_patch_current_policy_and_owner_viewer_outcomes(self):
        server.COMPAT_PROFILE = "core"
        rid = self.rec["id"]
        body = {"query": {"ids": [rid]}, "ops": [
            {"op": "add", "path": "/tags", "value": {"auth": "checked"}}]}
        before = deepcopy(server.storage_state())
        for who, field in (("bob", "notFoundRecordIds"), ("reader", "unAuthorizedRecordIds")):
            status, response = self.request("patch", "/api/storage/v2/records", body, who=who)
            self.assertEqual(status, 206, response)
            self.assertEqual(response[field], [rid])
            self.assertEqual(response["recordCount"], 0)
            self.assertEqual(server.storage_state(), before)
        status, response = self.request("patch", "/api/storage/v2/records", body, who="alice")
        self.assertEqual((status, response["recordCount"]), (200, 1), response)
        remove_viewer = {"query": {"ids": [rid]}, "ops": [
            {"op": "replace", "path": "/acl/viewers", "value": []}]}
        self.assertEqual(self.request("patch", "/api/storage/v2/records",
                                      remove_viewer, who="alice")[0], 200)
        for version in (1, 2):
            path = f"/api/storage/v2/records/{rid}/{version}"
            self.assertEqual(self.request("get", path, who="reader")[0], 404)
            status, record = self.request("get", path, who="alice")
            self.assertEqual((status, record["version"], record["tags"]),
                             (200, version, {"auth": "checked"}))

    def test_core_copy_and_workflow_admissions_use_request_principal(self):
        server.COMPAT_PROFILE = "core"
        rid = self.rec["id"]
        copy_path = f"/api/records/{rid}:copy"
        route = "/api/workflow/v1/workflow/Osdu_ingest/workflowRun"
        payload = {"executionContext": {"manifest": {"MasterData": [self.rec]}}}
        before = deepcopy(server.storage_state())
        for who, expected in (("bob", 404), ("reader", 403)):
            for path, body in ((copy_path, {}), (route, payload)):
                with self.subTest(who=who, path=path):
                    self.assertEqual(self.request("post", path, body, who=who)[0], expected)
                    self.assertEqual(server.storage_state(), before)
                    self.assertEqual(server.WORKFLOW_RUNS, {})
        self.assertEqual(self.request("post", copy_path, who="alice")[0], 200)
        copied = [r for key, r in server.RECORD_BY_ID.items() if key.startswith(rid + "_copy")]
        self.assertEqual(len(copied), 1)
        self.assertEqual(copied[0]["createUser"], "alice")
        self.assertEqual(self.request("get", "/api/storage/v2/records/" + copied[0]["id"])[0], 404)
        status, response = self.request("post", route, payload, who="alice")
        self.assertEqual(status, 200, response)
        run_id = response["workflowRunId"]
        self.assertTrue(wait_until(lambda: server.WORKFLOW_RUNS[run_id]["status"] == "succeeded"))
        self.assertEqual(server.RECORD_BY_ID[rid]["modifyUser"], "alice")
        self.assertEqual(self.request("get", route + "/" + run_id)[0], 404)

    def test_core_revocation_and_expiry_with_stale_engine_state(self):
        server.COMPAT_PROFILE = "core"
        rid = self.rec["id"]
        changed = deepcopy(self.rec)
        changed["data"]["FacilityName"] = "queued"
        with patch.object(server, "INDEX_DELAY_SECONDS", 60):
            self.assertEqual(self.request("put", "/api/storage/v2/records", [changed], who="alice")[0], 201)
            self.assertEqual(server.SEARCH_BACKEND.documents[rid]["data"]["FacilityName"], "new")
            with server.STORE_LOCK, server.storage_transaction():
                del server.GROUPS[self.a]["members"]["alice"]
            for who in ("alice", "reader"):
                if who == "reader":
                    with server.STORE_LOCK, server.storage_transaction():
                        server.LEGAL_TAGS[server.LEGAL_TAG]["properties"]["expirationDate"] = "2000-01-01"
                for suffix in ("", "/1", "/2", "/3"):
                    self.assertEqual(self.request("get", "/api/storage/v2/records/" + rid + suffix,
                                                  who=who)[0], 404)
                self.assertEqual(self.request("get", "/api/storage/v2/records", who=who)[1]["results"], [])
                self.assertEqual(self.request("post", "/api/storage/v2/query/records:batch",
                                              {"records": [rid]}, who=who)[1]["records"], [])
                self.assertEqual(self.request("post", f"/api/records/{rid}:copy", who=who)[0], 404)
            self.assertIn(self.a, server.SEARCH_BACKEND.documents[rid]["acl"]["owners"])

    def test_core_service_roles_are_live_and_legacy_aliases_do_not_grant(self):
        rid = self.rec["id"]
        with server.STORE_LOCK, server.storage_transaction():
            for group in server.GROUPS.values():
                group["members"].pop("limited", None)
            for name in (self.a, server.group_email("service.storage.editor")):
                server.GROUPS[name]["members"]["limited"] = "MEMBER"
        server.COMPAT_PROFILE = "legacy"
        self.assertEqual(self.request("put", "/api/storage/v2/records", [self.rec], who="limited")[0], 200)
        server.COMPAT_PROFILE = "core"
        self.assertEqual(self.request("put", "/api/storage/v2/records", [self.rec], who="limited")[0], 403)
        role = server.group_email("service.storage.creator")
        with server.STORE_LOCK, server.storage_transaction():
            server.GROUPS[role]["members"]["limited"] = "MEMBER"
        self.assertEqual(self.request("put", "/api/storage/v2/records", [self.rec], who="limited")[0], 201)
        with server.STORE_LOCK, server.storage_transaction():
            del server.GROUPS[role]["members"]["limited"]
        self.assertEqual(self.request("get", "/api/storage/v2/records/" + rid, who="limited")[0], 403)

    def test_core_gated_services_enforce_roles_without_claiming_business_success(self):
        server.COMPAT_PROFILE = "core"
        server.index_current_records()  # Make the owner's file visible before checking Search policy.
        server.FILE_BACKEND = None  # Explicit unavailable-backend case, not normal runtime.
        for method, path, body in [
            ("get", f"/api/file/v2/files/{self.did}/downloadURL", None),
            ("get", "/api/file/v2/files/uploadURL", None),
            ("post", "/api/dataset/v1/retrievalInstructions", {"datasetRegistryIds": [self.did]}),
            ("post", "/api/dataset/v1/storageInstructions", {}),
            ("post", "/api/search/v2/query", {"kind": "*"}),
        ]:
            with self.subTest(path=path):
                self.assertEqual(self.request(method, path, body, who="limited")[0], 403)
                status, response = self.request(method, path, body, who="alice")
                self.assertEqual(status, 200 if "/search/" in path else 503, response)
                self.assertNotIn("SignedUrl", json.dumps(response))
                if "/search/" in path:
                    self.assertEqual(response["totalCount"], 2)
                    self.assertEqual({r["id"] for r in response["results"]}, {self.rec["id"], self.did})
                    self.assertTrue(all(self.a in r["acl"]["owners"] for r in response["results"]))
                    status, foreign = self.request(method, path, body, who="bob")
                    self.assertEqual((status, foreign["totalCount"], foreign["results"]), (200, 0, []))
                    self.assertNotIn("private.bin", json.dumps(foreign))
                else:
                    self.assertNotIn("private.bin", json.dumps(response))

    def test_core_search_counts_terms_owner_and_reference_cursors_use_live_shared_policy(self):
        server.COMPAT_PROFILE = "core"
        server.index_current_records()
        route = "/api/search/v2/query"
        for who, total, owner_total in (("alice", 2, 2), ("bob", 0, 0), ("reader", 2, 0)):
            status, response = self.request("post", route, {
                "kind": "*", "limit": 0, "aggregateBy": "kind", "trackTotalCount": False}, who=who)
            self.assertEqual((status, response["totalCount"], response["results"]), (200, total, []))
            self.assertEqual(sum(b["count"] for b in response["aggregations"]), total)
            self.assertEqual(self.request("post", route, {
                "kind": "*", "queryAsOwner": True}, who=who)[1]["totalCount"], owner_total)
        cursor_route = route + "_with_cursor"
        status, response = self.request("post", cursor_route, {"kind": "*", "limit": 1}, who="alice")
        self.assertEqual(status, 200)
        token = response["cursor"]
        with server.STORE_LOCK, server.storage_transaction():
            del server.GROUPS[self.a]["members"]["alice"]
        self.assertEqual(self.request("post", cursor_route, {"cursor": token}, who="alice")[0], 400)
        self.assertEqual(self.request("post", route, {"kind": "*"}, who="alice")[1]["totalCount"], 0)
        with server.STORE_LOCK, server.storage_transaction():
            server.GROUPS[self.a]["members"]["alice"] = "MEMBER"
        token = self.request("post", cursor_route, {"kind": "*", "limit": 1}, who="alice")[1]["cursor"]
        with patch.object(server, "is_legal_tag_valid", return_value=False):
            self.assertEqual(self.request("post", cursor_route, {"cursor": token}, who="alice")[0], 400)
            response = self.request("post", route, {"kind": "*", "aggregateBy": "kind"}, who="alice")[1]
            self.assertEqual((response["totalCount"], response["results"], response["aggregations"]), (0, [], []))

    def test_core_published_files_enforce_live_principals_viewers_and_legal(self):
        import io
        from fake_file_backend import FakeFiles
        server.COMPAT_PROFILE = "core"
        files = FakeFiles()
        with patch.object(server, "FILE_BACKEND", files):
            status, grant = self.request("get", "/api/file/v2/files/uploadURL", who="alice")
            self.assertEqual(status, 200, grant)
            source = grant["Location"]["FileSource"]
            files.upload(source, io.BytesIO(b"private"), 7)
            record = deepcopy(self.dataset)
            record["id"] += "-bound"
            record["data"] = {"FileName": "same.bin", "FileSource": source}
            self.assertEqual(self.request("post", "/api/file/v2/files/metadata", record, who="bob")[0], 400)
            self.assertEqual(self.request("post", "/api/file/v2/files/metadata", record, who="alice")[0], 201)
            path = f'/api/file/v2/files/{record["id"]}/downloadURL'
            self.assertEqual(self.request("get", path, who="bob")[0], 404)
            self.assertEqual(self.request("get", path, who="reader")[0], 200)
            for who, count in (("alice", 1), ("reader", 1), ("bob", 0)):
                status, response = self.request("post", "/api/dataset/v1/retrievalInstructions",
                    {"datasetRegistryIds": [record["id"]]}, who=who)
                self.assertEqual((status, len(response["datasets"])), (200, count))
                if count:
                    self.assertIn("signedUrl", response["datasets"][0]["retrievalProperties"])
            self.assertEqual(self.request("delete", f'/api/file/v2/files/{record["id"]}/metadata', who="reader")[0], 403)
            with server.STORE_LOCK, server.storage_transaction():
                del server.GROUPS[self.v]["members"]["reader"]
            self.assertEqual(self.request("get", path, who="reader")[0], 404)
            with server.STORE_LOCK, server.storage_transaction():
                server.LEGAL_TAGS[server.LEGAL_TAG]["properties"]["expirationDate"] = "2000-01-01"
            self.assertEqual(self.request("get", path, who="alice")[0], 404)
            self.assertEqual(self.request("post", "/api/dataset/v1/retrievalInstructions",
                {"datasetRegistryIds": [record["id"]]}, who="alice")[1], {"datasets": []})

    def test_record_helpers_require_explicit_unforgeable_system_context(self):
        with self.assertRaises(TypeError):
            server.authorized_upsert([self.rec])
        forged = server.core_auth.Principal(server.core_auth.SYSTEM.name)
        self.assertIsNone(server.read_record(self.rec["id"], principal=forged))
        self.assertIsNotNone(server.read_record(self.rec["id"], principal=server.core_auth.SYSTEM))

    def test_core_invalid_resulting_policy_never_changes_authority(self):
        server.COMPAT_PROFILE = "core"
        rid = self.rec["id"]
        before = deepcopy(server.storage_state())
        for path, value in (("/acl/owners", [self.b]), ("/legal/legaltags", ["unknown-legal"])):
            body = {"query": {"ids": [rid]}, "ops": [
                {"op": "replace", "path": path, "value": value}]}
            status, response = self.request("patch", "/api/storage/v2/records", body, who="alice")
            self.assertEqual((status, response["failedRecordIds"]), (206, [rid]), response)
            self.assertEqual(response["recordCount"], 0)
            self.assertEqual(server.storage_state(), before)
        with server.STORE_LOCK, server.storage_transaction():
            server.LEGAL_TAGS[server.LEGAL_TAG]["properties"]["expirationDate"] = "2000-01-01"
        expired = deepcopy(server.storage_state())
        new = deepcopy(self.rec)
        new["id"] += "-expired"
        for path, body in (
            ("/api/storage/v2/records", [new]),
            ("/api/workflow/v1/workflow/Osdu_ingest/workflowRun",
             {"executionContext": {"manifest": {"MasterData": [new]}}}),
        ):
            method = "put" if path.endswith("/records") else "post"
            self.assertEqual(self.request(method, path, body, who="alice")[0], 400)
            self.assertEqual(server.storage_state(), expired)
            self.assertEqual(server.WORKFLOW_RUNS, {})
