"""EPIC-009 offline migration and deployment acceptance (no service startup)."""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
from fake_file_backend import FakeFiles
from fake_storage_backend import FakeStorage
from search_backend import BackendError
from storage_backend import AzureBlobStorage
import server


class FileMigrationTests(unittest.TestCase):
    def test_cli_sanitizes_initial_and_candidate_snapshot_capacity_failures(self):
        from contextlib import redirect_stderr
        import io
        import migrate_files
        original = deepcopy(self.storage.state)
        for stage in ("initial_groups", "candidate_bytes"):
            with self.subTest(stage=stage):
                self.storage.state = deepcopy(original)
                limit = server.MAX_CONTROL_BYTES
                if stage == "initial_groups":
                    template = next(iter(self.storage.state["groups"].values()))
                    for n in range(257 - len(self.storage.state["groups"])):
                        name = "migration-overflow-" + str(n)
                        email = server.group_email(name)
                        self.storage.state["groups"][email] = {
                            **deepcopy(template), "name": name, "email": email}
                else:
                    control = {key: self.storage.state[key] for key in (
                        "schemas", "legalTags", "groups", "recordMetadata", "fileBindings", "uploadGrants")}
                    limit = len(server.snapshot_bytes(control))
                before = deepcopy(self.storage.state)
                error = io.StringIO()
                with patch.dict(os.environ, {"FILES_DIR": str(self.base)}), \
                        patch.object(server, "MAX_CONTROL_BYTES", limit), \
                        patch.object(migrate_files.AzureBlobStorage, "from_env", return_value=self.storage), \
                        patch.object(migrate_files.AzureFiles, "from_env", return_value=self.files), \
                        patch.object(self.files, "initialize", side_effect=AssertionError("preflight only")), \
                        redirect_stderr(error):
                    status = migrate_files.main(["--manifest", str(self.manifest),
                                                 "--apply", "--writer-stopped"])
                self.assertEqual(status, 1)
                self.assertEqual(json.loads(error.getvalue()), {"error": "invalid_snapshot", "orphanKeys": []})
                self.assertEqual(self.storage.state, before)
                self.assertEqual(self.storage.saves, 0)
                self.assertFalse(self.files.blobs)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.source = self.base / "user.las"
        self.source.write_bytes(b"operator-original\n")
        self.record = deepcopy(next(r for r in server.FULL_SAMPLE if r["kind"] == server.DATASET_KIND))
        self.record["version"] = 1
        old = {"format": 1, "records": {self.record["id"]: self.record},
               "versions": {self.record["id"]: [{"version": 1, "record": self.record,
                            "timestamp": 1, "bytes": 0}]},
               "timestamps": {self.record["id"]: {"createTime": 1}},
               "fences": {self.record["id"]: [self.record["kind"], 1]},
               "deleted": [], "suppressed": []}
        self.storage = FakeStorage()
        self.storage.state = server.prepare_storage(old)[0]
        self.files = FakeFiles()
        self.manifest = self.base / "manifest.json"
        self.entry = {"recordId": self.record["id"], "sourcePath": "user.las",
                      "sha256": hashlib.sha256(self.source.read_bytes()).hexdigest()}
        self.write_manifest([self.entry])

    def write_manifest(self, entries):
        self.manifest.write_text(json.dumps(entries), encoding="utf-8")

    def run_migration(self, **kwargs):
        import migrate_files
        return migrate_files.migrate(self.manifest, self.base, server.PARTITION,
                                     self.storage, self.files, **kwargs)

    def test_default_dry_run_has_no_writes_even_initialization(self):
        before = deepcopy(self.storage.state)
        with patch.object(self.files, "initialize", side_effect=AssertionError("write")), \
                patch.object(self.files, "upload", side_effect=AssertionError("write")):
            result = self.run_migration()
        self.assertEqual(result["mode"], "dry-run")
        self.assertEqual(self.storage.state, before)
        self.assertEqual(self.storage.saves, 0)
        self.assertEqual(self.files.blobs, {})

    def test_apply_requires_explicit_stopped_writer_attestation(self):
        with self.assertRaisesRegex(Exception, "writer_not_attested"):
            self.run_migration(apply=True)
        self.assertEqual(self.files.blobs, {})

    def test_apply_preserves_snapshot_sources_and_identical_retry(self):
        before = deepcopy(self.storage.state)
        result = self.run_migration(apply=True, writer_stopped=True)
        after = self.storage.state
        self.assertEqual(result["changed"], 1)
        for key in before:
            if key not in ("fileBindings", "revision"):
                self.assertEqual(after[key], before[key], key)
        binding = after["fileBindings"][self.record["id"]]
        expected = (hashlib.sha256(server.PARTITION.encode()).hexdigest() + "/final/"
                    + hashlib.sha256(self.record["id"].encode()).hexdigest() + "/" + self.entry["sha256"])
        self.assertEqual(binding["finalKey"], expected)
        self.assertEqual(self.files.blobs[expected], self.source.read_bytes())
        self.assertEqual(after["revision"], before["revision"] + 1)
        with patch.object(self.files, "initialize", side_effect=AssertionError("no-op")):
            self.assertEqual(self.run_migration(apply=True, writer_stopped=True)["changed"], 0)
        self.assertEqual(self.storage.saves, 1)
        self.assertEqual(self.source.read_bytes(), b"operator-original\n")

    def test_all_entries_preflight_before_any_writes(self):
        missing = {**self.entry, "recordId": server.PARTITION + ":dataset--File.Generic:missing"}
        self.write_manifest([self.entry, missing])
        with self.assertRaisesRegex(Exception, "missing_record"):
            self.run_migration(apply=True, writer_stopped=True)
        self.assertEqual(self.files.blobs, {})
        self.assertEqual(self.storage.saves, 0)

    def test_invalid_paths_hashes_duplicates_and_shapes(self):
        cases = [
            ({**self.entry, "sourcePath": "../user.las"}, "invalid_source_path"),
            ({**self.entry, "sourcePath": str(self.source)}, "invalid_source_path"),
            ({**self.entry, "sourcePath": "missing"}, "missing_source"),
            ({**self.entry, "sha256": "0" * 64}, "hash_mismatch"),
            ({**self.entry, "sourcePath": "sub\\..\\user.las"}, "invalid_source_path"),
            ({**self.entry, "extra": True}, "invalid_manifest"),
        ]
        for entry, reason in cases:
            with self.subTest(reason=reason):
                self.write_manifest([entry])
                with self.assertRaisesRegex(Exception, reason):
                    self.run_migration(apply=True, writer_stopped=True)
                self.assertEqual(self.files.blobs, {})
        self.write_manifest([self.entry, {**self.entry, "sha256": "0" * 64}])
        with self.assertRaisesRegex(Exception, "conflicting_duplicate"):
            self.run_migration()
        self.write_manifest([self.entry, self.entry])
        self.assertEqual(self.run_migration()["changed"], 1)

    def test_symlink_and_reparse_rejected(self):
        link = self.base / "link"
        try:
            link.symlink_to(self.source)
        except OSError:
            # Windows unprivileged runners still exercise the reparse attribute branch.
            with patch("pathlib.Path.is_symlink", return_value=True):
                with self.assertRaisesRegex(Exception, "invalid_source_path"):
                    self.run_migration()
            return
        self.write_manifest([{**self.entry, "sourcePath": "link"}])
        with self.assertRaisesRegex(Exception, "invalid_source_path"):
            self.run_migration()

    def test_not_dataset_and_invalid_snapshot_fail_closed(self):
        rid = self.record["id"]
        self.storage.state["records"][rid]["kind"] = "osdu:wks:master-data--Well:1.0.0"
        with self.assertRaisesRegex(Exception, "invalid_snapshot"):
            self.run_migration()
        self.storage.state["format"] = 1
        with self.assertRaisesRegex(Exception, "format2_required"):
            self.run_migration()
        self.storage.state = None
        with self.assertRaisesRegex(Exception, "format2_required"):
            self.run_migration()

    def test_valid_non_dataset_record_rejected(self):
        record = deepcopy(next(r for r in server.FULL_SAMPLE if r["kind"] != server.DATASET_KIND))
        record["version"] = 1
        old = {"format": 1, "records": {record["id"]: record},
               "versions": {record["id"]: [{"version": 1, "record": record, "timestamp": 1, "bytes": 0}]},
               "timestamps": {record["id"]: {"createTime": 1}},
               "fences": {record["id"]: [record["kind"], 1]}, "deleted": [], "suppressed": []}
        self.storage.state = server.prepare_storage(old)[0]
        self.write_manifest([{**self.entry, "recordId": record["id"]}])
        with self.assertRaisesRegex(Exception, "not_dataset"):
            self.run_migration()

    def test_existing_final_content_is_verified_never_overwritten(self):
        self.run_migration(apply=True, writer_stopped=True)
        key = self.storage.state["fileBindings"][self.record["id"]]["finalKey"]
        self.files.blobs[key] = b"corrupt"
        with self.assertRaisesRegex(Exception, "blob_hash_mismatch"):
            self.run_migration(apply=True, writer_stopped=True)
        self.assertEqual(self.files.blobs[key], b"corrupt")
        self.assertEqual(self.storage.saves, 1)

    def test_unbound_existing_final_corruption_preflights_before_initialization(self):
        key = (hashlib.sha256(server.PARTITION.encode()).hexdigest() + "/final/"
               + hashlib.sha256(self.record["id"].encode()).hexdigest() + "/" + self.entry["sha256"])
        self.files.blobs[key] = b"bad"
        with patch.object(self.files, "initialize", side_effect=AssertionError("must preflight")):
            with self.assertRaisesRegex(Exception, "blob_hash_mismatch"):
                self.run_migration(apply=True, writer_stopped=True)
        self.assertEqual(self.storage.saves, 0)

    def test_binding_conflict_and_unknown_controls_are_not_silently_changed(self):
        self.run_migration(apply=True, writer_stopped=True)
        binding = self.storage.state["fileBindings"][self.record["id"]]
        binding["sha256"] = "0" * 64
        with self.assertRaisesRegex(Exception, "binding_conflict"):
            self.run_migration()
        self.storage.state["unknownControl"] = {}
        with self.assertRaisesRegex(Exception, "invalid_snapshot"):
            self.run_migration()

    def test_cli_failure_redacts_backend_details(self):
        import migrate_files
        from io import StringIO
        with patch("migrate_files.AzureBlobStorage.from_env", return_value=self.storage), \
                patch("migrate_files.AzureFiles.from_env", return_value=self.files), \
                patch.object(self.storage, "save", side_effect=BackendError(503, "AccountName=secret?sig=secret")), \
                patch.dict(os.environ, {"FILES_DIR": str(self.base)}), \
                patch("sys.stderr", new_callable=StringIO) as output:
            self.assertEqual(migrate_files.main(["--manifest", str(self.manifest),
                                               "--apply", "--writer-stopped"]), 1)
        report = json.loads(output.getvalue())
        self.assertEqual(report["error"], "migration_failed")
        self.assertTrue(report["orphanKeys"])
        self.assertNotIn("secret", output.getvalue())

    def test_failed_metadata_save_has_no_visible_binding_only_private_orphan_keys(self):
        import migrate_files
        before = deepcopy(self.storage.state)
        with patch.object(self.storage, "save", side_effect=BackendError(503, "credential?sig=secret")):
            with self.assertRaises(migrate_files.MigrationError) as raised:
                self.run_migration(apply=True, writer_stopped=True)
        self.assertEqual(self.storage.state, before)
        self.assertTrue(raised.exception.orphan_keys)
        self.assertNotIn("secret", str(raised.exception))
        for key in raised.exception.orphan_keys:
            self.assertNotIn("?", key)
            self.assertNotIn("devstoreaccount1", key)
            self.assertIn(key, self.files.blobs)

    def test_source_race_does_not_commit_unverified_bytes(self):
        self.files.on_publish_chunk = lambda: self.source.write_bytes(b"changed")
        with self.assertRaisesRegex(Exception, "hash_mismatch"):
            self.run_migration(apply=True, writer_stopped=True)
        self.assertEqual(self.storage.saves, 0)
        self.assertEqual(self.storage.state["fileBindings"], {})

    def test_cli_defaults_dry_run_and_rejects_conflicting_modes(self):
        import migrate_files
        self.assertFalse(migrate_files.parser().parse_args(["--manifest", "m.json"]).apply)
        with self.assertRaises(SystemExit):
            migrate_files.parser().parse_args(["--manifest", "m.json", "--apply", "--dry-run"])

    def test_readonly_snapshot_load_never_creates_container(self):
        storage = AzureBlobStorage.__new__(AzureBlobStorage)
        storage.container = unittest.mock.Mock()
        storage.blob = unittest.mock.Mock()
        download = storage.blob.download_blob.return_value
        raw = json.dumps(self.storage.state).encode()
        download.properties.size = len(raw)
        download.properties.etag = "etag"
        download.readall.return_value = raw
        self.assertEqual(storage.load(initialize=False), self.storage.state)
        storage.container.create_container.assert_not_called()


class DeploymentConfigTests(unittest.TestCase):
    def test_three_services_persistence_limits_and_reachable_public_root(self):
        try:
            import yaml  # Installed development-only parser, never a runtime dependency.
        except ImportError:
            self.skipTest("configuration parsing requires development-only PyYAML")
        for name in ("compose.yaml", "compose.pull.yaml"):
            with self.subTest(name=name):
                config = yaml.safe_load((ROOT / name).read_text())
                services = config["services"]
                self.assertEqual(set(services), {"server", "azurite", "opensearch"})
                for name, memory in (("server", "2g"), ("azurite", "2g"), ("opensearch", "6g")):
                    self.assertEqual(services[name]["mem_limit"], memory)
                self.assertIn("azurite-data:/data", services["azurite"]["volumes"])
                self.assertIn("opensearch-data:/usr/share/opensearch/data", services["opensearch"]["volumes"])
                self.assertIn("127.0.0.1", services["azurite"]["ports"][0])
                env = services["server"]["environment"]
                self.assertIn("legacy", env["OSDU_COMPAT_PROFILE"])
                self.assertIn("localhost:10000/devstoreaccount1", env["AZURE_STORAGE_PUBLIC_BLOB_ENDPOINT"])
                self.assertIn("http://azurite:10000/devstoreaccount1", env["AZURE_STORAGE_CONNECTION_STRING"])
                self.assertIn("-Xms2g -Xmx2g", services["opensearch"]["environment"]["OPENSEARCH_JAVA_OPTS"])
        self.assertIn("${IMAGE:-", config["services"]["server"]["image"])

    def test_ci_and_image_include_migration_and_real_coverage(self):
        ci = (ROOT.parent / ".github/workflows/ci.yml").read_text()
        for text in ("test_file_migration", "test_file_backend.py", "RealCoreSearchTests", "linux/arm64"):
            self.assertIn(text, ci)
        self.assertIn("COPY migrate_files.py /app/migrate_files.py", (ROOT / "Dockerfile").read_text())


@unittest.skipUnless(os.getenv("AZURITE_TEST_CONNECTION_STRING"), "opt-in real Azurite")
class RealFileMigrationTests(unittest.TestCase):
    setUp = FileMigrationTests.setUp
    write_manifest = FileMigrationTests.write_manifest
    run_migration = FileMigrationTests.run_migration

    def real_backends(self):
        import uuid
        from file_backend import AzureFiles
        connection = os.environ["AZURITE_TEST_CONNECTION_STRING"]
        # Unique record/content keys; never remove a shared sample or the shared files container.
        state = self.storage.state
        old_id, new_id = self.record["id"], server.PARTITION + ":dataset--File.Generic:" + uuid.uuid4().hex
        for name in ("records", "versions", "timestamps", "fences"):
            state[name][new_id] = state[name].pop(old_id)
        state["records"][new_id]["id"] = new_id
        for entry in state["versions"][new_id]:
            entry["record"]["id"] = new_id
            entry["bytes"] = len(json.dumps(entry["record"], sort_keys=True, separators=(",", ":"),
                                           ensure_ascii=False).encode())
        self.record["id"] = new_id
        self.entry["recordId"] = new_id
        self.write_manifest([self.entry])
        storage = AzureBlobStorage(connection, server.PARTITION, "migration-" + uuid.uuid4().hex)
        storage.load()
        storage.save(state)
        self.addCleanup(storage.container.delete_container)
        self.storage = storage
        self.files = AzureFiles(connection, None)
        self.files.initialize()
        self._real_keys = []
        self.addCleanup(self.cleanup_blobs)
        upload, publish = self.files.upload, self.files.publish

        def tracked_upload(key, *args):
            self._real_keys.append(key)
            return upload(key, *args)

        def tracked_publish(source, final):
            self._real_keys.append(final)
            return publish(source, final)

        self.files.upload = tracked_upload
        self.files.publish = tracked_publish

    def test_real_apply_retry(self):
        self.real_backends()
        result = self.run_migration(apply=True, writer_stopped=True)
        self.assertEqual(result["changed"], 1)
        self.assertEqual(self.run_migration(apply=True, writer_stopped=True)["changed"], 0)
        state = self.storage.load(initialize=False)
        self.assertEqual(state["records"][self.record["id"]]["version"], 1)
        self.assertEqual(state["fileBindings"][self.record["id"]]["sha256"], self.entry["sha256"])

    def test_real_stale_etag_leaves_no_binding(self):
        self.real_backends()
        competitor = AzureBlobStorage(os.environ["AZURITE_TEST_CONNECTION_STRING"], server.PARTITION,
                                      self.storage.container.container_name)
        state = competitor.load(initialize=False)
        initialize = self.files.initialize

        def concurrent_commit():
            initialize()
            state["revision"] += 1
            competitor.save(state)

        with patch.object(self.files, "initialize", side_effect=concurrent_commit):
            with self.assertRaisesRegex(Exception, "migration_failed") as raised:
                self.run_migration(apply=True, writer_stopped=True)
        self.assertTrue(raised.exception.orphan_keys)
        self.assertEqual(competitor.load(initialize=False)["fileBindings"], {})

    def cleanup_blobs(self):
        # Delete only explicitly recorded keys belonging to this test, not a container/prefix scan.
        from azure.core.exceptions import ResourceNotFoundError
        for key in set(self._real_keys):
            try:
                self.files.container.get_blob_client(key).delete_blob()
            except ResourceNotFoundError:
                pass
