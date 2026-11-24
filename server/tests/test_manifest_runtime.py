"""EPIC-004: atomic manifests and acknowledgement-owned volatile runs."""
import threading
import time
import json
from pathlib import Path
from unittest.mock import patch

from test_server import ServerTestCase, call, server, valid_well, wait_until
from runtime_jobs import Outcome


ROUTE = "/api/workflow/v1/workflow/Osdu_ingest/workflowRun"


def manifest(records):
    template = valid_well()
    return {"executionContext": {"acl": template["acl"], "legal": template["legal"],
                                 "manifest": {"MasterData": records}}}


class ManifestRuntimeTests(ServerTestCase):
    def submit(self, records):
        status, response = call("post", ROUTE, manifest(records))
        self.assertEqual(status, 200, response)
        self.assertEqual(response["status"], "submitted")
        return response["workflowRunId"]

    def poll(self, run_id):
        status, response = call("get", ROUTE + "/" + run_id)
        self.assertEqual(status, 200)
        self.assertEqual(response["recordCount"], response["indexedCount"] +
                         response["failedCount"] + response["pendingCount"])
        return response

    def test_inherited_context_and_acknowledged_envelope(self):
        record = valid_well()
        del record["acl"], record["legal"]
        run_id = self.submit([record])
        self.assertTrue(wait_until(lambda: self.poll(run_id)["status"] == "succeeded"))
        run = self.poll(run_id)
        self.assertEqual(run["records"], [{"id": record["id"], "version": 1, "status": "indexed"}])
        self.assertEqual(run["indexedCount"], 1)
        self.assertLessEqual(run["createdAt"], run["startedAt"])
        self.assertLessEqual(run["startedAt"], run["completedAt"])

    def test_repository_manifest_metadata_and_all_sections(self):
        root = Path(__file__).resolve().parents[2]
        payload = json.loads((root / "examples/well360/samples/manifest_example.json").read_text())
        status, response = call("post", ROUTE, payload)
        self.assertEqual(status, 200, response)
        run_id = response["workflowRunId"]
        self.assertEqual(response["recordCount"], 3)
        self.assertTrue(wait_until(lambda: self.poll(run_id)["status"] == "succeeded"))

    def test_invalid_last_item_and_capacity_are_atomic(self):
        broken = valid_well("broken")
        broken["data"]["FacilityName"] = 12
        status, response = call("post", ROUTE, manifest([valid_well(), broken]))
        self.assertEqual(status, 400)
        self.assertIn("MasterData", response["message"])
        self.assertIn("1", response["message"])
        self.assertEqual(response["errors"][0]["section"], "MasterData")
        self.assertEqual(server.WORKFLOW_RUNS, {})
        self.assertEqual(server.RECORD_BY_ID, {})
        reservation = server.INDEX_SCHEDULER.reserve("index", 1000)
        try:
            self.assertEqual(call("post", ROUTE, manifest([valid_well()]))[0], 429)
            self.assertEqual(server.WORKFLOW_RUNS, {})
            self.assertEqual(server.RECORD_BY_ID, {})
        finally:
            reservation.cancel()

    def test_bad_sections_and_context_are_rejected_without_mutation(self):
        for body in ({}, {"executionContext": []},
                     manifest([]), manifest([valid_well()] * 101),
                     {"executionContext": {"manifest": {"MasterData": {}}}},
                     {"executionContext": {"manifest": {"Data": []}}},
                     {"executionContext": {"manifest": {"MasterData": [valid_well()], "Surprise": [1]}}},
                     {"executionContext": {"manifest": {"Data": {"Surprise": [1]}}}}):
            with self.subTest(body=body):
                self.assertEqual(call("post", ROUTE, body)[0], 400)
                self.assertEqual(server.WORKFLOW_RUNS, {})
                self.assertEqual(server.RECORD_BY_ID, {})

    def test_unsupported_workflow_is_inspectable_but_not_executable(self):
        call("post", "/api/workflow/v1/workflow", {"workflowName": "custom"})
        self.assertEqual(call("post", ROUTE.replace("Osdu_ingest", "custom"), {})[0], 501)
        self.assertEqual(server.WORKFLOW_RUNS, {})

    def test_blocked_index_polling_never_succeeds_and_deadline_is_autonomous(self):
        entered, release = threading.Event(), threading.Event()
        def blocked(records):
            entered.set()
            release.wait(3)
            return [Outcome()]
        try:
            with patch.object(server, "MANIFEST_DEADLINE_SECONDS", .15), \
                    patch.object(server.SEARCH_BACKEND, "bulk", side_effect=blocked):
                run_id = self.submit([valid_well()])
                self.assertTrue(entered.wait(1))
                for _ in range(3):
                    self.assertEqual(self.poll(run_id)["status"], "running")
                # Do not poll to drive the deadline.
                time.sleep(.3)
                self.assertEqual(server.WORKFLOW_RUNS[run_id]["status"], "failed")
                terminal = self.poll(run_id)
                self.assertEqual(terminal["records"][0]["errorCode"], "deadline")
                release.set()
                self.assertTrue(wait_until(lambda: server.INDEX_SCHEDULER.pending("index") == 0))
                self.assertEqual(self.poll(run_id), terminal)
        finally:
            release.set()

    def test_mixed_ack_failure_waits_for_every_item(self):
        release, failed = threading.Event(), threading.Event()
        def execute(job):
            if job.key == "bad":
                failed.set()
                return Outcome(False, False, "mapping_error")
            release.wait(3)
            return Outcome()
        try:
            with patch.dict(server.INDEX_SCHEDULER.execute, {"index": execute}):
                run_id = self.submit([valid_well("ok"), valid_well("bad")])
                self.assertTrue(failed.wait(1))
                self.assertTrue(wait_until(lambda: self.poll(run_id)["failedCount"] == 1))
                self.assertEqual(self.poll(run_id)["pendingCount"], 1)
                self.assertEqual(self.poll(run_id)["status"], "running")
                release.set()
                self.assertTrue(wait_until(lambda: self.poll(run_id)["pendingCount"] == 0))
        finally:
            release.set()
        run = self.poll(run_id)
        self.assertEqual((run["status"], run["indexedCount"], run["failedCount"]), ("failed", 1, 1))

    def test_superseded_version_fails_run(self):
        server.INDEX_DELAY_SECONDS = .1
        run_id = self.submit([valid_well()])
        server.upsert_records([valid_well(name="replacement")])
        self.assertTrue(wait_until(lambda: self.poll(run_id)["status"] == "failed"))
        self.assertEqual(self.poll(run_id)["records"][0]["errorCode"], "superseded")

    def test_retry_exhaustion_and_retry_success(self):
        clock = [0.]
        scheduler = server.INDEX_SCHEDULER
        with patch.object(scheduler, "clock", side_effect=lambda: clock[0]), \
                patch.dict(scheduler.execute, {"index": lambda job: Outcome(
                    job.key == "retry-ok" and job.attempts == 2, True, "engine_unavailable")}):
            run_id = self.submit([valid_well("retry-ok"), valid_well("retry-bad")])
            for moment in (0, 1, 3, 7):
                clock[0] = moment
                while scheduler.run_ready():
                    pass
            run = self.poll(run_id)
        self.assertEqual((run["status"], run["indexedCount"], run["failedCount"]), ("failed", 1, 1))

    def test_bounded_registry_rejects_active_and_evicts_terminal(self):
        server.INDEX_DELAY_SECONDS = 5
        with patch.object(server, "MAX_WORKFLOW_RUNS", 2):
            first = self.submit([valid_well("one")])
            self.submit([valid_well("two")])
            self.assertEqual(call("post", ROUTE, manifest([valid_well("three")]))[0], 429)
            self.assertNotIn("three", server.RECORD_BY_ID)
            with server.WORKFLOW_LOCK:
                server.WORKFLOW_RUNS[first]["status"] = "failed"
                server.WORKFLOW_RUNS[first]["completedAt"] = time.time()
            self.submit([valid_well("three")])
            self.assertNotIn(first, server.WORKFLOW_RUNS)
            self.assertEqual(len(server.WORKFLOW_RUNS), 2)

    def test_status_update_cannot_forge_completion(self):
        server.INDEX_DELAY_SECONDS = 5
        run_id = self.submit([valid_well()])
        self.assertEqual(call("put", ROUTE + "/" + run_id, {"status": "succeeded"})[0], 409)

    def test_deadline_cancels_unsent_work_and_releases_capacity_and_pins(self):
        server.INDEX_DELAY_SECONDS = 5
        with patch.object(server, "MANIFEST_DEADLINE_SECONDS", .05), \
                patch.object(server.SEARCH_BACKEND, "bulk") as bulk:
            run_id = self.submit([valid_well()])
            self.assertTrue(wait_until(lambda: server.INDEX_SCHEDULER.pending("index") == 0))
            bulk.assert_not_called()
        self.assertEqual(self.poll(run_id)["status"], "failed")
        self.assertEqual(server.RECORD_VERSIONS[valid_well()["id"]][0]["pins"], 0)

    def test_terminal_ttl_eviction_and_bounded_diagnostics(self):
        with patch.dict(server.INDEX_SCHEDULER.execute,
                        {"index": lambda job: Outcome(False, False, "mapping_error")}):
            run_id = self.submit([valid_well()])
            self.assertTrue(wait_until(lambda: self.poll(run_id)["status"] == "failed"))
        with server.WORKFLOW_LOCK:
            server.WORKFLOW_RUNS[run_id]["completedAt"] = time.time() - 601
        server.sweep_workflow_runs()
        self.assertEqual(call("get", ROUTE + "/" + run_id)[0], 404)
        self.assertNotIn(run_id, server.WORKFLOW_DEADLINES)
        status, response = call("post", ROUTE, manifest([{} for _ in range(100)]))
        self.assertEqual(status, 400)
        self.assertLessEqual(len(response["errors"]), 20)
