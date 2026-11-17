"""Storage/indexer acceptance checks against an explicit isolated fake."""
from unittest.mock import patch
from test_server import ServerTestCase, call, server, valid_well, wait_until
from search_backend import BackendError
from search_backend import OpenSearch
import json
import unittest


class BulkOutcomeTests(unittest.TestCase):
    def test_partial_outcomes_and_external_generation(self):
        backend = OpenSearch("http://localhost:9200", "test")
        records = [dict(id=str(n), kind="k", version=n + 1) for n in range(3)]
        response = {"items": [{"index": {"status": status}} for status in (201, 429, 400)]}
        with patch.object(backend, "request", return_value=response) as request:
            outcomes = backend.bulk(records)
        self.assertEqual([(o.success, o.retryable) for o in outcomes],
                         [(True, False), (False, True), (False, False)])
        lines = request.call_args.args[2].splitlines()
        for n in range(3):
            self.assertEqual(json.loads(lines[n * 2])["index"]["version"], n + 1)
            self.assertEqual(json.loads(lines[n * 2])["index"]["version_type"], "external_gte")

    def test_versioned_delete_is_a_tombstone_not_engine_delete(self):
        backend = OpenSearch("http://localhost:9200", "test")
        with patch.object(backend, "bulk", return_value=[]) as bulk:
            backend.tombstone("a", "kind", 4)
        self.assertEqual(bulk.call_args.args[0],
                         [{"id": "a", "kind": "kind", "version": 4, "_deleted": True}])

    def test_startup_cleanup_only_owned_prefix_and_legacy(self):
        backend = OpenSearch("http://localhost:9200", "test")
        legacy = backend.index
        with patch.object(backend, "request", return_value={}) as request, \
                patch.object(backend, "provision") as provision:
            backend.start_epoch("abc123")
        self.assertEqual(request.call_args_list[0].args, ("DELETE", "/" + legacy + "-*"))
        self.assertEqual(request.call_args_list[1].args, ("DELETE", "/" + legacy))
        self.assertEqual(backend.index, legacy + "-abc123")
        provision.assert_called_once()


class IndexingBackendTests(ServerTestCase):
    def test_invalid_last_record_leaves_batch_unmodified(self):
        broken = valid_well("broken")
        broken["data"]["FacilityName"] = 123
        self.assertEqual(call("post", "/api/storage/v2/records", [valid_well(), broken])[0], 400)
        self.assertEqual(server.RECORD_BY_ID, {})
        self.assertEqual(server.RECORD_VERSIONS, {})

    def test_duplicate_ids_and_cross_kind_reuse_are_rejected(self):
        self.assertEqual(call("post", "/api/storage/v2/records", [valid_well(), valid_well()])[0], 400)
        server.upsert_records([valid_well()], index_immediately=True)
        other = valid_well()
        other["kind"], other["data"] = server.WELLBORE_KIND, {}
        self.assertEqual(call("post", "/api/storage/v2/records", [other])[0], 409)

    def test_pinned_fourth_version_rejects_without_mutation(self):
        import threading
        entered, release = threading.Event(), threading.Event()
        original = server.SEARCH_BACKEND.bulk
        def blocked(records):
            entered.set()
            release.wait(3)
            return original(records)
        try:
            with patch.object(server.SEARCH_BACKEND, "bulk", side_effect=blocked):
                server.upsert_records([valid_well()])
                self.assertTrue(entered.wait(1))
                server.upsert_records([valid_well(name="v2")])
                server.upsert_records([valid_well(name="v3")])
                self.assertEqual(call("post", "/api/storage/v2/records", [valid_well(name="v4")])[0], 429)
                self.assertEqual(server.RECORD_BY_ID[valid_well()["id"]]["version"], 3)
        finally:
            release.set()

    def test_byte_and_job_reservations_reject_atomically(self):
        with patch.object(server, "MAX_RECORD_BYTES", 1):
            self.assertEqual(call("post", "/api/storage/v2/records", [valid_well()])[0], 429)
        self.assertEqual(server.RECORD_BY_ID, {})
        reservation = server.INDEX_SCHEDULER.reserve("index", 1000)
        try:
            self.assertEqual(call("post", "/api/storage/v2/records", [valid_well()])[0], 429)
            self.assertEqual(server.RECORD_BY_ID, {})
        finally:
            reservation.cancel()

    def test_hard_delete_recreate_advances_generation_and_retains_kind(self):
        record = valid_well()
        server.upsert_records([record], index_immediately=True)
        self.assertEqual(call("delete", "/api/storage/v2/records/" + record["id"] + "?hardDelete=true")[0], 204)
        other = dict(record, kind=server.WELLBORE_KIND, data={})
        self.assertEqual(call("post", "/api/storage/v2/records", [other])[0], 409)
        result = server.upsert_records([record], index_immediately=True)
        self.assertEqual(result[0]["version"], 3)

    def test_delete_timeout_is_pending_and_canonical_deleted(self):
        record = valid_well()
        server.upsert_records([record], index_immediately=True)
        with patch.object(server, "DELETE_ACK_TIMEOUT", .01), \
                patch.object(server.SEARCH_BACKEND, "bulk", side_effect=BackendError(503, "offline")):
            status, body = call("delete", "/api/storage/v2/records/" + record["id"])
        self.assertEqual(status, 503)
        self.assertIn("deletion_pending", body["message"])
        self.assertIn(record["id"], server.DELETED)
        # Fresh searches must hide the canonical deletion even before marker ACK.
        self.assertEqual(call("post", "/api/search/v2/query", {"kind": "*"})[1]["totalCount"], 0)

    def test_purge_cannot_remove_a_pinned_version(self):
        with patch.object(server, "INDEX_DELAY_SECONDS", 5):
            server.upsert_records([valid_well()])
            server.upsert_records([valid_well(name="v2")])
        endpoint = "/api/storage/v2/records/" + valid_well()["id"] + "/versions?versionIds=1"
        self.assertEqual(call("delete", endpoint)[0], 429)
        self.assertEqual(len(server.RECORD_VERSIONS[valid_well()["id"]]), 2)

    def test_failed_epoch_startup_rejects_storage_and_search(self):
        backend = server.SEARCH_BACKEND
        try:
            with patch.object(backend, "start_epoch", side_effect=BackendError(503, "offline")):
                with self.assertRaises(BackendError):
                    server.start_runtime(backend)
            self.assertFalse(server.RUNTIME_ACCEPTING)
            self.assertIsNone(server.SEARCH_BACKEND)
            self.assertEqual(call("post", "/api/storage/v2/records", [valid_well()])[0], 503)
            self.assertEqual(call("post", "/api/search/v2/query", {"kind": "*"})[0], 503)
        finally:
            server.start_runtime(backend)

    def test_late_uncertain_request_cannot_replace_newer_ack(self):
        old = valid_well(name="old")
        server.upsert_records([old], index_immediately=True)
        delayed = dict(server.SEARCH_BACKEND.documents[old["id"]])
        server.upsert_records([valid_well(name="new")], index_immediately=True)
        # Simulate the engine applying an old timed-out request after a newer ACK.
        outcome = server.SEARCH_BACKEND.bulk([delayed])[0]
        self.assertFalse(outcome.success)
        self.assertFalse(outcome.retryable)
        self.assertEqual(server.SEARCH_BACKEND.documents[old["id"]]["data"]["FacilityName"], "new")

    def test_missing_index_rebuilds_storage_before_accepting_new_writes(self):
        old = valid_well(name="Already stored")
        server.upsert_records([old], index_immediately=True)
        server.SEARCH_BACKEND.documents.clear()
        new = valid_well("company-prod:master-data--Well:after-engine-replacement")
        with patch.object(server.SEARCH_BACKEND, "provision", return_value=True) as provision:
            server.upsert_records([new], index_immediately=True)
            provision.assert_called()
        self.assertEqual(set(server.SEARCH_BACKEND.documents), {old["id"], new["id"]})
        self.assertIsNone(server.INDEX_ERROR)

    def test_delayed_write_recovers_missing_index_without_a_search_request(self):
        old = valid_well(name="Already stored")
        server.upsert_records([old], index_immediately=True)
        server.SEARCH_BACKEND.documents.clear()
        new = valid_well("company-prod:master-data--Well:delayed-after-engine-replacement")
        with patch.object(server.SEARCH_BACKEND, "provision", return_value=True):
            server.upsert_records([new])
            self.assertTrue(wait_until(
                lambda: set(server.SEARCH_BACKEND.documents) == {old["id"], new["id"]}))
        self.assertIsNone(server.INDEX_ERROR)

    def test_failed_missing_index_rebuild_stays_unhealthy_until_reconciled(self):
        record = valid_well()
        server.upsert_records([record], index_immediately=True)
        server.SEARCH_BACKEND.documents.clear()
        with patch.object(server.SEARCH_BACKEND, "provision", return_value=True), \
                patch.object(server.SEARCH_BACKEND, "bulk", side_effect=BackendError(503, "offline")):
            self.assertEqual(call("get", "/api/indexer/v2/readiness_check")[0], 503)
        self.assertIsNotNone(server.INDEX_ERROR)
        self.assertEqual(server.RECORD_VERSIONS[record["id"]][0]["pins"], 0)
        self.assertEqual(call("get", "/api/indexer/v2/readiness_check")[0], 200)
        self.assertIn(record["id"], server.SEARCH_BACKEND.documents)
        self.assertIsNone(server.INDEX_ERROR)

    def test_liveness_is_independent_of_blocked_backend_readiness(self):
        import threading
        entered, release, live = threading.Event(), threading.Event(), threading.Event()
        responses = {}

        def blocked_count():
            entered.set()
            release.wait(5)
            return 0

        def readiness():
            responses["ready"] = call("get", "/api/indexer/v2/readiness_check")[0]

        def liveness():
            responses["live"] = call("get", "/api/indexer/v2/liveness_check")[0]
            live.set()

        ready_thread = threading.Thread(target=readiness)
        live_thread = threading.Thread(target=liveness)
        try:
            with patch.object(server.SEARCH_BACKEND, "count", side_effect=blocked_count):
                ready_thread.start()
                self.assertTrue(entered.wait(2))
                live_thread.start()
                self.assertTrue(live.wait(1), "Liveness must not wait for backend readiness")
        finally:
            release.set()
            ready_thread.join(5)
            if live_thread.ident is not None:
                live_thread.join(5)
        self.assertEqual(responses, {"live": 200, "ready": 200})

    def test_delayed_batch_uses_bounded_per_item_operations(self):
        records = [valid_well(), valid_well("company-prod:master-data--Well:second")]
        with patch.object(server.SEARCH_BACKEND, "bulk", wraps=server.SEARCH_BACKEND.bulk) as bulk:
            server.upsert_records(records)
            self.assertTrue(wait_until(lambda: server.SEARCH_BACKEND.count() == 2))
            # Per-item scheduling isolates retries and preserves same-ID ordering.
            self.assertEqual(bulk.call_count, 2)
            self.assertTrue(all(len(c.args[0]) == 1 for c in bulk.call_args_list))

    def test_hard_delete_recreate_same_id_version_rejects_stale_task(self):
        import threading
        entered, release = threading.Event(), threading.Event()
        original = server.SEARCH_BACKEND.bulk
        def pause(records):
            entered.set()
            release.wait(5)
            return original(records)

        old = valid_well(name="Old")
        try:
            # Intercept actual worker I/O, not the removed per-write Thread path.
            with patch.object(server.SEARCH_BACKEND, "bulk", side_effect=pause), \
                    patch.object(server, "DELETE_ACK_TIMEOUT", .01):
                server.upsert_records([old])
                self.assertTrue(entered.wait(2))
                self.assertEqual(call("delete", "/api/storage/v2/records/" + old["id"] + "?hardDelete=true")[0], 503)
                new = valid_well(name="New")
                self.assertEqual(server.upsert_records([new])[0]["version"], 3)
                release.set()
                self.assertTrue(wait_until(lambda: server.INDEX_SCHEDULER.pending("index") == 0))
            self.assertEqual(server.SEARCH_BACKEND.documents[old["id"]]["data"]["FacilityName"], "New")
        finally:
            release.set()

    def test_unpinned_eviction_and_byte_accounting(self):
        for n in range(5):
            server.upsert_records([valid_well(name=str(n))], index_immediately=True)
        history = server.RECORD_VERSIONS[valid_well()["id"]]
        self.assertEqual([e["version"] for e in history], [3, 4, 5])
        self.assertTrue(all(e["pins"] == 0 for e in history))
        self.assertEqual(server._retained_bytes(), sum(e["bytes"] for e in history))

    def test_shutdown_rejects_admission_and_restart_drops_state(self):
        backend = server.SEARCH_BACKEND
        server.upsert_records([valid_well()], index_immediately=True)
        self.assertTrue(server.stop_runtime())
        self.assertEqual(call("post", "/api/storage/v2/records", [valid_well()])[0], 503)
        server.start_runtime(backend)
        self.assertEqual(server.RECORD_BY_ID, {})
        self.assertEqual(backend.count(), 0)

    def test_backend_outage_is_not_successful_empty_search(self):
        with patch.object(server.SEARCH_BACKEND, "search", side_effect=BackendError(503, "unavailable")):
            status, body = call("post", "/api/search/v2/query", {"kind": "*"})
        self.assertEqual(status, 503)
        self.assertEqual(body["code"], 503)
        with patch.object(server.SEARCH_BACKEND, "count", side_effect=BackendError(503, "unavailable")):
            self.assertEqual(call("get", "/health")[0], 503)
            self.assertEqual(call("get", "/api/indexer/v2/readiness_check")[0], 503)

    def test_delayed_failure_recovers_from_authoritative_storage(self):
        record = valid_well()
        with patch.object(server.SEARCH_BACKEND, "bulk", side_effect=BackendError(503, "offline")):
            server.upsert_records([record])
            self.assertTrue(wait_until(lambda: server.INDEX_ERROR is not None))
        # Recovery is worker/retry-driven now, not a synchronous search-side rewrite.
        self.assertTrue(wait_until(lambda: server.INDEX_SCHEDULER.pending("index") == 0))
        status, result = call("post", "/api/search/v2/query", {"kind": "*"})
        self.assertEqual(status, 200)
        self.assertEqual(result["totalCount"], 1)
        self.assertIsNone(server.INDEX_ERROR)

    def test_owner_filter_and_cursor_are_caller_bound(self):
        record = valid_well()
        record["acl"] = {"owners": ["someone-else"], "viewers": [server.ACL_VIEWERS]}
        server.upsert_records([record], index_immediately=True)
        status, result = call("post", "/api/search/v2/query", {"kind": "*", "query_as_owner": True})
        self.assertEqual((status, result["totalCount"]), (200, 0))
        _, result = call("post", "/api/search/v2/query_with_cursor", {"kind": "*"})
        with patch.dict(server.GROUPS_BY_TOKEN, {server.TOKEN: {"service.search.user"}}):
            self.assertEqual(call("post", "/api/search/v2/query_with_cursor", {"cursor": result["cursor"]})[0], 403)
