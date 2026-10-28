"""Storage/indexer acceptance checks against an explicit isolated fake."""
from unittest.mock import patch
from test_server import ServerTestCase, call, server, valid_well, wait_until
from search_backend import BackendError


class IndexingBackendTests(ServerTestCase):
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

    def test_delayed_batch_uses_one_bulk_operation(self):
        records = [valid_well(), valid_well("company-prod:master-data--Well:second")]
        with patch.object(server.SEARCH_BACKEND, "bulk", wraps=server.SEARCH_BACKEND.bulk) as bulk:
            server.upsert_records(records)
            self.assertTrue(wait_until(lambda: server.SEARCH_BACKEND.count() == 2))
            self.assertEqual(bulk.call_count, 1)

    def test_hard_delete_recreate_same_id_version_rejects_stale_task(self):
        import threading
        entered, release = threading.Event(), threading.Event()
        threads = []
        real_thread = threading.Thread

        def capture(*args, **kwargs):
            thread = real_thread(*args, **kwargs)
            threads.append(thread)
            return thread

        def pause(_):
            entered.set()
            release.wait(5)

        old = valid_well(name="Old")
        try:
            with patch("server.threading.Thread", side_effect=capture), patch("server.time.sleep", side_effect=pause):
                server.upsert_records([old])
                self.assertTrue(entered.wait(2))
                call("delete", "/api/storage/v2/records/" + old["id"] + "?hardDelete=true")
                new = valid_well(name="New")
                server.upsert_records([new], index_immediately=True)
                release.set()
                for thread in threads:
                    thread.join(3)
            self.assertEqual(server.SEARCH_BACKEND.documents[old["id"]]["data"]["FacilityName"], "New")
        finally:
            release.set()

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
