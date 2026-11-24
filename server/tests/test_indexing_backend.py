"""Storage/indexer acceptance checks against an explicit isolated fake."""
from unittest.mock import Mock, patch
from test_server import ServerTestCase, call, server, valid_well, wait_until
from search_backend import BackendError
from search_backend import OpenSearch
from fake_storage_backend import FakeStorage
import json
import unittest
from copy import deepcopy
from fake_search_backend import FakeSearch


def custom_value_kind(n, value_type="number"):
    kind = f"local:test:Value{n}:1.0.0"
    server.SCHEMA_REGISTRY.register({"kind": kind, "schema": {
        "type": "object", "properties": {
            "Value": {"type": value_type},
            "Details": {"type": "object", "properties": {"Label": {"type": "string"}}}},
        "additionalProperties": True}}, "POST")
    return kind


def value_record(kind, n, value=1):
    return dict(valid_well(f"{server.PARTITION}:Value:{n}"), kind=kind,
                data={"Value": value, "Details": {"Label": "known", "Extra": "private"},
                      "Unknown": "not searchable"})


class SchemaIndexAdmissionTests(ServerTestCase):
    def test_partial_provision_failure_retries_lost_index_reconciliation(self):
        kinds = [custom_value_kind(n) for n in range(2)]
        records = [value_record(kind, n) for n, kind in enumerate(kinds)]
        server.upsert_records(records, index_immediately=True)
        server.ensure_index()
        backend = OpenSearch("http://localhost:9200", "partial007")
        backend.configure_registry(server.SCHEMA_REGISTRY)
        backend.indices = {kind: backend.kind_index(kind) for kind in kinds}
        present = {backend.kind_index(kinds[1])}
        fail = [True]
        published = []
        def request(method, path, body=None, **kwargs):
            if method == "GET":
                if path == "/" + backend.kind_index(kinds[1]) and fail[0]:
                    fail[0] = False
                    raise BackendError(503, "temporary_provision_failure")
                return {"index": {}} if path[1:] in present else {}
            if method == "PUT":
                present.add(path[1:])
                return {}
            if method == "POST":
                sources = [json.loads(line) for line in body.splitlines()[1::2]]
                published.extend(sources)
                return {"items": [{"index": {"status": 201}} for _ in sources]}
            self.fail("Unexpected transport operation")
        with patch.object(server, "SEARCH_BACKEND", backend), \
                patch.object(backend, "request", side_effect=request):
            with self.assertRaises(BackendError):
                server.ensure_index()
            self.assertFalse(published)
            server.ensure_index()
            self.assertEqual({r["id"] for r in published}, {r["id"] for r in records})
            self.assertFalse(backend.rebuild_required)
            self.assertFalse(server.INDEX_RECONCILE)
            server.ensure_index()
            self.assertEqual(len(published), len(records))

    def test_unicode_reconciliation_uses_actual_bounded_transport_bytes(self):
        kind = custom_value_kind(0)
        records = [value_record(kind, n) for n in range(30)]
        for record in records:
            record["data"]["Unknown"] = "\U0001f600" * 10000
        server.upsert_records(records, index_immediately=True)
        backend = OpenSearch("http://localhost:9200", "unicode007")
        backend.configure_registry(server.SCHEMA_REGISTRY)
        backend.indices[kind] = backend.kind_index(kind)
        bodies = []
        def request(method, path, body=None, **kwargs):
            if method == "GET":
                return {"index": {}}
            bodies.append(body)
            return {"items": [{"index": {"status": 201}}
                              for _ in body.splitlines()[1::2]]}
        with patch.object(server, "SEARCH_BACKEND", backend), \
                patch.object(server, "INDEX_RECONCILE", True), \
                patch.object(backend, "request", side_effect=request):
            server.ensure_index()
        sources = [json.loads(line) for body in bodies for line in body.splitlines()[1::2]]
        self.assertEqual(len(sources), 30)
        self.assertTrue(all(len(body.encode("utf-8")) <= 1024 * 1024 for body in bodies))
        self.assertTrue(all(r["data"]["Unknown"] == records[0]["data"]["Unknown"] for r in sources))
        self.assertTrue(all(e["pins"] == 0 for versions in server.RECORD_VERSIONS.values()
                            for e in versions))

    def test_legacy_injected_backend_keeps_original_noarg_provision_interface(self):
        inner = FakeSearch()
        legacy = Mock(spec=["start_epoch", "provision", "bulk", "count", "search"])
        legacy.start_epoch.side_effect = inner.start_epoch
        legacy.provision.side_effect = lambda: False
        legacy.bulk.side_effect = inner.bulk
        legacy.count.side_effect = inner.count
        legacy.search.side_effect = inner.search
        original = server.SEARCH_BACKEND
        try:
            server.start_runtime(legacy, storage=FakeStorage())
            server.upsert_records([valid_well()], index_immediately=True)
            self.assertEqual(server.indexed_count(), 1)
            self.assertTrue(all(not c.args for c in legacy.provision.call_args_list))
        finally:
            server.start_runtime(original, storage=FakeStorage())

    def test_eight_slots_and_ninth_rejected_before_save_or_queue(self):
        kinds = [custom_value_kind(n) for n in range(9)]
        server.upsert_records([value_record(k, n) for n, k in enumerate(kinds[:8])],
                              index_immediately=True)
        before = deepcopy(server.storage_state())
        with patch.object(server.STORAGE_BACKEND, "save") as save, \
                patch.object(server.INDEX_SCHEDULER, "reserve") as reserve:
            self.assertEqual(call("post", "/api/storage/v2/records",
                                  [value_record(kinds[8], 8)])[0], 429)
        save.assert_not_called()
        reserve.assert_not_called()
        self.assertEqual(server.storage_state(), before)
        self.assertEqual(server.ACTIVE_INDEX_KINDS, set(kinds[:8]))

    def test_failed_commit_and_scheduler_reservation_release_kind_slots(self):
        kind = custom_value_kind(0)
        before = deepcopy(server.storage_state())
        with patch.object(server.STORAGE_BACKEND, "save", side_effect=BackendError(503, "offline")):
            with self.assertRaises(BackendError):
                server.upsert_records([value_record(kind, 0)])
        self.assertEqual(server.storage_state(), before)
        self.assertEqual(server.ACTIVE_INDEX_KINDS, set())
        self.assertEqual(server.INDEX_SCHEDULER.pending("index"), 0)
        with patch.object(server.INDEX_SCHEDULER, "reserve", side_effect=server.CapacityError("full")):
            with self.assertRaises(server.CapacityError):
                server.upsert_records([value_record(kind, 0)])
        self.assertEqual(server.ACTIVE_INDEX_KINDS, set())
        server.upsert_records([value_record(kind, 0)], index_immediately=True)
        self.assertEqual(server.ACTIVE_INDEX_KINDS, {kind})

    def test_whole_batch_kind_limit_is_atomic_and_invalid_schema_never_reserves(self):
        kinds = [custom_value_kind(n) for n in range(9)]
        with self.assertRaises(server.CapacityError):
            server.upsert_records([value_record(k, n) for n, k in enumerate(kinds)])
        self.assertEqual(server.RECORD_BY_ID, {})
        self.assertEqual(server.ACTIVE_INDEX_KINDS, set())
        with self.assertRaises(server.SchemaError):
            server.upsert_records([value_record(kinds[0], 0, "invalid")])
        self.assertEqual(server.ACTIVE_INDEX_KINDS, set())

    def test_empty_slot_retained_until_epoch_rebuild(self):
        kinds = [custom_value_kind(n) for n in range(9)]
        records = [value_record(k, n) for n, k in enumerate(kinds[:8])]
        server.upsert_records(records, index_immediately=True)
        server._delete_records([records[0]["id"]], hard=True, principal=server.core_auth.SYSTEM)
        with self.assertRaises(server.CapacityError):
            server.upsert_records([value_record(kinds[8], 8)])
        storage = server.STORAGE_BACKEND
        server.start_runtime(FakeSearch(), storage=storage)
        self.assertEqual(server.ACTIVE_INDEX_KINDS, set(kinds[1:8]))
        server.upsert_records([value_record(kinds[8], 8)], index_immediately=True)
        self.assertEqual(len(server.ACTIVE_INDEX_KINDS), 8)

    def test_recovery_publishes_current_metadata_and_visible_version(self):
        record = valid_well()
        server.upsert_records([record], index_immediately=True)
        old_job, _, _ = server._index_job(server.effective_record(server.RECORD_BY_ID[record["id"]]))
        status, _ = server._patch_records([record["id"]],
            [("add", ["tags"], {"policy": "current"})],
            principal=server.core_auth.SYSTEM, data_patch=False)
        self.assertEqual(status, 200)
        self.assertTrue(wait_until(lambda: server.INDEX_SCHEDULER.pending("index") == 0))
        server.SEARCH_BACKEND.documents.clear()
        with patch.object(server.SEARCH_BACKEND, "provision", return_value=True):
            server.ensure_index()
        source = server.SEARCH_BACKEND.documents[record["id"]]
        self.assertEqual(source["version"], 1)
        self.assertEqual(source["tags"], {"policy": "current"})
        self.assertEqual(server._execute_index(old_job).error, "superseded")
        self.assertEqual(server.SEARCH_BACKEND.generations[record["id"]], 2)

    def test_recovery_batches_are_bounded_and_keep_deleted_suppressed_fences(self):
        records = [valid_well(f"{server.PARTITION}:master-data--Well:batch{n}") for n in range(205)]
        for start in range(0, len(records), 100):
            server.upsert_records(records[start:start + 100], index_immediately=True)
        server.soft_delete_records([records[0]["id"]])
        server.SEARCH_BACKEND.documents.clear()
        with patch.object(server.SEARCH_BACKEND, "provision", return_value=True), \
                patch.object(server.SEARCH_BACKEND, "bulk", wraps=server.SEARCH_BACKEND.bulk) as bulk:
            server.ensure_index()
        self.assertEqual([len(c.args[0]) for c in bulk.call_args_list], [100, 100, 5])
        self.assertEqual(server.SEARCH_BACKEND.count(), 204)
        self.assertTrue(server.SEARCH_BACKEND.documents[records[0]["id"]]["_deleted"])
        self.assertTrue(all(e["pins"] == 0 for versions in server.RECORD_VERSIONS.values() for e in versions))


class SchemaIndexMappingTests(unittest.TestCase):
    def test_exact_mapping_lazily_provisioned_and_no_dynamic_templates(self):
        registry = server.SchemaRegistry()
        backend = OpenSearch("http://localhost:9200", "mapping-test")
        backend.configure_registry(registry)
        with patch.object(backend, "request", return_value={}) as request:
            backend.start_epoch("abc")
            self.assertFalse(any(c.args[0] == "PUT" for c in request.call_args_list))
            backend.provision([server.WELL_KIND])
        put = [c for c in request.call_args_list if c.args[0] == "PUT"][0]
        mapping = put.args[2]["mappings"]
        self.assertEqual(mapping, registry.mapping(server.WELL_KIND))
        self.assertEqual(put.args[2]["settings"], {"number_of_shards": 1, "number_of_replicas": 0})
        self.assertIn(__import__("hashlib").sha256(server.WELL_KIND.encode()).hexdigest(), put.args[1])
        self.assertTrue(put.args[1].endswith("-abc"))
        self.assertNotIn("dynamic_templates", mapping)
        self.assertEqual(mapping["dynamic"], False)
        self.assertEqual(mapping["properties"]["data"]["dynamic"], False)
        self.assertEqual(mapping["properties"]["legal"]["dynamic"], False)

    def test_generation_transport_does_not_replace_visible_source_version(self):
        backend = OpenSearch("http://localhost:9200", "mapping-test")
        record = dict(valid_well(), version=1, _generation=7)
        with patch.object(backend, "provision"), \
                patch.object(backend, "request", return_value={"items": [{"index": {"status": 201}}]}) as req:
            backend.bulk([record])
        action, source = map(json.loads, req.call_args.args[2].splitlines())
        self.assertEqual(action["index"]["version"], 7)
        self.assertEqual(source["version"], 1)
        self.assertNotIn("_generation", source)

    def test_legacy_text_adapter_only_rewrites_registered_keyword_fields(self):
        backend = OpenSearch("http://localhost:9200", "mapping-test")
        backend.configure_registry(server.SchemaRegistry())
        backend.indices[server.WELL_KIND] = backend.kind_index(server.WELL_KIND)
        query = 'data.FacilityName:quick AND data.StartM:[2 TO 4] AND data.Unknown:x'
        with patch.object(backend, "request", return_value={"hits": {"hits": []}}) as req:
            backend.search({"kind": "*", "query": query, "_legacy_text": True})
        self.assertEqual(req.call_args.args[2]["query"]["bool"]["must"][0]["query_string"]["query"],
                         'data.FacilityName.text:quick AND data.StartM:[2 TO 4] AND data.Unknown:x')
        with patch.object(backend, "request", return_value={"hits": {"hits": []}}) as req:
            backend.search({"kind": "*", "query": 'data.FacilityName:"word data.FieldName:x" '
                            'AND data.FieldName.keyword:EAGLE-A', "_legacy_text": True})
        self.assertEqual(req.call_args.args[2]["query"]["bool"]["must"][0]["query_string"]["query"],
                         'data.FacilityName.text:"word data.FieldName:x" AND data.FieldName:EAGLE-A')

    def test_lazy_creation_is_not_loss_recovery_and_existing_slot_loss_is(self):
        backend = OpenSearch("http://localhost:9200", "mapping-test")
        backend.configure_registry(server.SchemaRegistry())
        with patch.object(backend, "request", return_value={}):
            self.assertTrue(backend.provision([server.WELL_KIND]))
            self.assertFalse(backend.rebuild_required)
            self.assertTrue(backend.provision())
            self.assertTrue(backend.rebuild_required)

    def test_empty_lazy_partition_does_not_hide_engine_outage(self):
        backend = OpenSearch("http://localhost:9200", "mapping-test")
        with patch.object(backend, "request", side_effect=BackendError(503, "offline")):
            with self.assertRaises(BackendError):
                backend.provision([])
            with self.assertRaises(BackendError):
                backend.count()


class BulkOutcomeTests(unittest.TestCase):
    def test_partial_outcomes_and_external_generation(self):
        backend = OpenSearch("http://localhost:9200", "test")
        records = [dict(id=str(n), kind=server.WELL_KIND, version=n + 1) for n in range(3)]
        backend.indices[server.WELL_KIND] = backend.kind_index(server.WELL_KIND)
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
        provision.assert_not_called()  # No eager empty shard.


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

    def test_pinned_retention_limit_rejects_without_mutation(self):
        import threading
        entered, release = threading.Event(), threading.Event()
        original = server.SEARCH_BACKEND.bulk
        def blocked(records):
            entered.set()
            release.wait(3)
            return original(records)
        try:
            with patch.object(server.SEARCH_BACKEND, "bulk", side_effect=blocked), \
                    patch.object(server, "MAX_RETAINED_VERSIONS", 3):
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
        self.assertEqual(result[0]["version"], 2)
        self.assertEqual(server.RECORD_FENCES[record["id"]][1:], (3, 2))

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
                    server.start_runtime(backend, storage=FakeStorage())
            self.assertFalse(server.RUNTIME_ACCEPTING)
            self.assertIsNone(server.SEARCH_BACKEND)
            self.assertEqual(call("post", "/api/storage/v2/records", [valid_well()])[0], 503)
            self.assertEqual(call("post", "/api/search/v2/query", {"kind": "*"})[0], 503)
        finally:
            server.start_runtime(backend, storage=FakeStorage())

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
                self.assertEqual(server.upsert_records([new])[0]["version"], 2)
                release.set()
                self.assertTrue(wait_until(lambda: server.INDEX_SCHEDULER.pending("index") == 0))
            self.assertEqual(server.SEARCH_BACKEND.documents[old["id"]]["data"]["FacilityName"], "New")
        finally:
            release.set()

    def test_unpinned_history_is_retained_and_all_bytes_are_accounted(self):
        for n in range(5):
            server.upsert_records([valid_well(name=str(n))], index_immediately=True)
        history = server.RECORD_VERSIONS[valid_well()["id"]]
        self.assertEqual([e["version"] for e in history], [1, 2, 3, 4, 5])
        self.assertTrue(all(e["pins"] == 0 for e in history))
        self.assertEqual(server._retained_bytes(), sum(e["bytes"] for e in history))

    def test_shutdown_rejects_admission_and_restart_drops_state(self):
        backend = server.SEARCH_BACKEND
        server.upsert_records([valid_well()], index_immediately=True)
        self.assertTrue(server.stop_runtime())
        self.assertEqual(call("post", "/api/storage/v2/records", [valid_well()])[0], 503)
        server.start_runtime(backend, storage=FakeStorage())
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
        self.assertIsNone(result["cursor"])  # Exhaustion no longer allocates a cache entry.
        second = deepcopy(record)
        second["id"] += "-next"
        server.upsert_records([second], index_immediately=True)
        _, result = call("post", "/api/search/v2/query_with_cursor", {"kind": "*", "limit": 1})
        self.assertIsInstance(result["cursor"], str)
        with patch.dict(server.GROUPS_BY_TOKEN, {server.TOKEN: {"service.search.user"}}):
            status, response = call("post", "/api/search/v2/query_with_cursor", {"cursor": result["cursor"]})
            self.assertEqual(status, 400)  # Changed effective groups invalidate the revision-bound cursor.
            self.assertNotIn("results", response)
            self.assertNotIn(result["cursor"], server.CURSORS)

    def test_replay_of_suppressed_dataset_is_mutation_not_stale_read(self):
        record = valid_well(server.PARTITION + ":dataset--File.Generic:replay-regression")
        record["kind"] = server.DATASET_KIND
        server.upsert_records([record], index_immediately=True)
        self.assertEqual(call("delete", "/api/indexer/v2/index?kind=" + record["kind"])[0], 200)
        self.assertIn(record["id"], server.INDEX_SUPPRESSED)
        status, result = call("post", "/api/storage/v2/replay", {"operation": "reindex"})
        self.assertEqual(status, 202, result)
        self.assertNotIn(record["id"], server.INDEX_SUPPRESSED)
        self.assertEqual(call("post", "/api/search/v2/query", {"kind": record["kind"]})[1]["totalCount"], 1)
