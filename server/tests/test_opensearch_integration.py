"""Real Lucene acceptance suite. Never runs against the unit-test fake.

OPENSEARCH_TEST_URL=http://localhost:9200 python -m unittest discover \
    -s server/tests -p test_opensearch_integration.py -v
"""
import os
import time
import unittest
import uuid
from urllib.parse import quote

from test_server import call, server, valid_well, wait_until
from search_backend import OpenSearch, BackendError
from schema_registry import SchemaError
from unittest.mock import patch
from copy import deepcopy
from fake_storage_backend import FakeStorage
from test_indexing_backend import custom_value_kind, value_record
import test_core_search as core_search


@unittest.skipUnless(os.getenv("OPENSEARCH_TEST_URL"), "requires a real OpenSearch node")
class RealCoreSearchTests(core_search.CoreSearchTests):
    def backend(self):
        return OpenSearch(os.environ["OPENSEARCH_TEST_URL"], "core008-" + uuid.uuid4().hex)

    def test_real_lucene_is_not_lenient_and_cursor_sources_are_disabled(self):
        self.assertEqual(self.request({"kind": "*", "query": "data.StartM:("})[0], 400)
        requests = []
        original = self.engine.request
        def capture(method, path, body=None, **kwargs):
            if path.endswith("/_search"):
                requests.append(deepcopy(body))
            return original(method, path, body, **kwargs)
        with patch.object(self.engine, "request", side_effect=capture):
            status, body = self.request({"kind": "*", "query": "data.StartM:[1 TO 3]",
                                        "limit": 1, "aggregateBy": "kind"}, cursor=True)
            self.assertEqual((status, body["totalCount"]), (200, 3))
        self.assertEqual(len(requests), 1)
        request = requests[0]
        self.assertFalse(request["_source"])
        self.assertFalse(request["query"]["bool"]["must"][0]["query_string"]["lenient"])
        self.assertEqual(len(request["query"]["bool"]["filter"][0]["terms"]["id"]), 3)
        self.assertEqual(request["size"], 5000)


@unittest.skipUnless(os.getenv("OPENSEARCH_TEST_URL"), "requires a real OpenSearch node")
class RealOpenSearchTests(unittest.TestCase):
    def setUp(self):
        self.previous = server.SEARCH_BACKEND
        self.previous_delay = server.INDEX_DELAY_SECONDS
        server.SEARCH_BACKEND = OpenSearch(os.environ["OPENSEARCH_TEST_URL"], "integration-" + uuid.uuid4().hex)
        self.addCleanup(self.cleanup_backend)
        server.clear_state()
        server.INDEX_DELAY_SECONDS = 0.1

    def cleanup_backend(self):
        try:
            server.clear_state()  # generation guard cancels all pending writes
            server.SEARCH_BACKEND.request("DELETE", "/" + server.SEARCH_BACKEND.index, missing_ok=True)
        finally:
            server.SEARCH_BACKEND = self.previous
            server.INDEX_DELAY_SECONDS = self.previous_delay

    def search(self, query="*", **extra):
        status, result = call("post", "/api/search/v2/query", {"kind": "*", "query": query, **extra})
        self.assertEqual(status, 200, result)
        return result

    def test_real_analyzers_lucene_operators_and_errors(self):
        record = valid_well("company-prod:master-data--Well:slash/id", "The Quick Brown Fox")
        record["data"]["StartM"] = 42.5
        server.upsert_records([record], index_immediately=True)
        for query in ('data.FacilityName:quick', 'data.FacilityName:"quick brown"',
                      'data.FacilityName:quik~1', 'data.FacilityName:quick^3 OR data.FacilityName:absent',
                      'data.FacilityName:/qu.*/', 'data.StartM:[40 TO 45]',
                      'id:"company-prod:master-data--Well:slash/id"'):
            self.assertEqual(self.search(query)["totalCount"], 1, query)
        self.assertEqual(self.search('data.FacilityName:"brown quick"')["totalCount"], 0)
        self.assertEqual(self.search('data.FacilityName.keyword:"quick brown"')["totalCount"], 0)
        status, error = call("post", "/api/search/v2/query", {"kind": "*", "query": "data.FacilityName:("})
        self.assertEqual((status, error["code"]), (400, 400))
        bad = valid_well("company-prod:master-data--Well:incompatible")
        bad["data"]["FacilityName"] = {"incompatible": "object-in-a-text-field"}
        # Known-field errors now fail schema preflight, without a partial write.
        version = server.RECORD_BY_ID[record["id"]]["version"]
        with self.assertRaises(SchemaError):
            server.upsert_records([record, bad], index_immediately=True)
        self.assertEqual(server.RECORD_BY_ID[record["id"]]["version"], version)
        self.assertNotIn(bad["id"], server.RECORD_BY_ID)
        # RISK-006: malformed known numeric data is rejected BEFORE engine I/O.
        bad["data"] = {"StartM": "not-a-number"}
        with patch.object(server.SEARCH_BACKEND, "bulk") as bulk, self.assertRaises(SchemaError):
            server.upsert_records([bad], index_immediately=True)
        bulk.assert_not_called()
        self.assertNotIn(bad["id"], server.RECORD_BY_ID)
        self.assertEqual(call("get", "/api/indexer/v2/readiness_check")[0], 200)
        self.assertEqual(self.search()["totalCount"], 1)

    def test_seed_reindex_delete_restore_and_reset(self):
        server.SEARCH_BACKEND.bulk([dict(
            valid_well("company-prod:master-data--Well:old-remote-document"), version=1)])
        server.load_full_sample()
        self.assertEqual(self.search()["totalCount"], 15)
        base = "/api/indexer/v2"
        server.SEARCH_BACKEND.request("DELETE", "/" + server.SEARCH_BACKEND.index)
        self.assertEqual(call("put", base + "/partitions/provision")[0], 200)
        self.assertEqual(self.search()["totalCount"], 15)  # RISK-006: provision reconciles Storage.
        self.assertEqual(call("patch", base + "/reindex")[0], 200)
        self.assertEqual(self.search()["totalCount"], 15)
        self.assertEqual(call("delete", base + "/index?kind=" + quote(server.WELL_KIND))[0], 200)
        self.assertEqual(self.search(kind=server.WELL_KIND)["totalCount"], 0)
        self.assertEqual(call("post", base + "/reindex", {"kind": server.WELL_KIND})[0], 200)
        self.assertEqual(self.search()["totalCount"], 15)
        rid = server.FULL_SAMPLE[0]["id"]
        server.soft_delete_records([rid])
        self.assertEqual(self.search()["totalCount"], 14)
        self.assertEqual(call("post", base + "/reindex/records", {"recordIds": [rid]})[0], 404)
        self.assertEqual(call("patch", base + "/reindex?force_clean=true")[0], 200)
        self.assertEqual(self.search()["totalCount"], 14)
        server.upsert_records([server.FULL_SAMPLE[0]], index_immediately=True)
        self.assertEqual(call("post", base + "/reindex/records", {"recordIds": [rid]})[0], 202)
        self.assertEqual(self.search()["totalCount"], 15)
        for path in ("/api/storage/v2/records/" + quote(rid, safe="") + "?hardDelete=true",):
            self.assertEqual(call("delete", path)[0], 204)
        self.assertEqual(self.search()["totalCount"], 14)
        server.load_full_sample()
        self.assertEqual(self.search()["totalCount"], 15)

    def test_delayed_update_delete_and_reset_cannot_resurrect(self):
        record = valid_well(name="Old Name")
        server.INDEX_DELAY_SECONDS = 0.4
        server.upsert_records([record])
        self.assertEqual(self.search()["totalCount"], 0)
        self.assertTrue(wait_until(lambda: self.search()["totalCount"] == 1, timeout=8))
        record["data"]["FacilityName"] = "New Name"
        server.upsert_records([record])
        self.assertTrue(wait_until(lambda: self.search('data.FacilityName:"New Name"')["totalCount"] == 1, timeout=8))
        server.upsert_records([record])
        server.soft_delete_records([record["id"]])
        time.sleep(0.6)
        self.assertEqual(self.search()["totalCount"], 0)
        server.upsert_records([record])
        server.clear_state()
        time.sleep(0.6)
        self.assertEqual(self.search()["totalCount"], 0)

    def test_engine_index_loss_rebuilds_mappings_and_all_stored_records(self):
        old = valid_well(name="Before engine replacement")
        server.upsert_records([old], index_immediately=True)
        server.SEARCH_BACKEND.request("DELETE", "/" + server.SEARCH_BACKEND.index)
        new = valid_well("company-prod:master-data--Well:new-after-engine-replacement")
        server.upsert_records([new], index_immediately=True)
        self.assertEqual(self.search()["totalCount"], 2)
        mapping = server.SEARCH_BACKEND.request("GET", "/" + server.SEARCH_BACKEND.index + "/_mapping")
        properties = mapping[server.SEARCH_BACKEND.index]["mappings"]["properties"]
        self.assertEqual(properties["id"]["type"], "keyword")
        self.assertEqual(properties["kind"]["type"], "keyword")
        server.SEARCH_BACKEND.request("DELETE", "/" + server.SEARCH_BACKEND.index)
        server.INDEX_DELAY_SECONDS = 0
        third = valid_well("company-prod:master-data--Well:delayed-after-engine-replacement")
        server.upsert_records([third])
        self.assertTrue(wait_until(lambda: self.search()["totalCount"] == 3, timeout=8))

    def test_patch_copy_workflow_dataset_restore_replay_and_version_purge(self):
        record = valid_well()
        server.upsert_records([record], index_immediately=True)
        self.assertEqual(call("patch", "/api/storage/v2/records/" + record["id"],
                              {"data": {"FacilityName": "Patched Facility"}})[0], 200)
        self.assertEqual(self.search('data.FacilityName:"Patched Facility"')["totalCount"], 1)
        status, copied = call("post", "/api/records/" + record["id"] + ":copy", {})
        self.assertEqual(status, 200)
        self.assertEqual(self.search('data.FacilityName:"Patched Facility"')["totalCount"], 2)
        self.assertEqual(call("delete", "/api/storage/v2/records/versions/" + copied["newRecordId"])[0], 204)
        self.assertEqual(self.search()["totalCount"], 1)
        dataset = {**valid_well(), "id": "company-prod:dataset--File.Generic:restore", "kind": server.DATASET_KIND}
        server.upsert_records([dataset], index_immediately=True)
        server.soft_delete_records([dataset["id"]])
        self.assertEqual(call("post", "/api/dataset/v1/metadataRecord/" + dataset["id"] + "/undelete")[0], 200)
        self.assertEqual(self.search(kind=server.DATASET_KIND)["totalCount"], 1)
        self.assertEqual(call("delete", "/api/indexer/v2/index?kind=" + quote(server.DATASET_KIND))[0], 200)
        self.assertEqual(call("post", "/api/storage/v2/replay", {"operation": "reindex"})[0], 202)
        self.assertEqual(self.search(kind=server.DATASET_KIND)["totalCount"], 1)
        workflow_record = valid_well("company-prod:master-data--Well:workflow")
        status, run = call("post", "/api/workflow/v1/workflow/Osdu_ingest/workflowRun", {
            "executionContext": {"acl": server.DEFAULT_ACL, "legal": record["legal"],
                                 "manifest": {"MasterData": [workflow_record], "Data": {}}}})
        self.assertEqual(status, 200, run)
        self.assertTrue(wait_until(lambda: self.search('id:"' + workflow_record["id"] + '"')["totalCount"] == 1, timeout=8))

    def test_complete_backend_pages_projection_acl_and_cursor_snapshot(self):
        records = [valid_well(f"company-prod:master-data--Well:{i:04d}", "Page Name") for i in range(1005)]
        for record in records[:3]:
            record["acl"] = {"owners": ["other"], "viewers": [server.ACL_VIEWERS]}
        records[-1]["acl"] = {"owners": ["other"], "viewers": ["other"]}
        # The public admission contract caps batches at 100; build the larger
        # paging fixture through that same bounded interface.
        for start in range(0, len(records), 100):
            server.upsert_records(records[start:start + 100], index_immediately=True)
        result = self.search(offset=1000, limit=10, returnedFields=["id"],
                             aggregateBy=[{"field": "id", "aggregateByType": "count"}])
        self.assertEqual(result["totalCount"], 1004)
        self.assertEqual(len(result["results"]), 4)
        self.assertEqual(set(result["results"][0]), {"id"})
        self.assertEqual(result["aggregates"][0]["Count"]["count"], 1004)
        self.assertEqual(self.search(query_as_owner=True)["totalCount"], 1001)
        _, first = call("post", "/api/search/v2/query_with_cursor", {"kind": "*", "limit": 1000})
        server.soft_delete_records([records[1000]["id"]])
        status, second = call("post", "/api/search/v2/query_with_cursor", {"cursor": first["cursor"]})
        self.assertEqual((status, second["reason"]), (400, "cursor_invalidated"))
        _, first = call("post", "/api/search/v2/query_with_cursor", {"kind": "*", "limit": 1000})
        status, second = call("post", "/api/search/v2/query_with_cursor", {"cursor": first["cursor"]})
        self.assertEqual((status, len(second["results"]), second["totalCount"], second["cursor"]),
                         (200, 3, 1003, None))
        stats = server.SEARCH_BACKEND.request("GET", "/" + server.SEARCH_BACKEND.index + "/_stats/search")
        self.assertEqual(stats["_all"]["total"]["search"]["open_contexts"], 0)


@unittest.skipUnless(os.getenv("OPENSEARCH_TEST_URL"), "requires a real OpenSearch node")
class RealSchemaIndexTests(unittest.TestCase):
    """EPIC-007 engine gates only; does not claim core query/cursor semantics."""

    def setUp(self):
        self.previous = (server.SEARCH_BACKEND, server.STORAGE_BACKEND, server.FILE_BACKEND,
                         server.INDEX_DELAY_SECONDS)
        self.profile = patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "core"})
        self.profile.start()
        self.backend = OpenSearch(os.environ["OPENSEARCH_TEST_URL"], "schema007-" + uuid.uuid4().hex)
        self.storage = FakeStorage()
        server.start_runtime(self.backend, storage=self.storage)
        server.INDEX_DELAY_SECONDS = 0
        self.addCleanup(self.restore_runtime)

    def restore_runtime(self):
        server.stop_runtime()
        self.backend.request("DELETE", "/" + self.backend.owned_prefix + "-*", missing_ok=True)
        self.profile.stop()
        backend, storage, files, delay = self.previous
        server.INDEX_DELAY_SECONDS = delay
        server.start_runtime(backend, storage=storage, files=files)

    def mapping(self, kind):
        name = self.backend.kind_index(kind)
        return self.backend.request("GET", "/" + name + "/_mapping")[name]["mappings"]

    def normalized_mapping(self, mapping):
        # OpenSearch serializes dynamic:false as "false" and omits the
        # redundant type:object on property-bearing objects. Compare every
        # other returned key/leaf exactly, without masking unexpected fields.
        mapping = deepcopy(mapping)
        def normalize(node):
            if node.get("dynamic") == "false":
                node["dynamic"] = False
            if "properties" in node:
                if node.get("type") == "object":
                    node.pop("type")
                for child in node["properties"].values():
                    normalize(child)
        normalize(mapping)
        return mapping

    def raw_search(self, kind, query, sort=None):
        body = {"query": query, "size": 100}
        if sort:
            body["sort"] = sort
        return self.backend.request("POST", "/" + self.backend.kind_index(kind) + "/_search", body)

    def test_partial_provision_failure_retains_lost_index_rebuild(self):
        kinds = [custom_value_kind(n) for n in range(2)]
        records = [value_record(kind, n) for n, kind in enumerate(kinds)]
        server.upsert_records(records, index_immediately=True)
        self.backend.request("DELETE", "/" + self.backend.kind_index(kinds[0]))
        transport = self.backend.request
        failed = [False]
        def request(method, path, *args, **kwargs):
            if method == "GET" and path == "/" + self.backend.kind_index(kinds[1]) and not failed[0]:
                failed[0] = True
                raise server.BackendError(503, "temporary_provision_failure")
            return transport(method, path, *args, **kwargs)
        with patch.object(self.backend, "request", side_effect=request):
            with self.assertRaises(server.BackendError):
                server.ensure_index()
            server.ensure_index()
        self.assertEqual(self.backend.count(), 2)
        self.assertFalse(self.backend.rebuild_required)
        self.assertEqual(self.raw_search(kinds[0], {"match_all": {}})["hits"]["total"]["value"], 1)

    def test_unicode_records_survive_loss_and_epoch_rebuild(self):
        kind = custom_value_kind(0)
        records = [value_record(kind, n) for n in range(30)]
        for record in records:
            record["data"]["Unknown"] = "\U0001f600" * 10000
        server.upsert_records(records, index_immediately=True)
        self.backend.request("DELETE", "/" + self.backend.kind_index(kind))
        server.ensure_index()
        self.assertEqual(self.backend.count(), 30)
        server.start_runtime(self.backend, storage=self.storage)
        self.assertEqual(self.backend.count(), 30)
        result = self.raw_search(kind, {"match_all": {}})
        self.assertTrue(all(h["_source"]["data"]["Unknown"] == records[0]["data"]["Unknown"]
                            for h in result["hits"]["hits"]))
        self.assertEqual(self.raw_search(kind, {"exists": {"field": "data.Unknown"}})["hits"]["total"]["value"], 0)

    def test_conflicting_custom_types_exact_mappings_range_sort_text_unknown_source(self):
        numeric = custom_value_kind(0)
        textual = custom_value_kind(1, "string")
        records = [value_record(numeric, n, value) for n, value in enumerate([10, 2, 30])]
        records.append(value_record(textual, 3, "Quick Brown Fox"))
        server.upsert_records(records, index_immediately=True)
        for kind in (numeric, textual):
            actual = self.mapping(kind)
            self.assertEqual(self.normalized_mapping(actual),
                             self.normalized_mapping(server.SCHEMA_REGISTRY.mapping(kind)))
            self.assertNotIn("dynamic_templates", actual)
            self.assertIn(actual["dynamic"], (False, "false"))
            data = actual["properties"]["data"]
            self.assertIn(data["dynamic"], (False, "false"))
            self.assertIn(data["properties"]["Details"]["dynamic"], (False, "false"))
            self.assertNotIn("Unknown", data["properties"])
            self.assertNotIn("Extra", data["properties"]["Details"]["properties"])
            name = self.backend.kind_index(kind)
            settings = self.backend.request("GET", "/" + name + "/_settings")[name]["settings"]["index"]
            self.assertEqual((settings["number_of_shards"], settings["number_of_replicas"]), ("1", "0"))
        self.assertEqual(self.mapping(numeric)["properties"]["data"]["properties"]["Value"]["type"], "double")
        self.assertEqual(self.mapping(textual)["properties"]["data"]["properties"]["Value"]["type"], "keyword")
        result = self.raw_search(numeric, {"range": {"data.Value": {"gte": 2, "lte": 10}}},
                                 [{"data.Value": "asc"}])
        self.assertEqual([h["_source"]["data"]["Value"] for h in result["hits"]["hits"]], [2, 10])
        self.assertEqual(self.raw_search(textual, {"term": {"data.Value": "Quick Brown Fox"}})["hits"]["total"]["value"], 1)
        self.assertEqual(self.raw_search(textual, {"match": {"data.Value.text": "quick"}})["hits"]["total"]["value"], 1)
        self.assertEqual(self.raw_search(textual, {"term": {"data.Value": "quick"}})["hits"]["total"]["value"], 0)
        for field in ("data.Unknown", "data.Details.Extra"):
            self.assertEqual(self.raw_search(numeric, {"exists": {"field": field}})["hits"]["total"]["value"], 0)
        source = self.raw_search(numeric, {"match_all": {}})["hits"]["hits"][0]["_source"]
        self.assertEqual(source["data"]["Unknown"], "not searchable")
        self.assertEqual(source["data"]["Details"]["Extra"], "private")
        self.assertEqual(server.read_record(source["id"], principal=server.core_auth.SYSTEM)["data"], source["data"])
        self.assertEqual(len(self.backend.search({"kind": "*", "query": "*"})), 4)

    def test_loss_epoch_obsolete_rebuild_preserves_policy_tombstones_and_partition(self):
        kind = custom_value_kind(0)
        records = [value_record(kind, n) for n in range(3)]
        server.upsert_records(records, index_immediately=True)
        stale, _, _ = server._index_job(server.effective_record(server.RECORD_BY_ID[records[0]["id"]]))
        new_acl = {"owners": [server.ACL_OWNERS], "viewers": []}
        new_legal = {"legaltags": [server.LEGAL_TAG], "otherRelevantDataCountries": ["US"]}
        status, _ = server._patch_records([records[0]["id"]], [
            ("replace", ["acl", "viewers"], []),
            ("replace", ["legal", "otherRelevantDataCountries"], ["US"]),
            ("add", ["tags"], {"policy": "latest"})],
            principal=server.core_auth.SYSTEM, data_patch=False)
        self.assertEqual(status, 200)
        self.assertTrue(wait_until(lambda: server.INDEX_SCHEDULER.pending("index") == 0))
        server.soft_delete_records([records[1]["id"]])
        server._delete_records([records[2]["id"]], hard=True, principal=server.core_auth.SYSTEM)
        # Preserve history and its mapping after OBSOLETE; no new admission.
        payload = server.SCHEMA_REGISTRY.get(kind)
        payload["status"] = "OBSOLETE"
        payload["schemaInfo"]["status"] = "OBSOLETE"
        with server.STORE_LOCK, server.storage_transaction():
            server.SCHEMA_REGISTRY.register(payload, "PUT")
        foreign = OpenSearch(os.environ["OPENSEARCH_TEST_URL"], "foreign007-" + uuid.uuid4().hex)
        foreign.configure_registry(server.SCHEMA_REGISTRY)
        self.addCleanup(foreign.request, "DELETE", "/" + foreign.owned_prefix + "-*", missing_ok=True)
        foreign.start_epoch("keep")
        foreign.bulk([dict(records[0], version=1)])
        original_mapping = self.mapping(kind)
        name = self.backend.kind_index(kind)
        self.backend.request("DELETE", "/" + name)
        server.ensure_index()
        self.assertEqual(self.mapping(kind), original_mapping)
        self.assertEqual(self.backend.count(), 1)
        self.assertEqual(foreign.count(), 1)
        old_epoch = self.backend.epoch
        server.start_runtime(self.backend, storage=self.storage)
        self.assertNotEqual(old_epoch, self.backend.epoch)
        self.assertEqual(self.mapping(kind), original_mapping)
        self.assertEqual(self.backend.count(), 1)
        self.assertEqual(foreign.count(), 1)
        self.assertEqual(server._execute_index(stale).error, "superseded")
        source = self.raw_search(kind, {"term": {"id": records[0]["id"]}})["hits"]["hits"][0]["_source"]
        self.assertEqual((source["version"], source["acl"], source["legal"], source["tags"]),
                         (1, new_acl, new_legal, {"policy": "latest"}))
        self.assertEqual(server.RECORD_FENCES[records[0]["id"]][1:], (2, 1))
        self.assertIn(records[1]["id"], server.DELETED)
        self.assertNotIn(records[2]["id"], server.RECORD_BY_ID)

    def test_eight_ninth_failed_admission_and_suppression_restart(self):
        kinds = [custom_value_kind(n) for n in range(9)]
        records = [value_record(k, n) for n, k in enumerate(kinds)]
        before = deepcopy(server.storage_state())
        with patch.object(server.STORAGE_BACKEND, "save", side_effect=BackendError(503, "offline")):
            with self.assertRaises(BackendError):
                server.upsert_records(records[:1])
        self.assertEqual(server.ACTIVE_INDEX_KINDS, set())
        self.assertEqual(self.backend.indices, {})
        self.assertEqual(server.storage_state(), before)
        server.upsert_records(records[:8], index_immediately=True)
        self.assertEqual(len(self.backend.indices), 8)
        self.assertEqual(self.backend.count(), 8)
        before = deepcopy(server.storage_state())
        with patch.object(server.STORAGE_BACKEND, "save") as save, \
                patch.object(server.INDEX_SCHEDULER, "reserve") as reserve:
            status, body = call("put", "/api/storage/v2/records", [records[8]])
        self.assertEqual(status, 429, body)
        save.assert_not_called()
        reserve.assert_not_called()
        self.assertEqual(server.storage_state(), before)
        stale, _, _ = server._index_job(server.effective_record(server.RECORD_BY_ID[records[0]["id"]]))
        server._delete_index_kind(kinds[0], principal=server.core_auth.SYSTEM)
        self.assertEqual(self.backend.count(), 7)
        self.assertEqual(len(server.ACTIVE_INDEX_KINDS), 8)  # Empty shard holds its slot.
        server.start_runtime(self.backend, storage=self.storage)
        self.assertEqual(len(server.ACTIVE_INDEX_KINDS), 7)
        self.assertEqual(self.backend.count(), 7)
        self.assertIn(records[0]["id"], server.INDEX_SUPPRESSED)
        self.assertEqual(server._execute_index(stale).error, "superseded")
        server.upsert_records(records[8:], index_immediately=True)
        self.assertEqual((len(self.backend.indices), self.backend.count()), (8, 8))
