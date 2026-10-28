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
        with self.assertRaises(BackendError):
            server.upsert_records([record, bad], index_immediately=True)
        self.assertEqual(call("get", "/api/indexer/v2/readiness_check")[0], 503)
        self.assertEqual(call("delete", "/api/storage/v2/records/" + bad["id"] + "?hardDelete=true")[0], 204)
        self.assertEqual(self.search()["totalCount"], 1)

    def test_seed_reindex_delete_restore_and_reset(self):
        server.SEARCH_BACKEND.bulk([valid_well("company-prod:master-data--Well:old-remote-document")])
        server.load_full_sample()
        self.assertEqual(self.search()["totalCount"], 15)
        base = "/api/indexer/v2"
        server.SEARCH_BACKEND.request("DELETE", "/" + server.SEARCH_BACKEND.index)
        self.assertEqual(call("put", base + "/partitions/provision")[0], 200)
        self.assertEqual(self.search()["totalCount"], 0)
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
        server.upsert_records(records, index_immediately=True)
        result = self.search(offset=1000, limit=10, returnedFields=["id"],
                             aggregateBy=[{"field": "id", "aggregateByType": "count"}])
        self.assertEqual(result["totalCount"], 1004)
        self.assertEqual(len(result["results"]), 4)
        self.assertEqual(set(result["results"][0]), {"id"})
        self.assertEqual(result["aggregates"][0]["Count"]["count"], 1004)
        self.assertEqual(self.search(query_as_owner=True)["totalCount"], 1001)
        _, first = call("post", "/api/search/v2/query_with_cursor", {"kind": "*", "limit": 1000})
        server.soft_delete_records([records[1000]["id"]])
        _, second = call("post", "/api/search/v2/query_with_cursor", {"cursor": first["cursor"]})
        self.assertEqual(len(second["results"]), 4)  # intentionally materialized snapshot
        self.assertEqual(second["totalCount"], 1004)
        stats = server.SEARCH_BACKEND.request("GET", "/" + server.SEARCH_BACKEND.index + "/_stats/search")
        self.assertEqual(stats["_all"]["total"]["search"]["open_contexts"], 0)
