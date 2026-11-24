"""EPIC-005 ITEM011/012: finite schema HTTP and snapshot contract."""
import os
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote

from test_server import call, server, valid_well, wait_until
from fake_search_backend import FakeSearch
from fake_storage_backend import FakeStorage
from schema_registry import SchemaRegistry, _identity
from search_backend import BackendError

ROOT = "/api/schema-service/v1/schema"
KIND = "test:local:Measurement:1.0.0"


def request(kind=KIND, status="DEVELOPMENT", schema=None):
    return {"schemaInfo": {"schemaIdentity": _identity(kind), "status": status},
            "schema": schema if schema is not None else {
                "type": "object", "properties": {"Name": {"type": "string"}}}}


class CoreSchemaTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.old_files = server.FILES_DIR
        server.FILES_DIR = Path(self.tmp.name)
        self.env = patch.dict(os.environ, {"OSDU_COMPAT_PROFILE": "core"})
        self.env.start()
        self.storage = FakeStorage()
        server.start_runtime(FakeSearch(), storage=self.storage)

    def tearDown(self):
        server.stop_runtime()
        self.env.stop()
        server.COMPAT_PROFILE = "legacy"
        server.start_runtime(FakeSearch(), storage=FakeStorage())
        server.FILES_DIR = self.old_files
        self.tmp.cleanup()

    def write(self, payload=None, method="post", root=ROOT):
        return call(method, root, payload if payload is not None else request())

    def listing(self, query=""):
        return call("get", ROOT + query)

    def test_raw_get_scope_provenance_alias_and_inventory(self):
        self.assertEqual(self.write()[0], 201)
        self.assertEqual(call("get", ROOT + "/" + quote(KIND, safe="")) ,
                         (200, request()["schema"]))
        self.assertEqual(call("get", "/schema-service/v1/schema/" + server.WELL_KIND),
                         (200, server.SCHEMA_REGISTRY.get(server.WELL_KIND)["schema"]))
        self.assertEqual(call("get", ROOT + "/test:local:Missing:1.0.0")[0], 404)
        self.assertEqual(self.listing()[1], {
            "schemaInfos": [], "count": 0, "totalCount": 0, "offset": 0})
        shared = self.listing("?scope=SHARED")[1]
        self.assertEqual(shared["count"], len(server.SUPPORTED_KINDS))
        self.assertTrue(all(i["scope"] == "SHARED" for i in shared["schemaInfos"]))
        info = call("get", "/info")[1]["compatibility"]
        self.assertFalse(info["localSchemas"]["upstreamVerified"])
        self.assertIn("schema-core", info["supportedFeatures"])
        self.assertNotIn("schema-core", info["deferredFeatures"])

    def test_lifecycle_and_durable_noops(self):
        p = request()
        self.assertEqual(self.write(p)[0], 201)
        before = deepcopy(server.storage_state())
        self.assertEqual(self.write(p)[0], 409)
        self.assertEqual(server.storage_state(), before)
        p["schema"]["properties"]["Value"] = {"type": "integer"}
        self.assertEqual(self.write(p, "put")[0], 200)
        self.assertEqual(self.listing("?status=DEVELOPMENT")[1]["count"], 1)
        p["schemaInfo"]["status"] = "PUBLISHED"
        revision = server.REVISION
        status, body = self.write(p, "put")
        self.assertEqual((status, body["status"]), (200, "PUBLISHED"))
        self.assertEqual(server.REVISION, revision + 1)
        self.assertEqual(self.listing()[1]["count"], 1)
        published = deepcopy(server.storage_state())
        saves = self.storage.saves
        self.assertEqual(self.write(p, "put")[0], 200)
        self.assertEqual(server.storage_state(), published)
        self.assertEqual(self.storage.saves, saves)
        changed = deepcopy(p)
        changed["schema"]["title"] = "immutable"
        self.assertEqual(self.write(changed, "put")[0], 409)
        p["schemaInfo"]["status"] = "DEVELOPMENT"
        self.assertEqual(self.write(p, "put")[0], 409)
        p["schemaInfo"]["status"] = "OBSOLETE"
        self.assertEqual(self.write(p, "put")[0], 200)
        before = deepcopy(server.storage_state())
        saves = self.storage.saves
        self.assertEqual(self.write(p, "put")[0], 200)
        self.assertEqual(server.storage_state(), before)
        self.assertEqual(self.storage.saves, saves)
        p["schemaInfo"]["status"] = "PUBLISHED"
        self.assertEqual(self.write(p, "put")[0], 409)
        self.assertEqual(self.write(request("test:local:New:1.0.0"), "put")[0], 201)

    def test_all_filters_latest_numeric_grouping_sort_and_pages(self):
        kinds = ["test:local:Measurement:2.0.0", "test:local:Measurement:10.0.0",
                 "test:other:Measurement:3.1.2", "other:local:Other:1.0.0"]
        for kind in kinds:
            p = request(kind)
            self.assertEqual(self.write(p)[0], 201)
            p["schemaInfo"]["status"] = "PUBLISHED"
            self.assertEqual(self.write(p, "put")[0], 200)
        self.assertEqual(self.write(request("test:local:Measurement:20.0.0"))[0], 201)
        for query, count in (("?authority=test", 3), ("?source=other", 1),
                             ("?entityType=Other", 1), ("?schemaVersionMajor=10", 1),
                             ("?schemaVersionMinor=1", 1), ("?schemaVersionPatch=2", 1),
                             ("?status=DEVELOPMENT", 1), ("?scope=SHARED&authority=test", 0)):
            with self.subTest(query=query):
                self.assertEqual(self.listing(query)[1]["count"], count)
        result = self.listing("?latestVersion=true")[1]
        self.assertEqual(result["count"], 3)
        identities = [i["schemaIdentity"] for i in result["schemaInfos"]]
        self.assertIn(_identity(kinds[1]), identities)
        self.assertNotIn(_identity(kinds[0]), identities)
        self.assertNotIn(_identity("test:local:Measurement:20.0.0"), identities)
        self.assertEqual(self.listing("?latestVersion=true&schemaVersionMajor=2")[1]["count"], 1)
        page = self.listing("?offset=1&limit=2")[1]
        self.assertEqual([i["schemaIdentity"] for i in page["schemaInfos"]],
                         [_identity(k) for k in sorted(kinds)[1:3]])
        self.assertEqual((page["count"], page["totalCount"], page["offset"]), (2, 4, 3))
        self.assertEqual(self.listing("?limit=0")[1],
                         {"schemaInfos": [], "count": 0, "totalCount": 4, "offset": 0})
        self.assertEqual(self.listing("?offset=99&limit=100")[1]["offset"], 99)

    def test_strict_params_requests_readonly_system_and_finite_vocabulary(self):
        for q in ("?x=1", "?limit=-1", "?limit=101", "?limit=1.0", "?limit=",
                  "?offset=-1", "?latestVersion=yes", "?status=DEPRECATED", "?scope=ALL",
                  "?authority=", "?limit=1&limit=2", "?schemaVersionMajor=-1"):
            with self.subTest(q=q):
                self.assertEqual(self.listing(q)[0], 400)
        for p in ({}, {"kind": KIND, "schema": {"type": "object"}},
                  {**request(), "ignored": True}, request(status="PUBLISHED"),
                  request(status="DEPRECATED"), request(schema={"type": "object", "allOf": []}),
                  request(schema={"type": "object", "$ref": "https://example.invalid/s"})):
            with self.subTest(payload=p):
                self.assertEqual(self.write(p)[0], 400)
        self.assertEqual(self.write(request(status="PUBLISHED"), "put")[0], 400)
        builtin = server.SCHEMA_REGISTRY.get(server.WELL_KIND)
        p = {"schemaInfo": {"schemaIdentity": builtin["schemaInfo"]["schemaIdentity"],
                            "status": "PUBLISHED"}, "schema": builtin["schema"]}
        self.assertEqual(self.write(p, "put")[0], 409)
        self.assertEqual(self.write({}, "put", "/api/schema-service/v1/schemas/system")[0], 501)
        self.assertEqual(self.write(request(), root=ROOT + "?unknown=1")[0], 400)

    def test_restart_import_development_references_mapping_and_custom_audit(self):
        p = request()
        uri = "urn:osdu-lite:schema:test:local:Leaf:1.0.0"
        p["references"] = [{"$id": uri, "type": "object",
                            "properties": {"Value": {"type": "integer"}}}]
        p["schema"]["properties"]["Nested"] = {"$ref": uri}
        self.assertEqual(self.write(p)[0], 201)
        exported = server.SCHEMA_REGISTRY.export_custom()
        exported[0]["createdAt"] = exported[0]["schemaInfo"]["created"] = "2001-01-01T00:00:00Z"
        exported[0]["createdBy"] = exported[0]["schemaInfo"]["createdBy"] = "custom-author"
        registry = SchemaRegistry()
        registry.import_custom(exported)
        self.assertEqual(registry.export_custom(), exported)
        self.assertEqual(registry.field_capabilities([KIND], ["data.Nested.Value"])["data.Nested.Value"]["type"], "long")
        self.storage.state["schemas"] = exported
        server.stop_runtime()
        server.start_runtime(FakeSearch(), storage=self.storage)
        self.assertEqual(server.SCHEMA_REGISTRY.export_custom(), exported)
        p["schemaInfo"]["status"] = "PUBLISHED"
        self.assertEqual(self.write(p, "put")[0], 200)
        self.assertEqual(server.SCHEMA_REGISTRY.get(KIND)["createdBy"], "custom-author")

    def test_failed_save_restores_create_edit_status_and_compiled_mapping(self):
        p = request()
        for phase in ("create", "edit", "publish", "obsolete"):
            if phase == "edit":
                self.assertEqual(self.write(p)[0], 201)
                p["schema"]["properties"]["Name"] = {"type": "integer"}
            elif phase == "publish":
                p["schemaInfo"]["status"] = "PUBLISHED"
            elif phase == "obsolete":
                self.assertEqual(self.write(p, "put")[0], 200)
                p["schemaInfo"]["status"] = "OBSOLETE"
            before = deepcopy(server.storage_state())
            fields = server.SCHEMA_REGISTRY.mapping(KIND) if phase != "create" else None
            with patch.object(self.storage, "save", side_effect=BackendError(503, "injected")):
                self.assertEqual(self.write(p, "post" if phase == "create" else "put")[0], 503)
            self.assertEqual(server.storage_state(), before)
            self.assertEqual(server.SCHEMA_REGISTRY.mapping(KIND) if fields else None, fields)

    def test_record_admission_only_published_obsolete_read_and_rebuild(self):
        p = request()
        self.assertEqual(self.write(p)[0], 201)
        rec = valid_well()
        rec.update(kind=KIND, data={"Name": "ok"})
        self.assertEqual(call("put", "/api/storage/v2/records", [rec])[0], 409)
        p["schemaInfo"]["status"] = "PUBLISHED"
        self.assertEqual(self.write(p, "put")[0], 200)
        self.assertEqual(call("put", "/api/storage/v2/records", [rec])[0], 201)
        p["schemaInfo"]["status"] = "OBSOLETE"
        self.assertEqual(self.write(p, "put")[0], 200)
        self.assertEqual(call("put", "/api/storage/v2/records", [rec])[0], 409)
        self.assertEqual(call("get", "/api/storage/v2/records/" + rec["id"])[0], 200)
        server.stop_runtime()
        backend = FakeSearch()
        server.start_runtime(backend, storage=self.storage)
        self.assertEqual(server.SCHEMA_REGISTRY.get(KIND)["status"], "OBSOLETE")
        self.assertEqual(call("put", "/api/storage/v2/records", [rec])[0], 409)
        self.assertEqual(call("get", "/api/storage/v2/records/" + rec["id"])[0], 200)
        self.assertTrue(wait_until(lambda: server.INDEX_SCHEDULER.pending("index") == 0))

    def test_legacy_direct_publish_and_deprecated_wire_mapping(self):
        server.COMPAT_PROFILE = "legacy"
        p = {"kind": KIND, "schema": request()["schema"]}
        self.assertEqual(self.write(p)[0], 201)
        p["status"] = "DEPRECATED"
        self.assertEqual(self.write(p, "put")[1]["schemaInfo"]["status"], "DEPRECATED")
        self.assertEqual(call("get", ROOT + "/" + KIND)[1]["status"], "DEPRECATED")
        self.assertEqual(server.SCHEMA_REGISTRY.export_custom()[0]["status"], "OBSOLETE")

    def test_development_dependency_edits_recompile_but_cannot_change_published_contracts(self):
        dependency = request("test:local:Leaf:1.0.0")
        self.assertEqual(self.write(dependency)[0], 201)
        p = request(schema={"type": "object", "properties": {
            "Nested": {"$ref": "urn:osdu-lite:schema:test:local:Leaf:1.0.0"}}})
        self.assertEqual(self.write(p)[0], 201)
        dependency["schema"]["properties"]["Name"] = {"type": "integer"}
        self.assertEqual(self.write(dependency, "put")[0], 200)
        self.assertEqual(server.SCHEMA_REGISTRY.field_capabilities(
            [KIND], ["data.Nested.Name"])["data.Nested.Name"]["type"], "long")
        p["schemaInfo"]["status"] = "PUBLISHED"
        self.assertEqual(self.write(p, "put")[0], 200)
        before = deepcopy(server.storage_state())
        dependency["schema"]["properties"]["Name"] = {"type": "string"}
        self.assertEqual(self.write(dependency, "put")[0], 409)
        self.assertEqual(server.storage_state(), before)

    def test_published_reference_siblings_cannot_hide_dependency_constraint_changes(self):
        dependency = request("test:local:Leaf:1.0.0")
        dependency["schema"]["properties"]["Name"]["minLength"] = 2
        self.assertEqual(self.write(dependency)[0], 201)
        published = request(schema={"type": "object", "properties": {
            "Nested": {"$ref": "urn:osdu-lite:schema:test:local:Leaf:1.0.0#/properties/Name",
                       "minLength": 5}}})
        self.assertEqual(self.write(published)[0], 201)
        published["schemaInfo"]["status"] = "PUBLISHED"
        self.assertEqual(self.write(published, "put")[0], 200)
        record = valid_well()
        record.update(kind=KIND, data={"Nested": "1234567"})
        self.assertEqual(call("put", "/api/storage/v2/records", [record])[0], 201)
        before = deepcopy(server.storage_state())
        dependency["schema"]["properties"]["Name"]["minLength"] = 10
        self.assertEqual(self.write(dependency, "put")[0], 409)
        self.assertEqual(server.storage_state(), before)
        server.SCHEMA_REGISTRY.validate_record(server.RECORD_BY_ID[record["id"]])


if __name__ == "__main__":
    unittest.main()
