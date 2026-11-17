import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from copy import deepcopy
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from schema_registry import SchemaRegistry, SchemaError

ROOT = Path(__file__).resolve().parents[2]
KIND = "test:local:Measurement:1.0.0"
URN = "urn:osdu-lite:schema:"


def registration(kind=KIND):
    return {
        "kind": kind,
        "schema": {
            "type": "object",
            "properties": {
                "Name": {"type": "string"},
                "Value": {"type": "number"},
                "When": {"type": "string", "format": "date-time"},
                "Point": {"type": "object", "required": ["lat", "lon"],
                          "additionalProperties": False,
                          "properties": {"lat": {"type": "number"}, "lon": {"type": "number"}}},
                "Labels": {"type": "array", "items": {"type": "string"}},
                "Objects": {"type": "array", "items": {
                    "type": "object", "properties": {"Hidden": {"type": "integer"}}}},
            },
        },
        "fieldMappings": {"data.Point": "geo_point"},
    }


def record(kind=KIND):
    return {"id": "test:1", "kind": kind, "data": {"Value": 2, "Point": {"lat": 1, "lon": 2}},
            "acl": {"owners": ["owner"], "viewers": []},
            "legal": {"legaltags": ["tag"], "otherRelevantDataCountries": ["SA"]}}


class TestSchemaRegistry(unittest.TestCase):
    def setUp(self):
        self.registry = SchemaRegistry()

    def assert_rejected(self, payload, status=400):
        before = self.registry.kinds()
        with self.assertRaises(SchemaError) as caught:
            self.registry.register(payload, "POST")
        self.assertEqual(caught.exception.status, status)
        self.assertEqual(self.registry.kinds(), before)

    def test_import_and_resolution_are_offline_and_thread_free(self):
        script = """
import socket, threading
def denied(*a, **k): raise AssertionError('network')
socket.socket = denied
before = set(threading.enumerate())
from schema_registry import SchemaRegistry
r = SchemaRegistry()
for kind in r.kinds(): r.validate_record({'id':'x','kind':kind,'data':{},
 'acl':{'owners':['x'],'viewers':[]},
 'legal':{'legaltags':['x'],'otherRelevantDataCountries':[]}})
assert set(threading.enumerate()) == before
"""
        result = subprocess.run([sys.executable, "-c", script], cwd=ROOT / "server",
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_catalog_hashes_and_declared_mappings(self):
        catalog = json.loads((ROOT / "server/schemas/catalog.json").read_text())
        self.assertEqual(len(self.registry.kinds()), 4)
        for entry in catalog["schemas"]:
            caps = self.registry.field_capabilities([entry["kind"]], list(entry["mappedFields"]))
            self.assertEqual(caps, entry["mappedFields"])
        with tempfile.TemporaryDirectory() as temp:
            for path in (ROOT / "server/schemas").glob("*.json"):
                (Path(temp) / path.name).write_bytes(path.read_bytes())
            with (Path(temp) / "common.json").open("ab") as stream:
                stream.write(b" ")
            with self.assertRaises(SchemaError):
                SchemaRegistry(Path(temp))

    def test_every_json_record_and_manifest_fixture(self):
        count = 0
        for path in (ROOT / "examples").rglob("*.json"):
            obj = json.loads(path.read_text(encoding="utf-8"))
            def walk(value):
                nonlocal count
                if isinstance(value, dict):
                    if value.get("kind") in self.registry.kinds() and "data" in value:
                        rec = deepcopy(value)
                        rec.setdefault("acl", record()["acl"])
                        rec.setdefault("legal", record()["legal"])
                        self.registry.validate_record(rec)
                        count += 1
                    for child in value.values():
                        walk(child)
                elif isinstance(value, list):
                    for child in value:
                        walk(child)
            walk(obj)
        self.assertGreaterEqual(count, 13)

    def test_fifth_kind_mapping_and_detached_state(self):
        payload = registration()
        status, response = self.registry.register(payload, "POST")
        self.assertEqual(status, 201)
        self.assertEqual(response["schemaInfo"]["schemaIdentity"]["schemaVersionPatch"], 0)
        self.registry.validate_record(record())
        caps = self.registry.field_capabilities([KIND], ["data.Value", "data.Labels", "data.Point"])
        self.assertEqual(caps["data.Value"], {"type": "double", "array": False})
        self.assertEqual(caps["data.Labels"], {"type": "keyword", "array": True})
        self.assertEqual(caps["data.Point"]["type"], "geo_point")
        for field in ("data.Objects.Hidden", "data.Objects", "data.Unknown"):
            with self.assertRaises(SchemaError):
                self.registry.field_capabilities([KIND], [field])
        mapping = self.registry.mapping(KIND)
        self.assertFalse(mapping["dynamic"])
        self.assertEqual(mapping["properties"]["data"]["properties"]["Name"]["fields"]["text"],
                         {"type": "text"})
        response["schema"]["type"] = "string"
        payload["schema"]["type"] = "number"
        payload["schema"]["properties"]["Value"]["type"] = "string"
        self.registry.validate_record(record())
        self.assertEqual(self.registry.get(KIND)["schema"]["type"], "object")

    def test_identity_normalization_and_mismatch(self):
        identity = dict(authority="test", source="local", entityType="Measurement",
                        schemaVersionMajor=1, schemaVersionMinor=0, schemaVersionPatch=0)
        for missing_kind in (True, False):
            payload = registration()
            payload["schemaInfo"] = {"schemaIdentity": deepcopy(identity)}
            if missing_kind:
                del payload["kind"]
            else:
                del payload["schemaInfo"]["schemaIdentity"]["schemaVersionPatch"]
            self.assertEqual(self.registry.register(payload, "POST")[1]["kind"], KIND)
        payload = registration()
        payload["schemaInfo"] = {"schemaIdentity": {**identity, "schemaVersionPatch": 2}}
        self.assert_rejected(payload)
        self.assert_rejected({"schemaInfo": {"schemaIdentity": {"entityType": "Only"}},
                              "schema": {"type": "object"}})
        self.assert_rejected(registration("test:local:X:1.0"))

    def test_lifecycle_and_builtin_protection(self):
        payload = registration()
        self.assertEqual(self.registry.register(payload, "POST")[0], 201)
        self.assertEqual(self.registry.register(payload, "POST")[0], 200)
        changed = deepcopy(payload)
        changed["schema"]["properties"]["Value"]["type"] = "string"
        self.assert_rejected(changed, 409)
        deprecated = {**payload, "status": "DEPRECATED"}
        self.assert_rejected(deprecated, 409)  # lifecycle transitions are PUT only
        for _ in range(2):
            status, response = self.registry.register(deprecated, "PUT")
            self.assertEqual((status, response["schemaInfo"]["status"]), (200, "DEPRECATED"))
        with self.assertRaises(SchemaError) as caught:
            self.registry.validate_record(record())
        self.assertEqual(caught.exception.status, 409)
        self.assertIsNotNone(self.registry.mapping(KIND))
        self.assert_rejected(payload, 409)
        for kind in self.registry.kinds()[:4]:
            builtin = self.registry.get(kind)
            self.assertEqual(self.registry.register(builtin, "POST")[0], 200)
            self.assert_rejected({**builtin, "status": "DEPRECATED",
                                  "schemaInfo": {**builtin["schemaInfo"], "status": "DEPRECATED"}}, 409)
        self.assert_rejected({**registration("test:local:Other:1.0.0"), "status": "DRAFT"})
        self.assert_rejected({**registration("test:local:Other:1.0.0"), "status": "PUBLISHED",
                              "schemaInfo": {"status": "DEPRECATED"}})

    def test_refs_closed_missing_duplicate_pointer_cycle_and_sibling(self):
        payload = registration()
        refid = URN + "test:local:Types:1.0.0"
        payload["references"] = [{"$id": refid, "$defs": {"value": {"type": "number"}}}]
        payload["schema"]["properties"]["Value"] = {"$ref": refid + "#/$defs/value", "minimum": 1}
        with patch("socket.socket", side_effect=AssertionError("network")):
            self.registry.register(payload, "POST")
            self.registry.validate_record(record())
        bad = record()
        bad["data"]["Value"] = 0
        with self.assertRaises(SchemaError):
            self.registry.validate_record(bad)
        for ref in ("https://example.com/schema", "file:///tmp/schema", "#/$defs/missing",
                    "#/bad~2escape", URN + "test:local:Missing:1.0.0"):
            candidate = registration("test:local:Bad:1.0.0")
            candidate["schema"]["properties"]["Value"] = {"$ref": ref}
            self.assert_rejected(candidate)
        for refs in ([{"$id": refid, "type": "string"}],
                     [{"$id": URN + KIND, "type": "string"}]):
            self.assert_rejected({**registration("test:local:Duplicate:1.0.0"), "references": refs})
        for cycle in ({"type": "object", "properties": {"self": {"$ref": "#"}}},
                      {"type": "object", "$defs": {"unused": {"$ref": "#/$defs/unused"}}}):
            self.assert_rejected({"kind": "test:local:Cycle:1.0.0", "schema": cycle})

    def test_profile_and_size_depth_field_kind_limits(self):
        for node in ({"pattern": "x"}, {"type": ["string", "null"]}, {"anyOf": []},
                     {"additionalProperties": {}}, {"format": "date"},
                     {"$id": "https://example.com/x"}, {"minimum": float("inf")}):
            payload = registration()
            payload["schema"]["properties"]["Bad"] = node
            self.assert_rejected(payload)
        self.assert_rejected({**registration(), "references": [{}] * 9})
        payload = registration()
        payload["schema"]["description"] = "x" * 131072
        self.assert_rejected(payload)
        schema = {"type": "string"}
        for _ in range(17):
            schema = {"type": "object", "properties": {"x": schema}}
        self.assert_rejected({"kind": KIND, "schema": schema})
        payload = {"kind": KIND, "schema": {"type": "object", "properties": {
            str(i): {"type": "string"} for i in range(129)}}}
        self.assert_rejected(payload)
        for i in range(28):
            self.registry.register({"kind": f"test:local:K{i}:1.0.0", "schema": {"type": "object"}}, "POST")
        self.assert_rejected(registration(), 429)

    def test_envelope_value_and_geo_validation(self):
        self.registry.register(registration(), "POST")
        changes = [("id", ""), ("id", 1), ("id", "x" * 513), ("kind", []),
                   ("data", []), ("acl", []), ("legal", []), ("version", True)]
        for key, value in changes:
            rec = record()
            rec[key] = value
            with self.subTest(key=key, value=str(value)[:20]), self.assertRaises(SchemaError):
                self.registry.validate_record(rec)
        for acl in ({"owners": [], "viewers": []}, {"owners": ["x"]},
                    {"owners": ["x"], "viewers": [1]}, {"owners": [""] , "viewers": []},
                    {"owners": ["x"] * 65, "viewers": []}):
            with self.assertRaises(SchemaError):
                self.registry.validate_record({**record(), "acl": acl})
        for legal in ({}, {"legaltags": "x", "otherRelevantDataCountries": []},
                      {"legaltags": ["x"]}, {"legaltags": ["x"], "otherRelevantDataCountries": "SA"},
                      {"legaltags": ["x"], "otherRelevantDataCountries": [1]}):
            with self.assertRaises(SchemaError):
                self.registry.validate_record({**record(), "legal": legal})
        for data in ({"Value": "bad"}, {"When": "not-a-date"}, {"When": "2026-99-99T00:00:00Z"},
                     {"Point": {"lat": 91, "lon": 0}}, {"Point": {"lat": 0, "lon": 181}},
                     {"Point": {"lat": 1, "lon": 2, "extra": 1}}, {"Unknown": float("nan")}):
            with self.assertRaises(SchemaError):
                self.registry.validate_record({**record(), "data": data})
        self.registry.validate_record({**record(), "data": {"When": "2026-11-16T00:00:00Z",
                                                         "Legacy": {"Unknown": True}}})

    def test_capability_conflicts_and_invalid_point_declarations(self):
        self.registry.register(registration(), "POST")
        other = registration("test:local:Other:1.0.0")
        other["schema"]["properties"]["Value"]["type"] = "string"
        self.registry.register(other, "POST")
        with self.assertRaises(SchemaError):
            self.registry.field_capabilities([KIND, other["kind"]], ["data.Value"])
        for mappings in ({"data.Missing": "geo_point"}, {"data.Value": "double"},
                         {"data.Objects.Hidden": "geo_point"},
                         {"data.Point": "geo_point", "data.Value": "geo_point"}):
            self.assert_rejected({**registration("test:local:Bad:1.0.0"), "fieldMappings": mappings})

    def test_registry_byte_value_depth_and_expansion_limits(self):
        for i in range(28):
            payload = {"kind": f"test:local:Large{i}:1.0.0", "schema": {
                "type": "object", "description": "x" * 125000}}
            try:
                self.registry.register(payload, "POST")
                retained = {"entries": self.registry._entries, "documents": self.registry._documents}
                self.assertLessEqual(len(json.dumps(retained, ensure_ascii=False, sort_keys=True,
                                                     separators=(",", ":")).encode()), 2 * 1024 * 1024)
            except SchemaError as exc:
                self.assertEqual(exc.status, 429)
                self.assertLess(i, 28)
                break
        else:
            self.fail("Registry byte capacity not enforced")
        self.registry = SchemaRegistry()
        payload = registration()
        payload["schema"]["$defs"] = {"x0": {"type": "string"}}
        for i in range(1, 15):
            payload["schema"]["$defs"][f"x{i}"] = {"type": "object", "properties": {
                "a": {"$ref": f"#/$defs/x{i-1}"}, "b": {"$ref": f"#/$defs/x{i-1}"}}}
        self.assert_rejected(payload)
        self.registry.register(registration(), "POST")
        value = {}
        for _ in range(17):
            value = {"nested": value}
        with self.assertRaises(SchemaError):
            self.registry.validate_record({**record(), "data": {"Unknown": value}})
        with self.assertRaises(SchemaError):
            self.registry.validate_record({**record(), "data": {"Unknown": "x" * 65536}})

    def test_all_known_sample_scalars_reject_wrong_types(self):
        catalog = json.loads((ROOT / "server/schemas/catalog.json").read_text())
        checked = 0
        for entry in catalog["schemas"]:
            for path, cap in entry["mappedFields"].items():
                if cap["array"]:
                    continue
                data = {}
                current = data
                parts = path.split(".")[1:]
                for part in parts[:-1]:
                    current = current.setdefault(part, {})
                current[parts[-1]] = 42 if cap["type"] in ("keyword", "date") else "not-numeric"
                with self.subTest(kind=entry["kind"], path=path), self.assertRaises(SchemaError):
                    self.registry.validate_record({**record(entry["kind"]), "data": data})
                checked += 1
        self.assertEqual(checked, 29)

    def test_registration_rejects_nonstring_keys_and_deep_const(self):
        payload = registration()
        payload["schema"]["properties"] = {1: {"type": "string"}}
        self.assert_rejected(payload)
        payload = registration()
        value = {}
        for _ in range(17):
            value = {"x": value}
        payload["schema"]["properties"]["Value"] = {"const": value}
        self.assert_rejected(payload)

    def test_ref_target_is_a_valid_finite_schema_not_arbitrary_literal(self):
        for target in ({"properties": 5}, {"minLength": {}}, {"pattern": "x"}):
            payload = registration()
            payload["schema"]["$defs"] = {"literal": {"const": target}}
            payload["schema"]["properties"]["Value"] = {"$ref": "#/$defs/literal/const"}
            self.assert_rejected(payload)

    def test_reset_discards_custom_kinds_and_reference_documents(self):
        payload = registration()
        payload["references"] = [{"$id": URN + "test:local:Ref:1.0.0", "type": "string"}]
        self.registry.register(payload, "POST")
        self.registry.reset()
        self.assertEqual(len(self.registry.kinds()), 4)
        self.assertIsNone(self.registry.get(KIND))
        payload = registration()
        payload["schema"]["properties"]["Name"] = {"$ref": URN + "test:local:Ref:1.0.0"}
        self.assert_rejected(payload)

    def test_total_mapped_leaf_limit_includes_fixed_envelope_fields(self):
        payload = {"kind": KIND, "schema": {"type": "object", "properties": {
            f"f{i}": {"type": "string"} for i in range(124)}}}
        self.assert_rejected(payload)
        del payload["schema"]["properties"]["f123"]
        self.registry.register(payload, "POST")
        self.assertEqual(len(self.registry._fields[KIND]), 128)

    def test_format_does_not_change_nonstring_mapping_type(self):
        payload = registration()
        payload["schema"]["properties"]["Value"]["format"] = "date-time"
        self.registry.register(payload, "POST")
        self.assertEqual(self.registry.field_capabilities([KIND], ["data.Value"])["data.Value"]["type"],
                         "double")

    def test_identity_null_and_boolean_content_changes_are_not_idempotent(self):
        payload = registration()
        payload["schemaInfo"] = {"schemaIdentity": None}
        self.assert_rejected(payload)
        payload = registration()
        payload["schema"]["properties"]["Value"] = {"const": 1}
        self.registry.register(payload, "POST")
        payload["schema"]["properties"]["Value"] = {"const": True}
        self.assert_rejected(payload, 409)

    def test_ref_sibling_properties_preserve_type_capabilities(self):
        payload = registration()
        payload["schema"]["$defs"] = {"base": {"type": "object", "properties": {
            "Name": {"type": "string"}}}}
        payload["schema"]["properties"]["Details"] = {
            "$ref": "#/$defs/base", "properties": {"Name": {"minLength": 2}}}
        self.registry.register(payload, "POST")
        self.assertEqual(self.registry.field_capabilities([KIND], ["data.Details.Name"])[
            "data.Details.Name"]["type"], "keyword")
        with self.assertRaises(SchemaError):
            self.registry.validate_record({**record(), "data": {"Details": {"Name": "x"}}})
        self.registry.validate_record({**record(), "data": {"Details": {"Name": "ok"}}})


if __name__ == "__main__":
    unittest.main()
