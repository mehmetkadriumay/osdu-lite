"""Finite, volatile schema catalog. No network retrieval or runtime startup."""
import datetime
import hashlib
import json
import math
import re
import threading
from copy import deepcopy
from pathlib import Path
from urllib.parse import unquote

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError as InvalidSchema
from referencing import Registry, Resource
from referencing.exceptions import NoSuchResource
from referencing.jsonschema import DRAFT202012


URN = "urn:osdu-lite:schema:"
DIALECT = "https://json-schema.org/draft/2020-12/schema"
KIND = re.compile(r"([^:\s]+):([^:\s]+):([^:\s]+):(\d+)\.(\d+)\.(\d+)\Z", re.ASCII)
VOCABULARY = {
    "$schema", "$id", "$defs", "$ref", "title", "description", "type",
    "properties", "required", "additionalProperties", "items", "enum", "const",
    "minimum", "maximum", "minLength", "maxLength", "minItems", "maxItems", "format",
}
TYPES = {"object", "array", "string", "integer", "number", "boolean", "null"}
ENGINE_TYPES = {"string": "keyword", "integer": "long", "number": "double", "boolean": "boolean"}
IDENTITY_KEYS = ("authority", "source", "entityType", "schemaVersionMajor",
                 "schemaVersionMinor", "schemaVersionPatch")
FORMAT = FormatChecker(formats=[])


class SchemaError(ValueError):
    def __init__(self, message, status=400):
        super().__init__(message[:512])
        self.status = status


@FORMAT.checks("date-time")
def _date_time(value):
    if not isinstance(value, str):
        return True
    if not re.fullmatch(r"\d{4}-\d\d-\d\d[Tt]\d\d:\d\d:\d\d(?:\.\d+)?(?:[Zz]|[+-]\d\d:\d\d)", value):
        return False
    try:
        datetime.datetime.fromisoformat(value.upper().replace("Z", "+00:00"))
        # fromisoformat accepts out-of-range timezone minutes.
        return value[-1].upper() == "Z" or (int(value[-5:-3]) <= 23 and int(value[-2:]) <= 59)
    except ValueError:
        return False


def _closed_retrieve(uri):
    raise NoSuchResource(ref=uri)


def _json_bytes(value):
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False,
                          sort_keys=True, separators=(",", ":")).encode("utf-8")
    except (ValueError, TypeError, RecursionError, UnicodeError) as exc:
        raise SchemaError("Invalid finite JSON value") from exc


def _bounded_value(value, depth=0):
    if depth > 16:
        raise SchemaError("Value nesting exceeds 16")
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise SchemaError("JSON object keys must be strings")
        for child in value.values():
            _bounded_value(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            _bounded_value(child, depth + 1)
    elif isinstance(value, float) and not math.isfinite(value):
        raise SchemaError("Numbers must be finite")


def _identity(kind):
    if not isinstance(kind, str) or not 1 <= len(kind) <= 256:
        raise SchemaError("kind must be a full kind string of at most 256 characters")
    match = KIND.fullmatch(kind)
    if not match:
        raise SchemaError("kind requires authority:source:entityType:major.minor.patch")
    parts = match.groups()
    return dict(zip(IDENTITY_KEYS, (*parts[:3], *(int(v) for v in parts[3:]))))


def _normalize(payload):
    if not isinstance(payload, dict):
        raise SchemaError("Schema registration must be an object")
    info = payload.get("schemaInfo", {})
    if not isinstance(info, dict):
        raise SchemaError("schemaInfo must be an object")
    supplied = info.get("schemaIdentity")
    if "schemaIdentity" in info and not isinstance(supplied, dict):
        raise SchemaError("schemaIdentity must be an object")
    kind = payload.get("kind")
    if kind is None:
        if supplied is None or any(key not in supplied for key in IDENTITY_KEYS):
            raise SchemaError("Complete schemaIdentity or explicit full kind required")
        for key in IDENTITY_KEYS[:3]:
            if not isinstance(supplied[key], str):
                raise SchemaError("Identity components must be strings")
        for key in IDENTITY_KEYS[3:]:
            if type(supplied[key]) is not int or supplied[key] < 0:
                raise SchemaError("Identity versions must be nonnegative integers")
        kind = ":".join(supplied[key] for key in IDENTITY_KEYS[:3]) + ":" + ".".join(
            str(supplied[key]) for key in IDENTITY_KEYS[3:])
    identity = _identity(kind)
    if supplied is not None:
        legacy = {**supplied}
        if "kind" in payload:
            legacy.setdefault("schemaVersionPatch", 0)
        if set(legacy) != set(IDENTITY_KEYS):
            raise SchemaError("schemaIdentity requires all full-kind components")
        for key in IDENTITY_KEYS:
            if type(legacy[key]) is not type(identity[key]) or legacy[key] != identity[key]:
                raise SchemaError(f"schemaIdentity mismatch: {key}")
    status = payload.get("status", info.get("status", "PUBLISHED"))
    if status not in ("PUBLISHED", "DEPRECATED"):
        raise SchemaError("Only PUBLISHED and DEPRECATED statuses supported")
    if "status" in info and info["status"] != status:
        raise SchemaError("Conflicting status fields")
    return kind, identity, status


def _profile(schema, depth=0, root=True):
    if depth > 16:
        raise SchemaError("Schema nesting exceeds 16")
    if not isinstance(schema, dict) or set(schema) - VOCABULARY:
        raise SchemaError("Unsupported schema node or keyword")
    if "$id" in schema:
        if not root or not isinstance(schema["$id"], str) or not schema["$id"].startswith(URN):
            raise SchemaError("$id must be a root local schema URN")
        _identity(schema["$id"][len(URN):])
    if "$schema" in schema and schema["$schema"] != DIALECT:
        raise SchemaError("Only Draft 2020-12 supported")
    if "type" in schema and (not isinstance(schema["type"], str) or schema["type"] not in TYPES):
        raise SchemaError("Unsupported type or type union")
    if "format" in schema and schema["format"] != "date-time":
        raise SchemaError("Only date-time format supported")
    if "additionalProperties" in schema and type(schema["additionalProperties"]) is not bool:
        raise SchemaError("additionalProperties must be boolean")
    for key in ("const", "enum"):
        if key in schema:
            _bounded_value(schema[key])
    for key in ("properties", "$defs"):
        if key in schema:
            if not isinstance(schema[key], dict):
                raise SchemaError(f"{key} must be an object")
            for name, child in schema[key].items():
                if not isinstance(name, str):
                    raise SchemaError("Schema names must be strings")
                if key == "properties" and (not name or "." in name):
                    raise SchemaError("Property names must be nonempty and contain no dots")
                _profile(child, depth + 1, False)
    if "items" in schema:
        _profile(schema["items"], depth + 1, False)


def _target(ref, current, documents):
    if not isinstance(ref, str):
        raise SchemaError("$ref must be a string")
    base, marker, fragment = ref.partition("#")
    if base and not base.startswith(URN):
        raise SchemaError("Only local schema references supported")
    if not base and not marker:
        raise SchemaError("Reference must be a fragment or local schema URN")
    base = base or current
    if base not in documents:
        raise SchemaError("Missing local reference")
    if re.search(r"%(?![0-9a-fA-F]{2})", fragment):
        raise SchemaError("Malformed JSON Pointer")
    try:
        fragment = unquote(fragment, errors="strict")
    except UnicodeError as exc:
        raise SchemaError("Malformed JSON Pointer") from exc
    node = documents[base]
    if fragment:
        if not fragment.startswith("/"):
            raise SchemaError("Only JSON Pointer fragments supported")
        for segment in fragment[1:].split("/"):
            if re.search(r"~(?![01])", segment):
                raise SchemaError("Malformed JSON Pointer")
            segment = segment.replace("~1", "/").replace("~0", "~")
            if not isinstance(node, dict) or segment not in node:
                raise SchemaError("Missing JSON Pointer target")
            node = node[segment]
    if not isinstance(node, dict):
        raise SchemaError("Reference must target a schema object")
    return node, base


def _merge_declarations(left, right):
    """Combine reference siblings for mapping only; validators retain both constraints."""
    result = dict(left)
    for key, value in right.items():
        if key in ("properties", "$defs"):
            children = dict(result.get(key, {}))
            for name, child in value.items():
                children[name] = _merge_declarations(children.get(name, {}), child)
            result[key] = children
        elif key == "items":
            result[key] = _merge_declarations(result.get(key, {}), value)
        elif key == "type" and key in result and result[key] != value:
            if {result[key], value} == {"integer", "number"}:
                result[key] = "integer"
            else:
                raise SchemaError("Conflicting reference sibling types")
        else:
            result[key] = value
    return result


def _expanded(schema, current, documents, active=None, budget=None, depth=0):
    active = set() if active is None else active
    budget = [8192, set()] if budget is None else budget
    budget[0] -= 1
    if depth > 16 or budget[0] < 0:
        raise SchemaError("Schema reference expansion/depth limit exceeded")
    key = (current, id(schema))
    if key in active:
        raise SchemaError("Cyclic schema reference")
    active = active | {key}
    if set(schema) - VOCABULARY:
        raise SchemaError("Reference target is not a supported schema")
    result = {}
    if "$ref" in schema:
        target, base = _target(schema["$ref"], current, documents)
        target_key = (base, id(target))
        if target_key not in budget[1]:
            _profile(target)
            try:
                Draft202012Validator.check_schema(target)
            except InvalidSchema as exc:
                raise SchemaError("Invalid referenced schema: " + exc.message) from exc
            budget[1].add(target_key)
        result = _expanded(target, base, documents, active, budget, depth + 1)
    for name, value in schema.items():
        if name in ("$ref", "$id", "$schema"):
            continue
        if name in ("properties", "$defs"):
            children = {k: _expanded(v, current, documents, active, budget, depth + 1)
                        for k, v in value.items()}
            result = _merge_declarations(result, {name: children})
        elif name == "items":
            child = _expanded(value, current, documents, active, budget, depth + 1)
            result = _merge_declarations(result, {name: child})
        else:
            result = _merge_declarations(result, {name: value})
    return result


def _fields(schema, overrides):
    fields, nodes = {}, {}

    def walk(node, path, array=False):
        nodes[path] = node
        kind = node.get("type")
        if kind == "object":
            for name, child in node.get("properties", {}).items():
                walk(child, path + "." + name, array)
        elif kind == "array":
            item = node.get("items", {})
            if item.get("type") in ENGINE_TYPES:
                walk(item, path, True)
        elif kind in ENGINE_TYPES:
            engine_type = "date" if kind == "string" and node.get("format") == "date-time" else ENGINE_TYPES[kind]
            fields[path] = {"type": engine_type, "array": array}

    walk(schema, "data")
    if not isinstance(overrides, dict) or len(overrides) > 1:
        raise SchemaError("fieldMappings permits at most one geo_point")
    for path, value in overrides.items():
        node = nodes.get(path)
        if value != "geo_point" or node is None or node.get("type") != "object":
            raise SchemaError("geo_point must reference a known point object")
        props = node.get("properties", {})
        if set(props) != {"lat", "lon"} or any(
                prop.get("type") not in ("number", "integer") for prop in props.values()):
            raise SchemaError("geo_point requires numeric lat and lon")
        fields = {k: v for k, v in fields.items() if not k.startswith(path + ".")}
        fields[path] = {"type": "geo_point", "array": False}
    return fields


class SchemaRegistry:
    def __init__(self, catalog_dir=None, lock=None):
        self._catalog_dir = Path(catalog_dir) if catalog_dir else Path(__file__).with_name("schemas")
        self._lock = lock if lock is not None else threading.RLock()
        self.reset()

    def reset(self):
        with self._lock:
            self._entries, self._documents, self._fields, self._validators = {}, {}, {}, {}
            self._builtins = set()
            catalog = json.loads((self._catalog_dir / "catalog.json").read_text(encoding="utf-8"))
            loaded = {}
            for entry in catalog["resources"] + catalog["schemas"]:
                raw = (self._catalog_dir / entry["file"]).read_bytes().replace(b"\r\n", b"\n")
                if hashlib.sha256(raw).hexdigest() != entry["sha256"]:
                    raise SchemaError("Catalog SHA-256 mismatch: " + entry["file"])
                doc = json.loads(raw)
                if doc.get("$id") != entry["id"] or entry["id"] in loaded:
                    raise SchemaError("Catalog identity mismatch or duplicate")
                _profile(doc)
                try:
                    Draft202012Validator.check_schema(doc)
                except InvalidSchema as exc:
                    raise SchemaError("Invalid catalog schema: " + exc.message) from exc
                loaded[entry["id"]] = doc
            self._documents = loaded
            for uri, doc in loaded.items():
                _expanded(doc, uri, loaded)
            for entry in catalog["schemas"]:
                payload = {"kind": entry["kind"], "schema": loaded[entry["id"]],
                           "fieldMappings": entry["fieldMappings"]}
                self._install(payload, "POST", builtin=True)
                declared = entry["mappedFields"]
                actual = {k: v for k, v in self._fields[entry["kind"]].items() if k.startswith("data.")}
                if actual != declared:
                    raise SchemaError("Catalog mapping declaration mismatch")

    def kinds(self):
        with self._lock:
            return list(self._entries)

    def get(self, kind):
        with self._lock:
            return deepcopy(self._entries.get(kind))

    def register(self, payload, method):
        with self._lock:
            return self._install(payload, method)

    def _check_bytes(self, entries, documents):
        if len(_json_bytes({"entries": entries, "documents": documents})) > 2 * 1024 * 1024:
            raise SchemaError("Registry byte capacity exhausted", 429)

    def _install(self, payload, method, builtin=False):
        if method not in ("POST", "PUT"):
            raise SchemaError("Unsupported schema method")
        raw_size = len(_json_bytes(payload))
        if raw_size > 128 * 1024:
            raise SchemaError("Registration exceeds 128 KiB")
        payload = deepcopy(payload)
        kind, identity, status = _normalize(payload)
        schema = payload.get("schema")
        references = payload.get("references", [])
        overrides = payload.get("fieldMappings", {})
        if not isinstance(references, list) or len(references) > 8:
            raise SchemaError("references must contain at most 8 local documents")
        content = {"schema": schema, "references": references, "fieldMappings": overrides}
        old = self._entries.get(kind)
        if old is not None:
            old_content = {key: old[key] for key in content}
            if _json_bytes(content) != _json_bytes(old_content):
                raise SchemaError("Published schema content is immutable", 409)
            if status != old["status"]:
                if kind in self._builtins or method != "PUT" or old["status"] == "DEPRECATED":
                    raise SchemaError("Schema lifecycle transition forbidden", 409)
                updated = deepcopy(old)
                updated["status"] = updated["schemaInfo"]["status"] = status
                self._check_bytes({**self._entries, kind: updated}, self._documents)
                self._entries[kind] = old = updated
            return 200, deepcopy(old)
        if len(self._entries) >= 32:
            raise SchemaError("Registry kind capacity exhausted", 429)
        documents = dict(self._documents)
        root_id = URN + kind
        _profile(schema)
        if schema.get("$id", root_id) != root_id:
            raise SchemaError("Schema $id must agree with full kind")
        root = {**schema, "$id": root_id}
        additions = {root_id: root}
        for doc in references:
            _profile(doc)
            uri = doc.get("$id")
            if not uri or uri in documents or uri in additions:
                raise SchemaError("Duplicate or missing reference $id")
            additions[uri] = doc
        if not builtin and root_id in documents:
            raise SchemaError("Duplicate schema $id")
        documents.update(additions)
        for uri, doc in additions.items():
            try:
                Draft202012Validator.check_schema(doc)
            except InvalidSchema as exc:
                raise SchemaError("Invalid JSON Schema: " + exc.message) from exc
            _expanded(doc, uri, documents)
        expanded = _expanded(root, root_id, documents)
        if expanded.get("type") != "object":
            raise SchemaError("Root data schema must declare object type")
        fields = _fields(expanded, overrides)
        fields.update({key: {"type": typ, "array": array} for key, typ, array in (
            ("id", "keyword", False), ("kind", "keyword", False), ("version", "long", False),
            ("acl.owners", "keyword", True), ("acl.viewers", "keyword", True))})
        if len(fields) > 128:
            raise SchemaError("Mapped scalar leaf limit exceeded")
        registry = Registry(retrieve=_closed_retrieve).with_resources(
            (uri, Resource.from_contents(doc, default_specification=DRAFT202012))
            for uri, doc in documents.items())
        validator = Draft202012Validator(root, registry=registry, format_checker=FORMAT)
        created = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
        response = {"kind": kind, **deepcopy(content), "status": status, "createdBy": "osdu-lite",
                    "createdAt": created, "schemaInfo": {
                        "schemaIdentity": identity, "status": status, "createdBy": "osdu-lite", "created": created}}
        self._check_bytes({**self._entries, kind: response}, documents)
        self._entries[kind] = response
        self._fields[kind] = fields
        self._validators[kind] = validator
        self._documents = documents
        if builtin:
            self._builtins.add(kind)
        return (201 if method == "POST" else 200), deepcopy(response)

    def validate_record(self, record):
        with self._lock:
            if not isinstance(record, dict):
                raise SchemaError("Record must be an object")
            _bounded_value(record)
            if len(_json_bytes(record)) > 64 * 1024:
                raise SchemaError("Record exceeds 64 KiB")
            for key, limit in (("id", 512), ("kind", 256)):
                if not isinstance(record.get(key), str) or not 1 <= len(record[key]) <= limit:
                    raise SchemaError(f"/{key}: nonempty string <= {limit} required")
            kind = record["kind"]
            if kind not in self._entries:
                raise SchemaError("Unsupported kind: " + kind)
            if self._entries[kind]["status"] == "DEPRECATED":
                raise SchemaError("Schema is DEPRECATED: " + kind, 409)
            acl = record.get("acl")
            if not isinstance(acl, dict):
                raise SchemaError("/acl: object required")
            for key in ("owners", "viewers"):
                values = acl.get(key)
                if (not isinstance(values, list) or len(values) > 64 or (key == "owners" and not values)
                        or any(not isinstance(v, str) or not 1 <= len(v) <= 256 for v in values)):
                    raise SchemaError(f"/acl/{key}: bounded principal array required")
            legal = record.get("legal")
            if not isinstance(legal, dict):
                raise SchemaError("legal.legaltags: legal object required")
            for key in ("legaltags", "otherRelevantDataCountries"):
                values = legal.get(key)
                if (not isinstance(values, list) or (key == "legaltags" and not values)
                        or any(not isinstance(v, str) or not v for v in values)):
                    raise SchemaError(f"legal.{key}: string array required")
            if "version" in record and (type(record["version"]) is not int or record["version"] < 1):
                raise SchemaError("/version: positive integer required")
            if not isinstance(record.get("data"), dict):
                raise SchemaError("/data: object required")
            error = next(self._validators[kind].iter_errors(record["data"]), None)
            if error is not None:
                path = "/data" + "".join("/" + str(p).replace("~", "~0").replace("/", "~1")
                                        for p in error.absolute_path)
                raise SchemaError(f"{path}: validation failed ({error.validator})")
            for path, capability in self._fields[kind].items():
                if capability["type"] != "geo_point":
                    continue
                value = record
                for part in path.split("."):
                    value = value.get(part) if isinstance(value, dict) else None
                if value is not None:
                    if (not isinstance(value, dict) or set(value) != {"lat", "lon"}
                            or any(type(value[k]) not in (int, float) for k in ("lat", "lon"))
                            or not -90 <= value["lat"] <= 90 or not -180 <= value["lon"] <= 180):
                        raise SchemaError("/" + path.replace(".", "/") + ": invalid geo_point")

    def field_capabilities(self, kinds, fields):
        with self._lock:
            if len(kinds) > 32:
                raise SchemaError("Too many selected kinds")
            result = {}
            for field in fields:
                capabilities = [self._fields.get(kind, {}).get(field) for kind in kinds]
                if not capabilities or any(cap is None or cap != capabilities[0] for cap in capabilities):
                    raise SchemaError(f"Incompatible or missing field {field} in kinds {','.join(kinds)}")
                result[field] = deepcopy(capabilities[0])
            return result

    def mapping(self, kind):
        with self._lock:
            if kind not in self._fields:
                raise SchemaError("Unknown kind")
            mapping = {"dynamic": False, "properties": {}}
            for path, capability in self._fields[kind].items():
                current = mapping
                parts = path.split(".")
                for part in parts[:-1]:
                    current = current["properties"].setdefault(part, {"type": "object", "dynamic": False,
                                                                      "properties": {}})
                leaf = {"type": capability["type"]}
                if leaf["type"] == "keyword":
                    leaf["fields"] = {"text": {"type": "text"}}
                current["properties"][parts[-1]] = leaf
            return mapping
