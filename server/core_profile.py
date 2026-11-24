"""Opt-in contract shell; business semantics are enabled by later bounded epics."""
import json
import math
import os
import re
import datetime
from copy import deepcopy
from urllib.parse import parse_qs


MAX_JSON_BYTES = 8 * 1024 * 1024
CORE_PREFIXES = (
    "/storage/v2", "/search/v2", "/schema-service/v1", "/file/v2", "/dataset/v1",
)


class ProfileError(ValueError):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def select_profile():
    profile = os.getenv("OSDU_COMPAT_PROFILE", "legacy")
    if profile not in ("legacy", "core"):
        raise ValueError("OSDU_COMPAT_PROFILE must be legacy or core")
    return profile


def canonical_path(path):
    return path[4:] if path.startswith("/api/") else path


def is_core_route(path):
    path = canonical_path(path)
    return any(path == prefix or path.startswith(prefix + "/") for prefix in CORE_PREFIXES)


def _header_values(headers, name):
    if hasattr(headers, "get_all"):
        return headers.get_all(name, [])
    value = headers.get(name)
    return [] if value is None else [value]


def body_length(headers, *, required=True):
    if _header_values(headers, "Transfer-Encoding"):
        raise ProfileError(400, "Transfer-Encoding is unsupported")
    lengths = _header_values(headers, "Content-Length")
    if not lengths and not required:
        return 0
    if len(lengths) != 1 or not re.fullmatch(r"[0-9]+", str(lengths[0])):
        raise ProfileError(400, "One finite nonnegative Content-Length is required")
    # Compare as text first: never convert an arbitrarily large integer header.
    text = str(lengths[0]).lstrip("0") or "0"
    if len(text) > len(str(MAX_JSON_BYTES)) or (
        len(text) == len(str(MAX_JSON_BYTES)) and text > str(MAX_JSON_BYTES)
    ):
        raise ProfileError(413, "Core JSON body exceeds 8 MiB")
    return int(text)


def _media_type(headers, allow_patch):
    values = _header_values(headers, "Content-Type")
    if len(values) != 1:
        raise ProfileError(415, "One JSON Content-Type is required")
    parts = [part.strip().lower() for part in str(values[0]).split(";")]
    allowed = {"application/json"}
    if allow_patch:
        allowed.add("application/json-patch+json")
    if parts[0] not in allowed or (
        len(parts) > 1 and parts[1:] not in (["charset=utf-8"], ['charset="utf-8"'])
    ):
        raise ProfileError(415, "Unsupported core JSON media type or charset")


def _nonfinite(_value):
    raise ValueError("Nonfinite JSON number")


def _finite_float(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("Nonfinite JSON number")
    return result


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def read_json(stream, headers, *, allow_patch=False):
    length = body_length(headers)
    _media_type(headers, allow_patch)
    raw = stream.read(length)
    if len(raw) != length:
        raise ProfileError(400, "Incomplete core JSON body")
    try:
        return json.loads(raw.decode("utf-8"), parse_constant=_nonfinite, parse_float=_finite_float,
                          object_pairs_hook=_unique_object)
    except (ValueError, UnicodeError, RecursionError):
        raise ProfileError(400, "Invalid finite core JSON body") from None


USER_FIELDS = {"kind", "acl", "legal", "data", "ancestry", "meta", "tags"}
AUDIT_FIELDS = {"version", "createUser", "createTime", "modifyUser", "modifyTime"}


def record_id(value, partition):
    if (not isinstance(value, str) or not value.startswith(partition + ":")
            or len(value.split(":")) != 3 or not all(value.split(":"))
            or len(value.encode("utf-8")) > 512 or any(c in value for c in "/\\")
            or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        raise ProfileError(400, "Invalid partition record ID")
    return value


def user_fields(record):
    return {key: record.get(key, {"ancestry": {}, "meta": [], "tags": {}}.get(key))
            for key in USER_FIELDS}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def query_values(query, allowed):
    values = parse_qs(query, keep_blank_values=True, strict_parsing=True) if query else {}
    if set(values) - set(allowed) or any(len(v) != 1 for v in values.values()):
        raise ProfileError(400, "Unknown or repeated core query parameter")
    return {key: value[0] for key, value in values.items()}


def boolean(value):
    if value not in ("true", "false"):
        raise ProfileError(400, "Expected true or false")
    return value == "true"


def timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(
            r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)", value):
        raise ProfileError(400, "Expected RFC3339 timestamp")
    try:
        if value[-1] != "Z" and (int(value[-5:-3]) > 23 or int(value[-2:]) > 59):
            raise ValueError("Invalid timezone offset")
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        raise ProfileError(400, "Invalid RFC3339 timestamp") from None


def metadata_value(path, value):
    if path == ["tags"]:
        valid = isinstance(value, dict) and all(
            isinstance(k, str) and isinstance(v, str) for k, v in value.items())
    else:
        valid = isinstance(value, list) and all(isinstance(v, str) and v for v in value)
    if not valid:
        raise ProfileError(400, "Invalid metadata field value")


def patch_request(payload, partition, *, data_patch):
    if not isinstance(payload, dict) or set(payload) != {"query", "ops"}:
        raise ProfileError(400, "Expected core {query:{ids:[...]},ops:[...]}")
    query = payload["query"]
    if not isinstance(query, dict):
        raise ProfileError(400, "Expected query object")
    if set(query) - {"ids"}:
        raise ProfileError(501, "unsupported_patch_selection")
    ids = query.get("ids")
    if not isinstance(ids, list) or not 1 <= len(ids) <= 100:
        raise ProfileError(400, "Expected 1..100 unique IDs")
    for rid in ids:
        record_id(rid, partition)
    if len(set(ids)) != len(ids):
        raise ProfileError(400, "Expected unique IDs")
    ops = payload["ops"]
    if not isinstance(ops, list) or not 1 <= len(ops) <= 100:
        raise ProfileError(400, "Expected 1..100 operations")
    prepared = []
    fields = [["acl", "owners"], ["acl", "viewers"],
              ["legal", "legaltags"], ["legal", "otherRelevantDataCountries"], ["tags"]]
    for op in ops:
        if (not isinstance(op, dict) or not isinstance(op.get("op"), str)
                or op["op"] not in {"add", "replace", "remove"}):
            raise ProfileError(400, "Unsupported patch operation")
        expected = {"op", "path"} | ({"value"} if op["op"] != "remove" else set())
        path = op.get("path")
        if (set(op) != expected or not isinstance(path, str) or not path.startswith("/")
                or re.search(r"~(?:[^01]|$)", path)):
            raise ProfileError(400, "Invalid patch operation or JSON Pointer")
        tokens = [part.replace("~1", "/").replace("~0", "~") for part in path[1:].split("/")]
        metadata = next((field for field in fields if tokens[:len(field)] == field), None)
        if not (tokens in fields or (data_patch and (
                tokens[0] == "data" or metadata is not None))):
            raise ProfileError(400, "Unsupported patch path")
        if tokens in fields and op["op"] != "remove":
            metadata_value(tokens, op["value"])
        if metadata is not None and tokens != metadata:
            if len(tokens) != len(metadata) + 1:
                raise ProfileError(400, "Unsupported metadata patch path")
            if metadata != ["tags"] and not (
                    re.fullmatch(r"0|[1-9][0-9]{0,18}", tokens[-1])
                    or (tokens[-1] == "-" and op["op"] == "add")):
                raise ProfileError(400, "Invalid metadata array index")
            if op["op"] != "remove" and (
                    not isinstance(op["value"], str) or (metadata != ["tags"] and not op["value"])):
                raise ProfileError(400, "Invalid metadata element value")
        if data_patch and tokens == ["data"] and op["op"] != "remove" and not isinstance(op["value"], dict):
            raise ProfileError(400, "data must be an object")
        prepared.append((op["op"], tokens, deepcopy(op.get("value"))))
    return ids, prepared


def apply_patch(record, ops):
    """Apply the bounded add/replace/remove subset; never create missing parents."""
    result = deepcopy(record)
    for op, tokens, value in ops:
        parent = result
        for token in tokens[:-1]:
            parent = _pointer_child(parent, token)
        key = tokens[-1]
        if isinstance(parent, list):
            if op == "add" and key == "-":
                index = len(parent)
            elif re.fullmatch(r"0|[1-9][0-9]{0,18}", key):
                index = int(key)
            else:
                raise ValueError("Invalid array index")
            if index >= len(parent) + (op == "add"):
                raise ValueError("Array index out of bounds")
            if op == "add":
                parent.insert(index, deepcopy(value))
            elif op == "replace":
                parent[index] = deepcopy(value)
            else:
                parent.pop(index)
        elif isinstance(parent, dict):
            if op != "add" and key not in parent:
                raise ValueError("Patch target does not exist")
            if op == "remove":
                del parent[key]
            else:
                parent[key] = deepcopy(value)
        else:
            raise ValueError("Patch parent must be an object or array")
    return result


def _pointer_child(parent, token):
    if isinstance(parent, dict) and token in parent:
        return parent[token]
    if isinstance(parent, list) and re.fullmatch(r"0|[1-9][0-9]{0,18}", token):
        index = int(token)
        if index < len(parent):
            return parent[index]
    raise ValueError("Patch parent does not exist")


def search_request(payload, registry, *, cursor=False):
    if not isinstance(payload, dict):
        raise ProfileError(400, "Core search requires a JSON object")
    allowed = {"kind", "query", "offset", "limit", "queryAsOwner", "trackTotalCount",
               "returnedFields", "excludedFields", "sort", "aggregateBy", "cursor",
               "spatialFilter", "suggestPhrase", "highlightedFields", "search_after"}
    if set(payload) - allowed:
        raise ProfileError(400, "Unknown or legacy-only core search key")
    if any(k in payload for k in ("spatialFilter", "suggestPhrase", "highlightedFields")) or (
            payload.get("search_after") is True) or (
            isinstance(payload.get("sort"), dict) and "filter" in payload["sort"]):
        raise ProfileError(501, "unsupported_core_search_feature")
    if "cursor" in payload:
        if not cursor or set(payload) != {"cursor"} or not isinstance(payload["cursor"], str):
            raise ProfileError(400, "Cursor continuation requires cursor only")
        return payload
    kinds = payload.get("kind")
    kinds = [kinds] if isinstance(kinds, str) else kinds
    if not isinstance(kinds, list) or not 1 <= len(kinds) <= 32 or any(
            not isinstance(k, str) or not k or len(k.encode("utf-8")) > 512 for k in kinds):
        raise ProfileError(400, "Expected 1..32 kind patterns")
    result = dict(payload, kind=kinds)
    for field, default in (("offset", 0), ("limit", 10)):
        value = payload.get(field, default)
        if type(value) is not int or not 0 <= value <= 1000:
            raise ProfileError(400, "Invalid search page bounds")
        result[field] = value
    if result["offset"] + result["limit"] > 1000:
        raise ProfileError(400, "Invalid search page bounds")
    for field in ("queryAsOwner", "trackTotalCount", "search_after"):
        if field in payload and type(payload[field]) is not bool:
            raise ProfileError(400, "Expected boolean search option")
    result["query"] = payload.get("query", "*")
    if not isinstance(result["query"], str):
        raise ProfileError(400, "query must be a string")
    for field in ("returnedFields", "excludedFields"):
        value = payload.get(field, [])
        if not isinstance(value, list) or any(not isinstance(p, str) or not re.fullmatch(
                r"[A-Za-z_][\w-]*(?:\.[A-Za-z_][\w-]*)*", p) for p in value):
            raise ProfileError(400, "Expected exact dotted field paths")
        result[field] = value
    if result["returnedFields"] and result["excludedFields"]:
        raise ProfileError(400, "Projection and exclusion are mutually exclusive")
    import fnmatch
    selected = [k for k in registry.kinds() if any(
        fnmatch.fnmatchcase(k.lower(), p.lower()) for p in kinds)]
    sort = payload.get("sort", {"field": ["id"], "order": ["ASC"]})
    if not isinstance(sort, dict) or set(sort) != {"field", "order"}:
        raise ProfileError(400, "Invalid sort")
    fields, orders = sort["field"], sort["order"]
    if (not isinstance(fields, list) or not isinstance(orders, list) or not 1 <= len(fields) <= 4
            or len(fields) != len(orders) or any(not isinstance(f, str) for f in fields)
            or any(o not in ("ASC", "DESC") for o in orders) or len(set(fields)) != len(fields)):
        raise ProfileError(400, "Invalid sort")
    def capability(field, scalar):
        if field in ("id", "kind", "version"):
            return
        cap = registry.field_capabilities(selected, [field])[field]
        if cap["type"] not in {"keyword", "long", "double", "boolean", "date"} or (
                scalar and cap["array"]):
            raise ProfileError(400, "Unsupported field capability")
    for field in fields:
        capability(field, True)
    result["_sort"] = [(f, o.lower()) for f, o in zip(fields, orders)]
    if "id" not in fields:
        result["_sort"].append(("id", "asc"))
    if "aggregateBy" in payload:
        field = payload["aggregateBy"]
        if not isinstance(field, str) or not field:
            raise ProfileError(400, "Expected one aggregation field")
        capability(field, False)
    return result


def search_projection(record, returned, excluded):
    missing = object()
    def include(value, parts):
        if not parts:
            return deepcopy(value)
        if isinstance(value, list):
            selected = [include(item, parts) for item in value]
            return [{} if item is missing else item for item in selected]
        if not isinstance(value, dict) or parts[0] not in value:
            return missing
        selected = include(value[parts[0]], parts[1:])
        return missing if selected is missing else {parts[0]: selected}
    def merge(target, selected):
        if isinstance(target, dict) and isinstance(selected, dict):
            for key, value in selected.items():
                target[key] = merge(target[key], value) if key in target else value
        elif isinstance(target, list) and isinstance(selected, list):
            for index, value in enumerate(selected):
                if index < len(target):
                    target[index] = merge(target[index], value)
                else:
                    target.append(value)
        else:
            return selected
        return target
    def exclude(value, parts):
        if isinstance(value, list):
            for item in value:
                exclude(item, parts)
        elif isinstance(value, dict):
            if len(parts) == 1:
                value.pop(parts[0], None)
            elif parts[0] in value:
                exclude(value[parts[0]], parts[1:])
    if returned:
        result = {}
        for path in returned:
            selected = include(record, path.split("."))
            if selected is not missing:
                result = merge(result, selected)
        return result
    result = deepcopy(record)
    for path in excluded:
        exclude(result, path.split("."))
    return result


def reject_pending_operation(method, path, query, payload):
    """Fail closed until each business epic replaces its own admission gate.

    Never route a core request through a legacy serializer just because its
    request body happens to resemble a demo request.
    """
    path = canonical_path(path)
    if path == "/storage/v2/records":
        if method == "POST":
            raise ProfileError(405, "Use PUT /api/storage/v2/records in core")
        if method == "PATCH" and (
            not isinstance(payload, dict) or set(payload) != {"query", "ops"}
        ):
            raise ProfileError(400, "Use core PATCH {query:{ids:[...]},ops:[...]}")
    if path.startswith("/storage/v2/records/") and method == "PATCH":
        raise ProfileError(400, "Use core bulk PATCH /api/storage/v2/records")
    if path == "/storage/v2/records" and "modifyAfterDate" in parse_qs(query, keep_blank_values=True):
        raise ProfileError(400, "Use modifiedAfterDate in core")
    if path in ("/search/v2/query", "/search/v2/query_with_cursor") and method == "POST":
        if not isinstance(payload, dict):
            raise ProfileError(400, "Core search requires a JSON object")
        allowed = {
            "kind", "query", "offset", "limit", "queryAsOwner", "trackTotalCount",
            "returnedFields", "excludedFields", "sort", "aggregateBy", "cursor",
            "spatialFilter", "suggestPhrase", "highlightedFields", "search_after",
        }
        if set(payload) - allowed:
            raise ProfileError(400, "Unknown or legacy-only core search key")
        if any(key in payload for key in ("spatialFilter", "suggestPhrase", "highlightedFields")):
            raise ProfileError(501, "unsupported_core_search_feature")
        if payload.get("search_after") is True or (
            isinstance(payload.get("sort"), dict) and "filter" in payload["sort"]
        ):
            raise ProfileError(501, "unsupported_core_search_feature")
    if path in ("/file/v2/files/revokeURL", "/dataset/v1/revokeURL"):
        raise ProfileError(501, "unsupported_immediate_sas_revocation")
    raise ProfileError(501, "core_operation_not_implemented")


def inventory(profile, builtin_kinds, retained_record_bytes):
    return {
        "profile": profile,
        "snapshotFormat": 1,
        "supportedOperations": ["GET /health", "GET /info"] + ([
            "PUT /api/storage/v2/records", "PATCH /api/storage/v2/records",
            "GET /api/storage/v2/records/{id}", "GET /api/storage/v2/records/{id}/{version}",
            "GET /api/storage/v2/records/versions/{id}",
            "POST /api/storage/v2/query/records:batch",
            "POST /api/storage/v2/records/{id}:delete", "DELETE /api/storage/v2/records/{id}",
            "GET /api/schema-service/v1/schema/{kind}", "GET /api/schema-service/v1/schema",
            "POST /api/schema-service/v1/schema", "PUT /api/schema-service/v1/schema",
            "GET /api/storage/v2/records", "POST /api/search/v2/query",
            "POST /api/search/v2/query_with_cursor", "DELETE /api/search/v2/query_with_cursor/{cursor}",
        ] if profile == "core" else []),
        "supportedFeatures": ["profile-selection"] + (
            ["bounded-json-requests", "storage-writes", "storage-retained-versions",
             "storage-patch", "storage-list-selection", "schema-core", "search-core",
             "storage-list-cursors"] if profile == "core" else []
        ),
        "deferredFeatures": [
            "durable-core-state", "live-acl", "storage-core",
            "file-sas", "dataset-files", "schema-driven-indexing",
        ],
        "legacyExtensions": "Existing demo/mock/workflow routes are not core-compatible operations",
        "localSchemas": {
            "dialect": "https://json-schema.org/draft/2020-12/schema",
            "scope": "Locally authored data-only samples, not an official OSDU schema catalog",
            "upstreamVerified": False,
            "builtinKinds": list(builtin_kinds),
        },
        "limits": {
            "jsonBodyBytes": MAX_JSON_BYTES if profile == "core" else None,
            "recordsPerBatch": 100,
            "recordBytes": 64 * 1024,
            "lifetimeRecordIds": 5000,
            "retainedRecordBytes": retained_record_bytes,
            "retainedVersionsPerId": 1000,
            "registeredKinds": 32,
            "registryBytes": 2 * 1024 * 1024,
            "registrationBytes": 128 * 1024,
            "localReferencesPerRegistration": 8,
            "mappedLeavesPerKind": 128,
            "schemaDepth": 16,
            "searchWindow": 1000, "searchKindPatterns": 32, "termsCardinality": 100,
            "liveCursors": 64, "cursorBytes": 16 * 1024 * 1024,
            "cursorReferences": 5000, "cursorSeconds": 300,
        },
    }
