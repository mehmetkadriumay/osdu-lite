"""Bounded, offline community-v3 reference service subset (not a full catalog)."""
import json
import math
import os
import re
from copy import deepcopy
from pathlib import Path

os.environ["PROJ_NETWORK"] = "OFF"
from pyproj import CRS, Transformer, network  # noqa: E402
from pyproj.exceptions import ProjError  # noqa: E402

network.set_network_enabled(False)

MAX_BODY = 1024 * 1024
CATALOG_PATH = "/crs/catalog/v3/coordinate-reference-system"
CONVERTER_PATH = "/crs/converter/v3/convert"
UNIT_PATH = "/unit/v3"
CATALOG_GROUP = "osdu-lite.crs.catalog.user"
UNIT_GROUP = "osdu-lite.unit.user"
CONVERTER_GROUP = "osdu-lite.crs.converter.user"
GROUPS = {CATALOG_GROUP, UNIT_GROUP, CONVERTER_GROUP}
_SOURCE = json.loads((Path(__file__).parent / "data/reference_catalog.json").read_text())


class ReferenceError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def bad(message):
    raise ReferenceError(400, message)


def object_fields(value, allowed, label):
    if not isinstance(value, dict):
        bad(label + " must be an object")
    if set(value) - set(allowed):
        bad(label + " has unsupported fields: " + ", ".join(sorted(set(value) - set(allowed))))


def text(value, label):
    if not isinstance(value, str) or not value.strip():
        bad(label + " must be a nonempty string")
    return value


def finite(value, label, strings=False):
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        bad(label + " must be a finite number")
    if isinstance(value, str) and (not strings or not re.fullmatch(
            r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?", value)):
        bad(label + " must be a finite decimal number")
    try:
        number = float(value)
    except (ValueError, OverflowError):
        bad(label + " must be finite")
    if not math.isfinite(number):
        bad(label + " must be finite")
    return number


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                bad("Duplicate JSON field: " + key)
            result[key] = value
        return result

    try:
        value = json.loads(raw, object_pairs_hook=pairs,
                           parse_constant=lambda value: bad("Nonfinite JSON number: " + value),
                           parse_float=lambda value: finite(value, "JSON number", strings=True))
    except (ValueError, UnicodeError, RecursionError):
        bad("Invalid JSON body or reference")
    return value


def integer(value, label, minimum=0):
    if isinstance(value, str) and re.fullmatch(r"-?(?:0|[1-9][0-9]*)", value):
        try:
            value = int(value)
        except ValueError:
            bad(label + " is too large")
    if type(value) is not int or value < minimum:
        bad(label + " must be an integer >= " + str(minimum))
    return value


def boolean(value, label):
    if type(value) is bool:
        return value
    if value in ("true", "false"):
        return value == "true"
    bad(label + " must be true or false")


def query_values(query, allowed, required=()):
    object_fields(query, allowed, "query")
    result = {}
    for key, values in query.items():
        if len(values) != 1:
            bad("Query parameter must occur once: " + key)
        result[key] = text(values[0], key)
    for key in required:
        if key not in result:
            bad("Missing query parameter: " + key)
    return result


def _crs_record(entry):
    code = entry["code"]
    reference = {
        "authCode": {"auth": "EPSG", "code": code}, "name": entry["name"],
        "type": "LBC", "ver": "OSDU-Lite/pyproj-3.7.2",
        "wkt": CRS.from_epsg(int(code)).to_wkt(),
    }
    west, south, east, north = entry["extent"]
    return {
        "id": "osdu:reference-data--CoordinateReferenceSystem:" + entry["id"],
        "kind": "osdu:wks:reference-data--CoordinateReferenceSystem:1.1.0",
        "data": {
            "ID": entry["id"], "Name": entry["name"], "Code": code, "CodeSpace": "EPSG",
            "Kind": entry["kind"], "InactiveIndicator": False,
            "PersistableReference": json.dumps(reference, separators=(",", ":"), allow_nan=False),
            "CoordinateSystem": {
                "Axes": entry["axes"], "AxisOrder": "OSDU-Lite always_xy",
                "HorizontalAxisUnitID": "osdu:reference-data--UnitOfMeasure:" + entry["unit"] + ":",
            },
            "PreferredUsage": {"Extent": {
                "Description": "OSDU-Lite conservative horizontal operating envelope",
                "BoundingBoxWestBoundLongitude": west, "BoundingBoxSouthBoundLatitude": south,
                "BoundingBoxEastBoundLongitude": east, "BoundingBoxNorthBoundLatitude": north,
            }},
        },
    }


_CRS = tuple(_crs_record(e) for e in sorted(_SOURCE["crs"], key=lambda e: int(e["code"])))
_UNITS = {u["symbol"]: u for u in _SOURCE["units"]}
_DEFAULT_FIELDS = ["id", "kind", "data.ID", "data.Name", "data.Code",
                   "data.CodeSpace", "data.Kind", "data.PersistableReference"]


def _project(record, fields):
    result = {}
    for field in fields:
        text(field, "returnedFields entry")
        source, target = record, result
        parts = field.split(".")
        for index, key in enumerate(parts):
            if not isinstance(source, dict) or key not in source:
                bad("Unsupported returnedFields path: " + field)
            source = source[key]
            if index == len(parts) - 1:
                target[key] = deepcopy(source)
            else:
                target = target.setdefault(key, {})
                if not isinstance(target, dict):
                    bad("Conflicting returnedFields paths")
    return result


def _search_response(records, query, fields=None):
    return {"searchResults": {
        "results": [deepcopy(r) if fields is None else _project(r, fields) for r in records],
        "totalCount": len(records),
    }, "query": "OSDU-Lite fixed catalog (not Search): " + json.dumps(query, sort_keys=True)}


def catalog_detail(query):
    q = query_values(query, {"recordId", "dataId"})
    if len(q) != 1:
        bad("Require exactly one recordId or dataId")
    key, value = next(iter(q.items()))
    if key == "recordId":
        if (not value.startswith("osdu:reference-data--CoordinateReferenceSystem:")
                or not value.endswith(":") or value.endswith("::")):
            bad("recordId requires the catalog prefix and one terminating colon")
        records = [r for r in _CRS if r["id"] == value[:-1]]
    else:
        records = [r for r in _CRS if r["data"]["ID"] == value]
    if not records:
        raise ReferenceError(404, "CRS not found in fixed catalog")
    return _search_response(records, q)


def catalog_query(payload):
    filters = {"codeSpace": "CodeSpace", "code": "Code", "name": "Name", "id": "ID", "kind": "Kind"}
    object_fields(payload, set(filters) | {"includeDeprecated", "offset", "limit",
                                        "returnAllFields", "returnedFields"}, "catalog query")
    for key in filters.keys() & payload.keys():
        text(payload[key], key)
    boolean(payload.get("includeDeprecated", False), "includeDeprecated")
    all_fields = boolean(payload.get("returnAllFields", False), "returnAllFields")
    offset = integer(payload.get("offset", 0), "offset")
    limit = integer(payload.get("limit", len(_CRS)), "limit")
    fields = None if all_fields else payload.get("returnedFields", _DEFAULT_FIELDS)
    if fields is not None:
        if not isinstance(fields, list):
            bad("returnedFields must be an array")
        _project(_CRS[0], fields)  # Validate even when no records match.
    records = list(_CRS)
    for key, field in filters.items():
        if key in payload:
            records = [r for r in records if (
                payload[key].casefold() in r["data"][field].casefold() if key == "name"
                else r["data"][field] == payload[key])]
    result = _search_response(records[offset:offset + limit], payload, fields)
    result["searchResults"]["totalCount"] = len(records)
    return result


def _unit_payload(unit):
    essence = {
        "type": "USO", "symbol": unit["symbol"],
        "baseMeasurement": {"type": "UM", "ancestry": unit["dimension"]},
        "scaleOffset": {"scale": unit["a"], "offset": -unit["b"] / unit["a"]},
    }
    return {"name": unit["name"], "namespace": "osdu-lite", "displaySymbol": unit["symbol"],
            "source": "OSDU-Lite curated SI-based subset", "essence": essence,
            "essenceJson": json.dumps(essence, separators=(",", ":"), allow_nan=False)}


def _lookup(symbol):
    if symbol not in _UNITS:
        raise ReferenceError(404, "Unit not found in fixed catalog")
    return _UNITS[symbol]


def _namespace(q):
    if q["namespaces"] != "osdu-lite":
        raise ReferenceError(404, "Only the emulator-local namespace osdu-lite is supported")


def _page(items, q, field):
    offset = integer(q.get("offset", 0), "offset")
    limit = integer(q.get("limit", 100), "limit", -1)
    selected = items[offset:] if limit == -1 else items[offset:offset + limit]
    return {field: selected, "offset": offset, "count": len(selected), "totalCount": len(items)}


def _conversion(source, target):
    if source["dimension"] != target["dimension"]:
        bad("Units have incompatible measurement dimensions")
    return {
        "fromUnit": _unit_payload(source), "toUnit": _unit_payload(target),
        "scaleOffset": {"scale": source["a"] / target["a"],
                        "offset": (target["b"] - source["b"]) / source["a"]},
    }


def _essence_unit(payload, side):
    reference_key = side + "PersistableReference"
    if (side in payload) == (reference_key in payload):
        bad("Require exactly one " + side + " or " + reference_key)
    essence = payload[side] if side in payload else strict_json(text(payload[reference_key], reference_key))
    object_fields(essence, {"type", "symbol", "baseMeasurement", "scaleOffset"}, side)
    symbol = text(essence.get("symbol"), side + ".symbol")
    unit = _UNITS.get(symbol)
    if unit is None:
        bad("Unsupported unit essence")
    coefficients = essence.get("scaleOffset")
    object_fields(coefficients, {"scale", "offset"}, side + ".scaleOffset")
    for key in ("scale", "offset"):
        finite(coefficients.get(key), side + ".scaleOffset." + key)
    if essence != _unit_payload(unit)["essence"]:
        bad("Unit essence conflicts with fixed catalog definition")
    return unit


def unit_post(payload):
    object_fields(payload, {"fromUnit", "toUnit", "fromUnitPersistableReference",
                            "toUnitPersistableReference"}, "conversion request")
    return _conversion(_essence_unit(payload, "fromUnit"), _essence_unit(payload, "toUnit"))


def unit_get(path, query):
    if path in {"/unit", "/measurement/list"}:
        q = query_values(query, {"offset", "limit"})
        if path == "/unit":
            return _page([_unit_payload(_UNITS[s]) for s in sorted(_UNITS)], q, "units")
        measurements = []
        for dimension in sorted({u["dimension"] for u in _UNITS.values()}):
            essence = {"type": "UM", "ancestry": dimension}
            measurements.append({"name": dimension, "namespace": "osdu-lite", "essence": essence,
                                 "essenceJson": json.dumps(essence, separators=(",", ":"))})
        return _page(measurements, q, "measurements")
    if path == "/unit/measurement":
        q = query_values(query, {"ancestry"}, {"ancestry"})
        units = [_unit_payload(u) for s, u in sorted(_UNITS.items()) if u["dimension"] == q["ancestry"]]
        if not units:
            raise ReferenceError(404, "Measurement not found in fixed catalog")
        return _page(units, {}, "units")
    if path == "/unit/symbol":
        q = query_values(query, {"namespaces", "symbol"}, {"namespaces", "symbol"})
        _namespace(q)
        return _unit_payload(_lookup(q["symbol"]))
    q = query_values(query, {"namespaces", "fromSymbol", "toSymbol"},
                     {"namespaces", "fromSymbol", "toSymbol"})
    _namespace(q)
    return _conversion(_lookup(q["fromSymbol"]), _lookup(q["toSymbol"]))


def _resolve_crs(value, label):
    reference = strict_json(text(value, label))
    for record in _CRS:
        if reference == json.loads(record["data"]["PersistableReference"]):
            return record["data"]["Code"]
    bad(label + " must be an unmodified persistable reference from this catalog")


def _transformer(source, target):
    # Only codes resolved from the fixed catalog ever reach PROJ.
    return Transformer.from_crs("EPSG:" + source, "EPSG:" + target,
                                always_xy=True, allow_ballpark=False, only_best=True)


def _in_domain(code, longitude, latitude):
    entry = next(e for e in _SOURCE["crs"] if e["code"] == code)
    west, south, east, north = entry["extent"]
    # Inverse floating-point noise at a boundary is not geographic extrapolation.
    epsilon = 1e-10
    return (math.isfinite(longitude) and math.isfinite(latitude)
            and west - epsilon <= longitude <= east + epsilon
            and south - epsilon <= latitude <= north + epsilon)


def convert_points(payload):
    object_fields(payload, {"fromCRS", "toCRS", "points"}, "conversion request")
    source = _resolve_crs(payload.get("fromCRS"), "fromCRS")
    target = _resolve_crs(payload.get("toCRS"), "toCRS")
    points = payload.get("points")
    if not isinstance(points, list) or not 1 <= len(points) <= 1000:
        bad("points must be an array of 1 through 1000 points")
    validated = []
    for index, point in enumerate(points):
        label = "points[" + str(index) + "]"
        object_fields(point, {"x", "y", "z"}, label)
        coordinates = [finite(point.get(axis), label + "." + axis, strings=True) for axis in ("x", "y", "z")]
        # z is validated but never sent through a geodetic/vertical operation.
        validated.append((coordinates[0], coordinates[1], str(point["z"])))

    network.set_network_enabled(False)  # Also applies to request-thread PROJ contexts.
    transformer = _transformer(source, target)
    inverse = _transformer(source, "4326")
    forward = _transformer("4326", source)
    results = []
    success = 0
    for x, y, z in validated:
        try:
            lon, lat = inverse.transform(x, y, errcheck=True)
            applicable = _in_domain(source, lon, lat) and _in_domain(target, lon, lat)
            if source == "4326":
                # Never wrap or clamp geographic input, including at the poles.
                applicable = applicable and -180 <= x <= 180 and -90 <= y <= 90
            elif applicable:
                # Reject projected coordinates that PROJ can wrap onto the globe.
                rx, ry = forward.transform(lon, lat, errcheck=True)
                applicable = math.isclose(x, rx, rel_tol=1e-12, abs_tol=1e-6) and math.isclose(
                    y, ry, rel_tol=1e-12, abs_tol=1e-6)
            if applicable:
                tx, ty = transformer.transform(x, y, errcheck=True)
                applicable = math.isfinite(tx) and math.isfinite(ty)
            if applicable:
                results.append({"x": str(tx), "y": str(ty), "z": z})
                success += 1
                continue
        except ProjError:
            # Well-formed coordinates outside the engine's projection domain.
            pass
        results.append({"x": "NaN", "y": "NaN", "z": "NaN"})
    return {"points": results, "successCount": str(success),
            "operationsApplied": [transformer.description] if success else []}


def route_group(method, path):
    path = path[4:] if path.startswith("/api/") else path
    if path == CATALOG_PATH and method in {"GET", "POST"}:
        return CATALOG_GROUP
    if path == CONVERTER_PATH and method == "POST":
        return CONVERTER_GROUP
    if path.startswith(UNIT_PATH + "/"):
        suffix = path[len(UNIT_PATH):]
        if (method == "GET" and suffix in {"/unit", "/unit/symbol", "/unit/measurement",
                                          "/measurement/list", "/conversion/scale"}
                or method == "POST" and suffix == "/conversion/scale"):
            return UNIT_GROUP
    return None


def handle(method, path, query, payload=None):
    path = path[4:] if path.startswith("/api/") else path
    if method == "POST":
        query_values(query, {})
        if path == CONVERTER_PATH:
            return convert_points(payload)
        return catalog_query(payload) if path == CATALOG_PATH else unit_post(payload)
    if path == CATALOG_PATH:
        return catalog_detail(query)
    return unit_get(path[len(UNIT_PATH):], query)
