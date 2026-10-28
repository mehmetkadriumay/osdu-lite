"""Public v3 reference operations over the existing curated offline catalog."""
import json
import math
from copy import deepcopy

from pyproj import CRS, Geod, Transformer, network
from pyproj.exceptions import ProjError

import reference_services as ref

MODIFIED = "2026-10-28T00:00:00Z"
BASE_UNITS = {"Length": "m", "Time": "s", "PlaneAngle": "rad", "Pressure": "Pa",
              "AbsoluteTemperature": "K", "Speed": "m/s"}
SYSTEM = {"name": "OSDU-Lite SI", "description": "Local SI assignments, not an official upstream unit system",
          "ancestry": "osdu-lite:SI", "persistableReference": '{"ancestry":"osdu-lite:SI"}'}


def object_value(value, allowed, label="request"):
    ref.object_fields(value, allowed, label)
    return value


def measurements():
    result = []
    for dimension, symbol in sorted(BASE_UNITS.items()):
        essence = {"type": "UM", "ancestry": dimension}
        units = [ref._unit_payload(u) for k, u in sorted(ref._UNITS.items()) if u["dimension"] == dimension]
        result.append({"name": dimension, "namespace": "osdu-lite", "essence": essence,
                       "essenceJson": json.dumps(essence, separators=(",", ":")), "lastModified": MODIFIED,
                       "baseMeasurement": True, "baseMeasurementEssenceJson": json.dumps(essence),
                       "unitEssenceJsons": [u["essenceJson"] for u in units],
                       "preferredUnitEssenceJsons": [ref._unit_payload(ref._UNITS[symbol])["essenceJson"]]})
    return result


def measurement(ancestry):
    ref.text(ancestry, "ancestry")
    for row in measurements():
        if row["essence"]["ancestry"] == ancestry:
            return row
    raise ref.ReferenceError(404, "Measurement is not in the local catalog")


def essence(payload):
    object_value(payload, {"essence", "persistableReference"})
    if ("essence" in payload) == ("persistableReference" in payload):
        ref.bad("Require exactly one essence or persistableReference")
    return payload["essence"] if "essence" in payload else ref.strict_json(
        ref.text(payload["persistableReference"], "persistableReference"))


def unit_essence(value):
    object_value(value, {"type", "symbol", "baseMeasurement", "baseMeasurementEssence", "scaleOffset", "abcd"},
                 "unit essence")
    unit = ref._lookup(ref.text(value.get("symbol"), "symbol"))
    expected = ref._unit_payload(unit)["essence"]
    base = value.get("baseMeasurement", value.get("baseMeasurementEssence"))
    if base != expected["baseMeasurement"]:
        ref.bad("Unit measurement conflicts with local definition")
    if ("scaleOffset" in value) == ("abcd" in value):
        ref.bad("Require one coefficient representation")
    if "scaleOffset" in value:
        if value.get("type") != "USO":
            ref.bad("scaleOffset requires USO type")
        object_value(value["scaleOffset"], {"scale", "offset"})
        for key in ("scale", "offset"):
            if not math.isclose(ref.finite(value["scaleOffset"].get(key), key, strings=True),
                                expected["scaleOffset"][key], rel_tol=1e-12, abs_tol=1e-12):
                ref.bad("Unit coefficients conflict with local definition")
    else:
        if value.get("type") != "UAD":
            ref.bad("abcd requires UAD type")
        abcd = object_value(value["abcd"], {"a", "b", "c", "d"})
        a, b, c, d = (ref.finite(abcd.get(k), k, strings=True) for k in ("a", "b", "c", "d"))
        if c == 0 or d != 0 or not math.isclose(a / c, unit["b"], abs_tol=1e-10) or not math.isclose(
                b / c, unit["a"], rel_tol=1e-12):
            ref.bad("Only the catalog's linear ABCD coefficients are supported")
    return unit


def measurement_essence(payload):
    value = essence(payload)
    object_value(value, {"type", "ancestry"}, "measurement essence")
    if value.get("type") != "UM":
        ref.bad("Measurement essence requires UM type")
    return measurement(value.get("ancestry"))


def page(rows, query, field):
    q = ref.query_values(query, {"offset", "limit"})
    offset = ref.integer(q.get("offset", "0"), "offset")
    limit = ref.integer(q.get("limit", "100"), "limit", -1)
    if limit > 1000:
        ref.bad("limit must not exceed 1000 (or -1 for this bounded catalog)")
    return ref._page(rows, {"offset": offset, "limit": limit}, field)


def unit_api(method, suffix, query, payload):
    units = [ref._unit_payload(ref._UNITS[k]) for k in sorted(ref._UNITS)]
    if suffix == "/unit" and method == "POST":
        ref.query_values(query, {})
        return ref._unit_payload(unit_essence(essence(payload)))
    if suffix in {"/measurement", "/unit/measurement", "/unit/measurement/preferred", "/unit/unitsystem"}:
        allowed = {"ancestry"} if method == "GET" else set()
        if suffix == "/unit/unitsystem":
            allowed = allowed | {"unitSystemName"}
        q = ref.query_values(query, allowed, allowed)
        row = measurement(q["ancestry"]) if method == "GET" else measurement_essence(payload)
        dimension = row["essence"]["ancestry"]
        if suffix == "/measurement":
            return row
        preferred = ref._unit_payload(ref._UNITS[BASE_UNITS[dimension]])
        if suffix == "/unit/unitsystem":
            if q["unitSystemName"] != SYSTEM["name"]:
                raise ref.ReferenceError(404, "Unknown local unit system")
            return preferred
        rows = [preferred] if suffix.endswith("/preferred") else [
            u for u in units if u["essence"]["baseMeasurement"]["ancestry"] == dimension]
        return page(rows, {}, "units")
    if suffix.endswith("/search"):
        object_value(payload, {"query"})
        query_text = ref.text(payload.get("query"), "query")
        rows = measurements() if suffix == "/measurement/search" else (
            units + measurements() if suffix == "/catalog/search" else units)
        # A bounded literal text search, not an undocumented Elasticsearch parser.
        if query_text != "*":
            if any(c in query_text for c in ":()"):
                ref.bad("Local catalog search supports '*' or literal text only")
            rows = [r for r in rows if query_text.casefold() in
                    (r["name"] + " " + r.get("displaySymbol", "")).casefold()]
        if suffix == "/catalog/search":
            result = page(rows, query, "items")
            selected = result.pop("items")
            result["units"] = [row for row in selected if row["essence"]["type"] != "UM"]
            result["measurements"] = [row for row in selected if row["essence"]["type"] == "UM"]
            return result
        return page(rows, query, "measurements" if suffix == "/measurement/search" else "units")
    if suffix == "/unitsystem/list":
        result = page([deepcopy(SYSTEM)], query, "unitSystemInfoList")
        result["totalCount"], result["offset"] = str(result["totalCount"]), str(result["offset"])
        return result
    if suffix == "/unitsystem":
        if method == "GET":
            q = ref.query_values(query, {"name", "offset", "limit"}, {"name"})
            if q.pop("name") != SYSTEM["name"]:
                raise ref.ReferenceError(404, "Unknown local unit system")
        else:
            q = ref.query_values(query, {"offset", "limit"})
            if essence(payload) != {"ancestry": SYSTEM["ancestry"]}:
                raise ref.ReferenceError(404, "Unknown local unit system essence")
        assignments = [{"measurement": m, "unit": ref._unit_payload(ref._UNITS[BASE_UNITS[m["name"]]]),
                        "lastModified": MODIFIED} for m in measurements()]
        selected = page(assignments, {k: [v] for k, v in q.items()}, "unitAssignments")
        return {**deepcopy(SYSTEM), "unitAssignments": selected["unitAssignments"],
                "unitAssignmentCount": len(assignments), "unitAssignmentCountTotal": len(assignments),
                "unitAssignmentCountInResponse": selected["count"], "offset": selected["offset"],
                "lastModified": MODIFIED, "source": "OSDU-Lite curated subset"}
    if suffix == "/unit/symbols":
        q = ref.query_values(query, {"symbol"}, {"symbol"})
        return page([ref._unit_payload(ref._lookup(q["symbol"]))], {}, "units")
    if suffix in {"/unit/maps", "/measurement/maps", "/catalog/mapstates"}:
        field = {"/unit/maps": "unitMapItems", "/measurement/maps": "measurementMapItems",
                 "/catalog/mapstates": "mapStates"}[suffix]
        # No fabricated cross-namespace mappings in a single-namespace catalog.
        return page([], query, field)
    if suffix == "/catalog/lastmodified":
        ref.query_values(query, {})
        return {"lastModified": MODIFIED}
    if suffix == "/catalog":
        ref.query_values(query, {})
        return {"units": units, "measurements": measurements(), "unitSystemInfos": [deepcopy(SYSTEM)],
                "unitMaps": [], "measurementMaps": [], "mapStates": [], "lastModified": MODIFIED,
                "totalUnitCount": len(units), "totalMeasurementCount": len(measurements()),
                "totalUnitSystemCount": 1, "totalUnitMapCount": 0, "totalMeasurementMapCount": 0,
                "totalMapStateCount": 0}
    if suffix == "/conversion/abcd":
        if method == "GET":
            q = ref.query_values(query, {"namespaces", "fromSymbol", "toSymbol"},
                                 {"namespaces", "fromSymbol", "toSymbol"})
            ref._namespace(q)
            source, target = ref._lookup(q["fromSymbol"]), ref._lookup(q["toSymbol"])
        else:
            ref.query_values(query, {})
            object_value(payload, {"fromUnit", "toUnit", "fromUnitPersistableReference", "toUnitPersistableReference"})
            values = []
            for side in ("fromUnit", "toUnit"):
                if (side in payload) == (side + "PersistableReference" in payload):
                    ref.bad("Require one representation of " + side)
                value = payload[side] if side in payload else ref.strict_json(
                    ref.text(payload[side + "PersistableReference"], side))
                values.append(unit_essence(value))
            source, target = values
        result = ref._conversion(source, target)
        coefficients = result.pop("scaleOffset")
        scale, offset = coefficients["scale"], coefficients["offset"]
        # y = scale*(x-offset) = (a+b*x)/(c+d*x)
        result["abcd"] = {"a": -scale * offset, "b": scale, "c": 1.0, "d": 0.0}
        return result
    raise ref.ReferenceError(404, "Unit operation not registered")


def crs_code(value, label="CRS"):
    ref.text(value, label)
    for record in ref._CRS:
        if value in {record["id"], record["id"] + ":", record["data"]["ID"]}:
            return record["data"]["Code"]
    return ref._resolve_crs(value, label)


def crs_reference(code):
    return next(r["data"]["PersistableReference"] for r in ref._CRS if r["data"]["Code"] == code)


def crs_by_id(payload):
    object_value(payload, {"recordId", "dataId", "points", "offset", "limit", "returnedFields"})
    if ("recordId" in payload) == ("dataId" in payload):
        ref.bad("Require one recordId or dataId")
    key = "recordId" if "recordId" in payload else "dataId"
    value = ref.text(payload[key], key)
    for record in ref._CRS:
        if value in ({record["id"], record["id"] + ":"} if key == "recordId" else {record["data"]["ID"]}):
            return record
    raise ref.ReferenceError(404, "CRS not found in fixed catalog")


def transformation_records():
    rows = []
    for source in ref._CRS:
        for target in ref._CRS:
            source_code, target_code = source["data"]["Code"], target["data"]["Code"]
            if source_code != "4326" and target_code != "4326":
                continue
            code = source_code + "-" + target_code
            rows.append({"id": "osdu-lite:reference-data--CoordinateTransformation:" + code,
                         "kind": "osdu:wks:reference-data--CoordinateTransformation:1.0.0",
                         "data": {"ID": "OSDU-Lite:" + code, "Code": code, "CodeSpace": "OSDU-Lite",
                                  "Kind": "horizontal projection", "Name": source["data"]["Name"] + " to " +
                                  target["data"]["Name"], "SourceCRS": source["id"], "TargetCRS": target["id"],
                                  "InactiveIndicator": False, "LocalEmulatorOperation": True,
                                  "PersistableReference": json.dumps({"fromCRS": crs_reference(source_code),
                                                                     "toCRS": crs_reference(target_code)}),
                                  "Description": ref._transformer(source_code, target_code).description}})
    return rows


def catalog_api(method, suffix, query, payload):
    if suffix == "/points-in-aou":
        ref.query_values(query, {})
        record = crs_by_id(payload)
        points = payload.get("points")
        if not isinstance(points, list) or not 1 <= len(points) <= 1000:
            ref.bad("points must contain 1-1000 points")
        entry = next(r for r in ref._SOURCE["crs"] if r["code"] == record["data"]["Code"])
        west, south, east, north = entry["extent"]
        failures = []
        geod = Geod(ellps="WGS84")
        for index, point in enumerate(points):
            object_value(point, {"longitude", "latitude"}, "point")
            lon, lat = ref.finite(point.get("longitude"), "longitude"), ref.finite(point.get("latitude"), "latitude")
            if not -180 <= lon <= 180 or not -90 <= lat <= 90:
                ref.bad("Point outside geographic domain")
            if not ref._in_domain(entry["code"], lon, lat):
                nearest_lon, nearest_lat = min(east, max(west, lon)), min(north, max(south, lat))
                _, _, distance = geod.inv(lon, lat, nearest_lon, nearest_lat)
                failures.append({"point": deepcopy(point), "index": index,
                                 "approximateKmDistanceOutside": math.ceil(abs(distance) / 1000)})
        return {"bboxFailedPoints": failures,
                "maxDistKmOutsideBBox": str(max((p["approximateKmDistanceOutside"] for p in failures), default=0))}
    rows = transformation_records()
    if method == "GET":
        q = ref.query_values(query, {"recordId", "dataId"})
        if len(q) != 1:
            ref.bad("Require one recordId or dataId")
        key, value = next(iter(q.items()))
        selected = [r for r in rows if value in {r["id"], r["id"] + ":"}] if key == "recordId" else [
            r for r in rows if r["data"]["ID"] == value]
        if not selected:
            raise ref.ReferenceError(404, "Transformation not found in local catalog")
        return ref._search_response(selected, q)
    ref.query_values(query, {})
    filters = {"codeSpace": "CodeSpace", "code": "Code", "name": "Name", "id": "ID",
               "kind": "Kind", "sourceCRS": "SourceCRS", "targetCRS": "TargetCRS"}
    object_value(payload, set(filters) | {"offset", "limit", "returnAllFields", "returnedFields",
                                         "includeDeprecated", "latitude", "longitude"})
    for key, field in filters.items():
        if key in payload:
            value = ref.text(payload[key], key)
            if key == "kind" and value == "All":
                continue
            if key == "kind" and value == "ExcludeVertical":
                rows = [r for r in rows if "vertical" not in r["data"]["Kind"].casefold()]
                continue
            rows = [r for r in rows if (value.casefold() in r["data"][field].casefold() if key == "name"
                                        else r["data"][field].rstrip(":") == value.rstrip(":"))]
    ref.boolean(payload.get("includeDeprecated", False), "includeDeprecated")
    ref.boolean(payload.get("returnAllFields", False), "returnAllFields")
    if "latitude" in payload or "longitude" in payload:
        lon = ref.finite(payload.get("longitude"), "longitude", strings=True)
        lat = ref.finite(payload.get("latitude"), "latitude", strings=True)
        rows = [r for r in rows if all(ref._in_domain(crs_code(r["data"][key]), lon, lat)
                                      for key in ("SourceCRS", "TargetCRS"))]
    offset = ref.integer(payload.get("offset", 0), "offset")
    limit = ref.integer(payload.get("limit", 100), "limit")
    if limit > 1000:
        ref.bad("limit must not exceed 1000")
    fields = payload.get("returnedFields")
    if fields is not None:
        if not isinstance(fields, list) or len(fields) > 100:
            ref.bad("returnedFields must be a bounded array")
        ref._project(transformation_records()[0], fields)
    result = ref._search_response(rows[offset:offset + limit], payload, fields)
    result["searchResults"]["totalCount"] = len(rows)
    return result


def length_unit(value, label):
    ref.text(value, label)
    symbol = None
    if value.startswith("osdu:reference-data--UnitOfMeasure:") and value.endswith(":"):
        symbol = value[len("osdu:reference-data--UnitOfMeasure:"):-1]
    unit = ref._lookup(symbol) if symbol else unit_essence(ref.strict_json(value))
    if unit["dimension"] != "Length":
        ref.bad(label + " must be a supported length unit")
    return unit


def convert_geojson(payload):
    object_value(payload, {"toCRS", "toUnitZ", "featureCollection"})
    target = crs_code(payload.get("toCRS"), "toCRS")
    fc = deepcopy(payload.get("featureCollection"))
    if not isinstance(fc, dict) or fc.get("type") != "FeatureCollection":
        ref.bad("featureCollection must be a GeoJSON FeatureCollection")
    source_id = fc.get("persistableReferenceCrs") or fc.get("coordinateReferenceSystemID") or fc.get(
        "CoordinateReferenceSystemID")
    source = crs_code(source_id) if source_id else "4326"
    vertical = None
    if "toUnitZ" in payload:
        source_unit = fc.get("persistableReferenceUnitZ") or fc.get("verticalUnitID") or fc.get("VerticalUnitID")
        source_z, target_z = length_unit(source_unit, "source unitZ"), length_unit(payload["toUnitZ"], "toUnitZ")
        vertical = source_z["a"] / target_z["a"]
    positions = []
    nodes = 0

    def coordinates(value, depth):
        if depth == 0:
            if not isinstance(value, list) or len(value) not in {2, 3}:
                ref.bad("GeoJSON positions require 2 or 3 numeric ordinates")
            for item in value:
                ref.finite(item, "ordinate")
            positions.append(value)
            if len(positions) > 1000:
                ref.bad("GeoJSON point limit is 1000")
        else:
            if not isinstance(value, list) or len(value) > 1000:
                ref.bad("Invalid or oversized GeoJSON coordinate array")
            for item in value:
                coordinates(item, depth - 1)

    def visit(value, depth=0):
        nonlocal nodes
        nodes += 1
        if nodes > 10000 or depth > 24 or not isinstance(value, dict):
            ref.bad("GeoJSON structure exceeds limits or is malformed")
        kind = ref.text(value.get("type"), "GeoJSON type")
        value.pop("bbox", None)  # An input bbox is invalid after projection.
        if kind == "FeatureCollection":
            children = value.get("features")
        elif kind == "GeometryCollection":
            children = value.get("geometries")
        elif kind == "Feature":
            if "geometry" not in value:
                ref.bad("Feature requires geometry")
            if value["geometry"] is not None:
                visit(value["geometry"], depth + 1)
            return
        else:
            levels = {"Point": 0, "MultiPoint": 1, "LineString": 1, "MultiLineString": 2,
                      "Polygon": 2, "MultiPolygon": 3}
            if kind not in levels:
                ref.bad("Unsupported GeoJSON type")
            coordinates(value.get("coordinates"), levels[kind])
            return
        if not isinstance(children, list) or len(children) > 1000:
            ref.bad("GeoJSON collection requires at most 1000 children")
        for child in children:
            visit(child, depth + 1)

    visit(fc)
    operations = []
    if positions:
        result = ref.convert_points({"fromCRS": crs_reference(source), "toCRS": crs_reference(target),
                                     "points": [{"x": p[0], "y": p[1], "z": p[2] if len(p) == 3 else 0}
                                                for p in positions]})
        if int(result["successCount"]) != len(positions):
            raise ref.ReferenceError(422, "GeoJSON position outside supported projection domain; no partial conversion")
        operations = result["operationsApplied"]
        for position, converted in zip(positions, result["points"]):
            position[0], position[1] = float(converted["x"]), float(converted["y"])
            if len(position) == 3 and vertical is not None:
                position[2] = ref.finite(position[2] * vertical, "converted z")
    fc["persistableReferenceCrs"] = crs_reference(target)
    for key in ("coordinateReferenceSystemID", "CoordinateReferenceSystemID"):
        if key in fc:
            fc[key] = next(r["id"] + ":" for r in ref._CRS if r["data"]["Code"] == target)
    if vertical is not None:
        fc["persistableReferenceUnitZ"] = ref._unit_payload(target_z)["essenceJson"]
        for key in ("verticalUnitID", "VerticalUnitID"):
            if key in fc:
                fc[key] = "osdu:reference-data--UnitOfMeasure:" + target_z["symbol"] + ":"
        operations.append("Linear vertical unit rescaling only; no vertical datum transformation")
    else:
        operations.append("Z ordinates preserved; no vertical datum transformation")
    return {"featureCollection": fc, "successCount": str(len(positions)), "totalCount": str(len(positions)),
            "operationsApplied": operations}


def convert_trajectory(payload):
    object_value(payload, {"trajectoryCRS", "azimuthReference", "unitXY", "unitZ", "referencePoint",
                           "inputStations", "method", "inputKind", "interpolate"})
    code = crs_code(payload.get("trajectoryCRS"), "trajectoryCRS")
    if payload.get("method") != "AzimuthalEquidistant" or payload.get("azimuthReference") != "TRUE_NORTH":
        ref.bad("Mock trajectory supports AzimuthalEquidistant and TRUE_NORTH only")
    kind = payload.get("inputKind", "MD_Incl_Azim")
    if kind not in {"MD_Incl_Azim", "MD_Inclination_Azimuth"}:
        ref.bad("Mock trajectory supports measured-depth/inclination/azimuth stations only")
    if "interpolate" in payload:
        ref.boolean(str(payload["interpolate"]).lower(), "interpolate")
    unit_z = length_unit(payload.get("unitZ"), "unitZ")
    unit_xy = length_unit(payload.get("unitXY", ref._unit_payload(ref._UNITS["m"])["essenceJson"]), "unitXY")
    point = payload.get("referencePoint", {"x": "0", "y": "0", "z": "0"})
    object_value(point, {"x", "y", "z"}, "referencePoint")
    x, y, z = [ref.finite(point.get(axis), axis, strings=True) for axis in ("x", "y", "z")]
    network.set_network_enabled(False)
    origin = ref.convert_points({"fromCRS": crs_reference(code), "toCRS": crs_reference("4326"),
                                 "points": [{"x": x, "y": y, "z": z}]})
    if origin["successCount"] != "1":
        ref.bad("Reference point outside CRS domain")
    lon, lat = float(origin["points"][0]["x"]), float(origin["points"][0]["y"])
    stations = ref.strict_json(ref.text(payload.get("inputStations"), "inputStations"))
    if not isinstance(stations, list) or not 1 <= len(stations) <= 1000:
        ref.bad("inputStations must encode an array of 1-1000 stations")
    validated = []
    for station in stations:
        object_value(station, {"md", "inclination", "azimuth"}, "station")
        md, inclination, azimuth = [ref.finite(station.get(k), k, strings=True)
                                    for k in ("md", "inclination", "azimuth")]
        if md < 0 or md * unit_z["a"] > 100000 or not 0 <= inclination <= 180 or not 0 <= azimuth <= 360:
            ref.bad("Station outside bounded MD/angle domain")
        if validated and md <= validated[-1][0]:
            ref.bad("Measured depths must be strictly increasing")
        if validated and (md - validated[-1][0]) * unit_z["a"] < 1e-9:
            ref.bad("Adjacent mock stations must be separated by at least 1e-9 metres")
        validated.append((md, inclination, azimuth))
    if validated[0][0] != 0:
        ref.bad("First mock station must start at MD 0")
    local = CRS.from_proj4(f"+proj=aeqd +lat_0={lat} +lon_0={lon} +datum=WGS84 +units=m +no_defs")
    to_geo = Transformer.from_crs(local, CRS.from_epsg(4326), always_xy=True, allow_ballpark=False)
    # Minimum-curvature integration in a local tangent frame; no grid convergence,
    # LMP, interpolation or geodetic survey correction is claimed.
    east = north = down = 0.0
    previous = None
    output = []
    for md, inc, azi in validated:
        i, a = math.radians(inc), math.radians(azi)
        dogleg = 0.0
        if previous is not None:
            old_md, old_i, old_a = previous
            interval = (md - old_md) * unit_z["a"]
            cosine = math.cos(old_i) * math.cos(i) + math.sin(old_i) * math.sin(i) * math.cos(a - old_a)
            dogleg = math.acos(max(-1.0, min(1.0, cosine)))
            if dogleg > math.pi - 1e-6:
                ref.bad("Antiparallel adjacent station tangents are unsupported")
            ratio = 1.0 if dogleg < 1e-10 else 2 * math.tan(dogleg / 2) / dogleg
            east += interval / 2 * (math.sin(old_i) * math.sin(old_a) + math.sin(i) * math.sin(a)) * ratio
            north += interval / 2 * (math.sin(old_i) * math.cos(old_a) + math.sin(i) * math.cos(a)) * ratio
            down += interval / 2 * (math.cos(old_i) + math.cos(i)) * ratio
        out_lon, out_lat = to_geo.transform(east, north, errcheck=True)
        tx, ty = ref._transformer("4326", code).transform(out_lon, out_lat, errcheck=True)
        if not ref._in_domain(code, out_lon, out_lat):
            ref.bad("Trajectory leaves supported CRS domain")
        dls = 0 if previous is None else math.degrees(dogleg) * 30 / ((md - previous[0]) * unit_z["a"])
        output.append({"md": str(md), "inclination": str(inc), "azimuthTN": str(azi),
                       "dxTN": str(east / unit_xy["a"]), "dyTN": str(north / unit_xy["a"]),
                       "dz": ref.finite(down / unit_z["a"], "dz"),
                       "point": json.dumps({"x": tx, "y": ty, "z": z - down / unit_z["a"]}, allow_nan=False),
                       "wgs84Longitude": str(out_lon), "wgs84Latitude": str(out_lat), "dls": str(dls),
                       "original": "true"})
        previous = md, i, a
    return {"trajectoryCRS": crs_reference(code), "unitXY": ref._unit_payload(unit_xy)["essenceJson"],
            "unitZ": ref._unit_payload(unit_z)["essenceJson"], "unitDls": "deg/30m",
            "stations": output, "localCRS": local.to_wkt(), "method": payload["method"], "inputKind": kind,
            "operationsApplied": ["OSDU-Lite MOCK approximate local minimum-curvature integration; "
                                  "no survey accuracy, grid convergence, datum correction or interpolation"]}


def handle(service, method, suffix, args, query, payload):
    if service == "unit":
        return unit_api(method, suffix, query, payload)
    if service == "crs-catalog":
        return catalog_api(method, suffix, query, payload)
    ref.query_values(query, {})
    if suffix == "/convertGeoJson":
        return convert_geojson(payload)
    try:
        return convert_trajectory(payload)
    except ProjError:
        ref.bad("Trajectory outside supported projection domain")
