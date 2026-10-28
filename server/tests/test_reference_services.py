import copy
import io
import json
import math
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlencode

from test_server import call, make_handler, _Headers, server

CATALOG = "/api/crs/catalog/v3/coordinate-reference-system"
CONVERT = "/api/crs/converter/v3/convert"
UNIT = "/api/unit/v3"
FIXTURE = json.loads((Path(__file__).parent / "fixtures/reference_contracts.json").read_text())


def get_unit(symbol):
    status, body = call("get", UNIT + "/unit/symbol?" + urlencode({
        "namespaces": "osdu-lite", "symbol": symbol}))
    if status != 200:
        raise AssertionError((status, body))
    return body


def coefficients(source, target):
    return call("get", UNIT + "/conversion/scale?" + urlencode({
        "namespaces": "osdu-lite", "fromSymbol": source, "toSymbol": target}))


class CatalogTests(unittest.TestCase):
    def test_fixed_catalog_and_contract_projection(self):
        status, body = call("post", CATALOG, {})
        self.assertEqual(status, 200)
        records = body["searchResults"]["results"]
        self.assertEqual(body["searchResults"]["totalCount"], 4)
        self.assertEqual([r["data"]["Code"] for r in records], ["3857", "4326", "32638", "32639"])
        self.assertIn("OSDU-Lite", body["query"])
        for r in records:
            status, detail = call("get", CATALOG + "?" + urlencode({"recordId": r["id"] + ":"}))
            self.assertEqual(status, 200)
            data = detail["searchResults"]["results"][0]["data"]
            for key in ["ID", "Name", "Kind", "CoordinateSystem", "PreferredUsage", "PersistableReference"]:
                self.assertIn(key, data)
            self.assertEqual(call("get", CATALOG + "?" + urlencode({"dataId": data["ID"]}))[0], 200)

    def test_filters_pagination_and_projection(self):
        status, body = call("post", CATALOG, {
            "codeSpace": "EPSG", "name": "utm", "kind": "projected", "offset": "1", "limit": "1",
            "returnedFields": ["id", "data.Name"], "includeDeprecated": "false"})
        self.assertEqual(status, 200)
        self.assertEqual(body["searchResults"]["totalCount"], 2)
        self.assertEqual(body["searchResults"]["results"][0]["data"], {"Name": "WGS 84 / UTM zone 39N"})
        for query in [{"code": "432"}, {"code": "4326", "kind": "projected"}, {"id": "EPSG:4326"}]:
            self.assertEqual(call("post", CATALOG, query)[1]["searchResults"]["totalCount"], 0)
        for query in [{"code": "4326"}, {"id": "Geographic2D:EPSG::4326"}]:
            self.assertEqual(call("post", CATALOG, query)[1]["searchResults"]["totalCount"], 1)
        self.assertIn("CoordinateSystem", call("post", CATALOG, {
            "returnAllFields": True, "returnedFields": ["nonsense"]})[1]["searchResults"]["results"][0]["data"])

    def test_catalog_errors(self):
        for suffix, expected in [
            ("", 400), ("?recordId=", 400), ("?recordId=x&dataId=x", 400),
            ("?dataId=x&dataId=y", 400), ("?dataId=x", 404), ("?recordId=x", 400),
            ("?recordId=osdu:reference-data--CoordinateReferenceSystem:Projected:EPSG::9999:", 404),
            ("?dataId=x&unsupported=1", 400),
        ]:
            self.assertEqual(call("get", CATALOG + suffix)[0], expected, suffix)
        for body in [[], None, {"latitude": "1"}, {"limit": -1}, {"offset": True},
                     {"includeDeprecated": "yes"}, {"returnedFields": ["data.Unknown"]},
                     {"name": ""}, {"name": 3}, {"code": 4326}, {"returnAllFields": None}]:
            self.assertEqual(call("post", CATALOG, body)[0], 400, body)

    def test_reference_state_is_independent_and_concurrent(self):
        server.clear_state()
        before = copy.deepcopy(server.RECORD_BY_ID)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: call("post", CATALOG, {}), range(32)))
        self.assertTrue(all(result == results[0] for result in results))
        server.clear_state()
        self.assertEqual(call("post", CATALOG, {}), results[0])
        self.assertEqual(server.RECORD_BY_ID, before)


class UnitTests(unittest.TestCase):
    def test_catalog_lists_and_namespace(self):
        for path, key, total in [("/unit", "units", 17), ("/measurement/list", "measurements", 6)]:
            status, body = call("get", UNIT + path + "?limit=-1")
            self.assertEqual(status, 200)
            self.assertEqual((body["offset"], body["count"], body["totalCount"]), (0, total, total))
            self.assertEqual(len(body[key]), total)
            page = call("get", UNIT + path + "?offset=1&limit=2")[1]
            self.assertEqual((page["offset"], page["count"], page["totalCount"]), (1, 2, total))
        body = call("get", UNIT + "/unit/measurement?ancestry=Length")[1]
        self.assertEqual(body["totalCount"], 4)
        self.assertEqual(get_unit("ft[US]")["namespace"], "osdu-lite")
        self.assertEqual(get_unit("m")["essence"], FIXTURE["examples"]["unitEssence"])
        self.assertEqual(json.loads(get_unit("m")["essenceJson"]), get_unit("m")["essence"])
        for query, code in [("symbol=m", 400), ("namespaces=&symbol=m", 400),
                            ("namespaces=unknown&symbol=m", 404),
                            ("namespaces=osdu-lite&symbol=m/unknown", 404),
                            ("namespaces=osdu-lite&symbol=m&symbol=s", 400)]:
            self.assertEqual(call("get", UNIT + "/unit/symbol?" + query)[0], code)
        for query in ["offset=-1", "offset=1.5", "limit=-2", "limit=true", "limit=", "limit=1&limit=2", "x=1"]:
            self.assertEqual(call("get", UNIT + "/unit?" + query)[0], 400, query)
        self.assertEqual(call("get", UNIT + "/unit?limit=0")[1]["count"], 0)
        self.assertEqual(call("get", UNIT + "/unit/measurement?ancestry=unknown")[0], 404)

    def test_independent_golden_unit_vectors(self):
        for source, target, x, expected in [
            ("ft", "m", 1, 0.3048), ("ft[US]", "m", 1, 1200 / 3937),
            ("ms", "s", 1000, 1), ("dega", "rad", 180, math.pi),
            ("bar", "Pa", 1, 100000), ("psi", "Pa", 1, 6894.757293168361),
            ("degC", "K", 0, 273.15), ("degC", "K", 100, 373.15),
            ("degF", "degC", 32, 0), ("degF", "degC", 212, 100),
            ("degF", "degC", -40, -40), ("K", "degF", 273.15, 32),
            ("ft/s", "m/s", 1, 0.3048), ("m", "m", 12, 12),
        ]:
            with self.subTest(source=source, target=target, x=x):
                status, body = coefficients(source, target)
                self.assertEqual(status, 200)
                c = body["scaleOffset"]
                self.assertAlmostEqual(c["scale"] * (x - c["offset"]), expected, delta=1e-10)
                self.assertEqual(set(body), {"fromUnit", "toUnit", "scaleOffset"})
        self.assertNotEqual(coefficients("ft", "m")[1], coefficients("ft[US]", "m")[1])
        self.assertEqual(coefficients("m", "s")[0], 400)
        self.assertEqual(coefficients("degC/s", "m/s")[0], 404)

    def test_post_essences_and_conflicts(self):
        a, b = get_unit("degF"), get_unit("degC")
        for source_key, source in [("fromUnit", a["essence"]),
                                   ("fromUnitPersistableReference", a["essenceJson"])]:
            for target_key, target in [("toUnit", b["essence"]),
                                       ("toUnitPersistableReference", b["essenceJson"])]:
                body = {source_key: source, target_key: target}
                before = copy.deepcopy(body)
                self.assertEqual(call("post", UNIT + "/conversion/scale", body), coefficients("degF", "degC"))
                self.assertEqual(body, before)
        valid = {"fromUnit": a["essence"], "toUnit": b["essence"]}
        invalid = [{}, {**valid, "fromUnitPersistableReference": a["essenceJson"]},
                   {**valid, "extra": 1}, {**valid, "fromUnitPersistableReference": None}]
        for field in ["scale", "offset"]:
            for value in [True, None, "1", "NaN", float("nan"), float("inf"), 1e309, 999]:
                body = copy.deepcopy(valid)
                body["fromUnit"]["scaleOffset"][field] = value
                invalid.append(body)
        for body in invalid:
            self.assertEqual(call("post", UNIT + "/conversion/scale", body)[0], 400, body)


class ReferenceHTTPTests(unittest.TestCase):
    def test_all_catalog_unit_routes_aliases_and_auth(self):
        cases = [
            ("post", CATALOG, {}), ("get", CATALOG + "?dataId=Geographic2D:EPSG::4326", None),
            ("get", UNIT + "/unit", None), ("get", UNIT + "/measurement/list", None),
            ("get", UNIT + "/unit/measurement?ancestry=Length", None),
            ("get", UNIT + "/unit/symbol?namespaces=osdu-lite&symbol=m", None),
            ("get", UNIT + "/conversion/scale?namespaces=osdu-lite&fromSymbol=ft&toSymbol=m", None),
            ("post", UNIT + "/conversion/scale", {
                "fromUnit": FIXTURE["examples"]["unitEssence"], "toUnit": FIXTURE["examples"]["unitEssence"]}),
        ]
        for method, path, body in cases:
            self.assertEqual(call(method, path, body)[0], 200, path)
            self.assertEqual(call(method, path[4:], body), call(method, path, body))
            self.assertEqual(call(method, path, body, token="")[0], 401)
            self.assertEqual(call(method, path, body, token="wrong")[0], 401)
            self.assertEqual(call(method, path, body, partition="")[0], 400)
            with patch.dict(server.GROUPS_BY_TOKEN, {server.TOKEN: set()}):
                self.assertEqual(call(method, path, body)[0], 403)
            with patch.object(server, "REQUIRE_AUTH", False):
                self.assertEqual(call(method, path, body, token="", partition="")[0], 200)

    def test_strict_new_json_and_body_limits(self):
        for raw in ["{", "[]", "null", '{"offset":NaN}', '{"offset":Infinity}',
                    '{"offset":1e9999}', '{"offset":1,"offset":2}']:
            self.assertEqual(call("post", CATALOG, raw, raw=True)[0], 400, raw)
        self.assertEqual(call("post", CATALOG, " " * (1048576 + 1), raw=True)[0], 413)
        for length in ["abc", "-1", "1048577"]:
            h = make_handler(CATALOG, _Headers({
                "Authorization": "Bearer " + server.TOKEN, "data-partition-id": server.PARTITION,
                "Content-Length": length}), "{}")
            h.do_POST()
            self.assertEqual(h._status, 413 if length == "1048577" else 400)

    def test_cors_correlation_and_options(self):
        for method, path, raw, expected in [("do_POST", CATALOG, "{}", 200),
                                            ("do_POST", CATALOG, "[]", 400),
                                            ("do_OPTIONS", CATALOG, "", 204)]:
            h = make_handler(path, _Headers({
                "Authorization": "Bearer " + server.TOKEN, "data-partition-id": server.PARTITION,
                "Content-Length": str(len(raw)), "correlation-id": "reference-test"}), raw)
            getattr(h, method)()
            self.assertEqual(h._status, expected)
            headers = dict(h._response_headers)
            self.assertEqual(headers["Access-Control-Allow-Origin"], "*")
            if expected != 204:
                self.assertEqual(headers["correlation-id"], "reference-test")


def crs_reference(code):
    status, body = call("post", CATALOG, {"code": str(code)})
    if status != 200:
        raise AssertionError((status, body))
    return body["searchResults"]["results"][0]["data"]["PersistableReference"]


def conversion_body(source=4326, target=3857, points=None):
    return {"fromCRS": crs_reference(source), "toCRS": crs_reference(target),
            "points": [{"x": 10, "y": 20, "z": 123.45}] if points is None else points}


class ConversionTests(unittest.TestCase):
    def test_golden_vectors_roundtrip_axis_and_z(self):
        for code, x, y, expected_x, expected_y in [
            (3857, 0, 0, 0, 0),
            (3857, 10, 20, 1113194.9079327357, 2273030.9269876895),
            (32638, 45, 0, 500000, 0), (32639, 51, 0, 500000, 0),
        ]:
            with self.subTest(code=code, x=x, y=y):
                request = conversion_body(4326, code, [{"x": x, "y": y, "z": "123.45"}])
                before = copy.deepcopy(request)
                status, result = call("post", CONVERT, request)
                self.assertEqual(status, 200)
                self.assertEqual(result["successCount"], "1")
                point = result["points"][0]
                self.assertAlmostEqual(float(point["x"]), expected_x, delta=1e-6)
                self.assertAlmostEqual(float(point["y"]), expected_y, delta=1e-6)
                self.assertEqual(float(point["z"]), 123.45)
                self.assertTrue(all(isinstance(v, str) for v in point.values()))
                self.assertTrue(result["operationsApplied"])
                reverse = call("post", CONVERT, conversion_body(code, 4326, [point]))[1]["points"][0]
                self.assertAlmostEqual(float(reverse["x"]), x, delta=1e-8)
                self.assertAlmostEqual(float(reverse["y"]), y, delta=1e-8)
                self.assertEqual(request, before)

    def test_ordered_domain_failures_and_limits(self):
        failed = FIXTURE["decisions"]["failedPoint"]
        points = [{"x": 10, "y": 20, "z": 0}, {"x": 10, "y": 86, "z": 3},
                  {"x": 0, "y": 0, "z": 9007199254740993}]
        result = call("post", CONVERT, conversion_body(points=points))[1]
        self.assertEqual(result["successCount"], "2")
        self.assertEqual(len(result["points"]), 3)
        self.assertEqual(result["points"][1], failed)
        self.assertEqual(result["points"][2]["z"], "9007199254740993")
        for source, target, x, y in [
            (4326, 4326, 181, 0), (4326, 4326, 0, 91), (4326, 3857, 0, -86),
            (4326, 32638, 41.9, 0), (4326, 32638, 45, -1), (4326, 32638, 45, 84.1),
            (4326, 32639, 47.9, 0), (3857, 4326, 40075016.68557849, 0),
            (3857, 4326, 0, 1e308), (32638, 4326, -1e8, -1e8),
        ]:
            request = conversion_body(source, target, [{"x": x, "y": y, "z": 0}])
            status, body = call("post", CONVERT, request)
            self.assertEqual(status, 200, request)
            self.assertEqual(body["points"], [failed], request)
            self.assertEqual(body["successCount"], "0")
            self.assertEqual(body["operationsApplied"], [])
        for code, x, y in [(3857, 180, 85.0511287798066), (3857, -180, -85.0511287798066),
                            (32638, 42, 0), (32638, 48, 84), (32639, 48, 0), (32639, 54, 84)]:
            body = call("post", CONVERT, conversion_body(4326, code, [{"x": x, "y": y, "z": 0}]))[1]
            self.assertEqual(body["successCount"], "1", (code, x, y, body))
            self.assertEqual(call("post", CONVERT, conversion_body(code, 4326, body["points"]))[1]["successCount"], "1")
        point = {"x": 0, "y": 0, "z": 0}
        for size, expected in [(0, 400), (1000, 200), (1001, 400)]:
            self.assertEqual(call("post", CONVERT, conversion_body(points=[point] * size))[0], expected)

    def test_rejects_malformed_and_nonfinite_coordinates(self):
        for value in [True, False, None, "NaN", "Infinity", "", " 1", "1.2m", "01", [], {},
                      float("nan"), float("inf"), -float("inf"), 10 ** 400, "1e9999"]:
            for axis in ["x", "y", "z"]:
                point = {"x": 10, "y": 20, "z": 0, axis: value}
                status, body = call("post", CONVERT, conversion_body(points=[point]))
                self.assertEqual(status, 400, (axis, value, body))
                self.assertIn("message", body)
        for points in [None, {}, [None], [[1, 2, 3]], [{"x": 1, "y": 2}],
                       [{"x": 1, "y": 2, "z": 0, "w": 1}]]:
            body = conversion_body()
            body["points"] = points
            self.assertEqual(call("post", CONVERT, body)[0], 400)
        body = conversion_body(points=[{"x": "1e1", "y": "2e1", "z": "-0.00"}])
        self.assertEqual(call("post", CONVERT, body)[1]["successCount"], "1")

    def test_allowlist_and_integrity(self):
        reference = json.loads(crs_reference(4326))
        invalid = ["EPSG:4326", "osdu:reference-data--CoordinateReferenceSystem:Geographic2D:EPSG::4326:",
                   "+proj=longlat", None, {}, json.dumps({**reference, "type": "EBC"}),
                   json.dumps({**reference, "wkt": "EPSG:3857"}),
                   json.dumps({**reference, "extra": 1}),
                   json.dumps({**reference, "authCode": {"auth": "EPSG", "code": "4979"}})]
        for field in ["fromCRS", "toCRS"]:
            for value in invalid:
                body = conversion_body()
                body[field] = value
                self.assertEqual(call("post", CONVERT, body)[0], 400)
        body = conversion_body()
        body["fromCRS"] = json.dumps(reference, sort_keys=True, indent=2)
        self.assertEqual(call("post", CONVERT, body)[0], 200)
        for body in [{}, {**conversion_body(), "unitZ": "m"}]:
            self.assertEqual(call("post", CONVERT, body)[0], 400)

    def test_route_security_limits_isolation_and_engine_error(self):
        body = conversion_body()
        for path in [CONVERT, CONVERT[4:]]:
            self.assertEqual(call("post", path, body)[0], 200)
            self.assertEqual(call("post", path, body, token="")[0], 401)
            self.assertEqual(call("post", path, body, token="wrong")[0], 401)
            self.assertEqual(call("post", path, body, partition="wrong")[0], 400)
            with patch.dict(server.GROUPS_BY_TOKEN, {server.TOKEN: set()}):
                self.assertEqual(call("post", path, body)[0], 403)
            with patch.object(server, "REQUIRE_AUTH", False):
                self.assertEqual(call("post", path, body, token="", partition="")[0], 200)
            self.assertEqual(call("post", path, " " * 1048577, raw=True)[0], 413)
            self.assertEqual(call("post", path, [], raw=False)[0], 400)
        before = copy.deepcopy((server.RECORD_BY_ID, server.SEARCH_BACKEND.documents, server.WORKFLOW_RUNS))
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: call("post", CONVERT, body), range(32)))
        self.assertTrue(all(r == results[0] for r in results))
        self.assertEqual((server.RECORD_BY_ID, server.SEARCH_BACKEND.documents, server.WORKFLOW_RUNS), before)
        import reference_services
        self.assertFalse(reference_services.network.is_network_enabled())
        with patch.object(reference_services.Transformer, "from_crs", side_effect=RuntimeError("secret path")):
            status, response = call("post", CONVERT, body)
            self.assertEqual(status, 500)
            self.assertNotIn("secret", json.dumps(response))


if __name__ == "__main__":
    unittest.main()
