"""Behavioral acceptance checks for every operation in community_api.json."""
import base64
import copy
import json
import math
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlencode

from test_server import ServerTestCase, call, make_handler, _Headers, server, valid_well

INVENTORY = json.loads((Path(__file__).parents[1] / "data/community_api.json").read_text())


class CommunityAcceptance(ServerTestCase):
    def test_reset_prevents_stale_delayed_index_work(self):
        sleeping, release = threading.Event(), threading.Event()
        threads = []
        real_thread = threading.Thread

        def capture_thread(*args, **kwargs):
            thread = real_thread(*args, **kwargs)
            threads.append(thread)
            return thread

        def delayed(_):
            sleeping.set()
            release.wait(5)

        old = {**valid_well(), "version": 1}
        server.RECORD_BY_ID[old["id"]] = copy.deepcopy(old)
        try:
            with patch.object(server.time, "sleep", side_effect=delayed), \
                    patch.object(server.threading, "Thread", side_effect=capture_thread):
                server.index_records_later([server.RECORD_BY_ID[old["id"]]])
                self.assertTrue(sleeping.wait(5))
                server.clear_state()
                # The same public ID/version can legitimately be recreated after reset.
                new = {**valid_well(name="NEW-AFTER-RESET"), "version": 1}
                server.RECORD_BY_ID[new["id"]] = new
                release.set()
                threads[0].join(5)
                self.assertFalse(threads[0].is_alive())
                self.assertNotIn(old["id"], server.SEARCH_BACKEND.documents)
        finally:
            release.set()
            for thread in threads:
                thread.join(5)

    def test_partition_lifecycle_limits_and_isolation(self):
        base = "/api/partition/v1/partitions"
        properties = {"properties": {"region": {"value": "offline", "sensitive": False}}}
        self.assertEqual(call("post", base + "/demo", properties)[0], 201)
        self.assertEqual(call("post", base + "/demo", properties)[0], 409)
        self.assertEqual(call("delete", base + "/" + server.PARTITION)[0], 409)
        for value in [None, [], {"x": 1}, {"properties": []},
                      {"properties": {"region": {"value": 1, "sensitive": "false"}}}]:
            self.assertEqual(call("patch", base + "/demo", value)[0], 400)
        self.assertEqual(call("get", base)[1], [server.PARTITION, "demo"])
        self.assertEqual(len(server.RECORD_BY_ID), 0)
        self.assertEqual(call("get", "/partition/v1/partitions")[1], call("get", base)[1])

    def test_secret_acl_lifecycle_and_no_list_leak(self):
        import community_services as c
        base = "/api/secret/v2/secrets"
        secret = {"id": "test-key", "value": "mock-not-a-credential", "secretAcls": copy.deepcopy(server.DEFAULT_ACL)}
        self.assertEqual(call("post", base, secret)[0], 200)
        self.assertEqual(call("post", base, secret)[0], 409)
        restricted = {c.group("secret"), server.ACL_VIEWERS}
        with patch.dict(server.GROUPS_BY_TOKEN, {server.TOKEN: restricted}):
            self.assertEqual(call("get", base + "/test-key")[1]["value"], secret["value"])
            self.assertEqual(call("put", base + "/test-key", secret)[0], 403)
            self.assertEqual(call("patch", base + "/test-key/disable")[0], 403)
        with patch.dict(server.GROUPS_BY_TOKEN, {server.TOKEN: {c.group("secret")}}):
            self.assertEqual(call("get", base + "/test-key")[0], 403)
            self.assertEqual(call("get", base + "/list")[1], [])
            self.assertEqual(call("post", base + "/list", ["test-key"])[1]["notAvailableSecrets"], ["test-key"])
        self.assertNotIn(secret["value"], json.dumps(call("post", base + "/list", ["test-key"])[1]))
        self.assertEqual(call("delete", base + "/test-key")[0], 204)
        self.assertEqual(call("get", base + "/test-key")[0], 404)
        self.assertFalse(c.SECRETS)

    def test_register_notification_no_network_and_capacity(self):
        import community_services as c
        base = "/api/register/v1"
        sub = {"id": "test-sub", "name": "Test subscription", "topic": "records-changed",
               "pushEndpoint": "https://169.254.169.254/never-called",
               "secret": {"secretType": "HMAC", "value": "mock-secret"}}
        with patch("urllib.request.urlopen", side_effect=AssertionError("No outbound HTTP allowed")):
            status, created = call("post", base + "/subscription", sub)
            self.assertEqual(status, 201)
            self.assertNotIn("secret", created)
            self.assertEqual(call("post", base + "/subscription", sub)[0], 409)
            payload = {"subscription": "projects/local/subscriptions/" + created["notificationId"],
                       "message": {"attributes": {"data-partition-id": server.PARTITION},
                                   "data": base64.b64encode(b'[{"id":"local"}]').decode().rstrip("=")}}
            endpoint = "/api/notification/v1/push-handlers/records-changed"
            self.assertEqual(call("post", endpoint, payload)[0], 200)
            self.assertFalse(c.EVENTS[0]["delivered"])
            payload["message"]["attributes"]["data-partition-id"] = "other"
            self.assertEqual(call("post", endpoint, payload)[0], 400)
            payload["message"]["attributes"]["data-partition-id"] = server.PARTITION
            with patch.object(c, "MAX_RESOURCES", 1):
                self.assertEqual(call("post", endpoint, payload)[0], 413)
            self.assertEqual(call("delete", base + "/subscription/test-sub")[0], 204)
            self.assertEqual(call("post", endpoint, payload)[0], 404)
            self.assertFalse(c.SUBSCRIPTION_SECRETS)
        self.assertEqual(call("get", base + "/ddms?type=well")[1], [])
        self.assertEqual(call("post", base + "/subscription", {**sub, "topic": []})[0], 400)

    def test_indexer_deleted_records_and_strict_input(self):
        record = valid_well()
        server.upsert_records([record], index_immediately=True)
        server.soft_delete_records([record["id"]])
        endpoint = "/api/indexer/v2"
        self.assertEqual(call("post", endpoint + "/reindex/records", {"recordIds": [record["id"]]})[0], 404)
        self.assertEqual(call("patch", endpoint + "/reindex?force_clean=true")[0], 200)
        self.assertFalse(server.SEARCH_BACKEND.documents)
        self.assertEqual(call("post", endpoint + "/reindex", {"kind": server.WELL_KIND})[0], 200)
        self.assertFalse(server.SEARCH_BACKEND.documents)
        for query in ["force_clean=maybe", "force_clean=true&force_clean=false"]:
            self.assertEqual(call("patch", endpoint + "/reindex?" + query)[0], 400)
        self.assertEqual(call("post", endpoint + "/reindex/records", {"recordIds": ["x"] * 1001})[0], 400)

    def test_unit_public_essences_search_pagination_and_unknowns(self):
        base = "/api/unit/v3"
        body = {"essence": {"type": "UAD", "symbol": "ft",
                "baseMeasurement": {"type": "UM", "ancestry": "Length"},
                "abcd": {"a": "0", "b": "0.3048", "c": "1", "d": "0"}}}
        self.assertEqual(call("post", base + "/unit", body)[1]["displaySymbol"], "ft")
        body["essence"]["abcd"]["b"] = "100"
        self.assertEqual(call("post", base + "/unit", body)[0], 400)
        status, page = call("post", base + "/unit/search?offset=1&limit=2", {"query": "*"})
        self.assertEqual(status, 200)
        self.assertEqual(page["count"], 2)
        self.assertEqual(page["totalCount"], 17)
        self.assertEqual(call("post", base + "/unit/search", {"query": "metre"})[1]["count"], 3)
        self.assertEqual(call("post", base + "/unit/search", {"query": "name:metre"})[0], 400)
        self.assertEqual(call("get", base + "/unitsystem?name=unknown")[0], 404)
        self.assertEqual(call("get", base + "/unit/maps?limit=1001")[0], 400)

    def test_catalog_search_includes_measurements_and_combined_pagination(self):
        endpoint = "/api/unit/v3/catalog/search"
        status, result = call("post", endpoint, {"query": "Length"})
        self.assertEqual(status, 200)
        self.assertEqual(result["totalCount"], 1)
        self.assertEqual(result["measurements"][0]["essence"]["ancestry"], "Length")
        self.assertEqual(result["units"], [])
        status, result = call("post", endpoint + "?offset=16&limit=3", {"query": "*"})
        self.assertEqual(status, 200)
        self.assertEqual(result["totalCount"], 23)
        self.assertEqual(result["count"], 3)
        self.assertEqual(len(result["units"]), 1)
        self.assertEqual(len(result["measurements"]), 2)
        result = call("post", endpoint, {"query": "not-in-the-local-catalog"})[1]
        self.assertEqual(result["totalCount"], 0)
        self.assertEqual(result["units"], [])
        self.assertEqual(result["measurements"], [])

    def test_action_filters_use_record_kind_components(self):
        base = "/api/register/v1"
        action = {"id": "well-action", "name": "Well action",
                  "filter": {"source": ["wks"], "entityType": ["master-data--Well"], "version": ["1.4.0"]},
                  "url": "https://example.invalid/static"}
        self.assertEqual(call("post", base + "/action", action)[0], 201)
        record = valid_well()
        status, result = call("post", base + "/action:test", {"action": action, "testPayload": record})
        self.assertEqual(status, 200)
        self.assertEqual(result["url"], action["url"])
        self.assertEqual(result["errors"], "")
        self.assertEqual(call("post", base + "/action:retrieve", record)[1][0]["id"], action["id"])
        record["kind"] = "osdu:wks:master-data--Well:2.0.0"
        self.assertEqual(call("post", base + "/action:retrieve", record)[1], [])
        record["kind"] = "invalid"
        self.assertEqual(call("post", base + "/action:retrieve", record)[0], 400)

    def test_transformation_kind_selectors_include_local_horizontal_operations(self):
        endpoint = "/api/crs/catalog/v3/coordinate-transformation"
        baseline = call("post", endpoint, {})[1]["searchResults"]
        self.assertGreater(baseline["totalCount"], 0)
        for selector in ("All", "ExcludeVertical"):
            with self.subTest(selector=selector):
                status, result = call("post", endpoint, {"kind": selector})
                self.assertEqual(status, 200)
                self.assertEqual(result["searchResults"], baseline)

    def test_area_checks_and_geojson_geometry_collections(self):
        records = call("post", "/api/crs/catalog/v3/coordinate-reference-system", {})[1]["searchResults"]["results"]
        refs = {r["data"]["Code"]: r for r in records}
        base = "/api/crs/converter/v3/convertGeoJson"
        aou = call("post", "/api/crs/catalog/v3/points-in-aou",
                   {"recordId": refs["32638"]["id"] + ":",
                    "points": [{"longitude": 45, "latitude": 10}, {"longitude": 55, "latitude": 10}]})[1]
        self.assertEqual(aou["bboxFailedPoints"][0]["index"], 1)
        self.assertGreater(int(aou["maxDistKmOutsideBBox"]), 0)
        geometries = [
            {"type": "MultiPoint", "coordinates": [[10, 20], [11, 21]]},
            {"type": "LineString", "coordinates": [[10, 20, 1], [11, 21, 2]]},
            {"type": "MultiLineString", "coordinates": [[[10, 20], [11, 21]]]},
            {"type": "Polygon", "coordinates": [[[10, 20], [11, 20], [11, 21], [10, 20]]]},
            {"type": "MultiPolygon", "coordinates": [[[[10, 20], [11, 20], [11, 21], [10, 20]]]]},
        ]
        fc = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {"id": "kept"},
              "geometry": {"type": "GeometryCollection", "geometries": geometries}}]}
        status, result = call("post", base, {"toCRS": refs["3857"]["id"] + ":", "featureCollection": fc})
        self.assertEqual(status, 200)
        self.assertEqual(result["successCount"], "14")
        first = result["featureCollection"]["features"][0]["geometry"]["geometries"][0]["coordinates"][0]
        self.assertAlmostEqual(first[0], 1113194.9079327357)
        self.assertEqual(fc["features"][0]["geometry"]["geometries"][0]["coordinates"][0], [10, 20])
        bad_fc = {"type": "FeatureCollection", "features": [{"type": "Feature",
                  "geometry": {"type": "Point", "coordinates": [10, 89]}}]}
        self.assertEqual(call("post", base, {"toCRS": refs["3857"]["id"], "featureCollection": bad_fc})[0], 422)
        bad_fc["features"][0]["geometry"]["coordinates"] = [True, 20]
        self.assertEqual(call("post", base, {"toCRS": refs["3857"]["id"], "featureCollection": bad_fc})[0], 400)
        bad_fc["features"][0]["geometry"] = {"type": [], "coordinates": [10, 20]}
        self.assertEqual(call("post", base, {"toCRS": refs["3857"]["id"], "featureCollection": bad_fc})[0], 400)
        bad_fc["features"][0]["geometry"] = {"type": "MultiPoint", "coordinates": [[10, 20]] * 1001}
        self.assertEqual(call("post", base, {"toCRS": refs["3857"]["id"], "featureCollection": bad_fc})[0], 400)

    def test_trajectory_computed_deviations_and_domain_errors(self):
        crss = call("post", "/api/crs/catalog/v3/coordinate-reference-system", {})[1]["searchResults"]["results"]
        refs = {r["data"]["Code"]: r["data"]["PersistableReference"] for r in crss}
        unit = call("get", "/api/unit/v3/unit/symbol?namespaces=osdu-lite&symbol=m")[1]["essenceJson"]
        payload = {"trajectoryCRS": refs["4326"], "unitXY": unit, "unitZ": unit,
                   "referencePoint": {"x": "10", "y": "20", "z": "0"}, "method": "AzimuthalEquidistant",
                   "azimuthReference": "TRUE_NORTH", "inputStations": json.dumps([
                       {"md": 0, "inclination": 90, "azimuth": 90},
                       {"md": 100, "inclination": 90, "azimuth": 90}])}
        endpoint = "/api/crs/converter/v3/convertTrajectory"
        status, result = call("post", endpoint, payload)
        self.assertEqual(status, 200)
        last = result["stations"][-1]
        self.assertAlmostEqual(float(last["dxTN"]), 100)
        self.assertAlmostEqual(float(last["dyTN"]), 0)
        self.assertAlmostEqual(last["dz"], 0)
        self.assertGreater(float(last["wgs84Longitude"]), 10)
        self.assertIn("mock", " ".join(result["operationsApplied"]).lower())
        payload["referencePoint"]["z"] = "100"
        payload["inputStations"] = json.dumps([
            {"md": 0, "inclination": 0, "azimuth": 0},
            {"md": 100, "inclination": 0, "azimuth": 0}])
        status, vertical = call("post", endpoint, payload)
        self.assertEqual(status, 200)
        self.assertAlmostEqual(vertical["stations"][-1]["dz"], 100)
        self.assertAlmostEqual(json.loads(vertical["stations"][-1]["point"])["z"], 0)
        self.assertAlmostEqual(json.loads(vertical["stations"][0]["point"])["z"], 100)
        payload["method"] = "LMP"
        self.assertEqual(call("post", endpoint, payload)[0], 400)
        payload["method"] = "AzimuthalEquidistant"
        payload["trajectoryCRS"] = refs["3857"]
        payload["referencePoint"]["x"] = "1e308"
        self.assertEqual(call("post", endpoint, payload)[0], 400)

    def test_replay_batch_acl_history_pagination_and_reset(self):
        record = valid_well()
        server.upsert_records([record], index_immediately=True)
        server.upsert_records([record], index_immediately=True)
        base = "/api/storage/v2"
        self.assertEqual(call("delete", base + "/records/" + record["id"] + "/versions?versionIds=2")[0], 400)
        self.assertEqual(call("delete", base + "/records/" + record["id"] + "/versions?limit=1")[0], 204)
        self.assertEqual(len(server.RECORD_VERSIONS[record["id"]]), 1)
        headers = call("post", base + "/query/records/headers",
                       {"records": [record["id"], "missing"], "attributes": ["kind"]})[1]
        self.assertEqual(set(headers["records"][0]), {"id", "kind"})
        self.assertEqual(headers["notFound"], ["missing"])
        with patch.dict(server.GROUPS_BY_TOKEN, {server.TOKEN: {"osdu-lite.storage.user"}}):
            response = call("post", base + "/query/records:batch", {"records": [record["id"]]})[1]
            self.assertEqual(response["notFound"], [record["id"]])
        self.assertEqual(call("post", base + "/replay", {"operation": "other"})[0], 400)
        self.assertEqual(call("post", base + "/replay", {"operation": []})[0], 400)
        self.assertEqual(call("post", base + "/replay", {"operation": "replay", "filter": {}})[0], 400)
        page = call("get", "/api/entitlements/v2/groups/all?type=DATA&limit=1")[1]
        self.assertEqual(len(page["groups"]), 1)
        self.assertEqual(page["cursor"], "1")
        page2 = call("get", "/api/entitlements/v2/groups/all?type=data&limit=1&cursor=1")[1]
        self.assertNotEqual(page["groups"], page2["groups"])

    def test_http_strict_framing_headers_and_aliases(self):
        health = make_handler("/api/secret/v1/health", _Headers({
            "Authorization": "Bearer demo-token", "data-partition-id": server.PARTITION}), "")
        health.do_GET()
        self.assertEqual(health._status, 200)
        self.assertEqual(health.wfile.buf.getvalue(), b"UP")
        self.assertIn(("Content-Type", "text/plain; charset=utf-8"), health._response_headers)
        for headers, body in [({"Content-Length": "-1"}, ""), ({"Content-Length": "abc"}, ""),
                              ({"Content-Length": "4"}, "{}"), ({"Transfer-Encoding": "chunked"}, "")]:
            handler = make_handler("/api/secret/v2/secrets", _Headers({
                "Authorization": "Bearer demo-token", "data-partition-id": server.PARTITION, **headers}), body)
            handler.do_POST()
            self.assertEqual(handler._status, 400)
        handler = make_handler("/api/partition/v1/partitions/demo", _Headers({
            "Authorization": "Bearer demo-token", "data-partition-id": server.PARTITION,
            "correlation-id": "test-correlation"}), "")
        handler.do_DELETE()
        self.assertEqual(handler._status, 404)
        self.assertIn(("correlation-id", "test-correlation"), handler._response_headers)
        self.assertIn(("Access-Control-Allow-Origin", "*"), handler._response_headers)

    def test_pinned_inventory_success(self):
        covered = set()

        def check(service, operation, body=None, status=200, fields=(), path=None, query=None):
            method, suffix = operation.split(" ", 1)
            url = INVENTORY["services"][service]["base"] + (path or suffix)
            if query:
                url += "?" + urlencode(query)
            actual, result = call(method.lower(), url, body)
            self.assertEqual(actual, status, (operation, result))
            for field in fields:
                self.assertIn(field, result, operation)
            covered.add((service, operation))
            return result

        # Health/info are registered operations, not a prefix-based catchall.
        for service, spec in INVENTORY["services"].items():
            for operation in spec["operations"]:
                if operation.endswith("/info"):
                    info = check(service, operation, fields=("version", "buildTime", "artifactId"))
                    self.assertEqual(info["version"], spec["version"])
                elif any(operation.endswith(p) for p in
                         ("/readiness_check", "/liveness_check", "/_ah/warmup", "/v1/health")):
                    check(service, operation)

        # Partition writes are public source-defined, hidden from live OpenAPI.
        prop = {"properties": {"region": {"sensitive": False, "value": "local"}}}
        check("partition", "POST /partitions/{partitionId}", prop, 201, path="/partitions/demo")
        self.assertIn("demo", check("partition", "GET /partitions"))
        p = check("partition", "GET /partitions/{partitionId}", fields=("region",), path="/partitions/demo")
        self.assertEqual(p["region"]["value"], "local")
        prop["properties"]["region"]["value"] = "updated"
        check("partition", "PATCH /partitions/{partitionId}", prop, 204, path="/partitions/demo")
        self.assertEqual(call("get", "/api/partition/v1/partitions/demo")[1]["region"]["value"], "updated")
        check("partition", "DELETE /partitions/{partitionId}", status=204, path="/partitions/demo")
        self.assertEqual(call("get", "/api/partition/v1/partitions/demo")[0], 404)

        # Indexer has observable Search effects, including delete and replay.
        record = valid_well()
        server.upsert_records([record], index_immediately=True)
        check("indexer", "PUT /partitions/provision")
        check("indexer", "DELETE /index", query={"kind": server.WELL_KIND})
        self.assertEqual(call("post", "/api/search/v2/query", {"kind": server.WELL_KIND})[1]["totalCount"], 0)
        check("indexer", "POST /reindex", {"kind": server.WELL_KIND})
        self.assertEqual(call("post", "/api/search/v2/query", {"kind": server.WELL_KIND})[1]["totalCount"], 1)
        check("indexer", "PATCH /reindex")
        check("indexer", "POST /reindex/records", {"recordIds": [record["id"]]}, 202)

        # Register uses URLs as inert data; no callbacks or template execution.
        sub = check("register", "POST /subscription",
                    {"name": "Demo subscription", "topic": "records-changed",
                     "pushEndpoint": "https://127.0.0.1/never-called"}, 201, ("id", "notificationId"))
        sid = sub["id"]
        check("register", "GET /subscription", query={"notificationId": sub["notificationId"]})
        self.assertEqual(len(check("register", "GET /subscriptions")), 1)
        self.assertEqual(check("register", "GET /subscription/{id}", path="/subscription/" + sid)["name"],
                         "Demo subscription")
        check("register", "PUT /subscription/{id}/secret",
              {"secretType": "HMAC", "value": "mock-only"}, 204, path="/subscription/" + sid + "/secret")
        self.assertNotIn("secret", call("get", "/api/register/v1/subscription/" + sid)[1])
        topics = check("register", "GET /topics")
        self.assertIn("name", topics[0])
        message = {"message": {"data": base64.b64encode(json.dumps([{"id": record["id"]}]).encode()).decode(),
                               "attributes": {"data-partition-id": server.PARTITION}},
                   "subscription": "projects/local/subscriptions/" + sub["notificationId"]}
        self.assertEqual(check("notification", "POST /push-handlers/records-changed", message),
                         "message acknowledged by client")
        import community_services
        self.assertEqual(len(community_services.EVENTS), 1)
        self.assertEqual(community_services.EVENTS[0]["subscriptionIds"], [sid])
        ddms = {"id": "demo-ddms", "name": "Demo DDMS",
                "interfaces": [{"entityType": "well", "schema": {"url": "https://127.0.0.1/no-fetch"}}]}
        check("register", "POST /ddms", ddms, 201, ("id", "interfaces"))
        self.assertEqual(len(check("register", "GET /ddms", query={"type": "well"})), 1)
        check("register", "GET /ddms/{id}", fields=("name",), path="/ddms/demo-ddms")
        action = {"id": "demo-action", "name": "Demo action", "filter": {"entityType": ["master-data--Well"]},
                  "url": "https://example.invalid/static"}
        check("register", "POST /action", action, 201, ("id", "filter"))
        check("register", "GET /action/{id}", fields=("name",), path="/action/demo-action")
        parsed = check("register", "POST /action:test", {"action": action, "testPayload": record},
                       fields=("url", "errors"))
        self.assertEqual(parsed["url"], action["url"])
        self.assertEqual(len(check("register", "POST /action:retrieve", record)), 1)
        check("register", "DELETE /action/{id}", status=204, path="/action/demo-action")
        check("register", "DELETE /ddms/{id}", status=204, path="/ddms/demo-ddms")
        check("register", "DELETE /subscription/{id}", status=204, path="/subscription/" + sid)

        # No list response exposes values. Disabled secrets are not readable.
        secret = {"id": "mock-secret", "value": "not-a-real-credential",
                  "secretAcls": copy.deepcopy(server.DEFAULT_ACL)}
        check("secret", "POST /v2/secrets", secret, fields=("id", "createdAt", "enabled"))
        value = check("secret", "GET /v2/secrets/{secretId}", fields=("value",), path="/v2/secrets/mock-secret")
        self.assertEqual(value["value"], secret["value"])
        self.assertEqual(check("secret", "GET /v2/secrets/list"), ["mock-secret"])
        batch = check("secret", "POST /v2/secrets/list", ["mock-secret", "missing"],
                      fields=("secrets", "notAvailableSecrets"))
        self.assertNotIn("value", batch["secrets"][0])
        self.assertEqual(batch["notAvailableSecrets"], ["missing"])
        secret["value"] = "changed"
        check("secret", "PUT /v2/secrets/{secretId}", secret, fields=("id",), path="/v2/secrets/mock-secret")
        check("secret", "PATCH /v2/secrets/{secretId}/disable", status=202, fields=("enabled",),
              path="/v2/secrets/mock-secret/disable")
        self.assertEqual(call("get", "/api/secret/v2/secrets/mock-secret")[0], 409)
        check("secret", "PATCH /v2/secrets/{secretId}/enable", status=202, fields=("enabled",),
              path="/v2/secrets/mock-secret/enable")
        check("secret", "DELETE /v2/secrets/{secretId}", status=204, path="/v2/secrets/mock-secret")

        # Reference endpoints retain old envelopes but extend verified public paths.
        units = check("unit", "GET /unit", fields=("units", "totalCount"))["units"]
        metre = next(u for u in units if u["essence"]["symbol"] == "m")
        ur = {"essence": metre["essence"]}
        mr = {"essence": {"type": "UM", "ancestry": "Length"}}
        check("unit", "POST /unit", ur, fields=("essence",))
        check("unit", "GET /unit/symbol", query={"namespaces": "osdu-lite", "symbol": "m"}, fields=("essence",))
        check("unit", "GET /unit/symbols", query={"symbol": "m"}, fields=("units",))
        check("unit", "GET /measurement/list", fields=("measurements",))
        check("unit", "GET /measurement", query={"ancestry": "Length"}, fields=("essence",))
        check("unit", "POST /measurement", mr, fields=("essence",))
        for suffix in ["/unit/measurement", "/unit/measurement/preferred"]:
            check("unit", "GET " + suffix, query={"ancestry": "Length"}, fields=("units",))
            check("unit", "POST " + suffix, mr, fields=("units",))
        for suffix, key in [("/unit/search", "units"), ("/measurement/search", "measurements"),
                            ("/catalog/search", "units")]:
            result = check("unit", "POST " + suffix, {"query": "*"}, fields=(key, "totalCount"))
            self.assertGreater(result["totalCount"], 0)
        info = check("unit", "GET /unitsystem/list", fields=("unitSystemInfoList",))["unitSystemInfoList"][0]
        check("unit", "GET /unitsystem", query={"name": info["name"]}, fields=("unitAssignments",))
        check("unit", "POST /unitsystem", {"persistableReference": info["persistableReference"]},
              fields=("unitAssignments",))
        check("unit", "GET /unit/unitsystem", query={"unitSystemName": info["name"], "ancestry": "Length"},
              fields=("essence",))
        check("unit", "POST /unit/unitsystem", mr, query={"unitSystemName": info["name"]}, fields=("essence",))
        for suffix, key in [("/unit/maps", "unitMapItems"), ("/measurement/maps", "measurementMapItems"),
                            ("/catalog/mapstates", "mapStates")]:
            check("unit", "GET " + suffix, fields=(key, "totalCount"))
        check("unit", "GET /catalog", fields=("units", "measurements", "totalUnitCount"))
        check("unit", "GET /catalog/lastmodified", fields=("lastModified",))
        foot = next(u for u in units if u["essence"]["symbol"] == "ft")
        conversion = {"fromUnit": foot["essence"], "toUnit": metre["essence"]}
        for suffix, key in [("/conversion/scale", "scaleOffset"), ("/conversion/abcd", "abcd")]:
            check("unit", "GET " + suffix, query={"namespaces": "osdu-lite", "fromSymbol": "ft", "toSymbol": "m"},
                  fields=(key,))
            result = check("unit", "POST " + suffix, conversion, fields=(key,))
            self.assertAlmostEqual(result[key]["scale" if key == "scaleOffset" else "b"], 0.3048)

        crss = check("crs-catalog", "POST /coordinate-reference-system", {},
                     fields=("searchResults",))["searchResults"]["results"]
        crs = next(c for c in crss if c["data"]["Code"] == "4326")
        check("crs-catalog", "GET /coordinate-reference-system", query={"recordId": crs["id"] + ":"},
              fields=("searchResults",))
        transforms = check("crs-catalog", "POST /coordinate-transformation", {},
                           fields=("searchResults",))["searchResults"]["results"]
        self.assertGreater(len(transforms), 0)
        check("crs-catalog", "GET /coordinate-transformation", query={"recordId": transforms[0]["id"]},
              fields=("searchResults",))
        aou = check("crs-catalog", "POST /points-in-aou",
                    {"recordId": crs["id"], "points": [{"longitude": 10, "latitude": 20}]},
                    fields=("bboxFailedPoints", "maxDistKmOutsideBBox"))
        self.assertEqual(aou["bboxFailedPoints"], [])
        target = next(c for c in crss if c["data"]["Code"] == "3857")["data"]["PersistableReference"]
        source = crs["data"]["PersistableReference"]
        cp = check("crs-converter", "POST /convert",
                   {"fromCRS": source, "toCRS": target, "points": [{"x": "10", "y": "20", "z": "5"}]},
                   fields=("points", "successCount"))
        self.assertAlmostEqual(float(cp["points"][0]["x"]), 1113194.9079327357)
        fc = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {"name": "kept"},
              "geometry": {"type": "GeometryCollection", "geometries": [
                  {"type": "Point", "coordinates": [10, 20, 5]}]}}]}
        geo = check("crs-converter", "POST /convertGeoJson", {"toCRS": target, "featureCollection": fc},
                    fields=("featureCollection", "totalCount", "successCount"))
        self.assertEqual(geo["featureCollection"]["features"][0]["properties"], {"name": "kept"})
        self.assertEqual(geo["successCount"], "1")
        trajectory = check("crs-converter", "POST /convertTrajectory",
                           {"trajectoryCRS": source, "unitXY": metre["essenceJson"], "unitZ": metre["essenceJson"],
                            "referencePoint": {"x": "10", "y": "20", "z": "0"},
                            "method": "AzimuthalEquidistant", "azimuthReference": "TRUE_NORTH",
                            "inputStations": json.dumps([{"md": 0, "inclination": 0, "azimuth": 0},
                                                        {"md": 100, "inclination": 0, "azimuth": 0}])},
                           fields=("stations", "localCRS", "unitDls", "operationsApplied"))
        self.assertEqual(len(trajectory["stations"]), 2)
        self.assertAlmostEqual(trajectory["stations"][-1]["dz"], 100)
        self.assertIn("mock", " ".join(trajectory["operationsApplied"]).lower())

        # Existing services: verified missing operations, without changing old ones.
        dataset = {**copy.deepcopy(record), "id": server.PARTITION + ":dataset--File.Generic:test-community",
                   "kind": server.DATASET_KIND}
        server.upsert_records([dataset], index_immediately=True)
        check("dataset", "GET /getDatasetRegistry", query={"id": dataset["id"]}, fields=("datasetRegistries",))
        server.soft_delete_records([dataset["id"]])
        restored = check("dataset", "POST /metadataRecord/{id}/undelete", fields=("datasetRegistries",),
                         path="/metadataRecord/" + dataset["id"] + "/undelete")
        self.assertEqual(restored["datasetRegistries"][0]["id"], dataset["id"])
        self.assertNotIn(dataset["id"], server.DELETED)
        self.assertEqual(call("delete", "/api/storage/v2/records/" + dataset["id"] + "?hardDelete=true")[0], 204)
        check("entitlements", "GET /groups/all", query={"type": "NONE"}, fields=("groups", "totalCount"))
        check("entitlements", "DELETE /members/{member_email}", status=204, path="/members/demo@example.com")
        check("file", "POST /files/revokeURL", {}, 204)
        self.assertEqual(check("legal", "GET /jobs/updateLegalTagStatus"), "200 OK")
        batch = check("storage", "POST /query/records:batch", {"records": [record["id"]]},
                      fields=("records", "notFound"))
        self.assertEqual(json.loads(batch["records"][0])["id"], record["id"])
        check("storage", "POST /query/records/headers", {"records": [record["id"]]},
              fields=("records", "notFound"))
        replay = check("storage", "POST /replay", {"operation": "reindex"}, status=202, fields=("replayId",))
        status = check("storage", "GET /replay/status/{id}", path="/replay/status/" + replay["replayId"],
                       fields=("overallState", "processedRecords"))
        self.assertEqual(status["processedRecords"], 1)
        server.upsert_records([record], index_immediately=True)
        check("storage", "DELETE /records/{id}/versions", status=204, path="/records/" + record["id"] + "/versions",
              query={"versionIds": "1"})
        check("storage", "POST /records/{id}:delete", status=204, path="/records/" + record["id"] + ":delete")
        self.assertIn(record["id"], server.DELETED)
        check("workflow", "POST /v1/workflow/system", {"workflowName": "demo-system",
              "description": "local mock", "registrationInstructions": {"dagName": {"value": "no-execution"}}},
              status=200, fields=("workflowName",))
        run = call("post", "/api/workflow/v1/workflow/Osdu_ingest/workflowRun", {"executionContext": {}})[1]
        run_id = run.get("workflowRunId") or run["runId"]
        check("workflow", "GET /v1/workflow/{workflow_name}/workflowRun/{runId}/latestInfo",
              path="/v1/workflow/Osdu_ingest/workflowRun/" + run_id + "/latestInfo", fields=("runId", "status"))
        check("workflow", "DELETE /v1/workflow/system/{workflow_name}", status=204,
              path="/v1/workflow/system/demo-system")

        expected = {(service, operation) for service, spec in INVENTORY["services"].items()
                    for operation in spec["operations"]}
        self.assertEqual(covered, expected, "Every pinned operation needs success/envelope coverage")

    def test_new_route_auth_limits_errors_and_reset(self):
        routes = ["/api/partition/v1/partitions", "/api/register/v1/topics",
                  "/api/secret/v2/secrets/list", "/api/indexer/v2/info", "/api/notification/v1/info",
                  "/api/unit/v3/catalog", "/api/crs/converter/v3/info"]
        for route in routes:
            with self.subTest(route=route):
                self.assertEqual(call("get", route, token="wrong")[0], 401)
                self.assertEqual(call("get", route, partition="wrong")[0], 400)
                with patch.dict(server.GROUPS_BY_TOKEN, {server.TOKEN: set()}):
                    self.assertEqual(call("get", route)[0], 403)
                with patch.object(server, "REQUIRE_AUTH", False):
                    self.assertEqual(call("get", route, token="wrong", partition="wrong")[0], 200)
        for route in ["/api/register/v1/subscription/missing", "/api/secret/v2/secrets/missing",
                      "/api/partition/v1/partitions/missing", "/api/storage/v2/replay/status/missing"]:
            self.assertEqual(call("get", route)[0], 404)
        for method, route, payload in [
                ("post", "/api/register/v1/subscription", []),
                ("post", "/api/register/v1/ddms", {"id": "xx"}),
                ("post", "/api/secret/v2/secrets", {"id": "xx", "value": 4}),
                ("post", "/api/indexer/v2/reindex/records", {"recordIds": []}),
                ("post", "/api/unit/v3/measurement", {"essence": {"ancestry": "Unknown"}}),
                ("post", "/api/notification/v1/push-handlers/records-changed", {"message": {"data": "!invalid!"}}),
                ("post", "/api/crs/converter/v3/convertGeoJson", {})]:
            with self.subTest(route=route):
                self.assertIn(call(method, route, payload)[0], (400, 404))
        for raw in ['{"id":"xx","value":NaN}', '{"id":"xx","id":"yy"}', "null", "["]:
            self.assertEqual(call("post", "/api/secret/v2/secrets", raw, raw=True)[0], 400)
        handler = make_handler("/api/secret/v2/secrets", _Headers({
            "Authorization": "Bearer demo-token", "data-partition-id": server.PARTITION,
            "Content-Length": "1048577"}), "")
        handler.do_POST()
        self.assertEqual(handler._status, 413)
        self.assertEqual(call("get", "/api/register/v1/not-registered")[0], 404)
        self.assertEqual(call("get", "/api/secret/v2/secrets/list/enable")[0], 404)
        call("post", "/api/partition/v1/partitions/new", {"properties": {"x": {"value": 1, "sensitive": False}}})
        server.clear_state()
        self.assertEqual(call("get", "/api/partition/v1/partitions/new")[0], 404)

    def test_abcd_temperature_and_dimension_validation(self):
        q = {"namespaces": "osdu-lite", "fromSymbol": "degC", "toSymbol": "degF"}
        status, result = call("get", "/api/unit/v3/conversion/abcd?" + urlencode(q))
        self.assertEqual(status, 200)
        p = result["abcd"]
        self.assertAlmostEqual((p["a"] + p["b"] * 100) / (p["c"] + p["d"] * 100), 212)
        q["toSymbol"] = "m"
        self.assertEqual(call("get", "/api/unit/v3/conversion/abcd?" + urlencode(q))[0], 400)


if __name__ == "__main__":
    unittest.main()
