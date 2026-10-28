#!/usr/bin/env python3
"""HTTP-only community mock smoke. Python stdlib; run against a local test server."""
import base64
import json
import math
import os
import uuid
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE = os.environ.get("OSDU_BASE_URL", "http://localhost:8089").rstrip("/")
PARTITION = os.environ.get("OSDU_PARTITION", "company-prod")
HEADERS = {"Authorization": "Bearer " + os.environ.get("OSDU_TOKEN", "demo-token"),
           "data-partition-id": PARTITION, "Content-Type": "application/json",
           "correlation-id": "community-smoke"}
MANIFEST = json.loads((Path(__file__).parent / "data/community_api.json").read_text())


def request(method, path, body=None, expected=200, headers=None):
    data = None if body is None else body if isinstance(body, bytes) else json.dumps(body, allow_nan=False).encode()
    req = Request(BASE + path, data=data, method=method, headers=HEADERS if headers is None else headers)
    try:
        response = urlopen(req, timeout=15)
    except HTTPError as error:
        response = error
    with response:
        raw = response.read()
        payload = json.loads(raw) if raw else None
        assert response.status == expected, (method, path, response.status, payload)
        if path.startswith("/api/") and headers is None:
            assert response.headers["correlation-id"] == "community-smoke", path
            assert response.headers["Access-Control-Allow-Origin"] == "*", path
        return payload


def main():
    before = request("GET", "/health")["storedRecords"]
    for service, spec in MANIFEST["services"].items():
        operation = next(op for op in spec["operations"] if op.endswith("/info"))
        info = request("GET", spec["base"] + operation.split(" ", 1)[1])
        assert info["version"] == spec["version"] and info["artifactId"].endswith(service), info
    if os.environ.get("REQUIRE_AUTH", "true").lower() not in {"0", "false", "no"}:
        request("GET", "/api/secret/v2/secrets/list", expected=401,
                headers={"data-partition-id": PARTITION})
    request("POST", "/api/secret/v2/secrets", b'{"value":NaN}', expected=400)
    request("GET", "/api/register/v1/not-a-route", expected=404)
    key = "smoke-" + uuid.uuid4().hex[:12]
    acl = {"owners": ["data.well360.owners@" + PARTITION + ".company.com"],
           "viewers": ["data.well360.viewers@" + PARTITION + ".company.com"]}
    cleanup = []
    try:
        part = "/api/partition/v1/partitions/" + key
        properties = {"properties": {"region": {"value": "offline", "sensitive": False}}}
        request("POST", part, properties, 201)
        cleanup.append(part)
        assert request("GET", part)["region"]["value"] == "offline"
        properties["properties"]["region"]["value"] = "updated"
        request("PATCH", part, properties, 204)
        assert request("GET", part)["region"]["value"] == "updated"

        secret = {"id": key, "value": "mock-only-not-a-real-credential", "secretAcls": acl}
        secret_url = "/api/secret/v2/secrets/" + key
        assert request("POST", "/api/secret/v2/secrets", secret)["enabled"]
        cleanup.append(secret_url)
        assert request("GET", secret_url)["value"] == secret["value"]
        secret["value"] = "mock-updated"
        request("PUT", secret_url, secret)
        assert request("GET", secret_url)["value"] == "mock-updated"
        batch = request("POST", "/api/secret/v2/secrets/list", [key, "not-present"])
        assert "value" not in batch["secrets"][0] and batch["notAvailableSecrets"] == ["not-present"]
        assert key in request("GET", "/api/secret/v2/secrets/list")
        request("PATCH", secret_url + "/disable", expected=202)
        request("GET", secret_url, expected=409)
        request("PATCH", secret_url + "/enable", expected=202)

        sub = request("POST", "/api/register/v1/subscription", {
            "id": key, "name": "Smoke subscription", "topic": "records-changed",
            "pushEndpoint": "https://example.invalid/never-called"}, 201)
        subscription_url = "/api/register/v1/subscription/" + sub["id"]
        cleanup.append(subscription_url)
        request("PUT", subscription_url + "/secret", {"secretType": "HMAC", "value": "mock-only"}, 204)
        assert "secret" not in request("GET", subscription_url)
        assert request("GET", "/api/register/v1/subscription?" +
                       urlencode({"notificationId": sub["notificationId"]}))[0]["id"] == key
        request("POST", "/api/notification/v1/push-handlers/records-changed", {
            "subscription": "projects/local/subscriptions/" + sub["notificationId"],
            "message": {"attributes": {"data-partition-id": PARTITION},
                        "data": base64.b64encode(b'[{"id":"local","op":"update"}]').decode()}})
        ddms_url = "/api/register/v1/ddms/" + key
        request("POST", "/api/register/v1/ddms", {"id": key, "name": "Smoke DDMS", "interfaces": [
            {"entityType": "smoke", "schema": {"url": "https://example.invalid/inert"}}]}, 201)
        cleanup.append(ddms_url)
        assert request("GET", ddms_url)["interfaces"][0]["entityType"] == "smoke"
        action_url = "/api/register/v1/action/" + key
        action = {"id": key, "name": "Smoke action", "filter": {"entityType": ["master-data--Well"]},
                  "url": "https://example.invalid/inert"}
        request("POST", "/api/register/v1/action", action, 201)
        cleanup.append(action_url)
        assert request("POST", "/api/register/v1/action:test", {
            "action": action, "testPayload": {"kind": "osdu:wks:master-data--Well:1.4.0"}})["url"] == action["url"]

        record_id = PARTITION + ":master-data--Well:" + key
        record_url = "/api/storage/v2/records/" + record_id
        record = {"id": record_id, "kind": "osdu:wks:master-data--Well:1.4.0", "acl": acl,
                  "legal": {"legaltags": [PARTITION + "-demo-legaltag"], "otherRelevantDataCountries": ["SA"]},
                  "data": {"FacilityName": key}}
        request("PUT", "/api/storage/v2/records", [record])
        cleanup.append(record_url + "?hardDelete=true")
        request("POST", "/api/indexer/v2/reindex/records", {"recordIds": [record_id]}, 202)
        result = request("POST", "/api/search/v2/query", {"kind": record["kind"], "query": 'id:"' + record_id + '"'})
        assert result["totalCount"] == 1, result
        batch = request("POST", "/api/storage/v2/query/records:batch", {"records": [record_id]})
        assert json.loads(batch["records"][0])["id"] == record_id
        request("POST", record_url + ":delete", expected=204)
        dataset_id = PARTITION + ":dataset--File.Generic:" + key
        dataset = {**record, "id": dataset_id, "kind": "osdu:wks:dataset--File.Generic:1.0.0"}
        request("PUT", "/api/dataset/v1/registerDataset", {"datasetRegistries": [dataset]})
        cleanup.append("/api/storage/v2/records/" + dataset_id + "?hardDelete=true")
        request("POST", "/api/dataset/v1/metadataRecord/" + dataset_id + "/softDelete", expected=204)
        assert request("POST", "/api/dataset/v1/metadataRecord/" + dataset_id + "/undelete")[
            "datasetRegistries"][0]["id"] == dataset_id
        assert request("GET", "/api/dataset/v1/getDatasetRegistry?" +
                       urlencode({"id": dataset_id}))["datasetRegistries"][0]["id"] == dataset_id
        replay = request("POST", "/api/storage/v2/replay", {"operation": "reindex",
                         "filter": {"kinds": [record["kind"]]}}, 202)
        status = request("GET", "/api/storage/v2/replay/status/" + replay["replayId"])
        count = request("POST", "/api/search/v2/query", {"kind": record["kind"]})["totalCount"]
        assert status["overallState"] == "COMPLETED" and status["processedRecords"] == count, status

        units = request("GET", "/api/unit/v3/catalog")
        assert units["totalUnitCount"] == 17 and units["totalMeasurementCount"] == 6
        measurement = request("POST", "/api/unit/v3/catalog/search", {"query": "Length"})
        assert measurement["totalCount"] == 1 and measurement["measurements"][0]["name"] == "Length"
        conversion = request("GET", "/api/unit/v3/conversion/abcd?" + urlencode({
            "namespaces": "osdu-lite", "fromSymbol": "degC", "toSymbol": "degF"}))["abcd"]
        assert math.isclose((conversion["a"] + conversion["b"] * 100) /
                            (conversion["c"] + conversion["d"] * 100), 212, abs_tol=1e-10)
        refs = {r["data"]["Code"]: r for r in request("POST",
                "/api/crs/catalog/v3/coordinate-reference-system", {})["searchResults"]["results"]}
        transformations = request("POST", "/api/crs/catalog/v3/coordinate-transformation",
                                  {"kind": "ExcludeVertical"})
        assert transformations["searchResults"]["totalCount"] > 0
        fc = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {"kept": True},
              "geometry": {"type": "Point", "coordinates": [10, 20, 5]}}]}
        geo = request("POST", "/api/crs/converter/v3/convertGeoJson", {
            "toCRS": refs["3857"]["id"] + ":", "featureCollection": fc})
        assert geo["successCount"] == "1"
        point = geo["featureCollection"]["features"][0]["geometry"]["coordinates"]
        assert math.isclose(point[0], 1113194.9079327357, abs_tol=1e-6) and point[2] == 5
        metre = next(unit for unit in units["units"] if unit["displaySymbol"] == "m")["essenceJson"]
        trajectory = request("POST", "/api/crs/converter/v3/convertTrajectory", {
            "trajectoryCRS": refs["4326"]["data"]["PersistableReference"], "unitXY": metre, "unitZ": metre,
            "referencePoint": {"x": "10", "y": "20", "z": "100"}, "method": "AzimuthalEquidistant",
            "azimuthReference": "TRUE_NORTH", "inputStations": json.dumps([
                {"md": 0, "inclination": 0, "azimuth": 0},
                {"md": 100, "inclination": 0, "azimuth": 0}])})
        assert math.isclose(trajectory["stations"][-1]["dz"], 100)
        assert math.isclose(json.loads(trajectory["stations"][-1]["point"])["z"], 0, abs_tol=1e-8)
    finally:
        for path in reversed(cleanup):
            request("DELETE", path, expected=204)
    assert request("GET", "/health")["storedRecords"] == before, "Smoke must not inflate seeded record count"
    print("PASS: 16 service infos; stateful mock lifecycles; indexing/Search, replay; offline GeoJSON and ABCD;"
          " auth, malformed input, CORS/correlation; record count preserved")


if __name__ == "__main__":
    main()
