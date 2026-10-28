#!/usr/bin/env bash
set -euo pipefail

# HTTP-only probe: no local pyproj or repository imports required.
python3 - <<'PY'
import json
import math
import os
import urllib.error
import urllib.parse
import urllib.request

base = os.environ.get("OSDU_BASE_URL", "http://localhost:8089").rstrip("/")
headers = {"Authorization": "Bearer " + os.environ.get("OSDU_TOKEN", "demo-token"),
           "data-partition-id": os.environ.get("OSDU_PARTITION", "company-prod"),
           "Content-Type": "application/json", "correlation-id": "reference-smoke"}

def request(method, path, body=None, expected=200):
    data = None if body is None else json.dumps(body, allow_nan=False).encode()
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    try:
        response = urllib.request.urlopen(req, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        payload = json.load(response)
        assert response.status == expected, (path, response.status, payload)
        assert response.headers["correlation-id"] == "reference-smoke"
        assert response.headers["Access-Control-Allow-Origin"] == "*"
        return payload

catalog = "/api/crs/catalog/v3/coordinate-reference-system"
records = request("POST", catalog, {})["searchResults"]["results"]
assert len(records) == 4, records
refs = {r["data"]["Code"]: r["data"]["PersistableReference"] for r in records}
record = next(r for r in records if r["data"]["Code"] == "4326")
detail = request("GET", catalog + "?" + urllib.parse.urlencode({"recordId": record["id"] + ":"}))
assert detail["searchResults"]["totalCount"] == 1
result = request("POST", "/api/crs/converter/v3/convert", {
    "fromCRS": refs["4326"], "toCRS": refs["3857"],
    "points": [{"x": 10, "y": 20, "z": 123.45}, {"x": 10, "y": 86, "z": 0}]})
assert result["successCount"] == "1" and len(result["points"]) == 2, result
point = result["points"][0]
assert math.isclose(float(point["x"]), 1113194.9079327357, rel_tol=0, abs_tol=1e-6)
assert math.isclose(float(point["y"]), 2273030.9269876895, rel_tol=0, abs_tol=1e-6)
assert float(point["z"]) == 123.45
assert result["points"][1] == {"x": "NaN", "y": "NaN", "z": "NaN"}, result
units = request("GET", "/api/unit/v3/unit?limit=-1")
assert units["totalCount"] == 17 and units["count"] == 17
assert request("GET", "/api/unit/v3/measurement/list")["count"] == 6
assert request("GET", "/api/unit/v3/unit/measurement?ancestry=Length")["count"] == 4
survey = request("GET", "/api/unit/v3/unit/symbol?namespaces=osdu-lite&symbol=ft%5BUS%5D")
metre = request("GET", "/api/unit/v3/unit/symbol?namespaces=osdu-lite&symbol=m")
c = request("POST", "/api/unit/v3/conversion/scale", {
    "fromUnitPersistableReference": survey["essenceJson"], "toUnit": metre["essence"]})["scaleOffset"]
assert math.isclose(c["scale"] * (1 - c["offset"]), 1200 / 3937, rel_tol=0, abs_tol=1e-12)
c = request("GET", "/api/unit/v3/conversion/scale?namespaces=osdu-lite&fromSymbol=degF&toSymbol=degC")["scaleOffset"]
assert math.isclose(c["scale"] * (212 - c["offset"]), 100, rel_tol=0, abs_tol=1e-10)
assert request("GET", "/unit/v3/unit") == request("GET", "/api/unit/v3/unit")
error = request("POST", "/api/crs/converter/v3/convert", {
    "fromCRS": refs["4326"], "toCRS": refs["3857"],
    "points": [{"x": True, "y": 20, "z": 0}]}, expected=400)
assert error["code"] == 400
print("PASS: reference catalog, real CRS conversion, unit coefficients, aliases and validation")
PY
