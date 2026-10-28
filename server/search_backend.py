"""Single-partition OpenSearch transport. No in-process query fallback."""
import hashlib
import json
import socket
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen


class BackendError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class OpenSearch:
    def __init__(self, url, partition):
        parsed = urlsplit(url)
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname
                or parsed.username or parsed.password or parsed.path not in {"", "/"}
                or parsed.query or parsed.fragment):
            raise ValueError("OPENSEARCH_URL must be an HTTP(S) origin without credentials or path")
        self.url = url.rstrip("/")
        self.index = "osdu-lite-" + hashlib.sha256(partition.encode()).hexdigest()[:32]

    def request(self, method, path, body=None, missing_ok=False, query=False, ndjson=False):
        data = (body.encode() if ndjson else json.dumps(body).encode()) if body is not None else None
        req = Request(self.url + path, data=data, method=method,
                      headers={"Content-Type": "application/x-ndjson" if ndjson else "application/json"})
        try:
            with urlopen(req, timeout=5) as response:
                return json.loads(response.read() or b"{}")
        except HTTPError as exc:
            if missing_ok and exc.code == 404:
                return {}
            # Do not expose engine internals or record contents in public errors.
            if query and exc.code == 400:
                raise BackendError(400, "Invalid OpenSearch query_string") from exc
            raise BackendError(503, f"OpenSearch operation failed (HTTP {exc.code})") from exc
        except (URLError, TimeoutError, socket.timeout, OSError, ValueError) as exc:
            raise BackendError(503, "OpenSearch unavailable or invalid response") from exc

    def provision(self):
        path = "/" + self.index
        existing = self.request("GET", path, missing_ok=True)
        if existing:
            return False
        self.request("PUT", path, {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "date_detection": False,
                "dynamic_templates": [{"strings": {"match_mapping_type": "string", "mapping": {
                    "type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 8191}}}}}],
                "properties": {
                    "id": {"type": "keyword"}, "kind": {"type": "keyword"},
                    "acl": {"properties": {"owners": {"type": "keyword"}, "viewers": {"type": "keyword"}}},
                    "data": {"properties": {
                        "StartM": {"type": "double"}, "StopM": {"type": "double"},
                        "MeasuredDepthM": {"type": "double"}, "TrueVerticalDepthM": {"type": "double"},
                        "SpudDate": {"type": "date"},
                    }},
                },
            },
        })
        return True

    def reset(self):
        self.request("DELETE", "/" + self.index, missing_ok=True)
        self.provision()

    def bulk(self, records):
        for start in range(0, len(records), 200):
            lines = []
            for record in records[start:start + 200]:
                lines.extend((json.dumps({"index": {"_id": record["id"]}}), json.dumps(record)))
            response = self.request("POST", f"/{self.index}/_bulk?refresh=true",
                                    "\n".join(lines) + "\n", ndjson=True)
            items = response.get("items", [])
            if len(items) != len(records[start:start + 200]) or response.get("errors") or any(
                    item.get("index", {}).get("status", 500) >= 300 for item in items):
                raise BackendError(503, "OpenSearch indexing failed; check mapping compatibility of stored data")

    def delete(self, rid):
        self.request("DELETE", f"/{self.index}/_doc/{quote(rid, safe='')}?refresh=true", missing_ok=True)

    def delete_kind(self, kind, pattern=False):
        query = {"wildcard": {"kind": {"value": kind, "case_insensitive": True}}} if pattern else {
            "term": {"kind": kind}}
        result = self.request("POST", f"/{self.index}/_delete_by_query?refresh=true",
                              {"query": query})
        if result.get("failures") or result.get("timed_out"):
            raise BackendError(503, "OpenSearch kind deletion failed")

    def count(self):
        response = self.request("GET", f"/{self.index}/_count")
        if response.get("_shards", {}).get("failed", 0):
            raise BackendError(503, "OpenSearch count incomplete")
        return response["count"]

    def search(self, payload, groups=None, owner=False):
        kinds = payload.get("kind")
        if not kinds:
            return []
        kinds = kinds if isinstance(kinds, list) else [kinds]
        filters = [{"bool": {"should": [
            {"wildcard": {"kind": {"value": kind, "case_insensitive": True}}} for kind in kinds
        ], "minimum_should_match": 1}}]
        if groups is not None:
            if owner:
                filters.append({"terms": {"acl.owners": sorted(groups)}})
            else:
                filters.append({"bool": {"should": [
                    {"terms": {"acl.owners": sorted(groups)}},
                    {"terms": {"acl.viewers": sorted(groups)}},
                ], "minimum_should_match": 1}})
        text = payload.get("query") or ""
        if not isinstance(text, str):
            raise BackendError(400, "query must be a string")
        must = {"match_all": {}} if text.strip() in {"", "*"} else {"query_string": {"query": text}}
        body = {"query": {"bool": {"must": [must], "filter": filters}},
                "sort": [{"id": "asc"}], "size": 500}
        scroll = None
        try:
            response = self.request("POST", f"/{self.index}/_search?scroll=1m", body, query=True)
            records = []
            while True:
                scroll = response.get("_scroll_id", scroll)
                if response.get("timed_out") or response.get("_shards", {}).get("failed", 0):
                    raise BackendError(503, "OpenSearch search incomplete")
                hits = response["hits"]["hits"]
                if not hits:
                    return records
                records.extend(hit["_source"] for hit in hits)
                if not scroll:
                    raise BackendError(503, "OpenSearch did not return a scroll snapshot")
                response = self.request("POST", "/_search/scroll", {"scroll": "1m", "scroll_id": scroll})
        finally:
            if scroll:
                self.request("DELETE", "/_search/scroll", {"scroll_id": [scroll]})
