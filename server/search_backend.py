"""Single-partition OpenSearch transport. No in-process query fallback."""
import hashlib
import fnmatch
import json
import socket
import re
from runtime_jobs import Outcome
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
        self.owned_prefix = self.index
        self.epoch = None
        self.indices = {}
        self.registry = None
        self.rebuild_required = False

    def configure_registry(self, registry):
        self.registry = registry

    def kind_index(self, kind):
        return self.owned_prefix + "-" + hashlib.sha256(kind.encode()).hexdigest() + "-" + (self.epoch or "legacy")

    def _refresh_target(self):
        # Keep the existing helper target usable for DELETE/stats and single-kind
        # mapping inspection. Queries route only over this process's owned indices.
        self.index = ",".join(self.indices[k] for k in sorted(self.indices)) or (
            self.owned_prefix + "-" + self.epoch if self.epoch else self.owned_prefix)

    def start_epoch(self, epoch):
        if not re.fullmatch(r"[a-z0-9]{1,32}", epoch):
            raise ValueError("Invalid process epoch")
        self.request("DELETE", "/" + self.owned_prefix + "-*", missing_ok=True)
        self.request("DELETE", "/" + self.owned_prefix, missing_ok=True)
        self.epoch = epoch
        self.indices.clear()
        self.rebuild_required = False
        self._refresh_target()

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
            error = BackendError(503, f"OpenSearch operation failed (HTTP {exc.code})")
            error.retryable = exc.code in {408, 429} or exc.code >= 500
            raise error from exc
        except (URLError, TimeoutError, socket.timeout, OSError, ValueError) as exc:
            raise BackendError(503, "OpenSearch unavailable or invalid response") from exc

    def provision(self, kinds=None):
        selected = set(self.indices) | set(kinds or ())
        if not selected:
            self.request("GET", "/")  # Lazy zero-shard partitions still fail closed on outage.
        if len(selected) > 8:
            raise BackendError(429, "active_kind_capacity")
        if selected and self.registry is None:
            # Direct legacy transport helpers use the same bundled registry,
            # never the old arbitrary dynamic mapping.
            from schema_registry import SchemaRegistry
            self.registry = SchemaRegistry()
        created = False
        for kind in sorted(selected):
            mapping = self.registry.mapping(kind)
            name = self.kind_index(kind)
            if not self.request("GET", "/" + name, missing_ok=True):
                # A genuinely lost previously-provisioned shard requires a
                # rebuild. First admission's lazy shard must not publish queued
                # records early merely because a client searches before delay.
                self.rebuild_required = self.rebuild_required or kind in self.indices
                self.request("PUT", "/" + name, {
                    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                    "mappings": mapping})
                created = True
            self.indices[kind] = name
        self._refresh_target()
        return created

    def provision_kinds(self, kinds):
        return self.provision(kinds)

    def reset(self):
        self.request("DELETE", "/" + self.owned_prefix + "-*", missing_ok=True)
        self.indices.clear()
        self.rebuild_required = False
        self._refresh_target()

    def bulk(self, records):
        if not records:
            return []
        kinds = {record["kind"] for record in records}
        if not kinds <= set(self.indices):
            self.provision(kinds)
        outcomes = []
        for start in range(0, len(records), 100):
            lines = []
            batch = records[start:start + 100]
            for record in batch:
                generation = record.get("_generation", record.get("version"))
                if type(generation) is not int or generation < 1:
                    raise BackendError(400, "external_generation_required")
                lines.extend((json.dumps({"index": {"_id": record["id"],
                              "_index": self.kind_index(record["kind"]),
                              "version": generation, "version_type": "external_gte"}},
                              ensure_ascii=False, separators=(",", ":")),
                              json.dumps({k: v for k, v in record.items() if k != "_generation"},
                                         ensure_ascii=False, separators=(",", ":"))))
            body = "\n".join(lines) + "\n"
            if len(body.encode("utf-8")) > 1024 * 1024:
                raise BackendError(400, "bulk_byte_limit")
            try:
                response = self.request("POST", "/_bulk?refresh=true", body, ndjson=True)
            except BackendError as exc:
                outcomes.extend(Outcome(False, getattr(exc, "retryable", exc.status == 503),
                                        "engine_unavailable") for _ in batch)
                continue
            items = response.get("items", [])
            if len(items) != len(batch):
                outcomes.extend(Outcome(False, True, "invalid_bulk_response") for _ in batch)
                continue
            for item in items:
                status = item.get("index", {}).get("status", 503)
                outcomes.append(Outcome(200 <= status < 300, status in {408, 429} or status >= 500,
                                        "" if 200 <= status < 300 else
                                        "superseded" if status == 409 else "index_failed"))
        return outcomes

    def tombstone(self, rid, kind, generation):
        return self.bulk([{"id": rid, "kind": kind, "version": generation, "_deleted": True}])

    def delete(self, rid):
        for name in self.indices.values():
            self.request("DELETE", f"/{name}/_doc/{quote(rid, safe='')}?refresh=true", missing_ok=True)

    def delete_kind(self, kind, pattern=False):
        if not self.indices:
            return
        query = {"wildcard": {"kind": {"value": kind, "case_insensitive": True}}} if pattern else {
            "term": {"kind": kind}}
        result = self.request("POST", f"/{self.index}/_delete_by_query?refresh=true",
                              {"query": query})
        if result.get("failures") or result.get("timed_out"):
            raise BackendError(503, "OpenSearch kind deletion failed")

    def count(self):
        if not self.indices:
            self.request("GET", "/")
            return 0
        response = self.request("POST", f"/{self.index}/_count",
                                {"query": {"bool": {"must_not": [{"term": {"_deleted": True}}]}}})
        if response.get("_shards", {}).get("failed", 0):
            raise BackendError(503, "OpenSearch count incomplete")
        return response["count"]

    def search(self, payload, groups=None, owner=False):
        kinds = payload.get("kind")
        if not kinds or not self.indices:
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
        if payload.get("_legacy_text") and self.registry is not None:
            # Only the legacy adapter retains its original bare-data-field
            # full-text convention. Mappings stay exact keyword + .text;
            # core/transport callers use those explicit registered paths.
            selected = [kind for kind in self.indices if any(
                fnmatch.fnmatchcase(kind.lower(), pattern.lower()) for pattern in kinds)]
            def legacy_field(match):
                field = match.group("field")
                if not field or not field.startswith("data."):
                    return match[0]
                base = field[:-8] if field.endswith(".keyword") else field
                try:
                    capability = self.registry.field_capabilities(selected, [base])[base]
                except ValueError:
                    return match[0]
                if capability["type"] != "keyword":
                    return match[0]
                return (base if field.endswith(".keyword") else base + ".text") + ":"
            text = re.sub(r'"(?:\\.|[^"\\])*"|\\.|(?P<field>[A-Za-z_][\w.]*):',
                          legacy_field, text)
        must = {"match_all": {}} if text.strip() in {"", "*"} else {
            "query_string": {"query": text, "lenient": False}}
        body = {"query": {"bool": {"must": [must], "filter": filters,
                                 "must_not": [{"term": {"_deleted": True}}]}},
                "sort": [{"id": "asc"}], "size": 500}
        if payload.get("_exclude_ids"):
            body["query"]["bool"]["must_not"].append({"terms": {"id": payload["_exclude_ids"]}})
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

    def page(self, payload, *, references=False):
        """One bounded engine request; include policy IDs before hits/count/terms."""
        kinds = payload["kind"]
        selected = [self.indices[k] for k in sorted(self.indices) if any(
            fnmatch.fnmatchcase(k.lower(), pattern.lower()) for pattern in kinds)]
        if not selected:
            self.request("GET", "/")
            result = {"results": [], "totalCount": 0}
            if "aggregateBy" in payload:
                result["aggregations"] = []
            return result
        text = payload.get("query", "*")
        must = {"match_all": {}} if text.strip() in ("", "*") else {
            "query_string": {"query": text, "lenient": False}}
        body = {"query": {"bool": {"must": [must],
                    "filter": [{"terms": {"id": payload["_allowed_ids"]}}],
                    "must_not": [{"term": {"_deleted": True}}]}},
                "from": 0 if references else payload["offset"],
                "size": 5000 if references else payload["limit"],
                "track_total_hits": True,
                "sort": [{field: {"order": order, "missing": "_last"}}
                         for field, order in payload["_sort"]]}
        if references:
            body["_source"] = False
            body["docvalue_fields"] = ["version"]
        if "aggregateBy" in payload:
            body["aggs"] = {"values": {"terms": {"field": payload["aggregateBy"],
                "size": 101, "shard_size": 5001, "order": [{"_count": "desc"}, {"_key": "asc"}]}}}
        response = self.request("POST", "/" + ",".join(selected) + "/_search", body, query=True)
        if response.get("timed_out") or response.get("_shards", {}).get("failed", 0):
            raise BackendError(503, "OpenSearch search incomplete")
        total = response["hits"]["total"]
        total = total["value"] if isinstance(total, dict) else total
        if references and total > 5000:
            raise BackendError(429, "cursor_reference_capacity")
        result = {"totalCount": total, "results": [
            {"id": hit["_id"], "version": hit["fields"]["version"][0], "sort": hit.get("sort", [])}
            if references else hit["_source"] for hit in response["hits"]["hits"]]}
        if "aggregateBy" in payload:
            buckets = response["aggregations"]["values"]["buckets"]
            if len(buckets) > 100:
                raise BackendError(400, "aggregation_cardinality_limit")
            result["aggregations"] = sorted(
                [{"key": str(b.get("key_as_string", b["key"])), "count": b["doc_count"]} for b in buckets],
                key=lambda b: (-b["count"], b["key"]))
        return result
