"""Explicit isolated test double, NOT a Lucene implementation."""
import fnmatch
import re
from copy import deepcopy
from runtime_jobs import Outcome


class FakeSearch:
    def __init__(self):
        self.documents = {}
        self.generations = {}
        self.indices = {}
        self.registry = None

    def configure_registry(self, registry):
        self.registry = registry

    def start_epoch(self, epoch):
        self.reset()

    def provision(self, kinds=None):
        for kind in kinds or ():
            self.indices[kind] = self.registry.mapping(kind)
        return False

    def provision_kinds(self, kinds):
        return self.provision(kinds)

    def reset(self):
        self.documents.clear()
        self.generations.clear()
        self.indices.clear()

    def bulk(self, records):
        outcomes = []
        for record in records:
            rid, generation = record["id"], record.get("_generation", record["version"])
            if generation < self.generations.get(rid, 0):
                outcomes.append(Outcome(False, False, "superseded"))
            else:
                self.generations[rid] = generation
                self.documents[rid] = deepcopy({k: v for k, v in record.items() if k != "_generation"})
                outcomes.append(Outcome())
        return outcomes

    def tombstone(self, rid, kind, generation):
        return self.bulk([{"id": rid, "kind": kind, "version": generation, "_deleted": True}])

    def delete(self, rid):
        self.documents.pop(rid, None)

    def delete_kind(self, kind, pattern=False):
        self.documents = {k: v for k, v in self.documents.items()
                          if not (fnmatch.fnmatchcase(v["kind"], kind) if pattern else v["kind"] == kind)}

    def count(self):
        return sum(not r.get("_deleted") for r in self.documents.values())

    def search(self, payload, groups=None, owner=False):
        def matches(record, query):
            if not query or query == "*":
                return True
            if " OR " in query:
                return any(matches(record, q) for q in query.split(" OR "))
            if " AND " in query:
                return all(matches(record, q) for q in query.split(" AND "))
            if query.startswith("NOT "):
                return not matches(record, query[4:])
            field, value = query.split(":", 1)
            actual = record
            for key in field.split("."):
                actual = actual.get(key, {}) if isinstance(actual, dict) else None
            bounds = re.fullmatch(r"\[(.*) TO (.*)\]", value)
            if bounds:
                lo, hi = bounds.groups()
                return actual is not None and (lo == "*" or float(actual) >= float(lo)) and (
                    hi == "*" or float(actual) <= float(hi))
            return fnmatch.fnmatchcase(str(actual), value.strip('"'))

        kinds = payload.get("kind") or []
        kinds = kinds if isinstance(kinds, list) else [kinds]
        return [deepcopy(r) for _, r in sorted(self.documents.items())
                if not r.get("_deleted") and r["id"] not in payload.get("_exclude_ids", [])
                and any(fnmatch.fnmatchcase(r["kind"].lower(), k.lower()) for k in kinds)
                and matches(r, payload.get("query"))
                and (groups is None or set(groups).intersection(
                    r.get("acl", {}).get("owners", []) +
                    ([] if owner else r.get("acl", {}).get("viewers", []))))]

    def page(self, payload, *, references=False):
        from search_backend import BackendError
        found = [r for r in self.search(payload) if r["id"] in payload["_allowed_ids"]]
        def value(record, path):
            obj = record
            for part in path.split("."):
                obj = obj.get(part) if isinstance(obj, dict) else None
            return obj
        for field, order in reversed(payload["_sort"]):
            present = [r for r in found if value(r, field) is not None]
            missing = [r for r in found if value(r, field) is None]
            found = sorted(present, key=lambda r: value(r, field), reverse=order == "desc") + missing
        result = {"totalCount": len(found)}
        if "aggregateBy" in payload:
            counts = {}
            for record in found:
                v = value(record, payload["aggregateBy"])
                values = v if isinstance(v, list) else ([] if v is None else [v])
                for key in set(str(v) for v in values):
                    counts[key] = counts.get(key, 0) + 1
            if len(counts) > 100:
                raise BackendError(400, "aggregation_cardinality_limit")
            result["aggregations"] = [{"key": k, "count": n} for k, n in
                                      sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]
        result["results"] = [
            {"id": r["id"], "version": r["version"],
             "sort": [value(r, f) for f, _ in payload["_sort"]]} for r in found
        ] if references else found[payload["offset"]:payload["offset"] + payload["limit"]]
        return result
