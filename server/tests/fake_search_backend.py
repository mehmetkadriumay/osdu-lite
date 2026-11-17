"""Explicit isolated test double, NOT a Lucene implementation."""
import fnmatch
import re
from copy import deepcopy
from runtime_jobs import Outcome


class FakeSearch:
    def __init__(self):
        self.documents = {}
        self.generations = {}

    def start_epoch(self, epoch):
        self.reset()

    def provision(self):
        pass

    def reset(self):
        self.documents.clear()
        self.generations.clear()

    def bulk(self, records):
        outcomes = []
        for record in records:
            rid, generation = record["id"], record["version"]
            if generation < self.generations.get(rid, 0):
                outcomes.append(Outcome(False, False, "superseded"))
            else:
                self.generations[rid] = generation
                self.documents[rid] = deepcopy(record)
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
