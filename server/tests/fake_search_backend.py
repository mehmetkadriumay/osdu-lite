"""Explicit isolated test double, NOT a Lucene implementation."""
import fnmatch
import re
from copy import deepcopy


class FakeSearch:
    def __init__(self):
        self.documents = {}

    def provision(self):
        pass

    def reset(self):
        self.documents.clear()

    def bulk(self, records):
        self.documents.update({r["id"]: deepcopy(r) for r in records})

    def delete(self, rid):
        self.documents.pop(rid, None)

    def delete_kind(self, kind, pattern=False):
        self.documents = {k: v for k, v in self.documents.items()
                          if not (fnmatch.fnmatchcase(v["kind"], kind) if pattern else v["kind"] == kind)}

    def count(self):
        return len(self.documents)

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
                if any(fnmatch.fnmatchcase(r["kind"].lower(), k.lower()) for k in kinds)
                and matches(r, payload.get("query"))
                and (groups is None or set(groups).intersection(
                    r.get("acl", {}).get("owners", []) +
                    ([] if owner else r.get("acl", {}).get("viewers", []))))]
