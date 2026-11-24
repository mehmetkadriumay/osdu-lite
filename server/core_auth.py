"""Local development identities and flat, live entitlement policy."""
import datetime
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Principal:
    name: str


# Only explicitly trusted, non-request callers may select this context.
SYSTEM = Principal("osdu-lite:system")
ANONYMOUS = Principal("osdu-lite:anonymous")


def load_identities(path, token, require_auth):
    try:
        if path:
            source = Path(path)
            if source.stat().st_size > 1024 * 1024:
                raise ValueError()
            entries = json.loads(source.read_text(encoding="utf-8"))
        else:
            entries = [{"principal": "demo@osdu-lite.local", "token": token, "groups": []}]
        if not isinstance(entries, list) or not entries or len(entries) > 256:
            raise ValueError()
        principals, tokens, seeds = set(), {}, {}
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {"principal", "token", "groups"}:
                raise ValueError()
            name, secret, groups = entry["principal"], entry["token"], entry["groups"]
            if (not isinstance(name, str) or not name or len(name) > 512
                    or name.startswith("osdu-lite:") or name in principals
                    or not isinstance(secret, str) or (require_auth and not secret)
                    or len(secret) > 4096 or secret in tokens
                    or not isinstance(groups, list) or len(groups) > 256
                    or any(not isinstance(g, str) or not g or len(g) > 512 for g in groups)):
                raise ValueError()
            principals.add(name)
            tokens[secret] = Principal(name)
            seeds[name] = groups
        durable_seeds = json.dumps(seeds)
        if any(secret and secret in durable_seeds for secret in tokens):
            raise ValueError()
        return tokens, seeds
    except (OSError, ValueError, TypeError, KeyError, RecursionError):
        raise ValueError("Invalid local identity configuration") from None


def effective_groups(principal, groups, partition):
    if principal is SYSTEM:
        raise ValueError("System context has no HTTP service roles")
    suffix = f"@{partition}.company.com"
    result = set()
    for email, group in groups.items():
        if principal.name in group["members"]:
            result.add(email)
            if email.endswith(suffix):
                result.add(email[:-len(suffix)])
    return result


def valid_legal_tag(tag):
    try:
        expiry = tag["properties"]["expirationDate"]
        if not isinstance(expiry, str) or len(expiry) != 10:
            return False
        return datetime.date.fromisoformat(expiry) >= datetime.datetime.now(datetime.timezone.utc).date()
    except (KeyError, TypeError, ValueError):
        return False


def can_read(record, groups, legal_valid, *, owner=False):
    acl = record.get("acl", {})
    allowed = acl.get("owners", []) + ([] if owner else acl.get("viewers", []))
    tags = record.get("legal", {}).get("legaltags", [])
    return bool(tags and all(legal_valid(tag) for tag in tags) and set(allowed) & groups)


def can_write(record, groups, legal_valid):
    return can_read(record, groups, legal_valid, owner=True)


def validate_acl(record, groups, partition):
    acl = record.get("acl", {})
    if not isinstance(acl, dict) or not acl.get("owners"):
        raise ValueError("acl owners must be nonempty")
    for field in ("owners", "viewers"):
        entries = acl.get(field, [])
        if (not isinstance(entries, list) or any(
                not isinstance(g, str) or not g.startswith("data.")
                or not g.endswith(f"@{partition}.company.com") or g not in groups for g in entries)):
            raise ValueError("ACL must reference existing partition data groups")


def roles(required, profile):
    result = set(required)
    if "service.storage.viewer" in result:
        result |= {"service.storage.creator", "service.storage.admin"}
    if "service.storage.editor" in result:
        result |= {"service.storage.creator", "service.storage.admin"}
        if profile == "core":
            result.discard("service.storage.editor")
    if "service.search.user" in result:
        result |= {"users.datalake.viewers", "users.datalake.editors",
                   "users.datalake.admins", "users.datalake.ops"}
        if profile == "core":
            result.discard("service.search.user")
    if "users.datalake.viewers" in result:
        result |= {"users.datalake.editors", "users.datalake.admins", "users.datalake.ops"}
    if "users.datalake.editors" in result:
        result.add("users.datalake.admins")
    return result
