"""Bounded in-memory community service mocks; no outbound I/O or persistence."""
import base64
import binascii
import datetime
import fnmatch
import json
import re
import threading
import uuid
from copy import deepcopy
from pathlib import Path
from urllib.parse import unquote, urlparse

import reference_services as ref
import reference_extensions

INVENTORY = json.loads((Path(__file__).parent / "data/community_api.json").read_text())
GROUPS = {"osdu-lite." + name + ".user" for name in INVENTORY["services"]
          if name not in {"crs-catalog", "crs-converter"}}
MAX_RESOURCES = 1000
LOCK = threading.RLock()
PARTITIONS = {}
SUBSCRIPTIONS = {}
SUBSCRIPTION_SECRETS = {}
DDMS = {}
ACTIONS = {}
SECRETS = {}
EVENTS = []
REPLAYS = {}
TOPICS = [{"name": "records-changed", "description": "Local record-change mock, never delivered externally",
           "state": "ACTIVE", "example": [{"id": "partition:record:example", "op": "update"}]}]

# The manifest is a closed operation table, not a generic CRUD engine.
ROUTES = []
for _name, _spec in INVENTORY["services"].items():
    for _operation in _spec["operations"]:
        _method, _suffix = _operation.split(" ", 1)
        _pattern = re.escape(_spec["base"] + _suffix)
        _pattern = re.sub(r"\\\{[^}]+\\\}", r"([^/]+)", _pattern)
        ROUTES.append((_method, re.compile(_pattern + r"\Z"), _name, _suffix))
# Match literal resources (e.g. /list) before variables.
ROUTES.sort(key=lambda route: "{" in route[3])


def match(method, path):
    normalized = path if path.startswith("/api/") else "/api" + path
    for verb, pattern, service, suffix in ROUTES:
        found = pattern.fullmatch(normalized)
        if verb == method and found:
            return service, suffix, tuple(unquote(v) for v in found.groups())
    return None


def group(service):
    return {"unit": ref.UNIT_GROUP, "crs-catalog": ref.CATALOG_GROUP,
            "crs-converter": ref.CONVERTER_GROUP}.get(service, "osdu-lite." + service + ".user")


def reset(partition):
    with LOCK:
        for state in (PARTITIONS, SUBSCRIPTIONS, SUBSCRIPTION_SECRETS, DDMS, ACTIONS, SECRETS, REPLAYS):
            state.clear()
        EVENTS.clear()
        PARTITIONS[partition] = {"dataPartitionId": {"sensitive": False, "value": partition}}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def obj(value, required=(), allowed=None):
    if not isinstance(value, dict):
        ref.bad("Request must be a JSON object")
    if allowed is not None and set(value) - set(allowed):
        ref.bad("Unsupported fields: " + ", ".join(sorted(set(value) - set(allowed))))
    for key in required:
        if key not in value:
            ref.bad("Missing field: " + key)
    return value


def strings(value, label, maximum=1000, empty=False):
    if not isinstance(value, list) or len(value) > maximum or (not value and not empty):
        ref.bad(label + " must be a bounded array")
    for entry in value:
        ref.text(entry, label + " item")
    return value


def identifier(value, label="id"):
    value = ref.text(value, label)
    if not re.fullmatch(r"[A-Za-z0-9_-]{2,100}", value):
        ref.bad(label + " must contain 2-100 letters, digits, underscores or hyphens")
    return value


def resource(state, key):
    if key not in state:
        raise ref.ReferenceError(404, "Resource not found")
    return state[key]


def insert(state, key, value):
    if key in state:
        raise ref.ReferenceError(409, "Resource already exists")
    if len(state) >= MAX_RESOURCES:
        raise ref.ReferenceError(413, "Mock resource limit reached")
    state[key] = deepcopy(value)


def url_data(value):
    value = ref.text(value, "URL")
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
        ref.bad("URL must be HTTP(S), without embedded credentials; stored only, never fetched")
    return value


def partition_api(method, suffix, args, query, payload):
    ref.query_values(query, {})
    if not args:
        return 200, sorted(PARTITIONS)
    key = identifier(args[0], "partitionId")
    if method == "GET":
        return 200, deepcopy(resource(PARTITIONS, key))
    if method == "DELETE":
        resource(PARTITIONS, key)
        if key == next(iter(PARTITIONS)):
            raise ref.ReferenceError(409, "Configured authentication partition cannot be deleted")
        del PARTITIONS[key]
        return 204, None
    obj(payload, {"properties"}, {"properties"})
    properties = obj(payload["properties"])
    if not properties or len(properties) > 100:
        ref.bad("properties must contain 1-100 entries")
    for name, value in properties.items():
        ref.text(name, "property name")
        obj(value, {"value", "sensitive"}, {"value", "sensitive"})
        if type(value["sensitive"]) is not bool:
            ref.bad("sensitive must be boolean")
    if method == "POST":
        insert(PARTITIONS, key, properties)
        return 201, None
    resource(PARTITIONS, key).update(deepcopy(properties))
    return 204, None


def index_records(s, ids=None, kind=None, clean=False):
    with s.STORE_LOCK:
        if clean:
            for rid, record in list(s.SEARCH_BY_ID.items()):
                if kind is None or fnmatch.fnmatchcase(record["kind"], kind):
                    del s.SEARCH_BY_ID[rid]
        selected = [record for rid, record in sorted(s.RECORD_BY_ID.items())
                    if rid not in s.DELETED and (ids is None or rid in ids)
                    and (kind is None or fnmatch.fnmatchcase(record["kind"], kind))]
        for record in selected:
            s.SEARCH_BY_ID[record["id"]] = deepcopy(record)
        return deepcopy(selected)


def indexer_api(s, method, suffix, query, payload):
    q = ref.query_values(query, {"kind"} if suffix == "/index" else
                         {"force_clean"} if suffix == "/reindex" else {})
    if suffix == "/partitions/provision":
        return 200, None  # The single in-memory index needs no external provisioning.
    if suffix == "/index":
        kind = ref.text(q.get("kind"), "kind")
        with s.STORE_LOCK:
            for rid, record in list(s.SEARCH_BY_ID.items()):
                if record["kind"] == kind:
                    del s.SEARCH_BY_ID[rid]
        return 200, None
    clean = ref.boolean(q["force_clean"], "force_clean") if "force_clean" in q else False
    if suffix == "/reindex/records":
        obj(payload, {"recordIds"}, {"recordIds"})
        ids = strings(payload["recordIds"], "recordIds")
        with s.STORE_LOCK:
            if any(rid not in s.RECORD_BY_ID or rid in s.DELETED for rid in ids):
                raise ref.ReferenceError(404, "One or more records do not exist")
        index_records(s, ids=ids)
        return 202, {}
    kind = None
    if method == "POST":
        obj(payload, {"kind"}, {"kind", "cursor"})
        kind = ref.text(payload["kind"], "kind")
        if payload.get("cursor"):
            ref.bad("Local synchronous reindex does not use a cursor")
    index_records(s, kind=kind, clean=clean)
    return 200, None


def validate_filter(value):
    obj(value, allowed={"entityType", "source", "version"})
    for key, entries in value.items():
        strings(entries, key, maximum=100, empty=True)


def registration_secret(value):
    obj(value, {"secretType", "value"}, {"secretType", "value"})
    secret_type = ref.text(value["secretType"], "secretType")
    if secret_type == "HMAC":
        ref.text(value["value"], "HMAC value")
    elif secret_type == "GSA":
        obj(value["value"], {"audience", "key"}, {"audience", "key"})
        ref.text(value["value"]["audience"], "GSA audience")
        ref.text(value["value"]["key"], "GSA key")
    else:
        ref.bad("secretType must be HMAC or GSA")
    return deepcopy(value)


def action_matches(action, payload):
    # Deliberately limited exact filters; never execute arbitrary expressions.
    obj(payload)
    if not any(action["filter"].values()):
        return True
    parts = ref.text(payload.get("kind"), "record kind").split(":")
    if len(parts) != 4 or not all(parts):
        ref.bad("record kind must contain authority:source:entityType:version")
    components = dict(zip(("source", "entityType", "version"), parts[1:]))
    return all(not values or components[key] in values for key, values in action["filter"].items())


def register_api(method, suffix, args, query, payload):
    allowed = {"notificationId"} if suffix == "/subscription" and method == "GET" else (
        {"type"} if suffix == "/ddms" and method == "GET" else set())
    q = ref.query_values(query, allowed, allowed)
    if suffix == "/topics":
        return 200, deepcopy(TOPICS)
    if suffix == "/subscriptions":
        return 200, [deepcopy(SUBSCRIPTIONS[key]) for key in sorted(SUBSCRIPTIONS)]
    if suffix == "/action:test":
        obj(payload, {"action", "testPayload"}, {"action", "testPayload"})
        action = obj(payload["action"], {"name", "filter"})
        validate_filter(action["filter"])
        matched = action_matches(action, payload["testPayload"])
        url = url_data(action["url"]) if action.get("url") else ""
        if "${" in url or "{{" in url:
            ref.bad("Remote/template action evaluation is not supported by this mock")
        return 200, {"url": url if matched else "", "errors": "" if matched else "Filter did not match"}
    if suffix == "/action:retrieve":
        obj(payload)
        return 200, [deepcopy(ACTIONS[k]) for k in sorted(ACTIONS) if action_matches(ACTIONS[k], payload)]
    state = SUBSCRIPTIONS if suffix.startswith("/subscription") else DDMS if suffix.startswith("/ddms") else ACTIONS
    if args:
        key = identifier(args[0])
        value = resource(state, key)
        if suffix.endswith("/secret"):
            # Never return the subscription's credential, even from list/create.
            SUBSCRIPTION_SECRETS[key] = registration_secret(payload)
            return 204, None
        if method == "GET":
            return 200, deepcopy(value)
        del state[key]
        SUBSCRIPTION_SECRETS.pop(key, None)
        return 204, None
    if method == "GET":
        if state is SUBSCRIPTIONS:
            rows = [v for k, v in sorted(state.items()) if v["notificationId"] == q["notificationId"]]
        else:
            rows = [v for k, v in sorted(state.items())
                    if any(i["entityType"] == q["type"] for i in v.get("interfaces", []))]
        return 200, deepcopy(rows)
    if state is SUBSCRIPTIONS:
        obj(payload, {"name", "topic", "pushEndpoint"},
            {"id", "name", "description", "topic", "pushEndpoint", "secret", "notificationId", "createdBy"})
        ref.text(payload["name"], "name")
        ref.text(payload["topic"], "topic")
        if payload["topic"] not in {topic["name"] for topic in TOPICS}:
            ref.bad("Unsupported mock topic")
        url_data(payload["pushEndpoint"])
    elif state is DDMS:
        obj(payload, {"id", "name"}, {"id", "name", "description", "contactEmail", "interfaces", "createdDateTimeEpoch"})
        interfaces = payload.get("interfaces", [])
        if not isinstance(interfaces, list) or len(interfaces) > 100:
            ref.bad("interfaces must be an array of at most 100 entries")
        for interface in interfaces:
            obj(interface, {"entityType", "schema"}, {"entityType", "schema"})
            ref.text(interface["entityType"], "entityType")
            obj(interface["schema"])
    else:
        obj(payload, {"name", "filter"}, {"id", "name", "description", "img", "url", "contactEmail", "filter"})
        validate_filter(payload["filter"])
        if "url" in payload:
            url_data(payload["url"])
    name = ref.text(payload["name"], "name")
    if not re.fullmatch(r"[A-Za-z0-9 -]{2,50}", name):
        ref.bad("name must contain 2-50 letters, digits, spaces or hyphens")
    key = identifier(payload.get("id", str(uuid.uuid4())))
    if len(key) > 50:
        ref.bad("Registration id exceeds 50 characters")
    secret = registration_secret(payload["secret"]) if state is SUBSCRIPTIONS and "secret" in payload else None
    value = deepcopy(payload)
    value.pop("secret", None)
    value["id"] = key
    if state is SUBSCRIPTIONS:
        value["notificationId"] = str(uuid.uuid4())
        value["createdBy"] = "osdu-lite"
    elif state is DDMS:
        value["createdDateTimeEpoch"] = now()
    else:
        value["createdOnEpoch"] = now()
    insert(state, key, value)
    if secret is not None:
        SUBSCRIPTION_SECRETS[key] = secret
    return 201, deepcopy(value)


def notification_api(s, query, payload):
    ref.query_values(query, {})
    obj(payload, {"message", "subscription"}, {"message", "subscription"})
    message = obj(payload["message"], {"data"}, {"data", "attributes", "messageId", "publishTime"})
    attributes = obj(message.get("attributes", {}))
    if any(not isinstance(value, str) for value in attributes.values()):
        ref.bad("message.attributes values must be strings")
    if attributes.get("data-partition-id") != s.PARTITION:
        ref.bad("Notification data-partition-id must match the configured partition")
    notification_id = ref.text(payload["subscription"], "subscription").rstrip("/").split("/")[-1]
    try:
        encoded = ref.text(message["data"], "message.data")
        data = base64.b64decode(encoded + "=" * (-len(encoded) % 4), validate=True)
    except (ValueError, binascii.Error):
        ref.bad("message.data must be valid base64 JSON")
    event = ref.strict_json(data)
    if not isinstance(event, (dict, list)):
        ref.bad("Notification data must be a JSON object or array")
    if len(EVENTS) >= MAX_RESOURCES:
        raise ref.ReferenceError(413, "Mock event limit reached")
    subscriptions = [key for key, value in sorted(SUBSCRIPTIONS.items())
                     if value["notificationId"] == notification_id]
    if not subscriptions:
        raise ref.ReferenceError(404, "No subscription for notificationId")
    EVENTS.append({"notificationId": notification_id, "data": deepcopy(event),
                   "subscriptionIds": subscriptions, "delivered": False})
    return 200, "message acknowledged by client"


def secret_api(s, caller_groups, method, suffix, args, query, payload):
    ref.query_values(query, {})

    def permitted(secret, write=False):
        acl = secret["secretAcls"]
        allowed = acl["owners"] if write else acl["owners"] + acl["viewers"]
        return not s.REQUIRE_AUTH or bool(set(allowed) & {s.group_email(g) for g in caller_groups})

    def visible(secret):
        return {k: deepcopy(v) for k, v in secret.items() if k != "value"}

    if suffix == "/v2/secrets/list":
        if method == "GET":
            return 200, [key for key in sorted(SECRETS) if permitted(SECRETS[key])]
        ids = strings(payload, "secret IDs", maximum=100)
        available = [visible(SECRETS[key]) for key in ids if key in SECRETS and permitted(SECRETS[key])]
        return 200, {"secrets": available,
                     "notAvailableSecrets": [key for key in ids if key not in SECRETS or not permitted(SECRETS[key])]}
    key = identifier(args[0] if args else obj(payload).get("id"))
    if args:
        existing = resource(SECRETS, key)
        if not permitted(existing, write=method != "GET"):
            raise ref.ReferenceError(403, "Secret ACL does not grant access")
        if method == "GET":
            if not existing["enabled"]:
                raise ref.ReferenceError(409, "Secret is disabled")
            return 200, deepcopy(existing)
        if method == "DELETE":
            del SECRETS[key]
            return 204, None
        if method == "PATCH":
            existing["enabled"] = suffix.endswith("/enable")
            return 202, visible(existing)
    obj(payload, {"value", "secretAcls"}, {"id", "value", "secretAcls", "metadata", "enabled"})
    if "id" in payload and payload["id"] != key:
        ref.bad("Body ID must match path ID")
    ref.text(payload["value"], "value")
    if len(payload["value"]) > 65536:
        raise ref.ReferenceError(413, "Secret value exceeds 64 KiB")
    acl = obj(payload["secretAcls"], {"owners", "viewers"}, {"owners", "viewers"})
    strings(acl["owners"], "owners", maximum=100)
    strings(acl["viewers"], "viewers", maximum=100)
    if "metadata" in payload:
        obj(payload["metadata"])
    if "enabled" in payload and type(payload["enabled"]) is not bool:
        ref.bad("enabled must be boolean")
    value = {**deepcopy(payload), "id": key, "createdBy": "osdu-lite",
             "createdAt": existing["createdAt"] if args else now(), "enabled": payload.get("enabled", True)}
    if not permitted(value, write=True):
        raise ref.ReferenceError(403, "Caller must belong to a secret owner group")
    if args:
        SECRETS[key] = value
    else:
        insert(SECRETS, key, value)
    return 200, deepcopy(value)


def legacy_extension(s, service, method, suffix, args, query, payload, caller_groups):
    if service == "dataset":
        q = ref.query_values(query, {"id"} if method == "GET" else {}, {"id"} if method == "GET" else ())
        if method == "GET":
            record = resource(s.RECORD_BY_ID, q["id"])
            if q["id"] in s.DELETED or not s.kind_matches("osdu:wks:dataset--*", record.get("kind", "")):
                raise ref.ReferenceError(404, "Dataset is deleted")
            if not record_visible(s, record, caller_groups):
                raise ref.ReferenceError(403, "Record ACL does not grant access")
            return 200, {"datasetRegistries": [deepcopy(record)]}
        with s.STORE_LOCK:
            record = resource(s.RECORD_BY_ID, args[0])
            if not s.kind_matches("osdu:wks:dataset--*", record.get("kind", "")):
                raise ref.ReferenceError(404, "Dataset metadata record not found")
            if not record_visible(s, record, caller_groups):
                raise ref.ReferenceError(403, "Record ACL does not grant access")
            s.DELETED.discard(args[0])
            s.SEARCH_BY_ID[args[0]] = deepcopy(record)
        return 200, {"datasetRegistries": [deepcopy(record)]}
    if service == "entitlements":
        q = ref.query_values(query, {"type", "cursor", "limit"} if method == "GET" else {},
                             {"type"} if method == "GET" else ())
        with s.GROUPS_LOCK:
            if method == "DELETE":
                for value in s.GROUPS.values():
                    value["members"].pop(args[0], None)
                return 204, None
            offset = ref.integer(q.get("cursor", "0"), "cursor")
            limit = ref.integer(q.get("limit", "100"), "limit", 1)
            if limit > 1000:
                ref.bad("limit must not exceed 1000")
            rows = [{"name": g["name"], "email": g["email"]} for k, g in sorted(s.GROUPS.items())]
            group_type = q["type"].upper()
            if group_type not in {"NONE", "DATA", "USER", "SERVICE"}:
                ref.bad("type must be NONE, DATA, USER or SERVICE")
            if group_type != "NONE":
                prefix = {"DATA": "data.", "USER": "users.", "SERVICE": "service."}[group_type]
                rows = [g for g in rows if g["name"].startswith(prefix)]
            end = offset + limit
            return 200, {"groups": rows[offset:end], "totalCount": len(rows),
                         "cursor": str(end) if end < len(rows) else ""}
    if service == "file":
        ref.query_values(query, {})
        obj(payload)
        if any(not isinstance(value, str) for value in payload.values()):
            ref.bad("Revoke parameters must be strings")
        # Local mock has one storage account; revoke all outstanding upload grants.
        with s.STORE_LOCK:
            s.REVOKED_URLS.update(s.UPLOAD_TOKENS)
            s.UPLOAD_TOKENS.clear()
        return 204, None
    if service == "legal":
        ref.query_values(query, {})
        for rid, record in list(s.RECORD_BY_ID.items()):
            if not all(s.is_legal_tag_valid(t) for t in record.get("legal", {}).get("legaltags", [])):
                s.soft_delete_records([rid])
        return 200, "200 OK"
    if service == "storage":
        return storage_extension(s, method, suffix, args, query, payload, caller_groups)
    if service == "workflow":
        ref.query_values(query, {})
        with s.WORKFLOW_LOCK:
            if method == "POST":
                obj(payload, {"workflowName"}, {"workflowName", "description", "registrationInstructions"})
                if "registrationInstructions" in payload:
                    instructions = obj(payload["registrationInstructions"])
                    for value in instructions.values():
                        obj(value)
                key = ref.text(payload["workflowName"], "workflowName")
                if key in s.WORKFLOW_DEFS:
                    raise ref.ReferenceError(409, "Workflow already exists")
                if len(s.WORKFLOW_DEFS) >= MAX_RESOURCES:
                    raise ref.ReferenceError(413, "Workflow mock resource limit reached")
                value = {**deepcopy(payload), "workflowId": str(uuid.uuid4()), "isSystemWorkflow": True,
                         "version": "1", "createdBy": "osdu-lite", "creationTimestamp": str(int(s.time.time() * 1000))}
                s.WORKFLOW_DEFS[key] = value
                return 200, deepcopy(value)
            workflow = resource(s.WORKFLOW_DEFS, args[0])
            if method == "DELETE":
                if not workflow.get("isSystemWorkflow"):
                    raise ref.ReferenceError(409, "Not a system workflow")
                del s.WORKFLOW_DEFS[args[0]]
                return 204, None
            run = resource(s.WORKFLOW_RUNS, args[1])
            if run.get("workflowName") != args[0]:
                raise ref.ReferenceError(404, "Workflow run not found")
            return 200, {"runId": args[1], **deepcopy(run)}
    raise ref.ReferenceError(404, "Operation not registered")


def record_visible(s, record, caller_groups):
    if not s.REQUIRE_AUTH:
        return True
    acl = record.get("acl", {})
    return bool(set(acl.get("owners", []) + acl.get("viewers", [])) &
                {s.group_email(g) for g in caller_groups})


def storage_extension(s, method, suffix, args, query, payload, caller_groups):
    q = ref.query_values(query, {"versionIds", "limit", "from"} if method == "DELETE" else {})
    if suffix == "/replay":
        obj(payload, {"operation"}, {"operation", "filter", "replayId"})
        ref.text(payload["operation"], "operation")
        if payload["operation"] not in {"replay", "reindex"}:
            ref.bad("operation must be replay or reindex")
        kinds = None
        if payload.get("filter") is not None:
            kinds = strings(obj(payload["filter"], {"kinds"}, {"kinds"})["kinds"], "kinds", maximum=100)
        key = identifier(payload.get("replayId", str(uuid.uuid4())))
        if key in REPLAYS:
            raise ref.ReferenceError(409, "Replay already exists")
        if len(REPLAYS) >= MAX_RESOURCES:
            raise ref.ReferenceError(413, "Replay mock resource limit reached")
        with s.STORE_LOCK:
            records = [deepcopy(r) for rid, r in sorted(s.RECORD_BY_ID.items())
                       if rid not in s.DELETED and (kinds is None or r["kind"] in kinds)]
            for record in records:
                s.SEARCH_BY_ID[record["id"]] = deepcopy(record)
        started = now()
        status = [{"kind": kind, "totalRecords": sum(r["kind"] == kind for r in records),
                   "processedRecords": sum(r["kind"] == kind for r in records),
                   "state": "COMPLETED", "startedAt": started, "elapsedTime": "0h 0m 0s"}
                  for kind in sorted({r["kind"] for r in records})]
        REPLAYS[key] = {"replayId": key, "operation": payload["operation"], "totalRecords": len(records),
                        "processedRecords": len(records), "overallState": "COMPLETED", "startedAt": started,
                        "elapsedTime": "0h 0m 0s", "status": status}
        if payload.get("filter") is not None:
            REPLAYS[key]["filter"] = deepcopy(payload["filter"])
        return 202, {"replayId": key}
    if suffix == "/replay/status/{id}":
        return 200, deepcopy(resource(REPLAYS, args[0]))
    with s.STORE_LOCK:
        if suffix == "/records/{id}:delete":
            resource(s.RECORD_BY_ID, args[0])
            s.soft_delete_records([args[0]])
            return 204, None
        if method == "DELETE":
            rid = args[0]
            record = resource(s.RECORD_BY_ID, rid)
            history = resource(s.RECORD_VERSIONS, rid)
            if "versionIds" in q:
                if len(q) != 1:
                    ref.bad("versionIds cannot be combined with from/limit")
                selected = {ref.integer(v, "version", 1) for v in q["versionIds"].split(",")}
            else:
                start = ref.integer(q.get("from", "1"), "from", 1)
                limit = ref.integer(q.get("limit", "100"), "limit", 1)
                if limit > 1000:
                    ref.bad("limit must not exceed 1000")
                selected = {entry["version"] for entry in history
                            if start <= entry["version"] < record["version"]}
                selected = set(sorted(selected)[:limit])
            if not selected <= {entry["version"] for entry in history}:
                raise ref.ReferenceError(404, "Version not found")
            # Preserve the latest live record: production also prohibits latest-version purge.
            if record["version"] in selected:
                ref.bad("Cannot purge the latest version")
            s.RECORD_VERSIONS[rid] = [entry for entry in history if entry["version"] not in selected]
            return 204, None
        obj(payload, {"records"}, {"records", "attributes"} if suffix.endswith("/headers") else {"records"})
        ids = strings(payload["records"], "records", maximum=1000 if suffix.endswith("/headers") else 20)
        attributes = payload.get("attributes", [])
        permitted_attributes = {"version", "kind", "acl", "legal", "ancestry", "tags",
                                "createUser", "createTime", "modifyUser", "modifyTime"}
        strings(attributes, "attributes", maximum=10, empty=True)
        if set(attributes) - permitted_attributes:
            ref.bad("Unsupported header attribute")
        records, missing = [], []
        for rid in ids:
            record = s.RECORD_BY_ID.get(rid)
            if record is None or rid in s.DELETED or not record_visible(s, record, caller_groups):
                missing.append(rid)
            elif suffix.endswith("/headers"):
                fields = set(attributes) if attributes else permitted_attributes
                records.append({k: deepcopy(v) for k, v in record.items() if k in fields | {"id"}})
            else:
                records.append(json.dumps(record, allow_nan=False))
        return 200, {"records": records, "notFound": missing,
                     **({"invalidRecords": []} if suffix.endswith("/headers") else {"conversionStatuses": []})}


def handle(s, route, method, query, payload, caller_groups):
    service, suffix, args = route
    with LOCK:
        if suffix.endswith("/info"):
            ref.query_values(query, {})
            spec = INVENTORY["services"][service]
            return 200, {"artifactId": "osdu-lite-" + service, "version": spec["version"],
                         "buildTime": "2026-10-28T00:00:00Z", "groupId": "org.osdu-lite",
                         "connectedOuterServices": [], "mock": spec["capability"] == "mock"}
        if method == "GET" and suffix.endswith(("readiness_check", "liveness_check", "warmup", "/health")):
            ref.query_values(query, {})
            return 200, {"status": "UP"} if service == "search" else "UP"
        if service in {"unit", "crs-catalog", "crs-converter"}:
            return 200, reference_extensions.handle(service, method, suffix, args, query, payload)
        if service == "partition":
            return partition_api(method, suffix, args, query, payload)
        if service == "indexer":
            return indexer_api(s, method, suffix, query, payload)
        if service == "register":
            return register_api(method, suffix, args, query, payload)
        if service == "notification":
            return notification_api(s, query, payload)
        if service == "secret":
            return secret_api(s, caller_groups, method, suffix, args, query, payload)
        return legacy_extension(s, service, method, suffix, args, query, payload, caller_groups)
