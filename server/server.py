#!/usr/bin/env python3
import csv
import datetime as _dt
import fnmatch
import hashlib
import json
import os
import sys
import threading
import time
import uuid
from copy import deepcopy
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import reference_services
import community_services
import core_profile
import core_auth
import re
from search_backend import OpenSearch, BackendError
from storage_backend import AzureBlobStorage, validate_envelope, snapshot_bytes, parse_json
from schema_registry import SchemaRegistry, SchemaError, legacy_entry
from runtime_jobs import Scheduler, Job, Outcome, CapacityError

ROOT = Path(__file__).resolve().parent
SAMPLE_HASHES = {
    "eagle01_gr_cali.las": "11ed61f91a4c2dddf17d3e0bf082461cb502a08203222090dc7123618cd52051",
    "eagle01_resistivity.las": "961eb71e367c0a0062020a9ec795961ac9f1ab6a4bfd6d7b1ef552c4a98fb73a",
    "eagle01_st1_combo.las": "78a6572dc424511992d1da07898e53e9679a0b90f2409161a4652cf3424c7119",
    "eagle02_gr_sp.las": "98816ed8fc5bb2a6692948472d39f62d9267d35449388decb8d4a63f44a1f97e",
}
DATA_DIR = Path(os.getenv("DATA_DIR", ROOT / "data"))
FILES_DIR = Path(os.getenv("FILES_DIR", ROOT / "files"))
PORT = int(os.getenv("PORT", "8089"))
PARTITION = os.getenv("OSDU_PARTITION", "company-prod")
TOKEN = os.getenv("OSDU_TOKEN", "demo-token")
REQUIRE_AUTH = os.getenv("REQUIRE_AUTH", "true").lower() not in {"0", "false", "no"}
PRELOAD_FULL_SAMPLE = os.getenv("PRELOAD_FULL_SAMPLE", "true").lower() not in {"0", "false", "no"}
INDEX_DELAY_SECONDS = float(os.getenv("INDEX_DELAY_SECONDS", "0.7"))
UPLOAD_TOKEN_TTL = float(os.getenv("UPLOAD_TOKEN_TTL", "3600"))
CURSOR_TTL = float(os.getenv("CURSOR_TTL", "300"))
MAX_CURSORS = 64
MAX_CURSOR_BYTES = 16 * 1024 * 1024
COMPAT_PROFILE = "legacy"  # Resolved from configuration at explicit runtime startup.

WELL_KIND = "osdu:wks:master-data--Well:1.4.0"
WELLBORE_KIND = "osdu:wks:master-data--Wellbore:1.4.0"
WELLLOG_KIND = "osdu:wks:work-product-component--WellLog:1.4.0"
DATASET_KIND = "osdu:wks:dataset--File.Generic:1.0.0"
SUPPORTED_KINDS = [DATASET_KIND, WELL_KIND, WELLBORE_KIND, WELLLOG_KIND]
LEGAL_TAG = f"{PARTITION}-demo-legaltag"
ACL_VIEWERS = f"data.well360.viewers@{PARTITION}.company.com"
ACL_OWNERS = f"data.well360.owners@{PARTITION}.company.com"
DEFAULT_ACL = {"viewers": [ACL_VIEWERS], "owners": [ACL_OWNERS]}

GROUPS_ALL = {
    ACL_VIEWERS, ACL_OWNERS,
    "users.datalake.viewers", "users.datalake.editors",
    "service.search.user", "service.storage.viewer", "service.storage.editor",
    "service.legal.user",
    "service.entitlements.user", "service.entitlements.admin",
    "service.storage.creator", "service.storage.admin",
    "service.file.viewers", "service.file.editors",
    "service.dataset.viewers", "service.dataset.editors",
    "service.schema-service.viewers", "service.schema-service.editors",
    "users.datalake.admins", "users.datalake.ops",
} | reference_services.GROUPS | community_services.GROUPS
community_services.reset(PARTITION)
GROUPS_BY_TOKEN = {TOKEN: set(GROUPS_ALL)} if TOKEN else {}
IDENTITIES, IDENTITY_SEEDS = {}, {}

STORE_LOCK = threading.RLock()
SCHEMA_REGISTRY = SchemaRegistry(lock=STORE_LOCK)
STATE_GENERATION = 0
RECORD_BY_ID = {}
STORAGE_BACKEND = None
FILE_BACKEND = None
FILE_RUNTIME_EPOCH = None
FILE_FINALIZATIONS = threading.BoundedSemaphore(2)
SEARCH_BACKEND = None
INDEX_ERROR = None
INDEX_RECONCILE = False
INDEX_RECONCILE_LOCK = threading.RLock()
INDEX_KIND_GENERATION = {}
INDEX_SUPPRESSED = set()
ACTIVE_INDEX_KINDS = set()
MAX_ACTIVE_INDEX_KINDS = 8
INDEX_SCHEDULER = None
RUNTIME_ACCEPTING = False
RECORD_FENCES = {}
MAX_RECORD_BYTES = 128 * 1024 * 1024
MAX_RECORD_IDS = 5000
MAX_RETAINED_VERSIONS = 1000
DELETE_ACK_TIMEOUT = 5
UPLOAD_TOKENS = {}
FILE_META = {}
CURSORS = {}
CURSOR_LOCK = threading.Lock()
WORKFLOW_RUNS = {}
WORKFLOW_LOCK = threading.RLock()
MAX_WORKFLOW_RUNS = 256
MANIFEST_DEADLINE_SECONDS = 120
WORKFLOW_DEADLINES = {}
GROUPS_LOCK = threading.RLock()
DELETED = set()
RECORD_TS = {}
RECORD_VERSIONS = {}
REVOKED_URLS = set()
REPLAY_OWNERS = {}
SCHEMAS = {}
REVISION = 0
RECORD_METADATA = {}
FILE_BINDINGS = {}
UPLOAD_GRANTS = {}
MAX_CONTROL_BYTES = 8 * 1024 * 1024

def seed_workflow_defs():
    now = time.time()
    return {
        "Osdu_ingest": {
            "workflowId": "wf-osdu-ingest-0001",
            "workflowName": "Osdu_ingest",
            "description": "Manifest based ingestion workflow",
            "version": "1.0.0",
            "createdby": "osdu-lite",
            "creationTimestamp": now,
            "registrationInstructions": {"dagName": "osdu-default"},
        },
    }

WORKFLOW_DEFS = seed_workflow_defs()

def default_legal_tag(name=None):
    return {
        "name": name or LEGAL_TAG,
        "description": "Demo LegalTag for OSDU-Lite ingestion",
        "properties": {
            "countryOfOrigin": ["SA"],
            "contractId": "No Contract Related",
            "expirationDate": "2099-12-31",
            "dataType": "Public Domain Data",
            "originator": "OSDU-Lite",
            "securityClassification": "Public",
            "exportClassification": "EAR99",
            "personalData": "No Personal Data",
            "extensionProperties": {},
        },
    }

LEGAL_TAGS = {LEGAL_TAG: default_legal_tag()}

def group_email(name):
    if "@" in name:
        return name
    return f"{name}@{PARTITION}.company.com"

def immutable_sample(filename):
    if filename not in SAMPLE_HASHES:
        return False
    target = ROOT / "files" / filename
    if not target.is_file() or target.is_symlink():
        return False
    digest = hashlib.sha256()
    with target.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest() == SAMPLE_HASHES[filename]

def seed_groups():
    groups = {}
    for name in GROUPS_ALL:
        email = group_email(name)
        groups[email] = {
            "name": name,
            "email": email,
            "description": f"{name} - entitlement group",
            "members": {"demo@osdu-lite.local": "OWNER"},
            "appIds": [],
        }
    for principal, names in IDENTITY_SEEDS.items():
        for name in names:
            email = group_email(name)
            groups.setdefault(email, {
                "name": name.split("@", 1)[0], "email": email,
                "description": "Configured local entitlement group", "members": {}, "appIds": [],
            })["members"][principal] = "MEMBER"
    return groups

GROUPS = seed_groups()

DATA_DIR.mkdir(parents=True, exist_ok=True)
FILES_DIR.mkdir(parents=True, exist_ok=True)

def read_csv(name):
    with (DATA_DIR / name).open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))

def scan_file_meta():
    for p in FILES_DIR.iterdir():
        if p.is_file():
            data = p.read_bytes()
            FILE_META[p.name] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}

def well_id(legacy):
    return f"{PARTITION}:master-data--Well:{legacy}"

def wellbore_id(legacy):
    return f"{PARTITION}:master-data--Wellbore:{legacy}"

def welllog_id(legacy):
    return f"{PARTITION}:work-product-component--WellLog:{legacy}"

def dataset_id(log_id):
    return f"{PARTITION}:dataset--File.Generic:{log_id}-LAS"

def record_base(record_id, kind, legacy_id, source):
    return {
        "id": record_id,
        "kind": kind,
        "version": 1,
        "acl": {"viewers": [ACL_VIEWERS], "owners": [ACL_OWNERS]},
        "legal": {"legaltags": [LEGAL_TAG], "otherRelevantDataCountries": ["SA"]},
        "tags": {"source-system": source, "legacy-id": legacy_id},
    }

def build_full_sample_records():
    records = []

    for row in read_csv("wells.csv"):
        rid = well_id(row["LEGACY_WELL_ID"])
        r = record_base(rid, WELL_KIND, row["LEGACY_WELL_ID"], "legacy-well-db")
        r["tags"]["field"] = row["FIELD_NAME"]
        r["data"] = {
            "FacilityName": row["WELL_NAME"],
            "FieldName": row["FIELD_NAME"],
            "Operator": row["OPERATOR"],
            "Country": row["COUNTRY"],
            "SpudDate": row["SPUD_DATE"],
            "Status": row["STATUS"],
            "SpatialLocation": {
                "Wgs84Coordinates": {
                    "type": "GeometryCollection",
                    "geometries": [{
                        "type": "Point",
                        "coordinates": [float(row["LONGITUDE"]), float(row["LATITUDE"])],
                    }],
                }
            },
        }
        records.append(r)

    for row in read_csv("wellbores.csv"):
        rid = wellbore_id(row["WELLBORE_ID"])
        r = record_base(rid, WELLBORE_KIND, row["WELLBORE_ID"], "legacy-well-db")
        r["data"] = {
            "FacilityName": row["WELLBORE_NAME"],
            "WellID": well_id(row["LEGACY_WELL_ID"]),
            "MeasuredDepthM": float(row["MD_M"]),
            "TrueVerticalDepthM": float(row["TVD_M"]),
            "WellboreType": row["TYPE"],
            "Status": row["STATUS"],
        }
        records.append(r)

    datasets = {}
    for row in read_csv("well_logs.csv"):
        did = dataset_id(row["LOG_ID"])
        ds = record_base(did, DATASET_KIND, f"{row['LOG_ID']}-LAS", "legacy-log-catalog")
        ds["tags"]["file-format"] = row["FORMAT"]
        ds["data"] = {
            "Name": row["FILE_NAME"],
            "FileName": row["FILE_NAME"],
            "Format": row["FORMAT"],
            "LegacyPath": row["LEGACY_PATH"],
        }
        datasets[did] = ds

        rid = welllog_id(row["LOG_ID"])
        r = record_base(rid, WELLLOG_KIND, row["LOG_ID"], "legacy-log-catalog")
        r["tags"]["file-format"] = row["FORMAT"]
        r["data"] = {
            "Name": f"{row['WELLBORE_ID']} {row['CURVES']}",
            "WellboreID": wellbore_id(row["WELLBORE_ID"]),
            "Datasets": [did],
            "Curves": [x.strip() for x in row["CURVES"].split("/")],
            "StartM": float(row["START_M"]),
            "StopM": float(row["STOP_M"]),
            "FileName": row["FILE_NAME"],
        }
        records.append(r)

    records.extend(datasets.values())
    return records

FULL_SAMPLE = build_full_sample_records()

def storage_state():
    return {
        "format": 2,
        "revision": REVISION,
        "records": RECORD_BY_ID,
        "versions": {
            rid: [{key: entry[key] for key in ("version", "record", "timestamp", "bytes")}
                  for entry in entries]
            for rid, entries in RECORD_VERSIONS.items() if rid in RECORD_BY_ID
        },
        "fences": {rid: list(fence) for rid, fence in RECORD_FENCES.items()},
        "timestamps": RECORD_TS,
        "deleted": sorted(DELETED),
        "suppressed": sorted(INDEX_SUPPRESSED),
        "schemas": SCHEMA_REGISTRY.export_custom(),
        "legalTags": LEGAL_TAGS,
        "groups": GROUPS,
        "recordMetadata": RECORD_METADATA,
        "fileBindings": FILE_BINDINGS,
        "uploadGrants": UPLOAD_GRANTS,
    }

def epoch_to_iso(value):
    return _dt.datetime.fromtimestamp(value, tz=_dt.timezone.utc).isoformat().replace("+00:00", "Z")


def policy_definition_in_use(value, *, legal):
    # Called under STORE_LOCK; raw history must remain recoverable even after overlays change.
    history = (entry["record"] for entries in RECORD_VERSIONS.values() for entry in entries)
    for records in (RECORD_BY_ID.values(), RECORD_METADATA.values(), history):
        for record in records:
            policy = record.get("legal" if legal else "acl", {})
            references = (policy.get("legaltags", []) if legal else
                          policy.get("owners", []) + policy.get("viewers", []))
            if value in references:
                return True
    return False


def validate_control(state, registry):
    """Validate bounded control state without consulting or mutating live globals."""
    if type(state["revision"]) is not int or state["revision"] < 0:
        raise ValueError("Invalid snapshot revision")
    for key, limit in (("groups", 256), ("legalTags", 256), ("uploadGrants", 256),
                       ("fileBindings", 5000), ("recordMetadata", 5000)):
        if not isinstance(state[key], dict):
            raise ValueError("Invalid control map")
        if len(state[key]) > limit:
            raise CapacityError("control_capacity: " + key)
    control = {key: state[key] for key in
               ("schemas", "legalTags", "groups", "recordMetadata", "fileBindings", "uploadGrants")}
    if len(snapshot_bytes(control)) > MAX_CONTROL_BYTES:
        raise CapacityError("control_byte_capacity")
    for email, group in state["groups"].items():
        if (not isinstance(group, dict) or set(group) != {"name", "email", "description", "members", "appIds"}
                or not isinstance(email, str) or not email.endswith("@" + PARTITION + ".company.com")
                or group["email"] != email or not isinstance(group["name"], str) or not group["name"]
                or not isinstance(group["description"], str) or not isinstance(group["appIds"], list)
                or any(not isinstance(app, str) for app in group["appIds"])
                or not isinstance(group["members"], dict)
                or any(not isinstance(member, str) or not member or role not in ("OWNER", "MEMBER")
                       for member, role in group["members"].items())):
            raise ValueError("Invalid entitlement group")
        if len(group["members"]) > 256:
            raise CapacityError("control_capacity: group_members")
    for name, tag in state["legalTags"].items():
        if (not isinstance(tag, dict) or set(tag) != {"name", "description", "properties"}
                or tag["name"] != name or not isinstance(name, str) or not name
                or not isinstance(tag["description"], str) or not isinstance(tag["properties"], dict)):
            raise ValueError("Invalid legal tag")
    for rid, metadata in state["recordMetadata"].items():
        if (rid not in state["records"] or not isinstance(metadata, dict)
                or set(metadata) - {"acl", "legal", "tags"}):
            raise ValueError("Invalid record metadata dependency")
        rendered = {**state["records"][rid], **metadata}
        registry.validate_record(rendered, allow_inactive=True)
        if ("acl" in metadata and any(email not in state["groups"]
                                     for email in metadata["acl"]["owners"] + metadata["acl"]["viewers"])):
            raise ValueError("Invalid metadata group dependency")
        if ("legal" in metadata and any(name not in state["legalTags"]
                                       for name in metadata["legal"]["legaltags"])):
            raise ValueError("Invalid metadata legal dependency")
    for rid, binding in state["fileBindings"].items():
        required = {"finalKey", "size", "sha256", "grantDigest", "principal",
                    "recordId", "metadataDigest", "version"}
        if (rid not in state["records"] or not isinstance(binding, dict)
                or set(binding) != required or binding["recordId"] != rid
                or type(binding["size"]) is not int or not 0 <= binding["size"] <= 256 * 1024 * 1024
                or type(binding["version"]) is not int or binding["version"] < 1
                or any(not isinstance(binding[key], str) or len(binding[key]) != 64
                       or any(c not in "0123456789abcdef" for c in binding[key])
                       for key in ("sha256", "grantDigest", "metadataDigest"))
                or not isinstance(binding["principal"], str) or not binding["principal"]
                or not isinstance(binding["finalKey"], str) or not binding["finalKey"]
                or any(c in binding["finalKey"] for c in ("?", ":", "\\"))):
            raise ValueError("Invalid file binding")
    for digest, grant in state["uploadGrants"].items():
        if (not isinstance(digest, str) or len(digest) != 64
                or any(c not in "0123456789abcdef" for c in digest)
                or not isinstance(grant, dict) or set(grant) != {"owner", "stagingKey", "expiresAt"}
                or not isinstance(grant["owner"], str) or not grant["owner"]
                or not isinstance(grant["stagingKey"], str) or not grant["stagingKey"]
                or any(c in grant["stagingKey"] for c in ("?", ":", "\\"))
                or type(grant["expiresAt"]) not in (int, float)):
            raise ValueError("Invalid upload grant")


def validate_storage(state, registry, legal_tags=None, groups=None):
    validate_envelope(state)
    for key in ("records", "versions", "fences", "timestamps"):
        if not isinstance(state[key], dict):
            raise ValueError("Invalid snapshot map")
    records, versions, fences, timestamps = (state[k] for k in ("records", "versions", "fences", "timestamps"))
    if len(fences) > 5000 or set(records) != set(versions) or set(records) != set(timestamps):
        raise ValueError("Invalid snapshot record dependencies")
    for key in ("deleted", "suppressed"):
        values = state[key]
        if (not isinstance(values, list) or any(not isinstance(rid, str) for rid in values)
                or len(values) != len(set(values)) or not set(values) <= set(fences)):
            raise ValueError("Invalid snapshot tombstones")
    unresolved = set()
    for rid, fence in fences.items():
        if (not isinstance(rid, str) or not rid.startswith(PARTITION + ":")
                or len(rid.encode("utf-8")) > 512 or not isinstance(fence, (list, tuple))
                or len(fence) != (2 if state["format"] == 1 else 3)
                or not isinstance(fence[0], str) or type(fence[1]) is not int or fence[1] < 1
                or (len(fence) == 3 and (type(fence[2]) is not int or not 1 <= fence[2] <= fence[1]))):
            raise ValueError("Invalid snapshot fence")
        if registry.get(fence[0]) is None:
            unresolved.add(fence[0])
    retained = 0
    for rid, record in records.items():
        if rid not in fences or not isinstance(record, dict) or record.get("id") != rid:
            raise ValueError("Invalid snapshot record identity")
        entries = versions[rid]
        ts = timestamps[rid]
        if (not isinstance(entries, list) or not 1 <= len(entries) <= 1000
                or not isinstance(ts, dict) or set(ts) - {"createTime", "modify"}
                or "createTime" not in ts or any(type(v) not in (int, float) for v in ts.values())):
            raise ValueError("Invalid snapshot history/timestamp")
        previous = 0
        for entry in entries:
            if (not isinstance(entry, dict) or set(entry) != {"version", "record", "timestamp", "bytes"}
                    or type(entry["version"]) is not int or not previous < entry["version"] <= fences[rid][1]
                    or type(entry["timestamp"]) not in (int, float) or type(entry["bytes"]) is not int
                    or entry["bytes"] < 0 or not isinstance(entry["record"], dict)):
                raise ValueError("Invalid retained version")
            rec = entry["record"]
            if rec.get("id") != rid or rec.get("kind") != fences[rid][0] or rec.get("version") != entry["version"]:
                raise ValueError("Invalid history identity")
            previous = entry["version"]
            actual_bytes = len(json.dumps(rec, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode())
            retained += actual_bytes
            if state["format"] == 2 and (entry["bytes"] != actual_bytes or entry["version"] > fences[rid][2]):
                raise ValueError("Invalid history byte accounting or watermark")
            if registry.get(rec["kind"]) is None:
                unresolved.add(rec["kind"])
                continue
            registry.validate_record(rec, allow_inactive=True)
            for email in rec["acl"]["owners"] + rec["acl"]["viewers"]:
                if email not in (groups if groups is not None else state["groups"]):
                    unresolved.add(email)
            for name in rec["legal"]["legaltags"]:
                if name not in (legal_tags if legal_tags is not None else state["legalTags"]):
                    unresolved.add(name)
        if record != entries[-1]["record"]:
            raise ValueError("Latest record/history mismatch")
    if retained > MAX_RECORD_BYTES:
        raise ValueError("Retained byte capacity")
    if unresolved:
        raise BackendError(503, "Unresolved snapshot dependencies: " + ", ".join(sorted(unresolved)))
    if state["format"] == 2:
        validate_control(state, registry)


def prepare_storage(state):
    """Return fully validated detached state; no search or live-map effects."""
    try:
        state = deepcopy(validate_envelope(state)) if state is not None else None
        initial = state is None or state["format"] == 1
        imported = {"schemas": [], "legalTags": {}, "groups": {}}
        if initial and os.getenv("OSDU_CORE_IMPORT_FILE"):
            path = Path(os.environ["OSDU_CORE_IMPORT_FILE"])
            if path.stat().st_size > MAX_CONTROL_BYTES:
                raise ValueError("Import byte capacity")
            imported = parse_json(path.read_bytes())
            if not isinstance(imported, dict) or set(imported) != {"schemas", "legalTags", "groups"}:
                raise ValueError("Invalid migration import")
        elif not initial and os.getenv("OSDU_CORE_IMPORT_FILE"):
            print("Storage format2: migration import was not reapplied", file=sys.stderr)
        registry = SchemaRegistry()
        registry.import_custom(imported["schemas"] if initial else state["schemas"])
        if initial:
            if not isinstance(imported["groups"], dict) or not isinstance(imported["legalTags"], dict):
                raise ValueError("Invalid migration import maps")
            control = {"schemas": registry.export_custom(),
                       "legalTags": {LEGAL_TAG: default_legal_tag(), **imported["legalTags"]},
                       "groups": {**seed_groups(), **imported["groups"]},
                       "recordMetadata": {}, "fileBindings": {}, "uploadGrants": {}, "revision": 1}
            if state is None:
                state = {"format": 2, "records": {}, "versions": {}, "fences": {},
                         "timestamps": {}, "deleted": [], "suppressed": [], **control}
            else:
                validate_storage(state, registry, control["legalTags"], control["groups"])
                for rid, entries in state["versions"].items():
                    created = epoch_to_iso(state["timestamps"][rid]["createTime"])
                    for entry in entries:
                        rec = entry["record"]
                        rec.setdefault("createUser", "osdu-lite:migrated")
                        rec.setdefault("createTime", created)
                        if rec["version"] > 1:
                            rec.setdefault("modifyUser", "osdu-lite:migrated")
                            rec.setdefault("modifyTime", epoch_to_iso(entry["timestamp"]))
                        entry["bytes"] = len(json.dumps(rec, sort_keys=True, separators=(",", ":"),
                                                       ensure_ascii=False).encode())
                    state["records"][rid] = deepcopy(entries[-1]["record"])
                state["fences"] = {rid: [fence[0], fence[1],
                                        max([fence[1]] + [e["version"] for e in state["versions"].get(rid, [])])]
                                   for rid, fence in state["fences"].items()}
                state.update(control, format=2)
        validate_storage(state, registry)
        return state, registry
    except BackendError:
        raise
    except (ValueError, TypeError, KeyError, IndexError, AttributeError, OSError, RecursionError,
            OverflowError, CapacityError) as exc:
        raise BackendError(503, "Invalid storage snapshot or migration import") from exc


def restore_storage(state, registry):
    global REVISION, SCHEMA_REGISTRY
    for target, key in ((RECORD_BY_ID, "records"), (RECORD_TS, "timestamps"),
                        (LEGAL_TAGS, "legalTags"), (GROUPS, "groups"),
                        (RECORD_METADATA, "recordMetadata"), (FILE_BINDINGS, "fileBindings"),
                        (UPLOAD_GRANTS, "uploadGrants")):
        target.clear()
        target.update(deepcopy(state[key]))
    RECORD_VERSIONS.clear()
    RECORD_VERSIONS.update({rid: [dict(deepcopy(entry), pins=0) for entry in entries]
                            for rid, entries in state["versions"].items()})
    RECORD_FENCES.clear()
    RECORD_FENCES.update({rid: tuple(fence) for rid, fence in state["fences"].items()})
    DELETED.clear()
    DELETED.update(state["deleted"])
    INDEX_SUPPRESSED.clear()
    INDEX_SUPPRESSED.update(state["suppressed"])
    REVISION = state["revision"]
    # Keep the canonical store lock even though compilation happened detached.
    registry._lock = STORE_LOCK
    SCHEMA_REGISTRY = registry

@contextmanager
def storage_transaction(include_workflows=False, force=False):
    """Caller holds STORE_LOCK; publish index work only after this commits."""
    global REVISION
    with WORKFLOW_LOCK:
        maps = [RECORD_BY_ID, RECORD_VERSIONS, RECORD_FENCES, RECORD_TS,
                LEGAL_TAGS, GROUPS, RECORD_METADATA, FILE_BINDINGS, UPLOAD_GRANTS]
        before = [dict(RECORD_BY_ID), {rid: list(entries) for rid, entries in RECORD_VERSIONS.items()},
                  dict(RECORD_FENCES), deepcopy(RECORD_TS),
                  deepcopy(LEGAL_TAGS), deepcopy(GROUPS), deepcopy(RECORD_METADATA),
                  deepcopy(FILE_BINDINGS), deepcopy(UPLOAD_GRANTS)]
        old_revision = REVISION
        old_schemas = SCHEMA_REGISTRY.export_custom()
        old_state = deepcopy(storage_state())
        entries_before = [(entry, dict(entry)) for entries in RECORD_VERSIONS.values() for entry in entries]
        record_objects = {id(record): record for record in
                          list(RECORD_BY_ID.values()) + [entry["record"] for entry, _ in entries_before]}
        records_before = [(record, deepcopy(record)) for record in record_objects.values()]
        if include_workflows:
            maps += [WORKFLOW_RUNS, WORKFLOW_DEADLINES]
            before += [deepcopy(WORKFLOW_RUNS), dict(WORKFLOW_DEADLINES)]
        sets = [DELETED, INDEX_SUPPRESSED]
        old_sets = [set(value) for value in sets]
        try:
            yield
            state = storage_state()
            if force or state != old_state:
                REVISION = old_revision + 1
                state["revision"] = REVISION
                try:
                    validate_control(state, SCHEMA_REGISTRY)
                    validate_envelope(state)
                except (ValueError, TypeError) as exc:
                    raise BackendError(503, "Invalid authoritative control state") from exc
                STORAGE_BACKEND.save(state)
        except Exception:
            # Jobs retain entry/payload references; restore in place rather than replacing pinned objects.
            for record, previous in records_before:
                record.clear()
                record.update(previous)
            for entry, previous in entries_before:
                entry.clear()
                entry.update(previous)
            for target, previous in zip(maps + sets, before + old_sets):
                target.clear()
                target.update(previous)
            REVISION = old_revision
            if SCHEMA_REGISTRY.export_custom() != old_schemas:
                SCHEMA_REGISTRY.import_custom(old_schemas)
            raise

def _clear_state(*, principal):
    global STATE_GENERATION, INDEX_ERROR, INDEX_RECONCILE, INDEX_SCHEDULER, RUNTIME_ACCEPTING
    with STORE_LOCK:
        for record in RECORD_BY_ID.values():
            require_write(record, principal=principal, deleted=True)
        RUNTIME_ACCEPTING = False
    if INDEX_SCHEDULER is not None:
        if not INDEX_SCHEDULER.stop():
            raise BackendError(503, "runtime_shutdown_pending")
    with STORE_LOCK, storage_transaction(force=True):
        for record in RECORD_BY_ID.values():
            require_write(record, principal=principal, deleted=True)
        STATE_GENERATION += 1
        INDEX_ERROR = None
        INDEX_RECONCILE = False
        INDEX_KIND_GENERATION.clear()
        INDEX_SUPPRESSED.clear()
        RECORD_BY_ID.clear()
        UPLOAD_TOKENS.clear()
        RECORD_VERSIONS.clear()
        RECORD_FENCES.clear()
        DELETED.clear()
        RECORD_TS.clear()
        GROUPS.clear()
        GROUPS.update(seed_groups())
        LEGAL_TAGS.clear()
        LEGAL_TAGS[LEGAL_TAG] = default_legal_tag()
        SCHEMA_REGISTRY.reset()
        RECORD_METADATA.clear()
        FILE_BINDINGS.clear()
        UPLOAD_GRANTS.clear()
    try:
        ACTIVE_INDEX_KINDS.clear()
        if hasattr(SEARCH_BACKEND, "configure_registry"):
            SEARCH_BACKEND.configure_registry(SCHEMA_REGISTRY)
        SEARCH_BACKEND.start_epoch(uuid.uuid4().hex)
    except BackendError as exc:
        INDEX_ERROR = exc
        raise
    INDEX_SCHEDULER = Scheduler(_execute_index, sweep=sweep_workflow_runs)
    INDEX_SCHEDULER.start()
    RUNTIME_ACCEPTING = True
    with CURSOR_LOCK:
        CURSORS.clear()
    with WORKFLOW_LOCK:
        WORKFLOW_RUNS.clear()
        WORKFLOW_DEADLINES.clear()
        WORKFLOW_DEFS.clear()
        WORKFLOW_DEFS.update(seed_workflow_defs())
    REVOKED_URLS.clear()
    REPLAY_OWNERS.clear()
    SCHEMAS.clear()
    community_services.reset(PARTITION)

def iso_to_epoch(value):
    if value is None:
        return None
    try:
        text = str(value).strip().replace("Z", "+00:00")
        dt = _dt.datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        return dt.timestamp()
    except (ValueError, TypeError):
        return None

def legal_tag_raw(name):
    return LEGAL_TAGS.get(name)

LEGAL_TAG_PROPERTIES = [
    {"propertyName": "countryOfOrigin", "allowedValues": ["SA", "US", "GB", "NO"]},
    {"propertyName": "contractId", "allowedValues": ["Unknown", "No Contract Related", "No Contract"]},
    {"propertyName": "dataType",
     "allowedValues": ["Public Domain Data", "Private Data", "Commercially Seismic", "Other Proprietary"]},
    {"propertyName": "expirationDate", "allowedValues": None},
    {"propertyName": "originator", "allowedValues": None},
    {"propertyName": "securityClassification",
     "allowedValues": ["Public", "Restricted", "Confidential", "Secret"]},
    {"propertyName": "exportClassification", "allowedValues": ["EAR99"]},
    {"propertyName": "personalData", "allowedValues": ["No Personal Data", "Personal Data"]},
    {"propertyName": "extensionProperties", "allowedValues": None},
]

def is_legal_tag_valid(name):
    return core_auth.valid_legal_tag(LEGAL_TAGS.get(name))

def group_for_caller(handler):
    if handler.principal is core_auth.SYSTEM:
        return set()
    return principal_groups(handler.principal)


def principal_groups(principal):
    if principal is core_auth.ANONYMOUS:
        return set()
    with STORE_LOCK, GROUPS_LOCK:
        groups = core_auth.effective_groups(principal, GROUPS, PARTITION)
        # Backward-compatible in-process restriction hook; it can only remove authority.
        ceiling = GROUPS_BY_TOKEN.get(TOKEN)
        if principal.name == "demo@osdu-lite.local" and ceiling is not None and ceiling != GROUPS_ALL:
            groups &= set(ceiling) | {group_email(g) for g in ceiling}
        return groups


def effective_record(record):
    result = deepcopy(record)
    if COMPAT_PROFILE == "core":
        # Early format2 snapshots can predate Storage audit ownership. Render
        # missing identity/time deterministically; a read is never a mutation.
        timestamps = RECORD_TS.get(record["id"], {})
        result.setdefault("createUser", "osdu-lite:migrated")
        if "createTime" in timestamps:
            result.setdefault("createTime", epoch_to_iso(timestamps["createTime"]))
        if record.get("version", 1) > 1:
            result.setdefault("modifyUser", "osdu-lite:migrated")
            if "modifyTime" not in result:
                entry = next((e for e in RECORD_VERSIONS.get(record["id"], [])
                              if e["version"] == record["version"]), None)
                if entry is not None:
                    result["modifyTime"] = epoch_to_iso(entry["timestamp"])
    current = RECORD_BY_ID.get(record["id"], record)
    for field in ("acl", "legal", "tags"):
        result.pop(field, None)
        if field in current:
            result[field] = deepcopy(current[field])
    result.update(deepcopy(RECORD_METADATA.get(record["id"], {})))
    return result


def readable(record, *, principal, owner=False, deleted=False):
    if not record or (record["id"] in DELETED and not deleted):
        return False
    if principal is core_auth.SYSTEM:
        return True
    return core_auth.can_read(effective_record(record), principal_groups(principal),
                              is_legal_tag_valid, owner=owner)


def read_record(rid, *, principal, owner=False, deleted=False):
    with STORE_LOCK:
        record = RECORD_BY_ID.get(rid)
        if not readable(record, principal=principal, owner=owner, deleted=deleted):
            return None
        return effective_record(record)


def _file_identity_fields(data):
    protected = {"filesource", "filesize", "filesizebytes", "filehash", "checksum",
                 "checksumalgorithm", "sha256", "size", "hash", "finalkey", "blobidentity", "blobkey"}
    fields = {}
    def collect(value, path):
        if isinstance(value, dict):
            for key, child in value.items():
                if key.lower() in protected:
                    fields[path + (key,)] = core_profile.canonical(child)
                else:
                    collect(child, path + (key,))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                collect(child, path + (index,))
    collect(data, ())
    return fields


def require_write(record, *, principal, existing=True, deleted=False, publishing=False):
    if principal is core_auth.SYSTEM:
        return
    if existing:
        current = read_record(record["id"], principal=principal, deleted=deleted)
        if current is None:
            raise BackendError(404, "Record not found")
        if not readable(current, principal=principal, owner=True, deleted=deleted):
            raise BackendError(403, "Record ownership required")
    core_auth.validate_acl(record, GROUPS, PARTITION)
    validate_record(record)
    if not core_auth.can_write(record, principal_groups(principal), is_legal_tag_valid):
        raise BackendError(403, "Resulting record ownership required")
    if record["id"] in FILE_BINDINGS:
        current = RECORD_BY_ID.get(record["id"])
        if current is None or _file_identity_fields(current["data"]) != _file_identity_fields(record["data"]):
            raise BackendError(409, "immutable_file_binding")
        return
    if publishing and record["kind"] == DATASET_KIND:
        return
    if (COMPAT_PROFILE == "core" and record["kind"] == DATASET_KIND
            and not immutable_sample(record_file_name(record))):
        raise BackendError(400, "Use File publication or Dataset registration for an unbound file")
    if kind_matches("*:dataset--*:*", record["kind"]):
        filename = record_file_name(record)
        if filename:
            data = record.get("data", {})
            source = (data.get("FileSource") or ((data.get("DatasetProperties") or {})
                      .get("FileSourceInfo") or {}).get("FileSource"))
            if source and (not isinstance(source, str) or Path(source).name != source
                           or "\\" in source or "://" in source or source in {".", ".."}):
                raise BackendError(400, "Invalid FileSource")
            grant_owner = FILE_META.get(filename, {}).get("principal")
            current = RECORD_BY_ID.get(record["id"])
            same_source = current is not None and record_file_name(current) == filename
            if (not same_source and filename in FILE_META and (FILES_DIR / filename).exists()
                    and not grant_owner and not immutable_sample(filename)
                    and not any(record_file_name(other) == filename and readable(
                        other, principal=principal, owner=True, deleted=True)
                        for other in RECORD_BY_ID.values())):
                raise BackendError(400, "Invalid FileSource")
            if grant_owner and grant_owner != principal.name and not same_source:
                raise BackendError(400, "Invalid FileSource")
            for other in RECORD_BY_ID.values():
                if other["id"] != record["id"] and record_file_name(other) == filename:
                    if not readable(other, principal=principal, owner=True, deleted=True):
                        raise BackendError(400, "Invalid FileSource")


def authorized_upsert(records, *, principal, **kwargs):
    return _upsert_records(records, principal=principal, **kwargs)


def authorized_delete(ids, *, principal, hard=False):
    with STORE_LOCK:
        for rid in ids:
            record = read_record(rid, principal=principal, deleted=True)
            if record is None:
                raise BackendError(404, "Record not found")
            require_write(record, principal=principal, deleted=True)
    return _delete_records(ids, hard=hard, principal=principal)

def validate_record(rec):
    SCHEMA_REGISTRY.validate_record(rec)
    if COMPAT_PROFILE == "core" and SCHEMA_REGISTRY.get(rec["kind"])["status"] != "PUBLISHED":
        raise BackendError(409, "schema_not_published")
    rid = rec.get("id")
    legal = rec.get("legal")
    for tag in legal["legaltags"]:
        if tag not in LEGAL_TAGS:
            raise ValueError(f"Record {rid}: unknown LegalTag '{tag}' (registered: {sorted(LEGAL_TAGS)})")
        if not is_legal_tag_valid(tag):
            raise ValueError(f"Record {rid}: LegalTag '{tag}' is not valid (expired)")

def check_index_kinds(kinds):
    if len(ACTIVE_INDEX_KINDS | set(kinds)) > MAX_ACTIVE_INDEX_KINDS:
        raise CapacityError("active_kind_capacity")


@contextmanager
def index_kind_admission(kinds):
    """STORE_LOCK serializes provisional slots with the snapshot transaction.

    Slots become active only on successful authoritative admission and remain
    reserved when an index is empty/unavailable, until reset or startup rebuild.
    No engine I/O occurs before the durable commit.
    """
    kinds = set(kinds)
    check_index_kinds(kinds)
    added = kinds - ACTIVE_INDEX_KINDS
    ACTIVE_INDEX_KINDS.update(added)
    try:
        yield
    except BaseException:
        ACTIVE_INDEX_KINDS.difference_update(added)
        raise


def _execute_index(job):
    # Reconciliation and normal writes share the same bounded engine boundary.
    # In particular, a rebuild must not overlap a same-ID scheduled operation.
    with INDEX_RECONCILE_LOCK:
        return _execute_index_locked(job)

def _execute_index_locked(job):
    global INDEX_ERROR
    record = job.payload
    with STORE_LOCK:
        fence = RECORD_FENCES.get(job.key)
        if fence is None or tuple(fence) != job.fence:
            return Outcome(False, False, "superseded")
    try:
        ensure_index()
        with STORE_LOCK:
            if tuple(RECORD_FENCES.get(job.key, ())) != job.fence:
                return Outcome(False, False, "superseded")
            # Inactive tombstone-only kinds need no new shard after an epoch
            # rebuild. Their durable fences still reject every stale job.
            if record["kind"] not in ACTIVE_INDEX_KINDS:
                return Outcome() if record.get("_deleted") else Outcome(False, False, "superseded")
            indexed = dict(record, _generation=job.fence[1])
        results = SEARCH_BACKEND.bulk([indexed])
        outcome = results[0]
    except BackendError as exc:
        outcome = Outcome(False, getattr(exc, "retryable", exc.status == 503), "engine_unavailable")
    with STORE_LOCK:
        if outcome.success and tuple(RECORD_FENCES.get(job.key, ())) != job.fence:
            outcome = Outcome(False, False, "superseded")
        if outcome.success:
            INDEX_ERROR = None
        else:
            INDEX_ERROR = BackendError(503, outcome.error)
    return outcome


def _retained_bytes():
    return sum(entry["bytes"] for versions in RECORD_VERSIONS.values() for entry in versions)


def _patch_records(ids, ops, *, principal, data_patch):
    result = {"recordIds": [], "notFoundRecordIds": [], "failedRecordIds": [],
              "errors": [], "recordCount": 0}
    if not data_patch:
        result.update(unAuthorizedRecordIds=[], lockedRecordIds=[])
    staged = []
    with STORE_LOCK:
        now = time.time()
        if INDEX_SCHEDULER is None or not RUNTIME_ACCEPTING:
            raise BackendError(503, "runtime_not_started")
        retained = _retained_bytes()
        for rid in ids:
            current = read_record(rid, principal=principal)
            if current is None:
                result["notFoundRecordIds"].append(rid)
                continue
            if not readable(current, principal=principal, owner=True):
                result["failedRecordIds" if data_patch else "unAuthorizedRecordIds"].append(rid)
                result["errors"].append(f"{rid}: patch_failed")
                continue
            try:
                require_write(current, principal=principal)
                final = core_profile.apply_patch(current, ops)
                final.setdefault("tags", {})
                data_changed = core_profile.canonical(final.get("data")) != core_profile.canonical(current["data"])
                metadata_changed = any(core_profile.canonical(final.get(k, {})) != core_profile.canonical(current.get(k, {}))
                                       for k in ("acl", "legal", "tags"))
                if data_changed:
                    final.update(version=RECORD_FENCES[rid][2] + 1,
                                 modifyUser=principal.name, modifyTime=epoch_to_iso(now))
                size = len(snapshot_bytes(final))
                if size > 65536:
                    raise BackendError(413, "record_byte_limit")
                for field in ("tags",):
                    if field in final:
                        core_profile.metadata_value([field], final[field])
                require_write(final, principal=principal)
                validate_record(final)
                # An overlay is rendered on *every* retained version.
                for entry in RECORD_VERSIONS[rid]:
                    historical = effective_record(entry["record"])
                    for field in ("acl", "legal", "tags"):
                        historical.pop(field, None)
                        if field in final:
                            historical[field] = final[field]
                    if len(snapshot_bytes(historical)) > 65536:
                        raise BackendError(413, "record_byte_limit")
                if not data_changed and not metadata_changed:
                    continue
                if data_changed:
                    if len(RECORD_VERSIONS[rid]) >= MAX_RETAINED_VERSIONS:
                        raise CapacityError("record_version_capacity")
                    if retained + size > MAX_RECORD_BYTES:
                        raise CapacityError("record_capacity")
                staged.append((final, size, data_changed))
                if data_changed:
                    retained += size
                result["recordIds"].append(rid)
            except (ValueError, BackendError, SchemaError, CapacityError) as exc:
                result["failedRecordIds"].append(rid)
                # No backend text or foreign-record details in per-record errors.
                result["errors"].append(f"{rid}: patch_failed")
        if staged:
            kinds = {record["kind"] for record, _, _ in staged}
            check_index_kinds(kinds)
            reservation = INDEX_SCHEDULER.reserve("index", len(staged))
            try:
                with index_kind_admission(kinds), storage_transaction():
                    jobs = []
                    for record, size, data_changed in staged:
                        rid = record["id"]
                        kind, generation, watermark = RECORD_FENCES[rid]
                        RECORD_FENCES[rid] = (kind, generation + 1,
                                              record["version"] if data_changed else watermark)
                        # Missing tags normalize to an empty record-wide map.
                        RECORD_METADATA[rid] = {field: deepcopy(record.get(field, {}))
                                                for field in ("acl", "legal", "tags")}
                        if data_changed:
                            RECORD_BY_ID[rid] = record
                            entry = {"version": record["version"], "record": record,
                                     "timestamp": now, "bytes": size, "pins": 1}
                            RECORD_VERSIONS[rid].append(entry)
                            RECORD_TS[rid]["modify"] = now
                        else:
                            entry = next(e for e in RECORD_VERSIONS[rid]
                                         if e["version"] == record["version"])
                            entry["pins"] += 1
                        INDEX_SUPPRESSED.discard(rid)
                        job, _, _ = _index_job(effective_record(record), entry)
                        jobs.append(job)
                reservation.submit(jobs)
            finally:
                reservation.cancel()
        result["recordCount"] = len(staged)
    partial = any(result[k] for k in ("notFoundRecordIds", "failedRecordIds")) or result.get("unAuthorizedRecordIds")
    return (206 if partial else 200), result


def _index_job(record, entry=None, run_id=None, deadline=float("inf")):
    done = threading.Event()
    result = []
    def settled(outcome):
        with STORE_LOCK:
            if entry is not None:
                entry["pins"] -= 1
                entry["outcome"] = outcome
                rid = record["id"]
                if rid not in RECORD_BY_ID:
                    RECORD_VERSIONS[rid] = [e for e in RECORD_VERSIONS.get(rid, []) if e["pins"]]
                    if not RECORD_VERSIONS[rid]:
                        RECORD_VERSIONS.pop(rid)
            result.append(outcome)
            done.set()
        if run_id:
            settle_workflow_item(run_id, record["id"], outcome)
    job = Job(record["id"], deepcopy(record), deadline=deadline, terminal=settled,
              started=(lambda: start_workflow_item(run_id)) if run_id else None,
              fence=tuple(RECORD_FENCES[record["id"]]))
    return job, done, result

def file_expiry(value="1H"):
    match = re.fullmatch(r"([0-9]{1,6})([MHD])", value.upper()) if isinstance(value, str) else None
    seconds = int(match[1]) * {"M": 60, "H": 3600, "D": 86400}[match[2]] if match else 0
    if not 60 <= seconds <= 3600:
        raise BackendError(400, "expiryTime must be between 1M and 1H")
    return seconds


def reclaim_file_grants():
    with STORE_LOCK:
        expired = [key for key, grant in UPLOAD_GRANTS.items() if grant["expiresAt"] <= time.time()]
        if expired and STORAGE_BACKEND is not None:
            with storage_transaction():
                for key in expired:
                    UPLOAD_GRANTS.pop(key)


def issue_file_grant(principal, expiry, *, legacy_host=None):
    with STORE_LOCK:
        if not RUNTIME_ACCEPTING or FILE_BACKEND is None:
            raise BackendError(503, "file_backend_unavailable")
        reclaim_file_grants()
        if len(UPLOAD_GRANTS) >= 256:
            raise CapacityError("upload_grant_capacity")
        identifier = uuid.uuid4().hex
        key = hashlib.sha256(PARTITION.encode()).hexdigest() + "/staging/" + identifier
        token = uuid.uuid4().hex if legacy_host is not None else key
        digest = hashlib.sha256(token.encode()).hexdigest()
        expires = time.time() + expiry
        with storage_transaction():
            UPLOAD_GRANTS[digest] = {"owner": principal.name, "stagingKey": key, "expiresAt": expires}
        url = (f"http://{legacy_host}/upload/{token}" if legacy_host is not None
               else FILE_BACKEND.signed_url(key, expires, write=True))
        return {"FileID": identifier, "Location": {"FileSource": key, "SignedUrl": url}}


def file_source(record):
    data = record.get("data", {})
    return data.get("FileSource") or ((data.get("DatasetProperties") or {}).get("FileSourceInfo") or {}).get("FileSource")


def publish_file_records(records, *, principal):
    if not isinstance(records, list) or not 1 <= len(records) <= 20:
        raise ValueError("Expected 1..20 file records")
    if not FILE_FINALIZATIONS.acquire(blocking=False):
        raise CapacityError("file_finalization_capacity")
    try:
        candidates, seen, grants = [], set(), set()
        with STORE_LOCK:
            backend, epoch = FILE_BACKEND, FILE_RUNTIME_EPOCH
            if not RUNTIME_ACCEPTING or backend is None:
                raise BackendError(503, "file_backend_unavailable")
            for incoming in records:
                if (not isinstance(incoming, dict) or incoming.get("kind") != DATASET_KIND
                        or not isinstance(incoming.get("data"), dict)):
                    raise ValueError("Expected File.Generic record")
                record = deepcopy(incoming)
                if record.get("id") in RECORD_BY_ID:
                    current = read_record(record["id"], principal=principal)
                    if current is None:
                        raise BackendError(404, "Record not found")
                    require_write(current, principal=principal)
                source = file_source(record)
                prefix = hashlib.sha256(PARTITION.encode()).hexdigest() + "/staging/"
                if (not isinstance(source, str) or not source.startswith(prefix)
                        or not re.fullmatch(r"[0-9a-f]{32}", source[len(prefix):])):
                    raise BackendError(400, "invalid_file_source")
                record.setdefault("id", f"{PARTITION}:dataset--File.Generic:{source.rsplit('/', 1)[1]}")
                core_profile.record_id(record["id"], PARTITION)
                if set(record) - (core_profile.USER_FIELDS | core_profile.AUDIT_FIELDS | {"id"}):
                    raise ValueError("Unknown record field")
                binding = FILE_BINDINGS.get(record["id"])
                if binding:
                    # Consumed legacy grants no longer exist. Derive their digest only
                    # after checking principal, record authority and the original source.
                    current = read_record(record["id"], principal=principal)
                    if (binding["principal"] != principal.name or current is None
                            or file_source(current) != source):
                        raise BackendError(400, "invalid_file_source")
                    require_write(current, principal=principal)
                    digest = binding["grantDigest"]
                else:
                    digest = next((key for key, grant in UPLOAD_GRANTS.items()
                                   if grant["stagingKey"] == source), None)
                    if digest is None:
                        raise BackendError(400, "invalid_file_source")
                metadata = core_profile.canonical({"id": record["id"], **core_profile.user_fields(record)})
                metadata_digest = hashlib.sha256(metadata.encode()).hexdigest()
                if record["id"] in seen or digest in grants:
                    raise ValueError("Duplicate record or grant")
                seen.add(record["id"])
                grants.add(digest)
                binding = FILE_BINDINGS.get(record["id"])
                if binding:
                    if binding["principal"] != principal.name or binding["grantDigest"] != digest:
                        raise BackendError(400, "invalid_file_source")
                    current = read_record(record["id"], principal=principal)
                    if current is None:
                        raise BackendError(404, "Record not found")
                    require_write(current, principal=principal)
                    if binding["metadataDigest"] != metadata_digest:
                        raise BackendError(409, "consumed_upload_grant")
                    candidates.append((record, digest, metadata_digest, None))
                    continue
                if record["id"] in RECORD_BY_ID:
                    raise BackendError(409, "file_record_already_exists")
                grant = UPLOAD_GRANTS.get(digest)
                if not grant or grant["owner"] != principal.name or grant["expiresAt"] <= time.time():
                    raise BackendError(400, "invalid_file_source")
                check = dict(record, version=1)
                require_write(check, principal=principal, existing=False, publishing=True)
                candidates.append((record, digest, metadata_digest, dict(grant)))
        publications = {}
        for record, digest, metadata_digest, grant in candidates:
            if grant is None:
                continue
            final_key = hashlib.sha256(PARTITION.encode()).hexdigest() + "/final/" + uuid.uuid4().hex
            published = backend.publish(grant["stagingKey"], final_key)
            for name in ("FileSize", "FileSizeBytes", "size"):
                if name in record["data"] and record["data"][name] != published["size"]:
                    raise BackendError(400, "file_size_mismatch")
            for name in ("sha256", "FileHash", "Checksum"):
                if name in record["data"] and record["data"][name] != published["sha256"]:
                    raise BackendError(400, "file_hash_mismatch")
            publications[record["id"]] = {
                **published, "grantDigest": digest, "principal": principal.name,
                "recordId": record["id"], "metadataDigest": metadata_digest, "version": 1}
        with STORE_LOCK:
            if not RUNTIME_ACCEPTING or FILE_RUNTIME_EPOCH != epoch or FILE_BACKEND is not backend:
                raise BackendError(503, "file_runtime_changed")
            admission = []
            for record, digest, metadata_digest, grant in candidates:
                # Another finalization may have consumed this grant while I/O was outside the lock.
                binding = FILE_BINDINGS.get(record["id"])
                if binding:
                    current = read_record(record["id"], principal=principal)
                    if (current is None or binding["principal"] != principal.name
                            or binding["grantDigest"] != digest):
                        raise BackendError(400, "invalid_file_source")
                    require_write(current, principal=principal)
                    if binding["metadataDigest"] != metadata_digest:
                        raise BackendError(409, "consumed_upload_grant")
                    continue
                live = UPLOAD_GRANTS.get(digest)
                if live != grant or live is None or live["expiresAt"] <= time.time():
                    raise BackendError(400, "invalid_file_source")
                admission.append(record)
            if admission:
                _upsert_records(admission, principal=principal, file_publications=publications)
            return [effective_record(RECORD_BY_ID[record["id"]]) for record, *_ in candidates]
    finally:
        FILE_FINALIZATIONS.release()


def _upsert_records(records, index_immediately=False, run_id=None, *, principal,
                   copy_source=None, skipdupes=False, skipped=None, file_publications=None):
    out, prepared, waits = [], [], []
    with STORE_LOCK:
        now = time.time()
        if INDEX_SCHEDULER is None or (not RUNTIME_ACCEPTING and principal is not core_auth.SYSTEM):
            raise BackendError(503, "runtime_not_started")
        if not isinstance(records, list) or len(records) > 100:
            raise ValueError("Expected at most 100 records")
        if copy_source is not None:
            current = read_record(copy_source, principal=principal)
            if current is None:
                raise BackendError(404, "Record not found")
            require_write(current, principal=principal)
        ids = set()
        retained = _retained_bytes()
        for incoming in records:
            rec = deepcopy(incoming)
            if not isinstance(rec, dict):
                raise ValueError("Record must be an object")
            if not isinstance(rec.get("kind"), str) or not rec["kind"]:
                raise ValueError("Record kind must be a nonempty string")
            if COMPAT_PROFILE == "core":
                if set(rec) - (core_profile.USER_FIELDS | core_profile.AUDIT_FIELDS | {"id"}):
                    raise ValueError("Unknown record field")
                if "id" not in rec:
                    kind = rec.get("kind")
                    if not isinstance(kind, str) or len(kind.split(":")) != 4:
                        raise ValueError("Full kind required")
                    rec["id"] = f"{PARTITION}:{kind.split(':')[2]}:{uuid.uuid4().hex}"
                core_profile.record_id(rec["id"], PARTITION)
                if "tags" in rec:
                    core_profile.metadata_value(["tags"], rec["tags"])
                if (("ancestry" in rec and not isinstance(rec["ancestry"], dict))
                        or ("meta" in rec and not isinstance(rec["meta"], list))):
                    raise ValueError("Invalid optional metadata")
            elif not isinstance(rec.get("id"), str):
                raise ValueError("Record ID must be a string")
            if COMPAT_PROFILE == "legacy" and "version" in rec and (
                    type(rec["version"]) is not int or rec["version"] < 1):
                raise ValueError("Invalid record version")
            for field in core_profile.AUDIT_FIELDS:
                rec.pop(field, None)
            rid = rec["id"]
            if rid in ids:
                raise ValueError("Duplicate record ID in batch")
            ids.add(rid)
            if principal is not core_auth.SYSTEM and rid in RECORD_BY_ID:
                current = read_record(rid, principal=principal, deleted=True)
                if current is None:
                    raise BackendError(404, "Record not found")
                if not readable(current, principal=principal, owner=True, deleted=True):
                    raise BackendError(403, "Record ownership required")
            fence = RECORD_FENCES.get(rid)
            if fence and fence[0] != rec["kind"]:
                raise BackendError(409, "record_kind_conflict")
            previous = RECORD_BY_ID.get(rid)
            if skipdupes and previous and rid not in DELETED:
                current = effective_record(previous)
                if (core_profile.canonical(core_profile.user_fields(current)) ==
                        core_profile.canonical(core_profile.user_fields(rec))):
                    validate_record(current)
                    if principal is not core_auth.SYSTEM:
                        require_write(current, principal=principal, existing=True, deleted=True)
                    if skipped is not None:
                        skipped.append(rid)
                    continue
            rec["version"] = fence[2] + 1 if fence else 1
            rec["createUser"] = previous.get("createUser", "osdu-lite:migrated") if previous else principal.name
            created = RECORD_TS.get(rid, {}).get("createTime", now)
            rec["createTime"] = previous.get("createTime", epoch_to_iso(created)) if previous else epoch_to_iso(now)
            if previous or fence:
                rec.update(modifyUser=principal.name, modifyTime=epoch_to_iso(now))
            size = len(json.dumps(rec, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                                  allow_nan=False).encode("utf-8"))
            if size > 65536:
                raise BackendError(413, "record_byte_limit")
            validate_record(rec)
            if principal is not core_auth.SYSTEM:
                require_write(rec, principal=principal, existing=rid in RECORD_BY_ID, deleted=True,
                              publishing=bool(file_publications and rid in file_publications))
            versions = RECORD_VERSIONS.get(rid, [])
            for entry in versions:
                rendered = effective_record(entry["record"])
                for field in ("acl", "legal", "tags"):
                    rendered.pop(field, None)
                    if field in rec:
                        rendered[field] = rec[field]
                if len(snapshot_bytes(rendered)) > 65536:
                    raise BackendError(413, "record_byte_limit")
            if len(versions) >= MAX_RETAINED_VERSIONS:
                raise CapacityError("record_version_capacity")
            retained += size
            prepared.append((rec, size))
        if len(set(RECORD_FENCES) | ids) > MAX_RECORD_IDS or retained > MAX_RECORD_BYTES:
            raise CapacityError("record_capacity")
        if not prepared:
            return out
        kinds = {rec["kind"] for rec, _ in prepared}
        check_index_kinds(kinds)
        reservation = INDEX_SCHEDULER.reserve("index", len(prepared))
        try:
            with index_kind_admission(kinds), storage_transaction(include_workflows=bool(run_id)):
                deadline = float("inf")
                if run_id:
                    with WORKFLOW_LOCK:
                        expired = workflow_evictions(now)
                        survivors = [rid for rid in WORKFLOW_RUNS if rid not in expired]
                        if len(survivors) >= MAX_WORKFLOW_RUNS:
                            terminal = [rid for rid in survivors
                                        if WORKFLOW_RUNS[rid]["status"] in ("succeeded", "failed")]
                            terminal.sort(key=lambda rid: WORKFLOW_RUNS[rid]["completedAt"])
                            if not terminal:
                                raise CapacityError("workflow_run_capacity")
                            expired.append(terminal[0])
                        for rid in expired:
                            WORKFLOW_RUNS.pop(rid, None)
                            WORKFLOW_DEADLINES.pop(rid, None)
                        deadline = INDEX_SCHEDULER.clock() + MANIFEST_DEADLINE_SECONDS
                        WORKFLOW_DEADLINES[run_id] = deadline
                        identities = [{"id": rec["id"], "version": rec["version"]} for rec, _ in prepared]
                        WORKFLOW_RUNS[run_id] = {
                            "principal": principal.name,
                            "workflowName": "Osdu_ingest", "status": "submitted",
                            "recordCount": len(prepared), "recordIds": identities,
                            "indexedCount": 0, "failedCount": 0, "pendingCount": len(prepared),
                            "createdAt": now, "updatedAt": now,
                            "records": [dict(item, status="pending") for item in identities],
                        }
                jobs = []
                for rec, size in prepared:
                    rid = rec["id"]
                    version = rec["version"]
                    RECORD_BY_ID[rid] = rec
                    if file_publications and rid in file_publications:
                        binding = dict(file_publications[rid], version=version)
                        FILE_BINDINGS[rid] = binding
                        UPLOAD_GRANTS.pop(binding["grantDigest"], None)
                    old_fence = RECORD_FENCES.get(rid)
                    RECORD_FENCES[rid] = (rec["kind"], old_fence[1] + 1 if old_fence else 1, version)
                    RECORD_METADATA.pop(rid, None)
                    entry = {"version": version, "record": rec, "timestamp": now, "bytes": size, "pins": 1}
                    RECORD_VERSIONS.setdefault(rid, []).append(entry)
                    ts = RECORD_TS.setdefault(rid, {})
                    if "createTime" not in ts:
                        ts["createTime"] = now
                    ts["modify"] = now
                    DELETED.discard(rid)
                    INDEX_SUPPRESSED.discard(rid)
                    out.append({"id": rid, "version": version})
                    job, done, result = _index_job(effective_record(rec), entry, run_id, deadline)
                    jobs.append(job)
                    waits.append((done, result))
            reservation.submit(jobs, 0 if index_immediately else min(max(INDEX_DELAY_SECONDS, 0), 5))
        finally:
            reservation.cancel()
    if index_immediately:
        for done, result in waits:
            if not done.wait(10) or not result[0].success:
                raise BackendError(503, "indexing_pending_or_failed")
    return out

def soft_delete_records(record_ids):
    return _delete_records(record_ids, principal=core_auth.SYSTEM)


def _delete_records(record_ids, hard=False, *, principal):
    waits, removed = [], []
    with STORE_LOCK:
        if INDEX_SCHEDULER is None or not RUNTIME_ACCEPTING:
            raise BackendError(503, "runtime_not_started")
        if (not isinstance(record_ids, list) or len(record_ids) > 100
                or any(not isinstance(rid, str) for rid in record_ids)):
            raise BackendError(400, "Expected at most 100 record IDs")
        selected = list(dict.fromkeys(rid for rid in record_ids if rid in RECORD_BY_ID
                                     and (hard or rid not in DELETED)))
        if principal is not core_auth.SYSTEM:
            for rid in record_ids:
                current = read_record(rid, principal=principal, deleted=True)
                if current is None:
                    raise BackendError(404, "Record not found")
                require_write(current, principal=principal, deleted=True)
        if not selected:
            return removed
        reservation = INDEX_SCHEDULER.reserve("index", len(selected))
        try:
            with storage_transaction():
                jobs = []
                for rid in selected:
                    kind, generation, watermark = RECORD_FENCES[rid]
                    generation += 1
                    RECORD_FENCES[rid] = (kind, generation, watermark)
                    DELETED.add(rid)
                    marker = {"id": rid, "kind": kind, "version": generation, "_deleted": True}
                    job, done, result = _index_job(marker)
                    jobs.append(job)
                    waits.append((done, result))
                    if hard:
                        RECORD_BY_ID.pop(rid, None)
                        RECORD_TS.pop(rid, None)
                        RECORD_METADATA.pop(rid, None)
                        FILE_BINDINGS.pop(rid, None)
                        RECORD_VERSIONS.pop(rid, None)
                    removed.append(rid)
            reservation.submit(jobs)
        finally:
            reservation.cancel()
    deadline = time.monotonic() + DELETE_ACK_TIMEOUT
    for done, result in waits:
        if not done.wait(max(0, deadline - time.monotonic())) or not result[0].success:
            raise BackendError(503, "deletion_pending")
    return removed

def record_mtime(rid):
    ts = RECORD_TS.get(rid)
    return ts.get("modify") if ts else None

def storage_records_snapshot(kind, deleted_only, modify_after_epoch, descending, *, principal):
    with STORE_LOCK:
        records = list(RECORD_BY_ID.values())
    out = []
    for r in records:
        rid = r["id"]
        if not readable(r, principal=principal, owner=deleted_only, deleted=deleted_only):
            continue
        if deleted_only != (rid in DELETED):
            continue
        if kind and not kind_matches(kind, r.get("kind", "")):
            continue
        if modify_after_epoch is not None:
            mtime = record_mtime(rid)
            if mtime is None or mtime < modify_after_epoch:
                continue
        out.append(effective_record(r))
    out.sort(key=lambda r: r["id"], reverse=descending)
    return out

def _load_full_sample(index_immediately=True, *, principal):
    # Community dispatch also takes these locks in this order.
    with community_services.LOCK:
        with STORE_LOCK:
            for record in FULL_SAMPLE:
                require_write(record, principal=principal, existing=False)
        _clear_state(principal=principal)
        publications = sample_file_bindings(FULL_SAMPLE, FILE_BACKEND) if FILE_BACKEND is not None else None
        _upsert_records(FULL_SAMPLE, index_immediately=index_immediately, principal=principal,
                        file_publications=publications)

def record_counts():
    with STORE_LOCK:
        records = list(RECORD_BY_ID.values())
    return {
        "wells": sum(1 for r in records if r.get("kind") == WELL_KIND),
        "wellbores": sum(1 for r in records if r.get("kind") == WELLBORE_KIND),
        "welllogs": sum(1 for r in records if r.get("kind") == WELLLOG_KIND),
        "datasets": sum(1 for r in records if r.get("kind") == DATASET_KIND),
    }

def is_full_sample_loaded():
    expected_ids = {r["id"] for r in FULL_SAMPLE}
    with STORE_LOCK:
        current_ids = set(RECORD_BY_ID)
    return current_ids == expected_ids

def current_data_profile():
    counts = record_counts()
    total = sum(counts.values())
    if total == 0:
        return "empty"
    if is_full_sample_loaded():
        return "full-sample"
    if counts == {"wells": 1, "wellbores": 1, "welllogs": 1, "datasets": 1}:
        return "tutorial-or-manifest"
    return "custom"

def get_path(obj, path):
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur

def set_path(obj, path, value):
    parts = path.split(".")
    cur = obj
    for part in parts[:-1]:
        cur = cur.setdefault(part, {})
    cur[parts[-1]] = value

def project_record(record, fields):
    if not fields:
        return deepcopy(record)
    out = {}
    for f in fields:
        value = get_path(record, f)
        if value is not None:
            set_path(out, f, deepcopy(value))
    return out

def kind_matches(pattern, kind):
    if isinstance(pattern, list):
        return any(kind_matches(x, kind) for x in pattern)
    return fnmatch.fnmatch(kind.lower(), str(pattern).lower())

def _index_current_records(ids=None, kind=None, clean=False, *, principal):
    """Reindex through the same pinned, ordered work path as storage."""
    waits = []
    with STORE_LOCK:
        if not RUNTIME_ACCEPTING:
            raise BackendError(503, "runtime_not_started")
        selected = [record for rid, record in sorted(RECORD_BY_ID.items())
                    if rid not in DELETED and (ids is None or rid in ids)
                    and (kind is None or kind_matches(kind, record["kind"]))]
        for record in selected:
            require_write(record, principal=principal)
        replaced = [record for record in selected if record["id"] in INDEX_SUPPRESSED]
        selected = [record for record in selected if record not in replaced]
        reservation = INDEX_SCHEDULER.reserve("index", len(selected))
        jobs = []
        for record in selected:
            entry = next(e for e in RECORD_VERSIONS[record["id"]] if e["version"] == record["version"])
            entry["pins"] += 1
            job, done, result = _index_job(effective_record(record), entry)
            jobs.append(job)
            waits.append((done, result))
        reservation.submit(jobs)
    deadline = time.monotonic() + 10
    for done, result in waits:
        if not done.wait(max(0, deadline - time.monotonic())) or not result[0].success:
            raise BackendError(503, "indexing_pending_or_failed")
    for record in replaced:
        authorized_upsert([record], index_immediately=True, principal=principal)
    return selected + replaced

def delete_index_record(rid):
    return _delete_records([rid], principal=core_auth.SYSTEM)

def _delete_index_kind(kind, *, principal):
    waits = []
    with STORE_LOCK:
        if not RUNTIME_ACCEPTING:
            raise BackendError(503, "runtime_not_started")
        selected = [(rid, fence) for rid, fence in RECORD_FENCES.items() if fence[0] == kind]
        for rid, _ in selected:
            record = read_record(rid, principal=principal, deleted=True)
            if record is None:
                raise BackendError(404, "Record not found")
            require_write(record, principal=principal, deleted=True)
        reservation = INDEX_SCHEDULER.reserve("index", len(selected))
        try:
            with storage_transaction():
                jobs = []
                for rid, (full_kind, generation, watermark) in selected:
                    generation += 1
                    RECORD_FENCES[rid] = (full_kind, generation, watermark)
                    INDEX_SUPPRESSED.add(rid)
                    job, done, result = _index_job(
                        {"id": rid, "kind": full_kind, "version": generation, "_deleted": True})
                    jobs.append(job)
                    waits.append((done, result))
            reservation.submit(jobs)
        finally:
            reservation.cancel()
    deadline = time.monotonic() + DELETE_ACK_TIMEOUT
    for done, result in waits:
        if not done.wait(max(0, deadline - time.monotonic())) or not result[0].success:
            raise BackendError(503, "deletion_pending")

def ensure_index():
    global INDEX_ERROR, INDEX_RECONCILE
    if SEARCH_BACKEND is None:
        raise BackendError(503, "runtime_not_started")
    try:
        with INDEX_RECONCILE_LOCK:
            with STORE_LOCK:
                kinds = {record["kind"] for rid, record in RECORD_BY_ID.items()
                         if rid not in DELETED and rid not in INDEX_SUPPRESSED}
                check_index_kinds(kinds)
                ACTIVE_INDEX_KINDS.update(kinds)
                created = (SEARCH_BACKEND.provision_kinds(sorted(ACTIVE_INDEX_KINDS))
                           if hasattr(SEARCH_BACKEND, "provision_kinds") else SEARCH_BACKEND.provision())
                INDEX_RECONCILE = INDEX_RECONCILE or getattr(
                    SEARCH_BACKEND, "rebuild_required", bool(created))
                if not INDEX_RECONCILE:
                    return
                snapshots = [effective_record(record) for rid, record in RECORD_BY_ID.items()
                             if rid not in DELETED and rid not in INDEX_SUPPRESSED]
                pinned = [(record["id"], next(
                    entry for entry in RECORD_VERSIONS[record["id"]]
                    if entry["version"] == record["version"])) for record in snapshots]
                for _, entry in pinned:
                    entry["pins"] += 1
                snapshots += [{"id": rid, "kind": RECORD_FENCES[rid][0],
                               "version": RECORD_FENCES[rid][1], "_deleted": True}
                              for rid in RECORD_FENCES if rid in DELETED or rid not in RECORD_BY_ID
                              or rid in INDEX_SUPPRESSED]
                snapshots = [dict(record, _generation=RECORD_FENCES[record["id"]][1])
                             for record in snapshots if record["kind"] in ACTIVE_INDEX_KINDS]
                outcomes = []
                try:
                    # Hold the authoritative lock through reconciliation: an
                    # overlay cannot be stamped with another mutation's fence.
                    # Byte-bound chunks remain <=100 docs even at 64KiB/doc.
                    batch, size = [], 0
                    for record in snapshots:
                        length = len(snapshot_bytes(record)) + 1024
                        if batch and (len(batch) == 100 or size + length > 1024 * 1024):
                            outcomes.extend(SEARCH_BACKEND.bulk(batch))
                            batch, size = [], 0
                        batch.append(record)
                        size += length
                    if batch:
                        outcomes.extend(SEARCH_BACKEND.bulk(batch))
                finally:
                    for rid, entry in pinned:
                        entry["pins"] -= 1
                        if rid not in RECORD_BY_ID:
                            remaining = [e for e in RECORD_VERSIONS.get(rid, []) if e["pins"]]
                            if remaining:
                                RECORD_VERSIONS[rid] = remaining
                            else:
                                RECORD_VERSIONS.pop(rid, None)
            if any(not outcome.success for outcome in outcomes):
                raise BackendError(503, "index_reconciliation_failed")
            INDEX_RECONCILE = False
            if hasattr(SEARCH_BACKEND, "rebuild_required"):
                SEARCH_BACKEND.rebuild_required = False
            INDEX_ERROR = None
    except BackendError as exc:
        INDEX_ERROR = exc
        raise

def indexed_count():
    ensure_index()
    return SEARCH_BACKEND.count()

def _search_records(payload, caller_groups=None, *, principal, policy=None):
    kind = payload.get("kind")
    if not kind:
        return []
    ensure_index()
    for attempt in range(2):
        with STORE_LOCK:
            revision = REVISION
            owner = bool(payload.get("query_as_owner") or payload.get("queryAsOwner"))
            allowed = {rid for rid, record in RECORD_BY_ID.items()
                       if rid not in INDEX_SUPPRESSED and readable(record, principal=principal, owner=owner)}
            # Existing engine/fake interface has a bounded exclusion filter. Never trust indexed ACLs.
            request = dict(payload, _exclude_ids=sorted(set(RECORD_FENCES) - allowed),
                           _legacy_text=COMPAT_PROFILE == "legacy")
        found = SEARCH_BACKEND.search(request, None, False)
        with STORE_LOCK:
            if revision != REVISION:
                continue
            if policy is not None:
                policy["revision"] = revision
            return [effective_record(RECORD_BY_ID[r["id"]]) for r in found
                    if r["id"] in allowed and readable(RECORD_BY_ID.get(r["id"]),
                                                       principal=principal, owner=owner)]
    raise BackendError(503, "concurrent_mutation_retry")

def _core_search_records(payload, *, principal, references=False):
    ensure_index()
    for attempt in range(2):
        with STORE_LOCK:
            revision = REVISION
            owner = payload.get("queryAsOwner", False)
            allowed = {rid for rid, record in RECORD_BY_ID.items()
                       if rid not in INDEX_SUPPRESSED and kind_matches(payload["kind"], record["kind"])
                       and readable(record, principal=principal, owner=owner)}
            request = dict(payload, _allowed_ids=sorted(allowed))
        response = SEARCH_BACKEND.page(request, references=references)
        with STORE_LOCK:
            # Legal validity may change solely with wall clock, not revision.
            current = {rid for rid, record in RECORD_BY_ID.items()
                       if rid not in INDEX_SUPPRESSED and kind_matches(payload["kind"], record["kind"])
                       and readable(record, principal=principal, owner=owner)}
            if revision != REVISION or current != allowed:
                continue
            if references:
                if any(r["id"] not in allowed or r["version"] != RECORD_BY_ID[r["id"]]["version"]
                       for r in response["results"]):
                    raise BackendError(503, "concurrent_mutation_retry")
            else:
                response["results"] = [effective_record(RECORD_BY_ID[r["id"]])
                                       for r in response["results"] if r["id"] in allowed]
            return response, revision, allowed
    raise BackendError(503, "concurrent_mutation_retry")

def _field_values(records, field):
    values = []
    for r in records:
        v = get_path(r, field)
        if isinstance(v, list):
            values.extend(v)
        elif v is not None:
            values.append(v)
    return values

def _num(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def aggregate_records(records, aggregate_by):
    out = []
    for agg in aggregate_by or []:
        if not isinstance(agg, dict):
            continue
        field = agg.get("field")
        agg_type = agg.get("aggregateByType", "count")
        entry = {"field": field, "aggregateByType": agg_type}
        if agg_type == "count":
            entry["Count"] = {"count": len(records)}
        elif agg_type == "filteredTerms":
            values = _field_values(records, field)
            counts = {}
            for v in values:
                key = str(v)
                counts[key] = counts.get(key, 0) + 1
            bucket_size = int(agg.get("bucketSize", 10) or 10)
            terms = [{"key": k, "count": c} for k, c in
                     sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:bucket_size]]
            entry["FilteredTerms"] = {
                "terms": terms,
                "termCount": len(counts),
                "totalCount": len(values),
            }
        elif agg_type == "histogram":
            interval = _num(agg.get("interval", 100))
            buckets = {}
            for v in _field_values(records, field):
                n = _num(v)
                if n is None:
                    continue
                start = int(n // interval * interval)
                buckets.setdefault(start, 0)
                buckets[start] += 1
            ordered = [{"start": s, "end": s + interval, "count": c}
                       for s, c in sorted(buckets.items())]
            entry["Histogram"] = {"buckets": ordered}
        else:
            entry["Count"] = {"count": len(records)}
        out.append(entry)
    return out

def record_file_name(record):
    data = record.get("data", {}) or {}
    name = data.get("FileName") or data.get("Name")
    if name:
        return name
    fsi = (data.get("DatasetProperties") or {}).get("FileSourceInfo") or {}
    src = fsi.get("FileSource") or data.get("FileSource")
    if src:
        return Path(str(src)).name
    return None

def dataset_file_name(dataset_registry_id, *, principal):
    with STORE_LOCK:
        ds = read_record(dataset_registry_id, principal=principal)
        return record_file_name(ds) if ds else None

def _record_version(rid, version, *, principal):
    with STORE_LOCK:
        if not read_record(rid, principal=principal):
            return None
        entries = RECORD_VERSIONS.get(rid, [])
        for entry in entries:
            if entry["version"] == version:
                record = effective_record(entry["record"])
                current = effective_record(RECORD_BY_ID[rid])
                for field in ("acl", "legal", "tags"):
                    if field in current:
                        record[field] = deepcopy(current[field])
                return record
    return None

def version_numbers(rid, *, principal):
    with STORE_LOCK:
        if not read_record(rid, principal=principal):
            return []
        entries = RECORD_VERSIONS.get(rid, [])
    return sorted(entry["version"] for entry in entries)

def patch_record(rid, payload, *, principal):
    with STORE_LOCK:
        current = read_record(rid, principal=principal)
        if not current:
            return None
        rec = deepcopy(current)
    for key in ("acl", "legal", "tags", "ancestry"):
        if key in payload and isinstance(payload[key], dict):
            rec[key] = deepcopy(payload[key])
    if isinstance(payload.get("data"), dict):
        merged = rec.setdefault("data", {})
        for key, value in payload["data"].items():
            merged[key] = deepcopy(value)
    return authorized_upsert([rec], index_immediately=True, principal=principal)

def copy_record(rid, acl=None, *, principal):
    with STORE_LOCK:
        current = read_record(rid, principal=principal)
        if not current or rid in DELETED:
            return None, None
        require_write(current, principal=principal)
        base = deepcopy(current)
        new_id = rid + "_copy"
        suffix = 1
        while new_id in RECORD_BY_ID:
            suffix += 1
            new_id = f"{rid}_copy_{suffix}"
    if acl:
        base["acl"] = deepcopy(acl)
    base["id"] = new_id
    base.pop("version", None)
    out = authorized_upsert([base], index_immediately=True, principal=principal, copy_source=rid)
    return new_id, out[0]

def _expiry_seconds(value):
    try:
        text = str(value or "1H").strip().upper()
        number = int(text[:-1])
        return number * {"M": 60, "H": 3600, "D": 86400}.get(text[-1:], 3600)
    except (ValueError, TypeError):
        return UPLOAD_TOKEN_TTL

def legal_tag_payload(name):
    tag = LEGAL_TAGS[name]
    payload = {
        "name": name,
        "description": tag.get("description"),
        "isValid": is_legal_tag_valid(name),
        "properties": deepcopy(tag.get("properties") or {}),
    }
    payload["properties"]["securityClassification"] = tag.get("classification", "Public")
    return payload

def _tag_value(tag, attr):
    props = tag.get("properties") or {}
    if attr in props:
        return props[attr]
    target = attr.lower()
    def walk(obj):
        if not isinstance(obj, dict):
            return _MISSING
        for key, value in obj.items():
            if str(key).lower() == target:
                return value
            if isinstance(value, dict):
                found = walk(value)
                if found is not _MISSING:
                    return found
        return _MISSING
    return walk(props.get("extensionProperties") or {})

_MISSING = object()

def _contains(value, needle):
    if isinstance(value, list):
        return any(_contains(v, needle) for v in value)
    if value is None:
        return False
    if isinstance(value, bool):
        value = "true" if value else "false"
    return needle.lower() in str(value).lower()

def _tag_between(value, lo, hi):
    values = value if isinstance(value, list) else [value]
    for v in values:
        if v is None:
            continue
        try:
            day = _dt.date.fromisoformat(str(v)[:10])
        except (ValueError, TypeError):
            continue
        if lo:
            try:
                lo_day = _dt.date.fromisoformat(lo[:10])
            except (ValueError, TypeError):
                lo_day = None
            if lo_day and day <= lo_day:
                continue
        if hi:
            try:
                hi_day = _dt.date.fromisoformat(hi[:10])
            except (ValueError, TypeError):
                hi_day = None
            if hi_day and day >= hi_day:
                continue
        return True
    return False

def _parse_between(clause):
    low = clause.lower()
    marker = low.find(" between (")
    if marker < 0:
        return None
    attr = clause[:marker].strip()
    rest = clause[marker + len(" between ("):]
    if ")" not in rest:
        return None
    parts = [x.strip().strip("'\"") for x in rest[:rest.index(")")].split(",")]
    if len(parts) != 2:
        return None
    return attr, parts[0], parts[1]

def _legal_clause_matches(tag, clause):
    clause = clause.strip()
    if not clause:
        return True
    between = _parse_between(clause)
    if between:
        attr, lo, hi = between
        value = _tag_value(tag, attr)
        return value is not _MISSING and _tag_between(value, lo, hi)
    if "=" in clause:
        attr, _, value = clause.partition("=")
        attr = attr.strip()
        value = value.strip().strip('"\'')
        if not attr or attr.lower() == "any":
            return _legal_free_text(tag, value)
        found = _tag_value(tag, attr)
        return found is not _MISSING and _contains(found, value)
    return _legal_free_text(tag, clause)

def _legal_free_text(tag, needle):
    props = tag.get("properties") or {}
    candidates = [
        tag.get("name"),
        tag.get("description"),
        props.get("contractId"),
        props.get("originator"),
        props.get("countryOfOrigin"),
        props.get("extensionProperties"),
    ]
    return any(_contains(v, needle) for v in candidates if v is not None)

def legal_tags_query(payload, valid_only):
    clauses = payload.get("queryList") or []
    if not clauses:
        return {"legalTags": []}
    operator = (payload.get("operatorList") or ["union"])[0]
    matched = []
    for tag in LEGAL_TAGS.values():
        if is_legal_tag_valid(tag.get("name")) != valid_only:
            continue
        matched.append((tag, [_legal_clause_matches(tag, c) for c in clauses]))
    chosen = []
    clause_count = len(clauses)
    for tag, flags in matched:
        hits = sum(1 for f in flags if f)
        if operator == "intersection" and hits == clause_count:
            chosen.append(legal_tag_payload(tag.get("name")))
        elif operator in ("add", "union") and hits > 0:
            repeats = hits if operator == "add" else 1
            chosen.extend([legal_tag_payload(tag.get("name"))] * repeats)
    return {"legalTags": chosen}

def schema_info_for(kind):
    return legacy_entry(SCHEMA_REGISTRY.get(kind))

def dataset_registration_records(record_ids, *, principal):
    out = []
    with STORE_LOCK:
        for rid in record_ids:
            rec = read_record(rid, principal=principal)
            if rec and rid not in DELETED and kind_matches("osdu:wks:dataset--*", rec.get("kind", "")):
                out.append(deepcopy(rec))
    return out


class RequestServices:
    """Explicit request authority for unchanged community extension handlers."""
    def __init__(self, handler, route, method):
        self.handler, self.route, self.method = handler, route, method

    def __getattr__(self, name):
        return globals()[name]

    @property
    def RECORD_BY_ID(self):
        with STORE_LOCK:
            restoring = self.route[0] == "dataset" and self.method != "GET"
            records = {rid: effective_record(record) for rid, record in RECORD_BY_ID.items()
                       if readable(record, principal=self.handler.principal,
                                   owner=restoring, deleted=restoring)}
            self.handler._observe(records.values(), REVISION)
            return records

    @property
    def WORKFLOW_RUNS(self):
        with STORE_LOCK, WORKFLOW_LOCK:
            return {rid: run for rid, run in WORKFLOW_RUNS.items() if self.handler._run_visible(run)}

    @property
    def WORKFLOW_LOCK(self):
        return self._workflow_lock()

    @contextmanager
    def _workflow_lock(self):
        with STORE_LOCK, WORKFLOW_LOCK:
            yield

    def upsert_records(self, records, **kwargs):
        return self.handler._upsert(records, **kwargs)

    def soft_delete_records(self, ids):
        return self.handler._delete(ids)

    def indexed_count(self):
        # Readiness checks use the engine but never serialize the count.
        return indexed_count()

    def index_current_records(self, ids=None, kind=None, clean=False):
        with STORE_LOCK:
            selected = [record for rid, record in RECORD_BY_ID.items()
                        if rid not in DELETED and (ids is None or rid in ids)
                        and (kind is None or kind_matches(kind, record["kind"]))]
            for record in selected:
                require_write(record, principal=self.handler.principal)
        return _index_current_records(ids=[r["id"] for r in selected], kind=kind, clean=clean,
                                     principal=self.handler.principal)

    def delete_index_kind(self, kind):
        return _delete_index_kind(kind, principal=self.handler.principal)

    @contextmanager
    def storage_transaction(self):
        with STORE_LOCK:
            if self.route[0] == "storage" and self.method == "DELETE":
                record = self.handler._record(self.route[2][0])
                if record is None:
                    raise BackendError(404, "Record not found")
                require_write(record, principal=self.handler.principal)
            with storage_transaction():
                yield


class ManifestError(ValueError):
    def __init__(self, errors):
        self.errors = errors[:20]
        super().__init__(json.dumps({"errors": self.errors}))


def manifest_records(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get("executionContext"), dict):
        raise ManifestError([{"section": "executionContext", "index": None,
                              "path": "/executionContext", "message": "must be an object"}])
    ctx = payload["executionContext"]
    manifest = ctx.get("manifest")
    if not isinstance(manifest, dict):
        raise ManifestError([{"section": "manifest", "index": None,
                              "path": "/executionContext/manifest", "message": "must be an object"}])
    acl = ctx.get("acl")
    legal = ctx.get("legal")
    errors, sections = [], []
    def error(section, index, path, message):
        if len(errors) < 20:
            errors.append({"section": section, "index": index, "path": path, "message": message[:512]})
    for key, value in manifest.items():
        # Existing sample manifests carry an envelope identity, not a record section.
        if key == "kind" and value == "osdu:wks:Manifest:1.0.0":
            continue
        if key not in ("MasterData", "ReferenceData", "Data") and value:
            error(key, None, "/" + key, "unsupported manifest section")
    data = manifest.get("Data", {})
    if not isinstance(data, dict):
        error("Data", None, "/Data", "must be an object")
        data = {}
    for key, value in data.items():
        if key not in ("Datasets", "WorkProductComponents") and value:
            error("Data." + key, None, "/Data/" + key, "unsupported manifest section")
    for source, keys, prefix in ((manifest, ("MasterData", "ReferenceData"), ""),
                                 (data, ("Datasets", "WorkProductComponents"), "Data/")):
        for key in keys:
            value = source.get(key, [])
            if not isinstance(value, list):
                error(prefix + key, None, "/" + prefix + key, "must be an array")
            else:
                sections.append((prefix + key, value))
    count = sum(len(items) for _, items in sections)
    if not 1 <= count <= 100:
        error("manifest", None, "/", "manifest requires 1..100 records")
    out, ids = [], set()
    if count <= 100:
        for section, items in sections:
            for index, item in enumerate(items):
                path = "/" + section + "/" + str(index)
                if not isinstance(item, dict):
                    error(section, index, path, "record must be an object")
                    continue
                r = deepcopy(item)
                if "acl" not in r and acl is not None:
                    r["acl"] = deepcopy(acl)
                if "legal" not in r and legal is not None:
                    r["legal"] = deepcopy(legal)
                try:
                    validate_record(r)
                    if r["id"] in ids:
                        raise ValueError("duplicate record ID")
                    ids.add(r["id"])
                except (ValueError, SchemaError) as exc:
                    # Preserve schema lifecycle conflicts as 409.
                    if getattr(exc, "status", 400) != 400:
                        raise
                    error(section, index, path, str(exc))
                out.append(r)
    if errors:
        raise ManifestError(errors)
    return out

def workflow_evictions(now):
    return [rid for rid, run in WORKFLOW_RUNS.items()
            if run["status"] in ("succeeded", "failed")
            and now - run.get("completedAt", now) >= 600]


def start_workflow_item(run_id):
    with WORKFLOW_LOCK:
        run = WORKFLOW_RUNS.get(run_id)
        if run and run["status"] == "submitted":
            run["status"] = "running"
            run["startedAt"] = run["updatedAt"] = time.time()


def _reconcile_workflow(run):
    run["indexedCount"] = sum(item["status"] == "indexed" for item in run["records"])
    run["failedCount"] = sum(item["status"] == "failed" for item in run["records"])
    run["pendingCount"] = run["recordCount"] - run["indexedCount"] - run["failedCount"]
    run["updatedAt"] = time.time()
    if not run["pendingCount"]:
        run["status"] = "failed" if run["failedCount"] else "succeeded"
        run["completedAt"] = run["updatedAt"]


def settle_workflow_item(run_id, record_id, outcome):
    with WORKFLOW_LOCK:
        run = WORKFLOW_RUNS.get(run_id)
        if not run or run["status"] in ("succeeded", "failed"):
            return
        for item in run["records"]:
            if item["id"] == record_id and item["status"] == "pending":
                item["status"] = "indexed" if outcome.success else "failed"
                if not outcome.success:
                    item["errorCode"] = outcome.error[:512] or "index_failed"
                break
        _reconcile_workflow(run)


def sweep_workflow_runs():
    # Separate lifecycle thread: even two blocked index workers cannot delay expiry.
    reclaim_file_grants()
    with WORKFLOW_LOCK:
        scheduler = INDEX_SCHEDULER
        if scheduler is None:
            return
        for rid, run in WORKFLOW_RUNS.items():
            if (run["status"] not in ("succeeded", "failed")
                    and scheduler.clock() >= WORKFLOW_DEADLINES[rid]):
                for item in run["records"]:
                    if item["status"] == "pending":
                        item.update(status="failed", errorCode="deadline")
                _reconcile_workflow(run)
        for rid in workflow_evictions(time.time()):
            del WORKFLOW_RUNS[rid]
            WORKFLOW_DEADLINES.pop(rid, None)

def sweep_temp_state():
    now = time.time()
    with STORE_LOCK:
        for token, info in list(UPLOAD_TOKENS.items()):
            if now - info.get("created", 0) > UPLOAD_TOKEN_TTL:
                del UPLOAD_TOKENS[token]
    with CURSOR_LOCK:
        for cursor, state in list(CURSORS.items()):
            if now >= state.get("expiresAt", state.get("updated", 0) + min(300, CURSOR_TTL)):
                del CURSORS[cursor]

def sample_file_bindings(records, files):
    """Import only immutable shipped bytes; never inspect operator FILES_DIR."""
    from file_backend import input_chunks, MAX_FILE_BYTES
    bindings = {}
    for record in records:
        if record["kind"] != DATASET_KIND:
            continue
        if record["id"] in FILE_BINDINGS:
            binding = FILE_BINDINGS[record["id"]]
            sample_names = [name for name, sha256 in SAMPLE_HASHES.items()
                            if sha256 == binding["sha256"]]
            sample_key = (hashlib.sha256(PARTITION.encode()).hexdigest() + "/final/"
                          + hashlib.sha256(record["id"].encode()).hexdigest() + "/"
                          + binding["sha256"])
            if (sample_names and binding["principal"] == core_auth.SYSTEM.name
                    and binding["finalKey"] == sample_key
                    and not any(immutable_sample(name) for name in sample_names)):
                raise BackendError(503, "file_unavailable")
            if files.probe(binding["finalKey"])["size"] != binding["size"]:
                raise BackendError(503, "file_unavailable")
            continue
        name = record_file_name(record)
        if not immutable_sample(name):
            raise BackendError(503, "unmigrated_file_record: " + record["id"])
        target = ROOT / "files" / name
        size = target.stat().st_size
        if size > MAX_FILE_BYTES:
            raise BackendError(503, "file_unavailable")
        digest = hashlib.sha256()
        with target.open("rb") as stream:
            for chunk in input_chunks(stream, size):
                digest.update(chunk)
        if digest.hexdigest() != SAMPLE_HASHES[name]:
            raise BackendError(503, "file_unavailable")
        key = (hashlib.sha256(PARTITION.encode()).hexdigest() + "/final/"
               + hashlib.sha256(record["id"].encode()).hexdigest() + "/" + digest.hexdigest())
        try:
            files.probe(key)
        except BackendError as error:
            if str(error) != "file_unavailable":
                raise
            with target.open("rb") as stream:
                files.upload(key, stream, size)
        verified, count = hashlib.sha256(), 0
        for chunk in files.read(key):
            if len(chunk) > 1024 * 1024:
                raise BackendError(503, "file_unavailable")
            count += len(chunk)
            verified.update(chunk)
        if count != size or verified.hexdigest() != SAMPLE_HASHES[name]:
            raise BackendError(503, "file_unavailable")
        bindings[record["id"]] = {
            "finalKey": key, "size": size, "sha256": digest.hexdigest(),
            "grantDigest": hashlib.sha256(key.encode()).hexdigest(),
            "principal": core_auth.SYSTEM.name, "recordId": record["id"],
            "metadataDigest": hashlib.sha256(core_profile.canonical(
                {"id": record["id"], **core_profile.user_fields(record)}).encode()).hexdigest(),
            "version": record.get("version", 1)}
    return bindings

def start_runtime(backend=None, *, storage=None, files=None):
    """Network initialization is explicit; failed cleanup never publishes readiness."""
    global SEARCH_BACKEND, STORAGE_BACKEND, INDEX_SCHEDULER, RUNTIME_ACCEPTING, INDEX_ERROR, INDEX_RECONCILE
    global COMPAT_PROFILE, IDENTITIES, IDENTITY_SEEDS
    global FILE_BACKEND, FILE_RUNTIME_EPOCH
    selected_profile = core_profile.select_profile()
    if selected_profile == "core" and not REQUIRE_AUTH:
        raise ValueError("Core profile requires authentication")
    identities, seeds = core_auth.load_identities(os.getenv("OSDU_IDENTITIES_FILE"), TOKEN, REQUIRE_AUTH)
    if files is None and backend is None and storage is None:
        from file_backend import AzureFiles
        files = AzureFiles.from_env(require_public=selected_profile == "core")
    with STORE_LOCK:
        RUNTIME_ACCEPTING = False
    if not stop_runtime():
        raise BackendError(503, "runtime_shutdown_pending")
    COMPAT_PROFILE = selected_profile
    IDENTITIES, IDENTITY_SEEDS = identities, seeds
    SEARCH_BACKEND = None
    selected = backend if backend is not None else OpenSearch(
        os.getenv("OPENSEARCH_URL", "http://localhost:9200"), PARTITION)
    try:
        STORAGE_BACKEND = storage if storage is not None else AzureBlobStorage.from_env(PARTITION)
        original = STORAGE_BACKEND.load()
        state, registry = prepare_storage(original)
        active = {record["kind"] for rid, record in state["records"].items()
                  if rid not in state["deleted"] and rid not in state["suppressed"]}
        if len(active) > MAX_ACTIVE_INDEX_KINDS:
            raise CapacityError("active_kind_capacity")
        if original is None or original["format"] == 1:
            if original is not None:
                STORAGE_BACKEND.backup_format1()
            STORAGE_BACKEND.save(state)
        with STORE_LOCK:
            restore_storage(state, registry)
            ACTIVE_INDEX_KINDS.clear()
        if files is not None:
            files.initialize()
        FILE_BACKEND, FILE_RUNTIME_EPOCH = files, uuid.uuid4().hex
        if files is not None and original is not None:
            publications = sample_file_bindings(list(RECORD_BY_ID.values()), files)
            if publications:
                with storage_transaction():
                    FILE_BINDINGS.update(publications)
        if hasattr(selected, "configure_registry"):
            selected.configure_registry(SCHEMA_REGISTRY)
        selected.start_epoch(uuid.uuid4().hex)
        SEARCH_BACKEND = selected
        INDEX_ERROR = None
        INDEX_RECONCILE = True
        ensure_index()
        INDEX_SCHEDULER = Scheduler(_execute_index, sweep=sweep_workflow_runs)
        INDEX_SCHEDULER.start()
        if original is None:
            if PRELOAD_FULL_SAMPLE:
                publications = sample_file_bindings(FULL_SAMPLE, files) if files is not None else None
                _upsert_records(FULL_SAMPLE, index_immediately=True, principal=core_auth.SYSTEM,
                                file_publications=publications)
        RUNTIME_ACCEPTING = True
    except Exception:
        stop_runtime()
        raise

def stop_runtime():
    global SEARCH_BACKEND, STORAGE_BACKEND, INDEX_SCHEDULER, RUNTIME_ACCEPTING
    global REVISION
    global FILE_BACKEND, FILE_RUNTIME_EPOCH
    with STORE_LOCK:
        RUNTIME_ACCEPTING = False
    if INDEX_SCHEDULER is not None and not INDEX_SCHEDULER.stop():
        return False
    INDEX_SCHEDULER = None
    SEARCH_BACKEND = None
    STORAGE_BACKEND = None
    FILE_BACKEND, FILE_RUNTIME_EPOCH = None, None
    with STORE_LOCK:
        RECORD_BY_ID.clear()
        RECORD_VERSIONS.clear()
        RECORD_FENCES.clear()
        DELETED.clear()
        RECORD_TS.clear()
        UPLOAD_TOKENS.clear()
        INDEX_KIND_GENERATION.clear()
        INDEX_SUPPRESSED.clear()
        SCHEMA_REGISTRY.reset()
        LEGAL_TAGS.clear()
        GROUPS.clear()
        RECORD_METADATA.clear()
        FILE_BINDINGS.clear()
        UPLOAD_GRANTS.clear()
        REPLAY_OWNERS.clear()
        REVISION = 0
    with CURSOR_LOCK:
        CURSORS.clear()
    with WORKFLOW_LOCK:
        WORKFLOW_RUNS.clear()
        WORKFLOW_DEADLINES.clear()
        WORKFLOW_DEFS.clear()
        WORKFLOW_DEFS.update(seed_workflow_defs())
    community_services.reset(PARTITION)
    return True

def backend_errors(method):
    def wrapped(self):
        try:
            self.command_name = method.__name__[3:]
            self._read_ids = set()
            self._policy_revision = None
            self._policy_stale = False
            path = urlparse(self.path).path
            self._deleted_read = (
                core_profile.canonical_path(path) == "/storage/v2/records"
                and parse_qs(urlparse(self.path).query).get("deleted", ["false"])[0].lower()
                in {"true", "1", "yes"})
            self._read_response = self.command_name == "GET" or (
                self.command_name == "POST" and (
                    "/search/" in path or path.endswith(("/retrievalInstructions", "/getRetrievalInstructions",
                    "/getDatasetRegistry", "/records:batch", "/headers", "/getFileSignedUrl",
                    "/query/records"))))
            if not self._check_auth(urlparse(self.path).path):
                return
            if (COMPAT_PROFILE == "core" and self.command_name == "POST"
                    and re.fullmatch(r"/(?:api/)?records/[^/]+:delete", path)):
                raise BackendError(405, "Use core POST /api/storage/v2/records/{id}:delete")
            route = community_services.match(self.command_name, urlparse(self.path).path)
            extension = (route and route[0] != "file"
                         and route[1] not in {"/query/records:batch", "/records/{id}:delete"})
            if COMPAT_PROFILE == "core" and core_profile.is_core_route(urlparse(self.path).path) and not extension:
                return self._pending_core_request(method.__name__[3:])
            return method(self)
        except core_profile.ProfileError as exc:
            self.close_connection = True
            return self._error(exc.status, str(exc), {"Connection": "close"})
        except (BackendError, SchemaError, CapacityError) as exc:
            return self._error(exc.status, str(exc), {"Retry-After": "1"} if exc.status == 429 else None)
        except ValueError:
            return self._error(400, "Invalid request")
    return wrapped

class Handler(BaseHTTPRequestHandler):
    server_version = "OSDU-Well360/0.7"

    def _upsert(self, records, **kwargs):
        return authorized_upsert(records, principal=self.principal, **kwargs)

    def _delete(self, ids, hard=False):
        return authorized_delete(ids, principal=self.principal, hard=hard)

    def _record(self, rid, **kwargs):
        with STORE_LOCK:
            record = read_record(rid, principal=self.principal, **kwargs)
            if record:
                self._observe([record], REVISION)
            return record

    def _observe(self, records, revision):
        if self._policy_revision is None:
            self._policy_revision = revision
        elif self._policy_revision != revision:
            self._policy_stale = True
        self._read_ids.update(r["id"] for r in records)

    def _search(self, payload):
        policy = {}
        records = _search_records(payload, principal=self.principal, policy=policy)
        self._observe(records, policy.get("revision", REVISION))
        return records

    def _filename(self, rid):
        record = self._record(rid)
        return record_file_name(record) if record else None

    def _file_properties(self, rid):
        record = self._record(rid)
        if record is None:
            raise BackendError(404, "File not found")
        binding = FILE_BINDINGS.get(rid)
        if binding:
            if FILE_BACKEND is None:
                raise BackendError(503, "file_backend_unavailable")
            FILE_BACKEND.probe(binding["finalKey"])
            return {"signedUrl": FILE_BACKEND.signed_url(binding["finalKey"], time.time() + 3600),
                    "expiresInSeconds": 3600, "size": binding["size"], "sha256": binding["sha256"]}
        name = record_file_name(record)
        if not immutable_sample(name):
            raise BackendError(503, "file_unavailable")
        target = ROOT / "files" / name
        return {"signedUrl": f"http://{self.headers.get('Host', f'localhost:{PORT}')}/files/{name}",
                "expiresInSeconds": 3600, "size": target.stat().st_size, "sha256": SAMPLE_HASHES[name]}

    def _datasets(self, ids):
        return [record for rid in ids if (record := self._record(rid))
                and kind_matches("osdu:wks:dataset--*", record["kind"])]

    def _cursor_state(self, cursor, service=None):
        state = CURSORS.get(cursor)
        if not state or time.time() >= state["expiresAt"]:
            CURSORS.pop(cursor, None)
            raise BackendError(400, "Unknown/expired cursor")
        if state.get("principal") != self.principal.name:
            raise BackendError(403, "Cursor does not belong to current caller")
        if service is not None and state.get("service") != service:
            raise BackendError(400, "cursor_service_mismatch")
        self._deleted_read = bool(state.get("deleted"))
        if (state.get("partition") != PARTITION or state.get("revision") != REVISION
                or state["groupsDigest"] != hashlib.sha256(
                    core_profile.canonical(sorted(group_for_caller(self))).encode()).hexdigest()
                or any(not (record := read_record(r["id"], principal=self.principal,
                           deleted=self._deleted_read, owner=state["owner"]))
                       or record["version"] != r["version"] for r in state["refs"])):
            CURSORS.pop(cursor, None)
            raise BackendError(400, "cursor_invalidated")
        self._observe([{"id": r["id"]} for r in state["refs"]], REVISION)
        return state

    def _cursor(self, state):
        now = time.time()
        for token, old in list(CURSORS.items()):
            if now >= old["expiresAt"]:
                CURSORS.pop(token, None)
        results = state.pop("results", None)
        if results is not None:
            state["refs"] = [{"id": r["id"], "version": r["version"]} for r in results]
        state = dict(state, principal=self.principal.name, revision=REVISION, partition=PARTITION,
                     groupsDigest=hashlib.sha256(core_profile.canonical(
                         sorted(group_for_caller(self))).encode()).hexdigest(),
                     deleted=self._deleted_read, owner=bool(state.get("owner") or self._deleted_read),
                     expiresAt=now + (300 if COMPAT_PROFILE == "core" else min(300, CURSOR_TTL)))
        if (len(state["refs"]) > 5000 or len(CURSORS) >= MAX_CURSORS or
                sum(len(core_profile.canonical(s).encode("utf-8")) for s in CURSORS.values()) +
                len(core_profile.canonical(state).encode("utf-8")) > MAX_CURSOR_BYTES):
            raise BackendError(429, "cursor_capacity")
        return state

    def _cursor_records(self, state, start=0, end=None):
        return [read_record(r["id"], principal=self.principal, deleted=state["deleted"],
                            owner=state["owner"]) for r in state["refs"][start:end]]

    def _core_search(self, method, path, query, payload):
        core_profile.query_values(query, set())
        root = "/search/v2/query_with_cursor"
        if method == "DELETE" and path.startswith(root + "/"):
            token = unquote(path[len(root) + 1:])
            with STORE_LOCK, CURSOR_LOCK:
                state = CURSORS.get(token)
                if state and state["principal"] != self.principal.name:
                    raise BackendError(403, "Cursor does not belong to current caller")
                CURSORS.pop(token, None)
            return self._no_content()
        if method != "POST" or path not in ("/search/v2/query", root):
            raise BackendError(501, "unsupported_core_search_operation")
        cursor_mode = path == root
        request = core_profile.search_request(payload, SCHEMA_REGISTRY, cursor=cursor_mode)
        if "cursor" in request:
            with STORE_LOCK, CURSOR_LOCK:
                token = request["cursor"]
                state = self._cursor_state(token)
                if state.get("service") != "search":
                    raise BackendError(400, "Invalid search cursor")
                pos, limit = state["pos"], state["limit"]
                count = len(state["refs"])
                page = self._cursor_records(state, pos, pos + limit)
                state["pos"] = min(pos + limit, count)
                next_token = token if state["pos"] < count else None
                if next_token is None:
                    CURSORS.pop(token, None)
                body = {"results": [core_profile.search_projection(
                    r, state["returned"], state["excluded"]) for r in page],
                        "totalCount": count, "cursor": next_token}
                if "aggregations" in state:
                    body["aggregations"] = state["aggregations"]
                self._projection_done = True
                return self._json(200, body)
        response, revision, allowed = _core_search_records(
            request, principal=self.principal, references=cursor_mode)
        with STORE_LOCK, CURSOR_LOCK:
            if revision != REVISION or any(not readable(RECORD_BY_ID.get(rid),
                    principal=self.principal, owner=request.get("queryAsOwner", False)) for rid in allowed):
                raise BackendError(503, "concurrent_mutation_retry")
            self._observe([{"id": rid} for rid in allowed], revision)
            if cursor_mode:
                refs = response["results"]
                pos = min(request["offset"] + request["limit"], len(refs))
                response["results"] = [effective_record(RECORD_BY_ID[r["id"]])
                                       for r in refs[request["offset"]:pos]]
                token = None
                if request["limit"] and pos < len(refs):
                    token = uuid.uuid4().hex.upper()
                    state = {"refs": refs, "pos": pos, "limit": request["limit"],
                             "returned": request["returnedFields"], "excluded": request["excludedFields"],
                             "owner": request.get("queryAsOwner", False), "service": "search"}
                    if "aggregations" in response:
                        state["aggregations"] = response["aggregations"]
                    CURSORS[token] = self._cursor(state)
                response["cursor"] = token
            response["results"] = [core_profile.search_projection(
                r, request["returnedFields"], request["excludedFields"]) for r in response["results"]]
            self._projection_done = True
            return self._json(200, response)

    def _run_visible(self, run):
        return bool(run and (self.principal is core_auth.SYSTEM or (
            run.get("principal") == self.principal.name
            and all(self._record(item["id"]) for item in run.get("recordIds", [])))))

    def _pending_core_request(self, method):
        parsed = urlparse(self.path)
        if not self._check_auth(parsed.path):
            return
        canonical = core_profile.canonical_path(parsed.path)
        roles = None
        if canonical.startswith("/storage/"):
            roles = ({"service.storage.admin"} if method == "DELETE" else
                     {"users.datalake.editors", "users.datalake.admins"} if method == "PATCH" else
                     {"service.storage.creator", "service.storage.admin"} if method == "PUT" else
                     {"service.storage.viewer"} if method == "GET" or "/query/" in canonical else
                     {"service.storage.creator", "service.storage.admin"})
        elif canonical.startswith("/search/"):
            roles = {"users.datalake.viewers", "users.datalake.editors", "users.datalake.admins", "users.datalake.ops"}
        elif canonical.startswith("/schema-service/"):
            roles = {"service.schema-service.viewers", "service.schema-service.editors"} if method == "GET" else {"service.schema-service.editors"}
        elif canonical.startswith("/file/"):
            write = method != "GET" or canonical.endswith("/uploadURL")
            roles = {"service.file.editors"} if write else {"service.file.viewers", "service.file.editors"}
        elif canonical.startswith("/dataset/"):
            write = method in {"PUT", "DELETE"} or "storageInstructions" in canonical
            roles = {"service.dataset.editors"} if write else {"service.dataset.viewers", "service.dataset.editors"}
        if roles and not self._rbac(roles):
            return
        if (method == "POST" and canonical.startswith("/storage/v2/records/")
                and canonical.endswith(":delete")
                and core_profile.body_length(self.headers) == 0):
            payload = None
        elif method in {"POST", "PUT", "PATCH"}:
            payload = core_profile.read_json(
                self.rfile, self.headers,
                allow_patch=method == "PATCH" and core_profile.canonical_path(parsed.path) == "/storage/v2/records")
        else:
            if core_profile.body_length(self.headers, required=False):
                raise core_profile.ProfileError(400, "Core request body is unsupported for this method")
            payload = None
        if canonical.startswith("/storage/v2/"):
            return self._core_storage(method, canonical, parsed.query, payload)
        if canonical.startswith("/schema-service/v1/"):
            return self._core_schema(method, canonical, parsed.query, payload)
        if canonical.startswith("/search/v2/"):
            return self._core_search(method, canonical, parsed.query, payload)
        if canonical.startswith(("/file/v2/", "/dataset/v1/")):
            return self._core_files(method, canonical, parsed.query, payload)
        core_profile.reject_pending_operation(method, parsed.path, parsed.query, payload)

    def _file_download(self, rid, expiry):
        with STORE_LOCK:
            record = self._record(rid)
            if record is None or record["kind"] != DATASET_KIND:
                raise BackendError(404, "Record not found")
            binding = FILE_BINDINGS.get(rid)
            if binding is None or FILE_BACKEND is None:
                raise BackendError(503, "file_unavailable")
            FILE_BACKEND.probe(binding["finalKey"])
            return FILE_BACKEND.signed_url(binding["finalKey"], time.time() + expiry)

    def _core_files(self, method, path, query, payload):
        file_root, dataset_root = "/file/v2/files", "/dataset/v1"
        if path == file_root + "/uploadURL" and method == "GET":
            params = core_profile.query_values(query, {"expiryTime"})
            return self._json(200, issue_file_grant(
                self.principal, file_expiry(params.get("expiryTime", "1H"))))
        if path == file_root + "/metadata" and method == "POST":
            core_profile.query_values(query, set())
            if not isinstance(payload, dict):
                raise ValueError("Expected one File.Generic record")
            records = publish_file_records([payload], principal=self.principal)
            return self._json(201, {"id": records[0]["id"]})
        if path.startswith(file_root + "/"):
            suffix = path[len(file_root) + 1:]
            if suffix.endswith("/downloadURL") and method == "GET":
                params = core_profile.query_values(query, {"expiryTime"})
                rid = core_profile.record_id(unquote(suffix[:-len("/downloadURL")]), PARTITION)
                return self._json(200, {"SignedUrl": self._file_download(
                    rid, file_expiry(params.get("expiryTime", "1H")))})
            if suffix.endswith("/metadata") and method in {"GET", "DELETE"}:
                core_profile.query_values(query, set())
                rid = core_profile.record_id(unquote(suffix[:-len("/metadata")]), PARTITION)
                record = self._record(rid)
                if record is None or record["kind"] != DATASET_KIND:
                    raise BackendError(404, "Record not found")
                if method == "DELETE":
                    self._delete([rid])
                    return self._no_content()
                return self._json(200, record)
        if path == dataset_root + "/storageInstructions" and method == "POST":
            params = core_profile.query_values(query, {"kindSubType", "expiryTime"})
            if params.get("kindSubType", "dataset--File.Generic") != "dataset--File.Generic":
                raise BackendError(501, "unsupported_kind_subtype")
            if payload not in ({}, None):
                raise ValueError("Storage instructions take no body fields")
            grant = issue_file_grant(self.principal, file_expiry(params.get("expiryTime", "1H")))
            return self._json(200, {"storageLocation": {
                "signedUrl": grant["Location"]["SignedUrl"],
                "fileSource": grant["Location"]["FileSource"]}, "providerKey": "AZURE"})
        if path == dataset_root + "/registerDataset" and method == "PUT":
            core_profile.query_values(query, set())
            if not isinstance(payload, dict) or set(payload) != {"datasetRegistries"}:
                raise ValueError("Expected datasetRegistries")
            records = publish_file_records(payload["datasetRegistries"], principal=self.principal)
            return self._json(201, {"datasetRegistries": records})
        if path == dataset_root + "/retrievalInstructions" and method in {"POST", "GET"}:
            params = core_profile.query_values(query, {"id", "expiryTime"} if method == "GET" else {"expiryTime"})
            if method == "GET":
                ids = [params.get("id")]
            else:
                if not isinstance(payload, dict) or set(payload) != {"datasetRegistryIds"}:
                    raise ValueError("Expected datasetRegistryIds")
                ids = payload["datasetRegistryIds"]
            if not isinstance(ids, list) or not 1 <= len(ids) <= 20:
                raise ValueError("Expected 1..20 registry IDs")
            for rid in ids:
                core_profile.record_id(rid, PARTITION)
            expiry = file_expiry(params.get("expiryTime", "1H"))
            items = []
            for rid in ids:
                if self._record(rid) is None:
                    continue
                items.append({"datasetRegistryId": rid, "retrievalProperties": {
                    "signedUrl": self._file_download(rid, expiry)}, "providerKey": "AZURE"})
            return self._json(200, {"datasets": items})
        core_profile.reject_pending_operation(method, path, query, payload)

    def _core_schema(self, method, path, query, payload):
        root = "/schema-service/v1/schema"
        if path.rstrip("/") == "/schema-service/v1/schemas/system" and method in {"POST", "PUT"}:
            raise BackendError(501, "unsupported_system_schema_write")
        if path == root and method == "GET":
            params = core_profile.query_values(query, {
                "authority", "source", "entityType", "schemaVersionMajor",
                "schemaVersionMinor", "schemaVersionPatch", "status", "scope",
                "latestVersion", "offset", "limit"})
            return self._json(200, SCHEMA_REGISTRY.list_core(params))
        core_profile.query_values(query, set())
        if path.startswith(root + "/") and method == "GET":
            entry = SCHEMA_REGISTRY.get(unquote(path[len(root) + 1:]))
            if entry is None:
                return self._error(404, "Schema not found")
            return self._json(200, entry["schema"])
        if path == root and method in {"POST", "PUT"}:
            with STORE_LOCK, storage_transaction():
                status, response = SCHEMA_REGISTRY.register(payload, method, core=True)
            return self._json(status, response)
        raise BackendError(501, "unsupported_core_schema_operation")

    def _core_storage(self, method, path, query, payload):
        root = "/storage/v2/records"
        if path == root and method == "PUT":
            params = core_profile.query_values(query, {"skipdupes"})
            skip = core_profile.boolean(params.get("skipdupes", "false"))
            if not isinstance(payload, list) or not 1 <= len(payload) <= 100:
                raise ValueError("Expected 1..100 records")
            skipped = []
            changed = self._upsert(payload, skipdupes=skip, skipped=skipped)
            return self._json(201, {
                "recordCount": len(changed), "recordIds": [r["id"] for r in changed],
                "recordIdVersions": [f'{r["id"]}:{r["version"]}' for r in changed],
                "skippedRecordIds": skipped,
            })
        if path == root and method == "PATCH":
            core_profile.query_values(query, set())
            data_patch = self.headers.get("Content-Type").split(";")[0].strip().lower() == "application/json-patch+json"
            ids, ops = core_profile.patch_request(payload, PARTITION, data_patch=data_patch)
            status, body = _patch_records(ids, ops, principal=self.principal, data_patch=data_patch)
            return self._json(status, body)
        if path == root and method == "GET":
            params = core_profile.query_values(query, {
                "kind", "deleted", "modifiedAfterDate", "sortOrder", "limit", "cursor"})
            if "cursor" in params:
                if set(params) != {"cursor"}:
                    raise ValueError("Cursor continuation requires cursor only")
                with STORE_LOCK, CURSOR_LOCK:
                    token = params["cursor"]
                    state = self._cursor_state(token)
                    if state.get("service") != "storage":
                        raise BackendError(400, "Invalid storage cursor")
                    pos, limit = state["pos"], state["limit"]
                    count = len(state["refs"])
                    page = self._cursor_records(state, pos, pos + limit)
                    state["pos"] = min(pos + limit, count)
                    next_token = token if state["pos"] < count else None
                    if next_token is None:
                        CURSORS.pop(token, None)
                    return self._json(200, {"results": page, "cursor": next_token})
            limit = params.get("limit", "20")
            if not re.fullmatch(r"[0-9]{1,3}", limit) or not 1 <= int(limit) <= 100:
                raise ValueError("Invalid limit")
            deleted = core_profile.boolean(params.get("deleted", "false"))
            order = params.get("sortOrder", "DESC")
            if order not in ("ASC", "DESC"):
                raise ValueError("Invalid sortOrder")
            kind = params.get("kind")
            if kind is not None and (len(kind.split(":")) != 4 or any(c in kind for c in "*?[]")):
                raise ValueError("Exact kind required")
            after = core_profile.timestamp(params["modifiedAfterDate"]) if "modifiedAfterDate" in params else None
            with STORE_LOCK:
                records = [effective_record(record) for rid, record in RECORD_BY_ID.items()
                           if (rid in DELETED) == deleted and (kind is None or record["kind"] == kind)
                           and readable(record, principal=self.principal, owner=deleted, deleted=deleted)]
                def stamp(record):
                    return core_profile.timestamp(record.get("modifyTime", record["createTime"]))
                records = [r for r in records if after is None or stamp(r) > after]
                records.sort(key=lambda r: (stamp(r), r["id"]), reverse=order == "DESC")
                self._observe(records, REVISION)
                token = None
                if len(records) > int(limit):
                    token = uuid.uuid4().hex.upper()
                    with CURSOR_LOCK:
                        CURSORS[token] = self._cursor({"results": records, "pos": int(limit),
                            "limit": int(limit), "service": "storage", "owner": deleted})
                return self._json(200, {"results": records[:int(limit)], "cursor": token})
        if path == "/storage/v2/query/records:batch" and method == "POST":
            core_profile.query_values(query, set())
            if not isinstance(payload, dict) or set(payload) != {"records"}:
                raise ValueError("Expected records array")
            ids = payload["records"]
            if not isinstance(ids, list) or not 1 <= len(ids) <= 100:
                raise ValueError("Expected 1..100 IDs")
            for rid in ids:
                core_profile.record_id(rid, PARTITION)
            with STORE_LOCK:
                records = [record for rid in ids if (record := self._record(rid))]
            return self._json(200, {"records": records})
        if path.startswith(root + "/"):
            core_profile.query_values(query, set())
            suffix = path[len(root) + 1:]
            if suffix.startswith("versions/") and method == "GET":
                rid = core_profile.record_id(unquote(suffix[len("versions/"):]), PARTITION)
                with STORE_LOCK:
                    if self._record(rid) is None:
                        raise BackendError(404, "Record not found")
                    versions = [entry["version"] for entry in RECORD_VERSIONS[rid]]
                return self._json(200, {"recordId": rid, "versions": versions})
            if suffix.endswith(":delete") and method == "POST":
                rid = core_profile.record_id(unquote(suffix[:-len(":delete")]), PARTITION)
                if payload not in (None, {}):
                    raise ValueError("Delete takes no fields")
                self._delete([rid])
                return self._no_content()
            if method == "DELETE":
                rid = core_profile.record_id(unquote(suffix), PARTITION)
                self._delete([rid], hard=True)
                return self._no_content()
            if method == "GET":
                parts = suffix.split("/")
                rid = core_profile.record_id(unquote(parts[0]), PARTITION)
                if len(parts) > 2 or (len(parts) == 2 and not re.fullmatch(r"[1-9][0-9]{0,18}", parts[1])):
                    raise ValueError("Invalid record version path")
                with STORE_LOCK:
                    record = self._record(rid)
                    if record is not None and len(parts) == 2:
                        record = next((effective_record(e["record"]) for e in RECORD_VERSIONS[rid]
                                       if e["version"] == int(parts[1])), None)
                if record is None:
                    raise BackendError(404, "Record not found")
                return self._json(200, record)
        core_profile.reject_pending_operation(method, path, query, payload)

    def log_message(self, fmt, *args):
        # Request lines can contain bearer upload capabilities and SAS query strings.
        print(f"[{time.strftime('%H:%M:%S')}] HTTP request")

    def _json(self, status, payload, headers=None):
        if status < 300 and getattr(self, "_read_response", False):
            with STORE_LOCK:
                if (self._policy_stale or (
                        self._policy_revision is not None and self._policy_revision != REVISION)
                        or any(not read_record(rid, principal=self.principal, deleted=self._deleted_read,
                                               owner=self._deleted_read) for rid in self._read_ids)):
                    raise BackendError(503, "concurrent_mutation_retry")

                def overlay(value):
                    if isinstance(value, dict):
                        if all(key in value for key in ("id", "kind", "acl", "legal", "data")):
                            current = read_record(value["id"], principal=self.principal,
                                                  deleted=self._deleted_read, owner=self._deleted_read)
                            if current is None:
                                raise BackendError(503, "concurrent_mutation_retry")
                            for field in ("acl", "legal", "tags"):
                                if field in current:
                                    value[field] = deepcopy(current[field])
                        else:
                            for child in value.values():
                                overlay(child)
                    elif isinstance(value, list):
                        for child in value:
                            overlay(child)
                if not getattr(self, "_projection_done", False):
                    overlay(payload)
                return self._write_json(status, payload, headers)
        return self._write_json(status, payload, headers)

    def _write_json(self, status, payload, headers=None):
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        correlation_id = self.headers.get("correlation-id")
        if correlation_id:
            self.send_header("correlation-id", correlation_id)
        if headers:
            for k, v in headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status, message, headers=None):
        for secret in IDENTITIES:
            if secret:
                message = message.replace(secret, "[redacted]")
        self._json(status, {"code": status, "reason": message, "message": message}, headers=headers)

    def _no_content(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        if self.headers.get("correlation-id"):
            self.send_header("correlation-id", self.headers.get("correlation-id"))
        self.end_headers()

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        if not length:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            self._error(400, "Invalid JSON body")
            return None

    def _is_public_path(self, path):
        return path in {"/health", "/info"} or (
            path.startswith("/files/") and immutable_sample(unquote(path[len("/files/"):])))

    def _reference_request(self, method, parsed):
        group = reference_services.route_group(method, parsed.path)
        if group is None:
            return False
        if not self._rbac({group}):
            return True
        try:
            payload = None
            if method == "POST":
                if self.headers.get("Transfer-Encoding"):
                    raise reference_services.ReferenceError(400, "Transfer-Encoding is unsupported")
                length_text = self.headers.get("Content-Length", "0")
                length = reference_services.integer(length_text, "Content-Length")
                if length > reference_services.MAX_BODY:
                    self.close_connection = True
                    raise reference_services.ReferenceError(413, "Reference service body exceeds 1 MiB")
                raw = self.rfile.read(length)
                if len(raw) != length:
                    raise reference_services.ReferenceError(400, "Incomplete JSON body")
                payload = reference_services.strict_json(raw)
            result = reference_services.handle(
                method, parsed.path, parse_qs(parsed.query, keep_blank_values=True), payload)
            # Validate new-service responses without altering legacy JSON behavior.
            json.dumps(result, allow_nan=False)
            self._json(200, result)
        except reference_services.ReferenceError as exc:
            self._error(exc.status, str(exc))
        except Exception:
            self._error(500, "Reference service operation failed")
        return True

    def _community_request(self, method, parsed):
        # Preserve every existing strict reference endpoint and its legacy envelope.
        if reference_services.route_group(method, parsed.path) is not None:
            return False
        route = community_services.match(method, parsed.path)
        if route is None:
            return False
        if not self._rbac({community_services.group(route[0])}):
            return True
        try:
            if self.headers.get("Transfer-Encoding"):
                self.close_connection = True
                raise reference_services.ReferenceError(400, "Transfer-Encoding is unsupported")
            length = reference_services.integer(self.headers.get("Content-Length", "0"), "Content-Length")
            if length > reference_services.MAX_BODY:
                self.close_connection = True
                raise reference_services.ReferenceError(413, "Community service body exceeds 1 MiB")
            if method == "GET" and length:
                raise reference_services.ReferenceError(400, "GET request bodies are unsupported")
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise reference_services.ReferenceError(400, "Incomplete JSON body")
            payload = reference_services.strict_json(raw) if raw else {}
            context = RequestServices(self, route, method)
            if route[0] == "storage" and route[1] == "/replay/status/{id}":
                with community_services.LOCK, STORE_LOCK:
                    summary = REPLAY_OWNERS.get(route[2][0])
                    if (self.principal is not core_auth.SYSTEM and (
                            not summary or summary["principal"] != self.principal.name
                            or summary["revision"] != REVISION
                            or any(not self._record(rid) for rid in summary["ids"]))):
                        raise BackendError(404, "Replay not found")
            if route[0] == "file" and method == "POST":
                # The legacy account-wide revoke mock may only remove this caller's grants.
                with STORE_LOCK:
                    owned = [token for token, grant in UPLOAD_TOKENS.items()
                             if grant.get("principal") == self.principal.name]
                    for token in owned:
                        UPLOAD_TOKENS.pop(token, None)
                self._no_content()
                return True
            if route[0] == "entitlements" and method in {"POST", "PUT", "PATCH", "DELETE"}:
                with community_services.LOCK, STORE_LOCK, storage_transaction():
                    status, result = community_services.handle(
                        context, route, method, parse_qs(parsed.query, keep_blank_values=True),
                        payload, group_for_caller(self))
            else:
                status, result = community_services.handle(
                    context, route, method, parse_qs(parsed.query, keep_blank_values=True),
                    payload, group_for_caller(self))
            if route[0] == "storage" and route[1] == "/replay" and status == 202:
                with community_services.LOCK, STORE_LOCK:
                    REPLAY_OWNERS[result["replayId"]] = {
                        "principal": self.principal.name, "revision": self._policy_revision,
                        "ids": sorted(self._read_ids),
                    }
            json.dumps(result, allow_nan=False)
            if status == 204:
                self._no_content()
            elif route[0] == "secret" and route[1] == "/v1/health":
                body = result.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Access-Control-Allow-Origin", "*")
                if self.headers.get("correlation-id"):
                    self.send_header("correlation-id", self.headers.get("correlation-id"))
                self.end_headers()
                self.wfile.write(body)
            else:
                self._json(status, result)
        except (reference_services.ReferenceError, BackendError, CapacityError) as exc:
            self._error(exc.status, str(exc), {"Retry-After": "1"} if exc.status == 429 else None)
        except Exception:
            # Do not log request data: a request may contain a mock credential.
            self._error(500, "Community service operation failed")
        return True

    def _check_auth(self, path):
        auth = self.headers.get("Authorization", "")
        self.principal = IDENTITIES.get(auth[7:]) if auth.startswith("Bearer ") else None
        if not REQUIRE_AUTH:
            self.principal = core_auth.SYSTEM
            return True
        if self._is_public_path(path):
            self.principal = self.principal or core_auth.ANONYMOUS
            return True
        partition = self.headers.get("data-partition-id")
        if self.principal is None:
            self._error(401, "Unknown or missing bearer identity")
            return False
        if partition != PARTITION:
            self._error(400, f"Use data-partition-id: {PARTITION}")
            return False
        if not RUNTIME_ACCEPTING and not path.endswith(("liveness_check", "/info")):
            raise BackendError(503, "runtime_not_started")
        if "/entitlements/" in path and self.command_name in {"POST", "PUT", "PATCH", "DELETE"}:
            if not self._rbac({"service.entitlements.admin"}):
                return False
        return True

    def _rbac(self, required):
        if not REQUIRE_AUTH or not required:
            return True
        path = core_profile.canonical_path(urlparse(self.path).path)
        method = self.command_name
        if path.startswith(("/file/", "/dataset/", "/schema-service/")):
            service = ("file" if path.startswith("/file/") else
                       "dataset" if path.startswith("/dataset/") else "schema-service")
            read = method == "GET" and not path.endswith("/uploadURL")
            read = read or (method == "POST" and path.endswith((
                "/getDatasetRegistry", "/retrievalInstructions", "/getRetrievalInstructions",
                "/getFileSignedUrl")))
            required = {f"service.{service}.editors"}
            if read:
                required.add(f"service.{service}.viewers")
            if COMPAT_PROFILE == "legacy":
                required |= core_auth.roles(
                    {"service.storage.viewer" if read else "service.storage.editor"}, "legacy")
        groups = group_for_caller(self)
        if groups & core_auth.roles(required, COMPAT_PROFILE):
            return True
        self._error(403, "Insufficient entitlements, requires one of: " + ", ".join(sorted(required)))
        return False

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type, data-partition-id, correlation-id, frame-of-reference")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
        self.end_headers()

    @backend_errors
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        if not self._check_auth(path):
            return

        if self._community_request("GET", parsed) or self._reference_request("GET", parsed):
            return

        if path in {"/health", "/info"}:
            indexed_count()  # Preserve backend readiness semantics, not an unrestricted public count.
            indexed = len(self._search({"kind": "*"}))
            with STORE_LOCK:
                visible = [r for r in RECORD_BY_ID.values() if readable(r, principal=self.principal)]
                self._observe(visible, REVISION)
                stored = len(visible)
                compatibility = core_profile.inventory(COMPAT_PROFILE, SUPPORTED_KINDS, MAX_RECORD_BYTES)
                if COMPAT_PROFILE == "core" and FILE_BACKEND is not None:
                    for feature in ("file-sas", "dataset-files"):
                        compatibility["supportedFeatures"].append(feature)
                        compatibility["deferredFeatures"].remove(feature)
                    compatibility["supportedOperations"].extend([
                        "GET /api/file/v2/files/uploadURL", "POST /api/file/v2/files/metadata",
                        "GET /api/file/v2/files/{id}/metadata", "DELETE /api/file/v2/files/{id}/metadata",
                        "GET /api/file/v2/files/{id}/downloadURL",
                        "POST /api/dataset/v1/storageInstructions", "PUT /api/dataset/v1/registerDataset",
                        "GET /api/dataset/v1/retrievalInstructions", "POST /api/dataset/v1/retrievalInstructions"])
                    compatibility["limits"].update(fileBytes=256 * 1024 * 1024,
                                                    fileFinalizations=2, sasSeconds=3600)
                compatibility["snapshotFormat"] = 2
                compatibility["supportedFeatures"].append("durable-core-state")
                compatibility["deferredFeatures"].remove("durable-core-state")
                compatibility["limits"].update({
                    "groups": 256, "membersPerGroup": 256, "legalTags": 256,
                    "pendingUploadGrants": 256, "fileBindings": 5000,
                    "controlStateBytes": MAX_CONTROL_BYTES, "snapshotBytes": 144 * 1024 * 1024,
                })
            counts = {name: sum(r["kind"] == kind for r in visible)
                      for name, kind in (("wells", WELL_KIND), ("wellbores", WELLBORE_KIND),
                                         ("welllogs", WELLLOG_KIND), ("datasets", DATASET_KIND))}
            full_sample_loaded = {r["id"] for r in visible} == {r["id"] for r in FULL_SAMPLE}
            return self._json(200, {
                "service": "osdu-well360-local",
                "status": "UP",
                "version": "0.7",
                "partition": PARTITION,
                "storedRecords": stored,
                "indexedRecords": indexed,
                "indexDelaySeconds": INDEX_DELAY_SECONDS,
                "preloadFullSampleConfigured": PRELOAD_FULL_SAMPLE,
                "preloadedFullSample": full_sample_loaded,
                "currentDataProfile": "empty" if not visible else "full-sample" if full_sample_loaded else "custom",
                "recordsByKind": counts,
                "compatibility": compatibility,
            })

        if path in {"/api/entitlements/v1/groups", "/entitlements/v1/groups"}:
            role_required = query.get("roleRequired", ["false"])[0].lower() in {"1", "true", "yes"}
            with GROUPS_LOCK:
                names = sorted(g["name"] for g in GROUPS.values())
                groups = []
                for name in names:
                    group = GROUPS.get(group_email(name))
                    item = {
                        "name": name,
                        "email": group["email"] if group else group_email(name),
                        "description": group["description"] if group else "",
                    }
                    if role_required:
                        item["role"] = "OWNER"
                    groups.append(item)
            return self._json(200, {"groups": groups})

        if path in {"/api/entitlements/v2/groups", "/entitlements/v2/groups"}:
            if not self._rbac({"service.entitlements.user"}):
                return
            caller = group_for_caller(self)
            role_required = query.get("roleRequired", ["false"])[0].lower() in {"1", "true", "yes"}
            with GROUPS_LOCK:
                groups = sorted((
                    {
                        "name": g["name"],
                        "email": g["email"],
                        "description": g["description"],
                    }
                    for g in GROUPS.values()
                    if g["name"] in caller or g["email"] in caller
                ), key=lambda g: g["name"])
            if role_required:
                for g in groups:
                    g["role"] = "OWNER"
            return self._json(200, {
                "desId": self.principal.name,
                "memberEmail": self.principal.name,
                "groups": groups,
            })

        for prefix in ("/api/entitlements/v2/members/", "/entitlements/v2/members/"):
            if path.startswith(prefix) and path.endswith("/groups"):
                if not self._rbac({"service.entitlements.user", "service.entitlements.admin"}):
                    return
                email = unquote(path[len(prefix):-len("/groups")])
                with GROUPS_LOCK:
                    groups = sorted((
                        {
                            "name": g["name"],
                            "email": g["email"],
                            "description": g["description"],
                        }
                        for g in GROUPS.values() if email in g["members"]
                    ), key=lambda g: g["name"])
                return self._json(200, {"desId": email, "memberEmail": email, "groups": groups})

        for prefix in ("/api/entitlements/v2/groups/", "/entitlements/v2/groups/"):
            if not path.startswith(prefix):
                continue
            rest = unquote(path[len(prefix):])
            if rest.endswith("/membersCount"):
                if not self._rbac({"service.entitlements.user"}):
                    return
                group_email_value = rest[:-len("/membersCount")]
                role = query.get("role", [None])[0]
                with GROUPS_LOCK:
                    group = GROUPS.get(group_email_value)
                    if not group:
                        return self._error(404, f"Group not found: {group_email_value}")
                    members = {
                        m_email: m_role
                        for m_email, m_role in group["members"].items()
                        if not role or m_role.lower() == role.lower()
                    }
                return self._json(200, {"count": len(members)})
            if rest.endswith("/members"):
                if not self._rbac({"service.entitlements.user"}):
                    return
                group_email_value = rest[:-len("/members")]
                with GROUPS_LOCK:
                    group = GROUPS.get(group_email_value)
                    if not group:
                        return self._error(404, f"Group not found: {group_email_value}")
                    include_type = query.get("includeType", ["false"])[0].lower() in {"1", "true", "yes"}
                    role = query.get("role", [None])[0]
                    members = []
                    for m_email, m_role in sorted(group["members"].items()):
                        if role and m_role.lower() != role.lower():
                            continue
                        item = {"email": m_email, "role": m_role}
                        if include_type:
                            item["type"] = "USER"
                        members.append(item)
                return self._json(200, {"members": members})

        if path in {"/api/legal/v1/legaltags:properties", "/legal/v1/legaltags:properties"}:
            if not self._rbac({"users.datalake.viewers"}):
                return
            return self._json(200, {"legaltagProperties": LEGAL_TAG_PROPERTIES})

        if path in {"/api/legal/v1/legaltags", "/legal/v1/legaltags"}:
            if not self._rbac({"users.datalake.viewers"}):
                return
            valid_only = query.get("valid", ["true"])[0].lower() not in {"0", "false", "no"}
            tags = []
            for name in sorted(LEGAL_TAGS):
                if is_legal_tag_valid(name) != valid_only:
                    continue
                tags.append(legal_tag_payload(name))
            return self._json(200, {"legalTags": tags})

        for prefix in ("/api/legal/v1/legaltags/", "/legal/v1/legaltags/"):
            if path.startswith(prefix):
                name = unquote(path[len(prefix):])
                if name not in LEGAL_TAGS:
                    return self._error(404, f"LegalTag not found: {name}")
                return self._json(200, {**LEGAL_TAGS[name], "isValid": is_legal_tag_valid(name)})

        if path in {"/api/storage/v2/query/records", "/storage/v2/query/records"}:
            if not self._rbac({"service.storage.viewer"}):
                return
            kind = query.get("kind", [None])[0]
            limit = min(max(int(query.get("limit", [10])[0]), 1), 1000)
            with STORE_LOCK:
                matched = storage_records_snapshot(kind, False, None, False, principal=self.principal)
                self._observe(matched, REVISION)
            record_ids = [r["id"] for r in matched]
            page = record_ids[:limit]
            next_cursor = None
            if len(record_ids) > limit:
                next_cursor = "sr-" + uuid.uuid4().hex[:16]
                with STORE_LOCK, CURSOR_LOCK:
                    CURSORS[next_cursor] = self._cursor({
                        "results": [deepcopy(r) for r in matched],
                        "service": "legacy-storage",
                        "pos": limit,
                        "updated": time.time(),
                    })
            return self._json(200, {"records": page, "cursor": next_cursor})

        if path in {"/api/storage/v2/query/kinds", "/storage/v2/query/kinds"}:
            limit = min(max(int(query.get("limit", [100])[0]), 1), 100)
            return self._json(200, SCHEMA_REGISTRY.kinds()[:limit])

        if path in {"/api/storage/v2/records", "/storage/v2/records"}:
            if not self._rbac({"service.storage.viewer"}):
                return
            kind = query.get("kind", [None])[0]
            try:
                limit = min(max(int(query.get("limit", [20])[0]), 1), 1000)
            except ValueError:
                limit = 20
            deleted_only = query.get("deleted", ["false"])[0].lower() in {"1", "true", "yes"}
            descending = query.get("sortOrder", ["asc"])[0].lower() == "desc"
            modify_epoch = iso_to_epoch(query.get("modifyAfterDate", [None])[0])
            cursor = query.get("cursor", [None])[0]
            if cursor:
                with STORE_LOCK, CURSOR_LOCK:
                    state = self._cursor_state(cursor, "legacy-storage")
                    pos = state["pos"]
                    page = self._cursor_records(state, pos, pos + limit)
                    state["pos"] = pos + limit
                    state["updated"] = time.time()
                    next_cursor = cursor if state["pos"] < len(state["refs"]) else None
                    if next_cursor is None:
                        CURSORS.pop(cursor, None)
                return self._json(200, {"records": page, "cursor": next_cursor})
            with STORE_LOCK:
                matched = storage_records_snapshot(kind, deleted_only, modify_epoch, descending, principal=self.principal)
                self._observe(matched, REVISION)
            page = [deepcopy(r) for r in matched[:limit]]
            if len(matched) > limit:
                cursor = "sr-" + uuid.uuid4().hex[:16]
                with STORE_LOCK, CURSOR_LOCK:
                    CURSORS[cursor] = self._cursor({
                        "results": [deepcopy(r) for r in matched],
                        "service": "legacy-storage",
                        "pos": limit,
                        "updated": time.time(),
                    })
                    return self._json(200, {"records": page, "cursor": cursor})
            return self._json(200, {"records": page, "cursor": None})

        for prefix in ("/api/storage/v2/records/versions/", "/storage/v2/records/versions/"):
            if path.startswith(prefix):
                if not self._rbac({"service.storage.viewer"}):
                    return
                rid = unquote(path[len(prefix):])
                self._record(rid)
                versions = version_numbers(rid, principal=self.principal)
                if not versions:
                    return self._error(404, f"Record not found: {rid}")
                return self._json(200, {"recordId": rid, "versions": versions})

        for prefix in ("/api/storage/v2/records/", "/storage/v2/records/"):
            if path.startswith(prefix):
                if not self._rbac({"service.storage.viewer"}):
                    return
                rest = unquote(path[len(prefix):])
                rid = rest
                if "/" in rest:
                    rid, version_text = rest.split("/", 1)
                    try:
                        version = int(version_text)
                    except ValueError:
                        return self._error(404, f"Record not found: {rid}")
                    rec = _record_version(rid, version, principal=self.principal)
                    if not rec:
                        return self._error(404, f"Record version not found: {rid} v{version}")
                    return self._json(200, rec)
                with STORE_LOCK:
                    rec = self._record(rid)
                    deleted = rid in DELETED
                if not rec or deleted:
                    return self._error(404, f"Record not found: {rid}")
                version_param = query.get("version", [None])[0]
                if version_param:
                    try:
                        stored = _record_version(rid, int(version_param), principal=self.principal)
                    except ValueError:
                        stored = None
                    if not stored:
                        return self._error(404, f"Record version not found: {rid} v{version_param}")
                    rec = stored
                attributes = query.get("attribute", [None])
                if attributes and attributes != [None]:
                    rec = project_record(rec, attributes)
                return self._json(200, rec)

        if path in {"/api/schema-service/v1/schema", "/schema-service/v1/schema"}:
            if not self._rbac({"service.storage.viewer"}):
                return
            with STORE_LOCK:
                infos = [schema_info_for(kind) for kind in SCHEMA_REGISTRY.kinds()]
            return self._json(200, infos)

        for prefix in ("/api/schema-service/v1/schema/", "/schema-service/v1/schema/"):
            if path.startswith(prefix):
                if not self._rbac({"service.schema-service.viewers", "service.schema-service.editors"}):
                    return
                kind = unquote(path[len(prefix):])
                info = schema_info_for(kind)
                if not info:
                    return self._error(404, f"Schema not found: {kind}")
                return self._json(200, info)

        if path in {"/api/dataset/v1/getDatasetRegistry/", "/dataset/v1/getDatasetRegistry/"}:
            if not self._rbac({"service.storage.viewer", "service.legal.user"}):
                return
            did = query.get("id", [None])[0]
            records = self._datasets([did])
            if not records:
                return self._error(404, f"Dataset not found: {did}")
            return self._json(200, {"datasetRegistries": records})

        if path in {"/api/dataset/v1/retrievalInstructions", "/dataset/v1/retrievalInstructions"}:
            if not self._rbac({"service.storage.viewer"}):
                return
            ids = query.get("id", [None])[0]
            items = []
            host = self.headers.get("Host", f"localhost:{PORT}")
            if ids:
                for did in [ids]:
                    if did in REVOKED_URLS:
                        continue
                    fname = self._filename(did)
                    if not fname:
                        continue
                    props = self._file_properties(did)
                    items.append({
                        "datasetRegistryId": did,
                        "retrievalProperties": props,
                    })
            return self._json(200, {"datasets": items})

        if path in {"/api/file/v2/files/uploadURL", "/file/v2/files/uploadURL"}:
            if not self._rbac({"users.datalake.viewers"}):
                return
            host = self.headers.get("Host", f"localhost:{PORT}")
            grant = issue_file_grant(self.principal, min(3600, UPLOAD_TOKEN_TTL), legacy_host=host)
            return self._json(200, {
                "fileID": grant["FileID"],
                "Location": {
                    **grant["Location"],
                    "expiresInSeconds": int(min(3600, UPLOAD_TOKEN_TTL)),
                },
            })

        for prefix in ("/api/file/v2/files/", "/file/v2/files/"):
            if not path.startswith(prefix):
                continue
            if not self._rbac({"service.storage.viewer", "service.legal.user"}):
                return
            rest = unquote(path[len(prefix):])
            host = self.headers.get("Host", f"localhost:{PORT}")
            if rest.endswith("/metadata"):
                fid = rest[:-len("/metadata")]
                with STORE_LOCK:
                    rec = self._record(fid)
                    deleted = fid in DELETED
                if not rec or deleted or not kind_matches("osdu:wks:dataset--*", rec.get("kind", "")):
                    return self._error(404, f"File not found: {fid}")
                return self._json(200, deepcopy(rec))
            if rest.endswith("/downloadURL") or rest.endswith("/DownloadURL"):
                capital = rest.endswith("/DownloadURL")
                fid = rest[:-len("/downloadURL")] if not capital else rest[:-len("/DownloadURL")]
                with STORE_LOCK:
                    rec = self._record(fid)
                    deleted = fid in DELETED
                if not rec or deleted or not kind_matches("osdu:wks:dataset--*", rec.get("kind", "")):
                    return self._error(404, f"File not found: {fid}")
                fname = self._filename(fid)
                if not fname:
                    return self._error(404, f"File source missing for: {fid}")
                body = self._file_properties(fid)
                if capital:
                    return self._json(200, {"signedURL": body["signedUrl"]})
                return self._json(200, body)
            with STORE_LOCK:
                rec = self._record(rest)
                deleted = rest in DELETED
            if not rec or deleted or not kind_matches("osdu:wks:dataset--*", rec.get("kind", "")):
                return self._error(404, f"File not found: {rest}")
            fname = self._filename(rest)
            location = self._file_properties(rest)
            ts = RECORD_TS.get(rest, {})
            init_time = _dt.datetime.fromtimestamp(
                ts.get("createTime", time.time()), tz=_dt.timezone.utc
            ).isoformat()
            return self._json(200, {
                "fileID": rest,
                "fileInitTime": init_time,
                "fileSourceDatasWithLocation": [
                    {"fileSource": fname or "", "location": location}
                ],
            })

        if path in {"/api/workflow/v1/workflow", "/workflow/v1/workflow"}:
            if not self._rbac({"service.storage.viewer"}):
                return
            with STORE_LOCK, WORKFLOW_LOCK:
                defs = [deepcopy(d) for d in sorted(WORKFLOW_DEFS.values(), key=lambda d: d["workflowName"])]
            return self._json(200, defs)

        for prefix in ("/api/workflow/v1/workflow/", "/workflow/v1/workflow/"):
            if path.startswith(prefix):
                rest = unquote(path[len(prefix):])
                segs = rest.split("/")
                if len(segs) == 3 and segs[1] == "workflowRun":
                    name, run_id = segs[0], segs[2]
                    if not self._rbac({"service.storage.viewer"}):
                        return
                    with STORE_LOCK, WORKFLOW_LOCK:
                        run = deepcopy(WORKFLOW_RUNS.get(run_id))
                    if not self._run_visible(run) or run.get("workflowName") != name:
                        return self._error(404, f"Workflow run not found: {run_id}")
                    return self._json(200, {"workflowRunId": run_id, **run})
                if len(segs) == 2 and segs[1] == "workflowRun":
                    if not self._rbac({"service.storage.viewer"}):
                        return
                    name = segs[0]
                    with STORE_LOCK, WORKFLOW_LOCK:
                        runs = [{"workflowRunId": rid, **deepcopy(r)}
                                for rid, r in sorted(WORKFLOW_RUNS.items())
                                if r.get("workflowName") == name and self._run_visible(r)]
                    return self._json(200, runs)
                if len(segs) == 1:
                    if not self._rbac({"service.storage.viewer"}):
                        return
                    name = segs[0]
                    if name in WORKFLOW_DEFS:
                        return self._json(200, deepcopy(WORKFLOW_DEFS[name]))
                    with STORE_LOCK, WORKFLOW_LOCK:
                        run = deepcopy(WORKFLOW_RUNS.get(name))
                    if self._run_visible(run):
                        return self._json(200, {"workflowRunId": name, **run})
                    return self._error(404, f"Workflow not found: {name}")
                return self._error(404, f"Unknown workflow endpoint: {path}")

        if path.startswith("/files/"):
            filename = unquote(path[len("/files/"):])
            if Path(filename).name != filename or "\\" in filename or filename in {".", ".."}:
                return self._error(400, "Invalid file path")
            with STORE_LOCK:
                sample = immutable_sample(filename)
                roles = {"service.file.viewers", "service.file.editors"}
                if COMPAT_PROFILE == "legacy":
                    roles |= core_auth.roles({"service.storage.viewer"}, "legacy")
                if not sample and not self._rbac(roles):
                    return
                binding = next((FILE_BINDINGS[rid] for rid in RECORD_BY_ID
                                if rid in FILE_BINDINGS and self._filename(rid) == filename), None)
                if not sample and binding is None:
                    return self._error(404, "File not found")
                if sample:
                    target = ROOT / "files" / filename
                    size = target.stat().st_size
                else:
                    if FILE_BACKEND is None:
                        raise BackendError(503, "file_backend_unavailable")
                    FILE_BACKEND.probe(binding["finalKey"])
                    size = binding["size"]
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Disposition", f'inline; filename="{filename}"')
                self.send_header("Content-Length", str(size))
                self.end_headers()
                if sample:
                    with target.open("rb") as stream:
                        while chunk := stream.read(1024 * 1024):
                            self.wfile.write(chunk)
                else:
                    for chunk in FILE_BACKEND.read(binding["finalKey"]):
                        if len(chunk) > 1024 * 1024:
                            raise BackendError(503, "file_unavailable")
                        self.wfile.write(chunk)
            return

        if path in {"/api/search/v2/liveness_check", "/search/v2/liveness_check",
                    "/api/search/v2/readiness_check", "/search/v2/readiness_check"}:
            if path.endswith("readiness_check"):
                indexed_count()
            return self._json(200, {"status": "UP"})

        self._error(404, f"Unknown endpoint: {path}")

    @backend_errors
    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        if not self._check_auth(path):
            return
        if self._community_request("POST", parsed) or self._reference_request("POST", parsed):
            return
        sweep_temp_state()
        payload = self._read_json()
        if payload is None:
            return

        if path in {"/api/storage/v2/records", "/storage/v2/records"}:
            if not self._rbac({"service.storage.editor"}):
                return
            records = payload if isinstance(payload, list) else [payload]
            try:
                result = self._upsert(records)
            except ValueError as e:
                return self._error(getattr(e, "status", 400), str(e))
            return self._json(200, {
                "recordCount": len(result),
                "recordIds": result,
                "skippedRecordIds": [],
            })

        if path in {"/api/search/v2/query", "/search/v2/query"}:
            if not self._rbac({"service.search.user"}):
                return
            found = self._search(payload)
            offset = max(0, int(payload.get("offset", 0)))
            limit = max(1, min(1000, int(payload.get("limit", 10))))
            page = found[offset:offset + limit]
            fields = payload.get("returnedFields")
            response = {
                "results": [project_record(r, fields) for r in page],
            }
            if payload.get("trackTotalCount", True) is not False:
                response["totalCount"] = len(found)
            if payload.get("aggregateBy"):
                response["aggregates"] = aggregate_records(found, payload["aggregateBy"])
            return self._json(200, response)

        if path in {"/api/search/v2/query_with_cursor", "/search/v2/query_with_cursor"}:
            if not self._rbac({"service.search.user"}):
                return
            cursor = payload.get("cursor")
            if cursor:
                with STORE_LOCK, CURSOR_LOCK:
                    state = self._cursor_state(cursor, "legacy-search")
                    pos = state["pos"]
                    limit = state["limit"]
                    fields = state["fields"]
                    page = self._cursor_records(state, pos, pos + limit)
                    state["pos"] = pos + limit
                    state["updated"] = time.time()
                    next_cursor = cursor if state["pos"] < len(state["refs"]) else None
                    if next_cursor is None:
                        CURSORS.pop(cursor, None)
                    return self._json(200, {
                        "results": [project_record(r, fields) for r in page],
                        "totalCount": len(state["refs"]),
                        "cursor": next_cursor,
                    })
            found = self._search(payload)
            limit = max(1, min(1000, int(payload.get("limit", 10))))
            fields = payload.get("returnedFields")
            cursor = uuid.uuid4().hex.upper() if len(found) > limit else None
            first = found[:limit]
            with STORE_LOCK, CURSOR_LOCK:
                if cursor:
                    CURSORS[cursor] = self._cursor({
                        "results": found, "pos": limit, "limit": limit,
                        "service": "legacy-search",
                        "owner": bool(payload.get("queryAsOwner") or payload.get("query_as_owner")),
                        "fields": fields, "updated": time.time()
                    })
            return self._json(200, {
                "results": [project_record(r, fields) for r in first],
                "totalCount": len(found),
                "cursor": cursor,
            })

        if path in {
            "/api/storage/v2/query/records:batch", "/storage/v2/query/records:batch",
            "/api/storage/v2/query/records", "/storage/v2/query/records"
        }:
            if not self._rbac({"service.storage.viewer"}):
                return
            ids = payload.get("records", [])
            with STORE_LOCK:
                records = [record for x in ids if (record := self._record(x)) is not None]
            return self._json(200, {"records": records})

        if path in {"/api/storage/v2/records/delete", "/storage/v2/records/delete"}:
            if not self._rbac({"users.datalake.editors"}):
                return
            ids = payload if isinstance(payload, list) else payload.get("records", [])
            if not isinstance(ids, list) or not ids:
                return self._error(400, "Expected a non-empty list of record ids")
            self._delete(ids)
            return self._no_content()

        if path in {"/api/dataset/v1/storageInstructions", "/dataset/v1/storageInstructions",
                    "/api/dataset/v1/getStorageInstructions", "/dataset/v1/getStorageInstructions"}:
            if not self._rbac({"service.storage.editor"}):
                return
            kind_subtype = query.get("kindSubType", ["dataset--File.Generic"])[0]
            host = self.headers.get("Host", f"localhost:{PORT}")
            if kind_subtype != "dataset--File.Generic":
                return self._error(400, "Unsupported kindSubType")
            grant = issue_file_grant(self.principal, min(3600, UPLOAD_TOKEN_TTL), legacy_host=host)
            return self._json(200, {
                "storageLocation": {
                    "signedUrl": grant["Location"]["SignedUrl"],
                    "FileSource": grant["Location"]["FileSource"],
                    "expiresInSeconds": int(min(3600, UPLOAD_TOKEN_TTL)),
                },
                "kindSubType": kind_subtype,
            })

        if path in {"/api/dataset/v1/retrievalInstructions", "/dataset/v1/retrievalInstructions",
                    "/api/dataset/v1/getRetrievalInstructions", "/dataset/v1/getRetrievalInstructions"}:
            if not self._rbac({"service.storage.viewer"}):
                return
            ids = payload.get("datasetRegistryIds") or payload.get("datasetRegistryId") or []
            if isinstance(ids, str):
                ids = [ids]
            host = self.headers.get("Host", f"localhost:{PORT}")
            items = []
            for did in ids:
                fname = self._filename(did)
                if not fname:
                    continue
                props = self._file_properties(did)
                items.append({
                    "datasetRegistryId": did,
                    "retrievalProperties": props,
                })
            return self._json(200, {"datasets": items})

        for prefix in ("/api/records/", "/records/"):
            if path.startswith(prefix):
                rest = unquote(path[len(prefix):])
                if rest.endswith(":copy"):
                    rid = rest[:-len(":copy")]
                    if not self._rbac({"service.storage.editor"}):
                        return
                    with STORE_LOCK:
                        exists = rid in RECORD_BY_ID and rid not in DELETED
                    if not exists:
                        return self._error(404, f"Record not found: {rid}")
                    acl = payload.get("acl") if isinstance(payload, dict) else None
                    new_id, entry = copy_record(rid, acl, principal=self.principal)
                    if new_id is None:
                        return self._error(404, "Record not found")
                    return self._json(200, {
                        "recordCount": 1,
                        "recordIds": [entry],
                        "skippedRecordIds": [],
                        "newRecordId": new_id,
                    })
                if rest.endswith(":delete"):
                    rid = rest[:-len(":delete")]
                    if not self._rbac({"users.datalake.editors"}):
                        return
                    with STORE_LOCK:
                        if rid not in RECORD_BY_ID:
                            return self._error(404, f"Record not found: {rid}")
                    self._delete([rid])
                    return self._no_content()

        if path in {"/api/dataset/v1/getDatasetRegistry", "/dataset/v1/getDatasetRegistry"}:
            if not self._rbac({"service.storage.viewer"}):
                return
            ids = payload.get("datasetRegistryIds") or payload.get("datasetRegistryId") or []
            if isinstance(ids, str):
                ids = [ids]
            return self._json(200, {"datasetRegistries": self._datasets(ids)})

        if path in {"/api/dataset/v1/revokeURL", "/dataset/v1/revokeURL"}:
            if not self._rbac({"service.storage.editor"}):
                return
            kind_subtype = query.get("kindSubType", [None])[0]
            target = payload.get("datasetRegistryIds") if isinstance(payload, dict) else payload
            target = target or payload
            if isinstance(target, str):
                target = [target]
            targets = [k for k in target if target[k]] if isinstance(target, dict) else target
            with STORE_LOCK:
                for rid in targets:
                    current = self._record(rid)
                    if current is None:
                        return self._error(404, "Record not found")
                    require_write(current, principal=self.principal)
            if isinstance(target, dict):
                REVOKED_URLS.update(k for k in target if target[k])
            elif isinstance(target, list):
                REVOKED_URLS.update(k for k in target if k)
            kind_text = f" kindSubType={kind_subtype}" if kind_subtype else ""
            return self._json(200, {
                "status": "revoked",
                "recordCount": len(REVOKED_URLS),
                "message": f"Revoked URLs{kind_text}",
            })

        for prefix in ("/api/dataset/v1/metadataRecord/", "/dataset/v1/metadataRecord/"):
            if path.startswith(prefix) and path.endswith("/softDelete"):
                if not self._rbac({"service.storage.editor"}):
                    return
                rid = unquote(path[len(prefix):-len("/softDelete")])
                with STORE_LOCK:
                    if rid not in RECORD_BY_ID:
                        return self._error(404, f"Record not found: {rid}")
                self._delete([rid])
                return self._no_content()

        if path in {"/api/legal/v1/legaltags:validate", "/legal/v1/legaltags:validate"}:
            if not self._rbac({"users.datalake.viewers"}):
                return
            names = payload.get("legaltags") or payload.get("name") or []
            if isinstance(names, str):
                names = [names]
            invalid = []
            for tag_name in names:
                if not tag_name:
                    continue
                if is_legal_tag_valid(tag_name):
                    continue
                if tag_name in LEGAL_TAGS:
                    expiry = (LEGAL_TAGS[tag_name].get("properties") or {}).get("expirationDate")
                    reason = f"LegalTag not valid (expirationDate={expiry})"
                else:
                    reason = "LegalTag not found"
                invalid.append({"name": tag_name, "reason": reason})
            return self._json(200, {"invalidLegalTags": invalid})

        if path in {"/api/legal/v1/legaltags:batchRetrieve", "/legal/v1/legaltags:batchRetrieve"}:
            if not self._rbac({"users.datalake.viewers"}):
                return
            names = payload.get("legaltags") or payload.get("name") or []
            if isinstance(names, str):
                names = [names]
            tags = [LEGAL_TAGS[n] for n in names if n in LEGAL_TAGS]
            return self._json(200, {"legalTags": tags})

        if path in {"/api/legal/v1/legaltags:query", "/legal/v1/legaltags:query"}:
            if not self._rbac({"users.datalake.viewers"}):
                return
            valid_only = payload.get("valid", True)
            if isinstance(valid_only, str):
                valid_only = valid_only.lower() not in {"0", "false", "no"}
            return self._json(200, legal_tags_query(payload, bool(valid_only)))

        if path in {"/api/legal/v1/legaltags", "/legal/v1/legaltags"}:
            if not self._rbac({"users.datalake.editors"}):
                return
            name = payload.get("name")
            if not name or not isinstance(name, str):
                return self._error(400, "LegalTag name is required")
            props = payload.get("properties")
            tag = {
                "name": name,
                "description": payload.get("description", "LegalTag managed by OSDU-Lite"),
                "properties": deepcopy(props) if isinstance(props, dict)
                else deepcopy(default_legal_tag()["properties"]),
            }
            with STORE_LOCK, storage_transaction():
                if name in LEGAL_TAGS:
                    return self._error(409, f"LegalTag already exists: {name}")
                LEGAL_TAGS[name] = tag
            return self._json(201, {**tag, "isValid": is_legal_tag_valid(name)})

        if path in {"/api/entitlements/v2/groups", "/entitlements/v2/groups"}:
            if not self._rbac({"service.entitlements.admin"}):
                return
            name = payload.get("name")
            if not name or not isinstance(name, str) or not name:
                return self._error(400, "Group name is required")
            email = group_email(name)
            with STORE_LOCK, storage_transaction(), GROUPS_LOCK:
                if email in GROUPS:
                    return self._error(409, f"Group already exists: {email}")
                GROUPS[email] = {
                    "name": name,
                    "email": email,
                    "description": payload.get("description", f"{name} - entitlement group"),
                    "members": {},
                    "appIds": list(payload.get("aPpIds") or payload.get("appIds") or []),
                }
                created = {
                    "name": name,
                    "email": email,
                    "description": GROUPS[email]["description"],
                    "appIds": GROUPS[email]["appIds"],
                }
            return self._json(201, created)

        for prefix in ("/api/entitlements/v2/groups/", "/entitlements/v2/groups/"):
            if not path.startswith(prefix):
                continue
            rest = unquote(path[len(prefix):])
            if rest.endswith("/members"):
                if not self._rbac({"service.entitlements.user", "service.entitlements.admin"}):
                    return
                group_email_value = rest[:-len("/members")]
                with STORE_LOCK, storage_transaction(), GROUPS_LOCK:
                    group = GROUPS.get(group_email_value)
                    if not group:
                        return self._error(404, f"Group not found: {group_email_value}")
                    entries = payload.get("members", payload if isinstance(payload, list) else None)
                    if entries is None:
                        entries = [payload]
                    if isinstance(entries, dict):
                        entries = [entries]
                    added = []
                    for entry in entries:
                        if not isinstance(entry, dict) or not entry.get("email"):
                            return self._error(400, "Each member requires an 'email'")
                        m_role = str(entry.get("role", "MEMBER"))
                        if m_role.upper() not in {"OWNER", "MEMBER"}:
                            return self._error(400, "Member role must be OWNER or MEMBER")
                        added.append({"email": entry["email"], "role": m_role.upper()})
                    for member in added:
                        group["members"][member["email"]] = member["role"]
                return self._json(200, {"members": added})

        if path in {"/api/file/v2/files/metadata", "/file/v2/files/metadata"}:
            if not self._rbac({"service.storage.editor"}):
                return
            metas = payload.get("fileRecord", payload)
            if isinstance(metas, dict) and any(k in metas for k in ("id", "kindId", "data")):
                metas = [metas]
            elif isinstance(metas, dict):
                metas = metas.get("records", [metas])
            created_ids = []
            for meta in metas:
                if not isinstance(meta, dict):
                    return self._error(400, "Each file metadata entry must be an object")
                rid = meta.get("id") or f"{PARTITION}:dataset--File.Generic:{uuid.uuid4().hex[:12]}"
                kind = meta.get("kindId") or meta.get("kind") or DATASET_KIND
                data = meta.get("data") or {}
                source = data.get("FileSource") or ((data.get("DatasetProperties") or {}).get("FileSourceInfo") or {}).get("FileSource")
                if not source:
                    return self._error(400, "File metadata requires a FileSource")
                rec = deepcopy(meta)
                rec.pop("kindId", None)
                if "FileName" in rec:
                    data = dict(data, FileName=rec.pop("FileName"))
                    rec["data"] = data
                rec["id"] = rid
                rec["kind"] = kind
                rec.setdefault("acl", deepcopy(DEFAULT_ACL))
                legal = rec.setdefault("legal", {})
                if not legal.get("legaltags"):
                    legal["legaltags"] = [LEGAL_TAG]
                legal["otherRelevantDataCountries"] = legal.get("otherRelevantDataCountries", [])
                try:
                    publish_file_records([rec], principal=self.principal)
                except ValueError as e:
                    return self._error(getattr(e, "status", 400), str(e))
                created_ids.append({"id": rid})
            return self._json(201, created_ids[0] if len(created_ids) == 1 else {"ids": created_ids})

        if path in {"/api/file/v2/getLocation", "/file/v2/getLocation"}:
            if not self._rbac({"users.datalake.editors"}):
                return
            file_id = payload.get("FileID", "") if isinstance(payload, dict) else ""
            host = self.headers.get("Host", f"localhost:{PORT}")
            grant = issue_file_grant(self.principal, min(3600, UPLOAD_TOKEN_TTL), legacy_host=host)
            return self._json(200, {
                "fileID": file_id,
                "Location": {
                    "SignedURL": grant["Location"]["SignedUrl"],
                    "FileSource": grant["Location"]["FileSource"],
                    "expiresInSeconds": int(min(3600, UPLOAD_TOKEN_TTL)),
                },
            })

        if path in {"/api/file/v2/delivery/getFileSignedUrl", "/file/v2/delivery/getFileSignedUrl"}:
            if not self._rbac({"users.datalake.viewers"}):
                return
            srns = payload.get("srn") or payload.get("srns") or []
            if isinstance(srns, str):
                srns = [srns]
            host = self.headers.get("Host", f"localhost:{PORT}")
            processed = {}
            unprocessed = []
            for srn in srns:
                fname = self._filename(str(srn))
                if not fname:
                    unprocessed.append(srn)
                    continue
                body = self._file_properties(str(srn))
                processed[srn] = body
            return self._json(200, {"processed": processed, "unprocessed": unprocessed})

        if path in {"/api/file/v2/files", "/file/v2/files"}:
            if not self._rbac({"service.storage.editor"}):
                return
            records = payload if isinstance(payload, list) else payload.get("fileRecord")
            if records is None:
                records = [payload]
            if isinstance(records, dict):
                records = [records]
            created_files = []
            host = self.headers.get("Host", f"localhost:{PORT}")
            for rec in records:
                if not isinstance(rec, dict) or "id" not in rec or "kind" not in rec:
                    return self._error(400, "Each file body requires a dataset record with id and kind")
                try:
                    publish_file_records([rec], principal=self.principal)
                except ValueError as e:
                    return self._error(getattr(e, "status", 400), str(e))
                rec_id = rec["id"]
                fname = self._filename(rec_id)
                location = self._file_properties(rec_id)
                ts = RECORD_TS.get(rec_id, {})
                created_files.append({
                    "fileID": rec_id,
                    "fileInitTime": _dt.datetime.fromtimestamp(
                        ts.get("createTime", time.time()), tz=_dt.timezone.utc
                    ).isoformat(),
                    "fileSourceDatasWithLocation": [
                        {"fileSource": rec.get("data", {}).get("FileName"), "location": location}
                    ],
                })
            result = created_files[0] if len(created_files) == 1 else created_files
            return self._json(200, result)

        if path in {"/api/workflow/v1/workflow", "/workflow/v1/workflow"}:
            if not self._rbac({"service.storage.editor"}):
                return
            name = payload.get("workflowName")
            if not name or not isinstance(name, str):
                return self._error(400, "workflowName is required")
            wf = deepcopy(payload)
            wf.setdefault("workflowId", "wf-" + uuid.uuid4().hex[:12])
            wf.setdefault("createdby", "osdu-lite")
            wf.setdefault("creationTimestamp", time.time())
            with WORKFLOW_LOCK:
                WORKFLOW_DEFS[name] = wf
            return self._json(200, deepcopy(wf))

        for prefix in ("/api/workflow/v1/workflow/", "/workflow/v1/workflow/"):
            if path.startswith(prefix):
                rest = unquote(path[len(prefix):])
                segs = rest.split("/")
                if len(segs) != 2 or segs[1] != "workflowRun":
                    return self._error(404, f"Unknown workflow endpoint: {path}")
                name = segs[0]
                if name not in WORKFLOW_DEFS:
                    return self._error(404, f"Workflow not found: {name}")
                if not self._rbac({"service.storage.editor"}):
                    return
                if name == "Osdu_ingest":
                    run_id = "workflow-" + uuid.uuid4().hex[:12]
                    try:
                        records = manifest_records(payload)
                        result = self._upsert(records, run_id=run_id)
                    except ManifestError as e:
                        return self._json(400, {"code": 400, "reason": str(e),
                                                "message": str(e), "errors": e.errors})
                    except ValueError as e:
                        return self._error(getattr(e, "status", 400), str(e))
                    return self._json(200, {
                        "workflowRunId": run_id,
                        "workflowName": "Osdu_ingest",
                        "status": "submitted",
                        "recordCount": len(result),
                        "recordIds": result,
                    })
                return self._error(501, "unsupported_workflow: only Osdu_ingest can execute")

        if path in {"/api/schema-service/v1/schema", "/schema-service/v1/schema"}:
            if not self._rbac({"service.storage.editor"}):
                return
            try:
                with STORE_LOCK, storage_transaction():
                    status, response = SCHEMA_REGISTRY.register(payload, "POST")
            except SchemaError as e:
                return self._error(e.status, str(e), {"Retry-After": "1"} if e.status == 429 else None)
            return self._json(status, response)

        if path == "/poc/reset":
            if not self._rbac({"service.storage.admin"}):
                return
            _clear_state(principal=self.principal)
            return self._json(200, {
                "status": "reset",
                "storedRecords": 0,
                "indexedRecords": 0,
                "currentDataProfile": current_data_profile(),
                "recordsByKind": record_counts(),
            })

        if path == "/poc/load-full-sample":
            if not self._rbac({"service.storage.admin"}):
                return
            _load_full_sample(index_immediately=True, principal=self.principal)
            counts = record_counts()
            return self._json(200, {
                "status": "loaded",
                "storedRecords": len(RECORD_BY_ID),
                "indexedRecords": indexed_count(),
                "currentDataProfile": current_data_profile(),
                "preloadedFullSample": is_full_sample_loaded(),
                **counts,
            })

        self._error(404, f"Unknown endpoint: {path}")

    @backend_errors
    def do_PUT(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if not self._check_auth(path):
            return
        if self._community_request("PUT", parsed):
            return

        if path.startswith("/upload/"):
            roles = {"service.file.editors"}
            if COMPAT_PROFILE == "legacy":
                roles |= core_auth.roles({"service.storage.editor"}, "legacy")
            if not self._rbac(roles):
                return
            token = path[len("/upload/"):]
            digest = hashlib.sha256(token.encode()).hexdigest()
            with STORE_LOCK:
                grant = deepcopy(UPLOAD_GRANTS.get(digest))
                if (not grant or grant["owner"] != self.principal.name
                        or grant["expiresAt"] <= time.time()):
                    return self._error(404, "Unknown/expired upload token")
                backend, epoch = FILE_BACKEND, FILE_RUNTIME_EPOCH
                if backend is None:
                    raise BackendError(503, "file_backend_unavailable")
            filename = self.headers.get("x-file-name") or grant["stagingKey"].rsplit("/", 1)[1]
            if Path(filename).name != filename or "\\" in filename or filename in {".", ".."}:
                return self._error(400, "Invalid file path")
            length_text = self.headers.get("Content-Length")
            if (not isinstance(length_text, str) or not length_text.isascii()
                    or not length_text.isdecimal() or self.headers.get("Transfer-Encoding")):
                return self._error(400, "Invalid upload length")
            length = int(length_text)
            backend.upload(grant["stagingKey"], self.rfile, length)
            with STORE_LOCK:
                if not self._rbac(roles):
                    return
                current = UPLOAD_GRANTS.get(digest)
                if (current != grant or current["expiresAt"] <= time.time()
                        or FILE_BACKEND is not backend or FILE_RUNTIME_EPOCH != epoch):
                    return self._error(404, "Unknown/expired upload token")
                sha256 = hashlib.sha256()
                size = 0
                for chunk in backend.read(grant["stagingKey"]):
                    if len(chunk) > 1024 * 1024:
                        raise BackendError(503, "file_unavailable")
                    size += len(chunk)
                    sha256.update(chunk)
                if size != length:
                    raise BackendError(503, "file_unavailable")
                current = UPLOAD_GRANTS.get(digest)
                if current != grant or current["expiresAt"] <= time.time():
                    return self._error(404, "Unknown/expired upload token")
            return self._json(200, {
                "status": "uploaded",
                "fileName": filename,
                "size": size,
                "sha256": sha256.hexdigest(),
                "FileSource": grant["stagingKey"],
            })

        sweep_temp_state()
        payload = self._read_json()
        if payload is None:
            return

        if path in {"/api/legal/v1/legaltags", "/legal/v1/legaltags"}:
            if not self._rbac({"users.datalake.editors"}):
                return
            name = payload.get("name")
            with STORE_LOCK, storage_transaction():
                if not name or name not in LEGAL_TAGS:
                    return self._error(404, f"LegalTag not found: {name}")
                props = LEGAL_TAGS[name].get("properties", {})
                incoming = payload.get("properties", {})
                if incoming and isinstance(incoming, dict):
                    for key in ("contractId", "expirationDate", "extensionProperties"):
                        if key in incoming:
                            props[key] = incoming[key]
                if payload.get("description") is not None:
                    LEGAL_TAGS[name]["description"] = payload["description"]
                response = {**LEGAL_TAGS[name], "isValid": is_legal_tag_valid(name)}
            return self._json(200, response)

        if path in {"/api/storage/v2/records", "/storage/v2/records"}:
            if not self._rbac({"service.storage.editor"}):
                return
            records = payload if isinstance(payload, list) else [payload]
            try:
                result = self._upsert(records)
            except ValueError as e:
                return self._error(getattr(e, "status", 400), str(e))
            return self._json(200, {
                "recordCount": len(result),
                "recordIds": result,
                "skippedRecordIds": [],
            })

        if path in {"/api/dataset/v1/registerDataset", "/dataset/v1/registerDataset"}:
            if not self._rbac({"service.storage.editor"}):
                return
            is_batch = isinstance(payload, dict) and "datasetRegistries" in payload
            records = (payload.get("datasetRegistries") if is_batch else payload.get("datasets")
                       ) if isinstance(payload, dict) else payload
            if records is None:
                records = payload if isinstance(payload, list) else [payload]
            try:
                metadata_only = isinstance(records, list) and 1 <= len(records) <= 20
                if metadata_only:
                    for record in records:
                        validate_record(record)
                    metadata_only = all(record["kind"] == DATASET_KIND and not file_source(record)
                                        for record in records)
                if metadata_only:
                    # Legacy declarative metadata never implies bytes or an upload grant.
                    result = self._upsert(records, index_immediately=True)
                else:
                    result = publish_file_records(records, principal=self.principal)
            except ValueError as e:
                return self._error(getattr(e, "status", 400), str(e))
            if is_batch:
                ids = [entry["id"] for entry in result]
                with STORE_LOCK:
                    stored = [deepcopy(RECORD_BY_ID[eid]) for eid in ids if eid in RECORD_BY_ID]
                return self._json(200, {"datasetRegistries": stored})
            return self._json(200, {
                "status": "registered",
                "recordCount": len(result),
                "recordIds": [{"id": record["id"], "version": record["version"]} for record in result],
            })

        if path in {"/api/schema-service/v1/schema", "/schema-service/v1/schema"}:
            if not self._rbac({"service.storage.editor"}):
                return
            try:
                with STORE_LOCK, storage_transaction():
                    status, response = SCHEMA_REGISTRY.register(payload, "PUT")
            except SchemaError as e:
                return self._error(e.status, str(e), {"Retry-After": "1"} if e.status == 429 else None)
            return self._json(status, response)

        if path in {"/api/schema-service/v1/schemas/system", "/schema-service/v1/schemas/system",
                    "/api/schema-service/v1/schemas/system/", "/schema-service/v1/schemas/system/"}:
            if not self._rbac({"service.storage.editor"}):
                return
            storable = deepcopy(payload)
            with STORE_LOCK:
                SCHEMAS["system"] = storable
            return self._json(200, storable)

        for prefix in ("/api/workflow/v1/workflow/", "/workflow/v1/workflow/"):
            if path.startswith(prefix):
                rest = unquote(path[len(prefix):])
                segs = rest.split("/")
                if len(segs) != 3 or segs[1] != "workflowRun":
                    return self._error(404, f"Unknown workflow endpoint: {path}")
                name, run_id = segs[0], segs[2]
                if not self._rbac({"service.storage.editor"}):
                    return
                with STORE_LOCK, WORKFLOW_LOCK:
                    run = WORKFLOW_RUNS.get(run_id)
                    if not self._run_visible(run):
                        return self._error(404, f"Workflow run not found: {run_id}")
                    if payload.get("status"):
                        return self._error(409, "workflow status is owned by indexing outcomes")
                return self._json(200, {"workflowRunId": run_id, **run})

        self._error(404, f"Unknown endpoint: {path}")

    @backend_errors
    def do_DELETE(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        if not self._check_auth(path):
            return
        if self._community_request("DELETE", parsed):
            return
        sweep_temp_state()

        if path in {"/api/storage/v2/records", "/storage/v2/records"}:
            if not self._rbac({"service.storage.editor"}):
                return
            payload = self._read_json()
            if payload is None:
                return
            ids = payload.get("records", []) if isinstance(payload, dict) else payload
            if not isinstance(ids, list):
                return self._error(400, "Expected a list of record ids ({\"records\": [...]})")
            removed = self._delete(ids)
            return self._json(200, {
                "recordCount": len(removed),
                "recordIds": [{"id": rid} for rid in removed],
                "skippedRecordIds": [{"id": rid} for rid in ids if rid not in removed],
            })

        for prefix in ("/api/search/v2/query_with_cursor/", "/search/v2/query_with_cursor/"):
            if path.startswith(prefix):
                if not self._rbac({"service.search.user"}):
                    return
                cursor = unquote(path[len(prefix):])
                with STORE_LOCK, CURSOR_LOCK:
                    state = CURSORS.get(cursor)
                    if state and state.get("principal") != self.principal.name:
                        return self._error(403, "Cursor does not belong to current caller entitlements")
                    CURSORS.pop(cursor, None)
                return self._no_content()

        for prefix in ("/api/storage/v2/records/versions/", "/storage/v2/records/versions/"):
            if path.startswith(prefix):
                if not self._rbac({"service.storage.editor"}):
                    return
                rid = unquote(path[len(prefix):])
                if not self._rbac({"service.storage.admin"}):
                    return
                with STORE_LOCK:
                    if rid not in RECORD_BY_ID and not RECORD_VERSIONS.get(rid):
                        return self._error(404, f"Record not found: {rid}")
                self._delete([rid], hard=True)
                return self._no_content()

        for prefix in ("/api/storage/v2/records/", "/storage/v2/records/"):
            if path.startswith(prefix):
                if not self._rbac({"service.storage.editor"}):
                    return
                rid = unquote(path[len(prefix):])
                hard = query.get("hardDelete", ["false"])[0].lower() in {"1", "true", "yes"}
                if hard and not self._rbac({"service.storage.admin"}):
                    return
                with STORE_LOCK:
                    if rid not in RECORD_BY_ID:
                        return self._error(404, f"Record not found: {rid}")
                self._delete([rid], hard=hard)
                return self._no_content()

        for prefix in ("/api/legal/v1/legaltags/", "/legal/v1/legaltags/"):
            if path.startswith(prefix):
                if not self._rbac({"users.datalake.editors"}):
                    return
                name = unquote(path[len(prefix):])
                with STORE_LOCK, storage_transaction():
                    if name not in LEGAL_TAGS:
                        return self._error(404, f"LegalTag not found: {name}")
                    if policy_definition_in_use(name, legal=True):
                        return self._error(409, "LegalTag is referenced by retained records")
                    del LEGAL_TAGS[name]
                return self._no_content()

        for prefix in ("/api/entitlements/v2/groups/", "/entitlements/v2/groups/"):
            if not path.startswith(prefix):
                continue
            rest = unquote(path[len(prefix):])
            if "/members/" in rest:
                group_email_value, member_email = rest.split("/members/", 1)
                if not self._rbac({"service.entitlements.admin", "service.entitlements.user"}):
                    return
                with STORE_LOCK, storage_transaction(), GROUPS_LOCK:
                    group = GROUPS.get(group_email_value)
                    if not group:
                        return self._error(404, f"Group not found: {group_email_value}")
                    if member_email not in group["members"]:
                        return self._error(404, f"Member not found: {member_email}")
                    del group["members"][member_email]
                return self._no_content()
            if not self._rbac({"service.entitlements.admin"}):
                return
            with STORE_LOCK, storage_transaction(), GROUPS_LOCK:
                if rest not in GROUPS:
                    return self._error(404, f"Group not found: {rest}")
                if policy_definition_in_use(rest, legal=False):
                    return self._error(409, "Group is referenced by retained records")
                del GROUPS[rest]
            return self._no_content()

        if path.startswith("/api/file/v2/files/") or path.startswith("/file/v2/files/"):
            prefix = "/api/file/v2/files/" if path.startswith("/api/file/v2/files/") else "/file/v2/files/"
            rest = unquote(path[len(prefix):])
            if rest.endswith("/metadata"):
                if not self._rbac({"users.datalake.editors"}):
                    return
                fid = rest[:-len("/metadata")]
                with STORE_LOCK:
                    if fid not in RECORD_BY_ID:
                        return self._error(404, f"File not found: {fid}")
                self._delete([fid])
                return self._no_content()

        for prefix in ("/api/workflow/v1/workflow/", "/workflow/v1/workflow/"):
            if path.startswith(prefix):
                rest = unquote(path[len(prefix):])
                segs = rest.split("/")
                if len(segs) == 3 and segs[1] == "workflowRun":
                    name, run_id = segs[0], segs[2]
                    if not self._rbac({"service.storage.editor"}):
                        return
                    with STORE_LOCK, WORKFLOW_LOCK:
                        if run_id not in WORKFLOW_RUNS:
                            return self._error(404, f"Workflow run not found: {run_id}")
                        if not self._run_visible(WORKFLOW_RUNS[run_id]):
                            return self._error(404, "Workflow run not found")
                        if WORKFLOW_RUNS[run_id]["status"] not in ("succeeded", "failed"):
                            return self._error(409, "active workflow run cannot be deleted")
                        del WORKFLOW_RUNS[run_id]
                        WORKFLOW_DEADLINES.pop(run_id, None)
                    return self._no_content()
                if len(segs) == 1:
                    if not self._rbac({"service.storage.editor"}):
                        return
                    name = segs[0]
                    with STORE_LOCK, WORKFLOW_LOCK:
                        if name not in WORKFLOW_DEFS:
                            return self._error(404, f"Workflow not found: {name}")
                        if any(r["workflowName"] == name and not self._run_visible(r)
                               for r in WORKFLOW_RUNS.values()):
                            return self._error(404, "Workflow not found")
                        if any(r["workflowName"] == name and r["status"] not in ("succeeded", "failed")
                               for r in WORKFLOW_RUNS.values()):
                            return self._error(409, "workflow has active runs")
                        del WORKFLOW_DEFS[name]
                        for run_id in [rid for rid, r in WORKFLOW_RUNS.items() if r.get("workflowName") == name]:
                            del WORKFLOW_RUNS[run_id]
                            WORKFLOW_DEADLINES.pop(run_id, None)
                    return self._no_content()
                return self._error(404, f"Unknown workflow endpoint: {path}")

        self._error(404, f"Unknown endpoint: {path}")

    @backend_errors
    def do_PATCH(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if not self._check_auth(path):
            return
        if self._community_request("PATCH", parsed):
            return
        payload = self._read_json()
        if payload is None:
            return

        for prefix in ("/api/entitlements/v2/groups/", "/entitlements/v2/groups/"):
            if path.startswith(prefix):
                email = unquote(path[len(prefix):])
                if not self._rbac({"service.entitlements.user", "service.entitlements.admin"}):
                    return
                with STORE_LOCK, storage_transaction(), GROUPS_LOCK:
                    group = GROUPS.get(email)
                    if not group:
                        return self._error(404, f"Group not found: {email}")
                    if isinstance(payload.get("op"), str):
                        op = payload["op"]
                        target = payload.get("path")
                        if op in {"replace", "add"} and target == "/appIds":
                            group["appIds"] = list(payload.get("value") or [])
                        elif op in {"replace", "add"} and target == "/description":
                            group["description"] = payload.get("value")
                        elif op in {"replace", "add"} and target == "/name":
                            group["name"] = payload.get("value")
                        else:
                            return self._error(400, f"Unsupported JSON Patch op/path: {op} {target}")
                        response = deepcopy(group)
                    else:
                        for key in ("name", "description"):
                            if payload.get(key) is not None:
                                group[key] = payload[key]
                        if payload.get("appIds") is not None:
                            group["appIds"] = list(payload["appIds"])
                        response = None
                if response is not None:
                    return self._json(200, response)
                return self._no_content()

        if path in {"/api/storage/v2/records", "/storage/v2/records"}:
            if not self._rbac({"service.storage.editor"}):
                return
            records = payload if isinstance(payload, list) else [payload]
            result = []
            for rec in records:
                if not isinstance(rec, dict) or "id" not in rec:
                    return self._error(400, "Each patch part requires an id")
                updated = patch_record(rec["id"], rec, principal=self.principal)
                if not updated:
                    return self._error(404, f"Record not found: {rec['id']}")
            return self._json(200, {
                "recordCount": len(records),
                "recordIds": [r["id"] for r in records],
                "skippedRecordIds": [],
            })

        for prefix in ("/api/storage/v2/records/", "/storage/v2/records/"):
            if path.startswith(prefix) and not path.startswith("/api/storage/v2/records/versions/"):
                if not self._rbac({"service.storage.editor"}):
                    return
                rid = unquote(path[len(prefix):])
                updated = patch_record(rid, payload, principal=self.principal)
                if not updated:
                    return self._error(404, f"Record not found: {rid}")
                return self._json(200, {
                    "recordCount": 1,
                    "recordIds": [updated[0]["id"]],
                    "skippedRecordIds": [],
                })

        self._error(404, f"Unknown endpoint: {path}")

# Trusted Python compatibility entry points for existing fixtures/offline operators.
# HTTP and community dispatch never use these: request-aware helpers require a context.
def clear_state():
    return _clear_state(principal=core_auth.SYSTEM)

def load_full_sample(index_immediately=True):
    return _load_full_sample(index_immediately, principal=core_auth.SYSTEM)

def upsert_records(records, index_immediately=False, run_id=None):
    return _upsert_records(records, index_immediately, run_id, principal=core_auth.SYSTEM)

def delete_records(record_ids, hard=False):
    return _delete_records(record_ids, hard, principal=core_auth.SYSTEM)

def index_current_records(ids=None, kind=None, clean=False):
    return _index_current_records(ids, kind, clean, principal=core_auth.SYSTEM)

def delete_index_kind(kind):
    return _delete_index_kind(kind, principal=core_auth.SYSTEM)

def search_records(payload, caller_groups=None):
    return _search_records(payload, caller_groups, principal=core_auth.SYSTEM)

def record_version(rid, version):
    return _record_version(rid, version, principal=core_auth.SYSTEM)

def main():
    start_runtime()
    print(f"OSDU Well 360 local server v0.7 listening on http://0.0.0.0:{PORT}")
    print(f"Partition: {PARTITION} | authentication: {'enabled' if REQUIRE_AUTH else 'disabled'}")
    print(f"Preload full sample configured: {PRELOAD_FULL_SAMPLE} | index delay: {INDEX_DELAY_SECONDS}s")
    print(f"Upload token TTL: {UPLOAD_TOKEN_TTL}s | cursor TTL: {CURSOR_TTL}s")
    httpd = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
        stop_runtime()

if __name__ == "__main__":
    main()