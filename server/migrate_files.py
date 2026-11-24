"""EPIC-009 stopped-writer, explicit-manifest migration; no API/search startup."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PureWindowsPath
import stat
import sys
import uuid

from file_backend import AzureFiles, CHUNK_BYTES, MAX_FILE_BYTES
from runtime_jobs import CapacityError
from search_backend import BackendError
from storage_backend import AzureBlobStorage, parse_json, validate_envelope


class MigrationError(Exception):
    def __init__(self, reason, orphan_keys=()):
        super().__init__(reason)
        self.orphan_keys = sorted(set(orphan_keys))


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--manifest", required=True, type=Path)
    modes = result.add_mutually_exclusive_group()
    modes.add_argument("--dry-run", action="store_true", help="default: preflight only; no initialization/writes")
    modes.add_argument("--apply", action="store_true")
    result.add_argument("--writer-stopped", action="store_true",
                        help="attest the sole API writer is stopped and sources are quiescent")
    return result


def source_path(base, name):
    if (not isinstance(name, str) or not name or "\\" in name or ":" in name
            or PureWindowsPath(name).is_absolute() or Path(name).is_absolute()
            or any(part in ("", ".", "..") for part in name.split("/"))):
        raise MigrationError("invalid_source_path")
    target = base
    try:
        for part in name.split("/"):
            target = target / part
            info = target.lstat()
            if (stat.S_ISLNK(info.st_mode)
                    or getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT
                    or target.is_symlink()):
                raise MigrationError("invalid_source_path")
        target.resolve(strict=True).relative_to(base.resolve(strict=True))
        if not target.is_file():
            raise MigrationError("invalid_source_path")
    except FileNotFoundError:
        raise MigrationError("missing_source") from None
    except (OSError, ValueError):
        raise MigrationError("invalid_source_path") from None
    return target


def source_hash(target):
    digest, size = hashlib.sha256(), 0
    try:
        with target.open("rb") as stream:
            while data := stream.read(CHUNK_BYTES):
                size += len(data)
                if size > MAX_FILE_BYTES:
                    raise MigrationError("file_size_limit")
                digest.update(data)
    except OSError:
        raise MigrationError("missing_source") from None
    return size, digest.hexdigest()


def validate_snapshot(state, partition):
    if not isinstance(state, dict) or state.get("format") != 2:
        raise MigrationError("format2_required: bootstrap/import the snapshot with the new API first, then stop it")
    try:
        validate_envelope(state)
        # Reuse the installed authoritative validator, not startup/recovery or live maps.
        # The server's historical import-time mkdir calls cannot create anything here:
        # both packaged data and operator files must already exist before importing it.
        root = Path(__file__).resolve().parent
        for path in (Path(os.getenv("DATA_DIR", root / "data")),
                     Path(os.getenv("FILES_DIR", root / "files"))):
            if not path.is_dir():
                raise ValueError()
        import server
        from schema_registry import SchemaRegistry
        if server.PARTITION != partition:
            raise ValueError()
        registry = SchemaRegistry()
        registry.import_custom(state["schemas"])
        server.validate_storage(state, registry)
        return server
    except (BackendError, CapacityError, ValueError, KeyError, TypeError, OverflowError, RecursionError):
        raise MigrationError("invalid_snapshot") from None


def verify_blob(files, key, size, expected):
    digest, count = hashlib.sha256(), 0
    for chunk in files.read(key):
        if not isinstance(chunk, bytes) or len(chunk) > CHUNK_BYTES:
            raise MigrationError("blob_hash_mismatch")
        count += len(chunk)
        if count > size:
            raise MigrationError("blob_hash_mismatch")
        digest.update(chunk)
    if count != size or digest.hexdigest() != expected:
        raise MigrationError("blob_hash_mismatch")


def migrate(manifest, files_dir, partition, storage, files, *, apply=False, writer_stopped=False):
    if apply and not writer_stopped:
        raise MigrationError("writer_not_attested")
    created = []
    try:
        if Path(manifest).stat().st_size > 8 * CHUNK_BYTES:
            raise MigrationError("invalid_manifest")
        entries = parse_json(Path(manifest).read_bytes())
        if not isinstance(entries, list) or not 1 <= len(entries) <= 5000:
            raise MigrationError("invalid_manifest")
        state = storage.load(initialize=False) if isinstance(storage, AzureBlobStorage) else storage.load()
        api = validate_snapshot(state, partition)
        base = Path(files_dir)
        if not base.is_dir():
            raise MigrationError("missing_source")
        unique = {}
        for entry in entries:
            if (not isinstance(entry, dict) or set(entry) != {"recordId", "sourcePath", "sha256"}
                    or not isinstance(entry["recordId"], str)
                    or not isinstance(entry["sha256"], str) or len(entry["sha256"]) != 64
                    or any(c not in "0123456789abcdef" for c in entry["sha256"])):
                raise MigrationError("invalid_manifest")
            rid = entry["recordId"]
            if rid in unique and entry != unique[rid]:
                raise MigrationError("conflicting_duplicate")
            unique[rid] = entry
        planned = []
        prefix = hashlib.sha256(partition.encode()).hexdigest()
        for rid, entry in sorted(unique.items()):
            record = state["records"].get(rid)
            if record is None:
                raise MigrationError("missing_record")
            if record["kind"] != api.DATASET_KIND:
                raise MigrationError("not_dataset")
            target = source_path(base, entry["sourcePath"])
            size, digest = source_hash(target)
            if digest != entry["sha256"]:
                raise MigrationError("hash_mismatch")
            key = prefix + "/final/" + hashlib.sha256(rid.encode()).hexdigest() + "/" + digest
            binding = state["fileBindings"].get(rid)
            if binding:
                if (binding["finalKey"], binding["size"], binding["sha256"]) != (key, size, digest):
                    raise MigrationError("binding_conflict")
                verify_blob(files, key, size, digest)
            else:
                # Verify pre-existing deterministic finals for every entry before any write.
                try:
                    files.probe(key)
                except BackendError as error:
                    if str(error) != "file_unavailable":
                        raise
                else:
                    verify_blob(files, key, size, digest)
            planned.append((rid, entry, size, key, binding))
        pending = [item for item in planned if not item[4]]
        import core_profile
        for rid, entry, size, key, _ in pending:
            record = state["records"][rid]
            state["fileBindings"][rid] = {
                "finalKey": key, "size": size, "sha256": entry["sha256"],
                "grantDigest": hashlib.sha256(key.encode()).hexdigest(),
                "principal": "osdu-lite:migrated", "recordId": rid,
                "metadataDigest": hashlib.sha256(core_profile.canonical(
                    {"id": rid, **core_profile.user_fields(record)}).encode()).hexdigest(),
                "version": record["version"]}
        if pending:
            state["revision"] += 1
            validate_snapshot(state, partition)
        result = {"mode": "apply" if apply else "dry-run", "changed": len(pending),
                  "recordIds": [item[0] for item in pending], "orphanKeys": []}
        if not apply or not pending:
            return result
        files.initialize()
        for rid, entry, size, key, _ in pending:
            try:
                files.probe(key)
            except BackendError as error:
                if str(error) != "file_unavailable":
                    raise
                staging = prefix + "/migration/" + uuid.uuid4().hex
                # Record possible private orphans before the first write, including partial failures.
                created.append(staging)
                with source_path(base, entry["sourcePath"]).open("rb") as stream:
                    files.upload(staging, stream, size)
                created.append(key)
                try:
                    files.publish(staging, key)  # SDK conditional stream -> create-only private final.
                except BackendError as error:
                    if str(error) != "file_already_exists":
                        raise
            verify_blob(files, key, size, entry["sha256"])
        # Source/path races cannot commit unverified published bytes. Verify every source again,
        # including reused bindings, and validate the entire detached candidate before one save.
        for _, entry, size, _, _ in planned:
            if source_hash(source_path(base, entry["sourcePath"])) != (size, entry["sha256"]):
                raise MigrationError("hash_mismatch")
        storage.save(state)  # exactly one original-ETag conditional replacement
        # Staging is deliberately retained; no delete/GC privilege or automatic cleanup.
        result["orphanKeys"] = sorted(key for key in created if "/migration/" in key)
        return result
    except MigrationError as error:
        raise MigrationError(str(error), created) from None
    except (BackendError, OSError, ValueError, TypeError, KeyError, RecursionError):
        # Never expose SDK exceptions (which may contain account endpoints/credentials).
        raise MigrationError("migration_failed", created) from None


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        result = migrate(args.manifest, os.getenv("FILES_DIR", Path(__file__).parent / "files"),
                         os.getenv("OSDU_PARTITION", "company-prod"),
                         AzureBlobStorage.from_env(os.getenv("OSDU_PARTITION", "company-prod")),
                         AzureFiles.from_env(), apply=args.apply, writer_stopped=args.writer_stopped)
        print(json.dumps(result, sort_keys=True))
        return 0
    except MigrationError as error:
        print(json.dumps({"error": str(error), "orphanKeys": error.orphan_keys}, sort_keys=True),
              file=sys.stderr)
        return 1
    except (BackendError, ValueError, OSError):
        print('{"error":"migration_configuration_failed","orphanKeys":[]}', file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
