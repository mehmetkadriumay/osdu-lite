# OSDU-Lite: two independent 16 GiB Raspberry Pi 5 hosts

**Hardware/performance: NOT VALIDATED.** This is a development runbook, not
production OSDU, HA, certification, or a measured capacity promise. The current
source implements the selected core profile; the published upstream `latest`
image does **not** establish that the committed source has been packaged or
release-validated. Committing/pushing code is not a successful image publication.
Use a locally built current image or your own verified image digest.
Final software acceptance is still blocked; see the
[release-verification handoff](docs/projects/feature-rpi-osdu-core/feature-rpi-osdu-core.release-verification.md).

There are exactly three runtime services, no Swarm/Kubernetes, broker, IdP,
multiwriter coordination, or cross-host Compose network:

| Host (example) | Services / hard memory limits | Storage |
| --- | --- | --- |
| Pi A `192.168.1.101` | one API writer 2 GiB; Azurite 2 GiB | host-local SSD for Azurite snapshots and private files |
| Pi B `192.168.1.102` | single-node OpenSearch 6 GiB, initial heap 2 GiB | host-local SSD node data |

Each host has **16 GiB**; they are not one pooled 32 GiB machine. Spare RAM on Pi A
cannot rescue Pi B from OOM. No failover/replica is provided. OpenSearch has one
primary shard per active kind and zero replicas: four sample kinds use four
shards; eight is the hard development ceiling, not a Pi recommendation.
Records, control metadata and uploaded files live in Azurite; OpenSearch is a
derived, schema-driven index. No DDMS or new workflow/ingestion service is needed.
Core uses local bearer identities and the bounded local schema dialect, not
OIDC or the full upstream OSDU schema catalog.

## Prepare and protect both hosts

Use a 64-bit OS (`uname -m` must return `aarch64`), reliable power, active
cooling, wired private LAN and Docker Engine. Install Docker following its
[Ubuntu instructions](https://docs.docker.com/engine/install/ubuntu/).
On Pi A also install Git, curl and Python3 for the checkout and HTTP probes.
Verify memory cgroups and local SSD mounts before starting containers.
Do not put OpenSearch node data on NFS, SMB or other network filesystems.
Keep backups separate from the live SSD.

HTTP, local bearer identities, security-disabled OpenSearch and Azurite's
**publicly known development account key** are not production security.
Anyone reaching Azurite with that key can bypass the API's ACL/SAS issuance
policy. Only trusted operators/clients may reach this setup. Never port-forward
it to the Internet. Restrict Docker's published ports at the LAN/router and
Docker forwarding firewall boundaries; do not assume UFW alone protects them.

| Destination | Permitted sources |
| --- | --- |
| Pi A TCP 8089 API | intended trusted clients |
| Pi A TCP 10000 Blob | intended trusted upload/download clients |
| Pi B TCP 9200 engine | Pi A only |
| SSH on both Pis | administration workstation only |

No cluster control-plane, overlay or kubelet ports are needed.

## Single-host Compose alternative (not two-host deployment)

From `server/`, `docker compose -f compose.yaml up --build -d` builds current
source and runs all three services on **one host**. Both Compose files retain
the existing `azurite-data` named volume and add `opensearch-data` persistence.
Make Docker's volume directory reside on a local SSD. Never use `down -v`
as a restart/upgrade command: it deletes authoritative snapshots/files.

Default profile is legacy. Azurite publishes `127.0.0.1:10000` and returned
SAS URLs use `http://localhost:10000/devstoreaccount1`. For trusted LAN clients,
set `AZURITE_BIND_IP` to the host's LAN address and
`AZURE_STORAGE_PUBLIC_BLOB_ENDPOINT` to that same client-reachable account
root; adjust the firewall first. Changing `AZURITE_BLOB_PORT` also requires
changing the public endpoint's port. The internal SDK endpoint remains
`http://azurite:10000/devstoreaccount1`, independent of client DNS.

`OSDU_COMPAT_PROFILE=core` selects core; `OPENSEARCH_JAVA_OPTS` can override the
initial `-Xms2g -Xmx2g`. Hard limits are API 2g, Azurite 2g, OpenSearch 6g.
The pull configuration accepts `IMAGE`; its default upstream `latest` is only
a legacy quickstart, **not a release of these changes**. Do not claim current
core behavior from that default image.

## Two-host standalone Docker placement

These Bash commands are **instructions, not execution evidence**.
Choose persistent SSD paths and actual LAN addresses first.
The commands below assume a **fresh installation**. For an existing deployment,
export volatile controls and follow the backup/upgrade procedure before starting
the new API; do not replace a running writer or its volumes.

On Pi B:

```bash
sudo mkdir -p /srv/osdu/opensearch
sudo chown 1000:1000 /srv/osdu/opensearch
sudo docker run -d --name opensearch --restart unless-stopped --memory 6g \
  --publish 192.168.1.102:9200:9200 \
  --mount type=bind,src=/srv/osdu/opensearch,dst=/usr/share/opensearch/data \
  --env discovery.type=single-node \
  --env DISABLE_INSTALL_DEMO_CONFIG=true --env DISABLE_SECURITY_PLUGIN=true \
  --env OPENSEARCH_JAVA_OPTS="-Xms2g -Xmx2g" \
  --env node.store.allow_mmap=false opensearchproject/opensearch:3.9.0
curl --retry 30 --retry-delay 2 --retry-connrefused -fsS \
  'http://192.168.1.102:9200/_cluster/health?wait_for_status=yellow&timeout=30s'
```

Confirm the response is not timed out and the status is yellow or green before
starting the API. A successful HTTP response alone is not a readiness assertion.

On Pi A, obtain and build the current checkout (or choose a separately verified ARM64
image digest). Building may require access to the pinned wheels/registry; do
not remove TLS verification or package hashes to work around failures.
Run the remaining Pi A commands from this repository root; reuse your existing
checkout instead of cloning over it.

```bash
git clone https://github.com/mehmetkadriumay/osdu-lite.git
cd osdu-lite
git rev-parse HEAD
IMAGE="osdu-lite:$(git rev-parse --short HEAD)"
sudo docker build --platform linux/arm64 -t "$IMAGE" ./server
sudo mkdir -p /srv/osdu/azurite
sudo docker network create osdu-core
sudo docker run -d --name azurite --network osdu-core \
  --restart unless-stopped --memory 2g --publish 192.168.1.101:10000:10000 \
  --mount type=bind,src=/srv/osdu/azurite,dst=/data \
  mcr.microsoft.com/azure-storage/azurite:3.35.0 \
  azurite-blob --blobHost 0.0.0.0 --location /data \
  --disableProductStyleUrl --disableTelemetry
```

Create a private, owner-readable `/srv/osdu/api.env` with an editor, outside
the repository; restrict permissions (`chmod 600`). It must contain:

```text
OSDU_PARTITION=company-prod
OSDU_TOKEN=<your-private-local-bearer>
REQUIRE_AUTH=true
OSDU_COMPAT_PROFILE=core
PRELOAD_FULL_SAMPLE=true
OPENSEARCH_URL=http://192.168.1.102:9200
AZURE_STORAGE_PUBLIC_BLOB_ENDPOINT=http://192.168.1.101:10000/devstoreaccount1
AZURE_STORAGE_CONNECTION_STRING=<copy the public Azurite development connection string from compose.yaml, retaining BlobEndpoint=http://azurite:10000/devstoreaccount1;>
```

The placeholder is not a working credential. Do not paste production account
keys into these development instructions. `OSDU_TOKEN` replaces the shipped
demo token; it does not add an IdP. Replace the token and connection-string
placeholders before starting. Copy the public development connection string
from `server/compose.yaml`, not a production storage account.

This fresh-install example explicitly opts into **core**. The application and
Compose default remain **legacy**; use that profile for existing legacy demos.
Existing format2 service roles/memberships are not reseeded by switching profiles.
Do not source the Docker env-file as a shell script: its unquoted connection
string contains semicolons. Keep `IMAGE` set in the same shell, or set it again
to the tag built above before each API/migration command.

```bash
sudo docker run -d --name osdu-api --network osdu-core \
  --restart unless-stopped --memory 2g --publish 192.168.1.101:8089:8089 \
  --env-file /srv/osdu/api.env "$IMAGE"
curl --retry 30 --retry-delay 2 --retry-connrefused -fsS \
  http://192.168.1.101:8089/health
```

Do not scale the API. A container health check alone does not verify authorized
Storage/Search/File behavior. Public health/info responses hide unauthorized
record counts; use the configured token for the checks below. Startup restores authoritative
metadata and rebuilds derived search, not an overwrite of saved records with
samples. Lost indices can be rebuilt from the snapshot.

### Authenticated readiness checks

In the operator shell, enter the same token as `/srv/osdu/api.env` without
putting its literal value in shell history:

```bash
export OSDU_BASE_URL=http://192.168.1.101:8089
export OSDU_PARTITION=company-prod
read -r -s -p "API bearer token: " OSDU_TOKEN
printf '\n'
export OSDU_TOKEN
curl -fsS "$OSDU_BASE_URL/info" \
  -H "Authorization: Bearer $OSDU_TOKEN" \
  -H "data-partition-id: $OSDU_PARTITION" | python3 -m json.tool
curl -fsS -X POST "$OSDU_BASE_URL/api/search/v2/query" \
  -H "Authorization: Bearer $OSDU_TOKEN" \
  -H "data-partition-id: $OSDU_PARTITION" \
  -H "Content-Type: application/json" \
  --data '{"kind":"osdu:wks:master-data--Well:*","limit":1}' \
  | python3 -m json.tool
unset OSDU_TOKEN
```

Check `compatibility.profile` is `core`, `compatibility.snapshotFormat` is2, and the expected
sample records are visible to the configured principal. Search is asynchronous;
allow indexing to finish before comparing counts. Verify the public Blob
endpoint from an actual upload/download client, not only from Pi A: a
`localhost` SAS endpoint is not reachable from another machine.

Core provides actual signed staging uploads and private final file reads.
New legacy uploads use the API's authenticated adapter backed by Azurite.
Uploaded bytes are not exposed through anonymous `/files/{name}`. Only the
four integrity-checked shipped samples may be imported automatically; existing
user files require the explicit offline migration below.

### Optional private identity and format1 import mounts

An optional `OSDU_IDENTITIES_FILE` contains
`[{"principal":"alice","token":"<private-token>","groups":["<role-or-data-group>"]}]`.
For example add `--mount type=bind,src=/srv/osdu/identities.json,dst=/private/identities.json,readonly`
and `--env OSDU_IDENTITIES_FILE=/private/identities.json` to the API command.
Use owner-only host permissions; never check the file into Git or show tokens
in commands/logs. Membership seeds only a new partition/format1 migration;
format2 membership is authoritative. Existing entitlement admins must add any
missing core service roles; a profile switch does not reseed grants.

Format1 cannot recover old volatile custom schemas/legal/groups. Export them
before stopping the old writer. An optional private import JSON
`{"schemas":[],"legalTags":{},"groups":{}}` uses the PRD's registration/export
shapes. Mount it read-only similarly and set `OSDU_CORE_IMPORT_FILE` for the
first new-partition/format1 bootstrap. **It is ignored after format2 exists**;
unknown/unresolved dependencies fail closed, never silently dropped.
After bootstrap, stop the new writer before migrating local user files.

## Offline user-file migration

First stop the sole API writer (`sudo docker stop osdu-api`) and quiesce local
source-file edits. Snapshot format2 must already exist: this tool **does not
upgrade format1 or import missing controls**. A proper new-API bootstrap/import
must happen first (it may stop before readiness on unmigrated file records).
The format1 backup/format2 commit occurs before those sample readiness checks.

Use an explicit JSON array, never an arbitrary directory scan:

```json
[
  {"recordId":"company-prod:dataset--File.Generic:YOUR_ID",
   "sourcePath":"subdir/original.las",
   "sha256":"REPLACE_WITH_64_LOWERCASE_HEX_SHA256"}
]
```

Record IDs must already exist as supported File.Generic Dataset records.
Relative paths must remain beneath `FILES_DIR`; traversal, symlinks/reparse
points, missing sources, conflicting duplicate IDs and hash mismatches fail.
Decide explicitly which user records/files to migrate; source filenames do not
grant authority. Unmapped user records are not automatically scanned/imported.

Prefer the packaged-container alternative below so migration uses the image's
verified dependencies. If matching pinned dependencies are already installed on
the operator host:

```bash
export FILES_DIR=/srv/operator-files
export OSDU_PARTITION=company-prod
# Supply the host-reachable internal SDK connection via a private environment.
python3 server/migrate_files.py --manifest /srv/osdu/files-manifest.json --dry-run
python3 server/migrate_files.py --manifest /srv/osdu/files-manifest.json --apply --writer-stopped
```

The default is dry-run, which makes no Blob container initialization or writes.
Apply requires an explicit stopped-writer attestation, validates every entry
and candidate snapshot before writes, streams bounded staging bytes through the
installed SDK publication path to create-only deterministic private final keys,
and verifies bytes/hash (including existing final blobs). It rechecks sources
before one original-ETag snapshot commit. Original local files, record payloads,
versions, fences, deletion and control state remain unchanged. Identical retries
do not save or initialize anything. No OpenSearch/service runtime is needed.
`FILES_DIR` and packaged data must already exist; missing directories are not
created by the tool.

Alternatively run the packaged script with the API container **stopped**, using
the same image/env-file and only a read-only source/manifest mount:

```bash
sudo docker run --rm --network osdu-core --env-file /srv/osdu/api.env \
  --mount type=bind,src=/srv/operator-files,dst=/operator-files,readonly \
  --mount type=bind,src=/srv/osdu/files-manifest.json,dst=/private/manifest.json,readonly \
  --env FILES_DIR=/operator-files "$IMAGE" \
  python /app/migrate_files.py --manifest /private/manifest.json --dry-run
```

Replace `--dry-run` with `--apply --writer-stopped` only after inspecting the
plan. Successful binding commit is atomic; byte upload and metadata are not a
distributed transaction. A failed commit exposes no new binding and reports
**possible private orphan keys only**, not URLs/accounts/SAS/tokens. Staging is
retained even on success and reported separately for later offline inventory.
Never delete possible orphans until comparing them with the authoritative
snapshot; do not delete shared containers or guess from filenames. A source
must stay quiescent: the tool verifies published bytes, not a filesystem lock.

## Backups, upgrade and rollback

Export old volatile controls while the old writer still runs, then stop it.
Take consistent stopped-writer backups of the Azurite SSD/volume, local user
files, private import/identity configuration and matching old image/source.
Stop Azurite while copying its volume too; a live directory copy is not a
consistent backup. Preserve original files and backup checksums.

Startup keeps a verified create-only original-format1 blob backup before the
single ETag format2 overwrite. No automatic downgrade occurs. Switching
`OSDU_COMPAT_PROFILE=legacy` is **not rollback**. For rollback: stop writers,
retain failed-upgrade volumes for diagnosis, restore matching **old image plus
old volumes/files** together, and rebuild search from restored metadata.
Restoring an old backup **loses all writes after that backup**. Keep uncertain
new blobs private, rather than deleting data to force readiness.

## Acceptance and resource evidence still required

An earlier checkpoint's offline-equivalent API image built and ran on WSL ARM64.
That checkpoint's real core CRUD/schema-search/private SAS and API/Azurite
recreation passed with no local-files mount/reseed:18 records and1,048,598
uploaded bytes/hash survived. These are not checks of the latest repaired source.
The normal Dockerfile build failed TLS download; existing hash-checked ARM64
wheels installed offline without weakening TLS/hashes. This is not a Pi pass.
**CI execution has not been verified.** Complete smoke and final full-backend
regression remain open release gates. Actual Pi performance
is **NOT VALIDATED** because hardware is unavailable.

Use a **separate disposable deployment**, not the operator's live API, Blob
volume or search indices. The API supports one configured partition; a different
request header does not create an isolated test tenant. Smoke scripts and demos
may mutate/reset data, and integration tests may recreate their test containers
or indices. Legacy tools are not proof of core contract conformance.

From the repository root, with dependencies installed for Python unit tests:

```bash
export OSDU_BASE_URL=http://192.168.1.101:8089
# These addresses must point to the disposable deployment, not live data.
export OSDU_PARTITION=company-prod
read -r -s -p "Disposable API bearer token: " OSDU_TOKEN
printf '\n'
export OSDU_TOKEN
# Run these legacy smoke tools against a legacy-profile test API.
bash server/smoke-test.sh
bash server/reference-smoke-test.sh
python3 server/community-smoke-test.py
unset OSDU_TOKEN
python3 -m unittest discover -s server/tests -v
# Set explicit private test connections/URL; never point tests at operator data.
python3 -m unittest discover -s server/tests -p test_storage_backend.py -v
python3 -m unittest discover -s server/tests -p test_file_backend.py -v
PYTHONPATH=server:server/tests python3 -m unittest test_file_migration.RealFileMigrationTests -v
python3 -m unittest discover -s server/tests -p test_opensearch_integration.py -v
```

Opt-in real-backend tests require `AZURITE_TEST_CONNECTION_STRING`,
`OPENSEARCH_TEST_URL` and a matching client-reachable
`AZURE_STORAGE_PUBLIC_BLOB_ENDPOINT` for the disposable services. Missing
configuration causes skips, not evidence that real-backend behavior passed.
Do not run `demo-all.sh` as a readiness probe: it manages its own default
deployment resources. Tutorial/full-sample/manifest demos may reset the API.

Prior unsupported Dataset-undelete baseline attribution is withdrawn; the
legacy publication/replay regressions were fixed. Latest whole all-backend
discovery is432 cases,430 passes,1 failure/1 error,0 skips; both failed cases
passed in isolation afterward, not a full pass. All3 HTTP smokes/full-sample
Java demo passed at an earlier checkpoint. The owner/import failures and tutorial
authentication/envelope defects have since been repaired; three focused checks
pass, but complete discovery and tutorial were not rerun after those repairs.
Manifest/latest-source recreation remain open. See the
[release-verification handoff](docs/projects/feature-rpi-osdu-core/feature-rpi-osdu-core.release-verification.md)
for exact evidence and pending checks.
Verify core CRUD/schema-query/file round trip, API/Azurite recreation without
reseeding, engine index-loss rebuild, and each running image's `aarch64`/digest.

For Pi acceptance populate four kinds ×250 IDs ×5 versions (average canonical
record ≤4 KiB, approximately 20 MiB retained), plus 16 sequential 16 MiB files.
Run two clients for ten minutes on wired LAN using supported search/page/read
and 100-record batches; include API restart and engine index-loss recovery.
Record `docker stats --no-stream`, process RSS, request timings and p95,
host SSD/media/OS/temperature/throttling, OOM counters and exact images/hashes.
Pass only with no OOM/steady-state 5xx, API/Azurite RSS ≤1.5 GiB each,
engine usage ≤5 GiB, p95 GET/query ≤2s, 100-record PUT ≤10s and rebuild ≤120s.

| Measurement | Result |
| --- | --- |
| Actual Pi workload/RSS/latency/thermal/SSD | **NOT VALIDATED — hardware unavailable** |
| Source complexity | Substantial changes across runtime, tests and migration tooling; line counts do not establish memory capacity |
| Earlier API ARM64 offline-equivalent build/core runtime/Blob recreation | **PASS on WSL ARM64**, not Pi or latest-source release closure; normal Dockerfile TLS download failed |
| Complete smoke/full-backend regression | **BLOCKED: last complete432-case run had1 failure/1 error; repaired focused checks pass3/3; final reruns pending** |
| CI execution | **NOT VERIFIED** |

Full-snapshot transactions copy retained history/controls and serialize/rewrite
the whole snapshot; Python dictionaries/strings, rollback copies, encoded JSON
and SDK buffers can multiply the 128 MiB canonical payload ceiling. Up to 5,000
lifetime fence IDs and pinned indexing history add memory; reference cursors
avoid full-source caches but still reserve up to 16 MiB serialized references.
Eight shards (four for samples) add engine native/heap overhead. These are
meaningful risks under the 2/2/6 GiB limits, not a load benchmark. Measure them
on each Pi; do not infer capacity from fake tests or pooled host RAM.
