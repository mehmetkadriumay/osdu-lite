# OSDU-Lite

An OSDU API emulator with in-memory Storage and real OpenSearch, for development, demos, and CI.

[![CI](https://github.com/mehmetkadriumay/osdu-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/mehmetkadriumay/osdu-lite/actions/workflows/ci.yml)
[![Container image](https://github.com/mehmetkadriumay/osdu-lite/actions/workflows/publish-docker.yml/badge.svg)](https://github.com/mehmetkadriumay/osdu-lite/actions/workflows/publish-docker.yml)

A small Python HTTP server that emulates selected contracts from Storage,
Search, Dataset, File, Workflow, Legal, Schema, Entitlements, CRS, Unit, Indexer,
Notification, Partition, Register and Secret. Clients exercising the supported
subset can point at `http://localhost:8089`; check the operation inventory and
documented mock limitations before substituting for a real deployment.
One local OpenSearch node is required; no cloud or OIDC is required.

This repo also contains a working integration around it - the Well 360 example
(Well -> Wellbore -> WellLog -> Dataset -> LAS) with a Java consumer compiled
against `os-core-common`. See [Releases and changelog](CHANGELOG.md).

> Scope: this is a test double, not a deployment. It proves workflows and payload
> shapes and uses actual Lucene query semantics. It is not certified upstream
> OSDU, durable Storage, data-plane ACLs at scale, legal blocking, or OIDC/IAM.

## Quickstart

### Run with WSL containers (no Docker Desktop)

Use the container CLI **`wslc.exe`**, included with WSL 2.9.3 or later.
There is no Docker engine to install. See Microsoft's
[WSL containers guide](https://learn.microsoft.com/en-us/windows/wsl/tutorials/wsl-containers).

First, update and verify WSL from **Windows PowerShell**:

```powershell
wsl --update
wsl --version
wslc.exe version
```

Then open your **WSL terminal** (for example, Ubuntu). The commands below use
Windows interoperability to invoke `wslc.exe` from the Linux shell; keep the
`.exe` suffix. No repository clone or local Python installation is needed to
start the published image:

```bash
wslc.exe network create osdu-lite-net
wslc.exe pull opensearchproject/opensearch:3.9.0
wslc.exe run --rm -d --name osdu-lite-opensearch --network osdu-lite-net --network-alias opensearch \
  -e discovery.type=single-node -e DISABLE_INSTALL_DEMO_CONFIG=true -e DISABLE_SECURITY_PLUGIN=true \
  -e 'OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m' -e node.store.allow_mmap=false \
  --health-cmd 'curl -fsS http://localhost:9200/_cluster/health?wait_for_status=yellow' \
  --health-interval 5s --health-timeout 3s --health-retries 30 opensearchproject/opensearch:3.9.0
# Wait for the OpenSearch container to be healthy before starting the API:
wslc.exe container list
wslc.exe pull ghcr.io/mehmetkadriumay/osdu-lite:latest
wslc.exe run --rm -d --name osdu-well360 --network osdu-lite-net -p 8089:8089 \
  -e OPENSEARCH_URL=http://opensearch:9200 ghcr.io/mehmetkadriumay/osdu-lite:latest
```

The API is available at **http://localhost:8089**, with 15 sample records
preloaded. From WSL, wait for startup and check health:

```bash
curl --fail --retry 20 --retry-connrefused --retry-delay 1 http://localhost:8089/health
```

Manage the container from the same terminal:

```bash
wslc.exe container list
wslc.exe logs osdu-well360
wslc.exe stop osdu-well360
wslc.exe stop osdu-lite-opensearch
wslc.exe network rm osdu-lite-net
```

`--rm` removes each container after it stops. Storage is in memory; starting a
new API container resets its managed partition index and reloads the sample.
Use unique names/network if any of these names are already in use; never stop
an unrelated instance. Reserve at least 2 GB for OpenSearch. Security is disabled
for this private local-development bridge only: do not publish its ports or
use this configuration in production. No global sysctl changes are needed.
Only the API port is published. A WSLc `--internal` network cannot forward the
API's published port by itself; Compose uses a separate frontend network for
the API and an internal backend network for OpenSearch.

`latest` tracks `main`; pushing a `v*` tag to `mehmetkadriumay/osdu-lite`
also publishes matching versioned images. Both `amd64` and `arm64` are supported.

### Build from source with wslc.exe

From the repository root in your **WSL terminal**, build the existing
`server/Dockerfile` without Docker Desktop. Convert the build-context path for
the Windows executable:

```bash
wslc.exe build -t osdu-lite:local "$(wslpath -w "$PWD/server")"
wslc.exe run --rm -d --name osdu-well360 --network osdu-lite-net -p 8089:8089 \
  -e OPENSEARCH_URL=http://opensearch:9200 osdu-lite:local
```

Stop any previously started `osdu-well360` container before running the local
image. The image includes Python, pyproj and the bundled offline PROJ resources.
Java 17+ and Maven are only needed if you run the reference Java client.

### Test a running WSL container

From the repository root in WSL, run the HTTP smoke checks. They use Bash,
`curl` and Python 3's standard library; they do not require Docker or a local
installation of pyproj:

```bash
bash server/smoke-test.sh
bash server/reference-smoke-test.sh
python3 server/community-smoke-test.py
```

These target `http://localhost:8089` by default. Set `OSDU_BASE_URL` if you
published a different host port. Use `make java` to run the reference Java
client against the same running server.

### Native Python alternative

Start OpenSearch first. For a native API with a WSL-container node, use the same
OpenSearch launch flags above but add `-p 127.0.0.1:9200:9200`. This is a
**loopback-only development exception**, not public engine exposure. Alternatively
use an existing local node. The API defaults to `http://localhost:9200` and fails
startup clearly if the node is unavailable (5-second request timeout).

For native development and tests, install the pinned **binary-only** runtime
dependencies first (including pyproj 3.7.2 and certifi). Use CPython 3.11 or
3.12 and an isolated environment:

```bash
python3 -m venv .venv
source .venv/bin/activate              # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --only-binary=:all: --require-hashes -r server/requirements.txt
python -m unittest discover -s server/tests -v
OPENSEARCH_TEST_URL=http://localhost:9200 python -m unittest discover -s server/tests -p test_opensearch_integration.py -v
python server/server.py
```

The hash manifest supports Windows x64 and Linux amd64/arm64 on Python 3.11/3.12;
other platforms need separately reviewed wheel hashes. Missing native resources
fail at startup; there is no stub fallback. The container build installs the same
dependencies on the existing Python 3.12 Alpine base without a compiler.

### Optional Docker/Compose alternative

The existing Make start/stop/demo targets use Docker Compose, **not**
`wslc.exe`. They remain available for users who choose Docker + Compose instead
of the WSL-container commands above:

```bash
make start        # build and start the emulator on :8089, 15 Well 360 records preloaded
make smoke        # existing checks + numerical reference + community lifecycle HTTP checks
make java         # reference client (real os-core-common) against the emulator
make stop
```

`make demo` does all four in sequence. `make pull-up` is the image equivalent of
`make start` (via `server/compose.pull.yaml`).

### Runtime defaults

| Env var              | Default              | Meaning                                       |
| -------------------- | -------------------- | --------------------------------------------- |
| `PORT`               | `8089`               | listen port                                   |
| `OSDU_PARTITION`     | `company-prod`       | accepted `data-partition-id`                  |
| `OSDU_TOKEN`         | `demo-token`         | accepted bearer token                         |
| `REQUIRE_AUTH`       | `true`               | validate `Authorization` and RBAC             |
| `PRELOAD_FULL_SAMPLE`| `true`               | seed 15 Well 360 records on start             |
| `INDEX_DELAY_SECONDS`| `0.7`                | local delay before real indexing and refresh |
| `OPENSEARCH_URL`     | `http://localhost:9200` | OpenSearch origin; containers use `http://opensearch:9200` |

The demo token is a member of every group, so RBAC checks (HTTP 403 on a missing
entitlement) pass in all walkthroughs but still fail for unknown tokens.

## What is emulated

Storage and non-search services are in memory; every process restart resets the
managed OpenSearch index to match that clean state.

| Service      | API  | Highlights                                                          |
| ------------ | ---- | ------------------------------------------------------------------- |
| Storage      | v2   | PUT/GET/PATCH/DELETE, version history, copy, batch ops, kinds, cursors |
| Search       | v2   | real Lucene query_string, cursor queries, aggregations, as-owner, health checks |
| Dataset      | v1   | registration, storage/retrieval instructions, registry, revoke, soft delete |
| File         | v2   | metadata, upload URLs, signed delivery, getLocation                  |
| Workflow     | v1   | deploy/list/run lifecycle, `Osdu_ingest` manifest ingestion          |
| Legal        | v1   | tag CRUD, validate, batch retrieve, properties, query operators      |
| Schema       | v1   | kind list, schema create/update/system                               |
| Entitlements | v1/v2 | groups, members, membersCount, roles, appIds                       |
| CRS Catalog  | v3 subset | four fixed horizontal CRS entries, detail and filtered queries |
| CRS Converter | v3 subset | offline pyproj x/y conversion, ordered batches; z retained |
| Unit         | v3 subset | 17 fixed units, six dimensions, scale/offset coefficients |
| Indexer      | v2 local | real OpenSearch provision, bulk reindex, record reindex, kind deletion |
| Notification | v1 mock | validate/record incoming records-changed events; no external delivery |
| Partition    | v1 mock | property CRUD; does not provision additional authenticated tenants |
| Register     | v1 mock | subscriptions, secret rotation, DDMS data and exact-filter actions |
| Secret       | v2 mock | memory-only ACL-controlled values, enable/disable, value-free lists |

Two behaviors matter for anyone writing code against it:

- **Records are validated.** An ingest requires a supported `kind`, a populated
  `acl` (`viewers`/`owners`), and `legal.legaltags` that exist in the Legal
  service. A record that fails validation returns 400 with the reason.
- **Storage and Search are asynchronous.** A record is stored immediately but
  only appears in Search after the indexer delay (0.7 s default) plus the
  actual OpenSearch write and refresh time.

Retrieval instructions carry `size` and `sha256` for every known file, and the
reference client aborts if either mismatches the downloaded LAS. Upload tokens
and search cursors expire and are cleaned up lazily.

## Reference example: Well 360

Walks Well -> Wellbore -> WellLog -> Dataset -> LAS through three scenarios.

```bash
./examples/well360/demo/run-tutorial.sh      # record-by-record ingest + LAS upload + Java client
./examples/well360/demo/run-full-sample.sh   # load all 3 Wells + 4 Wellbores + 4 WellLogs + 4 Datasets
./examples/well360/demo/run-manifest.sh      # register Dataset, then ingest a Manifest via Osdu_ingest
```

The Java client (`examples/well360/client/src/main/java/com/beyond/osdu/Well360App.java`)
searches EAGLE-01, reads `W-1001`, resolves Wellbores and WellLogs, fetches the
Dataset retrieval instructions, downloads and checksums the LAS, then runs a
cursor search. It never modifies state.

Record IDs are deterministic (`company-prod:master-data--Well:W-1001`).
`examples/well360/samples/` and the demos use the demo LegalTag
`company-prod-demo-legaltag`; the untouched sources in `reference/` keep the
`<approved-legal-tag-name>` placeholder.

## Repository layout

```text
server/          the emulator (server.py, reference_services.py + Docker image)
  composition    compose.yaml (build) and compose.pull.yaml (image)
examples/well360/
  client/        os-core-common Java consumer
  samples/       record JSONs (Well, Wellbore, WellLog, Dataset, Manifest)
  demo/          run-tutorial.sh, run-full-sample.sh, run-manifest.sh
  reference/     the unmodified OSDU tutorial pack the example is based on
```

## Endpoint reference

The supported routes are listed below. Reference-service compatibility decisions
and limitations are explicit in the following section.

### Independently sourced public community extensions

The extension inventory is
[`server/data/community_api.json`](server/data/community_api.json): **126
method/path operations across 16 services**, including nine preserved reference
operations and the existing Search health routes. It is an extension coverage
inventory, **not a count of all legacy routes or a claim of full production API
parity**. `test_pinned_inventory_success` executes every inventory entry, checks
its success status/envelope and observable behavior, and rejects missing coverage.
Baseline contracts remain covered; unit tests explicitly inject an isolated fake
backend. CI separately exercises a pinned real OpenSearch service and HTTP smokes.

Each entry inherits its exact source URL, public API version, capability
(`native`, `local-opensearch` or `mock`), and SHA-256 from the service. Repository revisions pin the
independently verified **Apache-2.0 LICENSE** and source-only operations; live
OpenAPI documents are independently pinned by their content hashes, not assumed
to have been generated from those commits. Source paths and fingerprints are
recorded for Partition's hidden write APIs and Notification's push handler and
Core Plus envelope extractor. Inventory origin:
[community Core Services Overview](https://community.opengroup.org/groups/osdu/platform/-/wikis/Core-Services-Overview).
Only independently retrieved public community contracts/code were used; no
private standard text or draft-derived artifacts are distributed or claimed as
implemented.

| Area | Public operations added or expanded |
| --- | --- |
| Indexer v2 | `PUT /partitions/provision`, `POST/PATCH /reindex`, `POST /reindex/records`, `DELETE /index?kind=...` |
| Partition v1 | `GET /partitions`; `POST/GET/PATCH/DELETE /partitions/{partitionId}` |
| Register v1 | subscription create/read/query/list/delete and secret rotation; DDMS create/read/type-query/delete; action create/read/delete/test/retrieve; topics |
| Secret v2 | create, get, update, delete, ID list, value-free batch metadata, enable/disable; public health remains v1 |
| Notification v1 | `POST /push-handlers/records-changed`, `GET /_ah/warmup` |
| Unit v3 | catalog/search/lastmodified/mapstates; measurement lookup/search; unit lookup/search/preferred; SI unit-system lookup/list/assignments; `GET/POST /conversion/abcd` |
| CRS Catalog v3 | coordinate-transformation query/detail and `POST /points-in-aou` |
| CRS Converter v3 | `POST /convertGeoJson`, `POST /convertTrajectory` |
| Existing services | slash-free Dataset single registry GET, undelete; all-partition groups and global member removal; File URL revoke; Legal status job; Storage replay/status, batch/header reads, public `:delete`, historical-version purge; Workflow system registration/deletion and run latestInfo |
| Health/info | exact publicly inventoried paths, with auth/RBAC; no arbitrary `/service/*` fallback |

**State and safety.** New CRUD collections, replay jobs and incoming events are
in-memory only, bounded to 1,000 per collection, and reset by `clear_state` /
`POST /poc/reset`. New JSON bodies are limited to 1 MiB and reject duplicate keys,
malformed framing and nonfinite numbers. Reference batches are limited to 1,000
points. The demo token is seeded with explicitly **local**, not upstream, service
roles (`osdu-lite.<service>.user`; existing CRS role spellings are preserved).
The configured bearer token and `data-partition-id` checks run before dispatch;
auth-disabled development mode is retained. All responses support correlation
and CORS, including 204s. Unknown/unregistered methods and resources do not
silently succeed.

Partition CRUD stores property objects `{"properties":{"region":{"sensitive":false,
"value":"local"}}}`; it neither changes the single configured authentication
partition nor grants access to other tenants. Sensitive partition properties
are configuration data, **not** a production secret store.

Secret create/update use `{"id":"mock-key","value":"mock-only","secretAcls":
{"owners":[...],"viewers":[...]}}`. Owner membership is required for writes;
viewers/owners can read. Disabled secrets return 409 on value reads; list and
batch-list responses never include values. Subscription credentials are also
memory-only and omitted from every registration response. **Never send real
credentials**: there is no encryption, durable storage, cloud vault, credential
rotation service or production IAM.

Notification uses the public Core Plus envelope: `subscription` is a resource
path ending in the registration's `notificationId`, `message.data` is base64 JSON,
and `message.attributes["data-partition-id"]` matches the configured partition.
An accepted event records its matching subscription IDs with `delivered=false`;
there are **no HTTP callbacks**. DDMS URLs are inert data and never fetched.
Actions use exact `entityType`/`source`/`version` filters and static HTTP(S) URLs;
the components are derived from a record's `authority:source:entityType:version`
`kind`. Remote expressions and URL-template evaluation are deliberately unsupported.

Indexer operations synchronously provision/rebuild the managed OpenSearch index,
exclude soft-deleted records, and reject unknown/deleted IDs. `POST /reindex`
accepts a kind (wildcards supported); its optional upstream cursor is rejected
because this local implementation completes synchronously. `PATCH /reindex`
reindexes all kinds; `force_clean=true` removes matching kind documents before
rebuilding. `DELETE /index?kind=...` deletes only exact-kind documents, not a
physical/foreign index. Refresh completes before these operations return success.
Storage replay uses the same real indexing path, but remains a
synchronous mock job with completed per-kind status, not a cloud queue/event
replay. Historical version purge accepts `versionIds` or `from`/`limit`, and
refuses to delete the latest version. File revoke invalidates the local mock
storage account's outstanding upload grants; it is not cloud signed-URL
cryptography or revocation of already downloaded files.

### OpenSearch behavior and limits

The official image is pinned to **3.9.0** (published image manifest
`sha256:3b55f6b6249cfdb92b52ee59adc8a266203ba9ef9f5d82544fad6f1af2f3609e`),
as listed in the [official Docker guide](https://docs.opensearch.org/latest/install-and-configure/install-opensearch/docker/).
GHCR publishes only the Python API image; it consumes this separate official
engine image. No Dashboards, Kafka, Java Indexer deployment or cloud messaging
is involved.

One index named `osdu-lite-<32 hex SHA-256 characters of OSDU_PARTITION>` is owned
by the configured API. Startup/reset deletes **only that exact index**. Do not
run multiple API processes with the same partition against one node: their
ephemeral Storage would disagree. No foreign or wildcard index deletion occurs.

Queries go unchanged to OpenSearch `query_string`; empty/`*` becomes `match_all`.
Malformed Lucene syntax returns 400, never substring fallback. Standard text
analysis means `data.FacilityName:"EAGLE-01"` is an analyzed phrase, not Python
whole-value equality; use `data.FacilityName.keyword:"EAGLE-01"` for exact values.
IDs, kinds and ACLs are keyword fields; data strings have text and `.keyword`
fields. Sample depth/coordinate fields are numeric and `data.SpudDate` is a
date. Other values use dynamic types with automatic date detection disabled.
Heterogeneous kinds with incompatible types for the same path will fail indexing
explicitly (503); normalize input rather than expecting per-kind mappings.

Kind and viewer/owner ACL filters run in OpenSearch **before** totals, paging and
aggregations; `query_as_owner` restricts to owners. Auth-disabled mode is a local
bypass. Results retain stable ID order, not relevance order (boosts are parsed
but do not change that order). The adapter exhausts a 500-document scroll,
clears engine scroll resources, and materializes all authorized matches for the
existing Python pagination/aggregation and expiring cursor snapshot contracts.
This intentionally scales with matched data, not for unbounded production
datasets. Cursor snapshots are caller-and-entitlement-bound and may retain
records changed/deleted after snapshot creation until expiry.

Delayed writes use a generation/version guard under the Storage lock; reset,
newer versions and deletes cannot be undone by stale tasks. Engine refresh
completes after each indexing operation (the delay is 0.7 seconds **plus actual
transport time**, not a visibility guess). All synchronous mutation hooks,
dataset restore and Storage replay use the same backend. Failed asynchronous
writes are retained as an index error: the next search/readiness check rebuilds
from current Storage, or returns 503 if recovery fails. Bulk item failures are
checked even on HTTP 200; no successful-empty-search fallback exists. Storage
may already contain a write when indexing fails; reindex/recovery is authoritative.
Missing managed indexes are recreated with explicit mappings and rebuilt from
Storage before reads or writes, including delayed writes.
Transport requests time out after 5 seconds and index actions are serialized
under the local Storage lock. Startup is fail-fast; runtime liveness remains
independent, while `/health` and Search/Indexer readiness require the backend.

**Reference extensions.** The 17 units and four CRSs do not inflate the 15 seeded
Storage records. Unit POST lookups resolve existing essences, not create new
definitions. ABCD coefficients use `y=(a+b*x)/(c+d*x)`; for existing affine
coefficients they are `a=-scale*offset, b=scale, c=1, d=0`. New Unit essence
lookups also validate the public UAD/ABCD form; legacy `/conversion/scale`
retains its strict USO shape. The sole unit system is honestly named
`OSDU-Lite SI`. Namespace maps/mapstates return empty, not invented mappings.
Catalog search supports `*` or literal text only; unsupported query syntax
returns 400. Catalog search includes units and measurements with combined
pagination. The namespace remains `osdu-lite`, not an asserted upstream alias.
Generated upstream documents contain two notable schema inconsistencies: some
204 responses declare bodies (this server correctly sends no body), and
GeoJSON's generated `oneOf` geometry variants have overlapping type constraints.
The emulator uses standard GeoJSON geometry discriminators, not artificial fields
to satisfy an ambiguous union.

CRS transformation records are **local horizontal projection operations**, not
official datum-transformation records. Area checks use the catalog's rectangular
operating extents and geodesic approximate distances to the bounding box.
GeoJSON recursively visits Feature/Geometry collections and all standard geometry
coordinate arrays, preserves properties, removes stale bboxes, accepts
catalog-issued CRS IDs/references, and performs real offline horizontal PROJ
conversion. Any out-of-domain position rejects the whole GeoJSON operation (422);
no nonfinite numeric JSON or silent partial geometry is returned. Optional Z
unit rescaling is linear, never a vertical datum transformation.

Trajectory is explicitly **mock/approximate**, not survey-accurate: local
minimum-curvature integration, `TRUE_NORTH`, `AzimuthalEquidistant`, increasing
MD/inclination/azimuth stations starting at MD 0, at most 1,000 stations and
100 km measured depth. `inputStations` is JSON serialized into a string, as in
the public schema. Units are catalog Length essences/OSDU unit IDs. It returns
computed local deviations and geographic positions, with positive-down `dz`;
`operationsApplied` explicitly disclaims survey accuracy, grid convergence,
datum correction and interpolation. LMP, other station kinds and antiparallel
adjacent tangents are rejected rather than echoed as successful conversions.

**Explicit exclusions:** Policy and Project & Workflow services; `convertBinGrid`;
official datum/grid transformations, complete CRS/unit catalogs; DDMS redirects;
remote action expression/template evaluation; cross-namespace Storage copying;
tenant provisioning; and unlisted upstream functionality. Existing legacy
interfaces and emulator-specific payloads are preserved, not re-labelled as
newly verified production conformance.

Run the additional stdlib-only HTTP smoke against a local test instance:

```bash
python server/community-smoke-test.py
```

It exercises all 16 infos, new mock lifecycles, indexing/Search, replay, Dataset
undelete, GeoJSON, ABCD, auth/error handling, and CORS/correlation. Temporary
records/resources are cleaned up and the original Storage count is checked.
Incoming mock notification events/replay statuses remain until reset/restart.

### CRS and Unit: bounded community v3 subset

This is **partial community-API emulation, not OSDU Standard v1.0 compliance,
certification, or a full reference catalog**. Reviewed mutable OpenAPI v3.0.0
sources (retrieved 2026-10-27), fingerprints, examples, and deliberate differences
are frozen in `server/tests/fixtures/reference_contracts.json`. The bundled data
and attribution are in `server/data/reference_catalog.json`; no records are
added to Storage/Search, and reset/preload counts are unchanged.

| Method | Path (also accepted without `/api`) |
| --- | --- |
| GET / POST | `/api/crs/catalog/v3/coordinate-reference-system` |
| POST | `/api/crs/converter/v3/convert` |
| GET | `/api/unit/v3/unit` |
| GET | `/api/unit/v3/unit/symbol?namespaces=osdu-lite&symbol=ft%5BUS%5D` |
| GET | `/api/unit/v3/unit/measurement?ancestry=Length` |
| GET | `/api/unit/v3/measurement/list` |
| GET / POST | `/api/unit/v3/conversion/scale` |

All use the existing bearer token, partition, error envelope, CORS and correlation
conventions. Separate **emulator-local** roles are `osdu-lite.crs.catalog.user`,
`osdu-lite.crs.converter.user`, and `osdu-lite.unit.user` (seeded for the demo token).
These names are not asserted to be official upstream entitlements.

**Catalog.** EPSG:4326, 3857, 32638, 32639 only. POST supports exact string filters
`codeSpace`, `code`, `id` (the data ID), `kind`, case-insensitive substring `name`
(no wildcard interpretation), `includeDeprecated`, `offset`, `limit`,
`returnAllFields`, and `returnedFields`. Filters combine with AND; results are
ordered by numeric EPSG code, with totals before pagination and default limit all.
Boolean/integer options accept their JSON types or strict string forms. Unknown
filters/field paths fail with 400; `returnAllFields` overrides projection. The local
default projection is `id`, `kind`, and `data.{ID,Name,Code,CodeSpace,Kind,PersistableReference}`;
explicit projection uses nested record paths. The returned `query` is a diagnostic
of local filtering, **not an executed Search query**.

GET requires exactly one `recordId` with a terminating colon, e.g.
`osdu:reference-data--CoordinateReferenceSystem:Geographic2D:EPSG::4326:`, or
`dataId=Geographic2D:EPSG::4326` (no terminating colon), and returns full detail in
`searchResults.results` plus `totalCount`. Unknown identifiers return 404.

**Point converter (`/convert`).** Pass the exact JSON-serialized `data.PersistableReference` strings
returned by this catalog as `fromCRS`/`toCRS`, with `points: [{"x":10,"y":20,"z":123.45}]`.
Only catalog-issued definitions are accepted, ignoring JSON whitespace/key order
but checking all WKT and metadata. These local LBC references use bundled PROJ
WKT2 and `ver=OSDU-Lite/pyproj-3.7.2`, not upstream PE WKT/versioning. Bare EPSG
strings, record IDs, alternate/custom WKT, pipelines, bound/vertical/dynamic
definitions and datum changes are unsupported. No definitions supplied by a caller
are executed by PROJ.

`always_xy=True`: longitude/latitude for geographic input; easting/northing for
projected input. Only x/y is converted; successful z is retained numerically
unchanged with **no vertical semantics, unit conversion or accuracy guarantee**.
PROJ uses `allow_ballpark=False`, `only_best=True` and checked errors; there is no
approximate or identity fallback. Identity requests for an allowed CRS still use
PROJ and validate its domain. Actual successful operation descriptions are returned
in `operationsApplied` (empty for an all-failed batch).

Operating envelopes are deliberately conservative: 4326 longitude ±180°, latitude
±90°; 3857 latitude ±85.0511287798066°; UTM 38N longitude [42,48]°, 39N [48,54]°,
both latitude [0,84]°. Source and target domains are checked; projected coordinates
must round-trip (1e-6 m absolute / 1e-12 relative tolerance) to reject wrapping.
Inverse boundary noise allows 1e-10°; coordinates are never clamped.

Generated public schemas declare coordinates/counts as strings despite numeric
request examples. This emulator accepts finite JSON numbers or strict decimal/
exponent strings, and **returns string x/y/z and string `successCount`**. Malformed
points reject the entire request with 400. A well-formed out-of-domain point becomes
`{"x":"NaN","y":"NaN","z":"NaN"}` without changing order/batch length. This failure
encoding is an **explicit emulator policy, not verified upstream serialization**.
Booleans, null, NaN, Infinity, overflow and arbitrary numeric coercion are rejected.
Batches require 1–1,000 points; JSON bodies on these new routes are capped at 1 MiB
(413 above that); strict parsing is confined to new routes.

**Units.** The namespace is **`osdu-lite` only**: upstream namespace and ancestry
identity could not be established, so this curated SI-based catalog is not passed
off as the official catalog. Dimension identities and supported symbols:

| Local ancestry | Symbols |
| --- | --- |
| Length | `m`, `km`, `ft`, `ft[US]` |
| Time | `s`, `ms` |
| PlaneAngle | `dega`, `rad` |
| Pressure | `Pa`, `kPa`, `bar`, `psi` |
| AbsoluteTemperature | `K`, `degC`, `degF` |
| Speed | `m/s`, `ft/s` |

International foot is 0.3048 m; US survey foot is 1200/3937 m. Absolute temperature
has affine offsets; temperature differences and arbitrary compound expressions are
not supported. Dimensions must match. GET scale requires `namespaces`, `fromSymbol`,
`toSymbol`. POST takes exactly one of `fromUnit`/`fromUnitPersistableReference`
and exactly one of `toUnit`/`toUnitPersistableReference`. Objects must match catalog
`essence`; string references are the corresponding `essenceJson`. The source-backed
wire field is `baseMeasurement`, not the generated interface's `baseMeasurementEssence`.
Caller-supplied conflicting coefficients are rejected. The new
`/conversion/abcd` route covers affine coefficients only; there is no general
nonlinear-unit solver or converted-value/batch endpoint.

The response is `fromUnit`, `toUnit`, `scaleOffset` using **`y = scale * (x - offset)`**.
For example `degF` → `degC` yields scale 5/9 and offset 32; applying to 212 gives 100.
Unknown units/namespaces return 404; malformed definitions/incompatible dimensions
return 400. Unit/measurement lists default to offset 0, limit 100; limit -1 returns
the whole subset; limit 0 is an empty page. Duplicate, missing and blank parameters
are rejected, not silently defaulted.

**Offline runtime and checks.** pyproj uses its wheel's bundled PROJ database with
`PROJ_NETWORK=OFF` and the network API disabled. There are no runtime HTTP calls,
grid downloads or writable-cache requirements. Local validation used pyproj 3.7.2,
PROJ 9.5.1, EPSG v11.022. Run `bash server/reference-smoke-test.sh` against a running
server for numerical HTTP assertions; `make smoke` includes it. CI builds Alpine
for amd64 and arm64 and runs a real conversion probe under `--network none`.
CI uses Docker for these checks; local WSL-container use does not require Docker
Desktop. Wheel availability alone does not establish that either image works.

### Health

| Method | Path      | Notes                          |
| ------ | --------- | ------------------------------ |
| GET    | `/health` | status, record counts, profile |

### Entitlements

| Method | Path                                             | Notes                  |
| ------ | ------------------------------------------------ | ---------------------- |
| GET    | `/api/entitlements/v1/groups`                    | `?roleRequired=true`   |
| GET    | `/api/entitlements/v2/groups`                    |                        |
| POST   | `/api/entitlements/v2/groups`                    |                        |
| GET    | `/api/entitlements/v2/groups/{email}/members`    | `?includeType&role=`   |
| GET    | `/api/entitlements/v2/groups/{email}/membersCount` | `?role=`             |
| POST   | `/api/entitlements/v2/groups/{email}/members`    |                        |
| PATCH  | `/api/entitlements/v2/groups/{email}`            | JSON Patch, incl `/appIds` |
| DELETE | `/api/entitlements/v2/groups/{email}`            |                        |
| DELETE | `/api/entitlements/v2/groups/{email}/members/{member}` |                 |
| GET    | `/api/entitlements/v2/members/{email}/groups`    |                        |

### Legal

| Method | Path                                    | Notes                          |
| ------ | --------------------------------------- | ------------------------------ |
| GET    | `/api/legal/v1/legaltags`               |                                |
| POST   | `/api/legal/v1/legaltags`               |                                |
| GET    | `/api/legal/v1/legaltags/{name}`        |                                |
| PUT    | `/api/legal/v1/legaltags`               |                                |
| DELETE | `/api/legal/v1/legaltags/{name}`        |                                |
| GET    | `/api/legal/v1/legaltags:properties`    |                                |
| POST   | `/api/legal/v1/legaltags:validate`      |                                |
| POST   | `/api/legal/v1/legaltags:batchRetrieve` |                                |
| POST   | `/api/legal/v1/legaltags:query`         | `queryList`/`operatorList` (union/intersection/add), `valid`, `between`, `attr=value` |

### Schema service

| Method | Path                                 | Notes          |
| ------ | ------------------------------------ | -------------- |
| GET    | `/api/schema-service/v1/schema`      |                |
| GET    | `/api/schema-service/v1/schema/{kind}` |              |
| POST   | `/api/schema-service/v1/schema`      |                |
| PUT    | `/api/schema-service/v1/schema`      |                |
| PUT    | `/api/schema-service/v1/schemas/system` |             |

### Storage

| Method | Path                                     | Notes                                   |
| ------ | ---------------------------------------- | --------------------------------------- |
| GET    | `/api/storage/v2/query/kinds`            |                                         |
| GET    | `/api/storage/v2/query/records`          | `?kind&limit` -> `{records:[ids], cursor}` |
| GET    | `/api/storage/v2/records`                | `?kind&deleted&sortOrder&modifyAfterDate&cursor&limit` |
| GET    | `/api/storage/v2/records/{id}`           | `?version=` / `?attribute=`             |
| GET    | `/api/storage/v2/records/{id}/{version}` | fixed version read                      |
| GET    | `/api/storage/v2/records/versions/{id}`  | -> `{recordId, versions:[ints]}`        |
| PUT    | `/api/storage/v2/records`                | upsert; returns `{recordCount, recordIds}` |
| PATCH  | `/api/storage/v2/records/{id}`           | in-place merge, version increment        |
| PATCH  | `/api/storage/v2/records`                | collection merge                        |
| POST   | `/api/records/{id}:copy`                 | -> `{recordCount, recordIds, newRecordId}` |
| POST   | `/api/records/{id}:delete`               | single logical delete (204)             |
| POST   | `/api/storage/v2/records/delete`         | batch soft delete (204), non-empty list |
| DELETE | `/api/storage/v2/records`                |                                         |
| DELETE | `/api/storage/v2/records/{id}`           | `?hardDelete=true` deletes permanently   |
| DELETE | `/api/storage/v2/records/versions/{id}`  | purge every version                     |
| POST   | `/api/storage/v2/query/records:batch`    |                                         |

### Search

| Method | Path                                   | Notes             |
| ------ | -------------------------------------- | ----------------- |
| POST   | `/api/search/v2/query`                 | see below         |
| POST   | `/api/search/v2/query_with_cursor`     |                   |
| DELETE | `/api/search/v2/query_with_cursor/{cursor}` |               |
| GET    | `/api/search/v2/liveness_check`        |                   |
| GET    | `/api/search/v2/readiness_check`       |                   |

`POST /api/search/v2/query` supports `offset`, `limit` (max 1000),
`returnedFields`, `kind` (OpenSearch keyword wildcard patterns), `query` (Lucene `AND`/`OR`/
`NOT`, quoted fields, `[a TO b]` ranges, parentheses), `trackTotalCount` (false
omits `totalCount`), `query_as_owner` (only records the caller owns), and
`aggregateBy` with `count`, `filteredTerms`, and `histogram` aggregations.

### Dataset / File

| Method | Path                                           | Notes                                   |
| ------ | ---------------------------------------------- | --------------------------------------- |
| POST   | `/api/dataset/v1/storageInstructions`          | -> `{storageLocation: {signedUrl, token}}` |
| POST   | `/api/dataset/v1/getDatasetRegistry`           | batch by `datasetRegistryIds`           |
| GET    | `/api/dataset/v1/getDatasetRegistry/?id=`      | single                                  |
| POST   | `/api/dataset/v1/retrievalInstructions`        | by `datasetRegistryIds`                 |
| GET    | `/api/dataset/v1/retrievalInstructions`        | `?id=` single; skips revoked URLs       |
| POST   | `/api/dataset/v1/revokeURL`                    | marks storage URLs revoked              |
| POST   | `/api/dataset/v1/metadataRecord/{id}/softDelete` | 204                                   |
| PUT    | `/api/dataset/v1/registerDataset`              | single record or `{datasetRegistries:[...]}` |
| POST   | `/api/file/v2/files`                           | upload a file blob                      |
| GET    | `/api/file/v2/files/{id}`                      | metadata + `fileSourceDatasWithLocation` |
| GET    | `/api/file/v2/files/{id}/downloadURL`          | lower-case download                     |
| GET    | `/api/file/v2/files/{id}/DownloadURL`          | OSDU-cased response, `{signedURL}`      |
| GET    | `/api/file/v2/files/{id}/metadata`             |                                        |
| DELETE | `/api/file/v2/files/{id}/metadata`             |                                        |
| GET    | `/api/file/v2/files/uploadURL`                 |                                        |
| POST   | `/api/file/v2/files/metadata`                  | 201 `{id}`                              |
| POST   | `/api/file/v2/getLocation`                     |                                        |
| POST   | `/api/file/v2/delivery/getFileSignedUrl`       | `{processed, unprocessed}`              |

### Workflow

| Method | Path                                              | Notes             |
| ------ | ------------------------------------------------- | ----------------- |
| GET    | `/api/workflow/v1/workflow`                       | list workflows    |
| POST   | `/api/workflow/v1/workflow`                       | deploy            |
| GET    | `/api/workflow/v1/workflow/{name}`                |                   |
| DELETE | `/api/workflow/v1/workflow/{name}`                |                   |
| GET    | `/api/workflow/v1/workflow/{name}/workflowRun`    | run history       |
| POST   | `/api/workflow/v1/workflow/{name}/workflowRun`    | trigger           |
| GET    | `/api/workflow/v1/workflow/{name}/workflowRun/{runId}` | status + results |
| PUT    | `/api/workflow/v1/workflow/{name}/workflowRun/{runId}` | update         |
| DELETE | `/api/workflow/v1/workflow/{name}/workflowRun/{runId}` |                 |
| POST   | `/api/workflow/v1/workflow/Osdu_ingest/workflowRun` | manifest ingest  |

### POC helpers (not OSDU)

| Method | Path                        | Notes                          |
| ------ | --------------------------- | ------------------------------ |
| POST   | `/poc/reset`                | clear all state                |
| POST   | `/poc/load-full-sample`     | reseed the 15-record sample    |

## Manual ingestion (production style, from zero)

Start empty, then ingest one of each record type with plain `curl`, no POC
helpers. This mirrors what a real client does against a deployment.

```bash
# 1. start empty with WSL containers
# If the quickstart container is still running, stop it first:
wslc.exe stop osdu-well360
wslc.exe run --rm -d --name osdu-well360 -p 8089:8089 \
  -e PRELOAD_FULL_SAMPLE=false ghcr.io/mehmetkadriumay/osdu-lite:latest
curl --fail --retry 20 --retry-connrefused --retry-delay 1 http://localhost:8089/health

export OSDU_BASE_URL=http://localhost:8089 OSDU_PARTITION=company-prod OSDU_TOKEN=demo-token
export AUTH="Authorization: Bearer $OSDU_TOKEN"
export PARTITION_HEADER="data-partition-id: $OSDU_PARTITION"

# 2. checks
curl -s -H "$AUTH" -H "$PARTITION_HEADER" "$OSDU_BASE_URL/api/entitlements/v1/groups?roleRequired=true" | jq
curl -s -H "$AUTH" -H "$PARTITION_HEADER" "$OSDU_BASE_URL/api/legal/v1/legaltags" | jq
curl -s -H "$AUTH" -H "$PARTITION_HEADER" "$OSDU_BASE_URL/api/storage/v2/query/kinds?limit=100" | jq

# 3. Dataset storage instructions + LAS upload
export UPLOAD_URL=$(curl -s -X POST "$OSDU_BASE_URL/api/dataset/v1/storageInstructions?kindSubType=dataset--File.Generic" \
  -H "$AUTH" -H "$PARTITION_HEADER" -H "Content-Type: application/json" -d '{}' | jq -r '.storageLocation.signedUrl')
curl -s -X PUT "$UPLOAD_URL" -H "x-file-name: eagle01_gr_cali.las" \
  --data-binary @examples/well360/demo/eagle01_gr_cali.las | jq

# 4. register Dataset + ingest business records via a Manifest
curl -s -X PUT "$OSDU_BASE_URL/api/dataset/v1/registerDataset" \
  -H "$AUTH" -H "$PARTITION_HEADER" -H "Content-Type: application/json" \
  --data-binary @examples/well360/samples/sample_dataset_record.json | jq
curl -s -X POST "$OSDU_BASE_URL/api/workflow/v1/workflow/Osdu_ingest/workflowRun" \
  -H "$AUTH" -H "$PARTITION_HEADER" -H "Content-Type: application/json" \
  --data-binary @examples/well360/samples/manifest_example.json | jq

# 5. read + search
curl -s -H "$AUTH" -H "$PARTITION_HEADER" \
  "$OSDU_BASE_URL/api/storage/v2/records/company-prod:master-data--Well:W-1001" | jq
curl -s -X POST "$OSDU_BASE_URL/api/search/v2/query" \
  -H "$AUTH" -H "$PARTITION_HEADER" -H "Content-Type: application/json" \
  -d '{"kind":"osdu:wks:master-data--Well:*","query":"data.FacilityName:\"EAGLE-01\"","limit":20}' | jq

# 6. retrieval instructions + download + verify the LAS
export DOWNLOAD_URL=$(curl -s -X POST "$OSDU_BASE_URL/api/dataset/v1/retrievalInstructions" \
  -H "$AUTH" -H "$PARTITION_HEADER" -H "Content-Type: application/json" \
  -d '{"datasetRegistryIds":["company-prod:dataset--File.Generic:LOG-1001-GR-LAS"]}' | jq -r '.datasets[0].retrievalProperties.signedUrl')
curl -s "$DOWNLOAD_URL" -o /tmp/eagle01_gr_cali.las && shasum -a 256 /tmp/eagle01_gr_cali.las
```

Step 5 demonstrates the storage/search split: an ingested record is stored at
once but only shows up in Search after the indexer delay (0.7 s). Retry a search
that returns zero after about a second.

## Tests

The test runner uses stdlib `unittest`, with pinned runtime dependencies installed;
no container runtime or live server is needed:

```bash
make test
```

Tests cover: record validation and version history; PATCH, `:copy`, batch and
single delete, hard purge; the search query language and cursor lifecycle; the
indexer delay; Dataset upload -> register -> retrieval with size + SHA-256;
batch register, revokeURL, soft delete; File v2 metadata/uploadURL/delivery;
Workflow deploy/list/run and manifest ingest; LegalTag CRUD, validate, and
`:query` operators; Schema list/create/system; entitlements membersCount/appIds;
RBAC 403s; token and cursor TTL cleanup; all 126 inventoried public operations;
new-service read-after-write/update/delete/enable-disable lifecycles; ACL/value
redaction, limits, malformed framing, reset isolation; and reference numerical
checks. Inventory coverage is checked independently of the dispatcher's route
registration.

CI (`.github/workflows/ci.yml`) runs the unit suite, starts the emulator for
`smoke-test.sh`, `reference-smoke-test.sh`, and `community-smoke-test.py`, and runs
the client contract check (pinned os-core-common
3.1.0 API surface, fully offline - the OSDU community Maven registry has been
flaky since the OSDU->OpenDES hosting migration). A second workflow builds and
publishes the multi-arch container image to GHCR.

## Troubleshooting

| Symptom                              | Cause and fix                                                          |
| ------------------------------------ | --------------------------------------------------------------------- |
| `wslc.exe: command not found`        | Update WSL to 2.9.3 or later from PowerShell; ensure Windows interoperability and Windows PATH import are enabled in the WSL distribution. |
| Container name or port already used | Use `wslc.exe container list` to identify your previous instance and `wslc.exe stop osdu-well360` to stop it, or choose another name and host port. |
| 401                                  | use `Authorization: Bearer demo-token`                                 |
| 400                                  | use `data-partition-id: company-prod`                                  |
| 403                                  | token is not in the required entitlement; the demo token has all groups |
| 400 on ingest                        | record failed validation - missing `acl`/`legal`, unknown kind or LegalTag |
| EAGLE-01 not found                  | data not loaded; check `/health`                                       |
| in Storage, missing from Search      | indexer delay, wait ~1 s and retry                                     |
| no LAS downloadable                  | Dataset missing: `GET /api/dataset/v1/getDatasetRegistry/?id=company-prod:dataset--File.Generic:LOG-1001-GR-LAS` |
| `mvn` cannot resolve `os-core-common` | the artifact is only on the OSDU community registry (`https://community.opengroup.org/api/v4/projects/67/packages/maven`), which has been flaky during the OSDU->OpenDES hosting migration; retry `mvn -U clean compile` or wait for the registry. CI does not depend on it (`client/check-client.sh`). |

## Moving to a real OSDU

Swap the defaults for client-provided values: base URL, partition, bearer token,
ACL groups, approved LegalTags. The Java Search/Storage code stays as is; confirm
the real deployment's routes, schema versions, and OpenAPI before ingesting.
Production hardening (OIDC, TLS, retries, metrics) is intentionally out of scope.

## License

Apache License 2.0 - see [LICENSE](LICENSE). Copyright 2026 Sohaib Qasem.