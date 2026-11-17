---
goal: Implement schema-backed ingestion, bounded OpenSearch queries, executable manifests, and volatile authenticated delivery
version: 1.0
date_created: 2026-11-16
last_updated: 2026-11-16
owner: osdu-lite maintainers
tags: [feature, architecture, ingestion, search, raspberry-pi]
---

# Introduction

Implement a useful, explicitly limited OSDU-like ingestion and search service on two Raspberry Pi 5 devices with 16 GB RAM each: one Python API replica and one OpenSearch node. Replace placeholder schemas, whole-result search materialization, timer-based workflow completion, and notification recording without delivery. All application state remains volatile. This document authorizes no persistence, database, broker, Airflow deployment, or general platform rewrite.

Planning baseline: repository `sohaibqasem/osdu-lite`, revision `36ef763`; initial `git status --short` was empty. These line references describe that revision, not future edits. Implementation MUST preserve unrelated work introduced after this baseline. This task authorizes planning only. This PRD does not initiate implementation or add a new approval gate.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in RFC 2119.

**Cross-reference conventions**: `FR-` functional requirements; `NFR-` non-functional requirements; `FM-` failure modes; `AC-` acceptance criteria; `RD-` resolved decisions; `EPIC-` implementation epics; `ITEM-` work items. There is no input `.req.md`.

## 1. Goals and Non-Goals

- **Goal 1**: Validate records against offline, versioned schemas while allowing bounded registration of additional kinds.
- **Goal 2**: Push authorized paging, sorting, projection, aggregations, and selected point-spatial filters into OpenSearch without materializing matches in Python.
- **Goal 3**: Execute supported manifests through the same admission, validation, storage, and indexing path as direct ingestion.
- **Goal 4**: Deliver opt-in authenticated callbacks through bounded volatile queues with explicit retry and failure reporting.
- **Non-Goal 1**: Persistence or restart recovery of records, schema registrations, workflow runs, cursors, queues, or subscriptions.
- **Non-Goal 2**: Full OSDU conformance, arbitrary public schemas, arbitrary search DSL, orchestration engines, or exactly-once delivery.

### In Scope

- Four bundled compatibility schemas plus a finite JSON Schema registration profile and local reference resolver.
- Schema-derived per-full-kind indices; finite search request profile; PIT/search-after cursors.
- The existing `Osdu_ingest` workflow only; finite manifest extraction; index-outcome-driven completion.
- Shared bounded worker infrastructure, per-record index retries, per-subscription HTTPS/HMAC delivery, diagnostics, unit and real-engine tests.
- Small lifecycle changes in the existing HTTP server; explicitly bounded HTTP concurrency and body sizes.

### Out of Scope (deferred)

- SQLite, migrations, durable files/outbox, durable volumes, brokers, Kafka, Airflow, distributed workers, extra services — contradict intentional volatility and Pi constraints.
- OIDC/authentication redesign, new ACL model, general trajectory/catalog/domain expansion, or changing the real `pyproj` behavior.
- Workflow DAGs, custom workflow execution, arbitrary webhooks/action URL execution, GSA token minting, replay API, or notification bodies containing record data.
- Arbitrary GeoJSON polygons/intersections, nearest-neighbor search, scripts/runtime fields, unrestricted aggregations, schema migration/reindex APIs, or arbitrary nested JSON Schema composition.
- Changelog/package-version updates and unrelated cleanup.

## 2. Terminology

| Term | Definition |
|------|------------|
| Full kind | Exact `authority:source:entityType:major.minor.patch` identity; never entity type alone. |
| Compatibility schema | Bundled, documented subset of public OSDU field contracts, deliberately permissive on legacy sample extras; not an official full schema or conformity claim. |
| Catalog | Read-only bundled schemas, local reference documents, source provenance and supported field mapping declarations. |
| Admission | Atomic reservation of record, byte, job, and queue capacity before visible mutation. |
| Generation | Monotonic per-record revision/tombstone fence preventing stale worker operations from superseding newer state. |
| Terminal run | `succeeded` or `failed`, with immutable outcome and per-record details. |
| PIT | OpenSearch point-in-time context holding an engine snapshot; Python retains metadata, not result documents. |
| Destination | Exact administrator-allowlisted HTTPS origin and path, associated with approved network addresses. |
| Volatile | Process/container restart deliberately invalidates or removes the relevant state; there is no recovery guarantee. |

## 3. Solution Architecture

### Verified codebase facts

| Evidence at baseline | Consequence |
|------|------------|
| `server/server.py:33-37,311-328`: four fixed kinds and ACL/legal checks; `:67,810-845`: separate schema dictionary and generated empty schemas. | Replace fixed-kind validation with registry lookup without removing ACL/legal validation. |
| `server/server.py:355-389,2100-2113`: POST storage writes mutate memory and schedule daemon-thread indexing; later invalid items can leave earlier writes. | Prevalidate whole batches and reserve capacity before committing. Do not invent a storage PUT alias. |
| `server/search_backend.py:16-24,43-81`: partition index, shared dynamic mapping, bulk batches of 200. | Isolate full kinds and restrict mappings using registered schema fields. |
| `server/search_backend.py:100-140`; `server/server.py:1519-1577`: engine scroll returns every authorized hit; Python slices/projects/aggregates; cursor stores full snapshots. | Replace adapter contract and preserve public response envelopes, not backend materialization. |
| `server/server.py:856-878,1932-2010`: manifest extraction and storage ingestion already exist, but run completion uses elapsed indexing delay. | Retain extraction compatibility; completion MUST follow actual item outcomes. |
| `server/community_services.py:191-328`: subscription/HMAC/GSA secret registration, partition-checked incoming events, `delivered:false` event records; no actual HTTP delivery. | Implement a separately testable outbound transport and report real results. |
| `server/server.py:56,890-891,2401-2408`; `server/tests/test_server.py:19-22`: backend construction/sample preload occur around import; tests replace backend afterwards. | Move network/worker startup to explicit runtime lifecycle; keep dependency injection deterministic. |
| `server/requirements.txt`, `server/Dockerfile:1-5`: hash-pinned dependencies and binary-wheel installation; compose/CI pin OpenSearch 3.9.0. | Pin schema validation dependencies with hashes and validate arm64 wheels. |
| `server/compose.yaml:1-25`: no OpenSearch data volume. | Do not introduce one; additionally fence stale engine data when API alone restarts. |

### Components and interfaces

| Component | Owned behavior and interface |
|------|------------|
| `server/schema_registry.py` (new) | Load catalog; normalize identity; `register(payload, method)`, `get(kind)`, `validate_record(record)`, `field_capabilities(kinds, fields)`, `mapping(kind)`. Pure validation/import; no network. |
| `server/schemas/` (new) | Four compatibility JSON schemas, one shared local definitions document, one catalog manifest including provenance, SHA-256 and mapping paths. |
| `server/runtime_jobs.py` (new) | Lifecycle-owned bounded scheduler, index/delivery capacity reservations, bounded retry heap, worker limits, terminal callbacks, monotonic clock injection. No durable state. |
| `server/search_backend.py` | Per-kind index lifecycle, bounded search DTO/DSL translation, PIT create/search/delete, bounded bulk operations, engine error normalization. No API authorization decisions. |
| `server/callback_delivery.py` (new) | Destination policy, DNS/IP validation and connection pinning, canonical JSON/HMAC signing, no-redirect HTTPS transport, bounded response reads. Transport injectable in tests. |
| `server/server.py` | Existing auth/ACL/legal checks and routing; atomic admission; schema registry and worker injection; workflow run state; HTTP body/concurrency caps; explicit startup/shutdown. |
| `server/community_services.py` | Preserve register/event routes; hold subscriptions/secrets; authorize and enqueue delivery; expose redacted delivery status. Does not execute action URLs. |

Data flow: authenticated request → request/body limit → authorization → complete schema/legal/ACL validation → reserve capacity → commit volatile records and enqueue version-fenced work → bounded index workers → indexed/failed terminal result → workflow status and optional minimal notification → bounded delivery workers → authenticated destination.

Schema registration and admission share the existing process-state lock or a narrowly introduced single lock. Never hold it during network I/O. Same-record operations MUST be serialized and carry engine-enforced external versions: use the per-ID monotonically increasing generation as OpenSearch `version`, with `version_type:external_gte` for identical retries. Delete writes a minimal, same-versioned `_deleted:true` document, not a physical engine DELETE. All search/count/aggregation queries exclude deletion markers. Keep the highest per-ID generation/kind fence until epoch teardown, including hard-delete/recreate; fences count against the 5,000-ID cap. Thus even a timed-out old HTTP request applied after a newer acknowledged write cannot overwrite it. Equal-generation retries MUST have identical payloads. Version conflicts from superseded work are not retryable failures of the newer version. Cross-record execution uses only configured workers.

### Runtime contract and hard limits

All limits apply per API process/partition. Constants below are fixed in the initial release except the explicitly named environment variables. Rejection MUST precede mutation; no silent truncation except terms bucket selection and retry diagnostics bounded as specified.

| Resource | Limit / behavior |
|------|------------|
| API replicas / partitions | One replica; retain current configured partition model; no new multi-tenant routing. |
| HTTP workers / body | 16 active handlers; reject excess with 503 before spawning request threads; 2 MiB decoded body, 10 s socket timeout; oversized body 413, including chunked/unsupported transfer handling. |
| Records | 5,000 distinct IDs including tombstones/hard-delete fences; 64 KiB canonical UTF-8 JSON each; 128 MiB total retained canonical record/version bytes. At most 3 versions per record; evict only unpinned older versions, preserving existing missing-version error envelope; reject a fourth version with 429 if required eviction would remove queued/in-flight state. |
| Kinds / schemas | 32 full kinds including built-ins; 128 KiB per registration; 2 MiB registry bytes; 128 mapped scalar leaf fields per kind; maximum schema/value nesting 16. |
| Catalog / mapping | At most 32 single-shard, zero-replica kind indices; explicit mapping, `dynamic:false`, no field explosion; unmodeled legacy fields remain in `_source` but are not searchable. |
| Admission / indexing | 100 records per storage request or manifest; 1,000 pending/in-flight/retry index items in total; up to 2 index workers; bulk request at most 100 records and 1 MiB. |
| Query / offset | Query string 4 KiB; body uses same 2 MiB gate; result `limit` clamps to 1..1000 as today; `offset` nonnegative, `offset+limit <= 10000`, else 400. |
| Sort / projection / aggregation | 3 user sort fields plus unique `id` tie-breaker; 32 returned fields; 3 aggregate objects, at most 100 buckets per object; no arbitrary caller DSL. |
| Spatial | One point field and one box or distance constraint; distance 1..100000 m; finite lat/lon in geographic range; maximum 32 selected kinds. |
| Cursors | 16 globally, 4 per caller; 60 s absolute TTL (`CURSOR_TTL`, default 60, permitted 1..60 s); plan metadata <=16 KiB per cursor; 1 in-flight continuation per cursor. Expired contexts closed by a 5 s sweep. |
| Workflow / event history | 256 runs, 1,000 redacted delivery status entries; terminal entries evicted oldest-first after 10 min or for capacity; active runs never evicted. Poll of unknown/evicted run retains existing not-found behavior. |
| Notification resources | 64 subscriptions, 64 existing actions, 1,000 delivery items including retries/in-flight; aggregate serialized delivery payload/diagnostic budget 16 MiB; 2 delivery workers; incoming decoded event <=64 KiB. |
| Attempts | Index and delivery: 4 total attempts at delays 0,1,2,4 s, no jitter for this single-instance release; monotonic clock; delivery honors valid Retry-After seconds capped at 30 s. |
| Network / deadline | Engine HTTP timeout 5 s; delivery connect 2 s / total 5 s; read <=4 KiB callback response; manifest deadline 120 s; worker retries stop at item/run deadline. |
| Graceful stop | Reject admission, stop scheduler, finish in-flight work for <=10 s, close PITs, discard queues and runs. Imports MUST create no threads or engine traffic. |
| Pi envelope | API container memory limit 2 GiB and CPU limit 2; OpenSearch memory limit 6 GiB and heap 2 GiB, single shard per kind/zero replicas; no swap dependency. Existing real `pyproj` stays. |

Non-record metadata MUST also remain bounded: lifecycle indexes/locks are attached to bounded registries, version snapshots count against the record budget, all queued index payloads reference retained version entries, and all retained retry messages cap at 512 redacted characters. Existing ancillary mock APIs MUST NOT acquire new background threads or persistence; this change does not claim that every unrelated mock collection is hardened.

## 4. Requirements

**Summary**: Implement exactly the finite profiles below. Unknown advanced options MUST return 400 rather than appear supported while ignored. Existing documented request fields unaffected by this feature retain their behavior.

**Items**:

| ID | Requirement |
|------|------------|
| FR-001 | MUST bundle schemas for the four existing full kinds, validate complete record envelope plus schema-governed `data`, preserve existing legal/ACL enforcement and sample success. |
| FR-002 | MUST register additional full kinds using the offline profile, explicit immutable identity/version and lifecycle. |
| FR-003 | MUST execute ACL/kind filtering, paging, sorting, projection, terms aggregation and selected spatial filters in OpenSearch, with mapping isolation and explicit errors. |
| FR-004 | MUST preserve cursor response/ownership semantics using bounded engine snapshots, not Python result snapshots. |
| FR-005 | MUST execute only `Osdu_ingest`, with complete preflight and truthful per-record indexing outcomes, deadline and terminal status. |
| FR-006 | MUST replace per-write threads with capacity-reserved, ordered, retried, volatile indexing work and make deletes honor the same fences. |
| FR-007 | MUST deliver authenticated, allowlisted, opt-in notifications and record actual per-delivery outcomes without claiming exactly-once delivery. |
| NFR-001 | MUST enforce the runtime hard limits, no unbounded result/queue/worker/context growth, and reject overload without partial admission. |
| NFR-002 | MUST drop state on restart, remove/fence owned engine state, avoid network/worker import side effects and support deterministic shutdown. |
| NFR-003 | MUST retain baseline routes/envelopes, examples, entitlement checks and real projection library behavior except documented narrow compatibility changes. |
| NFR-004 | MUST protect callback transport against SSRF, DNS rebinding, redirects, credential leakage and payload-based data exfiltration. |
| NFR-005 | MUST run dependency-isolated unit tests, real OpenSearch 3.9.0 tests, x86/arm64 builds and a bounded Pi soak profile. |

### Schema identity, lifecycle and offline validation profile

- Built-in kinds MUST be exactly `osdu:wks:dataset--File.Generic:1.0.0`, `osdu:wks:master-data--Well:1.4.0`, `osdu:wks:master-data--Wellbore:1.4.0`, and `osdu:wks:work-product-component--WellLog:1.4.0`.
- Author local subset schemas from the public OSDU data-definition field contracts and existing sample fixtures, not a bulk upstream import. `catalog.json` MUST record each schema's full kind, local file, SHA-256, public source URL, subset limitations and license/provenance note. Public source root is `https://community.opengroup.org/osdu/data-definitions`; upstream retrieval was unavailable during planning, so upstream revision/content/license MUST NOT be claimed verified. The implementation MUST verify public field provenance before claiming a public-derived subset; if unavailable it MUST label the schemas locally authored compatibility subsets rather than fabricate attribution. Schema behavior remains fully defined here regardless of upstream availability.
- The root `schema` validates `record.data`. Envelope `id` is a nonempty string <=512 characters; `kind` is a nonempty registered full-kind string <=256 characters; `data` MUST be an object. `acl` requires `owners` and `viewers`, arrays of <=64 nonempty strings <=256 characters; owners MUST be nonempty. `legal` requires the existing valid registered LegalTags and existing otherRelevantDataCountries validation; do not broaden legal eligibility or remove existing required fields. Reject malformed envelope types before existing legal checks. Compatibility schemas MUST require no additional `data` property absent from any existing bundled sample of that kind; known sample fields receive correct types. Unknown legacy data properties remain allowed/stored but unmapped. Validate all repository samples as a release fixture.
- Support JSON Schema Draft 2020-12 with the following finite vocabulary only: `$schema`, `$id`, `$defs`, `$ref`, `title`, `description`, `type`, `properties`, `required`, `additionalProperties`, `items`, `enum`, `const`, numeric minimum/maximum, string minLength/maxLength, array minItems/maxItems, and `format:date-time`. Types: object, array, string, integer, number, boolean, null; no type unions. Reject unsupported validation keywords and `pattern`, combinators, recursion and unbounded schema depth before validator invocation. `additionalProperties` is boolean only. Object arrays can be stored/validated but MUST NOT be mapped/queryable; scalar arrays use their scalar mapping.
- `$ref` MUST resolve only in-document fragments or `urn:osdu-lite:schema:<full-kind>#/...` against bundled/registered resources. Registration may include `references` (maximum 8 local documents within its byte cap). Every dependency MUST already exist or be in that request. Reject HTTP/file references, missing refs, cycles, duplicate `$id`, malformed JSON Pointer and excessive expansion. Use a closed `referencing.Registry`; never an automatic network retriever.
- POST/PUT `/api/schema-service/v1/schema` accept `{kind,schema,schemaInfo?,references?,fieldMappings?}`. Full `kind` is authoritative. When `schemaInfo.schemaIdentity` is supplied, all authority/source/entityType/major/minor/patch components MUST agree; normalize omitted patch to 0 only for legacy identity input with explicit full kind. Derive full kind from complete identity when `kind` is absent. Entity-type-only identity is 400, not a new registry key. Return compatible schema payload plus normalized full identity including patch.
- Lifecycle supports `PUBLISHED` and `DEPRECATED`, default `PUBLISHED`; other statuses 400. Accept status in top-level `status` and/or `schemaInfo.status`; if both are supplied they MUST agree, otherwise 400. Responses always include both normalized status fields, `kind`, `schema`, normalized `schemaInfo.schemaIdentity` and existing created metadata. The normalized identity contains `authority`, `source`, `entityType`, `schemaVersionMajor`, `schemaVersionMinor`, `schemaVersionPatch` integers. A published full kind's schema, references and mappings are immutable. Identical POST, including a built-in schema, is idempotent 200; new POST 201. Identical PUT is 200. PUT may transition PUBLISHED→DEPRECATED (200), not reverse or rewrite content (409); repeated deprecation is 200. Deprecated kinds remain readable/searchable but reject new writes with 409. Built-ins cannot be changed/deprecated (409). No delete endpoint. Custom registrations disappear on restart; built-ins reload.
- Default mappings: string=`keyword` with a `.text` text subfield; integer=`long`, number=`double`, boolean=`boolean`, date-time=`date`. Metadata `id`,`kind`, ACL principals and version are explicit. `fieldMappings` may declare at most one known schema leaf/object point path as `geo_point`, whose validated record value MUST be `{lat:number,lon:number}`; no arbitrary engine mapping input. This also supplies a supported spatial field for custom kinds; compatibility schemas MAY declare a point only when sample shape matches. Fixed server-side paths MUST NOT guess coordinate conversion.

### Search contract

| Surface | Required behavior |
|------|------------|
| Routes and envelope | Existing `/api/search/v2/query` returns `results`, optional `totalCount` and optional `aggregates`; `/query_with_cursor` returns `results,totalCount,cursor`. Existing aggregate output shape and returned-field projection semantics MUST be preserved by adapters and frozen baseline tests. |
| Filter and query | Keep existing `kind`, `query`, `limit`, `offset`, `returnedFields`, `trackTotalCount`, `aggregateBy`. Match kind patterns against registry, bounded by 32 kinds; empty selection returns an empty result without arbitrary index wildcard access. Query remains bounded Lucene query-string; absent/blank/`"*"` is match-all. Disallow regex, leading wildcards other than match-all, scripts and arbitrary DSL; explicit field queries require mapped compatible fields. Use finite parser/field checks, engine clause limit <=256 and timeout 2 s. |
| ACL | Resolve groups by existing token/group rules; apply owner/viewer plus partition/kind restrictions in the engine's top-level query before hits, totals and aggregation. Schema registration and worker execution MUST NOT bypass entitlement routes. |
| Mapping compatibility | Index name is a server-owned prefix plus partition hash, process epoch and full-kind hash; never raw client input. Query only resolved indices. Across kinds, sort/aggregation/field-query/spatial paths MUST have identical declared engine type and capability; absence or conflict returns 400 with path and kinds, not a partial shard result. Unqualified free text searches only declared text fields using an engine query safe across selected indices. Reject partial shard failures. |
| Sorting | New `sort:{field:[path,...],order:["ASC"|"DESC",...]}`; equal lengths 1..3, mapped scalar keyword/numeric/date fields only; default `id ASC`. Append unique `id ASC` unless present. IDs MUST be unique across canonical records, preserving existing ID-keyed store. Nested/object arrays cannot sort. Null/unset sort values last. |
| Projection | Existing `returnedFields` <=32; absent or `[]` returns full record. Explicit compatibility correction: `["*"]` now returns full record, whereas baseline `project_record` returned `{}`. Mixing `"*"` with paths is 400. Otherwise send engine `_source` includes, omit missing/null paths and reconstruct nested objects with no mandatory id/kind injection as baseline `server/server.py:471-479`. A parent path subsumes child paths irrespective of input order. Internal engine fields, including `_deleted` and fencing metadata, MUST never appear in any API record/projection. No unrelated field leaks. |
| Aggregation | Preserve `aggregateBy` as an array of <=3 objects and all three existing operations: `count`, `filteredTerms`, `histogram`. `server/server.py:567-608` at baseline is the normative request-key/default and response-envelope contract (`Count`, `FilteredTerms`, `Histogram`); ITEM-012 MUST freeze it in explicit fixture tests before replacement. Retain `bucketSize`/`interval` inputs, `termCount`/`totalCount` fields and baseline bucket key order. Deliberate compatibility restriction: aggregation fields MUST be non-array scalars throughout the selected schema; array/object fields return 400, avoiding OpenSearch duplicate-value deduplication silently changing baseline occurrence counts. Missing scalar values retain baseline omission rules. Clamp no inputs silently: bucketSize outside 1..100 is 400; histogram interval MUST be finite positive, and authorized min/max spanning >100 buckets returns 400 `aggregation_bucket_limit`. Compute over all authorized matches before paging in engine; no full hits in Python. `filteredTerms` returns its requested bounded top buckets; totalCount and termCount MUST remain exact rather than inferred from truncated engine buckets. Use bounded engine subaggregations/value-count and, if necessary, bounded paged composite bucket iteration (<=1,000 scanned buckets/2 s total, then 400 `aggregation_work_limit`) to reproduce scalar count semantics and key order. No scripts, analyzed-text or object-array aggregation. Unsupported type/operation is 400. |
| Spatial | New `spatialFilter:{field,byBoundingBox:{topLeft:{latitude,longitude},bottomRight:{latitude,longitude}}}` OR `{field,byDistance:{point:{latitude,longitude},distance:<meters>}}`. Exactly one mode; box north>=south, west<=east; reject dateline crossing in this release. Compile to `geo_bounding_box`/`geo_distance`; no Python geometric post-filtering. |
| Engine failure | Timeouts/unavailable=503 with retryable error code; malformed query=400; no successful empty fallback on failures or partial shards. Track total accurately when requested; `trackTotalCount:false` omits totals on ordinary queries. |

### Cursor contract

- Initial call preserves existing search arguments except offset MUST be 0; create one PIT over resolved indices, use stable engine `search_after` values and ACL-filtered query. `totalCount` is the snapshot count.
- Return an opaque random token with the existing `demo-` prefix; retain same token across pages, including final empty pages. The token is not an engine PIT identifier. The initial result counts as the first page.
- Bind token to a SHA-256 fingerprint of existing authorization identity, effective groups, partition, and process epoch. Never store raw bearer strings. Changed caller/groups/partition=403; unknown/expired/restart-invalid token=400.
- Continuation accepts `cursor` and optional unchanged `limit`; ignores no changed query: conflicting additional search parameters or changed limit return 400. Cursor state contains normalized query, last sort values, count, TTL, terminal flag and PIT ID only.
- Expiry is absolute, not extended by requests. On final hit page close PIT, keep a terminal cursor stub until expiry and return repeated empty `results` with unchanged token/count. Explicit engine PIT loss produces 400 expired-cursor, not silent restart.
- Context quota is reserved before engine creation; backend creation failure releases capacity. Serialize concurrent continuations; competing continuation returns 409. Release contexts on end, expiry, failure, shutdown and canceled initial creation.

### Admission, indexing and manifest workflow

- Direct storage retains **POST** `/api/storage/v2/records` response `{recordCount,recordIds:[{id,version}],skippedRecordIds:[]}`. Prevalidate entire batch; duplicate IDs in one batch=400. An existing ID, including soft-deleted or hard-deleted IDs retained as fences, MUST retain its full kind; changing kind returns 409 before mutation. This deliberate restriction prevents duplicate/stale documents across per-kind indices. Validation failure=400, schema deprecation=409, body limit=413, state/job capacity=429 with `Retry-After:1`. MUST NOT mutate any record, version, run or job on rejection.
- Storage acceptance means memory committed and indexing pending, not immediately searchable. Existing indexing-delay configuration MAY remain a bounded nonnegative initial scheduling delay <=5 s for compatibility, but MUST NOT drive completion status.
- Reserve job and version-byte capacity under the admission lock; assign versions and generation; enqueue immutable references. Pin referenced versions until work settles; retained pin bytes count against state budget. A superseded operation MUST NOT index stale data after newer writes/deletes, including uncertain old timeouts.
- All existing soft-delete, hard-delete and purge routes MUST reserve index capacity first. Then canonical memory becomes soft-deleted (retaining existing deleted-list behavior) or hard-deleted (payload/versions removed after pins settle, ID/kind/generation fence retained). Wait <=5 s for engine deletion-marker ACK: normal existing success response only after ACK; otherwise return 503 with `deletion_pending`, retain canonical deletion and continue bounded retry. Storage reads follow canonical deletion immediately; an engine failure does not revive memory. Until marker ACK, fresh searches additionally exclude the bounded canonical deletion-ID set using one terms filter so search cannot leak pending-deletion records; cursors created before deletion retain their documented snapshot behavior. Re-upsert of a soft-deleted ID revives it with greater version; hard-delete recreation also advances the retained generation and cannot suffer ABA.
- Retry connection failures, timeout, 429 and 5xx. A schema/mapping 400 and other deterministic 4xx fail immediately. Inspect individual bulk results: retry only failed retryable items; never classify a whole successful HTTP bulk as success. Store final redacted index error and record version; no unbounded audit log.
- Existing workflow registration endpoints remain inspectable; attempts to execute workflows other than `Osdu_ingest` return 501 with explicit unsupported-workflow error; no fake completion.
- Accept `executionContext.manifest` arrays `MasterData`, `ReferenceData`, `Data.Datasets`, `Data.WorkProductComponents`, inheriting `executionContext.acl` and `legal` only when absent on record. Reject unknown nonempty manifest sections, non-array sections, invalid execution context, empty manifest and >100 records with 400. No dependency/download processing.
- Before creating a run, flatten once and validate every item plus resource reservations. Invalid manifests return one bounded list of <=20 errors with section/index/path; no run/record/job mutation. Successful admission retains current HTTP 200 response including `status:submitted` and its run identifier fields.
- Internal state machine: `submitted`→`running`→`succeeded|failed`; index worker acknowledgement drives outcomes. `running` begins at first item attempt. A run succeeds only when all its admitted versions are engine-acknowledged. Mapping failure, exhausted retry, superseded version, or deadline fails that item and run; other admitted writes are not rolled back. Timeout/deadline is an uncertain outcome: an already-sent engine request may apply later, but MUST NOT change terminal run failure or overwrite a higher generation. Do not claim transactional cancellation.
- Expose existing workflow polling envelopes plus additive `recordCount`, `indexedCount`, `failedCount`, `pendingCount`, timestamps and ordered per-record `{id,version,status,errorCode?}`. Nonterminal/terminal counters MUST sum to `recordCount`. Wait for every item's terminal outcome, or at deadline mark unresolved items failed and cancel remaining unsent writes for that run. Terminal status cannot change. Polling MUST NOT itself cause completion.

### Notifications and callback security contract

- Default `CALLBACKS_ENABLED=false`. Environment `CALLBACK_DESTINATIONS_JSON` defaults to `[]`; enabled delivery requires a parseable list of `{url,addresses}` entries, <=16 entries, each <=8 literal IPv4/IPv6 addresses, HTTPS port 443 and exact normalized path, no query/fragment/userinfo. No wildcards or redirect policies. Misconfiguration fails startup; secret values MUST NOT be supplied in this environment variable.
- Preserve `POST /api/register/v1/subscription`, subscription secret PUT, event ingress `/api/notification/v1/`, and action test/retrieve matching. Delivery is subscription-only. GSA secret objects may still be stored for compatibility but a subscription using GSA MUST be explicitly `deliveryStatus:unsupported_auth`, never silently delivered without auth; enable requests for that subscription return 400. Actions remain metadata/filter operations and MUST NOT launch their URLs.
- Preserve `name`, topic, `pushEndpoint`, HMAC secret shape and existing response secret redaction. Add `deliveryEnabled` default false. Add `PUT /api/register/v1/subscription/{id}/delivery` accepting exactly `{deliveryEnabled:boolean}`; return 200 redacted subscription, unknown ID=404, owner mismatch=403, missing HMAC/unapproved endpoint/global disabled=400. Creation may enable only if the same validation succeeds. This supports create→secret PUT→enable. Enabling requires global enabled, exact destination match and nonempty HMAC secret >=32 UTF-8 bytes. Secret rotation affects later attempts; delete/disable cancels queued deliveries before connecting. Callback errors never echo secret bodies/headers.
- Incoming event keeps `{subscription,message:{data,attributes,messageId,publishTime}}` and existing base64 JSON/partition validation. Enqueue only matching subscriptions with `deliveryEnabled:true`; capacity reservation for the entire fan-out is atomic. No matching enabled subscription returns successful accepted response with queued count 0, not `delivered:true`.
- Successful storage indexing MUST create exactly one version event per enabled subscription matching the existing records-changed topic; MUST use the same delivery path. Payload is a canonical JSON object `{eventId,partition,recordId,kind,version,operation}` with no record data or ACL/legal contents. `operation` is exactly `create` or `update`; deletion callbacks are out of scope because absent/tombstoned records cannot pass current-record disclosure checks.
- Incoming decoded `message.data` MUST be one object `{recordId,kind,version,operation}`, no arrays/extra fields; recordId/kind obey envelope string limits, version is a positive integer and operation is create/update. Invalid shape=400 before enqueue; partition still comes from verified attributes/configuration, not body. Missing/unreadable/deleted records, kind mismatch or noncurrent version=403 generic `invalid_record_reference`, with no existence details. Normalize to the minimal output shape; never forward arbitrary decoded JSON.
- At registration bind `ownerPrincipalId=SHA256` of the authenticated configured bearer credential, never retain that bearer in the subscription. Existing token-to-groups configuration is the identity/group authority; resolve current groups from its live mapping by hashed configured credential, not an external directory. Unknown/revoked credential fails closed. Preserve current static-token auth; no OIDC redesign. Before **every** connection attempt (including retries), recheck current credential/group eligibility and ACL read permission against the current canonical record and exact version; revocation, deletion or replacement cancels delivery terminally. Status/update/secret/delete access requires both existing service role and matching ownerPrincipalId; service-role membership alone is not cross-owner access.
- Delivery is opt-in best effort with bounded at-least-once attempts while the process lives; accepted storage remains committed if subsequent notification capacity is exhausted. Record `notification_dropped_capacity` in bounded counters and workflow additive warning; do not retry an already indexed record merely to regenerate delivery. Incoming explicit event publish returns 429 without enqueue on full fan-out.
- Generate one stable UUID eventId per event/subscription delivery item; reuse body and eventId on retries. POST UTF-8 canonical JSON (`sort_keys`, compact separators), `Content-Type:application/json`; `X-OSDU-Lite-Event-Id`, `X-OSDU-Lite-Timestamp` (current UTC epoch seconds) and `X-OSDU-Lite-Signature:sha256=<hex>`. HMAC-SHA256 input is ASCII timestamp + `.` + exact body bytes; secret is the subscription HMAC value. Document receiver timestamp window 300 s and eventId deduplication.
- Resolve destination each attempt; reject loopback, link-local, multicast, unspecified, metadata addresses, IPv4-mapped equivalents and any DNS result outside configured IP addresses. Private unicast destinations are allowed only as explicitly configured addresses, supporting a trusted Pi LAN receiver. Pin TCP connection to the validated address while retaining original hostname for TLS SNI/certificate verification. Never disable TLS verification or use proxy environment variables. No automatic redirects, even to another allowlisted URL. No URL constructed from payload fields.
- HTTPS 2xx=delivered; 408/429/5xx/network timeout retry; other 4xx or any redirect fail terminally; DNS/policy/TLS failures terminal. Do not retain response bodies. Callback duplicate delivery is possible following uncertain timeout.
- Existing event records keep `delivered` truthful: true only after all targeted active delivery items succeed; pending/partial/failed remain false with additive counts/statuses. Provide authorized `GET /api/notification/v1/deliveries/{eventId}` with only IDs, status, attempts, timestamps, redacted error code; unknown/evicted=404. No secret-returning API is introduced.

## 5. Risk Classification

**Risk**: 🔴 HIGH RISK

**Summary**: Changes cross validation, authorization-sensitive engine queries, concurrent mutation and outbound networking. The deployment remains intentionally small; finite profiles and real-engine/transport tests are release gates.

**Items**:

| ID | Risk or assumption | Mitigation |
|------|------------|------------|
| RISK-001 | Existing full-scan tests may mask ACL/aggregation/mapping defects. | Freeze envelopes and require real OpenSearch assertions, including denied records in counts/buckets. |
| RISK-002 | SSRF via DNS rebinding/IPv6/redirects or secret leakage. | Pinned transport, exact destination policy, no redirects/proxies, adversarial tests. |
| RISK-003 | In-flight older index operation can defeat generation-only guards. | Serialize same-record operations through backend acknowledgement, then apply generation fence. |
| RISK-004 | Validation changes reject previously accepted bad data/schema registrations. | Compatibility schemas pass every current sample; document deliberate rejection boundaries. |
| RISK-005 | PITs/shards and per-kind fields consume Pi memory. | 32 indices, 16 PITs, TTL, heap/container limits; soak gate before release. |
| RISK-006 | Engine may retain records after API-only restart. | Exclusive namespaced startup cleanup, new epoch, fail readiness on cleanup failure; never search old epochs. |
| RISK-007 | External callbacks time out after receiver processes them. | Stable event IDs and explicit best-effort at-least-once wording; receiver dedup guide. |
| ASSUMPTION-001 | Existing deployment owns one partition-specific index namespace exclusively. | Enforce single replica operational contract and delete only that exact owned prefix. |
| ASSUMPTION-002 | Public upstream revision/license could not be verified during planning. | Do not claim official/upstream validation; provenance labels and subset limitations are mandatory. |
| ASSUMPTION-003 | Pi hardware is not available in this planning session. | Hardware soak is a reproducible release gate, not a claimed measured result. |

### Failure modes

| ID | Trigger | Required observable outcome |
|------|------------|------------|
| FM-001 | Bad schema, record, manifest or incompatible field | Deterministic 400/409; bounded JSON-pointer diagnostics; zero admission mutation. |
| FM-002 | Worker/record/run/delivery/context capacity exhausted | 429 with Retry-After; no partial fan-out/admission; HTTP concurrency overload alone uses 503. |
| FM-003 | Engine unavailable or partial bulk failure | Search 503; retry eligible index items; final run/index failure, never timer success. |
| FM-004 | Stale write races delete/update | Serialized acknowledgement plus generation fence prevents resurrection. |
| FM-005 | Callback policy/auth/TLS failure | No unsafe connection; terminal redacted delivery status; no secret output. |
| FM-006 | Restart or shutdown | Intentional job/run/cursor loss; no replay; old engine epoch invisible and cleaned. |
| FM-007 | PIT expiry or changed principal | 400 expired/unknown or 403 identity mismatch; no cursor recreation/leak. |
| FM-008 | Deadline reached | Terminal failed run with all counters reconciled; canceled/fenced remaining run jobs. |

## 6. Dependencies

**Summary**: Retain the standard-library HTTP/OpenSearch approach and existing real coordinate library. Add only offline schema-validation libraries.

**Items**:

- **DEP-001**: OpenSearch 3.9.0 already pinned in compose/CI; verify PIT and typed mappings with the actual container. No new Python OpenSearch client required.
- **DEP-002**: Add exact hash-pinned `jsonschema` and `referencing` plus their transitive dependencies to `server/requirements.txt`; choose latest mutually compatible releases with Python 3.12 x86_64/aarch64 binary wheels at implementation time and commit exact resolved hashes. No unconstrained runtime dependency ranges.
- **DEP-003**: Existing `pyproj==3.7.2` and hash-checked installation MUST stay real and pass reference-service tests.
- **DEP-004**: Python unittest, injected clocks/transports, and existing FakeSearch are primary test tools; no queue/database framework.
- **DEP-005**: An administrator-provisioned HTTPS/HMAC receiver and verified destination addresses are necessary only for opt-in deployment callback verification; unit tests use injected transport, not public internet.

## 7. Quality & Testing

**Summary**: Test-first by epic. Deterministic fake-clock unit tests cannot substitute for real engine semantics. Existing tests and examples remain regression fixtures.

**Items**:

- **TEST-001**: Registry profile, identity/lifecycle, cycles, remote refs, depth/bytes/field limits, every sample; import with denied network and assert no threads.
- **TEST-002**: Admission atomicity, capacity boundaries, version-byte accounting, same-record race, partial bulk results, retry clock and shutdown.
- **TEST-003**: Real OpenSearch typed-kind isolation, engine totals/buckets/ACL filtering, sort ties, projection, geo point box/distance, PIT snapshots/cleanup and partial shard errors.
- **TEST-004**: Manifest fixtures with inherited ACL/legal, bad middle item, unsupported sections/workflows, engine retry/exhaustion, supersession, deadline and counter invariants.
- **TEST-005**: Callback DNS/pinning/IPv4/IPv6/redirect/proxy/TLS policy, exact HMAC bytes, secret rotation, queue fan-out, retries, disable/delete and redacted status.
- **TEST-006**: Baseline envelopes/error status snapshots, all existing tests including real pyproj; x86/arm64 hash-pinned container builds.
- **TEST-007**: Real-Pi or equivalent capped deployment soak, restart and outage profile; record actual results in a test report, not this PRD.

### Acceptance Criteria

| ID | Criterion | Verification | Traces To |
|----|-----------|--------------|-----------|
| AC-001 | All four existing kinds and every repository record/manifest fixture ingest using bundled nonempty schemas; malformed known field and missing ACL/legal fail without mutation. | Registry/storage tests over fixture inventory. | FR-001, NFR-003, FM-001 |
| AC-002 | Register fifth kind with numeric/string/date/point fields and local ref; store/search it. Same-kind schema mutation, identity mismatch, remote/cyclic reference, 33rd kind and unsupported keywords fail with specified status. Deprecation blocks new writes only. | Unit + real engine test. | FR-002, FM-001 |
| AC-003 | Two kinds with `data.Value` integer versus string index successfully into separate indices; mixed-type sort/aggregation/field query returns 400; valid single-kind query succeeds. | Real OpenSearch mapping isolation test. | FR-003 |
| AC-004 | Among 1,200 records with denied ACL matches, queries return correct sorted/projection pages and authorized totals. Baseline scalar Count/FilteredTerms/Histogram envelopes, interval/bucketSize and key ordering match frozen fixtures; array aggregation explicitly returns 400, >100 histogram buckets or >1,000 scanned buckets reject explicitly. Absent/empty/wildcard/nested/null/overlap projection cases obey the specified compatibility correction, without internal engine fields. Backend hit request size <=1000, no scroll or Python collection of all hits; offset beyond 10,000 rejected. | Real engine fixtures + recording transport asserting DSL and response-bound processing. | FR-003, NFR-001, NFR-003 |
| AC-005 | Box and distance filters return predetermined point fixtures; invalid range/dateline/non-geo/type-conflicting filters return 400. | Real engine tests, not FakeSearch geometry. | FR-003 |
| AC-006 | Cursor spans three pages with equal sort values without duplicates; update after first page does not alter snapshot. Token stays constant; final calls empty. Changed groups=403, expiry=400; 17th context rejected, all PITs closed. | Fake clock quota tests + real PIT test. | FR-004, NFR-001, FM-007 |
| AC-007 | Batch with invalid last item or insufficient reserved bytes/jobs changes zero records/versions/jobs. The 1,001st pending index item rejects 429. At most two index operations execute concurrently and same ID never overlaps. | Barrier/fake-clock unit tests. | FR-006, NFR-001, FM-002, FM-004 |
| AC-008 | Bulk success/429/mapping failure retries only 429 at 1/2/4 s, four attempts maximum. An old request applied after timeout and a newer ACK cannot overwrite external generation. Soft/hard delete, outage, recreate and delayed responses never resurrect old data; uncertain delete returns 503 and canonical deletion remains. Cross-kind ID reuse returns 409, including deleted IDs. Fourth-version admission with oldest pinned returns 429. | Recording backend, concurrency tests + real engine external-version/tombstone tests. | FR-006, FM-003, FM-004, NFR-001 |
| AC-009 | Real supported manifest remains nonterminal while indexing blocked; all ACKs cause success; one deterministic failure/exhaustion/deadline/supersession causes terminal failure with reconciled per-record counts. Polling/time passage alone cannot cause success. | Unit state-machine + real engine manifest test. | FR-005, FM-003, FM-008 |
| AC-010 | Unsupported workflow returns 501; malformed/empty/oversized manifest returns bounded 400 with zero mutation; existing sample workflow HTTP 200 envelope preserved. | Handler contract tests. | FR-005, NFR-003 |
| AC-011 | Default callbacks produce zero outbound requests. Enabled matching HMAC subscription receives exact signed canonical record-reference bytes; receiver returns 2xx before status becomes delivered. GSA never becomes unauthenticated delivery. | Injected receiver + opt-in HTTPS smoke test. | FR-007, NFR-004 |
| AC-012 | Malicious URLs, userinfo, ports, redirect, DNS rebinding, metadata, mapped IPv6, unlisted private IP, TLS failure and proxy environment cannot redirect outbound connection; logs/status contain no secret/token/body. | Adversarial transport tests capturing connect address, SNI and logs. | NFR-004, FM-005 |
| AC-013 | Callback 429/500/timeout retries within bounds using identical eventId/body; 400/302 terminal. Create→secret→enable→deliver→disable works; disable/delete, ACL/group revocation or record replacement between enqueue/retry cancels next attempt. Nonowner status/update fails 403. Full fan-out returns 429 with no partial enqueue. Malformed/array ingress=400; unauthorized reference=generic 403 with no disclosure. | Fake-clock and authorization tests. | FR-007, NFR-001, NFR-004 |
| AC-014 | API restart while OpenSearch survives returns no previous records, runs, custom schemas, subscriptions or cursors; only owned namespace deleted. Import performs no backend requests/threads; shutdown exits <=10 s. | Subprocess + real-engine restart test and fake import test. | NFR-002, FM-006 |
| AC-015 | All existing unittests pass after intentional compatibility assertions updated with reasons; real OpenSearch suite passes, reference pyproj tests pass, Linux amd64/arm64 images build with binary/hash constraints. | `python -m unittest discover -s server/tests -v`; same command with `OPENSEARCH_TEST_URL` configured; CI build matrix. | NFR-003, NFR-005 |
| AC-016 | On two Pi5 16GB nodes, 30 min run with 5,000 records, 16 cursor contexts, 2 concurrent query clients and 2 ingestion clients at aggregate 10 records/s (repeated updates), plus 60 s engine/callback outage, remains below API 2 GiB/OpenSearch 6 GiB limits; no OOM or unbounded threads; all gauges within caps; overload explicit and recovery works. Warm query p95 <=2 s for fixed 1,000-hit test query while engine healthy. | Reproducible bounded soak script and measured release report; fail release on exceeded cap/latency. | NFR-001, NFR-005 |

## 8. Security Considerations

- **Data handling**: Keep existing ACL/legal gates for reads/writes. Worker service context cannot broaden original authorized scope. External events are treated as record references, never blindly relayed. No raw bearer, HMAC value, callback body, response body or engine document in diagnostics.
- **Input validation**: Limit body before JSON parsing, schema before registry insertion, and complete batch before admission. Reject nonfinite numeric JSON, dangerous refs and unsupported search operations. Bound all error arrays and diagnostic strings.
- **Access control**: Preserve service role groups and partition checks. Bind subscription owner/principal at registration using existing authenticated identity, reevaluate effective groups at dispatch. New delivery status endpoint requires existing notification/register service access and subscription ownership-equivalent authorization; never a public status oracle.
- **Secrets**: Existing secret registration accepts HMAC; values remain memory-only and response-redacted. New responses redact stored GSA material too. No secret in URL, config example, log, exception, metrics label or PRD fixture.
- **Outbound trust**: Exact admin allowlist, current DNS checks, address-pinned TLS/SNI, no redirects, no proxy inheritance. Private LAN IPs require explicit enrollment, not broad RFC1918 permissions.
- **Engine isolation**: OpenSearch remains internal; callers cannot select raw index names, PIT IDs, mappings or DSL. Cleanup only the exact owned partition prefix. Do not weaken existing bearer checks or introduce an auth rewrite.

## 9. Deployment & Rollback

1. Run unit/real-engine tests and both architecture builds. Back up no application state: there is deliberately nothing durable to migrate.
2. Deploy one API replica only. Configure memory/CPU/heap caps in both compose variants. Leave callbacks disabled until destination policy and receiver HMAC verification are tested.
3. Explicit startup loads bounded bundled schemas, initializes backend, deletes only owned partition indices including previous epoch and legacy partition index, creates new epoch, and starts workers. Cleanup/index creation failure means not ready; readiness MUST NOT serve search/storage against stale state. Imports/tests do none of this implicitly.
4. Document startup-loss behavior prominently: API-only restart invalidates engine records as well as jobs/subscriptions/custom schemas; clients must recreate registration/legal/sample state and reingest. Existing unrelated engine namespaces survive.
5. Add authenticated diagnostic summary under the existing health/service access model or a new protected `/api/osdu-lite/v1/runtime`: counts/bytes, queue capacities, worker active counts, retry/exhausted totals, run states, PIT counts, redacted last error codes. Never expose record data, raw URLs/secrets or identities as metric labels. Liveness is process health; readiness depends on initialized runtime and current engine connectivity.
6. Graceful shutdown closes admission, joins <=10 s, closes contexts and drops outstanding work. Abrupt termination has the same documented loss semantics.
7. Rollback: stop API, delete only this feature's owned partition namespace, deploy prior image, reinitialize samples and reingest. Prior code does not understand per-kind epoch indices; do not reuse them or claim state-preserving rollback. No database migration or durable rollback path exists.

## 10. Resolved Decisions

| ID | Decision | Rationale |
|----|----------|-----------|
| RD-001 | Volatile in-process records, registries, jobs and subscriptions; clear/fence owned engine state on API restart. | Matches explicit no-durability scope, including the case where engine survives API. |
| RD-002 | Offline finite schema profile plus four locally bundled compatibility subsets, not complete upstream schema import. | Preserves useful samples without unlimited schema ecosystem expansion or remote retrieval. |
| RD-003 | Full-kind immutable identity and per-kind explicit mappings. | Prevents type collisions across kinds/versions; deprecation avoids in-place migrations. |
| RD-004 | One shared bounded scheduler with separate index/delivery capacity and workers. | Retries must not starve ingestion or create a thread per job; no broker needed. |
| RD-005 | Engine PIT/search-after with opaque stable API tokens. | Keeps snapshot/cursor ownership behavior without Python result snapshots. |
| RD-006 | Whole-batch prevalidation/admission but no rollback of asynchronously failed admitted writes. | Gives deterministic input failure semantics without pretending to offer distributed transactions. |
| RD-007 | HMAC-only delivery, GSA explicitly unsupported, exact allowlisted HTTPS transport. | Actual authenticated delivery without cloud credentials, token minting or SSRF surface. |
| RD-008 | One executable manifest workflow and explicit 501 for others. | Complete useful scope rather than timer-based fake orchestration. |
| RD-009 | Existing response envelopes frozen, unsupported new capabilities rejected explicitly. | Improves truthfulness without breaking supported demo clients gratuitously. |

## 11. Alternatives Considered

| Alternative | Pros | Cons | Decision |
|-------------|------|------|----------|
| SQLite/outbox/broker/workflow engine | Restart recovery | Explicitly forbidden; more operations and memory | Rejected. |
| Keep scrolling then cap Python results | Small adapter change | Incorrect totals/pages, unnecessary transfer and memory | Rejected. |
| One shared dynamic-mapped index | Fewer shards | Cross-kind type conflicts | Rejected; bounded 32 per-kind indices. |
| Python materialized cursor pages | Simple snapshot behavior | Memory proportional to matching records | Rejected; bounded PIT metadata. |
| Full official schema catalog and arbitrary refs | Broad advertised compatibility | Dependency graph, licensing/provenance and resource scope unbounded | Rejected; documented subsets/profile. |
| Generic plugin/task framework | Future flexibility | Additional abstraction unrelated to four goals | Rejected; concrete modules and typed work items. |
| Unrestricted callback URLs / redirects | Easy integration | SSRF and credential leakage | Rejected; exact administrator destination policy. |
| Fire-and-forget delivery | Few code changes | Cannot report actual success or retry | Rejected; finite observable volatile retries. |

## 12. Files

Paths are relative to repository root. New files are labeled; existing paths are verified except where marked a proposed file.

- **FILE-001**: `server/server.py` — route adapters, registry/admission integration, workflow state, startup/shutdown, request limits and diagnostics.
- **FILE-002**: `server/search_backend.py` — per-kind indices, mappings, bounded query/aggregation/spatial/PIT and bulk outcomes.
- **FILE-003**: `server/community_services.py` — subscriptions/secret redaction, event fan-out and delivery status.
- **FILE-004**: `server/schema_registry.py` (new) — finite offline validation and mapping capabilities.
- **FILE-005**: `server/runtime_jobs.py` (new) — bounded reservation/scheduler/retries and lifecycle.
- **FILE-006**: `server/callback_delivery.py` (new) — destination-pinned HTTPS/HMAC transport.
- **FILE-007**: `server/schemas/catalog.json`, `common.json`, `file-generic-1.0.0.json`, `well-1.4.0.json`, `wellbore-1.4.0.json`, `welllog-1.4.0.json` (six new files) — schema assets/provenance.
- **FILE-008**: Existing `server/tests/test_server.py`, `test_search_backend.py`, `test_indexing_backend.py`, `test_community_services.py`, `test_opensearch_integration.py`, `fake_search_backend.py` — compatibility and adapter evolution.
- **FILE-009**: `server/tests/test_schema_registry.py`, `test_runtime_jobs.py`, `test_callback_delivery.py`, `test_manifest_runtime.py`, `test_runtime_limits.py` (new) — focused deterministic coverage.
- **FILE-010**: `server/requirements.txt` — exact hashes for schema validator dependencies; `server/Dockerfile` only if wheel/build installation requires a targeted adjustment.
- **FILE-011**: `server/compose.yaml`, `server/compose.pull.yaml`, `.github/workflows/ci.yml` — resource limits, integration and architecture validation.
- **FILE-012**: `README.md`, `installRPI.md` — supported subset, new limits, callbacks, volatile lifecycle and deployment.
- **FILE-013**: `server/tests/soak_runtime.py` (new) — bounded opt-in soak, explicit target allowlist/test authorization, no production-default execution.
- **FILE-014**: This PRD — planning artifact only; no package version/CHANGELOG update.

## 13. Simplicity Rationale

- **Scope justification**: EPIC-001/002→FR-001/002; EPIC-003/004→FR-005/006,NFR-001/002; EPIC-005/006→FR-003/004; EPIC-007/008→FR-007,NFR-004; EPIC-009/010→NFR-001/003/005. Schema assets and registry are split solely to keep each epic at seven files or fewer. No structural-only epic authorizes broad monolith extraction.
- **Abstractions check**: A concrete registry is needed for common validation/mapping identity; one scheduler for bounded two-lane retry semantics; one transport for secure outbound address pinning and deterministic tests. No abstract plugin bases, generic repository layer, ORM, task framework or dependency injection framework.
- **Configuration check**: Only new user-facing callback flags are `CALLBACKS_ENABLED` and `CALLBACK_DESTINATIONS_JSON`; existing cursor/index-delay settings remain bounded. Other limits are constants and compose resource settings, not a tuning matrix. Injectable clocks/backends/transports are test seams, not runtime plugins.
- **Could this be simpler?**: Adding checks around existing fixed schemas, full scrolls, per-record threads and event logs would be smaller but would not produce bounded memory, isolated mappings, truthful completion or actual secure delivery. Concrete modules at the validation, worker and network boundaries are the smallest reliable changes. Retaining the existing HTTP framework and OpenSearch HTTP adapter avoids an unrelated rewrite.

## 14. Implementation Plan

All tasks start **Not Started**. Each epic includes tests before implementation, a finite file scope (maximum seven distinct files), dependencies and a completion gate. An implementation agent MUST stop/report unexpected additional-file requirements rather than silently broaden scope; a targeted PRD correction can authorize them. No task authorizes changing real `pyproj` or adding persistence.

- **EPIC-001: Bundle four finite compatibility schemas.** **DONE**. Depends on: none. Traces: FR-001, FR-002. Gate: all six JSON assets parse, contain provenance/limits and describe every existing sample without advertising full OSDU conformance.

| Task | Description | Status | Relevant Files |
|------|-------------|--------|----------------|
| ITEM-001 | Inventory the four existing kinds and every existing sample data field; author four root-data schemas and shared local definitions using only the finite Draft 2020-12 vocabulary. Preserve sample extras with additionalProperties true; give known fields actual types. | Done | `server/schemas/common.json`, `server/schemas/file-generic-1.0.0.json`, `server/schemas/well-1.4.0.json`, `server/schemas/wellbore-1.4.0.json`, `server/schemas/welllog-1.4.0.json` |
| ITEM-002 | Create catalog identities, local refs, field mapping declarations, SHA-256 and truthful public/local provenance. Verify public attribution or explicitly mark local compatibility authorship; do not download schemas at runtime. Validate JSON assets with standard-library JSON parsing before next epic. | Done | `server/schemas/catalog.json` |

- **EPIC-002: Implement offline registry and storage validation.** Depends on: EPIC-001. Traces: FR-001, FR-002, NFR-003. Gate: AC-001/002 registry cases and all existing sample storage tests pass.

| Task | Description | Status | Relevant Files |
|------|-------------|--------|----------------|
| ITEM-003 | Write failing registry tests for sample data, fifth kind, identity normalization, immutable lifecycle, profile/ref/cycle limits, field capabilities and zero-network resolution. Pin compatible schema libraries and hashes with binary arm64/x86 availability. | Not Started | `server/tests/test_schema_registry.py`, `server/requirements.txt` |
| ITEM-004 | Implement finite registry, catalog hash verification, closed resolver, envelope/data validation and mapping derivation with size/depth/type bounds. Add schema route normalization while preserving response shape and legal/ACL checks. | Not Started | `server/schema_registry.py`, `server/server.py` |
| ITEM-005 | Add handler regression tests for schema POST/PUT/list/lookup, deprecation, entity-only rejection, built-in protection, and current record/manifest samples. Keep catalog files unchanged unless correcting explicit fixture type evidence. | Not Started | `server/tests/test_server.py` |

- **EPIC-003: Implement bounded volatile indexing and runtime lifecycle.** Depends on: EPIC-002. Traces: FR-006, NFR-001/002. Gate: AC-007/008 and import/shutdown portions of AC-014.

| Task | Description | Status | Relevant Files |
|------|-------------|--------|----------------|
| ITEM-006 | First write barrier/fake-clock tests for capacity reservations, whole-batch atomicity, pinned version bytes/eviction, retry exhaustion, same-ID serialization, cross-kind ID rejection, uncertain timeout, soft/hard-delete/recreate fences and shutdown <=10 s. | Not Started | `server/tests/test_runtime_jobs.py`, `server/tests/test_indexing_backend.py` |
| ITEM-007 | Add bounded two-lane scheduler with index lane active and delivery lane reserved for later integration; implement four-attempt retry heap, terminal callbacks, generation fences and explicit lifecycle. Replace daemon-per-write behavior, timer readiness and import-time backend/preload side effects. | Not Started | `server/runtime_jobs.py`, `server/server.py` |
| ITEM-008 | Return per-item bulk outcomes, normalize retry classes, enforce external_gte generation and same-generation identical retries, versioned engine tombstones and all delete acknowledgement/503 semantics. Implement exclusive owned-prefix/legacy cleanup and epoch creation at explicit startup. Adapt fake backend and existing fake-injection tests; no import network. | Not Started | `server/search_backend.py`, `server/tests/fake_search_backend.py`, `server/tests/test_server.py` |

- **EPIC-004: Execute supported manifests with truthful lifecycle.** Depends on: EPIC-003. Traces: FR-005, NFR-003. Gate: AC-009/010.

| Task | Description | Status | Relevant Files |
|------|-------------|--------|----------------|
| ITEM-009 | Write failing tests for valid inherited sample context, invalid final item, empty/unsupported sections/workflows, capacity rejection, blocked/failed indexing, deadline and supersession with reconciled counters. | Not Started | `server/tests/test_manifest_runtime.py` |
| ITEM-010 | Replace timer success with per-version worker acknowledgement callbacks, bounded run registry, submitted/running/terminal timestamps and additive per-record poll diagnostics. Prevalidate/reserve manifest atomically and return 501 for unsupported workflow execution. | Not Started | `server/server.py`, `server/runtime_jobs.py` |
| ITEM-011 | Preserve existing workflow admission/run response keys and sample polling route; prove polling alone never completes a blocked run and late acknowledgements cannot change terminal status. | Not Started | `server/tests/test_server.py`, `server/tests/test_indexing_backend.py` |

- **EPIC-005: Push finite authorized search into isolated OpenSearch indices.** Depends on: EPIC-003. Traces: FR-003, NFR-001/003. Gate: AC-003/004/005.

| Task | Description | Status | Relevant Files |
|------|-------------|--------|----------------|
| ITEM-012 | Freeze baseline count/filteredTerms/histogram request keys, defaults, scalar Count/FilteredTerms/Histogram envelopes, termCount/totalCount and key order into fixtures from existing helpers before replacing them. Assert deliberate array-aggregation 400 restriction, explicit aggregation work bounds and projection empty/wildcard/null/overlap cases. Add real-engine fixtures for external version timeout fencing, incompatible kinds, >1,000 records, sort ties, denied totals/buckets and spatial boundaries. | Not Started | `server/tests/test_search_backend.py`, `server/tests/test_opensearch_integration.py` |
| ITEM-013 | Implement per-kind explicit mappings and bounded resolved index sets, typed field checks, engine paging/sort/source/terms/spatial DSL and finite query-string safety checks. Remove scroll/all-hit materialization and Python aggregations from query routes. | Not Started | `server/search_backend.py`, `server/schema_registry.py`, `server/server.py` |
| ITEM-014 | Update fake backend to bounded search request/result contract without claiming analyzer/spatial equivalence; normalize 400/503 and partial-shard failures; retain ordinary query response envelopes and exact authorized counts. | Not Started | `server/tests/fake_search_backend.py`, `server/tests/test_server.py` |

- **EPIC-006: Implement bounded compatible engine cursors.** Depends on: EPIC-005. Traces: FR-004, NFR-001/002. Gate: AC-006, cursor aspects of AC-014.

| Task | Description | Status | Relevant Files |
|------|-------------|--------|----------------|
| ITEM-015 | First test token ownership/groups, same-token final empties, immutable query/limit, concurrent continuation, quotas, absolute expiry and resource release on every error path. Real engine test inserts/updates after page one to verify snapshot. | Not Started | `server/tests/test_server.py`, `server/tests/test_opensearch_integration.py` |
| ITEM-016 | Replace record snapshots with <=16 KiB cursor metadata/PIT/search-after, reserve quotas before creation, hash principal binding and close contexts at completion/expiry/shutdown. Maintain demo-prefix opaque response contract. | Not Started | `server/server.py`, `server/search_backend.py`, `server/tests/fake_search_backend.py` |

- **EPIC-007: Build secure bounded HMAC callback transport.** Depends on: EPIC-003. Traces: FR-007, NFR-004. Gate: AC-011/012 transport cases.

| Task | Description | Status | Relevant Files |
|------|-------------|--------|----------------|
| ITEM-017 | First implement tests for exact canonical HMAC bytes and stable eventId; URLs, metadata IPv4/IPv6/mapped forms, rebind, private enrollment, redirect, SNI verification, proxy environment, timeout and redaction attacks using injected DNS/connect/clock seams. | Not Started | `server/tests/test_callback_delivery.py` |
| ITEM-018 | Implement strict destination config parsing, HTTPS address-pinned verified transport with bounded response/time, no redirects/proxies, HMAC headers and retry classification. Do not mint GSA tokens or execute action URLs. | Not Started | `server/callback_delivery.py` |

- **EPIC-008: Connect notification registration, ingress and delivery outcomes.** Depends on: EPIC-004, EPIC-007. Traces: FR-007, NFR-001/004. Gate: AC-011/013.

| Task | Description | Status | Relevant Files |
|------|-------------|--------|----------------|
| ITEM-019 | Write failing subscription/event tests for disabled default, explicit enable/HMAC requirement, GSA unsupported status, full-fan-out capacity, minimal authorized record-reference payloads, duplicate retries and secret rotation/cancel. | Not Started | `server/tests/test_community_services.py` |
| ITEM-020 | Integrate delivery scheduler lane with subscription owner/current-group authorization and safe transport; index ACK emits minimal matching record event once. Enforce queue/history/byte caps and retain truthful delivered flag plus protected delivery detail route. | Not Started | `server/community_services.py`, `server/server.py`, `server/runtime_jobs.py`, `server/callback_delivery.py` |
| ITEM-021 | Test no secret/token/body in handler, worker or transport error/log output; unsupported actions remain metadata-only; notification capacity loss is explicit without failing already-indexed storage or causing duplicate indexing. | Not Started | `server/tests/test_callback_delivery.py`, `server/tests/test_manifest_runtime.py` |

- **EPIC-009: Enforce process bounds and prove runtime/restart behavior.** Depends on: EPIC-006, EPIC-008. Traces: NFR-001/002/005. Gate: AC-014 and all resource boundary assertions.

| Task | Description | Status | Relevant Files |
|------|-------------|--------|----------------|
| ITEM-022 | Write actual HTTP tests for admission-before-thread spawn, content length/transfer/body/time limits, bounded histories/versions/metadata and protected diagnostic counters. Add subprocess/real-engine restart tests for stale namespace cleanup and other-namespace survival. | Not Started | `server/tests/test_runtime_limits.py`, `server/tests/test_opensearch_integration.py` |
| ITEM-023 | Enforce all remaining HTTP/state/byte caps, reconcile counters/eviction, and expose redacted runtime diagnostics/readiness. Validate bounded stop/cleanup and startup failures with no stale reads. | Not Started | `server/server.py`, `server/runtime_jobs.py`, `server/community_services.py` |

- **EPIC-010: Deployment, documentation and end-to-end gates.** Depends on: EPIC-009. Traces: NFR-001/003/005. Gate: AC-015/016 with measured results, no fabricated hardware claim.

| Task | Description | Status | Relevant Files |
|------|-------------|--------|----------------|
| ITEM-024 | Set both compose variants' API/OpenSearch resource limits and 2 GiB engine heap, maintain no data volumes/brokers; retain single replica and internal engine network. Extend existing CI for real-engine suites and both architecture hash/wheel builds. | Not Started | `server/compose.yaml`, `server/compose.pull.yaml`, `.github/workflows/ci.yml` |
| ITEM-025 | Add bounded opt-in soak implementing the specified 30 min workload/outage and asserting status/capacity/thread/memory/latency results. It MUST require explicit non-production test target and token; never launch by default in unittest discovery. Run on actual target hardware before release. | Not Started | `server/tests/soak_runtime.py` |
| ITEM-026 | Document exact schema/search/manifest/notification profiles, deliberate compatibility restrictions, callback receiver signing/dedup/allowlist setup and no-durability/restart/rollback behavior. Run all old/new tests, examples and reference-service tests; record failures honestly, never replace pyproj with a mock to pass. | Not Started | `README.md`, `installRPI.md` |

## 15. Change Log

- 2026-11-16: Initial planning artifact for the agreed volatile, bounded four-capability scope. No application version bump or repository CHANGELOG change is authorized.
- 2026-11-16: Completed EPIC-001 only (ITEM-001/002), with explicit user implementation approval. Added the six schema/catalog assets; catalog inventories seven standalone JSON records, six manifest records, both 15-record CSV sample sets and additional positive test-fixture fields. Provenance explicitly states local compatibility authorship; upstream content/revision/license remain unverified. Shared definitions are reference resources, not a fifth registered kind. All data properties remain optional; unknown legacy extras are allowed but unmapped. SpudDate stays a date-only keyword string (not date-time); GeometryCollection object-array descendants are not mapped and no geo_point conversion is claimed.
- 2026-11-16: EPIC-001 validation: six temporary executable acceptance checks were written and run before assets existed (six expected failures), then all six passed. Standard-library JSON parsing passed for all six assets. Closed offline Draft 2020-12 resolution validated 47 fixture instances (43 JSON/CSV sample records plus four positive legacy-field cases), covered every sampled data field, and rejected 41 malformed known-field cases. Checked finite vocabulary, acyclic local refs, exact identities, five resource SHA-256 values, scalar-only mapping declarations, registry limit declarations and permissive legacy extras. Hash policy explicitly normalizes CRLF to LF for Windows Git checkouts; both newline variants checked. No server imports, network, runtime integration, dependency changes or persistence introduced; AC-001/002 runtime gates remain for EPIC-002. EPIC-002 through EPIC-010 remain Not Started.
