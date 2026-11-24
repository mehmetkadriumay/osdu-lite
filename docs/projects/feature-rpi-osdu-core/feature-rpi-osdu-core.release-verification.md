# Final release handoff — BLOCKED

## Commit checkpoint

The Dataset registration envelope correction is now implemented in
`examples/well360/demo/common.sh`. `server/server.py` safely handles object/list
payloads, and malformed-list regressions in `server/tests/test_server.py`
assert HTTP400 with no mutation. The payload, owner-authorization and import
checks passed **3/3 in2.043s**. Trailing blank lines in three test files were
removed before staging.

Tutorial, complete discovery, manifest and latest recreation checks have not
been rerun after these repairs. ITEM-022 remains **Blocked**. Earlier evidence
below records the last complete run and superseded blocker descriptions; it
does not establish a green release for the committed source.

## Last narrow repair supersedes the old blocker descriptions below

Changed only three repair paths:
`server/tests/test_core_auth.py`, `server/tests/test_runtime_jobs.py`,
`examples/well360/demo/common.sh` (plus truthful evidence/status documentation).

- Made owner Search deterministic with acknowledged indexing. Reproduced the
  old assertion failure, then asserted Alice's exact owned IDs/owner ACL and
  Bob's zero total/empty results/no private filename. No runtime ACL policy changed.
- Retained denied socket connection/thread startup and backend-not-started
  assertions. Guarded cold import exited0 in3.469s; increased the subprocess
  bound from10 to30s because the previous integration environment exceeded10s.
  This changes test allowance only, not server import behavior.
- Reproduced tutorial's omitted bearer/partition headers. Added authentication
  only to same-API `/upload/` URLs and refused forwarding credentials elsewhere.
  Register metadata now carries the issued opaque FileSource, not a local name.
  Header/origin executable check passed; auth/grant/ownership policies unchanged.
- Exact affected command
  `PYTHONPATH=server;server/tests python -m unittest test_core_auth.LiveAuthorizationTests.test_core_gated_services_enforce_roles_without_claiming_business_success test_runtime_jobs.SchedulerTests.test_server_import_creates_no_threads_or_network -v`
  **2/2 passed in1.573s**.
- One real tutorial attempt got past authenticated upload, then exited1 with
  curl52/empty reply. API traceback: `server.py:4124`, legacy Dataset registration
  calls `payload.get("datasets")` on a list, raising `AttributeError`.
  **Next precise action:** in `common.sh` emit
  `print(json.dumps({"datasetRegistries": records}))` for registration rather
  than the array. This retains the existing accepted envelope and strict policy.
  No claim of an irreducible blocker; verification was limited to one distinct
  repair attempt. No further broad check/review/discovery loop.
- The requested complete432 real-backend run was **not started after tutorial
  failure**. Last complete result below remains430 passes/1 failure/1 error;
  it is not relabeled green. Manifest/latest recreation not started. No image
  build or migration rerun; runtime modules unchanged this turn.
- Only unique lastrepair-a63d native authenticated loopback API/real backend
  helpers were used. API process,2 named containers,2 volumes,network and unique
  tutorial/Maven context removed. No remaining helper/check; no staging/commit.
  ITEM-022 remains Blocked; baseline correction and CI/Pi limitations unchanged.

Verification stopped at the implementation checkpoint; the user subsequently
requested commit and sync. No helper/check remains running. Original dirty
work was not reset.

## Exact evidence

| Command / check | Results |
|---|---|
| Initial `python -m unittest discover -s server/tests -q` | 428 run;377 passed;7 failures;1 error;43 skips;159.232s |
| Earlier green same command, before last added registration regression test | 431 run;388 passed;43 opt-in skips;0 failures/errors;143.860s |
| `PYTHONPATH=server;server/tests python -m unittest test_opensearch_integration test_storage_backend test_file_backend test_file_migration test_server.TestRealCoreFilesHTTP test_server.TestRealLegacySampleHTTP -q` with both backends/public endpoint configured | 102/102 passed;0 skips/failures/errors;127.954s |
| **Current** `python -m unittest discover -s server/tests -q` with both backends/public endpoint configured | **432 run;430 passed;1 failure;1 error;0 skips;3894.811s** |
| `PYTHONPATH=server;server/tests python -m unittest test_core_auth.LiveAuthorizationTests.test_core_gated_services_enforce_roles_without_claiming_business_success test_runtime_jobs.SchedulerTests.test_server_import_creates_no_threads_or_network -v` afterward | 2/2 passed;2.012s;not a full-run pass |
| `bash server/smoke-test.sh`, `bash server/reference-smoke-test.sh`, `python server/community-smoke-test.py` | All3 exit0 against final ARM64 legacy image |
| Source-identical `examples/well360/demo/run-full-sample.sh`, using temporary Maven/client/cache | Exit0, including real `os-core-common:3.1.0` Java client and LAS download |
| Source-identical `run-tutorial.sh` | Exit1: `curl: (22) ...401`; precise failed operation not isolated before stop |
| `run-manifest.sh`, top-level deployment bootstrap `demo-all.sh` | Not run; bootstrap targets non-unique default resources and was not substituted for isolated verification |
| Latest rebuilt image | Offline-equivalent Linux ARM64,142,554,912 bytes;aarch64;12 current packaged Python SHA-256 matches |
| Latest rebuilt core image roundtrip/recreation | Not rerun; previous batch's real core/recreation evidence retained, not presented as latest-source release closure |

Real test environment used isolated loopback:
`OPENSEARCH_TEST_URL=http://127.0.0.1:55944`,
`AZURITE_TEST_CONNECTION_STRING` with BlobEndpoint
`http://127.0.0.1:55943/devstoreaccount1`,
`AZURE_STORAGE_PUBLIC_BLOB_ENDPOINT=http://localhost:55943/devstoreaccount1`.
The public development key was used, not an operator credential. Resources
were removed; these ports are no longer serving. POSIX PYTHONPATH uses `:`,
Windows uses `;`. Git Bash smoke runs exported `python3(){ python "$@"; }`;
temporary Maven3.9.11 was SHA-512 verified and used strict artifact checksums.

## Exact unresolved failures / next necessary actions

1. `test_core_auth.LiveAuthorizationTests.test_core_gated_services_enforce_roles_without_claiming_business_success`
   (`test_core_auth.py:583`): asserts `private.bin` absent from Alice's successful
   Search, but Alice owns that file. Full run returns2 authorized records.
   Isolated test passes before delayed file indexing completes. Make the test
   deterministic and assert actual owner/foreign policy rather than absence of
   the owner's data. Do not conceal this failed current full-run result.
2. `test_runtime_jobs.SchedulerTests.test_server_import_creates_no_threads_or_network`
   (`test_runtime_jobs.py:165`): `subprocess.TimeoutExpired`,10-second import
   bound. Passed isolated; no captured pre-core baseline proves this pre-existing.
   Establish bounded subprocess/import timing under the complete run; retain
   the denied socket/thread-start side-effect assertions.
3. Isolate tutorial HTTP401 (the script's signed-upload grant curl lacks normal
   bearer headers, but the failing operation was not conclusively traced).
   Preserve legacy bearer-in-URL grant semantics/security, then run tutorial and
   manifest with their real Java consumer on unique resources.
4. Re-run full normal and all-backend discovery after only these concrete fixes,
   then final rebuilt core CRUD/schema/file and API/Azurite recreation. Only
   close ITEM-022 after required software checks pass. CI remains external,
   actual two-Pi workloads/RSS/p95/thermal/SSD remain **NOT VALIDATED**.

## Fixes already persisted and attribution

- Core-only unbound File.Generic guard had applied to legacy declarative
  metadata/undelete. Reproduced400 and restricted guard to core.
- POST replay was incorrectly a read: its own reindex mutation produced503
  `concurrent_mutation_retry`. Reproduced, removed replay from read-response
  classification; other read-policy guards remain.
- Legacy metadata-only Dataset registration incorrectly required an upload
  source/grant. Reproduced400; validated legacy declarative metadata can store
  without binding/bytes. Core and Source-bearing publication remain strict.
- Transport tests now explicitly model admitted per-kind indices, lazy reset,
  external generations and structured bulk Outcomes. Cursor test creates a
  genuinely live cursor and tests group-change invalidation, not null exhaustion.
- Original user baseline:205 passes/6 skips; only the invalid StartM and
  provision0-versus-rebuilt15 engine mismatches were previously established.
  **Withdrawn:** every unsupported "unrelated baseline Dataset-undelete" claim.
  No pre-core byte snapshot was available; no replay of HEAD as baseline.

## Status / use / migration / budget

EPIC-001..008 / ITEM-001..021 retain recorded milestone completion.
EPIC-009 release acceptance / ITEM-022 **Blocked**. Earlier per-epic evidence
does not establish a currently green full release.

Default `OSDU_COMPAT_PROFILE=legacy`; opt-in `core`, `REQUIRE_AUTH=true`.
Set internal `AZURE_STORAGE_CONNECTION_STRING` and separate client-reachable
`AZURE_STORAGE_PUBLIC_BLOB_ENDPOINT`. Optional private `OSDU_IDENTITIES_FILE`
and first-migration `OSDU_CORE_IMPORT_FILE` must be read-only protected files.
Single authoritative API writer. No OIDC, complete upstream schema catalog or
full OSDU conformance; no DDMS/workflow service additions.

Before upgrade: stop writer, back up the complete Azurite snapshot/file volume
to separate storage plus matching old image/source/config/local originals.
Format1 migration creates a digest-verified create-only backup before format2;
no downgrade. Bootstrap format2 with new API, then stop it for explicit local
manifest migration:

```bash
# Configure private backend env, OSDU_PARTITION and FILES_DIR first.
python server/migrate_files.py --manifest /safe/files-manifest.json --dry-run
python server/migrate_files.py --manifest /safe/files-manifest.json --apply --writer-stopped
```

Manifest entries `{recordId,sourcePath,sha256}`; relative paths under FILES_DIR.
No arbitrary automatic local import, no deletion; hashes/ETag/idempotency
checked. Inventory reported private orphan keys after failure; preserve sources.
Never use `down -v` as a restart. Rollback restores matching old backup/image.

No complete **core-only** added/deleted line metric without pre-core bytes.
Known wholly-new core files:4 runtime modules1,027 lines;8 test files3,143 lines.
These are only identifiable new-file subtotals, excluding shared-file edits;
they are not a full feature delta. Cumulative dirty/HEAD totals include earlier
user/Azurite changes and are not feature attribution.

Exactly3 services,0 added runtime dependencies: API2GiB/Azurite2GiB on Pi A;
OpenSearch6GiB with initial2GiB heap on Pi B. Each host16GiB, nonpooled, no HA.
Full snapshot/history/rollback/JSON/Python object and SDK copies amplify128MiB
canonical retained-record limits;144MiB snapshot and8 shard ceilings are not
RSS or fit guarantees. Four samples use4 shards. SSD/private trusted LAN,
development-key/HTTP trust caveats and staging-disk cleanup/quota risks remain.
