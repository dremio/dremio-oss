# Polaris RESTCATALOG Test Results

- Status 값: `PASS` / `FAIL` / `ENVIRONMENT_BLOCKED` / `NOT_SUPPORTED`. 해당 Phase에서 실행하지 않은 항목은 `N/A` 또는 이월 표시를 하고 집계에서 뺀다.
- Phase 1–4는 [progress.md](progress.md)와 Phase 문서에 기록된 결과를 옮긴 것이다. Phase 5는 Agent A(lifecycle), B(read), C(write, namespace/view), E(cache/metadata), F(장애/regression), G(UI) 보고와 Integration 재검증(INSTANCE=7)을 옮긴 것이다. Phase 6은 Fix, Release build, Structure audit 보고와 Integration 최종 검증(INSTANCE=3)이다 (§7).
- 환경 공통: Apache Polaris `apache/polaris:1.1.0-incubating`, Iceberg `1.7.0-5f7c992-20250730084652-3bf8b99` (Dremio fork), 로컬 MinIO (plain HTTP, region `us-east-1`), TLS/region 검증용 throwaway MinIO(`bitnamilegacy/minio`), Dremio tarball(single node, JDK 17 runtime), build JDK 21 / test JDK 11. AWS 계정 없음.
- Live 실행은 `scripts/polaris-e2e` harness를 쓴다 ([README](../../scripts/polaris-e2e/README.md)). Secret은 환경 변수로만 넘기고, 결과에는 건수만 남긴다.

## 요약

| Phase | 내용 | Unit / module test | Local CI | Live | Commit |
|---|---|---|---|---|---|
| 1 | 구조 분석 / Gap Analysis | N/A (코드 변경 없음) | N/A | client-probe, proto-E2E PASS | `67cf10e4c` |
| 2 | RESTCATALOG OSS Source | 신규 89, module 182, UI spec 103 passing / 9 pending | local `full` SUCCESS (원격 QUEUED) | live gate 19항목 (§2) | `f2553db4a` (+ `ffd1cc1b2`) |
| 3 | OAuth2 / Catalog 연동 | 신규/변경 251, module 319 | local `full` SUCCESS, 원격 `16c8597a` SUCCESS | INSTANCE=1–6 PASS | `3b85d3619` |
| 4 | Object Storage | 신규 62 + `TestS3FileSystem` 25, module 381 + `plugins/s3` 112 | local `full` SUCCESS, 원격 `d2dfe26e` SUCCESS | INSTANCE=1–6 PASS (MinIO), AWS ENVIRONMENT_BLOCKED | `691f2b5a9` |
| 5 | 실제 E2E / Regression | 신규 5 (CTAS 3, layout 1, config 재시작 조건 1) + flaky test 수정 1, module 386, `plugins/s3` 112, `dac/backend` 78, UI spec 103 passing / 9 pending | local `full` SUCCESS (2차와 review 수정 뒤 3차; 1차는 flaky TLS test로 FAILED → test 수정), 원격은 Lead | INSTANCE=1–7: PASS 139, FAIL 8 (kernel K-01 3, known C-08 3, known K-15 1, minor 1), NOT_SUPPORTED 5 (§5) | `6d2f6f4e9` |
| 6 | 최종 통합 / Release Readiness | 신규 26 (icebergcatalog, 최종 review 3 포함) + kernel 2, module 412, `sabot/kernel` `TestManagedStoragePlugin`+`TestPluginsManager` 34 | local `full --no-cache` SUCCESS (6 step) | release smoke 12/12 (INSTANCE=2, Release Agent), 최종 tarball smoke 12/12 + P6-2–P6-8 7/7 (INSTANCE=3) (§7) | Lead 기록 |

## 1. Phase 1 — 구조 분석

| 항목 | 결과 | Status |
|---|---|---|
| 정적 분석, Gap G-01..G-28 | [phase1-analysis.md](phase1-analysis.md) | PASS |
| Iceberg client-probe (Dremio의 Iceberg jar → Polaris 1.1.0) | config, OAuth2, namespace/table/view operation | PASS |
| Dremio tarball + out-of-tree `@SourceType` prototype | source 생성, namespace 조회 | PASS |
| Dremio data path (DremioFileIO, S3) | Phase 4/5로 이월 | (Phase 4 PASS) |

## 2. Phase 2 — RESTCATALOG OSS Source

### 2.1 Unit / regression

| 범위 | 결과 |
|---|---|
| `TestRestCatalogSourceRegistration` 12, `TestRestCatalogLayout` 11, `TestRestIcebergCatalogPluginConfig` 28, `TestRestIcebergCatalogPlugin` 38 | 89 tests, 0 failures |
| `plugins/icebergcatalog` module 전체 | 182 tests, 0 failures, 0 skipped |
| UI spec 6개 (sourceUtils, SourceFormJsonPolicy, sourcesMapper, SelectSourceType, AddSourceModal, EditSourceView) | 103 passing, 9 pending (기존 skip), 0 failing |
| `scripts/dev fmt` / `lint`, errorprone, forbiddenapis | clean |

### 2.2 AbleOps Local CI

| Step | 결과 |
|---|---|
| `icebergcatalog-test` | SUCCESS (182 tests) |
| `icebergcatalog-lint` | SUCCESS |
| `icebergcatalog-static` | SUCCESS (이 시점에는 errorprone이 실제로 돌지 않았다. Phase 3에서 보완) |
| `ui-source-specs` | SUCCESS (103 passing, 9 pending) |
| 원격 `localci submit` | run `7213d373` QUEUED 후 취소 (CI 서버 할당 문제). `localci run`(local mode)으로 대체 |

### 2.3 Live gate (요약)

| # | 항목 | Status |
|---|---|---|
| 1–2 | `GET /api/v3/source/type`, `/api/v3/source/type/RESTCATALOG` (uiConfig) | PASS |
| 2b | `/apiv2/source/type/RESTCATALOG` | NOT_SUPPORTED (404, v3만) |
| 3 | 공식 JSON으로 `POST /api/v3/catalog` → `good` | PASS |
| 4–5 | GET masking, masked PUT 후 credential 유지 | PASS |
| 6–7 | Polaris(MinIO) state `good`, namespace 노출 | PASS |
| 8–9 | `restEndpointUri` 누락 400, 잘못된 credential/warehouse 400 (hint는 log에만 → Phase 3 해결) | PASS |
| 10 | `isUsingVendedCredentials=true` (flag) | PASS (runtime은 Phase 4) |
| 11–12 | CREATE/INSERT/SELECT smoke, 재시작 후 SELECT | PASS (smoke) |
| 13 | log secret 노출 | PASS (0건) |
| 19 | UI 브라우저 생성 | Phase 2에서는 미실행 → Phase 5 U-1–U-13 PASS (§5.8) |

## 3. Phase 3 — OAuth2 / Catalog 연동

### 3.1 Unit / regression

| 범위 | 결과 |
|---|---|
| `TestRestCatalogAllowedNamespaces` 44, `TestRestCatalogNamespaceTableOps` 36, `TestRestCatalogAccessor` 33, `TestRestCatalogHttpErrors` 30, `TestRestCatalogOAuth2` 18, `TestRestIcebergCatalogPluginConfig` 28, `TestRestIcebergCatalogPlugin` 41, `TestIcebergRestCatalogAccessor` 21 | 251 tests, 0 failures, 0 skipped (Review 반영 후) |
| `plugins/icebergcatalog` module 전체 | 319 tests, 0 failures, 0 skipped |
| UI spec | 103 passing |
| fmt / lint / errorprone / forbiddenapis / shellcheck | clean |

### 3.2 AbleOps Local CI

| Step | local (`localci run --profile full --no-cache`) | 원격 (`localci submit`, run `16c8597a`, 104 s) |
|---|---|---|
| `icebergcatalog-test` | SUCCESS (319) | SUCCESS (319 tests, 0 failures) |
| `icebergcatalog-lint` | SUCCESS | SUCCESS |
| `icebergcatalog-static` | SUCCESS | SUCCESS |
| `ui-source-specs` | SUCCESS | SUCCESS (103 passing) |
| `e2e-harness-lint` (신규) | SUCCESS | SUCCESS |

### 3.3 Live (요약, 상세는 [phase3-oauth-catalog.md §4–§5, §9](phase3-oauth-catalog.md))

| 항목 | Status |
|---|---|
| 잘못된 credential / scope / warehouse / endpoint → API 400 `errorMessage`에 hint (G-09) | PASS |
| 403 principal (`warn`), read-only principal (SELECT 성공, 쓰기 PERMISSION ERROR) | PASS |
| Token 만료/갱신 (TTL 60초, 6분간 SELECT 12회, 새 config 0, 401 0) | PASS |
| Polaris pause / stop / 재생성(새 signing key) 후 복구 | PASS |
| Namespace/table/view CRUD, CTAS, INSERT/UPDATE/DELETE/MERGE | PASS |
| 비어 있지 않은 namespace DROP 메시지 (G-10), DROP VIEW purge hint (G-06) | PASS |
| allowedNamespaces matrix (첫 실행 FAIL → folder listing 순서 수정 후 7분 안정) | PASS (수정 후) |
| 거부된 commit 후 S3 잔여 object 0 (`CommitForbiddenException`) | PASS |
| Rename | NOT_SUPPORTED |
| 공백이 든 namespace | FAIL (upstream O-1) |
| Secret scan (6개 instance) | PASS (0건) |

## 4. Phase 4 — Object Storage

### 4.1 Unit / regression

| 범위 | 결과 |
|---|---|
| 신규: `TestRestCatalogStorageConfig` 13, `TestRestCatalogS3CompatibleProps` 9, `TestRestCatalogVendedCredentials` 33, `TestRestCatalogSecretExposure` 7 | 62 tests, 0 failures |
| 변경: `TestS3FileSystem` (신규 2) | 25 tests, 0 failures |
| `plugins/icebergcatalog` module 전체 | 381 tests, 0 failures, 0 skipped |
| `plugins/s3` module 전체 | 112 tests, 0 failures, 0 skipped |

### 4.2 AbleOps Local CI

| Step | local (`localci run --profile full --no-cache`, 2분 26초) | 원격 (`localci submit`, run `d2dfe26e`, 147 s) |
|---|---|---|
| `icebergcatalog-test` | SUCCESS (381) | SUCCESS (381 tests, 0 failures) |
| `s3-test` (review 신규) | SUCCESS (112) | SUCCESS (112 tests, 0 failures) |
| `icebergcatalog-lint` (icebergcatalog + s3) | SUCCESS | SUCCESS |
| `icebergcatalog-static` | SUCCESS | SUCCESS |
| `ui-source-specs` | SUCCESS (103 passing, 9 pending) | SUCCESS |
| `e2e-harness-lint` | SUCCESS | SUCCESS |

### 4.3 Live (요약, 상세는 [storage.md §14](storage.md#14-검증-matrix))

| 항목 | Status |
|---|---|
| 공식 static baseline + MinIO endpoint: CREATE/INSERT/CTAS/INSERT…SELECT/UPDATE/DELETE, metadata table, `AT SNAPSHOT`, 재시작 | PASS (MinIO) |
| 공식 static baseline on AWS S3 | ENVIRONMENT_BLOCKED |
| MinIO 최소 recipe, key별 필요 여부 | PASS |
| provider 생략 (수정 전 FAIL) / key 없는 source fail closed | PASS |
| endpoint scheme (수정 전 FAIL) | PASS |
| SSL/TLS, region, path-style, requester-pays, bucket discovery (MinIO) | PASS. custom region 이름 NOT_SUPPORTED, AWS 항목 ENVIRONMENT_BLOCKED |
| Vended credentials (static key 없음 / 있음 / bucket별 provider), 만료 갱신 | PASS (single node) |
| Vended만 있는 source의 기본 location 밖 `LOCATION` | NOT_SUPPORTED |
| Multi-node, AWS STS vending, instance profile | ENVIRONMENT_BLOCKED |
| Secret 노출 matrix (REST/job/profile/system table/log/KV) | PASS (KV 평문 G-12 제외) |

## 5. Phase 5 — 실제 E2E / Regression

### 5.0 환경과 요약

Status·결과 칸 괄호 안의 두 자리 ID(`C-17`, `K-01`, `N-10`, `S-13`, `X-09` 등)는 [known-limitations.md](known-limitations.md)의 항목이다. 각 표 첫 칸의 번호(`A-1`, `C-6` 등)는 이 문서의 시나리오 번호다.

| 항목 | 값 |
|---|---|
| Dremio | `scripts/dev build distribution/server`로 만든 tarball(HEAD `691f2b5a9` tree)을 scratchpad에 새로 풀고, `dac/ui` bundle을 다시 build해 `dremio-dac-ui` jar만 교체한 배포본(`p5-dist`). Single node, JDK 17 runtime. 각 Agent는 이 배포본을 `dremio-up.sh`로 복사해 썼다 |
| Plugin jar | A, B, E, F, G: 배포본의 jar(Phase 4). C와 Integration(INSTANCE=7): Phase 5 수정이 들어간 `plugins/icebergcatalog` jar를 자기 instance 복사본에만 넣었다 |
| Polaris | `apache/polaris:1.1.0-incubating`, container `p5-polaris-e2e-<INSTANCE>`, catalog `e2ecat`. F는 token 수명 30초 |
| Storage | 로컬 MinIO(`http://127.0.0.1:9000`, bucket `dremiodev`, prefix `polaris-p5-<name>`). F의 storage down은 throwaway MinIO(`p5-minio-down`) |
| Generic REST catalog | `apache/iceberg-rest-fixture:latest` (JDBC/sqlite + S3FileIO, OAuth 없음, throwaway MinIO) |
| Source 설정 | Harness 기본값(`source.sh create`, Phase 2 recipe), `namesRefreshMs=60000`. 이 기본값은 [configuration.md §4](configuration.md#4-minio-사용-예제) 최소 recipe의 상위 집합이다: `dremio.bucket.discovery.enabled=false`, `dremio.s3.region`, `fs.s3a.endpoint.region`, `fs.s3a.requester.pays.enabled=false`를 더 보낸다. 따라서 Phase 5 E2E는 §4 최소 recipe 자체를 실행하지 않았고, 최소 recipe의 근거는 Phase 4 live([storage.md §5](storage.md#5-minio-recipe-최소-설정))다 |
| Instance | A=1 lifecycle, B=2 read, C=3 write + namespace/view, E=4 cache/metadata, F=5 장애/regression, G=6 UI, Integration=7 (Prep=0) |

| 영역 | PASS | FAIL | ENVIRONMENT_BLOCKED | NOT_SUPPORTED | 비고 |
|---|---|---|---|---|---|
| 5.1 Source lifecycle (A) | 19 | 3 | 0 | 1 | FAIL 3건은 모두 kernel K-01 (update 실패 후 background refresh 정지). A-12는 Phase 5에서 실행하지 않은 이월 항목이라 집계에서 뺐다 |
| 5.2 Read (B) | 24 | 0 | 0 | 0 | `SHOW TABLES` 0 rows(N-06)는 harness bug였고 수정 |
| 5.3 Write (C) | 23 | 1 | 0 | 2 | partitioned CTAS는 FAIL → plugin 수정 후 PASS. FAIL 1건은 `DROP TABLE IF EXISTS` 메시지(minor) |
| 5.4 Namespace / View (C) | 14 | 0 | 0 | 2 | NOT_SUPPORTED는 grammar(`DROP FOLDER IF EXISTS`, `ALTER VIEW`) |
| 5.5 Cache / Metadata (E) | 23 | 0 | 0 | 0 | |
| 5.6 장애 (F) | 11 | 4 | 0 | 0 | FAIL 4건은 알려진 결함: F-5 기본 S3A 재시도의 hang(K-15), F-12–F-14 429/5xx/timeout의 hint 없는 raw 메시지(C-08). 네 건 모두 장애 해제 뒤 복구는 정상. K-12, K-13, X-09 제약은 PASS 항목의 관찰 |
| 5.7 Regression | 5 | 0 | 0 | 0 | `sabot/kernel`은 실행하지 않아 N/A (변경 없음, 집계 제외) |
| 5.8 UI (G) | 13 | 0 | 0 | 0 | 완료 기준 #2 충족 |
| 5.9 Integration 재검증 (INSTANCE=7) | 7 | 0 | 0 | 0 | Phase 5 code 변경 범위만 |
| 합계 | 139 | 8 | 0 | 5 | AWS S3, multi-node는 Phase 4부터 ENVIRONMENT_BLOCKED (Phase 5에서 재실행하지 않음). 실행하지 않은 항목(A-12, `sabot/kernel` regression)은 집계하지 않았다 |

### 5.1 Source lifecycle (Agent A, INSTANCE=1)

Seed: source 생성 전에 Polaris namespace `ns1`, `ns2`, table `ns1.t0`.

| # | 시나리오 | 결과 | Status |
|---|---|---|---|
| A-1 | Create (`POST /api/v3/catalog`) | 200, `good`, children `polaris.ns1`/`polaris.ns2`. CREATE TABLE `ns1.t1`, INSERT 2, SELECT, CREATE VIEW `ns2.v1`, view SELECT 모두 `COMPLETED` | PASS |
| A-2 | Get, secret masking | `/api/v3/catalog/{id}`, `/api/v3/catalog/by-path/polaris`, `/api/v3/source/{id}`, `/apiv2/source/polaris` 200. `credential`, `fs.s3a.access.key`, `fs.s3a.secret.key` 모두 `$DREMIO_EXISTING_VALUE$`. MinIO/Polaris secret 0건 | PASS |
| A-3 | Update (masked secret, `fs.s3a.connection.maximum=64`, `rest.client.connection-timeout-ms=10000` 추가) | 200 `good`, tag 변경, property 저장, secret masked 유지. 이후 SELECT/INSERT 성공 (실제 credential 유지) | PASS |
| A-4 | `allowedNamespaces=[ns1]` 설정 후 제거 | 200 `good`. tree와 `INFORMATION_SCHEMA`에 `ns1`만. 목록 밖 table `ns2.t2` 직접 SELECT는 **성공**(N-01), 목록 밖 view `ns2.v1`은 "not found in namespace"(N-11). 제거 후 `ns2`와 view 복귀 | PASS |
| A-5 | `isUsingVendedCredentials` true → false | 200 `good` 양방향, GET 반영. true일 때 INFO "requests vended credentials", SELECT/INSERT 성공 | PASS |
| A-6 | Dremio 재시작 (`dremio-up.sh --reuse`, 약 44초) | `good`, 설정(추가 property, vended=false, `namesRefreshMs=60000`) 유지, GET masked(secret 0건), SELECT 4 rows/INSERT/view 성공 | PASS |
| A-7 | `ALTER SOURCE polaris REFRESH STATUS` | "Successfully refreshed status … New status is: Healthy". **state만** 갱신 (직전에 만든 namespace는 안 보임) | PASS (state만) |
| A-8 | `ALTER TABLE polaris.ns1.t0 REFRESH METADATA` (`FORCE UPDATE`, `FORGET METADATA` 포함) | "Metadata for table … refreshed.", "Successfully removed table …". 다음 query가 다시 등록 (count 5) | PASS |
| A-9 | 외부 schema 변경 (Polaris REST `add-schema`, column `extra int`) | 다음 query에서 refresh 없이 `DESCRIBE` 3 column | PASS |
| A-10 | Source 생성 뒤 Polaris에서 만든 namespace | 51–121초 뒤 listing (여러 번 측정, `source.sh wait`) | PASS (최소 60초 지연) |
| A-11 | Polaris에서 drop(purge)한 table `ns3.t3` | 150초 이상, names refresh 여러 번 뒤에도 tree와 `INFORMATION_SCHEMA`에 남음. query는 "Object 't3' not found". `ALTER TABLE … REFRESH METADATA` → "no longer exists, metadata removed." 후 사라짐 | PASS (N-10 제약) |
| A-12 | `/api/v3/catalog/{id}/refresh` | Phase 5에서 실행하지 않음. 이전 Phase 결과(N-09: dataset reflection refresh, source에는 404)를 옮겼다 | NOT_SUPPORTED (N-09 이월, Phase 5 집계 제외) |
| A-13 | Delete | 204. by-path/id/`ns1/t1`/`/apiv2/source` 모두 404, root catalog에 `@dremio`만, `INFORMATION_SCHEMA` 0, SELECT "not found". Polaris(namespace `ns1`–`ns6`, table, view)와 S3 object(22→22) 그대로. 같은 이름 재생성 → `good`, 데이터/뷰 복귀 | PASS |
| A-14 | Rename (v3 PUT, `name`/`path` 변경) | **HTTP 404** "Source name is immutable." source 변화 없음 | NOT_SUPPORTED (K-10) |
| A-15 | 중복 이름 생성 (`polaris`, `POLARIS`) | 둘 다 409 "A source with the name [...] already exists." | PASS |
| A-16 | 잘못된 Polaris credential로 생성 | 400 + OAuth2 hint, 저장 안 됨 (GET 404) | PASS |
| A-17 | 잘못된 credential로 update | 400 "Failure creating/updating this source [polaris]: …SourceBadStateException: Unavailable" + `unauthorized_client` hint(중복, K-02). secret 없음. 기존 plugin은 `good`, query 정상 | PASS (API 응답) |
| A-18 | A-17 뒤 background refresh | 직후 만든 namespace `ns4`가 206초 동안 안 보임 | FAIL (kernel K-01) |
| A-19 | 같은 설정(masked secret)으로 다시 update | 200 `good`, 200초 더 기다려도 `ns4` 안 보임. 같은 config는 `replacePlugin`의 "equal config → setLocals" 경로라 재시작 없음 | FAIL (kernel K-01) |
| A-20 | 설정이 실제로 바뀌는 update (`fs.s3a.connection.maximum` 64→65) | 200, update 중 일회성 names refresh로 `ns4` 즉시 보임. 이후 만든 `ns5`는 170초 동안 안 보임 (주기 refresh 계속 정지). `ns4.t4` 직접 SELECT는 성공 | FAIL (kernel K-01) |
| A-21 | Dremio 재시작으로 복구 | 재시작 116초 뒤 `ns5`, 새 `ns6`은 생성 121초 뒤 listing. 주기 refresh 복구 | PASS (우회책) |
| A-22 | 잘못된 S3 secret으로 update 후 원복 | update 200 `good` (state check는 storage를 보지 않음, K-04). SELECT "PERMISSION ERROR: Access denied on …metadata.json", INSERT "SYSTEM ERROR: AmazonS3Exception … SignatureDoesNotMatch … 403" (C-24). 원복 200, SELECT 성공. update가 실패하지 않았으므로 background refresh 유지 (`ns7` 91초) | PASS |
| A-23 | Secret scan (기본 + 잘못된 test secret 3개, MinIO/Polaris root secret, `Bearer`/`access_token`/`s3.*`/`client_secret` marker) | Dremio log, Polaris container log 0건 | PASS |
| A-24 | 정리 | source 204, Dremio purge, container 삭제, MinIO 22 object 삭제(0 남음), port 해제 | PASS |

Reload 경로 정리:
- `ALTER SOURCE <name> REFRESH STATUS`: state만.
- `ALTER TABLE <t> REFRESH METADATA [FORCE UPDATE]` / `FORGET METADATA`: table 1개. 외부에서 drop된 table을 목록에서 지우는 방법이기도 하다.
- Names refresh(최소 60초): 새 namespace/table.
- Table 직접 query: 즉시 등록.
- 설정이 바뀌는 source update: 일회성 names refresh.
- Source refresh REST endpoint는 없다.

### 5.2 Read (Agent B, INSTANCE=2)

Data: source 생성 전에 Polaris namespace `rd`, `rd.nested`, `rd.nested.deep`, `rd2`. `rd.ptypes`(Polaris API로 생성, identity(`region`) partition, long/int/decimal(10,2)/date/timestamp/timestamptz/boolean/double/list/struct/string, Dremio INSERT 4회 → 5 rows, 5 files, 3 partitions, 4 snapshots, all-NULL row 1개). `rd2.papi_simple`(Polaris API 생성 + Dremio INSERT). `rd.nested.events`(Dremio `PARTITION BY (cat)`, 5,000 rows INSERT 2회 → 10,000 rows, 14 files, 7 partitions, 2 snapshots, MinIO parquet 14개). `rd.nested.deep.ctas_events`(CTAS 2,858 rows), `rd2.dim`(8 rows).

| # | 시나리오 | 결과 | Status |
|---|---|---|---|
| B-1 | `SHOW SCHEMAS` / `SHOW SCHEMAS LIKE 'polaris%'` | 9 / 5 rows (`polaris`, `polaris.rd`, `polaris.rd.nested`, `polaris.rd.nested.deep`, `polaris.rd2`) | PASS |
| B-2 | `SHOW TABLES` "0 rows" (N-06) | 원인은 harness. coordinator에서 답하는 문장(SHOW, DESCRIBE, DDL 요약)은 `GET /api/v3/job/{id}`의 `rowCount`가 0이지만 `/results`는 row를 준다. `sql.sh`가 `rowCount>0`일 때만 결과를 읽었다 → 수정 | PASS (harness 수정) |
| B-3 | `SHOW TABLES IN polaris.rd` / `rd2` / `rd4.sub` | 각 1 row, 이름 정확 | PASS |
| B-4 | `SHOW TABLES IN sys` | 1 row (`jobs_recent`), `INFORMATION_SCHEMA`도 1 | PASS (OSS 기본 동작) |
| B-5 | context 없는 `SHOW TABLES` | "A context is required…" (Dremio 기본 동작). `/api/v3/sql`에 `"context":["polaris","rd","nested"]`를 주면 `events` | PASS |
| B-6 | `INFORMATION_SCHEMA."TABLES"` / `SCHEMATA` / `COLUMNS` | nested 포함 모든 table/namespace | PASS |
| B-7 | Polaris API로 만든 table `DESCRIBE` | 11 column, DECIMAL(10,2)/DATE/TIMESTAMP(timestamptz도 TIMESTAMP)/ARRAY/ROW | PASS |
| B-8 | `SELECT *`, complex type projection | 값 정확 (decimal 99999999.99/-5.50, 윤일 date/ts, ms ts, NULL row). `addr['city']`, `tags[0]`, `cardinality` | PASS |
| B-9 | Filter (decimal, date, ts BETWEEN, boolean, LIKE) | 기대 row | PASS |
| B-10 | Aggregate (Python 계산값과 비교) | count 10000, sum(id) 49995000, sum(val) 149995000, count distinct 10000, sum(length(s)) 48890, sum(mod(id*id,1000003)) 4803349563, epoch-day 합 199034050, partition 7개 count/sum, CTAS 2858/14285713 | PASS |
| B-11 | Join | `events`⋈`dim` category별 값 일치, LEFT JOIN anti-match 1, `sys.options`×`rd.ptypes` 5, `papi_simple`⋈`ptypes` 2 | PASS |
| B-12 | Source 생성 뒤 Polaris에서 만든 table | 직접 query는 즉시 (`rd3.late_t` count 0). 새 nested namespace/table(`rd4.sub.late_w`)은 35초 뒤 tree, 이어서 SHOW TABLES/`INFORMATION_SCHEMA` | PASS |
| B-13 | Polaris 쪽 schema 변경 (column `score` 추가) | 마지막 validity check가 60초 이내면 안 보임 (`dremio.metadata_expiry_check_interval_in_secs`=60), commit 약 71초 뒤 보임 | PASS (framework 동작, N-12) |
| B-14 | Polaris 쪽 column rename (`data`→`payload`, 같은 field id) | validity 창 이후 이전 Parquet file도 새 이름으로 읽음 (field-id mapping) | PASS |
| B-15 | column 추가 4초 뒤 `ALTER TABLE … REFRESH METADATA` | "refreshed", 다음 SELECT에 `extra` | PASS |
| B-16 | Dremio `ALTER TABLE ADD COLUMNS` | Polaris schema `3:note:string`, INSERT/SELECT 성공 | PASS |
| B-17 | `EXPLAIN PLAN` | `IcebergManifestList`(snapshot, metadataFileLocation) → `SPLIT_GEN_MANIFEST_SCAN`(partition filter `cat==3`, `region=='eu'`) → `DATA_FILE_SCAN`(row-group filter) | PASS |
| B-18 | 실행 시 partition pruning | `WHERE cat=3` 1,429 records(1 partition), 대조 `WHERE val>0` 10,000 | PASS |
| B-19 | `AT SNAPSHOT` | 첫 snapshot 5000/12497500/max 4999, 둘째 10000. EXPLAIN에 요청 snapshot. `ptypes` 첫 snapshot 1 row | PASS |
| B-20 | `AT TIMESTAMP` (UTC) | 두 commit 사이 5000, 이후 10000, table 생성 전 "out of range", snapshot `'123'` "invalid" | PASS |
| B-21 | Schema 변경 전 snapshot time travel | 그 시점 schema(`id`, `data`) | PASS |
| B-22 | Metadata table | `table_history` 2, `table_snapshot`(append, added-records 5000/total 10000), `table_files` 14 files/10000 records/7 partitions/118986 bytes, `table_manifests` 2, `table_partitions` 7 | PASS |
| B-23 | Polaris에서 drop(purge)한 table | query 즉시 "table not found". tree/`INFORMATION_SCHEMA`에는 3분 이상 남음. `REFRESH METADATA` → "no longer exists, metadata removed." 후 0 | PASS (N-10 제약) |
| B-24 | `scan-secrets.sh` | 0건 | PASS |

관찰 (bug 아님): 외부 변경 후 최대 60초 cached metadata(N-12, 그 사이 schema가 바뀐 table INSERT는 "Table schema … doesn't match"), table cache 3초 안의 `REFRESH METADATA`(N-13), INSERT 때 executor의 `WARN … Missing user context when loading table`(K-16, upstream 26.0.5), complex value INSERT의 SQL 제약(`CONVERT_FROM(json)`은 숫자를 bigint로, `CAST(ROW…)` 미지원, 다중 row VALUES의 untyped NULL). Spark 등 외부 engine이 data file을 쓴 table은 실행하지 않았다 (Polaris API로 만든 table에 Dremio가 쓴 data로 대신).

### 5.3 Write (Agent C, INSTANCE=3)

모든 write는 Polaris `loadTable`(snapshot, schema, partition spec, sort order, property, record/file 수)과 S3 object listing으로 확인했다.

| # | 시나리오 | 결과 | Status |
|---|---|---|---|
| C-1 | `CREATE TABLE` (int, string, double, date) | Polaris schema 일치. TIMESTAMP는 `timestamptz` (Dremio 기본 mapping) | PASS |
| C-2 | `CREATE TABLE … PARTITION BY` (identity / `day()` / `bucket()`) | Polaris spec 정확 | PASS |
| C-3 | `CREATE TABLE … LOCALSORT BY` | sort order 1 asc | PASS |
| C-4 | `CREATE TABLE IF NOT EXISTS`, 중복 CREATE/CTAS | "already exists" (IF NOT EXISTS는 요약, 그 외 오류) | PASS |
| C-5 | CTAS (unpartitioned) | append snapshot 1, record 수 정확 | PASS |
| C-6 | CTAS `PARTITION BY` | **수정 전 FAIL**: COMPLETED이고 file은 partition directory(`0_eu/`, `2_us/`)에 쓰였지만 Polaris spec이 `[{0: unpartitioned}, {1: region}]`, `default-spec-id=0` (사실상 unpartitioned, 이후 INSERT도 unpartitioned). 원인과 수정은 §5.3.1. **수정 후**: spec `[{0: region}]`, default 0, `table_files` partition `{region=eu}`/`{region=us}`, 이후 INSERT도 partitioned, plan에 `ref(region) == "us"` | PASS (수정 후) |
| C-7 | CTAS `LOCALSORT BY` (`PARTITION BY (day(ts), bucket(4,id)) LOCALSORT BY (id)` 포함) | sort order와 spec 유지 | PASS |
| C-8 | 없는 namespace로 CTAS | "A table must be created within a valid folder…" | PASS |
| C-9 | `INSERT` VALUES / SELECT (partitioned 포함) | 각 append snapshot 1, partition directory | PASS |
| C-10 | `ALTER TABLE` ADD COLUMNS / DROP COLUMN / ALTER COLUMN int→bigint / CHANGE COLUMN(rename) / MODIFY | Polaris schema 갱신 (`1:id:long`, `3:amount` …), 기존 row 정상 | PASS |
| C-11 | `ALTER TABLE SET / UNSET TBLPROPERTIES` | Polaris property 반영 | PASS |
| C-12 | `ALTER TABLE ADD / DROP PARTITION FIELD`, `ALTER TABLE LOCALSORT BY` | spec, sort order 갱신 | PASS |
| C-13 | `ALTER TABLE ADD PRIMARY KEY` | `IllegalArgumentException: Cannot add field id as an identifier field: not a required field` (Iceberg는 NOT NULL column 필요) | NOT_SUPPORTED (C-19) |
| C-14 | `UPDATE` / `DELETE` / `MERGE` (partitioned 포함) | copy-on-write, delete file 0, 결과 정확 | PASS |
| C-15 | `TRUNCATE TABLE` | delete snapshot, 0 records | PASS |
| C-16 | `OPTIMIZE TABLE`(기본), `BIN_PACK (MIN_INPUT_FILES=2)`, `REWRITE MANIFESTS` | 4 files → 1, `replace` snapshot, count 불변 | PASS |
| C-17 | `VACUUM TABLE … EXPIRE SNAPSHOTS RETAIN_LAST 1` (OLDER_THAN 없음) | COMPLETED, 기본 5일 기준이라 만료 없음 | PASS (기대 동작) |
| C-18 | `VACUUM … EXPIRE SNAPSHOTS OLDER_THAN <now+1min> RETAIN_LAST 1` | snapshot 7 → 1, data file 4/manifest 7/manifest list 6 삭제, table S3 object 28 → 16 | PASS |
| C-19 | `VACUUM … REMOVE ORPHAN FILES` | "command is not supported" | NOT_SUPPORTED (C-20) |
| C-20 | `DROP TABLE` | Polaris 404, S3 object는 남음 (purge 없음, C-02) | PASS |
| C-21 | 없는 table DROP / table에 DROP VIEW / view에 DROP TABLE | "does not exist" / "is not a VIEW" / "is not a TABLE" | PASS |
| C-22 | `DROP TABLE IF EXISTS` (없는 table) | "Table [...] dropped" (accessor가 `catalog.dropTable`의 `false`를 무시해 handler의 "not found" 분기가 돌지 않는다) | FAIL (minor, C-17) |
| C-23 | 지운 이름으로 다시 CREATE | 성공 | PASS |
| C-24 | 동시 INSERT 4개 × 3 round | 12건 COMPLETED, 12 rows. Polaris log 409 8건(client 자동 재시도) + 200 12건 | PASS |
| C-25 | 동시 UPDATE 2개 + INSERT 1개 | INSERT COMPLETED, UPDATE 둘 다 `CONCURRENT_MODIFICATION ERROR … please retry`, table 정상, 재실행 성공 | PASS |
| C-26 | Partitioned CTAS + namespace 안 explicit `LOCATION` | 수정 후 PASS. namespace 밖 `LOCATION`은 Phase 5에서 재실행하지 않음 (Polaris 정책 C-05) | PASS |

`ROLLBACK TABLE`과 namespace location 밖 `LOCATION`은 Phase 5에서 실행하지 않았다.

#### 5.3.1 Partitioned CTAS 수정 (Phase 5 code 변경)

- 원인: CTAS는 schema만으로 table을 staged create한 뒤 `TableMetadata.newTableMetadata`로 만든 새 metadata를 commit한다. 새 metadata builder는 default spec id 0에서 시작하므로 같은 id 0으로 추가한 partition spec은 `SetDefaultPartitionSpec` 변경을 만들지 않는다. REST commit은 staged 변경(unpartitioned spec, default 지정) 뒤에 Dremio 변경(region spec, default 지정 없음)을 보내고, server는 region spec을 spec 1로 두고 0을 default로 유지한다. Sort order는 id가 1이라 set-default가 전송되어 영향이 없다. Upstream Dremio 26.0.8의 "CTAS unpartitioned" fix(G-26)와 같은 증상이다.
- 수정 (`AbstractRestCatalogAccessor`): `stagedCreateOperations(TableIdentifier, TableMetadata)`가 commit할 metadata의 spec, sort order, location, property로 `buildTable(...).createTransaction()`을 다시 stage한다. `ForbiddenMappingTableOperations.commit`은 `base == null`이고 spec이 partitioned일 때만 다시 stage한 operations로 commit하고 `current()`/`refresh()`도 그 operations를 쓴다. Unpartitioned CTAS를 포함한 다른 commit은 그대로다. 재 stage의 403은 `CommitForbiddenException`(PERMISSION ERROR)으로 매핑한다.
- Unit: `TestRestCatalogNamespaceTableOps` 3건 추가 (`testPartitionedCtasCommitsThroughCreationStagedWithItsSpec`은 원인인 `SetDefaultPartitionSpec` 부재도 assert, `testUnpartitionedCtasCommitsThroughSchemaOnlyStage`, `testPartitionedCtasRestageForbiddenIsPermissionError`).
- Live: C(INSTANCE=3)와 Integration(INSTANCE=7, §5.9).

### 5.4 Namespace / View (Agent C, INSTANCE=3)

| # | 시나리오 | 결과 | Status |
|---|---|---|---|
| D-1 | `CREATE FOLDER` 1–3 level | Polaris에 보이고 `location` 설정됨 | PASS |
| D-2 | `CREATE FOLDER IF NOT EXISTS` / 중복 / 부모 없음 | 명확한 메시지 | PASS |
| D-3 | 비어 있지 않은 `DROP FOLDER` (table 또는 하위 folder) | "cannot be deleted because it is not empty…" | PASS |
| D-4 | 빈 `DROP FOLDER` (bottom-up) / 이미 지운 folder | COMPLETED / "Folder does not exist." | PASS |
| D-5 | `DROP FOLDER IF EXISTS` | parser error | NOT_SUPPORTED (grammar, C-21) |
| D-6 | Source 생성 뒤 Polaris에서 만든/지운 namespace | 약 74초 뒤 보임 (names refresh 60초), Polaris에서 지운 namespace는 사라짐 | PASS |
| D-7 | `CREATE VIEW`, SELECT, 중복 CREATE VIEW | Polaris view dialect `DremioSQL`, format-version 1, metadata는 S3 | PASS |
| D-8 | `CREATE OR REPLACE VIEW` | Polaris version 2개, current 2, 새 SQL/schema | PASS |
| D-9 | `ALTER VIEW … AS` | parser error | NOT_SUPPORTED (grammar, C-21) |
| D-10 | view 위 view + table join, `INFORMATION_SCHEMA` TABLES/VIEWS, catalog의 DATASET/VIRTUAL | 정상 | PASS |
| D-11 | Polaris API view 목록 | `w.v1 w.vv w.vspark w.vmulti` | PASS |
| D-12 | 기반 table을 지운 view | "Error while expanding view polaris.w.vd. Object 'td' not found…" | PASS |
| D-13 | Polaris API로 만든 spark dialect 전용 view | 읽힘, `default-namespace` 반영 | PASS |
| D-14 | dialect 여러 개(spark 먼저, DremioSQL 다음) view | Dremio가 첫 representation(spark)을 씀 (G-25) | PASS (C-10 제약 확인) |
| D-15 | `DROP VIEW` / 다시 / `DROP VIEW IF EXISTS` (catalog `drop-with-purge` true) | COMPLETED / "Unknown view" / "not found." | PASS |
| D-16 | `plugins.restcatalog.views_supported=false` | CREATE/DROP VIEW 차단. 기존 view는 names refresh 2회 이상 뒤에도 목록에 남고 SELECT는 "Views are not supported in this catalog." (전환 직후 첫 SELECT는 성공). RESET으로 복구 | PASS (C-23 제약) |

allowedNamespaces 회귀는 A-4 (lifecycle)와 Phase 3 matrix를 본다.

### 5.5 Cache / Metadata (Agent E, INSTANCE=4)

Source 2개(`polaris` reader, `writer`가 외부 snapshot commit)를 같은 Polaris catalog에 연결했다. 둘 다 `namesRefreshMs=60000`, `PREFETCH_QUERIED`. 변경은 `PUT /api/v3/catalog/{id}`(masked secret), profile은 `GET /apiv2/profiles/{jobId}.json`.

| # | 시나리오 | 결과 | Status |
|---|---|---|---|
| E-1 | `enableAsync=true` | Parquet scan `NUM_ASYNC_STREAMS=1 NUM_ASYNC_READS=1 NUM_ASYNC_BYTES_READ=854`, manifest scan도 async | PASS |
| E-2 | `enableAsync=false` | `NUM_ASYNC_*` 없음, `NUM_IO_READ=57`, `PARQUET_BYTES_READ=854` | PASS |
| E-3 | OSS single node의 C3 cache | 배포본에 `dremio-ce-services-cachemanager`(`CacheFileSystemWrapper`), mount `<data>/cm`, `sys.cache."mount_points"` max 721554505728 / total 1031865892864, `CacheFSController - adding storage-plugin-id polaris` | PASS (기록) |
| E-4 | `isCachingEnabled=false` | 새 CTAS table `tnc` 2회 query, `sys.cache."objects"`에 0. 기존 `t1` object atime 불변 | PASS |
| E-5 | `isCachingEnabled=true` | `tnc` object 4개(`metadata.json`, avro 2, parquet) cache, 다음 query에서 atime 증가 | PASS |
| E-6 | C3 + `enableAsync=false` | `tsync` object 0 → C3는 async reader로만 동작 (UI가 async 해제 시 cache option을 끄는 것과 일치) | PASS (기록) |
| E-7 | `maxCacheSpacePct` 0/101/-5 (PUT), 0/101 (POST), `"abc"` | 모두 400 (`must be between 1 and 100`, `Invalid value found at: maxCacheSpacePct`) | PASS |
| E-8 | `maxCacheSpacePct=50` | PUT 200 `good`. 재시작 전까지 `max_available_space` 그대로, 재시작 후 polaris 360777252864(정확히 50%), writer 721554505728. C3는 plugin limit을 JVM당 한 번 등록 (closed CE cachemanager) | PASS (재시작 후 적용, S-14) |
| E-9 | metadataPolicy update | PUT 200, `/apiv2/source`에 `datasetDefinitionRefreshAfterMillis`/`ExpireAfterMillis` 반영. expire(60000) < refresh(120000)도 허용 | PASS |
| E-10 | metadataPolicy validation (최소값 미만, `datasetUpdateMode=BOGUS`) | 모두 거부, 단 **HTTP 404** (`… must be greater than or equal to 60000.`) | PASS (status code는 K-11) |
| E-11 | names refresh: Polaris에서 만든 table | `polaris.ns2.tp` 40초 뒤 | PASS |
| E-12 | names refresh: Polaris에서 만든 namespace | E-11 직후 첫 poll(0초) | PASS |
| E-13 | Polaris에서 drop한 table (`deleteUnavailableDatasets=true`) | 56초 뒤 tree에서 사라짐, `metadata_refresh.log` "has 1 unavailable datasets to be deleted", SELECT "Object 'tp' not found" | PASS |
| E-14 | 외부 snapshot (기본: validity check 60초, table cache 3초) | `writer` INSERT 후 `polaris` count 300→301, 63초 | PASS |
| E-15 | 수동 refresh (check interval 3600) | 75초 뒤에도 301, `ALTER TABLE … REFRESH METADATA` 후 302 | PASS |
| E-16 | `datasetExpireAfterMs=60000` (check interval 3600) | 54초 뒤 보임 | PASS |
| E-17 | background dataset refresh (`datasetRefreshAfterMs=60000`, expire 3h) | 54초 뒤 보임, `metadata_refresh.log` "Full update for source 'polaris'" | PASS |
| E-18 | `plugins.restcatalog.*` option validation | `table_cache.expire_after_write_seconds` 0/121 → `between 1 and 120`, `catalog.expire_seconds` 0/3601 → `between 1 and 3600`. 기본값 table cache 3초, catalog 1800초, file_system 5분, size_items 10000 | PASS |
| E-19 | `table_cache.expire_after_write_seconds` 기본 3 (check interval 1초) | 외부 INSERT 4초 뒤 보임 | PASS |
| E-20 | `table_cache…=120`, plugin 재시작 전 | 효과 없음 (첫 poll 0초). accessor 생성 시에만 읽음 | PASS (기록, N-14) |
| E-21 | `table_cache…=120`, plugin 재시작 후 (metadata-impacting PUT) | 외부 INSERT가 정확히 120초 stale (t+120초에 306→307) | PASS |
| E-22 | `catalog.expire_seconds=30`, plugin 재시작 후 | Polaris `GET /api/catalog/v1/config` 200이 t+33/65/97초에 12→13→14→15 (약 30초마다 새 REST client). 기본 1800은 약 11분간 6 유지. `closing the catalog` WARN 없음, client 교체 중 query 정상 | PASS |
| E-23 | Dremio 재시작 후 유지 + secret scan | `good`, `enableAsync=true, isCachingEnabled=true, maxCacheSpacePct=50` 유지, C3 object 유지(`approx_file_count=8`). `scan-secrets.sh` 0건 | PASS |

Plugin 재시작 조건 (정정): connection config의 field가 하나라도 바뀐 PUT은 새 plugin을 만들어 시작하고, 새 accessor가 그때의 `plugins.restcatalog.*` option을 읽는다. `ManagedStoragePlugin.replacePlugin`은 `ConnectionConf.equals`(protostuff byte 전체 비교)가 같을 때만 `setLocals`로 재시작을 건너뛰므로, `@NotMetadataImpacting` field(`isCachingEnabled`, `maxCacheSpacePct`, `isUsingVendedCredentials`)만 바꾼 PUT도 재시작한다. `@NotMetadataImpacting`은 names 전체 삭제/refresh 여부만 정한다. 재시작하지 않는 PUT은 `metadataPolicy`만 바꾼 PUT과 같은 config를 다시 보낸 PUT이다. 근거: kernel code, unit `TestRestIcebergCatalogPluginConfig.testCacheSettingChangeRestartsPluginButKeepsMetadata`, A-5(`isUsingVendedCredentials`만 바꾼 PUT 뒤 `start()`의 INFO). E 보고는 cache field만 바꾼 PUT도 재시작하지 않는다(`/v1/config` 6 유지)고 요약했으나 code와 맞지 않아 정정했다. E-20의 "재시작 전 효과 없음"은 option을 바꾼 뒤 plugin이 아직 다시 시작되지 않은 상태의 관찰이다. Cache field만 바꾼 PUT 뒤의 `/v1/config` 횟수는 다시 실측하지 않았다 (Phase 5 main code 변경 없음). `enableAsync` PUT은 재시작한다(E-21, E-22). Profile에 `NUM_CACHE_HITS`/`NUM_CACHE_MISSES`가 나오지 않아 cache 사용은 `sys.cache.*`로 확인했다.

### 5.6 장애 (Agent F, INSTANCE=5)

Polaris token 수명 30초. Fault proxy(`faultproxy.py`)를 source `px` 앞에 두었다 (socket timeout 8000 ms, connection timeout 3000 ms).

| # | 시나리오 | 결과 | Status |
|---|---|---|---|
| F-1 | Polaris down (idle, proxy 중지 → connection refused) | API 400 "The source [px] is currently unavailable … Unable to reach the Iceberg REST catalog at …/api/catalog. Check the endpoint URI and network connectivity. Details: Connect to … failed" (JVM locale 메시지). cached/uncached SELECT 같은 문구. proxy 재시작 후 46초에 `good`, SELECT c=3 | PASS |
| F-2 | Query 실행 중 Polaris 중지 (19초 join, 10M rows, 8초 시점 `docker stop`) | COMPLETED, 결과 정확(320000000). planning 뒤 실행은 catalog가 필요 없다 | PASS |
| F-3 | Polaris container 중지 (idle) | polaris/norefresh/rot/stor: 400 + "Unable to reach … 18231" hint. px: 일반 "Source is not currently available." (proxy 502 재시도가 10초 state check보다 김). `generic` source 영향 없음 | PASS |
| F-4 | Polaris가 down인 채 Dremio 재시작 | 중지 4초, web ready 49초(Polaris up이면 18초), hang 없음. Polaris source 7개 `bad` + 일반 메시지(K-12), `generic` `good`. Polaris 재생성 후 polaris 15초, 전체 약 30초에 `good`. SELECT/INSERT/count(big)=1000000/stor SELECT 성공 | PASS (메시지 gap K-12) |
| F-5 | Storage down (throwaway MinIO 중지) | state `good` 유지(K-04). INSERT 6초에 FAILED "Server error: SdkClientException … Connect to 127.0.0.1:18238 failed … (SDK Attempt Count: 6)" (Polaris 쪽 오류). SELECT는 기본 S3A 재시도로 **399초 이상 hang**, job CANCEL 204였지만 MinIO 복구까지 RUNNING 후 CANCELED. generic fixture SELECT 92초에 FAILED. MinIO 복구 후 SELECT/INSERT/count 성공(재생성 없음). `fs.s3a.attempts.maximum=1`, `fs.s3a.retry.limit=1`, `fs.s3a.connection.establish.timeout=3000`이면 2초에 "IO_EXCEPTION ERROR: Unable to read …metadata.json" | FAIL (known K-15). 장애 해제 후 복구와 완화 설정은 PASS |
| F-6 | 잘못된 OAuth credential (생성) | 400 "Could not connect to badoauth. The Iceberg REST catalog rejected the credentials (OAuth2 error 'unauthorized_client'). Check the 'credential' …", 생성 안 됨. 잘못된 scope: `warn` + 403 LIST_NAMESPACES hint, secret 유지 PUT으로 `good`, SELECT 성공 | PASS |
| F-7 | 실행 중 credential 무효화 (Polaris principal secret 2회 rotate) | Polaris는 1회 rotate 뒤에는 이전 secret도 유효, 2회 뒤 원래 secret 401. 실행 중 source `rot`/`rot2`(`token-exchange-enabled=false`)는 `good`, query 정상 (session이 client secret 없이 token 갱신). Dremio 재시작 뒤에야 API 일반 "Source is not currently available."(hint는 server.log만). `/api/v3/source/{id}` GET+PUT으로 `credential`만 교체 → `good` | PASS (K-12, K-13, X-09) |
| F-8 | 잘못된 S3 secret | `good`. `SELECT count(*)` COMPLETED(snapshot stats, S-13), `SELECT *` "PERMISSION ERROR: Access denied on …metadata.json", INSERT "SYSTEM ERROR: AmazonS3Exception … SignatureDoesNotMatch (403)". 올바른 secret PUT 후 성공 | PASS |
| F-9 | Token 만료 (TTL 30초, 2.3분 idle) | polaris, px, norefresh(`token-refresh-enabled=false`), rot 모두 COMPLETED. proxy에 약 27초마다 token refresh. norefresh는 state check의 client 교체로 복구 (Polaris log namespace 401 1건) | PASS |
| F-10 | 401 주입 (uncached table) | 빠르게 FAILED "The Iceberg REST catalog rejected the credentials of the request to look up [ns1.f401]: Not authorized … Check the 'credential' (or 'token') … retry in a minute." 다음 query 정상 | PASS |
| F-11 | 403 주입 | "…denied the request to look up [ns1.f403]: Forbidden … (for Apache Polaris: the grants …)". cached table: "denied the request to load table [ns1.t1]". 다음 query 정상 | PASS |
| F-12 | 429 주입 (`Retry-After: 1`) | 요청 6회, 약 5.4초 뒤 "Unable to process: injected fault 429" (raw, hint 없음). 다음 query 정상 | FAIL (known C-08: hint 없는 raw 메시지). 복구는 PASS |
| F-13 | 500 / 503 주입 | 500: 재시도 없이 "Server error: InjectedFault: injected fault 500". 503: 6회, 약 5.4초, "Service unavailable: …". 지속 503이면 state 400 + "unavailable (HTTP 503), also after the client retried … 'rest.client.max-retries'" hint, 해제 50초 뒤 `good` | FAIL (known C-08: query 오류는 raw 메시지, state hint는 있음). 복구는 PASS |
| F-14 | Timeout (20초 지연, socket timeout 8초) / connection close | query 8.6초에 "Error occurred while processing GET request" (`SocketTimeoutException: Read timed out`은 server.log만). state "… did not respond in time … 'rest.client.connection-timeout-ms' and 'rest.client.socket-timeout-ms'. Details: Read timed out", 해제 15초 뒤 `good`. connection close: 6회, 같은 일반 query 메시지 | FAIL (known C-08: query 오류에 원인/hint 없음, state hint는 있음). 복구는 PASS |
| F-15 | Secret scan (모든 run의 Dremio log, Polaris/proxy/fixture/MinIO container log; S3/Polaris root/잘못된 S3·OAuth/현재·이전 rotate secret, `Bearer`, `access_token`, `client_secret`, `s3.secret-access-key`, `s3.session-token`, `eyJ`) | 모두 0건 | PASS |

### 5.7 Regression

| 범위 | 명령 / 방법 | 결과 | Status |
|---|---|---|---|
| `plugins/icebergcatalog` module | `scripts/dev test plugins/icebergcatalog` (Integration, Phase 5 최종 tree) | 386 tests, 0 failures, 0 errors, 0 skipped (review 수정 뒤. Integration 최종 tree 385, F: HEAD tree 381, C 수정 포함 384) | PASS |
| `plugins/s3` module | `scripts/dev test plugins/s3` (F) | 112 tests, 0 failures. Phase 5에서 `plugins/s3` 변경 없음 | PASS |
| `sabot/kernel` | `git diff --stat 799ccbda4..HEAD -- sabot/kernel` 비어 있음, Phase 5 변경도 없음 | 실행하지 않음 | N/A (변경 없음, 실행하지 않음. 집계 제외) |
| `dac/backend` source/catalog API | `TestSourceService` 23, `TestSourceResource` 8, `TestSourcesResource` 1, `TestCatalogResource` 46 (F) | 78 tests, 0 failures, 136초 | PASS |
| UI spec (source form 6개, Node 22.10.0) | AddSourceModal, SelectSourceType, EditSourceView, SourceFormJsonPolicy, sourcesMapper, sourceUtils (F) | 103 passing, 9 pending, 0 failing | PASS |
| Generic RESTCATALOG (Polaris 외) | `apache/iceberg-rest-fixture` + throwaway MinIO, `credential`/`scope`/`warehouse` 없음 (F) | `good`. CREATE FOLDER, CREATE TABLE, INSERT 2, SELECT, CTAS(c=2, s=30), UPDATE 1 row, DROP TABLE 모두 COMPLETED. fixture API에 namespace `gns`, table `g1`. Polaris 장애와 격리 | PASS |

### 5.8 UI (Agent G, INSTANCE=6)

Playwright 1.47.0(`dac/ui/node_modules`, headless `chromium_headless_shell-1228`)으로 Prep의 새 UI bundle이 든 배포본을 조작했다. Polaris에 namespace `uins1`, table `t0`을 source 생성 전에 만들었다.

| # | 단계 | 결과 | Status |
|---|---|---|---|
| U-1 | `dremio`/`dremio123` 로그인 | home에 Add Source 버튼 | PASS |
| U-2 | Add Source → Lakehouse Catalogs tile | "Apache Polaris OSS"와 "Iceberg REST Catalog"(둘 다 `data-qa=sources/RESTCATALOG`)가 "Lakehouse Catalogs"와 "Object Storage" 제목 사이 | PASS |
| U-3 | Polaris preset form | 제목 "New Apache Polaris OSS Source". Properties `warehouse=''`, `scope=PRINCIPAL_ROLE:ALL`. Credentials 빈 `credential` row 1개(password input) | PASS |
| U-4 | Preset, 빈 채로 Save | dialog 유지, "Name is required.", "Endpoint URI is required.", "Value is required."(warehouse, credential). source 생성 안 됨 | PASS |
| U-5 | Preset 입력 후 Save | endpoint, warehouse, oauth2-server-uri, MinIO property 8개, credential 3개. REST: `type=RESTCATALOG`, `state=good`, `isUsingVendedCredentials=false`, property 11개, credential 3개 모두 `$DREMIO_EXISTING_VALUE$` | PASS |
| U-6 | UI에 source 표시, namespace 탐색 | 왼쪽 nav "Lakehouse Catalogs" 아래. source 화면에 `uins1`, `/source/uipolaris/folder/uins1`에 `t0` | PASS |
| U-7 | Edit Source: secret masking | 설정 아이콘(`data-qa=settings-button`). credential row 3개, 모두 23자 placeholder password input. page와 input 값에 secret 0건 | PASS |
| U-8 | Edit Source, 변경 없이 Save | Save 활성, 저장됨. tag 변경, `good` 유지, `SELECT COUNT(*)` on `uins1.t0` 성공 | PASS |
| U-9 | Generic tile | 제목 "New Iceberg REST Catalog Source", property/credential row 없음 | PASS |
| U-10 | Generic, endpoint 없음 | "Endpoint URI is required.", dialog 유지 | PASS |
| U-11 | Generic, Catalog Properties에 `fs.s3a.secret.key` | `"fs.s3a.secret.key" is a secret. Move it to Catalog Credentials so that its value is masked.` 저장 차단, source 생성 안 됨 (dummy 값) | PASS |
| U-12 | Generic 입력 후 Save | `good`. `uigeneric` → `uins1` → `t0` | PASS |
| U-13 | HTTP 응답과 page의 secret | text/JSON 응답 150개, 최종 page 모두 0건 | PASS |

추가 (harness): unchanged-save 뒤 `CREATE TABLE uipolaris.uins1.ui_t1` + INSERT 2 rows COMPLETED, 다른 source(`uigeneric.uins1.ui_t1`)로 SELECT 2 rows, MinIO object 6개. `scan-secrets.sh` 중간/최종 0건.

제약: access key 값이 3자라 browser 쪽 scan(U-7, U-13)은 그 값을 건너뛴다 (harness log scan은 포함, 0건). Save 전 Add form의 password input에는 사용자가 입력한 secret이 있다 (사용자 자신의 입력). 오류 dialog의 hint 표시(잘못된 credential)는 UI로 실행하지 않았다 (API 응답의 같은 `errorMessage`는 A-16, F-6). Screenshot 4장: [img/](img/) (`ui-add-source-lakehouse.png`, `ui-polaris-preset-advanced.png`, `ui-edit-source-masked-credentials.png`, `ui-validation-secret-in-properties.png`, secret 없음). **주의**: Advanced Options 3장(`ui-polaris-preset-advanced.png`, `ui-edit-source-masked-credentials.png`, `ui-validation-secret-in-properties.png`)은 Prep 배포본(Phase 4 plugin jar)에서 찍어 U-09 수정 전 Catalog Properties help text(`(no scheme)`, `fs.s3a.requester.pays.enabled=false`, `dremio.bucket.discovery.enabled=false`, `dremio.s3.region=<region>`)가 보인다. 이 text는 틀린 안내다. 현재 text는 `restcatalog-layout.json`이 기준이며 I-1에서 확인했다 (`fs.s3a.endpoint.region=<region>`, us-east-1이 아닐 때). 다시 찍지 않았다.

### 5.9 Integration 재검증 (INSTANCE=7)

Phase 5 code 변경(partitioned CTAS, layout help text)만 다시 확인했다. 배포본 복사본의 `jars/`에만 새 `dremio-icebergcatalog-plugin` jar(`scripts/dev build plugins/icebergcatalog`)를 넣었다.

| # | 시나리오 | 결과 | Status |
|---|---|---|---|
| I-1 | `GET /api/v3/source/type/RESTCATALOG` help text | `fs.s3a.endpoint.region=<region>` 1건, `dremio.s3.region=<region>` 0건 | PASS |
| I-2 | `CREATE TABLE … PARTITION BY (region) AS SELECT` (3 rows) | Polaris `default-spec-id=0`, spec `[{0: region:identity}]`, snapshot 1 | PASS |
| I-3 | 이어서 `INSERT` 1 row | spec 그대로, snapshot 2. `table_files` partition `{region=eu}` 2개, `{region=us}` 1개. group by eu 2 / us 2 | PASS |
| I-4 | `PARTITION BY (day(ts), bucket(4, id)) LOCALSORT BY (id)` CTAS | spec `[{0: ts_day:day, id_bucket:bucket[4]}]`, default sort order 1 (`1 asc`), count 2 | PASS |
| I-5 | Unpartitioned CTAS | spec `[{0: []}]`, sort order 0, SELECT 2 rows | PASS |
| I-6 | 같은 이름으로 partitioned CTAS | "A table or view with given name [polaris.w.c2] already exists." | PASS |
| I-7 | Dremio 재시작 후 읽기, secret scan | `good`, `WHERE region='eu'` count 2. `scan-secrets.sh` total 0 | PASS |

### 5.10 정리

| 항목 | 결과 |
|---|---|
| Container (`p5-*`) 삭제 | A–G, Integration 모두 삭제 (`p5-polaris-e2e-1`…`-7`, `p5-minio-down`, `p5-iceberg-rest`). 남은 `p5-` container 0 |
| Dremio instance 중지, work dir 삭제 | 모두 `dremio-down.sh --purge`, work dir 삭제. Instance port 모두 해제 |
| MinIO prefix object 삭제 | lifecycle 22, read 79, write 238, cache 42, F 29, UI 6, Integration 24 삭제. 각 prefix 남은 object 0. 사용자 MinIO는 중지하지 않았다 |

### 5.11 Phase 5 unit / Local CI

| 범위 | 결과 |
|---|---|
| 신규: `TestRestCatalogNamespaceTableOps` partitioned/unpartitioned CTAS 3건, `TestRestCatalogLayout` help text 1건, `TestRestIcebergCatalogPluginConfig.testCacheSettingChangeRestartsPluginButKeepsMetadata` 1건 (review: cache field만 바꾼 config는 `equals`가 다르고 `equalsIgnoringNotMetadataImpacting`은 같다 → plugin 재시작, metadata 유지) | 5 tests, 0 failures (class 39 / 12 / 29) |
| 수정: `TestRestCatalogHttpErrors.testUntrustedSelfSignedCertificate` (Phase 3 test) | Local CI 1차 실행에서 "handshake was attempted" 실패. fake server가 handshake 실패를 read 중 `SSLException`으로만 셌는데, 부하가 있으면 client 거부가 reset/EOF로 보인다. Server가 `startHandshake()`를 명시적으로 하고 모든 `IOException`을 센다 (test code만). class 30 tests 0 failures |
| `plugins/icebergcatalog` module 전체 | 386 tests, 0 failures, 0 errors, 0 skipped |
| `scripts/dev fmt` / `lint` | clean |
| Harness `sql.sh` shellcheck | clean (B, Local CI `e2e-harness-lint`) |

AbleOps Local CI (`localci run --profile full --no-cache`). Review 수정 뒤 3차 실행(log `target/dev/localci-phase5-rf.log`, wall 2분 16초):

| Step | 결과 |
|---|---|
| `icebergcatalog-test` | SUCCESS (386 tests, 0 failures, 0 skipped) |
| `s3-test` | SUCCESS (112 tests, 0 failures, 0 skipped) |
| `icebergcatalog-lint` | SUCCESS |
| `icebergcatalog-static` (errorprone, forbiddenapis, enforcer) | SUCCESS |
| `ui-source-specs` | SUCCESS (103 passing, 9 pending, eslint 0 errors) |
| `e2e-harness-lint` (license header, shellcheck; `sql.sh` 포함) | SUCCESS |

1차 실행(`target/dev/localci-phase5-run1.log`)은 `icebergcatalog-test`가 `TestRestCatalogHttpErrors.testUntrustedSelfSignedCertificate` 1건으로 FAILED(나머지 step SUCCESS, static SKIPPED, ui CANCELED)였다. 위 표의 test 수정 뒤 2차 실행(`target/dev/localci-phase5.log`, 385 tests)이 모두 SUCCESS였고, review 문서 수정과 config test 1건 추가 뒤 3차 실행도 6개 step 모두 SUCCESS다. 원격 `localci submit` 결과와 SHA는 Lead가 기록한다.

## 6. 전체 요약

| 완료 기준 | 근거 | Status |
|---|---|---|
| RESTCATALOG Source 노출 | Phase 2 #1–2, Phase 5 Prep(`/api/v3/source/type` 25 type 중 RESTCATALOG) | PASS |
| UI Source 생성 | Phase 5 U-1–U-13 (preset, generic, validation, edit masking) | PASS |
| REST API Source 생성 | Phase 2 #3, Phase 5 A-1/A-13/A-15/A-16 | PASS |
| Polaris OAuth2 인증 | Phase 3, Phase 5 F-6/F-7/F-9/F-10 | PASS |
| Namespace / Table 탐색 | Phase 3, Phase 5 B-1–B-6, B-12, D-1–D-6 | PASS |
| SELECT | Phase 5 B-7–B-22 | PASS |
| CREATE TABLE / CTAS | Phase 5 C-1–C-8, C-26 (partitioned CTAS는 수정 후) | PASS |
| INSERT (+UPDATE/DELETE/MERGE) | Phase 5 C-9, C-14, C-24, C-25 | PASS |
| Polaris + S3/MinIO | Phase 4 (MinIO), Phase 5 전 영역. AWS S3는 ENVIRONMENT_BLOCKED | PASS (MinIO) / ENVIRONMENT_BLOCKED (AWS) |
| Source restart / reload | Phase 5 A-6–A-10, A-21, E-23, F-4. update 실패 뒤 background refresh 정지(kernel K-01)는 Phase 6에서 수정 (unit, live P6-6) | PASS |
| allowedNamespaces | Phase 3 matrix, Phase 5 A-4 | PASS (discovery 범위만, N-01) |
| Secret masking | Phase 2–4, Phase 5 A-2/A-23, E-23, F-15, U-7/U-13, I-7, Phase 6 P6-8 (모두 0건) | PASS |
| Generic RESTCATALOG 회귀 없음 | Phase 5 §5.7 (iceberg-rest-fixture E2E, module 386, s3 112, dac/backend 78, UI spec), Phase 6 module 412, s3 112, UI spec 103 (Local CI) | PASS |
| Unit / integration / E2E | Phase 2–6 unit, harness E2E 7개 instance (Phase 5) + release tarball smoke 2회 (Phase 6) | PASS |
| Release packaging | Phase 6 §7.2: `dac/ui` 포함 Maven build, 새 UI bundle 1개, plugin/kernel/s3 jar md5 일치, logback 고정, pom 변경 없음 | PASS |

남은 결함과 제약은 [known-limitations.md](known-limitations.md)에 있다. Phase 6에서 C-08(F-12–F-14), C-17(C-22), K-01(A-18–A-20)을 고쳤다. 남은 known FAIL은 K-15(F-5: 기본 S3A 재시도의 storage down hang, 설정으로 완화), C-04, C-06, C-16, K-11, N-04(upstream)이다. 아래는 Phase 5 시점의 기록이다: Phase 5에서 FAIL로 남은 항목은 kernel K-01(A-18–A-20), 알려진 결함 C-08(F-12–F-14: table lookup/load 경로의 429/5xx/timeout raw 메시지)과 K-15(F-5: 기본 S3A 재시도의 storage down hang, 설정으로 완화), `DROP TABLE IF EXISTS` 메시지(C-22, minor)다. F의 네 건은 장애 해제 뒤 복구는 정상이다.

## 7. Phase 6 — 최종 통합 / Release Readiness

### 7.1 Code 변경과 unit

| 구분 | 변경 | 검증 |
|---|---|---|
| C-08 (plugin) | `RestCatalogExceptionMapper.requestFailed`: dataset lookup, load table/view(cache 여부 무관), storage credential 조회, drop table의 `RESTException`을 CONNECTION ERROR + 원인별 hint로 (unreachable, TLS, timeout, connection reset, 500, 503, 그 밖 status). Server message와 cause는 redact, HTML 제거, 300자 제한 | `TestRestCatalogHttpErrors`(fake HTTP catalog: 500, 429, 503, 502 HTML, read timeout, 연결 끊김, 중지된 catalog, secret masking), `TestRestCatalogNamespaceTableOps` |
| C-08 보완 (Integration) | `BadRequestException`(400)과 `UnprocessableEntityException`(422)도 `RESTException` 하위라 "unexpected HTTP error (not 400 …)"로 잘못 분류되던 것을 VALIDATION ERROR "rejected the request … as invalid (HTTP 400/422)"로. HTML만 있는 server message에서 `asSentence`가 빈 문자열로 실패하던 것을 예외 type 이름으로 | `testLoadTableBadRequestIsValidationError`, `testRequestFailedWithMarkupOnlyMessageNamesTheExceptionType` |
| 401 매핑 확장 (audit M1) | create table, create/drop/replace view(존재 확인 포함), CTAS staging, create/drop folder, drop table, commit(`CommitNotAuthorizedException`, `NotAuthorizedException` 하위라 `CleanableFailure` 유지)의 401을 PERMISSION ERROR + credential hint로 | `testNotAuthorizedOnWriteOperationsIsPermissionError`, `testCommitNotAuthorizedIsPermissionError` |
| C-17 (plugin) | `dropTable`이 `false`(404)와 없는 namespace를 `NoSuchTableException`으로. Plugin은 Dremio catalog에 entry가 없으면 "Table [<path>] not found", 있으면 성공 처리 (외부에서 지운 table 정리, design.md D-16) | `TestRestCatalogNamespaceTableOps` 3, `TestRestIcebergCatalogPlugin` 2 |
| K-01 (kernel) | `ManagedStoragePlugin.replacePlugin`/`replacePluginDeprecated`가 `startReplacementAsync(config, skipEqualityCheck)`. 시작 실패 시 metadata manager는 생성 경로에서만 닫는다 (design.md D-17) | `TestManagedStoragePlugin.testFailedUpdateKeepsMetadataRefreshScheduled`(두 update 경로), `testFailedCreateCancelsMetadataRefresh`. Fix Agent가 수정을 되돌리면 update test가 실패하는 것을 확인 |
| Log redaction (audit M4) | Listing 오류: ERROR + stack trace → WARN 한 줄(redact된 message) + DEBUG stack trace. DEBUG로 넘기는 예외는 `redactedForLogging`(cause chain에 secret이 있으면 redact된 message + 원래 stack trace만) | `testRedactedForLoggingDropsSecretsFromTheCauseChain` |
| Sensitive key 동기화 (audit M6) | backend 규칙을 상수(`SENSITIVE_PROPERTY_KEYS`, `SENSITIVE_KEY_SUBSTRINGS`, `SENSITIVE_KEY_SUFFIXES`)로 정리하고, `dac/ui/src/utils/sourceUtils.ts`의 같은 이름 배열과 비교하는 test 추가 (UI source가 없으면 skip). 규칙 자체는 바꾸지 않았다 (`rest.access-key-id`, bucket별 access key는 known-limitations X-10) | `testSensitiveKeyRuleIsInSyncWithUi` (Local CI에서도 실행, skip 0) |
| 정리 (audit M3, low) | test/Javadoc의 실제 MinIO access key ID를 placeholder로, `CommitForbiddenException.getPermissionError()`(미사용) 삭제, `S3A_CREDENTIALS_PROVIDER` 중복 상수 제거, `closeQuietly` 하나로(`ExpiringCatalogCache`, DEBUG), 중복 WARN 문구 helper로, `G-07:` 주석 tag 제거, `isRecursiveAllowedNamespaces` Javadoc 수정·annotation 위로 이동 | module test |

Audit 항목 중 바꾸지 않은 것과 이유:
- `RestIcebergCatalogPlugin.deleteFolder`의 `!isDeleted` 분기와 `createFolder`의 `ForbiddenException` catch: upstream 26.0.5 code이고 `CatalogAccessor` 계약(다른 구현)을 위한 방어 code라 그대로 둔다.
- `buildCatalogProperties`의 `applyConfigPropertiesToFsConf` 재호출: `createCatalog`를 거치지 않고 `buildCatalogProperties`를 직접 부르는 경로(subclass, test)도 fs conf를 채워야 하므로 유지 (idempotent).
- Backend의 빈 property 값 허용: `fs.s3a.aws.credentials.provider=`(빈 값)이 host identity opt-in이라 유지 (known-limitations X-11).
- UI `SENSITIVE_PROPERTY_KEYS` export, layout placeholder의 Polaris URL: UI/layout 변경은 release UI jar를 다시 만들어야 해서 하지 않았다 (기능 영향 없음).
- Screenshot 3장 재촬영: 하지 않았다 (configuration.md §1 주의 문구 유지).
- `.localci.yaml`의 절대 경로(`DREMIO_NODE_MODULES`, `DREMIO_UI_NODE_DIR`): 로컬 Agent 전용 설정이라 유지.

| 범위 | 결과 |
|---|---|
| `scripts/dev test plugins/icebergcatalog` | 409 tests, 0 failures, 0 errors, 0 skipped (Phase 5 386 + Fix 17 + Integration 6). 최종 review 반영 뒤 412 (§7.5) |
| `scripts/dev test TestManagedStoragePlugin TestPluginsManager` (`sabot/kernel`) | 34 tests, 0 failures (14 + 20) |
| `scripts/dev fmt`, `scripts/dev lint` (`plugins/icebergcatalog`, `sabot/kernel`) | BUILD SUCCESS |
| errorprone / forbiddenapis | icebergcatalog: Local CI `icebergcatalog-static` SUCCESS (Fix 단계에서 `FormatStringAnnotation` 1건 수정). `sabot/kernel`은 errorprone을 실행하지 않았다 (변경은 private method 1개와 호출 2곳) |
| `git diff`와 신규 파일의 MinIO secret | 0건 |

### 7.2 Release build

Release Agent가 `dac/ui`를 `-Ddremio.no-ui` 없이 Maven install(webpack production bundle)한 뒤 `distribution/server`를 build했다. 첫 build에서 `dac/ui/target/classes`에 남은 이전 bundle(`app.fee6e290.js`, Polaris 문자열 없음, `index.html`이 가리키지 않음)이 jar에 같이 들어가 그 file을 지우고 다시 build했다 (`clean`은 쓰지 않음). Integration은 Fix/audit 반영 code로 `sabot/kernel`, `plugins/icebergcatalog`, `distribution/server`를 다시 build했고 UI jar는 Release build의 것을 그대로 썼다.

| 확인 | 결과 | Status |
|---|---|---|
| 최종 tarball | `distribution/server/target/dremio-community-26.0.5-202509091642240013-f5051a07.tar.gz`, 905,759,568 bytes, md5 `410d54ef74edf36e4a4a48370da04138` | PASS |
| UI jar (`jars/dremio-dac-ui-*.jar`) | md5 `4312b20c2a570e557ff679eb83a02a7d` (Release build와 같음). `static/js/app.*.js`는 `app.a4aa0288.js` 하나, `index.html`이 가리키는 file, "Apache Polaris OSS" 포함 | PASS |
| icebergcatalog plugin jar | md5 `769370ad16ba6b3403c2d0b65a5617e6` = `~/.m2`. `RestCatalogExceptionMapper.requestFailed`/`redactedForLogging`, `CommitNotAuthorizedException` class 포함 | PASS |
| kernel jar | md5 `95c5243e35a28f0dbdc829c5f912eb46` = `~/.m2`. `ManagedStoragePlugin.startReplacementAsync` 포함 | PASS |
| s3 plugin jar | Release build에서 확인: endpoint scheme 수정 포함, md5 = module/`~/.m2` (Phase 6에서 `plugins/s3` 변경 없음) | PASS |
| `conf/logback.xml` | HEAD와 같음. HttpClient 4/5 wire·headers, SigV4 signer 2개, AWS4Signer, Netty logger 2개 INFO 고정 | PASS |
| 중복 RESTCATALOG class / prototype jar | 없음 (`RestIcebergCatalogPlugin`과 config는 icebergcatalog jar에만) | PASS |
| Maven dependency | `git diff 799ccbda4 -- '*pom.xml'` 비어 있음. 새 third-party dependency/license 없음 | PASS |

### 7.3 Release smoke와 최종 smoke

Release Agent: Fix 반영 전 tarball(HEAD=P5 code)을 새로 풀어 INSTANCE=2 `smoke.sh` 12/12 PASS, 서빙된 UI(`GET /` → `app.a4aa0288.js`, "Apache Polaris OSS") PASS, secret scan 0, 정리 완료 (158초).

Integration 최종 검증 (INSTANCE=3, 최종 tarball을 scratchpad에 새로 풀어 실행, container prefix `p6-`, S3 prefix `polaris-p6-final`, names refresh 60초):

| # | 시나리오 | 결과 | Status |
|---|---|---|---|
| P6-1 | `smoke.sh` (source 생성 `good`, Polaris API로 만든 namespace/table이 Dremio에 보임, SELECT, CREATE TABLE, INSERT, SELECT 2 rows, Polaris가 t1을 나열, DROP TABLE, secret scan) | 12/12 PASS, 157초 | PASS |
| P6-2 | 서빙된 UI | `GET /` → `/static/js/app.a4aa0288.js`, "Apache Polaris OSS" 포함 | PASS |
| P6-3 | `DROP TABLE IF EXISTS polaris.smoke.nosuch` (C-17) | COMPLETED, "Table [polaris.smoke.nosuch] not found." (Phase 5 C-22는 "dropped") | PASS |
| P6-4 | `DROP TABLE polaris.smoke.nosuch` | FAILED "Table [polaris.smoke.nosuch] does not exist." (filesystem source와 같은 handler 오류. 검사 script의 문구 기대값("not found")이 틀려 script는 FAIL로 찍었고, 결과는 확인 후 PASS로 판정) | PASS |
| P6-5 | Polaris container 중지 직후 `SELECT * FROM polaris.smoke.polaris_t` (C-08) | FAILED "Unable to reach the Iceberg REST catalog for the request to load table [smoke.polaris_t]: Error occurred while processing GET request (Connect to http://127.0.0.1:18211 … failed: …). Check the endpoint URI, the network connectivity and that the catalog service is up, then retry the query." (Phase 5 F-12–F-14는 raw 메시지) | PASS |
| P6-6 | 잘못된 credential로 source update 후 background refresh (K-01) | update HTTP 400 + OAuth2 hint, state `good` 유지. 이후 Polaris에 만든 namespace `k01ns`가 **40초** 뒤 names refresh로 Dremio에 보임 (Phase 5 A-18–A-20은 재시작 전까지 안 보임) | PASS |
| P6-7 | `ROLLBACK TABLE … TO SNAPSHOT` (C-25) | INSERT 2회(snapshot 2개) 뒤 첫 snapshot으로 "Table [polaris.smoke.rb] rollbacked", SELECT 1 row | PASS |
| P6-8 | Secret scan (Dremio log 전체, Polaris container log: S3 secret, Polaris root secret, `Bearer`, `access_token`, `client_secret`, `s3.secret-access-key`, `s3.session-token`) | total 0. Build/smoke/Local CI log의 MinIO secret 0건 | PASS |

정리: source 삭제(204), Dremio 중지와 work dir 삭제, `p6-polaris-e2e-3` 삭제, `s3://dremiodev/polaris-p6-final/` object 16개 삭제(남은 0), 압축 해제 디렉터리 삭제. 남은 `p6-` container 0, INSTANCE=3 port 모두 해제. 사용자 MinIO는 중지하지 않았다.

### 7.4 AbleOps Local CI

`localci run --profile full --no-cache` (local mode, Phase 6 최종 review 반영 code, log `target/dev/localci-phase6-final.log`, wall 2분 21초). 그 전 실행(`target/dev/localci-phase6.log`, 409 tests, wall 2분 16초)도 모두 SUCCESS였다. 원격 `localci submit` 결과와 SHA는 Lead가 기록한다.

| 구분 | Step | 결과 |
|---|---|---|
| 컴파일 + 테스트 | `icebergcatalog-test` | SUCCESS (412 tests, 0 failures, 0 skipped) |
| 컴파일 + 테스트 | `s3-test` | SUCCESS (112 tests, 0 failures, 0 skipped) |
| lint | `icebergcatalog-lint` (spotless, license, checkstyle; icebergcatalog + s3) | SUCCESS (0 violations) |
| 후속: 정적 분석 | `icebergcatalog-static` (errorprone, forbiddenapis, enforcer) | SUCCESS |
| 후속: UI | `ui-source-specs` | SUCCESS (103 passing, 9 pending, eslint 0 errors) |
| 후속: harness | `e2e-harness-lint` | SUCCESS |

Local CI pipeline은 `sabot/kernel`을 build/test하지 않는다. K-01 kernel 변경은 위 §7.1의 module test와 최종 tarball live(P6-6)로 확인했다.

### 7.5 최종 review 반영

최종 review finding 4건을 확인하고 모두 반영했다.

| Finding | 확인 | 반영 |
|---|---|---|
| DDL 경로의 403/401 외 `RESTException`이 매핑·redact되지 않는데 design.md §5.2/security.md는 commit과 `updateFolder`만 남았다고 적음 | 맞음: create table, CTAS staging, create/drop view, view replace, create/drop folder는 403/401만 잡았다 | Code 수정: 위 경로에 `catch (RESTException e) → requestFailed`. Drop folder는 400 "not empty" 분기가 먼저이고, 그 밖 400은 raw 재throw 대신 `requestFailed`. 문서(design.md §5.2, security.md, known-limitations C-08)를 code에 맞췄다 |
| `DROP TABLE IF EXISTS`가 catalog의 400/422 거부를 "not found" 성공으로 보고 | 맞음: `requestFailed`의 400/422는 VALIDATION ERROR이고 `DropTableHandler`가 `IF EXISTS`에서 VALIDATION ERROR를 성공으로 바꾼다 | `RestCatalogExceptionMapper.dropFailed`: drop table의 400/422는 같은 메시지의 UNSUPPORTED_OPERATION ERROR. 다른 drop(view, folder)의 handler는 VALIDATION ERROR를 삼키지 않아 그대로 둔다 |
| Status 값이 spec의 네 값(PASS/FAIL/ENVIRONMENT_BLOCKED/NOT_SUPPORTED) 밖 (`미검증 (범위 밖)`, `기록`, `PASS (수정됨)`) | 맞음 | compatibility-matrix, known-limitations, security.md의 결과 열을 네 값으로 바꿨다. 실행하지 않은 항목은 `NOT_SUPPORTED (검증하지 않음: <이유>)`, 환경이 없던 항목은 `ENVIRONMENT_BLOCKED`. 괄호는 비고로만 쓴다고 정의했다 |
| C-07이 401/403도 HTTP status를 표기한다고 적음 | 맞음: 401/403 메시지에는 숫자가 없다 (Phase 3 문구로 구분) | known-limitations C-07 수정 |

| 범위 | 결과 |
|---|---|
| 신규/수정 unit (`TestRestCatalogNamespaceTableOps`) | `testRequestFailuresOnWriteOperationsAreMapped`(7개 DDL 경로의 500 → CONNECTION ERROR + action), `testCreateTableBadRequestIsValidationError`, `testDropTableRejectedRequestIsNotValidationError`(400/422 → UNSUPPORTED_OPERATION), `testDropFolderOtherBadRequestIsValidationError`(이전 `…IsRethrown`) |
| `scripts/dev test plugins/icebergcatalog` | 412 tests, 0 failures, 0 errors, 0 skipped |
| `scripts/dev fmt`, `scripts/dev lint` | BUILD SUCCESS (0 Checkstyle violations) |
| Local CI `full --no-cache` | 6 step 모두 SUCCESS (§7.4) |

Live 재확인 (INSTANCE=4, 최종 tarball 복사본에 새로 build한 icebergcatalog jar만 교체, container prefix `p6-`, S3 prefix `polaris-p6-final`):

| # | 시나리오 | 결과 | Status |
|---|---|---|---|
| P6-9 | DDL 회귀: CREATE FOLDER, CREATE TABLE, INSERT, SELECT, CREATE VIEW, SELECT view, DROP VIEW, DROP TABLE, `DROP TABLE IF EXISTS` (없는 table), DROP FOLDER | 10/10 COMPLETED. `IF EXISTS`는 "Table [polaris.fin.nosuch] not found." | PASS |
| P6-10 | Polaris container 중지 직후 `CREATE FOLDER polaris.fin2` | FAILED "Unable to reach the Iceberg REST catalog for the request to create folder [fin2]: Error occurred while processing POST request (Connect to http://127.0.0.1:18221 … failed: …). Check the endpoint URI, the network connectivity and that the catalog service is up, then retry the query." (수정 전에는 raw `RESTException`) | PASS |
| P6-11 | Secret scan (Dremio log, Polaris container log) | total 0 | PASS |

정리: source 삭제(204), Dremio 중지와 work dir 삭제, `p6-polaris-e2e-4` 삭제, `s3://dremiodev/polaris-p6-final/` object 6개 삭제(남은 0). 남은 `p6-` container 0, INSTANCE=4 port 해제. 사용자 MinIO는 중지하지 않았다. 400/422 거부는 Polaris에서 재현할 DDL 경로가 없어 unit으로만 확인했다.
