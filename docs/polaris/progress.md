# Polaris RESTCATALOG 진행 현황

- Branch: `feature/polaris-restcatalog` (base: `799ccbda4 Release 26.0.5`)
- 작업 정의: [`docs/reference_docs/catalog-support/polaris-catalog-support.md`](../reference_docs/catalog-support/polaris-catalog-support.md)
- 상태 값: `PASS` / `FAIL` / `ENVIRONMENT_BLOCKED` / `NOT_SUPPORTED`. 진행 중인 항목은 `IN_PROGRESS`, 시작하지 않은 항목은 `NOT_STARTED`로 표시한다.
- 최종 갱신: 2026-10-03 (Phase 6 Integration)
- 문서 목록: [README.md](README.md)

## Phase 현황

| Phase | 내용 | 상태 | 주요 산출물 | Commit SHA |
|---|---|---|---|---|
| 1 | 구조 분석 / Gap Analysis | PASS | [phase1-analysis.md](phase1-analysis.md), [compatibility-matrix.md](compatibility-matrix.md), progress.md | `67cf10e4c` |
| 2 | RESTCATALOG OSS Source 완성 | PASS | `@SourceType`, `isUsingVendedCredentials`(tag 13), `restcatalog-layout.json`, Polaris preset, registration/round-trip test, live gate (아래 §Phase 2) | `f2553db4a` (후속 CI/log fix `ffd1cc1b2`) |
| 3 | Polaris OAuth2 / Catalog 연동 | PASS | G-09/G-10/G-11/G-06 해결, 403/401 매핑, allowedNamespaces folder bug 수정, E2E harness `scripts/polaris-e2e`, [phase3-oauth-catalog.md](phase3-oauth-catalog.md), [security.md](security.md) | `3b85d3619` |
| 4 | Object Storage | PASS | 공식 static baseline MinIO PASS (AWS는 ENVIRONMENT_BLOCKED), MinIO matrix, provider 생략/endpoint scheme 수정, vended credential runtime (G-04), HttpClient 4·Netty logger 고정, review 13건 반영, [storage.md](storage.md), [security.md](security.md) §7 | `691f2b5a9` |
| 5 | 실제 E2E / Regression | PASS | Dremio → Polaris → MinIO E2E 7개 instance (lifecycle, read, write, namespace/view, cache/metadata, 장애, regression, UI): PASS 139 / FAIL 8 (kernel K-01 3, known C-08 3, known K-15 1, minor 1) / NOT_SUPPORTED 5. partitioned CTAS 수정, layout help text 수정, harness `sql.sh` 수정, [test-results.md §5](test-results.md#5-phase-5--실제-e2e--regression), [known-limitations.md](known-limitations.md) | `6d2f6f4e9` |
| 6 | 최종 통합 / 문서 / Release Readiness | PASS | C-08/C-17/K-01 수정, audit 반영(401 매핑 확장, 400/422 분류, log redaction, sensitive key 동기화 test, 중복 정리), release tarball(`dac/ui` 포함), 최종 smoke INSTANCE=3 PASS, 최종 review 반영(DDL 경로 `RESTException` 매핑, drop table 400/422 UNSUPPORTED_OPERATION, status 값 정리, live INSTANCE=4 재확인), Local CI full SUCCESS, 문서 7종 + [README.md](README.md) 완성 ([test-results.md §7](test-results.md#7-phase-6--최종-통합--release-readiness)) | `b2bf2e0cc` |

## Phase 1 — 공통 완료 Gate

```text
[x] 병렬 Agent 작업 완료        (A Backend, B Source 등록, C 공식 설정, D UI, E Polaris; 각각 Verifier 재검증)
[x] Integration 완료            (Verifier 정정 반영, Gap G-01..G-28 통합)
[x] 신규 테스트 통과            (N/A: 분석 Phase, repo 코드 변경 없음)
[x] 관련 regression 통과        (N/A: repo 코드 변경 없음)
[x] secret/log 검토             (문서의 secret은 placeholder 또는 로컬 테스트 전용 값으로 명시)
[x] git diff/status 검토        (Lead)
[x] 문서 갱신                   (docs/polaris/ 3개 파일)
[x] commit 완료                 (67cf10e4c)
[x] origin push 완료            (origin/feature/polaris-restcatalog)
[x] commit SHA 기록             (67cf10e4c)
```

Phase 1 결과:
- 분석: **PASS**
- Live probe: Polaris 1.1.0 client-probe와 Dremio tarball prototype 모두 **PASS**
- Dremio 내부 data path(S3/DremioFileIO) E2E: **TBD (Phase 4/5)**

## Phase 2 — RESTCATALOG OSS Source 완성

### 공통 완료 Gate

```text
[x] 병렬 Agent 작업 완료        (A Backend 등록, B Config/API, C UI, D Test)
[x] Integration 완료            (contract 일치 확인, checkstyle/errorprone 지적 2건 수정)
[x] 신규 테스트 통과            (Phase 2 test 4개 class: 89 tests, 0 failures, 리뷰 수정 반영 후)
[x] 관련 regression 통과        (icebergcatalog module 전체: 182 tests, UI spec 103 passing, 0 failures)
[x] AbleOps Local CI             (`localci run --profile full`: test/lint/static/ui-source-specs 모두 SUCCESS)
[x] secret/log 검토             (live server.log/server.out/json log grep 0건, 문서는 placeholder만 사용)
[x] git diff/status 검토        (Integration Agent)
[x] 문서 갱신                   (progress.md, compatibility-matrix.md)
[x] commit 완료                 (Lead)
[x] origin push 완료            (Lead)
[x] commit SHA 기록             (Phase 현황 표 참고)
```

### AbleOps Local CI

파이프라인은 저장소 루트 [`.localci.yaml`](../../.localci.yaml)에 있다 (profile `fast`: test+lint, `full`: +static, ui-source-specs).

| Step | 내용 | 결과 |
|---|---|---|
| `icebergcatalog-test` | `plugins/icebergcatalog` compile + 전체 test (JDK 11) | SUCCESS (182 tests, 0 failures) |
| `icebergcatalog-lint` | spotless, license, checkstyle | SUCCESS |
| `icebergcatalog-static` | `verify -DskipTests`: errorprone, forbiddenapis, enforcer | SUCCESS |
| `ui-source-specs` | source form Mocha spec 6개 + eslint + prettier (Node 22) | SUCCESS (103 passing, 9 pending) |

- 원격 제출(`localci submit`)은 서버 큐에 들어가지만 Agent에 할당되지 않아 `QUEUED`에 머물렀다 (run `7213d373`, 취소). AbleOps CI 서버 쪽 이슈로 보고, 같은 파이프라인을 `localci run`(로컬 실행)으로 검증했다.
- `localci` 압축본은 32 MiB / 10000 entry 제한이 있고 ignore된 `target/` 디렉터리까지 포함하므로 `.localci.yaml`의 `archive_exclude`로 제외한다.
- CI 실행 중 eslint가 `AddSourceModal.jsx`의 기존 `promise/no-return-wrap` 오류를 잡아 `return "done"`으로 수정했다 (동작 동일).

### Gate 세부 (phase1-analysis.md §7.4)

검증 환경:
- `scripts/dev build distribution/server`로 만든 tarball을 scratchpad에 복사해 실행했다. JDK 17, 단일 노드(coordinator+executor), 사용자 `dremio`.
- Polaris `apache/polaris:1.1.0-incubating` (`p2-polaris`, `--network host`, 8181/8182).
  - S3 storage는 로컬 MinIO(`http://127.0.0.1:9000`, bucket `dremiodev`, prefix `s3://dremiodev/polaris-p2/`)를 사용했다.
  - catalog `p2cat`에 `polaris.config.drop-with-purge.enabled=true`를 설정했다.
  - catalog role `p2_admin`에 `CATALOG_MANAGE_CONTENT`를 부여하고 `service_admin`에 연결했다.
- Source JSON은 phase1-analysis.md §5.3 (5)의 형태다 (`warehouse=p2cat`, `scope=PRINCIPAL_ROLE:ALL`, MinIO `fs.s3a.*`/`dremio.*`, secret `credential`/`fs.s3a.access.key`/`fs.s3a.secret.key`, `isUsingVendedCredentials=false`).

| # | 항목 | 결과 (HTTP) | Status |
|---|---|---|---|
| 1 | `GET /api/v3/source/type`에 RESTCATALOG 존재 | 200, `{"sourceType":"RESTCATALOG","label":"Iceberg REST Catalog"}` | PASS |
| 2 | `GET /api/v3/source/type/RESTCATALOG`에 uiConfig 포함 | 200. `uiConfig.sourceType=RESTCATALOG`, `metadataRefresh={datasetDiscovery:true, authorization:false}`, tab `General`/`Advanced Options`, element 9개 (`isUsingVendedCredentials` 포함) | PASS |
| 2b | `GET /apiv2/source/type/RESTCATALOG` | 404. 26.x에서 source type API는 `/api/v3`에만 있다 (UI도 v3를 사용) | NOT_SUPPORTED (해당 endpoint 없음) |
| 3 | 공식 문서 JSON으로 `POST /api/v3/catalog` | 200, state `good` | PASS |
| 4 | GET 응답 masking (`GET /api/v3/catalog/{id}`, `GET /apiv2/source/polaris`) | 둘 다 200. secret 3개 모두 `$DREMIO_EXISTING_VALUE$`. 응답에서 secret 원문 0건 | PASS |
| 5 | masked 값 그대로 `PUT /api/v3/catalog/{id}` | 200, state `good`. 이후 namespace 조회도 정상 → credential 유지 | PASS |
| 6 | Polaris(MinIO) 대상 state `good` | `good` | PASS |
| 7 | Polaris에서 만든 namespace `p2ns`가 Dremio에 보임 | `GET /api/v3/catalog/by-path/polaris` → 200, children `[["polaris","p2ns"]]` | PASS |
| 8 | `restEndpointUri` 누락 | 400 `config.restEndpointUri` validation error (`@NotBlank`) | PASS |
| 9 | 잘못된 credential / warehouse 누락 | 둘 다 400 `Could not connect to <name>, check your connection information and credentials`. server.log에는 hint가 남는다: `(HTTP 400) … 'warehouse' … 'scope' … 'credential'. Details: Malformed request: unauthorized_client` / `… Please specify a warehouse`. 응답과 로그에 secret 없음 | PASS (hint가 API 응답에는 노출되지 않음 → Phase 3) |
| 10 | `isUsingVendedCredentials=true` source | 200, state `good`. INFO 로그 `requests vended credentials (header.X-Iceberg-Access-Delegation=vended-credentials)`. SELECT 성공 (static `fs.s3a.*`로 읽음) | PASS (flag). runtime 사용은 NOT_SUPPORTED (G-04, Phase 4) |
| 11 | Bonus: SQL API `CREATE TABLE polaris.p2ns.t1` / `INSERT` 2 rows / `SELECT` | 모두 `COMPLETED`. SELECT 결과 2 rows. MinIO에 parquet 1개, metadata.json 2개, avro 2개가 생성됨 | PASS (smoke, Phase 5에서 정식 검증) |
| 12 | Bonus: Dremio 재시작 후 state와 `SELECT count(*)` | `good`, `c=2`. 이후 `DROP TABLE` `COMPLETED` | PASS (smoke) |
| 13 | 로그 secret 노출 (server.log, server.out, json log, queries.json 등 log/ 전체) | MinIO secret, Polaris secret, 잘못된 secret, `Bearer`, `access_token`, vended `s3.secret-access-key`/`s3.session-token`: 모두 0건 | PASS |
| 14 | §7.1 D test (`TestRestCatalogSourceRegistration` 12, `TestRestCatalogLayout` 11, `TestRestIcebergCatalogPluginConfig` 28, `TestRestIcebergCatalogPlugin` 38) | 89 tests, 0 failures (리뷰 수정 후 재실행) | PASS |
| 15 | icebergcatalog module regression | 182 tests, 0 failures, 0 skipped (리뷰 수정 후 재실행) | PASS |
| 16 | dac/backend source-type test | dac/backend는 icebergcatalog에 의존하지 않아 영향받는 test가 없다 (dac/daemon만 의존). live #1, #2로 대체 | PASS (N/A) |
| 17 | UI spec (6개 spec: sourceUtils, SourceFormJsonPolicy, sourcesMapper, SelectSourceType, AddSourceModal, EditSourceView) | 103 passing, 9 pending (기존 skip block), 0 failing (리뷰 수정 후 재실행). prettier clean, eslint 신규 문제 없음 (`AddSourceModal.jsx` `promise/no-return-wrap` 1건은 HEAD부터 존재) | PASS |
| 18 | `scripts/dev fmt` / `scripts/dev lint` | clean (checkstyle `VisibilityModifier` 3건을 수정한 뒤). errorprone(`ReturnValueIgnored` 1건 수정)과 forbiddenapis도 module 단위로 clean | PASS |
| 19 | UI에서 브라우저로 source 생성 | 미실행. 새 UI bundle(preset 포함)이 서빙되는 것까지만 확인 | TBD (Phase 3) |

정리: Polaris container 삭제, Dremio 중지, `s3://dremiodev/polaris-p2/` 아래 생성 object 삭제.

### Phase 2에서 처리한 Gap

| Gap | 처리 |
|---|---|
| G-01, G-18 | `@SourceType(value="RESTCATALOG", label="Iceberg REST Catalog", uiConfig="restcatalog-layout.json")`. UI 상수 label도 변경 |
| G-02 | `restcatalog-layout.json` 추가 (General / Advanced Options, help text에 Polaris 필수 key와 MinIO key) |
| G-03 | `@Tag(13) isUsingVendedCredentials = false`. 25.2.0 bytes 호환 test 포함 |
| G-04 (flag) | true면 catalog props에만 `header.X-Iceberg-Access-Delegation=vended-credentials` (사용자 값 우선). runtime 사용은 Phase 4 |
| G-07 | `createCatalog`와 `getFsConfCopy()`에서 property를 fs conf로 eager copy. REST client 전용 key(`credential`, `token`, `scope`, `oauth2-server-uri`, `audience`, `resource`, `token-*`, `header.*`, `rest.auth.*`, token-exchange type key)는 복사하지 않는다 |
| G-09 (일부) | `getState()`가 오류 종류별 hint와 secret redaction을 붙인다. redaction은 긴 값부터 치환하고, 4자 미만 값은 건너뛰며, 8자 미만 값은 앞뒤가 영숫자가 아닐 때만 치환한다 (예: 5자 access key `minio`가 bucket `myminio`를 `my****`로 망가뜨리지 않음). API 응답 노출과 다른 경로의 매핑은 Phase 3 |
| G-13 | UI validator가 `propertyList`의 secret key를 막는다. 판정 규칙은 backend `isSensitivePropertyKey`와 같다 (정확한 key 목록 + `secret`/`password`/`account.key`/`private.key`/`private-key`/`sas-token` 포함, `.token`/`-token`/`_token` 접미사). Edit에서는 저장된 source에 이미 있던 key는 막지 않고 새로 추가하거나 이름을 바꾼 row만 막는다. backend는 key 이름만 WARN |
| G-14 (backend) | null 원소, 빈 이름, null 값을 건너뛴다. UI의 rename 시 값 비우기는 미완 |
| G-15 | UI preset "Apache Polaris OSS" (저장 type은 RESTCATALOG) |
| G-16 | registration, layout, round-trip, property 전달, fs conf, secret test |
| G-17 | `restEndpointUri`에 `@NotBlank` |
| (리뷰) | `isUsingVendedCredentials`에 `@NotMetadataImpacting`. 켜고 끄는 것만으로 dataset metadata가 삭제/재수집되지 않는다 (`equalsIgnoringNotMetadataImpacting` unit test) |
| G-12 (일부) | REST auth key가 Hadoop conf(`getFsConfCopy`, executor, `DatasetFileSystemCache`)에 더 이상 복사되지 않는다. at-rest 평문은 남음 |
| G-19 | `metadataRefresh?.`, safe errorHandler. `EditSourceView`의 undefined `errorMessage`도 수정 |

### Phase 3 이후로 넘기는 항목

- 잘못된 credential / warehouse 누락 시 API 응답은 일반 메시지뿐이다. hint는 server.log의 Suggested User Action에만 있다 (G-09).
- `createCatalog`의 catch는 여전히 dead code다. `checkState` 외 경로의 오류는 매핑되지 않는다 (G-09). `checkState`는 매번 새 catalog를 만든다 (G-11).
- UI: secret key 이름을 바꿀 때 값 비우기 (G-14), `location.state.selectedSourceType` 직접 진입 시 preset 미적용.
- `propertyList`에 secret key가 이미 있는 기존 source는 Edit에서 막지 않는다 (backend WARN만). 사용자가 Catalog Credentials로 옮기도록 안내하는 UI 경고는 아직 없다.
- 리뷰 수정(REST auth key 비복사, redaction 규칙, `@NotMetadataImpacting`, UI 규칙)은 unit/UI spec과 module build로 확인했다. tarball live gate는 수정 전 build로 실행했으며 재실행하지 않았다.
- source 시작 실패 시 `DatasetFileSystemCache.close`에서 NPE WARN (`this.cache` null). 기존 동작이며 기능 영향은 없다.
- Polaris 권한 관찰: `CATALOG_MANAGE_CONTENT`만 부여한 상태에서도 `loadTable` + vended header가 200과 `s3.*` credential을 반환했다. phase1-analysis.md §5.3 (4)의 "delegation 권한 없음" 설명은 Phase 4에서 다시 확인한다.
- `distribution/server/target`의 tarball에 들어간 `dremio-dac-ui` jar는 Phase 2 UI 변경 이전 build다 (`dac/ui` Maven build는 하지 않음). Live gate는 `corepack pnpm run build`로 만든 bundle을 scratchpad 복사본의 jar에만 넣어 확인했다. 배포용 tarball은 `dac/ui`를 다시 build해야 한다.

## Phase 3 — Polaris OAuth2 / Catalog 연동

상세 결과, 코드 변경, 재현 방법: [phase3-oauth-catalog.md](phase3-oauth-catalog.md). 보안: [security.md](security.md).

### 공통 완료 Gate

```text
[x] 병렬 Agent 작업 완료        (Harness, A OAuth2, B Namespace/Table, C Allowed Namespace, D 오류/보안)
[x] Integration 완료            (A/B 겹침 정리, C B-1/B-3/B-4와 D P-1/P-3 반영, live에서 발견한 folder listing 순서 bug 수정)
[x] 신규 테스트 통과            (신규/변경 test class 8개: 251 tests, 0 failures, 0 skipped; Review 반영 후)
[x] 관련 regression 통과        (icebergcatalog module 전체: 319 tests, 0 failures, 0 skipped; UI spec 103 passing)
[x] AbleOps Local CI             (`localci run --profile full --no-cache`: 5개 step 모두 SUCCESS, 아래 표)
[x] secret/log 검토             (unit echo test, 6개 instance live scan 0건 (Review 반영 INSTANCE=6 포함), `git diff` 전체에 MinIO secret 0건)
[x] git diff/status 검토        (Integration Agent)
[x] 문서 갱신                   (phase3-oauth-catalog.md, security.md 신규, progress.md, compatibility-matrix.md; phase3-security-notes.md는 security.md로 병합 후 삭제)
[x] commit 완료                 (Lead, 3b85d3619)
[x] origin push 완료            (Lead)
[x] commit SHA 기록             (3b85d3619)
```

### AbleOps Local CI

| Step | 내용 | 결과 |
|---|---|---|
| `icebergcatalog-test` | `plugins/icebergcatalog` compile + 전체 test (JDK 11) | SUCCESS (319 tests, 0 failures. Review 반영 후 재실행) |
| `icebergcatalog-lint` | spotless, license, checkstyle | SUCCESS |
| `icebergcatalog-static` | `verify -DskipTests`: errorprone, forbiddenapis, enforcer. Phase 3부터 source를 `touch`해 errorprone이 실제로 돈다 | SUCCESS |
| `ui-source-specs` | source form Mocha spec 6개 + eslint + prettier | SUCCESS (103 passing, 9 pending) |
| `e2e-harness-lint` (Phase 3 신규) | `scripts/polaris-e2e/*.sh` license header + shellcheck | SUCCESS |

- 원격 `localci submit`은 여전히 QUEUED 문제가 있어 local mode로 실행했다. Log는 `target/dev/localci-phase3.log`.
- Phase 2의 static step은 앞 step이 이미 compile해 둔 상태라 errorprone이 돌지 않았다. 그 사이 들어온 `TestRestIcebergCatalogPluginConfig`의 `Slf4jIllegalPassedClass` 1건을 고쳤다.

### Gate 세부 (Integration 재검증, INSTANCE=5)

| 항목 | 결과 | Status |
|---|---|---|
| Source 생성 | 200, `good` | PASS |
| 잘못된 credential / warehouse 누락 / 닿지 않는 endpoint / 잘못된 scope | 400 응답 `errorMessage`에 원인 hint와 redact된 detail (G-09) | PASS |
| 403 principal | namespace listing 403이면 `warn`. read-only는 SELECT 성공, CREATE/INSERT/CREATE FOLDER는 permission error, REST v3 folder 403 | PASS |
| Token 만료/갱신 (TTL 60초) | 6분 SELECT 12회 성공, 새 config 요청 0, 401 0 (G-11) | PASS |
| Polaris down/up | pause → unavailable → 15초 내 복구. stop → `bad` + "Unable to reach" hint. 재생성(새 key) → client 교체 후 45초 내 `good` | PASS |
| Namespace/table/view 동작 | CREATE FOLDER/TABLE, CTAS, INSERT/UPDATE/DELETE, view CREATE/SELECT/DROP | PASS |
| 비어 있지 않은 namespace DROP | "Folder [..] cannot be deleted because it is not empty…" (G-10) | PASS |
| DROP VIEW (`drop-with-purge` false) | permission error + `polaris.config.drop-with-purge.enabled` hint (G-06) | PASS |
| allowedNamespaces | 첫 실행 FAIL (nested subtree 삭제) → folder listing 순서 수정 후 7분간 안정 | PASS (수정 후) |
| Secret scan | 0 | PASS |
| 정리 | source 7개, Dremio, container, MinIO object 39개 삭제 | PASS |

### Review 반영 (INSTANCE=6)

Phase 3 diff review finding 11건을 모두 확인(실제 문제)하고 반영했다. 상세는 [phase3-oauth-catalog.md §9](phase3-oauth-catalog.md#9-review-반영-instance6).

- commit 403: `CommitForbiddenException`(`ForbiddenException` = Iceberg `CleanableFailure`)으로 던져 Iceberg가 거부된 commit의 manifest를 지우게 했다. 사용자에게는 같은 PERMISSION ERROR. live: 거부된 INSERT/DELETE 3건 뒤 남은 S3 object 0.
- state check: HTTP 401일 때만 client 교체(교체 시 table/view cache 비움). 5xx/429 등은 cached client 유지. root listing 403은 cache를 비우지 않는다. `ExpiringCatalogCache` close/replace race 수정.
- `dropFolder`: accessor는 typed `NamespaceNotEmptyException`, plugin이 validation error로 변환. live 메시지 동일.
- 401/403 오류 메시지, cause, listing WARN의 server message redaction. test는 secret을 echo하는 server로 검증.
- allowedNamespaces: 허용된 부모의 property를 못 읽어도 부모가 자식보다 먼저 listing된다.
- test 안정화(token refresh grace + polling), listing 403 test 결과 고정. harness `redact()` 대상 확대, `scan-secrets.sh` `.gz` 검사.
- 검증: module 319 tests 0 failures, fmt/lint clean, Local CI full SUCCESS, live INSTANCE=6 PASS(정리 완료), secret scan 0.

### Phase 3에서 처리한 Gap

| Gap | 처리 |
|---|---|
| G-05 (hint) | `warehouse`/`scope` 누락과 잘못된 값에 대한 hint가 API 응답에 나온다. Polaris 요구사항 자체는 그대로 |
| G-06 | DROP VIEW 403을 purge hint가 붙은 permission error로. 다른 403(load/create/drop/commit/CTAS staging/folder/dataset lookup)도 동작별 permission error |
| G-09 | 미시작 instance가 최근 실패 state를 반환하도록 해 source 생성/수정 API 오류에 hint가 나온다. OAuth2 error code, warehouse, 5xx/503, timeout, TLS, 분류되지 않는 HTTP 오류별 hint. `createCatalog`에 raw 예외 문자열 없음 |
| G-10 | 비어 있지 않은 namespace(400 "not empty" 또는 409)를 validation error로 (accessor는 typed `NamespaceNotEmptyException`, plugin이 변환). 없는 namespace drop은 "Folder does not exist" |
| G-11 | state check가 cached client 재사용, HTTP 401(stale session)일 때만 새 client로 교체하고 table/view cache를 비운다. `getDefaultBaseLocation` leak 제거. `ExpiringCatalogCache.replace`/`close` race 수정 |
| G-22 (매핑) | commit/CTAS staging 403을 permission error로 (unit, live INSERT/DELETE). commit 403은 Iceberg cleanup이 돌도록 `CleanableFailure`로 유지. out-of-tree LOCATION live는 Phase 5 |
| G-24 | separator와 parsing 규칙 문서화 (phase3-oauth-catalog.md §4.3) |
| G-27 (문서) | 최소 권한 principal 예시와 동작 (read-only, `CATALOG_READ_PROPERTIES`만) 문서화 |
| allowedNamespaces | nested entry의 조상 folder 포함, 부모 먼저 정렬, 중복 discovery 제거, 없는 namespace는 WARN 한 줄 |

### Phase 4 이후로 넘기는 항목

- Kernel: source update가 실패하면 `SourceMetadataManager`가 닫혀 background refresh가 멈춘다 (A). update 실패 문구 중복, REST v3 folder 오류의 409/class prefix, `DropFolderHandler`의 `CatalogFolderNotEmptyException` 미처리 (plugin이 validation error로 우회).
- 기본 `rest.client.*-timeout-ms` 없음 (권장값만 문서화). 정확한 HTTP status(404/409/429/502/504) 표기.
- allowedNamespaces 강제 여부(B-2), non-recursive의 빈 하위 folder(B-5). 공백이 든 namespace(O-1, upstream).
- `SHOW TABLES`가 harness SQL API 경로에서 0 rows (`sys` 포함, plugin 문제 아님).
- `distribution/server/target`의 압축 해제 디렉터리는 rebuild 후에도 `conf/`가 갱신되지 않을 수 있다. live 검증은 tarball을 새로 풀어서 한다. tarball의 `dremio-dac-ui` jar는 여전히 Phase 2 이전 UI (UI 브라우저 생성 미실행).
- G-04 vended credential runtime, G-08 storage matrix (Phase 4). out-of-tree LOCATION live, ALTER/OPTIMIZE, multi-node (Phase 5).

### Phase 3 — AbleOps Local CI 원격 검증

`localci submit --profile full --no-cache` (run `16c8597a`, 원격 Agent의 새 작업 공간, 대기 87 ms, 104 s):

| 구분 | Step | 결과 |
|---|---|---|
| 컴파일 + 테스트 | `icebergcatalog-test` | SUCCESS (319 tests, 0 failures, 0 skipped) |
| lint | `icebergcatalog-lint` | SUCCESS |
| 후속: 정적 분석 | `icebergcatalog-static` (errorprone, forbiddenapis, enforcer) | SUCCESS |
| 후속: UI | `ui-source-specs` | SUCCESS (103 passing, eslint 0 errors) |
| 후속: harness | `e2e-harness-lint` (license header, shellcheck) | SUCCESS |

## Phase 4 — Object Storage

상세 설정, 결과, troubleshooting: [storage.md](storage.md). 보안: [security.md](security.md) §7.

### 공통 완료 Gate

```text
[x] 병렬 Agent 작업 완료        (A Polaris + S3 기본 경로, B S3-compatible/MinIO, C Vended Credentials, D 보안)
[x] Integration 완료            (A P-A1 + B P2 → provider 기본 chain 교체, B P1 endpoint scheme, D logback patch 반영; B test의 forbiddenapis setAccessible 제거)
[x] Review 반영                 (finding 13건 모두 code로 확인 후 반영. 아래 §Review 반영)
[x] 신규 테스트 통과            (icebergcatalog 신규 test class 4개 62 tests + 변경된 `TestS3FileSystem` 25 tests(신규 2), 0 failures)
[x] 관련 regression 통과        (icebergcatalog module 381 tests, plugins/s3 module 112 tests, 0 failures)
[x] AbleOps Local CI             (`localci run --profile full --no-cache`: 6개 step 모두 SUCCESS (review로 `s3-test` 추가), 아래 표. 원격 submit은 Lead)
[x] secret/log 검토             (D live matrix 0건(KV 평문 G-12 제외), Integration INSTANCE=5와 review INSTANCE=6 scan 0건, `git diff`와 신규 파일에 MinIO secret 0건)
[x] git diff/status 검토        (Integration Agent, review 반영 Agent)
[x] 문서 갱신                   (storage.md 신규, security.md §1–§3·§6·§7, progress.md, compatibility-matrix.md)
[x] commit 완료                 (Lead, 691f2b5a9)
[x] origin push 완료            (Lead, origin/feature/polaris-restcatalog = 691f2b5a9)
[x] commit SHA 기록             (691f2b5a9)
```

### AbleOps Local CI

`localci run --profile full --no-cache` (local mode, review 반영 최종 code, log `target/dev/localci-phase4.log`, wall 2분 26초). 원격 `localci submit` 결과와 SHA는 Lead가 기록한다.

| 구분 | Step | 결과 |
|---|---|---|
| 컴파일 + 테스트 | `icebergcatalog-test` | SUCCESS (381 tests, 0 failures, 0 skipped) |
| 컴파일 + 테스트 | `s3-test` (review 신규) | SUCCESS (112 tests, 0 failures, 0 skipped) |
| lint | `icebergcatalog-lint` (spotless, license, checkstyle; review부터 `plugins/s3` 포함) | SUCCESS (0 violations) |
| 후속: 정적 분석 | `icebergcatalog-static` (errorprone, forbiddenapis, enforcer; review부터 `plugins/s3` 포함, `s3-test` 뒤) | SUCCESS (forbiddenapis s3/icebergcatalog main/test 0 errors) |
| 후속: UI | `ui-source-specs` | SUCCESS (103 passing, 9 pending) |
| 후속: harness | `e2e-harness-lint` (license header, shellcheck) | SUCCESS |
| 원격 | `localci submit --profile full --no-cache` | Lead 실행 예정 |

- Review 이전에는 `plugins/s3/src`가 archive에서 빠지고 어떤 step도 `plugins/s3`를 build하지 않았다 (Integration이 따로 확인). Review 반영으로 `.localci.yaml`이 `plugins/s3/src`를 포함하고 `s3-test` step(fast, full)과 lint/static의 `plugins/s3`를 추가했다. `plugins/icebergcatalog`의 test-scope `dremio-s3-plugin` 의존성은 여전히 `~/.m2`에서 resolve하지만, icebergcatalog test는 변경된 `S3FileSystem.getEndpoint()`를 쓰지 않는다.
- C 시점에 `icebergcatalog-static`을 막던 `TestRestCatalogS3CompatibleProps`의 `setAccessible`(forbiddenapis)은 reflection 없이 같은 규칙을 test 안에서 계산하도록 바꿨다.

### Gate 세부

| # | 항목 | 결과 | Status |
|---|---|---|---|
| 1 | 공식 baseline (`isUsingVendedCredentials=false`, `warehouse`, `scope`, `SimpleAWSCredentialsProvider`, secret `credential`/S3 key 2개) + MinIO endpoint 설정, 전체 read/write cycle | A INSTANCE=1, Integration INSTANCE=5 `base`: CREATE/INSERT/CTAS/INSERT…SELECT/UPDATE/DELETE/metadata table/`AT SNAPSHOT`/재시작 | **PASS** (MinIO) |
| 2 | 공식 baseline on AWS S3 | AWS 계정 없음 | ENVIRONMENT_BLOCKED |
| 3 | MinIO 최소 recipe, key별 필요 여부 | endpoint·compat·ssl(HTTP) 필수, path-style은 hostname이면 필수, 나머지 불필요 | PASS |
| 4 | provider 생략 | 수정 전 FAIL → 수정 후 PASS (unit, live `noprov`). Review: Hadoop 기본 chain을 빈 값 대신 `SimpleAWSCredentialsProvider`로 바꿔 key 없는 source는 fail closed (live `snp`) | PASS |
| 5 | endpoint scheme | 수정 전 FAIL (G-08) → 수정 후 PASS (unit, live `scheme`/`scheme2`, B `https://`) | PASS |
| 6 | SSL/TLS, region, path-style, requester-pays, bucket discovery | B matrix ([storage.md §14.2](storage.md#142-agent-b--s3-compatible--minio-instance2-user-minio--throwaway-tlsregion-minio)). custom region 이름은 NOT_SUPPORTED, AWS 전용 항목은 ENVIRONMENT_BLOCKED | PASS (MinIO) |
| 7 | Vended credentials (Polaris OSS, generic 구현) | C INSTANCE=3 + Integration `vn` + review INSTANCE=6 (`vn`/`vk`/`vbp`): static key 없이 read/write/DML, 만료 갱신. static key도 있는 source는 기본 location 밖 `LOCATION`도 PASS. vended만 있는 source의 기본 location 밖 `LOCATION` NOT_SUPPORTED | PASS (compatibility 결과) |
| 8 | Secret 노출 | D live matrix 0건 (KV 평문 G-12, `docker inspect` harness env 기록), HttpClient 4 header logger·Netty `LoggingHandler`(review) patch 적용, Integration/review scan 0 | PASS |
| 9 | 정리 | A–D, Integration, review 모두 container(`p4-*`), Dremio instance, MinIO prefix(Integration 86, review 94 object) 삭제. 남은 `p4-` container 0 | PASS |

### Review 반영 (INSTANCE=6)

Phase 4 diff review finding 13건을 하나씩 code로 확인한 뒤 반영했다. Live 재검증 결과는 [storage.md §14.5](storage.md#145-review-반영-재검증-instance6)에 있다.

| # | Finding | 확인 | 처리 |
|---|---|---|---|
| 1 | `vk`(vended + static key)의 새 table이 staged create credential(기본 location 전용)을 써서 기본 location 밖 `LOCATION`이 AccessDenied (Phase 4 이전에는 성공) | 실제 문제 | `loadTableStorageProperties(table, stageNewTable)`: 자기 S3 credential이 있는 source는 staged create를 하지 않고 static key로 쓴다. CREATE/CTAS/DROP TABLE 시 이 node의 table credential과 FS를 비운다. Live `vk`: `ns1/custom/*` CTAS·CREATE+INSERT, vended cache된 table DROP 후 다른 location으로 CTAS PASS |
| 2 | AWS에서 bucket discovery 기본값(true)이면 vended credential로 FS 생성 실패 (`ListAllMyBuckets` 없음) | code로 확인 (MinIO는 filtering이라 live로 재현 불가) | `applyTo`가 `dremio.bucket.discovery.enabled=false`를 설정. 처음 보는 bucket의 root listing 확인도 함께 빠진다. unit, live `vbp`(discovery 기본값) PASS |
| 3 | bucket별 key/provider가 vended credential을 덮음 | 실제 문제 (`FileSystemConfUtil.updateSourcePropsFromBucketProps`) | `applyTo`가 `fs.s3a.bucket.*.access.key`/`.secret.key`/`.session.token`/`.aws.credentials.provider`를 지운다. unit, live `vbp`(bucket별 provider Simple) PASS |
| 4 | source의 `s3.*` property가 vended credential로 오인됨 | 실제 문제 (FileIO property = catalog property + table config) | plugin이 source property와 같은 key/값을 뺀다 (`withoutSourceProperties`). unit |
| 5 | 만료 시각이 지난 FS entry 수명 0 → 매 조회 새 FS, lock 실패. wall clock step 후 만료된 credential 반환 | 실제 문제 | FS entry 최소 수명 1초. `VendedCredentialsCache.get`이 wall clock 기준으로 지난 결과를 다시 조회. unit (fake ticker/clock) |
| 6 | 200 ms 만료 test가 timing에 민감 | 실제 문제 | `DatasetFileSystemCache`에 ticker/clock 주입, test는 fake clock. 3회 연속 PASS |
| 7 | Local CI가 `plugins/s3`를 build/test하지 않음 | 실제 문제 | `plugins/s3/src` archive 포함, `s3-test` step, lint/static에 `plugins/s3` 추가 |
| 8 | Netty `LoggingHandler`(SDK v2 async 읽기)가 DEBUG에서 session token을 찍음 | 실제 문제 (bytecode, unit에서 고정을 빼면 session token 노출 확인) | `logback.xml`과 test `LogCapture`에 `io.netty.handler.logging.LoggingHandler`, `io.netty.handler.codec.http2.Http2FrameLogger` INFO 고정. test가 async 읽기와 `io.netty` DEBUG를 포함. security.md §3 문구 정정 |
| 9 | key 없는 source가 Dremio host identity(`AWS_*` env, instance profile)로 fallback | 실제 문제. 추가 발견: finding이 제안한 "chain 유지"도 fail closed가 아니다 (S3A가 chain의 env/instance provider를 시도, live 1차 `snp`) | Hadoop 기본 chain을 `SimpleAWSCredentialsProvider`로 교체 → key 없으면 두 client 모두 실패. host identity는 provider를 빈 값/명시할 때만 (opt-in). security.md §6·§7.4, storage.md §10·§12.4, compatibility-matrix 정정 |
| 10 | storage.md의 "vended source는 table마다 provider를 명시" 문구와 bucket별 설정 권장이 틀림 | 실제 문제 | #3 code 수정 + storage.md §4·§10·§12.4 정정 |
| 11 | security.md의 test 근거 과장 (`toString`/오류는 2개 test만, 102 event는 S3A+SDK v1 합계) | 실제 문제 | security.md §3, §7.1 정정, test class Javadoc도 정정 |
| 12 | storage.md가 `rest.*` 전체를 Hadoop conf에서 뺀다고 기술 (실제는 `rest.auth.*`/`rest.client.*`만) | 실제 문제 | 문서 정정 + `rest.access-key-id`/`rest.secret-access-key`/`rest.session-token`을 REST client 전용 key에 추가 (unit) |
| 13 | harness가 `AWS_ACCESS_KEY_ID`를 Dremio process에 남김 | 실제 문제 | `dremio-up.sh`가 `AWS_ACCESS_KEY_ID`/`AWS_ACCESS_KEY`/`AWS_SECRET_KEY`/`AWS_SESSION_TOKEN`/`AWS_PROFILE`/`S3_ACCESS_KEY`도 제거. live: 넘겨도 Dremio process env 0개 |

### Phase 4에서 처리한 Gap

| Gap | 처리 |
|---|---|
| G-04 | Vended credential runtime: table별 S3 FileSystem에 vended `s3.*` credential 적용, node별 cache와 만료 전 갱신 (C). Credential 외 설정은 source 값 |
| G-08 | MinIO 설정 실측. endpoint scheme은 `S3FileSystem.getEndpoint()` 수정으로 해결, requester-pays 기본값은 MinIO에서 무해 (변경 없음), 필수 key 확정 |
| G-28 | Dremio data path(DremioFileIO, S3, CTAS commit, DML)를 실제 Polaris + MinIO로 실행 (Phase 5에서 정식 E2E) |
| (신규) provider | 공식 문서/compatibility-matrix의 "provider가 비면 자동으로 채운다"는 틀렸다. Hadoop 기본 chain을 `SimpleAWSCredentialsProvider`로 바꿔 access key로 동작하게 했다. key가 없으면 host identity로 넘어가지 않고 실패한다 (review) |
| G-12 (일부) | 완화책 `dremio-admin encrypt` live 확인. 기본 동작은 그대로 |
| (Phase 2 known) | `DatasetFileSystemCache.close` NPE WARN 수정 (C) |

### Phase 5 이후로 넘기는 항목

- AWS S3 전부 (static baseline, requester-pays bucket, `ListAllMyBuckets` 없는 IAM user, instance profile, assumed role, AWS vending): ENVIRONMENT_BLOCKED.
- Multi-node executor (static, vended).
- Commit 시 403 외 `RESTException`(Polaris storage credential 오류 등)을 redact된 `UserException`으로 매핑 (A P-A3).
- AWS 외 region 이름 허용(B P3), non-AWS endpoint + compat off WARN(B P4), harness truststore mount(B P5 나머지).
- Known limitations: source state check가 storage를 검사하지 않음, Dremio 쪽 S3 오류로 실패한 CREATE TABLE이 Polaris에 남음, vended만 있는 source의 기본 location 밖 `LOCATION`, read-only vended INSERT의 SYSTEM 오류, 잘못된 region INSERT의 "Memory was leaked" 표시, logback hot reload 시 turboFilter 경고(D 관찰), vended cache 무효화는 DDL을 실행한 node만.
- Review 후속 후보: vended source(static key 없음)에서 explicit `LOCATION`을 staged create에 넘겨 그 location의 credential을 받기 (finding 1의 대안. FS cache key에 write path가 없어 범위가 크다). `AWS_*` env opt-in 경로 live 확인.

### Phase 4 — AbleOps Local CI 원격 검증

`localci submit --profile full --no-cache` (run `d2dfe26e`, 원격 Agent의 새 작업 공간, 147 s):

| 구분 | Step | 결과 |
|---|---|---|
| 컴파일 + 테스트 | `icebergcatalog-test` | SUCCESS (381 tests, 0 failures, 0 skipped) |
| 컴파일 + 테스트 | `s3-test` | SUCCESS (112 tests, 0 failures, 0 skipped) |
| lint | `icebergcatalog-lint` (icebergcatalog + s3) | SUCCESS |
| 후속: 정적 분석 | `icebergcatalog-static` (errorprone, forbiddenapis, enforcer) | SUCCESS |
| 후속: UI | `ui-source-specs` | SUCCESS (103 passing, eslint 0 errors) |
| 후속: harness | `e2e-harness-lint` | SUCCESS |

## Phase 5 — 실제 E2E / Regression

상세 결과(시나리오별 관찰값): [test-results.md §5](test-results.md#5-phase-5--실제-e2e--regression). 제약과 결함: [known-limitations.md](known-limitations.md).

### 공통 완료 Gate

```text
[x] 병렬 Agent 작업 완료        (Prep 배포본+UI bundle, A Lifecycle, B Read, C Write+Namespace/View, E Cache/Metadata, F 장애/Regression, G UI, Docs 초안)
[x] Integration 완료            (C의 partitioned CTAS 수정 검토/통합, layout help text 수정, flaky TLS test 수정, INSTANCE=7 재검증)
[x] 신규 테스트 통과            (신규 5: CTAS 3 + layout 1 + config 재시작 조건 1, 수정 1: TestRestCatalogHttpErrors. 0 failures)
[x] 관련 regression 통과        (icebergcatalog 386, plugins/s3 112, dac/backend 78, UI spec 103 passing / 9 pending, generic REST fixture E2E. sabot/kernel은 변경이 없어 실행하지 않음(N/A))
[x] AbleOps Local CI             (`localci run --profile full --no-cache`: 6개 step 모두 SUCCESS (2차, review 수정 뒤 3차). 원격 submit은 Lead)
[x] secret/log 검토             (7개 instance scan-secrets 0건, UI 응답/page 0건, `git diff`와 신규 파일에 MinIO secret 0건)
[x] git diff/status 검토        (Integration Agent)
[x] 문서 갱신                   (test-results.md §5–§6, known-limitations.md, progress.md, compatibility-matrix.md, configuration.md, design.md, security.md, storage.md §16, harness README)
[x] commit 완료                 (Lead, 6d2f6f4e9)
[x] origin push 완료            (Lead, origin/feature/polaris-restcatalog = 6d2f6f4e9)
[x] commit SHA 기록             (6d2f6f4e9)
```

### AbleOps Local CI

`localci run --profile full --no-cache` (local mode). 아래는 review 수정 뒤 3차 실행(log `target/dev/localci-phase5-rf.log`, wall 2분 16초)이다. 2차 실행(`target/dev/localci-phase5.log`, 385 tests)도 모두 SUCCESS였다. 원격 `localci submit` 결과와 SHA는 Lead가 기록한다.

| 구분 | Step | 결과 |
|---|---|---|
| 컴파일 + 테스트 | `icebergcatalog-test` | SUCCESS (386 tests, 0 failures, 0 skipped) |
| 컴파일 + 테스트 | `s3-test` | SUCCESS (112 tests, 0 failures, 0 skipped) |
| lint | `icebergcatalog-lint` (icebergcatalog + s3) | SUCCESS |
| 후속: 정적 분석 | `icebergcatalog-static` (errorprone, forbiddenapis, enforcer) | SUCCESS |
| 후속: UI | `ui-source-specs` | SUCCESS (103 passing, 9 pending, eslint 0 errors) |
| 후속: harness | `e2e-harness-lint` | SUCCESS |
| 원격 | `localci submit --profile full --no-cache` | Lead 실행 예정 |

- 1차 실행(`target/dev/localci-phase5-run1.log`)은 `icebergcatalog-test`가 Phase 3 test `TestRestCatalogHttpErrors.testUntrustedSelfSignedCertificate`("handshake was attempted") 1건으로 FAILED였다. 같은 test는 그 전 module 단독 실행(Integration 2회, C와 F의 실행)에서는 PASS였다. 다른 step과 동시에 돌 때 client의 인증서 거부가 fake server에 alert(`SSLException`)가 아니라 reset/EOF로 보이는 경우 count가 0이 된다. Fake server가 `startHandshake()`를 명시적으로 하고 모든 `IOException`을 세도록 test code만 고쳤다.

### E2E 요약 (영역별)

| 영역 | Agent / INSTANCE | PASS | FAIL | ENVIRONMENT_BLOCKED | NOT_SUPPORTED |
|---|---|---|---|---|---|
| Source lifecycle | A / 1 | 19 | 3 (kernel K-01) | 0 | 1 (rename. `/catalog/{id}/refresh` A-12는 미실행 이월이라 제외) |
| Read | B / 2 | 24 | 0 | 0 | 0 |
| Write | C / 3 | 23 | 1 (`DROP TABLE IF EXISTS` 메시지, minor) | 0 | 2 (ADD PRIMARY KEY, REMOVE ORPHAN FILES) |
| Namespace / View | C / 3 | 14 | 0 | 0 | 2 (`DROP FOLDER IF EXISTS`, `ALTER VIEW`) |
| Cache / Metadata | E / 4 | 23 | 0 | 0 | 0 |
| 장애 | F / 5 | 11 | 4 (known K-15 F-5, known C-08 F-12–F-14. 복구는 정상) | 0 | 0 |
| Regression | F / 5 + Integration | 5 (`sabot/kernel`은 미실행 N/A, 제외) | 0 | 0 | 0 |
| UI | G / 6 | 13 | 0 | 0 | 0 |
| Integration 재검증 | Integration / 7 | 7 | 0 | 0 | 0 |
| 합계 | | 139 | 8 | 0 | 5 |

AWS S3와 multi-node는 Phase 4부터 ENVIRONMENT_BLOCKED이며 Phase 5에서 다시 실행하지 않았다.

### Phase 5 code 변경

| 파일 | 변경 | 검증 |
|---|---|---|
| `plugins/icebergcatalog/.../AbstractRestCatalogAccessor.java` | Partitioned CTAS가 Polaris에서 unpartitioned default spec으로 남던 문제 수정 (C). `stagedCreateOperations`로 commit할 metadata의 spec/sort order/location/property로 다시 stage하고, `ForbiddenMappingTableOperations.commit`은 `base == null` + partitioned spec일 때만 그 operations로 commit한다. 403은 `CommitForbiddenException` | unit 3, live C(INSTANCE=3), Integration I-2–I-7 |
| `plugins/icebergcatalog/src/main/resources/restcatalog-layout.json` | MinIO help text: `dremio.s3.region=<region>` 대신 `fs.s3a.endpoint.region=<region>`(us-east-1이 아닐 때), 불필요한 requester-pays/bucket discovery key 제거 (Docs 초안 지적, storage.md와 일치) | unit 1 (`TestRestCatalogLayout`), live I-1 |
| `plugins/icebergcatalog/src/test/.../TestRestCatalogNamespaceTableOps.java` | CTAS test 3건 | 39 tests |
| `plugins/icebergcatalog/src/test/.../TestRestCatalogLayout.java` | help text test 1건 | 12 tests |
| `plugins/icebergcatalog/src/test/.../TestRestIcebergCatalogPluginConfig.java` | cache field만 바꾼 config의 `equals`/`equalsIgnoringNotMetadataImpacting` test 1건 (review: N-14 재시작 조건 정정의 근거) | 29 tests |
| `plugins/icebergcatalog/src/test/.../TestRestCatalogHttpErrors.java` | fake TLS server의 handshake 실패 count (flaky 수정) | 30 tests, Local CI |
| `scripts/polaris-e2e/sql.sh` | COMPLETED 뒤 항상 `/results` 조회 (`SHOW`/`DESCRIBE`의 `rowCount=0` 문제, N-06) (B) | shellcheck, Local CI `e2e-harness-lint` |
| `scripts/polaris-e2e/README.md` | G-09 이후 hint가 API 400 응답에 나온다는 점, `sql.sh` 동작 정정 | — |

### Phase 6으로 넘기는 항목

Phase 6 처리 결과: K-01, C-08, C-17, U-02는 고쳤다 (§Phase 6). `ROLLBACK TABLE … TO SNAPSHOT`은 최종 smoke P6-7에서 PASS. 나머지 운영 항목과 미실행 항목은 [known-limitations.md](known-limitations.md)에 상태와 이유를 남겼다 (K-15 FAIL known, 실행하지 않은 항목은 `NOT_SUPPORTED (검증하지 않음: <이유>)`, 환경이 없던 항목은 ENVIRONMENT_BLOCKED).

- **Kernel K-01**: source update 실패 시 `SourceMetadataManager`가 닫혀 background refresh가 재시작 전까지 멈춘다 (A-18–A-20 FAIL). 수정안: update 경로(`replacePlugin`/`replacePluginDeprecated`)는 `newStartSupplier(config, false)`로 metadata manager를 닫지 않는다. 공통 kernel 변경이라 Phase 5에서는 고치지 않았다.
- **Plugin C-08**: table lookup/load 경로의 429/5xx/timeout/connection reset을 `RestCatalogExceptionMapper`로 hint와 redaction이 붙은 오류로 매핑 (F-12–F-14 FAIL, known).
- **Plugin C-17**: `DROP TABLE IF EXISTS`(없는 table) 메시지. 외부에서 지워진 table의 목록 정리 동작과 함께 확인한 뒤 수정.
- **Release packaging U-02**: release tarball은 `dac/ui`를 포함해 Maven으로 build해야 한다 (Phase 5 배포본은 scratchpad에서 jar만 교체).
- 문서/운영: storage down hang 완화 설정(K-15, F-5 FAIL known. 기본 S3A 재시도 값 조정 여부도 검토), 외부 변경 stale 창(N-12, N-13), option 적용 시점(N-14: connection config가 바뀌는 update는 `@NotMetadataImpacting` field만 바꿔도 plugin 재시작, `metadataPolicy`만 바꾼 update는 재시작 없음), secret rotate가 실행 중 source를 끊지 않음(X-09), bad source 복구 경로(K-13).
- 미실행: `ROLLBACK TABLE`, namespace location 밖 `LOCATION`(Phase 3/4 결과 유지), Spark 등 외부 engine이 data를 쓴 table, `mutable.enabled=false`(G-23), static bearer `token`, namespace property update, AWS S3/multi-node(ENVIRONMENT_BLOCKED).
- 공통 code의 사소한 표기: rename 404(K-10), metadataPolicy 404(K-11), 재시작 시 일반 상태 메시지(K-12), class 이름 prefix(C-18).

### Phase 5 — AbleOps Local CI 원격 검증

`localci submit --profile full --no-cache` (run `ad6dd949`, 원격 Agent의 새 작업 공간, 144 s):

| 구분 | Step | 결과 |
|---|---|---|
| 컴파일 + 테스트 | `icebergcatalog-test` | SUCCESS (386 tests, 0 failures, 0 skipped) |
| 컴파일 + 테스트 | `s3-test` | SUCCESS (112 tests, 0 failures, 0 skipped) |
| lint | `icebergcatalog-lint` | SUCCESS |
| 후속: 정적 분석 | `icebergcatalog-static` | SUCCESS |
| 후속: UI | `ui-source-specs` | SUCCESS |
| 후속: harness | `e2e-harness-lint` | SUCCESS |

## Phase 6 — 최종 통합 / 문서 / Release Readiness

상세 결과: [test-results.md §7](test-results.md#7-phase-6--최종-통합--release-readiness). 설계 결정 D-16(`DROP TABLE` 404), D-17(K-01)은 [design.md §9](design.md#9-설계-결정과-근거).

### 공통 완료 Gate

```text
[x] 병렬 Agent 작업 완료        (Fix: C-08/C-17/K-01, Release build: dac/ui 포함 tarball + smoke, Structure audit)
[x] Integration 완료            (audit finding 확인 후 반영, Fix 보완(400/422 분류, 빈 message), kernel/plugin/distribution 재build, 최종 smoke)
[x] 신규 테스트 통과            (icebergcatalog 신규 26 (Fix 17 + Integration 6 + 최종 review 3), kernel 신규 2, 0 failures)
[x] 관련 regression 통과        (icebergcatalog 412, sabot/kernel TestManagedStoragePlugin+TestPluginsManager 34, plugins/s3 112, UI spec 103 passing / 9 pending)
[x] AbleOps Local CI             (`localci run --profile full --no-cache`: 6개 step 모두 SUCCESS. 원격 submit은 Lead)
[x] secret/log 검토             (최종 smoke scan 0, build/smoke/Local CI log와 `git diff`·신규 파일에 MinIO secret 0건, 실제 access key ID를 test/Javadoc/문서에서 placeholder로 교체)
[x] git diff/status 검토        (Integration Agent. tooling 파일 `.gitignore`, `.claude/`, `CLAUDE.md`, `dac/ui/CLAUDE.md`, `scripts/dev`는 feature commit에서 제외 권장)
[x] 문서 갱신                   (design, configuration, storage, security, test-results §7, known-limitations, compatibility-matrix, progress, README 신규)
[ ] commit 완료                 (Lead)
[ ] origin push 완료            (Lead)
[ ] commit SHA 기록             (Lead)
```

### Phase 6 확인 항목 (작업 정의)

| 항목 | 결과 | Status |
|---|---|---|
| Generic REST Catalog 구조 유지 | 새 `@SourceType`/engine 없음. Polaris 이름은 hint/Javadoc/layout 문구와 UI preset에만 (audit A) | PASS |
| Polaris-specific hack 제거 | 동작을 Polaris에 묶는 code 없음. 400 "not empty"와 `expiration-time`은 generic fallback (audit A) | PASS |
| 공통 코드 중복 제거 | `closeQuietly`, credential provider 상수, namespace WARN 문구 통합. 미사용 method 삭제 | PASS |
| Secret masking | §7.1 redaction 확장, live scan 0 | PASS |
| UI/API/Backend property 일치 | 9개 field 이름·tag·기본값·label 일치 (audit B), sensitive key 규칙 동기화 unit test | PASS |
| Plugin packaging | tarball의 plugin/kernel/s3/UI jar md5 = build 산출물, 중복 class 없음 | PASS |
| Maven dependency | pom 변경 없음 | PASS |
| Frontend build | `dac/ui` Maven production build, 새 bundle 1개 | PASS |
| Backend tests | icebergcatalog 412, kernel 34, s3 112 | PASS |
| RESTCATALOG regression | module test, Local CI, release smoke 2회 | PASS |

### AbleOps Local CI

`localci run --profile full --no-cache` (local mode, 최종 review 반영 code, log `target/dev/localci-phase6-final.log`, wall 2분 21초. 그 전 `target/dev/localci-phase6.log`(409 tests)도 모두 SUCCESS):

| 구분 | Step | 결과 |
|---|---|---|
| 컴파일 + 테스트 | `icebergcatalog-test` | SUCCESS (412 tests, 0 failures, 0 skipped) |
| 컴파일 + 테스트 | `s3-test` | SUCCESS (112 tests, 0 failures, 0 skipped) |
| lint | `icebergcatalog-lint` | SUCCESS |
| 후속: 정적 분석 | `icebergcatalog-static` (errorprone, forbiddenapis, enforcer) | SUCCESS |
| 후속: UI | `ui-source-specs` | SUCCESS (103 passing, 9 pending) |
| 후속: harness | `e2e-harness-lint` | SUCCESS |
| 원격 | `localci submit --profile full --no-cache` | Lead 실행 예정 |

Local CI는 `sabot/kernel`을 build하지 않는다. K-01은 kernel module test(34)와 최종 tarball live(P6-6)로 확인했다.

### Lead에게 넘기는 항목

- Phase 6 commit/push와 SHA 기록, 원격 `localci submit`.
- Commit 범위: `docs/polaris/`, `plugins/icebergcatalog/`, `sabot/kernel/.../ManagedStoragePlugin.java`와 test. `.gitignore`, `.claude/`, `CLAUDE.md`, `dac/ui/CLAUDE.md`, `scripts/dev`는 feature와 무관한 tooling 변경이다 (따로 commit하거나 제외. `.claude/`를 commit하면 `settings.local.json`이 ignore되는지 확인).
- Release 배포본: `distribution/server/target/dremio-community-26.0.5-202509091642240013-f5051a07.tar.gz` (md5 `410d54ef74edf36e4a4a48370da04138`, Phase 6 최종 code + Release build UI jar).

### Phase 6 — AbleOps Local CI 원격 검증

`localci submit --profile full --no-cache` (run `4f295c83`, commit `b2bf2e0cc`, 원격 Agent의 새 작업 공간, 146 s):

| 구분 | Step | 결과 |
|---|---|---|
| 컴파일 + 테스트 | `icebergcatalog-test` | SUCCESS (412 tests, 0 failures, 0 skipped) |
| 컴파일 + 테스트 | `s3-test` | SUCCESS (112 tests, 0 failures, 0 skipped) |
| lint | `icebergcatalog-lint` | SUCCESS |
| 후속: 정적 분석 | `icebergcatalog-static` | SUCCESS |
| 후속: UI | `ui-source-specs` | SUCCESS (103 passing) |
| 후속: harness | `e2e-harness-lint` | SUCCESS |

- 원격 압축본은 32 MiB 제한 때문에 `sabot/kernel/src`를 제외하므로, K-01 kernel 변경(`ManagedStoragePlugin`)은 로컬에서 검증했다: `TestManagedStoragePlugin` + `TestPluginsManager` 34 tests 0 failures, `sabot/kernel` lint SUCCESS, errorprone 포함 compile SUCCESS (`ManagedStoragePlugin.java` 재컴파일 확인).

## 완료 기준 현황

`[ ]`는 아직 충족하지 않았다는 뜻이다. 현재 상태와 근거를 함께 적는다.

| # | 기준 | 현재 상태 | 근거 / 다음 단계 |
|---|---|---|---|
| 1 | `[x]` Dremio OSS에서 RESTCATALOG Source 노출 | PASS | Phase 2 live gate #1, #2 (`GET /api/v3/source/type` 200) |
| 2 | `[x]` UI Source 생성 성공 | PASS | Phase 5 G(INSTANCE=6, Playwright) U-1–U-13: Polaris preset과 generic tile로 생성, `good`, namespace/table 탐색, 필수값·secret key validation, Edit masking, secret 0건. Phase 6 release tarball에 새 UI 포함 확인 (P6-2, [test-results.md §7.2](test-results.md#72-release-build)) |
| 3 | `[x]` REST API Source 생성 성공 | PASS | Phase 2 live gate #3. Phase 5 A-1/A-13/A-15/A-16: 생성, 삭제 후 재생성, 중복 이름 409, 잘못된 credential 400 + hint |
| 4 | `[x]` Polaris OAuth2 인증 성공 | PASS | Phase 2 live. Phase 3: 잘못된 credential/scope/warehouse/endpoint hint가 API 응답에 나오고, token 만료(60초)·갱신, 401/403, Polaris 재시작 복구 확인 ([phase3-oauth-catalog.md](phase3-oauth-catalog.md)) |
| 5 | `[x]` Namespace 탐색 성공 | PASS | Phase 2 live gate #7. Phase 3: nested(3 level) 양방향, CREATE/DROP FOLDER, 비어 있지 않은 namespace 메시지 |
| 6 | `[x]` Table 탐색 성공 | PASS | Phase 3. Phase 5 B-1–B-6, B-12: `SHOW SCHEMAS`/`SHOW TABLES`(harness 수정 후)/`INFORMATION_SCHEMA`, nested, 늦게 만든 table. Spark가 data를 쓴 table은 미실행 (Polaris API로 만든 table로 대신) |
| 7 | `[x]` SELECT 성공 | PASS | Phase 5 B-7–B-22: type, complex type, filter, aggregate(Python 계산값 일치), join, EXPLAIN/pruning, `AT SNAPSHOT`/`AT TIMESTAMP`, metadata table |
| 8 | `[x]` CREATE TABLE 또는 CTAS 성공 | PASS | Phase 5 C-1–C-8, C-26, I-2–I-6. Partitioned CTAS는 수정 전 FAIL(Polaris default spec unpartitioned) → plugin 수정 후 PASS |
| 9 | `[x]` INSERT 성공 | PASS | Phase 5 C-9, C-14, C-24, C-25: INSERT VALUES/SELECT(partitioned 포함), UPDATE/DELETE/MERGE, 동시 INSERT(409 자동 재시도)·동시 UPDATE(`CONCURRENT_MODIFICATION`) |
| 10 | `[x]` Polaris + S3/MinIO 접근 성공 | PASS (MinIO) / ENVIRONMENT_BLOCKED (AWS S3) | Phase 4: 공식 static baseline + MinIO endpoint로 전체 read/write cycle (A, Integration `base`), MinIO matrix (B), vended credential (C), secret 0건 (D). [storage.md](storage.md) |
| 11 | `[x]` Source restart/reload 성공 | PASS | Phase 5 A-6(재시작 후 config/secret 유지, `good`, SELECT/INSERT), A-7–A-10(REFRESH STATUS, REFRESH METADATA, 외부 schema 변경, names refresh), E-23, F-4(Polaris down 중 재시작 후 복구). update 실패 뒤 background refresh 정지(kernel K-01, Phase 5 A-18–A-20 FAIL)는 Phase 6에서 수정: unit `TestManagedStoragePlugin`, live P6-6(실패한 update 뒤 40초에 새 namespace 표시) |
| 12 | `[x]` allowedNamespaces 동작 | PASS | Phase 3 matrix와 안정성 확인. Phase 5 A-4: update로 설정/제거, tree와 `INFORMATION_SCHEMA`에 목록 namespace만. discovery 범위만 정하고 접근 제어는 아니다 (목록 밖 table 직접 SELECT 성공 N-01, 목록 밖 view는 안 풀림 N-11) |
| 13 | `[x]` secret masking 정상 | PASS | Phase 6: 동작 중 오류(429/5xx/timeout/400/401) redaction 확장, listing log redaction, 최종 smoke scan 0 (P6-8). Phase 2 live gate #4, #5, #13. Phase 3: state/오류 메시지 redaction(unit echo test), DEBUG 포함 live scan 0건. Phase 4: vended credential 포함 REST/job/profile/system table/log 노출 matrix 0건, HttpClient 4 header logger·Netty `LoggingHandler` 고정, key 없는 source fail closed ([security.md](security.md) §7). at-rest 암호화는 NOT_SUPPORTED (G-12, `dremio-admin encrypt` 완화책). Phase 5: GET 4개 endpoint masking(A-2), 재시작 후 유지(A-6), UI Edit masking과 HTTP 응답 150개 0건(U-7, U-13), 7개 instance log scan 0건 |
| 14 | `[x]` Generic RESTCATALOG 회귀 없음 | PASS | Phase 5: `apache/iceberg-rest-fixture`(OAuth/warehouse/scope 없음) E2E에서 CREATE FOLDER/TABLE, INSERT, SELECT, CTAS, UPDATE, DROP PASS. UI generic tile 생성(U-9–U-12). icebergcatalog 386, plugins/s3 112, dac/backend 78, UI spec 103 passing. Phase 6: icebergcatalog 412, kernel 34, s3 112, UI spec 103 (Local CI) |
| 15 | `[x]` 관련 unit/integration/E2E 검증 | PASS | Phase 3 251 tests, Phase 4 62 + 2, Phase 5 신규 5 + flaky 수정 1, Phase 6 신규 26 + kernel 2. E2E harness로 7개 instance live (test-results.md §5), Phase 6 release tarball smoke 2회 (test-results.md §7.3). 별도 opt-in IT는 만들지 않았다 (harness E2E로 대신) |
| 16 | `[x]` 문서 완료 | PASS | 작업 정의의 7종(design, configuration(§1 UI, §2 REST API, §3 Polaris+S3, §4 MinIO), storage, security, test-results, known-limitations, progress) + compatibility-matrix, phase1/phase3 문서, [README.md](README.md). Secret은 placeholder만. 남은 `TBD` 없음. 결과 상태는 spec의 네 값만 쓴다: 최종 review에서 비표준 값(`미검증 (범위 밖)`, `기록`, `PASS (수정됨)`)을 찾아 실행하지 않은 항목은 `NOT_SUPPORTED (검증하지 않음: <이유>)`, 환경이 없던 항목은 `ENVIRONMENT_BLOCKED`로 바꿨다. 괄호는 비고다 |
| 17 | `[x]` 모든 Phase commit/push 완료 | PASS | Phase 1 `67cf10e4c`, Phase 2 `f2553db4a` (+ `ffd1cc1b2`), Phase 3 `3b85d3619`, Phase 4 `691f2b5a9`, Phase 5 `6d2f6f4e9`, Phase 6 `b2bf2e0cc`. 모두 `origin/feature/polaris-restcatalog`에 push 완료 |

## Gap 요약 (상세: [phase1-analysis.md §4](phase1-analysis.md#4-gap-목록))

| 심각도 | Gap |
|---|---|
| blocker | ~~G-01 `@SourceType` 누락~~, ~~G-02 layout 누락~~ (Phase 2 해결) |
| high | ~~G-03~~ (해결), ~~G-04~~ (Phase 4 vended runtime), G-05 Polaris `warehouse`/`scope` 필수 (UI/문서/hint 반영, 요구사항 자체는 Polaris), ~~G-06~~ (Phase 3 오류 매핑), ~~G-07~~ (해결), ~~G-08~~ (Phase 4: scheme 수정, MinIO 실측), ~~G-28~~ data path (Phase 4 live, Phase 5 정식 E2E PASS) |
| medium | ~~G-09~~, ~~G-10~~, ~~G-11~~ (Phase 3 해결), G-12 secret at-rest (Hadoop conf 복사는 해결, `dremio-admin encrypt` 완화책), ~~G-13~~, ~~G-15~~, ~~G-16~~ (해결), ~~G-22~~ LOCATION 403 (매핑 Phase 3, live Phase 4/5) |
| low | G-14 (backend 해결, UI rename 미완), ~~G-17~~, ~~G-18~~, ~~G-19~~ (해결), G-20, G-21, G-23, G-24, G-25 (Phase 5 live 확인, 제약), G-26 (partitioned CTAS는 Phase 5 plugin 수정), G-27 |

## 환경 메모

- **JDK와 build**
  - Build는 JDK 21, test는 JDK 11 toolchain을 쓴다.
  - Module 단위 build와 test는 `scripts/dev`로 한다.
  - `clean`과 `-Dmaven.test.skip`은 쓰지 않는다.
- **`dac/ui`**
  - Node 의존성은 설치되어 있다 (`dac/ui/target/frontend/node`의 node 22). 시스템 node 24는 `.ts` spec을 읽지 못한다.
  - UI spec: `PATH=$PWD/target/frontend/node:$PATH DREMIO_UI_TESTS="<abs spec paths>" corepack pnpm run test:only`
  - UI bundle만 필요하면 Maven 없이 `corepack pnpm run build --output-path=<dir>/rest/dremio_static`로 만든다 (약 90초).
  - `~/.m2`의 `dremio-dac-ui` jar는 Phase 6 Release build(새 UI, md5 `4312b20c2a570e557ff679eb83a02a7d`)다. `dac/ui`를 다시 Maven build할 때는 `dac/ui/target/classes`에 이전 `app.*.js`가 남지 않았는지 확인한다.
- **Docker** 사용 가능.
- **사용 가능한 이미지 (로컬)**

  | 이미지 | 용도 |
  |---|---|
  | `apache/polaris:1.1.0-incubating` | 기준 버전 |
  | `apache/polaris:0.10.0-beta` | 비교용 (이 작업에서 실행하지 않음) |
  | `bitnamilegacy/minio:latest` | MinIO 대체 |
  | `chrislusf/seaweedfs` | 대안 S3-compatible storage |

- **`minio/minio` 사용 불가**: Docker Hub의 `minio/minio`는 pull할 수 없다(차단). 대신 `bitnamilegacy/minio:latest`를 쓴다.
  - 환경 변수: `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_DEFAULT_BUCKETS=warehouse`
- **Phase 2 live 환경**
  - Polaris는 `--network host`로 띄워 host의 MinIO(`http://127.0.0.1:9000`)에 접근한다. AWS_* env에 MinIO `<access-key>`/`<secret-key>`를 넣는다.
  - Dremio tarball은 scratchpad에 복사해 실행했다 (JDK 17, `conf/dremio-env`에 `JAVA_HOME`). 9047/31010/45678과 8181/8182가 비어 있어야 한다.
  - 첫 사용자: `PUT /apiv2/bootstrap/firstuser` (header `Authorization: _dremionull`).
  - MinIO 정리는 임시 `MC_CONFIG_DIR`의 `mc` alias로 한다 (`~/.mc` 사용 금지).
- **E2E harness** (Phase 3): `scripts/polaris-e2e` ([README](../../scripts/polaris-e2e/README.md)). INSTANCE(0..9)별 port, container(`p3-polaris-e2e-<i>`), work dir, MinIO prefix를 분리한다. Secret은 환경 변수로만 받는다.
- **Container 규칙**
  - 이름에 `p1-` prefix를 붙인다 (Phase N이면 `pN-`).
  - 높은 port를 쓴다.
  - 사용 후 `docker rm -f`로 제거한다. Phase 1에서 쓴 container는 모두 제거했다.
- **Polaris FILE storage 실행 flag**
  - `polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["FILE","S3"]`
  - `polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true`
  - `polaris.readiness.ignore-severe-issues=true`
  - 명령 전체는 [phase1-analysis.md §5.3](phase1-analysis.md#53-재사용-가능한-명령) 참고.
- **Phase 6 live**: harness INSTANCE=3, container prefix `p6-`, `E2E_WORK`는 scratchpad, MinIO prefix `polaris-p6-final` (정리 완료).
- **로컬 테스트 전용 credential**
  - Polaris root: `root` / `s3cr3t`
  - MinIO: `minioadmin` / `minioadmin`
  - 문서와 설정 예제에서는 `<client_id>:<client_secret>`, `<s3AccessKey>`, `<s3SecretKey>` placeholder를 쓴다.
- **Probe 관련 파일** (repo 밖 scratchpad, commit 대상 아님)
  - Iceberg client probe (`Probe.java`, `Probe2.java`, `Refresh.java`)
  - Agent B prototype (`p1b/proto/P1ProtoRestCatalogConf.java`, 25.2.0 layout)
  - Prototype jar는 Phase 2의 in-tree annotation과 **동시에 classpath에 두면 안 된다**. 두 class가 같은 `RESTCATALOG` 값을 가지면 duplicate key 때문에 startup이 실패한다.
