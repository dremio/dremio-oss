# Polaris / RESTCATALOG Compatibility Matrix

- 기준: Dremio OSS 26.0.5 + Phase 2/3/4/5/6 변경 (`feature/polaris-restcatalog`), Apache Polaris `1.1.0-incubating`, Iceberg `1.7.0-5f7c992-20250730084652-3bf8b99` (`pom.xml:82`)
- 갱신 시점: Phase 6 Integration (2026-10-03). Phase 6 최종 smoke 근거(`live-P6`, `P6-…`)는 [test-results.md §7](test-results.md#7-phase-6--최종-통합--release-readiness). Phase 5 E2E 근거(`live-P5`, 시나리오 번호)는 [test-results.md §5](test-results.md#5-phase-5--실제-e2e--regression). Storage 상세는 [storage.md](storage.md). Phase 1 근거는 [phase1-analysis.md](phase1-analysis.md), Phase 2 live gate는 [progress.md](progress.md#phase-2--restcatalog-oss-source-완성), Phase 3 결과는 [phase3-oauth-catalog.md](phase3-oauth-catalog.md)에 있다.

## 상태 값

Status 열에는 작업 정의(spec "결과 상태")의 아래 네 값만 쓴다. 괄호 안은 비고(근거, 제약, 이유)이며 별도 상태가 아니다. 한 칸에 하위 항목이 여럿이면 `/`로 나눠 각각 네 값 중 하나를 쓴다.

| 값 | 의미 |
|---|---|
| `PASS` | 검증 완료 |
| `FAIL` | 현재 동작하지 않음 (Gap) |
| `ENVIRONMENT_BLOCKED` | 환경 제약 때문에 검증하지 못함 |
| `NOT_SUPPORTED` | 설계상 지원하지 않음. 또는 이 작업(Phase 1–6)에서 검증하지 않아 지원한다고 주장하지 않음: `NOT_SUPPORTED (검증하지 않음: <이유>)`로 적는다 |

## 검증 수준

"검증" 열에서 쓰는 약어:

| 약어 | 의미 |
|---|---|
| **static** | 코드 정적 분석 |
| **client-probe** | Dremio의 Iceberg jar로 Polaris를 직접 호출. Dremio data path(DremioFileIO, S3)는 거치지 않는다 |
| **proto-E2E** | 빌드된 Dremio tarball에 `@SourceType` prototype(out-of-tree)을 넣고 확인 |
| **E2E** | 실제 Dremio SQL 경로로 확인 |
| **live-P2** | Phase 2 in-tree build tarball + Polaris 1.1.0 + 로컬 MinIO로 REST API/SQL 확인 (smoke) |
| **live-P3** | Phase 3 tarball + Polaris 1.1.0 + 로컬 MinIO, `scripts/polaris-e2e` harness (Agent A–D 각 instance와 integration 재검증 INSTANCE=5) |
| **live-P4** | Phase 4 tarball + Polaris 1.1.0 + 로컬 MinIO(+ TLS/region용 throwaway MinIO), harness (Agent A–D, integration 재검증 INSTANCE=5, review 반영 재검증 INSTANCE=6). 상세: [storage.md §14](storage.md#14-검증-matrix) |
| **live-P5** | Phase 5 E2E 배포본 + Polaris 1.1.0 + 로컬 MinIO, harness 7개 instance ([test-results.md §5](test-results.md#5-phase-5--실제-e2e--regression)) |
| **live-P6** | Phase 6 release tarball(`dac/ui` 포함 Maven build) + Polaris 1.1.0 + 로컬 MinIO, harness INSTANCE=3 ([test-results.md §7](test-results.md#7-phase-6--최종-통합--release-readiness)) |
| **unit** | icebergcatalog module unit test |

---

## (a) 공식 Dremio RESTCATALOG config 필드 ↔ OSS 코드

출처:
- 공식: API Catalog > Source > Source Configuration (RESTCATALOG), Iceberg REST Catalog 페이지
- 코드: `plugins/icebergcatalog/src/main/java/com/dremio/plugins/icebergcatalog/store/IcebergCatalogPluginConfig.java:41-66`, `RestIcebergCatalogPluginConfig.java:43-75`

| 필드 | Type | 공식 Default | OSS Default | UI label (공식 / OSS `@DisplayMetadata`) | OSS Tag | 필드 정의 상태 | Source 생성 경로 상태 (Phase 2) | 비고 |
|---|---|---|---|---|---|---|---|---|
| `propertyList` | Array<{name,value}> | (optional) | null | Catalog Properties / Catalog Properties | 1 | PASS | PASS (live-P2) | Iceberg props에는 그대로 전달된다. Hadoop conf에는 REST client 전용 key(`credential`, `token`, `scope`, `oauth2-server-uri`, `header.*` 등)를 뺀 나머지만 복사된다 |
| `secretPropertyList` | Array<{name,value}> | (optional), masked `$DREMIO_EXISTING_VALUE$` | null | Catalog Credentials / Catalog Credentials (`@Secret`) | 2 | PASS | PASS (live-P2) | API에서 masking된다. at-rest는 평문이다 (G-12) |
| `enableAsync` | Boolean | true | true | (IRC 문서에 별도 toggle 없음) / "Enable asynchronous access for Parquet datasets" | 3 | PASS | PASS (live-P2) | label 차이는 경미하다 |
| `isCachingEnabled` | Boolean | true | true | Enable local caching when possible | 4 | PASS | PASS (live-P2) | `@NotMetadataImpacting` |
| `maxCacheSpacePct` | Integer 1..100 | 100 | 100 | Max percent of total available cache space to use when possible | 5 | PASS | PASS (live-P2) | `@NotMetadataImpacting` |
| `restEndpointUri` | String (required) | — | null | Endpoint URI | 10 | PASS (`@NotBlank`, G-17) | PASS (live-P2). 누락 시 400 validation error | layout에서도 required |
| `allowedNamespaces` | Array<String> | (optional) | null (= 전체, recursive) | Allowed Namespaces | 11 | PASS | PASS (live-P3: nested entry의 부모 folder 유지 수정 후) | separator는 option `plugins.restcatalog.allowed.ns.separator`(`\\.`, regex)로 정한다. discovery 범위만 정하며 접근 제어는 아니다 |
| `isRecursiveAllowedNamespaces` | Boolean | true | true | Allowed Namespaces include their whole subtrees | 12 | PASS | PASS (live-P3) | false면 직접 하위 namespace가 빈 folder로 보인다 (C B-5, 문서화) |
| `isUsingVendedCredentials` | boolean (선택, 기본 false. validation annotation 없음) | UI: checked (true) / Polaris OSS recipe: false | false | Use vended credentials / Use vended credentials | 13 | PASS (unit: tag, 기본값, 25.2.0 bytes 호환) | PASS (live-P2: false/true 모두 200) | `@NotMetadataImpacting`. 기본값 false는 기존 source 동작 유지를 위한 것. true면 header를 보내고 Phase 4부터 vended S3 credential을 table별로 쓴다 (G-04 해결, live-P4) |
| Source type `RESTCATALOG` | — | "Dremio source type: RESTCATALOG" | `@SourceType("RESTCATALOG")` | Iceberg REST Catalog / Iceberg REST Catalog (UI 상수도 동일) | — | PASS (unit) | PASS (live-P2: `/api/v3/source/type` 200, create 200) | Polaris 전용 type 없음. Polaris는 UI preset |
| uiConfig layout | — | General / Advanced Options / Reflection Refresh / Metadata / Privileges | `restcatalog-layout.json` (General / Advanced Options) | — | — | PASS (unit: propName ↔ `@Tag` 필드 일치) | PASS (live-P2: `/api/v3/source/type/RESTCATALOG`에 uiConfig) | 나머지 tab은 UI가 공통으로 추가한다. 브라우저 생성 PASS (live-P5 U-1–U-13), release tarball UI PASS (live-P6 P6-2) |

### (a-2) 공식 Polaris OSS recipe의 property key ↔ OSS 처리

| UI 위치 | Key | 예시 값 | OSS 처리 | 검증 | Status |
|---|---|---|---|---|---|
| General | Endpoint URI | `http://<polaris>:8181/api/catalog` | Iceberg `uri` | client-probe, proto-E2E | PASS |
| General | Use vended credentials | Unchecked | `isUsingVendedCredentials` (tag 13). true면 `header.X-Iceberg-Access-Delegation=vended-credentials`를 보내고 Phase 4부터 table별 vended S3 credential로 파일에 접근한다 | unit, live-P2, live-P4 | PASS (flag, runtime: Polaris OSS + MinIO, single node) |
| Catalog Properties | `warehouse` | `<polaris_catalog>` (대소문자 정확히 일치) | `/v1/config?warehouse=`로 전달. 응답의 `prefix` 사용 | client-probe, proto-E2E | PASS |
| Catalog Properties | `scope` | `PRINCIPAL_ROLE:ALL` | Iceberg OAuth2. 기본값 `catalog`는 Polaris가 거부 | client-probe, proto-E2E, live-P3 | PASS (scope 지정 시). 미지정/잘못된 scope는 source 생성 400과 `invalid_scope` hint (Phase 3) |
| Catalog Properties | `fs.s3a.aws.credentials.provider` | `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` | Hadoop conf. Phase 4 이전에는 생략하면 Hadoop 기본 chain이 남아 `Invalid AWSCredentialsProvider provided`로 실패했다 ("비어 있으면 자동으로 채운다"는 Phase 1–3 설명은 틀렸다). Phase 4부터 plugin이 Hadoop 기본 chain만 `SimpleAWSCredentialsProvider`로 바꾼다: key가 있으면 그대로 동작하고, 없으면 host의 `AWS_*` env나 instance profile로 넘어가지 않고 실패한다. 빈 값을 명시하면 `FileSystemConfUtil`이 access key / `AWS_*` env / instance profile 순으로 정한다 (opt-in) | unit, live-P4 (MinIO: 명시 `base`, 생략 `noprov`, key 없음 `snp`, 빈 값 `sne`) | PASS (MinIO). AWS S3는 ENVIRONMENT_BLOCKED |
| Catalog Properties | `oauth2-server-uri` (권장, 공식 Polaris recipe에는 없음) | `http://<polaris>:8181/api/catalog/v1/oauth/tokens` | 미지정 시 `<uri>/v1/oauth/tokens`와 deprecation WARN | client-probe | PASS |
| Catalog Properties | `token-refresh-enabled` | 기본 true (false 금지) | Iceberg | client-probe (PT20S), live-P3 (Polaris token TTL 60초) | PASS. false면 만료 직후 query 1건이 실패하고 state check가 client를 교체한다 |
| Catalog Properties | `rest.client.connection-timeout-ms` / `rest.client.socket-timeout-ms` / `rest.client.max-retries` | 권장 `10000` / `60000` / 기본 5 | Iceberg HTTPClient. Hadoop conf로 복사하지 않는다 (Phase 3) | unit, live-P3 | PASS. 미설정이면 timeout 3분 ([security.md §5](security.md#5-재시도와-timeout-iceberg-rest-client)) |
| Catalog Credentials | `credential` | `<client_id>:<client_secret>` | OAuth2 client_credentials | client-probe, proto-E2E | PASS |
| Catalog Credentials | `fs.s3a.access.key` / `fs.s3a.secret.key` | `<s3AccessKey>` / `<s3SecretKey>` | Hadoop conf로 S3FileSystem에 전달 (Phase 2부터 `start()` 시점 eager copy, G-07) | unit, live-P4 (MinIO 공식 baseline 전체 cycle) | PASS (MinIO). AWS S3는 ENVIRONMENT_BLOCKED |

---

## (b) Iceberg REST operations

Dremio plugin 사용 여부와 Polaris 1.1.0 지원 여부를 함께 표시한다.

| Operation (REST) | Dremio plugin에서 사용 | Polaris 1.1.0 | 검증 | Status | 비고 |
|---|---|---|---|---|---|
| `GET /v1/config?warehouse=` | Yes (RESTCatalog initialize) | Yes. warehouse 필수 (없으면 400, 틀리면 404) | client-probe, proto-E2E, live-P3 | PASS | Phase 3: state check는 cached client를 재사용하고 `listNamespaces(root)` 1회만 호출한다. 새 client(config + token)는 cached client가 없거나 401 등으로 실패할 때만 만든다 (G-11 해결) |
| `POST /v1/oauth/tokens` (client_credentials) | Yes (Iceberg OAuth2Util) | Yes. `scope=PRINCIPAL_ROLE:<role>\|ALL` 필수 | client-probe, proto-E2E, live-P3 | PASS | 잘못된 secret: Polaris 401 `unauthorized_client` → Iceberg `BadRequestException`. Phase 3부터 source 생성 400 응답에 OAuth2 error code와 hint가 나온다 (G-09 해결) |
| Token refresh (token-exchange) | Yes (기본값 true) | Yes | client-probe, live-P3 (TTL 60초) | PASS | 만료 전 자동 갱신. 새 config 요청 없음 |
| Endpoint discovery (`endpoints`) | Yes | Yes. 비표준 항목 포함 | client-probe | PASS | |
| `GET /v1/{prefix}/namespaces` (list, nested) | Yes (folder listing, allowedNamespaces, state check) | Yes (`%1F` 지원) | client-probe, proto-E2E, live-P2, live-P3 | PASS | nested(3 level) 포함. 403이면 state `warn` (allowedNamespaces 미설정 시). 공백이 든 namespace 이름은 Iceberg client가 `+`로 encode해 Polaris가 404를 반환한다 (C O-1, upstream) |
| `POST /v1/{prefix}/namespaces` (create) | Yes (CREATE FOLDER) | Yes. `location` 자동 설정 | client-probe, live-P3 | PASS | 1–3 level. 부모 없음은 validation error, 403은 permission error (REST v3 403) |
| `GET /v1/{prefix}/namespaces/{ns}` (load) | Yes (namespace location → CTAS, folder listing) | Yes | client-probe, live-P3 (CTAS) | PASS | allowedNamespaces의 조상 folder는 load하지 않는다 (Phase 3) |
| `POST …/namespaces/{ns}/properties` (update) | 제한적 (storageUri 변경은 거부) | Yes | static | NOT_SUPPORTED (검증하지 않음: Dremio에서 쓰는 경로 없음) | Dremio SQL/UI에 namespace property를 바꾸는 경로가 없다 (REST v3 folder storage URI 변경은 거부, C-09). known-limitations N-15 |
| `DELETE /v1/{prefix}/namespaces/{ns}` (빈 namespace) | Yes (DROP FOLDER) | Yes | client-probe, live-P3 | PASS | 없는 namespace는 "Folder does not exist" |
| `DELETE …/namespaces/{ns}` (비어 있지 않음) | Yes | 400 `NamespaceNotEmptyException` | client-probe, unit, live-P3 | PASS | Phase 3: "Folder [..] cannot be deleted because it is not empty…" validation error (G-10 해결). 표준 409도 같은 메시지 |
| `GET …/namespaces/{ns}/tables` (list) | Yes | Yes | client-probe, live-P3 | PASS | live-P5 B-1–B-6: `SHOW TABLES IN …`, `INFORMATION_SCHEMA` 모두 정상. Phase 3의 "0 rows"는 harness `sql.sh` bug였다 (수정, N-06) |
| `GET …/tables/{t}` (load) | Yes (metadata). REST FileIO는 버리고 DremioFileIO 사용 | Yes | client-probe, live-P2, live-P5 | PASS | live-P5 B-7–B-22 (type, filter, aggregate, join, pruning, time travel, metadata table). 외부 변경은 최대 약 60초 stale (N-12) |
| `HEAD …/tables/{t}` (exists) | Yes (dataset lookup) | Yes | unit, live-P3 | PASS | 401/403은 permission error로 매핑 (Phase 3) |
| `POST …/tables` (create) | Yes (CREATE TABLE) | Yes. location은 namespace 아래여야 함 | client-probe, live-P2, live-P5 | PASS | live-P5 C-1–C-4. out-of-tree LOCATION은 403 (G-22) |
| `POST …/tables` (stage-create) + commit | Yes (CTAS: `newCreateTableTransaction`) | Yes | client-probe, live-P3, live-P5, unit | PASS | 3 level namespace 포함. 403은 permission error (`CREATE_TABLE_STAGED`). Partitioned CTAS는 Phase 5에서 commit할 spec/sort order/location/property로 다시 stage해 commit한다 (수정 전에는 Polaris default spec이 unpartitioned로 남음, C-6, I-2–I-4) |
| `POST …/tables/{t}` (commit / update) | Yes (INSERT, DML, ALTER, OPTIMIZE) | Yes | live-P2, live-P3, unit | PASS (INSERT, UPDATE, DELETE, MERGE) | 403은 permission error (이전에는 `WRITER_COMMITTER` SYSTEM ERROR). 403 예외는 Iceberg `CleanableFailure`로 유지되어 거부된 commit의 manifest가 정리된다 (live-P3 INSTANCE=6: 남은 object 0). 409 `CommitFailedException`은 기존 concurrent modification 경로 (unit; live-P5 C-24: 동시 INSERT의 409 8건을 client가 자동 재시도, C-25: 동시 UPDATE는 `CONCURRENT_MODIFICATION`). ALTER/OPTIMIZE/VACUUM/TRUNCATE live-P5 PASS |
| `POST /v1/{prefix}/tables/rename` | No (Dremio에 rename API 없음) | Yes (cross-namespace 포함) | client-probe | NOT_SUPPORTED | |
| `DELETE …/tables/{t}?purgeRequested=false` | Yes (DROP TABLE) | Yes (204) | client-probe, live-P2 | PASS | data file이 남는다 |
| `DELETE …/tables/{t}?purgeRequested=true` | No | drop-with-purge 설정 필요 | — | NOT_SUPPORTED | |
| `GET …/namespaces/{ns}/views` (list) | Yes (`views_supported`) | Yes | client-probe, live-P3 | PASS | INFORMATION_SCHEMA에 view 포함 |
| `GET …/views/{v}` (load) | Yes (첫 SQL representation 사용) | Yes | client-probe, live-P5 | PASS | dialect 우선순위 무시 (G-25, live-P5 D-14 확인). spark 전용 view도 SQL이 호환되면 읽힘 (D-13) |
| `POST …/views` (create) | Yes (dialect `DremioSQL`) | Yes | client-probe, live-P3, live-P5 | PASS | live-P5 D-7: dialect `DremioSQL`, format-version 1 |
| `POST …/views/{v}` (replace) | Yes (CREATE OR REPLACE) | Yes | live-P5 | PASS | live-P5 D-8: version 2개, current 2. `ALTER VIEW … AS`는 parser error (NOT_SUPPORTED, C-21) |
| `DELETE …/views/{v}` (Polaris 기본 설정) | Yes | 403 `Unable to purge entity` | client-probe, live-P3 | PASS (오류 매핑) | Phase 3: permission error + `polaris.config.drop-with-purge.enabled` hint (G-06 해결). drop 자체는 catalog 설정이 필요하다 |
| `DELETE …/views/{v}` (`polaris.config.drop-with-purge.enabled=true`) | Yes | 204 | client-probe, live-P5 | PASS | live-P5 D-15 |
| `POST /v1/{prefix}/views/rename` | No | Yes | client-probe | NOT_SUPPORTED | |
| `X-Iceberg-Access-Delegation: vended-credentials` | Yes (Phase 4): `isUsingVendedCredentials=true`이면 header 전송, loadTable/staged create 응답 `config`의 `s3.*` credential을 table별 S3 FileSystem에 적용, 만료 전 갱신 | Yes. `CATALOG_MANAGE_CONTENT`만으로 loadTable 200과 `s3.*`(MinIO STS `AssumeRole`, table prefix scope) 반환 | unit, live-P4 | PASS (Polaris OSS + MinIO, single node) | G-04 해결. S3 credential만 옮기고 endpoint/region은 source 설정 (bucket discovery는 끈다). static key도 있는 source는 아직 없는 table을 static key로 쓴다. vended만 있는 source의 기본 location 밖 `LOCATION`은 NOT_SUPPORTED ([storage.md §12](storage.md#12-vended-credentials-compatibility-결과)) |

---

## (c) SQL 기능

- "Gating option"이 false이면 해당 기능이 비활성화된다. 모든 기능은 공통으로 `plugins.restcatalog.enabled=true`가 필요하다.
- Phase 2부터 Source를 만들 수 있다 (G-01 해결). Phase 5에서 정식 E2E로 검증했다 ([test-results.md §5](test-results.md#5-phase-5--실제-e2e--regression)의 시나리오 번호).
- "기대 상태"는 Phase 2 완료 후 코드상 기대되는 동작이다.

| SQL | 코드 경로 | Gating option | 기대 상태 (Phase 2 이후) | Status |
|---|---|---|---|---|
| `SHOW SCHEMAS` / folder 탐색 | `AbstractRestCatalogAccessor` namespace listing | `plugins.restcatalog.enabled` (+ allowedNamespaces) | 지원 | PASS (live-P5 B-1, D-1–D-6) |
| `SHOW TABLES` / dataset discovery | `listDatasetHandles` | 동일 | 지원 | PASS (live-P5 B-2–B-6. Phase 3의 0 rows는 harness bug, N-06) |
| `SELECT` | `getDatasetMetadata` / `listPartitionChunks` → DremioFileIO | 동일 | 지원 | PASS (live-P5 B-7–B-18) |
| `SELECT … AT SNAPSHOT/TIMESTAMP` | `TimeTravelProcessors` | 동일 | 지원 | PASS (live-P5 B-19–B-21) |
| `CREATE TABLE` | `createEmptyTable` → `buildTable().create()` | `plugins.restcatalog.mutable.enabled` | 지원 | PASS (live-P5 C-1–C-4) |
| `CREATE TABLE … AS SELECT` (CTAS) | `createNewTable` → staged create. namespace `location` 필요 (Polaris는 자동 설정) | `mutable.enabled` | 지원. PARTITION BY는 upstream 26.0.8 fix가 없어 Phase 5에서 plugin이 수정 (`stagedCreateOperations`) | PASS (live-P5 C-5–C-8, C-26, I-2–I-6. partitioned는 수정 전 FAIL) |
| CTAS with out-of-tree `LOCATION` | 위와 동일 | `mutable.enabled` | Polaris 403 → permission error (Phase 3 매핑, unit). Polaris는 namespace location 밖의 `LOCATION`을 commit에서 거부한다 ("Invalid locations … not in the list of allowed locations", unstructured table location 기본 off) | namespace 안, 기본 table location 밖: PASS (live-P4 review INSTANCE=6, static key source와 vended+static key source). vended credential만 있는 source는 NOT_SUPPORTED ([storage.md §12.4](storage.md#124-known-limitations)) |
| `INSERT INTO` | `IcebergCatalogModel` commit | `mutable.enabled` | 지원 | PASS (live-P5 C-9, C-24) |
| `UPDATE` / `DELETE` / `MERGE` | `FileSystemTableModifyPrule:44` (`SupportsIcebergRestApi`) | `mutable.enabled` | 지원. positional delete 관련 upstream 26.1.6 fix가 없음 | PASS (live-P5 C-14, C-25: copy-on-write, delete file 0) |
| `OPTIMIZE TABLE` / `VACUUM TABLE` | `FileSystemTableOptimizePrule:44`, `FileSystemVacuumTablePrule:43` | `mutable.enabled` | 지원 (`REMOVE ORPHAN FILES` 제외) | PASS (live-P5 C-16–C-18). `VACUUM … REMOVE ORPHAN FILES`는 NOT_SUPPORTED (C-20) |
| `ALTER TABLE ADD/DROP/CHANGE COLUMN` | `IcebergCatalogModel` | `mutable.enabled` | 지원 | PASS (live-P5 C-10, B-16) |
| `ALTER TABLE … PRIMARY KEY` / `LOCALSORT` / `SET/UNSET TBLPROPERTIES` / partition field | `IcebergCatalogModel` | `mutable.enabled` | 지원 | PASS (live-P5 C-11, C-12). PRIMARY KEY는 nullable column이라 NOT_SUPPORTED (C-19) |
| `TRUNCATE TABLE` / `ROLLBACK TABLE` | `IcebergCatalogModel` | `mutable.enabled` | 지원 | TRUNCATE PASS (live-P5 C-15). `ROLLBACK TABLE … TO SNAPSHOT` PASS (live-P6 P6-7). timestamp rollback은 NOT_SUPPORTED (검증하지 않음, 범위 밖) |
| `ALTER TABLE … RENAME` | 구현 없음 | — | 미지원 | NOT_SUPPORTED |
| `DROP TABLE` | `dropTable(purge=false)` | `mutable.enabled` | 지원. data file은 남음 | PASS (live-P5 C-20, C-21). 없는 table: `DROP TABLE IF EXISTS` "not found", `DROP TABLE` 오류 (Phase 6 수정 C-17, unit, live-P6 P6-3/P6-4). Catalog가 drop을 400/422로 거부하면 UNSUPPORTED_OPERATION ERROR라 `IF EXISTS`도 실패로 보고 (최종 review, unit) |
| `CREATE [OR REPLACE] VIEW` / `ALTER VIEW` | `IcebergCatalogViewProvider`, dialect `DremioSQL` | `plugins.restcatalog.views_supported` + `mutable.enabled` | 지원 | PASS (live-P3, live-P5 D-7, D-8). `ALTER VIEW … AS`는 parser error로 NOT_SUPPORTED (C-21) |
| `SELECT` from view (Dremio에서 생성) | 첫 SQL representation | `views_supported` | 지원 | PASS (live-P3) |
| `SELECT` from view (Spark에서 생성) | 첫 SQL representation (spark dialect) | `views_supported` | SQL 호환성에 따라 다름 (G-25) | PASS (live-P5 D-13: Polaris API로 만든 spark 전용 view 읽힘, D-14: 여러 dialect면 첫 representation) |
| `DROP VIEW` | `ViewCatalog.dropView` | `views_supported` + `mutable.enabled` | Polaris catalog에 `polaris.config.drop-with-purge.enabled=true`가 있으면 지원, 없으면 403 (G-06) | PASS (live-P3: true면 COMPLETED, false면 hint가 붙은 permission error) |
| `CREATE FOLDER` / `DROP FOLDER` (namespace) | `createFolder` / `deleteFolder` | `plugins.restcatalog.folders_supported` + `mutable.enabled` | 지원. 비어 있지 않은 namespace는 명확한 validation error (G-10 해결) | PASS (live-P3, live-P5 D-1–D-6). `DROP FOLDER IF EXISTS`는 parser error (C-21) |
| Folder storage URI 변경 | `updateFolder` → `validateStorageUri` 거부 | — | 미지원 | NOT_SUPPORTED |
| Read-only 모드 (`mutable.enabled=false`) | `getId()` throw | `mutable.enabled` | read 경로 영향은 미확인 (G-23) | NOT_SUPPORTED (검증하지 않음: 기본값이 아닌 support option, upstream code 그대로, known-limitations C-13) |

---

## (d) Storage / Auth 모드

### Storage

상세 설정과 matrix: [storage.md](storage.md).

| 모드 | 설정 | 검증 | Status | 비고 |
|---|---|---|---|---|
| AWS S3 static key (공식 Polaris OSS recipe) | `fs.s3a.aws.credentials.provider=SimpleAWSCredentialsProvider`, secret `fs.s3a.access.key`/`fs.s3a.secret.key` | static, unit | ENVIRONMENT_BLOCKED | 실제 AWS 계정 없음. 같은 key 묶음 + MinIO endpoint로는 PASS (아래) |
| 공식 baseline + MinIO endpoint (필수 성공 기준) | 위 + `fs.s3a.endpoint=<host>:<port>`, `fs.s3a.connection.ssl.enabled=false`, `fs.s3a.path.style.access=true`, `dremio.s3.compat=true` | live-P4 (A INSTANCE=1, Integration `base`) | PASS | CREATE/INSERT/CTAS/INSERT…SELECT/UPDATE/DELETE, metadata table, `AT SNAPSHOT`, 재시작. Polaris snapshot id 일치 |
| MinIO recipe의 key별 필요 여부 | endpoint·compat·(HTTP면) ssl false 필수. path-style은 hostname endpoint면 필수. discovery/`dremio.s3.region`/requester-pays는 불필요 | live-P4 (A, B) | PASS | Phase 2 recipe(모든 key 포함)도 PASS. [storage.md §4](storage.md#4-property-reference) |
| `fs.s3a.aws.credentials.provider` 생략 | — | unit, live-P4 (`noprov`, review INSTANCE=6 재확인) | PASS (Phase 4 수정) | 이전에는 `Invalid AWSCredentialsProvider provided` (FAIL). plugin이 Hadoop 기본 chain(만)을 `SimpleAWSCredentialsProvider`로 바꾼다. key가 없으면 fail closed (아래 env fallback 행) |
| `fs.s3a.endpoint`에 scheme 포함 (`http://minio:9000`) | — | unit (`TestS3FileSystem`), live-P4 (`scheme`, `scheme2`, B의 `https://` TLS) | PASS (Phase 4 수정) | 이전에는 `http://http://…`로 hang (FAIL, G-08). `S3FileSystem.getEndpoint()`가 scheme을 유지한다. 권장 형식은 여전히 `host:port` |
| TLS (self-signed CA) | truststore (`DREMIO_JAVA_SERVER_EXTRA_OPTS`, Polaris `JAVA_OPTS_APPEND`) | live-P4 (B) | PASS | truststore 없으면 PKIX (Dremio), 422 (Polaris) |
| Non-default region MinIO | `fs.s3a.endpoint.region=<region>` | live-P4 (B) | PASS | `dremio.s3.region`만으로는 S3A가 us-east-1로 서명해 FAIL |
| AWS region id가 아닌 region 이름 (`minio-local`) | — | live-P4 (B) | NOT_SUPPORTED | `S3PluginUtils` region 검사 |
| Requester-pays 기본값 (true) | `S3ClientProperties.java:129` | live-P4 (B MinIO trace) | PASS (MinIO) / ENVIRONMENT_BLOCKED (AWS requester-pays bucket) | MinIO는 header 무시. AWS 일반 bucket은 `false` 권장 |
| Bucket discovery 기본값 (true) | `dremio.bucket.discovery.enabled` | live-P4 (A E11, B) | PASS (MinIO) / ENVIRONMENT_BLOCKED (AWS, `ListAllMyBuckets` 없는 user) | MinIO는 거부 대신 filtering. vended credential을 쓰는 table FS는 discovery를 항상 끈다 (table prefix로 scope된 credential에는 `ListAllMyBuckets`가 없다. unit) |
| Key도 provider도 없는 source (fail closed) | access key 없음, provider 생략 | unit, live-P4 review (INSTANCE=6 `snp`) | PASS (fail closed) | Dremio host의 `AWS_*` env나 EC2 instance profile로 넘어가지 않는다: 읽기 PERMISSION ERROR, 쓰기 `SimpleAWSCredentialsProvider: No AWS credentials in the Hadoop configuration`. Hadoop 기본 chain을 그대로 두면 S3A가 env provider까지 시도했다 (review 1차 live: "Unable to load AWS credentials from environment variables") |
| S3 env credential / InstanceProfile fallback (opt-in) | provider를 **빈 값으로 명시**, access key 없음 | live-P4 (A C5, review `sne`) | instance profile: ENVIRONMENT_BLOCKED (EC2 아님, IMDS 연결 실패). `AWS_*` env: NOT_SUPPORTED (검증하지 않음: AWS 계정과 무관하게 test 가능하지만 실행하지 않음) | vended source에서 credential을 받지 못한 table도 이 경로를 탄다. [security.md §7.4](security.md#74-phase-4-storage-secret-처리-원칙) |
| S3 assumed role | `fs.s3a.assumed.role.arn` + `com.dremio.plugins.s3.store.STSCredentialProviderV1` | static | ENVIRONMENT_BLOCKED | AWS 계정 없음 |
| Azure shared key | `fs.azure.account.key…` | static | ENVIRONMENT_BLOCKED | Azure 계정 없음, 범위 밖 (known-limitations S-11). vended ADLS credential은 매핑하지 않는다 |
| Polaris FILE storage (local test 전용) | Polaris flag 2개와 readiness ignore | client-probe | NOT_SUPPORTED (Dremio 경로는 검증하지 않음, 범위 밖) | Polaris local test 전용 storage (known-limitations S-11). Dremio에서 `file://` 경로로 읽는 것은 미검증 |
| Polaris 서버 측 S3 (MinIO) metadata write | storageConfigInfo `endpoint`(scheme 필수) + `pathStyleAccess` + `region`, AWS_* env | client-probe, live-P4 (B trace) | PASS | `metadata.json`은 Polaris, data/manifest는 Dremio가 쓴다. Polaris는 vended 요청이 없어도 STS subscoped key를 쓴다 |
| Vended credentials (`isUsingVendedCredentials=true`) | header + Polaris grant (`CATALOG_MANAGE_CONTENT`) | unit (33), live-P4 (C, Integration `vn`, review INSTANCE=6 `vn`/`vk`/`vbp`) | PASS (compatibility 결과, single node) | static S3 key 없이 SELECT/INSERT/CTAS/UPDATE/DELETE/OPTIMIZE, 만료 후 갱신. static key도 있는 source는 새 table을 static key로 쓴다 (기본 location 밖 `LOCATION` PASS). vended만 있는 source의 기본 location 밖 `LOCATION`은 NOT_SUPPORTED, read-only INSERT는 SYSTEM `AmazonS3Exception`. AWS vending과 multi-node는 ENVIRONMENT_BLOCKED |
| Executor storage 설정 (multi-node) | `start()`/`getFsConfCopy()`에서 eager copy. vended면 node별로 catalog에 직접 조회 | unit | PASS (unit) | G-07 해결. multi-node 실측은 Phase 5 미실행 (환경 없음, ENVIRONMENT_BLOCKED, known-limitations S-02) |

### Auth

| 모드 | 설정 | 검증 | Status | 비고 |
|---|---|---|---|---|
| OAuth2 client_credentials | `credential=<client_id>:<client_secret>`, `scope=PRINCIPAL_ROLE:ALL` | client-probe, proto-E2E, live-P2, live-P3 | PASS | Phase 3: 잘못된 credential / scope / warehouse, 닿지 않는 endpoint는 source 생성 400 응답의 `errorMessage`에 원인 hint가 나온다 (G-09 해결) |
| OAuth2 scope 미지정 | — | client-probe, live-P3 (A) | FAIL (Polaris 요구사항) | Polaris가 400 `invalid_scope`로 거부한다. Phase 3부터 "Set the 'scope' catalog property" hint가 API 응답에 나온다 (G-05 hint 해결) |
| OAuth2 dedicated principal (root 아님) | `scope=PRINCIPAL_ROLE:dremio_role` | client-probe, live-P3 | PASS | `CATALOG_MANAGE_CONTENT`. read-only principal은 SELECT 성공, 쓰기는 permission error. `NAMESPACE_LIST`가 없으면 state `warn` (allowedNamespaces 미설정 시) |
| 401 / 403 / token 만료 / catalog 재시작 | — | unit, live-P3 | PASS | 403과 401은 동작별 permission error (Phase 6: 401을 view/folder/CTAS/commit/drop table까지 확장). Polaris 재시작(새 signing key) 후 cached client가 401이면 state check가 새 client로 교체한다 |
| Timeout / Polaris unavailable / 429 / 5xx | `rest.client.*-timeout-ms`, `rest.client.max-retries` | unit, live-P3, live-P5 (F-12–F-14), live-P6 (P6-5) | PASS (복구, hint) | timeout 미설정 시 hang은 일반 메시지("Source is not currently available"). 설정하면 "did not respond in time" hint. Phase 6: query의 table lookup/load 오류도 원인별 hint와 redaction (C-08 수정; live-P6 Polaris 중지 직후 SELECT "Unable to reach the Iceberg REST catalog for the request to load table …") |
| TLS 실패 (self-signed, scheme 불일치) | — | unit | PASS (hint) | Phase 3 integration: TLS hint와 trust store 안내. credential은 handshake 전에 실패하므로 전송되지 않는다 |
| Static bearer `token` | `token=<bearer>` | static | NOT_SUPPORTED (검증하지 않음, 범위 밖) | Iceberg 표준 property를 그대로 넘긴다. 만료 후 갱신 수단이 없어 `credential` 사용을 권한다 (known-limitations K-07) |
| External IdP (`oauth2-server-uri`) | `oauth2-server-uri=<idp>` | static, live-P3 (Polaris 자체 token endpoint를 명시) | PASS (Polaris endpoint) / ENVIRONMENT_BLOCKED (외부 IdP 없음) | |
| SigV4 (`rest.sigv4-enabled`) | Glue/S3 Tables용 | static | NOT_SUPPORTED | Polaris에는 해당 없음 |
| Secret masking (API GET/PUT) | `secretPropertyList` | proto-E2E, unit, live-P2 (v3 GET, v2 GET, masked PUT 후 state good) | PASS | `$DREMIO_EXISTING_VALUE$` |
| Secret at-rest 암호화 | `List<Property>` | static, live-P4 (D) | NOT_SUPPORTED (기본) / PASS (`dremio-admin encrypt` 값 사용 시) | G-12. 평문 대신 `secret:1.…`를 넣으면 KV에 암호문만 남는다 (key는 같은 data dir). kernel 전체 범위의 open item |
| `propertyList`(plain)에 secret key | — | unit (backend WARN, key 이름만), UI spec (validator가 저장 차단) | PASS | G-13. UI와 backend가 같은 판정 규칙을 쓴다 (Phase 6: unit `testSensitiveKeyRuleIsInSyncWithUi`가 `sourceUtils.ts` 목록과 비교). Edit에서는 이미 저장된 key는 막지 않고 새/이름 바뀐 row만 막는다. API로 저장하면 backend WARN만 남는다 |
| Secret 로그 노출 (기본 log level) | — | proto-E2E, unit, live-P2, live-P3, live-P4 (DEBUG, vended 값 포함 0건) | PASS | HttpClient 5 wire/headers, HttpClient 4 wire/headers(S3A), SigV4 signer, Netty `LoggingHandler`/`Http2FrameLogger`(AWS SDK v2 async S3 읽기) logger는 `conf/logback.xml`에서 INFO로 고정된다 (HttpClient 4와 Netty는 Phase 4 추가. Netty `LoggingHandler`를 DEBUG로 두면 session token이 찍히는 것을 unit으로 확인). 상세는 [security.md](security.md) |
