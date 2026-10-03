# Phase 3: Polaris OAuth2 / Catalog 연동

- 작업 정의: [polaris-catalog-support.md](../reference_docs/catalog-support/polaris-catalog-support.md) Phase 3 (Agent A OAuth2, B Namespace/Table, C Allowed Namespace, D 오류/보안)
- 기준: `feature/polaris-restcatalog` (Phase 2 `f2553db4a`, CI/log fix `ffd1cc1b2`) + Phase 3 작업 트리. Apache Polaris `1.1.0-incubating`, Iceberg `1.7.0-5f7c992-20250730084652-3bf8b99`, 로컬 MinIO.
- 원칙: Iceberg `RESTCatalog`의 OAuth2와 HTTP client를 그대로 쓴다. 별도 OAuth HTTP client나 Polaris 전용 engine은 만들지 않았다. Polaris 이름은 hint 문구에만 나온다.
- 상태 값: `PASS` / `FAIL` / `ENVIRONMENT_BLOCKED` / `NOT_SUPPORTED`. 보안 관련 상세는 [security.md](security.md)에 있다.

## 1. 요약

| Gap | 내용 | Phase 3 결과 |
|---|---|---|
| G-09 | source 생성 API가 일반 메시지("Could not connect …")만 반환. hint는 server.log에만 있었다 | 해결. API 400 응답의 `errorMessage`에 원인별 hint와 redact된 detail이 나온다 |
| G-11 | `checkState`가 매번 새 REST catalog를 만들고 token을 발급받았다 | 해결. cached client 재사용, `listNamespaces(root)` 1회. 필요할 때만 새 client로 교체 |
| G-10 | 비어 있지 않은 namespace DROP이 `BadRequestException` raw 메시지 | 해결. "Folder [..] cannot be deleted because it is not empty…" |
| G-06 | DROP VIEW 403이 raw `ForbiddenException` | 해결. permission error와 `drop-with-purge` hint. 다른 403도 동작별 permission error |
| G-05 (hint) | `warehouse`/`scope` 누락 시 원인 불명 | 해결 (hint). Polaris 요구사항 자체는 그대로 |
| G-22 | CTAS/commit 403이 SYSTEM ERROR | 해결 (unit). permission error로 매핑. out-of-tree LOCATION live는 Phase 5 |
| allowedNamespaces | 실측 | nested entry의 부모 folder가 names refresh마다 삭제되던 bug(B-1)와 listing 순서 bug(Integration live에서 발견) 수정. 중복 discovery(B-3), ERROR log spam(B-4) 수정 |

## 2. 재현 방법 (`scripts/polaris-e2e`)

Harness는 Polaris container, scratchpad로 복사한 Dremio tarball, MinIO prefix를 INSTANCE(0..9)별로 분리해 띄운다. 상세는 [scripts/polaris-e2e/README.md](../../scripts/polaris-e2e/README.md).

```bash
scripts/dev build plugins/icebergcatalog distribution/resources distribution/server
cd scripts/polaris-e2e
export INSTANCE=5 E2E_WORK=<scratch>/p3 S3_PREFIX=polaris-p3-<name>
export S3_ACCESS_KEY=<s3AccessKey> MINIO_SECRET_KEY=<s3SecretKey>   # 환경 변수로만 전달
export POLARIS_EXTRA_ENV='polaris.authentication.token-broker.max-token-generation=PT60S'  # token 만료 test
# 압축 해제 디렉터리가 stale일 수 있으므로 tarball을 새로 풀어 DREMIO_TARBALL_DIR로 지정한다
export DREMIO_TARBALL_DIR=<scratch>/tarball/dremio-community-<version>

./polaris-up.sh && ./dremio-up.sh
SOURCE_NAMES_REFRESH_MS=60000 ./source.sh create polaris            # state good
SOURCE_CREDENTIAL='root:<wrong>' ./source.sh create bad              # 400 + OAuth2 hint
SOURCE_OMIT_PROPS=warehouse ./source.sh create nowh                  # 400 + warehouse hint
SOURCE_URI=http://127.0.0.1:1/api/catalog ./source.sh create unreach # 400 + unreachable hint
SOURCE_SCOPE=bogus ./source.sh create badscope                       # 400 + invalid_scope hint
./polaris-principal.sh reader && SOURCE_PRINCIPAL=reader ./source.sh create ro
./polaris-principal.sh nolist CATALOG_READ_PROPERTIES && SOURCE_PRINCIPAL=nolist ./source.sh create nolist  # warn
SOURCE_ALLOWED_NS="sales.eu" SOURCE_NAMES_REFRESH_MS=60000 ./source.sh create scoped
./sql.sh "DROP FOLDER polaris.ops.l2"                                # not-empty 메시지
./scan-secrets.sh                                                     # 0이어야 한다
./source.sh delete <name>; ./dremio-down.sh --purge; ./polaris-down.sh; ./s3.sh clean
```

주의:
- 새 namespace는 names refresh(최소 60초, 실측 60–120초) 뒤에 Dremio tree에 보인다. query로 직접 접근하면 즉시 등록된다. source를 만들기 전에 있던 namespace는 바로 보인다.
- `bash -x`로 실행하지 않는다 (trace에 secret이 찍힌다).
- `DROP TABLE`은 purge=false라 S3 object가 남는다. `s3.sh clean`으로 지운다.
- Polaris container는 in-memory persistence다. container를 다시 만들면 catalog와 namespace가 사라진다 (`polaris-up.sh`가 catalog와 권한을 다시 만든다).

## 3. 코드 변경

모두 `plugins/icebergcatalog/src/main/java/com/dremio/plugins/icebergcatalog/store/` 아래다.

| 파일 | 변경 | 담당 |
|---|---|---|
| `RestIcebergCatalogPlugin.java` | G-09: 시작된 instance의 실패 state를 `RECENT_FAILURES`(source 이름+endpoint, 2분, redact된 state만)에 기록하고, 생성 실패 후 Dremio가 되돌려 놓은 미시작 instance가 그 state를 반환한다. suggested user action을 "Could not connect to X. \<hint\> Details: …"로 바꿨다. OAuth2 error code(`invalid_client`, `unauthorized_client`, `invalid_scope`, `invalid_grant`, `unsupported_grant_type`, `invalid_request`), warehouse 누락/불일치, 5xx, timeout을 분류한다. namespace listing 403은 allowedNamespaces가 없으면 `warn`, 있으면 `good`. `createCatalog`는 raw 예외 문자열을 넣지 않는다 | A |
| `RestIcebergCatalogPlugin.java` | TLS(`SSLException`) hint, 503(`ServiceUnavailableException`) hint, 분류되지 않는 HTTP 오류(404/409/429/502/504)용 hint, server detail의 HTML 제거·300자 제한(자르기 전후 redact), `rest.client.*` key를 Hadoop conf에서 제외, `createFolder`의 permission error를 `CatalogEntityForbiddenException`(REST v3 403)으로 변환 | Integration (D P-1/P-2, B 제안) |
| `IcebergRestCatalogAccessor.java` | G-11: `checkStateInternal`이 cached client로 `listNamespaces(root)`를 호출한다. cached client가 없거나 HTTP 401(`NotAuthorizedException`, stale session)로 실패할 때만 새 client로 확인하고, 성공하면 교체하면서 table/view cache도 비운다(cache의 table operations가 닫힌 client에 묶여 있다). 그 밖의 오류(network, timeout, 5xx, 429 등)는 cached client를 유지하고 바로 보고한다 (Review 반영: 이전에는 network 외 모든 오류에서 교체해 일시적 5xx 한 번에 사용 중인 client가 닫혔다). root listing 403은 table/view cache를 비우지 않는다. `getDefaultBaseLocation`은 cached client를 쓴다(이전에는 매번 새 client를 만들고 닫지 않았다) | A, Review |
| `ExpiringCatalogCache.java` | `getIfPresent()`, `replace()` 추가. Integration에서 `replace()`가 새 client를 먼저 넣고 이전 client를 나중에 닫도록 고쳤다 (동시 `get()`이 null을 볼 수 있던 race). Review 반영: `close()`가 `closed` flag를 세우고 `invalidate()`도 lock을 잡는다. 닫힌 뒤 `replace()`에 온 client는 바로 닫는다 (state check와 plugin close가 겹칠 때의 client leak) | A, Integration, Review |
| `RestCatalogExceptionMapper.java` (신규) | Iceberg 예외 type과 server message만으로 `UserException`을 만든다. 403 → permission error + privilege hint 또는 purge hint, not-empty → validation error, 부모 namespace 없음 / 이미 존재 → validation error. Integration에서 401 → permission error(`notAuthorized`) 추가. Review 반영: plugin의 `redactSecrets`를 받아 server message를 redact하고 300자로 줄인다(`abbreviateDetail`을 plugin에서 옮김). cause로 남기는 예외도 redact된 사본으로 바꾼다 | B, Integration, Review |
| `AbstractRestCatalogAccessor.java` | `loadTable`/`loadView`, `createTable`, `createView`, `updateView`, `dropView`, `dropTable`, `createFolder`, `dropFolder`, CTAS staging, commit(`ForbiddenMappingTableOperations`)의 403 매핑. `dropFolder`가 404면 `NoSuchNamespaceException` | B |
| `AbstractRestCatalogAccessor.java`, `RestIcebergCatalogPlugin.java` | Review 반영: (1) commit 403은 `UserException`이 아니라 `CommitForbiddenException`(`ForbiddenException` 하위 class, permission error를 cause로 가짐)으로 던진다. `ForbiddenException`은 Iceberg `CleanableFailure`라 `SnapshotProducer`/`BaseTransaction`이 거부된 commit의 manifest/manifest list를 지운다 (`UserException`으로 바꾸면 strict cleanup 때문에 지우지 않는다). Dremio의 `UserException` builder는 cause chain의 `UserException`을 그대로 쓰므로 사용자에게는 같은 PERMISSION ERROR가 보인다. (2) `dropFolder`는 409 `NamespaceNotEmptyException`을 그대로, Polaris 400 "not empty"는 `NamespaceNotEmptyException`으로 바꿔 던지고(typed 계약 유지), plugin `deleteFolder`가 validation error로 바꾼다 (`DropFolderHandler`가 `CatalogFolderNotEmptyException`을 잡지 않기 때문). (3) folder listing은 listing에 실제로 들어간 namespace 기준으로 빠진 조상을 property 없이 넣는다: 허용된 부모의 property를 읽지 못해도(403/404) 자식보다 먼저 나온다 | Review |
| `AbstractRestCatalogAccessor.java` | Integration: dataset lookup(`getDatasetHandle`의 `tableExists`/`viewExists`) 401/403 매핑 (D P-3). allowedNamespaces: nested entry의 조상 folder를 property 없이 listing에 추가(C B-1)하고 folder listing을 부모가 먼저 오도록 정렬(live에서 발견: Dremio names refresh는 부모보다 먼저 나온 folder를 삭제한다), recursive일 때 다른 entry에 포함된 entry를 discovery 시작점에서 제외(C B-3), folder listing 중복 제거, 없는/금지된 namespace는 stack trace 없는 WARN 한 줄(C B-4) | Integration (C/D 제안) |

동작 변화:
- state check가 top-level namespace를 list하므로 catalog 수준 `NAMESPACE_LIST`가 없는 principal은 allowedNamespaces가 없을 때 `warn`이 된다 (이전에는 `good`).
- REST v3 `POST /api/v3/catalog`로 folder를 만들 때 403이면 HTTP 403과 실제 원인 메시지를 반환한다. SQL `CREATE FOLDER`는 validation error로 같은 메시지를 보여준다.
- allowedNamespaces는 discovery 범위만 정한다. 직접 경로로 하는 SELECT/CREATE는 막지 않는다 (C B-2, 설계 결정으로 유지. Polaris grant로 제한한다).

변경하지 않은 것 (제안만 기록):
- `rest.client.connection-timeout-ms` / `socket-timeout-ms` 기본값 주입(D P-2): generic RESTCATALOG의 기존 동작을 바꾸므로 넣지 않고 권장값으로 문서화했다 ([security.md §5](security.md#5-재시도와-timeout-iceberg-rest-client)).
- allowedNamespaces 강제(B-2), non-recursive에서 직접 하위 namespace를 빈 folder로 보이는 동작(B-5): 문서화만 했다.
- 정확한 HTTP status 표기(404/409/429/502): Iceberg 예외가 status를 남기지 않는다. HTTPClient를 감싸야 하므로 하지 않고 후보 status를 나열하는 hint로 대신했다.

## 4. Agent별 시나리오 결과

### 4.1 Agent A — OAuth2 (INSTANCE=1, live + unit)

| 시나리오 | 결과 | Status |
|---|---|---|
| 정상 credential (`root`, `PRINCIPAL_ROLE:ALL`) | 200, `good` | PASS |
| 잘못된 client secret | 400 "…rejected the credentials (OAuth2 error 'unauthorized_client')…". Polaris는 401이지만 Iceberg가 `BadRequestException`으로 바꾼다 | PASS |
| 잘못된 scope / scope 누락 | 400 `invalid_scope` hint (설정 여부에 따라 문구가 다름) | PASS |
| `warehouse` 누락 / 잘못된 warehouse | 400 warehouse hint / "did not accept the warehouse … Unable to find warehouse" | PASS |
| 닿지 않는 endpoint | 400 "Unable to reach …" | PASS |
| 잘못된 secret / warehouse 없이 update | 400 "Failure creating/updating this source …SourceBadStateException … \<hint\>". hint는 있으나 kernel이 문구를 중복/감싼다. 기존 plugin은 `good` 유지 | PASS (hint). 문구는 kernel issue |
| `CATALOG_READ_PROPERTIES`만 가진 principal | 200, `warn` (namespace listing 403). allowedNamespaces를 주면 `good`, SELECT는 LOAD_TABLE 403 | PASS |
| `PRINCIPAL_ROLE:nonexistent_role` | 200, `warn` (이전: `good`인데 아무것도 보이지 않음) | PASS |
| Token 만료/갱신 (Polaris token TTL 60초) | 4.5분 동안 SELECT 9회 성공. `/v1/config`는 증가 없음, token은 약 54초마다 갱신, 401 없음 | PASS |
| `token-refresh-enabled=false` + 60초 token | 만료 후 query 1건 "Not authorized", 다음 state check가 client를 교체 | PASS (refresh는 켜 둔다. known limitation) |
| Polaris hang (`docker pause`), timeout 미설정 | 약 2분 뒤 `bad`, 일반 메시지(10초 state check 제한). unpause 후 약 40초에 복구 | PASS (복구). hint 없음 |
| hang + `rest.client.socket-timeout-ms=3000` | `bad`, "did not respond in time … Read timed out" | PASS |
| Polaris 중지 (connection refused) | 약 2분 뒤 `bad`, "Unable to reach …". query도 같은 hint로 빠르게 실패 | PASS |
| Polaris 재시작 (새 signing key) | cached client 401 → 새 client 성공 → 교체, 약 1분 안에 `good` | PASS |
| Secret scan (DEBUG 포함) | 0 | PASS |
| Unit `TestRestCatalogOAuth2` (18) | raw socket emulator: credential/scope 전달, check당 request 수, 짧은 `expires_in` refresh, stale session 교체, 오류별 hint, 미시작 instance의 기록된 실패, log/state secret 0 | PASS |

### 4.2 Agent B — Namespace / Table (INSTANCE=2)

| 동작 | 결과 | Status |
|---|---|---|
| listNamespaces (nested `pre.a.b`, `l1.l2.l3`) | 양방향(Polaris ↔ Dremio)으로 보인다 | PASS |
| createNamespace (`CREATE FOLDER` 1–3 level) | COMPLETED, Polaris에 보인다 | PASS |
| 이미 있는 folder | "Folder [[polaris, ns1]] already exists." | PASS |
| 부모 없는 folder | 이전 REST v3 500 → "Cannot create folder [zz.yy]: parent folder [zz] does not exist…" (SQL error, REST v2 400, v3 409) | PASS (FIXED) |
| dropNamespace (빈 namespace) | COMPLETED | PASS |
| dropNamespace (비어 있음, G-10) | "Folder [ns1] cannot be deleted because it is not empty…" | PASS (FIXED) |
| 없는 namespace drop | 이전 "There was an Catalog error." → "Folder does not exist." | PASS (FIXED) |
| listTables | `INFORMATION_SCHEMA."TABLES"`에 table과 view (nested 포함) | PASS |
| loadTable (Polaris/Dremio에서 만든 table) | SELECT COMPLETED | PASS |
| createTable / CTAS (3 level 포함) | COMPLETED | PASS |
| 이미 있는 table / 없는 namespace에 table | 명확한 validation error | PASS |
| commit: INSERT, UPDATE, DELETE, MERGE | COMPLETED. Polaris `loadTable`에 snapshot 4개 | PASS |
| commit 충돌 (409 `CommitFailedException`) | live 재현 불가. unit에서 그대로 전달되어 기존 concurrent modification 경로를 탄다 | PASS (unit) |
| rename | `ALTER TABLE … RENAME`은 parse error. Dremio에 rename 경로가 없다 | NOT_SUPPORTED |
| dropTable | purge=false로 drop, data와 metadata는 MinIO에 남는다 | PASS |
| 없는 table drop | "Table [polaris.ns1.nope] does not exist." | PASS |
| CREATE VIEW / SELECT / CREATE OR REPLACE / 중복 / 없는 namespace | 모두 정상, 명확한 메시지 | PASS |
| DROP VIEW (`drop-with-purge.enabled=true`) | COMPLETED | PASS |
| DROP VIEW (`drop-with-purge.enabled=false`, G-06) | 이전 raw `ForbiddenException` → purge hint가 붙은 permission error | PASS (FIXED) |
| read-only principal: CREATE FOLDER / DROP FOLDER / DROP TABLE / CREATE·REPLACE·DROP VIEW / CREATE TABLE | 동작 이름과 privilege hint가 붙은 denied error | PASS (FIXED) |
| read-only principal: CTAS / INSERT / DELETE | 이전 `WRITER_COMMITTER` SYSTEM ERROR → PERMISSION ERROR | PASS (FIXED) |
| read-only principal: SELECT table/view | COMPLETED | PASS |
| Unit `TestRestCatalogNamespaceTableOps` (34), `TestRestCatalogAccessor` (+2, 30) | 매핑과 pass-through, commit 성공/403/409, CTAS staging 403, purge 판별, nested folder stream | PASS |

### 4.3 Agent C — Allowed Namespaces (INSTANCE=3)

Polaris 구성 (source 생성 전): `sales`{s_top}, `sales.eu`{s_eu}, `sales.eu.de`{s_de}, `sales.us`{s_us}, `hr`{h_top}, `hr.private`{h_priv}, `Mixed_Case`, `Mixed_Case.Sub-Ns`, `with space`. names refresh 60초.

| 설정 | 결과 | Status |
|---|---|---|
| 미설정 / `[]` / `["."]` | 전체 (`[]`는 `null`로 저장, `.`은 root namespace) | PASS (`with space`는 O-1) |
| `[sales]` recursive / non-recursive | subtree 전체 / `s_top`만 (직접 하위 namespace는 빈 folder, B-5) | PASS |
| `[sales.eu]` recursive / non-recursive, `[sales.eu.de, hr]` | 부모 `sales`가 listing에 없어 names refresh마다 subtree 전체가 삭제/복구를 반복 (B-1) | FAIL → Integration에서 수정 (조상 포함 + 부모 먼저 정렬), §5 #11–#12에서 PASS |
| `[sales, sales.eu]` | tree는 정상, 내부적으로 중복 discovery (B-3) | PASS → Integration에서 중복 제거 |
| `[nonexistent]` | 아무것도 없음, state `good`. refresh마다 ERROR stack trace 4개 (B-4) | PASS → Integration에서 WARN 한 줄로 변경 |
| `[SALES]` | 아무것도 없음 (대소문자 구분) | PASS |
| `[Mixed_Case.Sub-Ns, with space]` | `Mixed_Case → Sub-Ns/m_sub` | PASS |
| `["sales/eu"]` + separator option `/` | `sales.eu` subtree (option은 source 시작 시 읽는다) | PASS |
| allow list 밖 table을 SELECT | COMPLETED, 다음 refresh까지 tree에 보인다 (B-2: 접근 제어가 아님) | 설계 결정 (문서화) |
| allow list 밖 CREATE TABLE / INSERT / CREATE FOLDER | 모두 COMPLETED | 설계 결정 (문서화) |
| 공백이 든 namespace (`with space`) | Iceberg client가 `with+space`로 보내 Polaris 404. allow list와 무관하게 보이지 않는다 (O-1) | FAIL (upstream Iceberg/Polaris interop) |
| Unit `TestRestCatalogAllowedNamespaces` (Integration 후 43) | mock catalog로 recursive/non-recursive, 중첩, 중복, 대소문자, separator(regex), 금지/없는 namespace, folder listing(조상 포함), 직접 lookup | PASS |

Parsing 참고: separator는 regex라 escape하지 않은 `|`는 모든 문자를 나눈다. entry는 trim하지 않는다. 뒤의 separator는 무시되고 앞의 separator는 빈 level을 만든다. 기본 separator로는 이름에 `.`이 든 namespace를 지정할 수 없다 (G-24).

### 4.4 Agent D — 오류 / 보안 (INSTANCE=4, unit + live)

Unit `TestRestCatalogHttpErrors` (Integration 후 30 tests, skip 0): loopback HTTP(S) server가 REST catalog를 흉내 내고 error message에 secret을 echo한다. 실제 `start()`/`getState()`와 `RESTCatalog`를 실행한다.

| 시나리오 | Phase 3 최종 메시지 (요약) | Status |
|---|---|---|
| config 400 / 401 / 403 | "HTTP 4xx" hint, echo된 secret은 `****` | PASS |
| config 404 / 409 / 429 / 502 | "unexpected HTTP error (not 400, 401, 403, 500 or 503), for example 404 … 409, 429 … 502/504 …" + `rest.client.max-retries` 안내. 정확한 status는 표기하지 못한다 | PASS (hint). 정확한 code는 NOT_SUPPORTED (Iceberg 예외에 status 없음) |
| config 429 + `Retry-After: 1` | 기본 5회 재시도(요청 6번, 약 5초) 후 위 hint | PASS |
| config 500 | "server error (HTTP 5xx)", 재시도 없음 | PASS |
| config 502 HTML body | HTML tag 제거, 300자 제한. 재시도함 | PASS (Integration) |
| config 503 | "unavailable (HTTP 503), also after the client retried …" | PASS (Integration) |
| token 401 `invalid_client` / token 400 `invalid_scope` | OAuth2 hint | PASS |
| token 503 | token POST도 재시도. 분류되지 않는 HTTP 오류 hint | PASS |
| read timeout (`socket-timeout-ms=500`) | "did not respond in time … rest.client.*-timeout-ms", 약 0.5초, 재시도 없음 | PASS |
| connection refused | "Unable to reach …" | PASS |
| https + self-signed / https → plain http | "The TLS connection … failed … scheme … certificate … javax.net.ssl.trustStore". handshake 전 실패라 credential 미전송 | PASS (Integration) |
| `tableExists` 401 / 403 | permission error ("rejected the credentials" / "denied the request to look up [ns.t]"). Review 반영: server가 secret을 echo해도 message와 cause chain은 `****` | PASS (Integration, Review) |
| `tableExists` 500 / 404 | raw `ServiceFailureException`(secret 없음) / `Optional.empty()` | PASS |
| `listNamespaces` 403 (discovery) | 예외 없이 빈 목록. catalog walk(table, view)마다 stack trace 없는 WARN 한 줄, server message는 redact. source state에는 `warn`으로 반영 (A). Review 반영 후 이 결과만 허용하도록 test를 고정했다 | PASS |
| config `toString()` / `clearSecrets()` JSON / `SourceState.toString()` | secret 없음 | PASS |

Live (Phase 2 tarball): masking(L3/L4) PASS, read-only SELECT/CREATE TABLE PASS, DROP TABLE 403 class 이름 prefix와 INSERT 403 SYSTEM ERROR는 B의 매핑으로 해결(§5에서 재확인), G-09 L5–L7은 수정 전 build라 FAIL로 기록되었고 §5에서 재검증했다. Secret scan 0.

## 5. Integration 재검증 (INSTANCE=5)

Phase 3 최종 코드로 `plugins/icebergcatalog`, `distribution/resources`, `distribution/server`를 다시 build하고, tarball을 scratchpad에 새로 풀어 실행했다 (plugin jar md5가 module jar와 같음, `conf/logback.xml`에 HttpClient wire/headers INFO 고정 block 포함). Polaris token TTL은 `PT60S`로 줄였다. MinIO prefix는 `s3://dremiodev/polaris-p3-integration/`.

첫 실행에서 allowedNamespaces 시나리오가 FAIL했다 (아래 #11 참고). 수정 후 tarball을 다시 만들어 Dremio를 새로 띄우고 source를 다시 만들어 #1–#4, #11–#20을 재실행했다. #5–#10은 첫 실행 결과다 (그 사이 바뀐 코드는 folder listing 순서뿐이다).

| # | 시나리오 | 결과 | Status |
|---|---|---|---|
| 1 | Source 생성 (`root`, `PRINCIPAL_ROLE:ALL`, names refresh 60초) | `POST /api/v3/catalog` 200, state `good` | PASS |
| 2 | 잘못된 credential | HTTP 400, `errorMessage`: "Could not connect to bad. The Iceberg REST catalog rejected the credentials (OAuth2 error 'unauthorized_client'). Check the 'credential' catalog credential (client ID and client secret separated by ':') and, if set, the 'oauth2-server-uri' catalog property. Details: Malformed request: unauthorized_client: The client is not authorized" | PASS |
| 3 | `warehouse` 누락 / 닿지 않는 endpoint / scope `bogus` | 각각 HTTP 400과 warehouse hint ("…because the 'warehouse' catalog property is not set… Details: Malformed request: Please specify a warehouse"), "Unable to reach the Iceberg REST catalog at http://127.0.0.1:1/api/catalog…", `invalid_scope` hint | PASS |
| 4 | 403 principal: `CATALOG_READ_PROPERTIES`만 (`nolist`) | 200, state `warn` "…denied listing the top-level namespaces (HTTP 403)… Details: Forbidden: … not authorized for op LIST_NAMESPACES". `allowedNamespaces=ns1`이면 `good`, SELECT는 "denied the request to look up [ns1.p_t]: … LOAD_TABLE" permission error | PASS |
| 5 | read-only principal (`reader`) | SELECT COMPLETED. `CREATE TABLE` → "denied the request to create table [ns1.x1]: … CREATE_TABLE_DIRECT. Check the privileges…". `CREATE FOLDER` (SQL) → "denied the request to create folder [newf]: … CREATE_NAMESPACE…". REST v3 folder 생성 → **HTTP 403** (이전 409/잘못된 문구) | PASS |
| 6 | read-only INSERT (재실행 때 `reader2`) | `PERMISSION ERROR: … denied the request to commit to table [ro2.after.t9]: … UPDATE_TABLE …` (이전 SYSTEM ERROR) | PASS |
| 7 | Namespace/table: `CREATE FOLDER` 2 level, `CREATE TABLE`, `INSERT` 2 rows, `SELECT`, CTAS, `UPDATE`, `DELETE`, view `CREATE`/`SELECT`/`DROP` | 모두 COMPLETED, 결과 정상. Polaris `ns-list`/`tables`에 `ops.l2`, `ops.l2.t1` | PASS |
| 8 | 비어 있지 않은 namespace DROP | "Folder [ops.l2] cannot be deleted because it is not empty. Drop the tables, views and folders inside it first." (재실행: `[after]`도 동일) | PASS |
| 9 | 없는 folder DROP / 부모 없는 folder (REST v3) | "Folder does not exist." / HTTP 409 "com.dremio.common.exceptions.UserException: Cannot create folder [zz.yy]: parent folder [zz] does not exist…" (409와 class prefix는 kernel `CatalogServiceHelper`) | PASS (메시지). 409는 kernel 동작 |
| 10 | DROP VIEW, catalog `polaris.config.drop-with-purge.enabled=false` | "The Iceberg REST catalog denied the request to drop view [ops.v2]: Forbidden: Unable to purge entity: v2… Ask the catalog administrator to allow drop with purge (for Apache Polaris: set the catalog property 'polaris.config.drop-with-purge.enabled' to 'true')." `true`로 되돌린 뒤 DROP COMPLETED | PASS |
| 11 | allowedNamespaces `[sales.eu]` recursive, `[sales.eu.de, hr]` non-recursive, `[nonexistent]` (첫 실행) | 생성 직후 정상이었으나 약 3분 뒤 names refresh에서 `scoped.sales.eu` subtree가 삭제되었다. 원인: Dremio `SourceMetadataManager.handleFolderListing`은 folder를 listing 순서대로 처리하며, 부모보다 먼저 나온 folder를 "no longer found"로 삭제한다. C의 B-1 patch가 조상 folder를 listing **끝**에 붙여서 생긴 문제다 | FAIL → 수정 |
| 12 | 같은 설정 (수정 후: 부모 먼저, 중복 제거) | 07:59–08:06 UTC 동안 30초 간격 14회 확인: `scoped` → `sales` → `eu` → (`de`, `s_eu`), `scoped2` → `hr`, `sales`. `INFORMATION_SCHEMA`는 매번 4 rows (`scoped.sales.eu.s_eu`, `scoped.sales.eu.de.s_de`, `scoped2.hr.h_top`, `scoped2.sales.eu.de.s_de`). server.log의 folder add/delete WARN 0건 | PASS |
| 13 | `[nonexistent]` | 아무것도 없음, state `good`. refresh마다 stack trace 없는 WARN 4줄 ("Namespace nonexistent does not exist …", "Skipping namespace nonexistent …") | PASS |
| 14 | Token 만료/갱신 (Polaris token TTL 60초) | 6분 동안 SELECT 12회 모두 COMPLETED. Polaris metrics: `/v1/config` 200은 13 → 13 (새 client 없음), token 200은 97 → 133 (source 6개가 약 1분마다 갱신), catalog endpoint 401 0건, client 교체 log 0건 | PASS |
| 15 | Polaris hang (`docker pause`, timeout 미설정) | 약 60초 뒤 source unavailable (by-path 400 "The source [polaris] is currently unavailable…"). unpause 후 15초 안에 `good`, SELECT `c=2` | PASS (복구). 원인 hint는 timeout 설정 시에만 (A) |
| 16 | Polaris 중지 (`docker stop`) | 약 60초 뒤 `bad`. state message와 suggested user action: "Could not connect to polaris. Unable to reach the Iceberg REST catalog at http://127.0.0.1:18231/api/catalog. Check the endpoint URI and network connectivity. Details: Connect to … failed: Connection refused" | PASS |
| 17 | Polaris 재생성 (`polaris-up.sh`, 새 signing key, in-memory store 초기화) | 45초 안에 `good`. log "Replaced the cached Iceberg REST catalog client after it failed a health check that a new client passed." 이후 Polaris API로 만든 `after` namespace에 CTAS와 SELECT 성공 | PASS |
| 18 | 실패한 source 삭제 (harness) | `source.sh delete`가 root listing에서 id를 찾아 `bad` 상태 source도 지운다 | PASS |
| 19 | Secret scan (두 번째 Dremio 실행의 `log/` 전체, 재생성된 Polaris container log) | MinIO secret, Polaris root secret, 잘못 넣은 secret, `nolist`/`reader` principal secret, `Bearer`, `access_token`, `s3.secret-access-key`, `s3.session-token`, `client_secret`, JWT `eyJ`: 모두 0 | PASS |
| 20 | 정리 | source 7개 삭제(204), Dremio 중지와 work dir 삭제, container `p3-polaris-e2e-5` 삭제, MinIO object 39개 삭제(남은 0), 임시 env 파일 삭제 | PASS |

첫 Dremio 실행의 log는 tarball 교체 때 지웠으므로 #19 scan에는 들어가지 않았다. 대신 #5–#10의 대표 동작(folder/table/view, not-empty DROP, read-only CREATE/INSERT)을 두 번째 실행에서 다시 수행한 뒤 scan했다.

## 6. Test

| 범위 | 결과 |
|---|---|
| 신규/변경 test class (`TestRestCatalogAllowedNamespaces` 44, `TestRestCatalogNamespaceTableOps` 36, `TestRestCatalogAccessor` 33, `TestRestCatalogHttpErrors` 30, `TestRestCatalogOAuth2` 18, `TestRestIcebergCatalogPluginConfig` 28, `TestRestIcebergCatalogPlugin` 41, `TestIcebergRestCatalogAccessor` 21) | 251 tests, 0 failures, 0 skipped (Review 반영 후) |
| `plugins/icebergcatalog` module 전체 (`scripts/dev test plugins/icebergcatalog`) | 319 tests, 0 failures, 0 skipped (Phase 2: 182, Review 반영 전 312). 한 번 `testUntrustedSelfSignedCertificate`가 server thread 기록 지연으로 실패해 bounded wait로 고쳤다 |
| `scripts/dev fmt` / `scripts/dev lint` | clean |
| errorprone + forbiddenapis (`verify -DskipTests`, module 전체 강제 재compile) | clean. Phase 2 test의 `Slf4jIllegalPassedClass` 1건 수정 |
| shellcheck (`koalaman/shellcheck:stable`, `scripts/polaris-e2e/*.sh`) | clean |

## 7. AbleOps Local CI

`.localci.yaml`의 `full` profile에 `e2e-harness-lint` step을 추가했다. 이 step은 `scripts/polaris-e2e/*.sh`의 Apache license header를 확인하고 shellcheck를 실행한다 (local binary가 없으면 `koalaman/shellcheck:stable` image). `icebergcatalog-static`은 source를 `touch`한 뒤 verify한다. 앞 step이 errorprone 없이 compile해 두면 static step이 "Nothing to compile"로 errorprone을 건너뛰었기 때문이다 (Phase 2 test의 `Slf4jIllegalPassedClass`가 이 때문에 지나갔다).

`localci submit`(원격)은 Phase 2부터 QUEUED에 머무르므로 `localci run --profile full --no-cache`(local mode)로 실행했다. Log: `target/dev/localci-phase3.log`.

| Step | 내용 | 결과 |
|---|---|---|
| `icebergcatalog-test` | `plugins/icebergcatalog` compile + 전체 test (JDK 11) | SUCCESS (319 tests, 0 failures, 0 skipped), 1m18s |
| `icebergcatalog-lint` | spotless, license, checkstyle | SUCCESS, 9s |
| `icebergcatalog-static` | `verify -DskipTests`: errorprone, forbiddenapis, enforcer (source `touch` 후 재compile) | SUCCESS, 24s |
| `ui-source-specs` | source form Mocha spec 6개 + eslint + prettier | SUCCESS, 1m12s |
| `e2e-harness-lint` | harness license header + shellcheck | SUCCESS, 6s |

위 표는 Review 반영 후 다시 실행한 결과다 (`localci run --profile full --no-cache`, exit 0).

## 8. 남은 Gap (Phase 4/5 이후)

| 항목 | 내용 | 대상 |
|---|---|---|
| Kernel: update 실패 후 metadata manager 종료 | source update가 실패하면 `ManagedStoragePlugin.newStartSupplier`가 `closeMetaDataManager=true`로 `SourceMetadataManager`를 닫는다. 이후 Dremio 재시작 전까지 background state/metadata refresh가 멈춘다 (A 재현) | Phase 5 (kernel, 범위 밖이면 known limitation) |
| Kernel: update 실패 문구 | `postSourceModify` fallback이 hint를 중복하고 `SourceBadStateException` 문구로 감싼다 | Phase 5/6 |
| Kernel: REST v3 folder 오류 code | `CatalogServiceHelper.createCatalogItem`이 folder 생성의 `UserException`을 409로 바꾸고 class 이름을 붙인다 (부모 없음 등) | Phase 6 known limitation |
| Kernel: `DropFolderHandler` | `CatalogFolderNotEmptyException`을 잡지 않는다. accessor는 typed `NamespaceNotEmptyException`을 던지고, plugin `deleteFolder`가 그것을 validation error(`UserException`)로 바꿔 우회한다 | Phase 6 |
| 기본 timeout 없음 | hang 시 state가 일반 메시지. 권장값 문서화만 함 | Phase 5 결정 |
| 정확한 HTTP status | 404/409/429/502/504는 후보를 나열하는 hint | 선택 (HTTPClient 감싸기) |
| allowedNamespaces 강제 (B-2), non-recursive 빈 folder (B-5) | 설계 결정 필요 | Phase 5/6 |
| 공백이 든 namespace (O-1) | Iceberg client `+` encoding과 Polaris 404 | upstream |
| `SHOW TABLES` 0 rows | harness SQL API 경로에서 `sys`도 0 rows. plugin 문제가 아님 | Phase 5 조사 |
| DROP TABLE purge | 항상 purge=false, S3 object 잔존 | Phase 6 문서 |
| G-04 vended credentials runtime, G-08 MinIO 설정 matrix, AWS S3 | Phase 4 | Phase 4 |
| out-of-tree LOCATION (G-22) live, ALTER/OPTIMIZE/time travel, multi-node | Phase 5 | Phase 5 |
| UI 브라우저 source 생성 (완료 기준 #2) | 미실행. tarball의 `dremio-dac-ui`는 Phase 2 이전 UI | Phase 5/6 (`dac/ui` build 필요) |
| G-20 `restcatalog.enabled` gating, G-21 `catalogName()` null | low | Phase 6 |

## 9. Review 반영 (INSTANCE=6)

Phase 3 diff의 review finding 11건을 확인하고 모두 반영했다. 상세 코드 변경은 §3의 "Review" 행에 있다.

| Finding | 확인 | 처리 |
|---|---|---|
| commit 403을 `TableOperations.commit` 안에서 `UserException`으로 바꾸면 Iceberg cleanup이 건너뛰어진다 | 맞음. Iceberg 1.7 `ForbiddenException implements CleanableFailure`, `SnapshotProducer`가 `instanceof CleanableFailure`를 확인한다 (bytecode) | `CommitForbiddenException extends ForbiddenException` (cause = permission error). unit: `CleanableFailure`이고 `UserException.systemError(t)`가 PERMISSION error를 돌려준다 |
| `checkStateInternal`이 401 외의 오류(5xx 등)에도 client를 교체하고 cache를 비우지 않는다 | 맞음 | 401만 교체, 교체 시 table/view cache 비움. 나머지는 cached client 유지. 이전 client를 일정 시간 뒤에 닫는 grace period는 넣지 않았다: 교체는 server가 그 session을 401로 거부한 경우뿐이라 그 client를 쓰는 요청도 어차피 실패한다 |
| root listing 403마다 table/view cache가 비워진다 | 맞음 | `invalidatesCachesOnFailure` hook. `NamespaceListingForbiddenException`은 cache를 유지 |
| `dropFolder`가 typed `NamespaceNotEmptyException` 계약을 깼다 | 맞음 | accessor는 `NamespaceNotEmptyException`(400 not-empty도 변환), plugin이 validation error로 변환 |
| `ExpiringCatalogCache.replace()`가 close 뒤에 client를 넣을 수 있다 | 맞음 | `closed` flag, `invalidate()`도 lock, 닫힌 뒤 replacement는 즉시 close |
| 403/401 매핑과 listing WARN에 redaction 없음, test가 echo를 쓰지 않음 | 맞음 | redactor 전달, message와 cause redact, test는 `echoMessage`로 secret echo |
| `testTokenIsRefreshedBeforeItExpires`가 timing에 민감 | 맞음 (refresh 여유 200ms) | 만료 후 2초 grace(늦은 token은 따로 셈), 고정 sleep 대신 token 요청 3회까지 bounded polling |
| `testListNamespaces403…`가 두 결과를 모두 통과 | 맞음, Javadoc 위치 오류도 맞음 | 빈 listing + walk당 WARN 1줄(no throwable, redact) 고정, Javadoc 이동 |
| harness `redact()`가 `MINIO_SECRET_KEY` 등을 가리지 않음 | 맞음 | env 8개 + principal cred 파일, `id:secret`의 secret 부분 |
| `scan-secrets.sh`가 `.gz` rotate log를 못 읽음 | 맞음 | `zcat -f`로 `.gz`도 센다 (dummy 값으로 plain 1 + gz 1 = 2 확인) |
| 허용된 부모의 property를 못 읽으면 자식만 listing된다 | 맞음 (recursive/non-recursive 모두) | listing 결과 기준으로 빠진 조상 추가. unit `allowedParentWhosePropertiesAreDeniedIsStillListedBeforeItsChildren` |

Live 재검증: plugin, `distribution/resources`, `distribution/server`를 다시 build하고 tarball을 scratchpad에 새로 풀었다 (tarball의 plugin jar md5 = module jar). MinIO prefix `s3://dremiodev/polaris-p3-6/`.

| # | 시나리오 | 결과 | Status |
|---|---|---|---|
| 1 | Source `polaris`(root) 생성, `CREATE FOLDER rv`, `CREATE TABLE rv.t1`, `INSERT`, `SELECT` | 모두 COMPLETED | PASS |
| 2 | `DROP FOLDER polaris.rv` (비어 있지 않음, Polaris 400) | "Folder [rv] cannot be deleted because it is not empty. Drop the tables, views and folders inside it first." (accessor typed 예외 → plugin 변환 경로) | PASS |
| 3 | read-only principal source `ro`: SELECT / INSERT ×2 / DELETE | SELECT COMPLETED. INSERT/DELETE는 "PERMISSION ERROR: The Iceberg REST catalog denied the request to commit to table [ro.rv.t1]: Forbidden: … not authorized for op UPDATE_TABLE. Check the privileges…" (`WRITER_COMMITTER`, `BaseTransaction.commitSimpleTransaction` 경로) | PASS |
| 4 | 거부된 commit 3건 뒤 table 경로의 S3 object | 거부 전후 목록이 같다 (data 1, metadata.json 2, manifest 1, manifest list 1). 거부된 commit이 남긴 object 0 | PASS |
| 5 | Polaris 재생성(새 signing key, catalog 초기화) | 약 65초 뒤 "Replaced the cached Iceberg REST catalog client…" 1건(`polaris`), state `good`. 새 catalog에 `CREATE FOLDER rv2`, `CREATE TABLE`, `INSERT`, `SELECT` COMPLETED. `Connection pool shut down` 0건. `ro`는 principal이 사라져 `bad` + `unauthorized_client` hint (예상대로) | PASS |
| 6 | Secret scan (`scan-secrets.sh`, `.gz` 포함) | MinIO secret, Polaris root secret, marker 5종 모두 0 | PASS |
| 7 | 정리 | source 2개 삭제(204), Dremio 중지와 work dir 삭제, container `p3-polaris-e2e-6` 삭제, MinIO object 10개 삭제(남은 0), 압축 해제 tarball 삭제 | PASS |

`reader` principal secret은 Polaris 재생성(`polaris-down.sh`가 cred 파일 삭제) 뒤 scan해 검사 대상에서 빠졌다. 같은 실행의 다른 값과 marker는 모두 0이다.

