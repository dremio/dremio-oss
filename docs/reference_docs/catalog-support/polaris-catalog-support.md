# 목표

`plugins/icebergcatalog`와 Iceberg `RESTCatalog` 구현을 최대한 재사용하여 **Dremio OSS에서 Apache Polaris OSS를 Lakehouse Catalog로 사용할 수 있도록 RESTCATALOG 기능을 완성**한다.

Dremio 공식 26.x Lakehouse Catalog / Iceberg REST Catalog 사용가이드를 기준으로 구현한다.

참고:
`https://docs.dremio.com/current/data-sources/lakehouse-catalogs/`

Polaris 전용 Catalog 엔진을 새로 만들지 말고 **Generic Iceberg REST Catalog + Polaris 호환 구성**으로 구현한다.

---

# 공통 수행 원칙

각 Phase 내부 독립 작업은 여러 Agent로 **병렬 수행**한다.

예:

```text
Agent A: Backend/Plugin
Agent B: Config/API
Agent C: UI/UX
Agent D: Polaris/Auth
Agent E: Storage/E2E
Agent F: Test/Security
```

동일 파일 동시 수정은 피하고 작업영역을 분리한다.

각 Phase 종료 시 반드시:

```text
병렬 작업 완료
→ 통합
→ 관련 테스트
→ regression 검증
→ 문서 갱신
→ git diff/status 검토
→ commit
→ origin remote push
→ commit SHA 기록
→ 다음 Phase
```

순서로 진행한다.

`git commit`, `push`, `rebase`는 Integration/Lead Agent만 수행한다.

기존 사용자 변경사항을 삭제하지 않는다.

금지:

```text
git reset --hard
git clean -fd
git push --force
git push --force-with-lease
```

작업 branch가 없으면:

```text
feature/polaris-restcatalog
```

를 생성한다.

각 Phase 완료 후 반드시 remote에 push한다.

---

# Phase 1 — 구조 분석 / Gap Analysis

병렬 분석한다.

## Agent A — Backend

집중 분석:

```text
plugins/icebergcatalog
RestIcebergCatalogPlugin
RestIcebergCatalogPluginConfig
IcebergRestCatalogAccessor
AbstractRestCatalogAccessor
IcebergCatalogPlugin
IcebergCatalogModel
DremioRESTTableOperations
```

확인:

- Iceberg REST 호환성
- Namespace/Table/View
- SELECT/CREATE/CTAS/INSERT/ALTER/DROP
- OAuth2
- REST config handshake
- FileIO
- cache
- mutable 기능

## Agent B — OSS Source 등록

확인:

```text
@SourceType
ConnectionConf
classpath scanning
SourceTypeTemplate
RESTCATALOG
plugin packaging
distribution
source create/get/update
```

특히 `RESTCATALOG`가 OSS 배포판에서 실제 Source로 생성 가능한지 확인한다.

## Agent C — 공식 Dremio 설정 비교

현재 코드와 공식 Source Config 비교:

```text
propertyList
secretPropertyList
enableAsync
isCachingEnabled
maxCacheSpacePct
restEndpointUri
allowedNamespaces
isRecursiveAllowedNamespaces
isUsingVendedCredentials
```

누락/불일치 항목을 찾는다.

## Agent D — UI

확인:

```text
RESTCATALOG source 표시
Add Source
General
Advanced Options
Catalog Properties
Catalog Credentials
Allowed Namespaces
Cache
Async
```

## Agent E — Polaris

공식 Polaris OSS 구성과 현재 코드를 비교한다.

기본 목표:

```text
Endpoint:
http://<polaris>:8181/api/catalog

warehouse=<catalog>
scope=PRINCIPAL_ROLE:ALL

credential=<client_id>:<client_secret>

isUsingVendedCredentials=false
```

### Phase 1 산출물

```text
docs/polaris/phase1-analysis.md
docs/polaris/compatibility-matrix.md
docs/polaris/progress.md
```

분석 완료 후 commit + remote push하고 Phase 2 진행.

---

# Phase 2 — RESTCATALOG OSS Source 완성

Phase 1 Gap을 기준으로 병렬 구현한다.

## Agent A — Backend 등록

필요 시:

```text
@SourceType(value="RESTCATALOG", ...)
classpath scanning
module/plugin registration
distribution packaging
enable option
```

을 보완한다.

## Agent B — Config/API

공식 Dremio 설정과 호환하도록 구현한다.

최소:

```text
propertyList
secretPropertyList
enableAsync
isCachingEnabled
maxCacheSpacePct
restEndpointUri
allowedNamespaces
isRecursiveAllowedNamespaces
isUsingVendedCredentials
```

Source Create/Get/Update API에서 round-trip 검증한다.

Secret은 기존 Dremio secret masking 체계를 사용한다.

## Agent C — UI

`Lakehouse Catalogs → REST Iceberg Catalog` 생성 화면을 완성한다.

가능하면 preset:

```text
Generic Iceberg REST
Apache Polaris OSS
```

를 제공하되 backend type은 가능한 한 모두:

```text
RESTCATALOG
```

를 사용한다.

Polaris preset 기본값:

```text
scope=PRINCIPAL_ROLE:ALL
isUsingVendedCredentials=false
```

## Agent D — Test

검증:

```text
serialization
deserialization
source create/get/update
secret masking
plugin discovery
UI form
RESTCATALOG backward compatibility
```

통합 후 commit + remote push.

---

# Phase 3 — Polaris OAuth2 / Catalog 연동

병렬 수행.

## Agent A — OAuth2

기존 Iceberg `RESTCatalog` OAuth2 기능을 우선 사용한다.

지원:

```text
credential=<client_id>:<client_secret>
scope=PRINCIPAL_ROLE:ALL
```

검증:

```text
valid credential
invalid credential
401
403
token expiration
token refresh
timeout
Polaris unavailable
```

불필요한 별도 OAuth HTTP client는 만들지 않는다.

## Agent B — Namespace/Table

실제 Polaris 대상으로 검증:

```text
listNamespaces
createNamespace
dropNamespace
listTables
loadTable
createTable
commit
rename
dropTable
```

## Agent C — Allowed Namespace

검증:

```text
allowedNamespaces
isRecursiveAllowedNamespaces
```

## Agent D — 오류/보안

검증:

```text
400
401
403
404
409
429
5xx
timeout
TLS failure
```

절대 로그에 노출하지 않을 값:

```text
client_secret
credential
OAuth token
S3 secret
session token
```

통합/E2E 가능한 범위 검증 후 commit + remote push.

---

# Phase 4 — Object Storage

Static Credential과 Vended Credential을 병렬 분리한다.

## Agent A — Polaris + S3 기본 경로

공식 Dremio Polaris OSS 기준선:

```text
isUsingVendedCredentials=false
```

Catalog Properties:

```text
warehouse=<catalog>
scope=PRINCIPAL_ROLE:ALL
fs.s3a.aws.credentials.provider=
org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
```

Secret Properties:

```text
credential=<client_id>:<client_secret>
fs.s3a.access.key=<access-key>
fs.s3a.secret.key=<secret-key>
```

이 경로를 **필수 성공 기준**으로 한다.

## Agent B — S3-compatible / MinIO

기존 Dremio S3 기능을 재사용해 검증:

```text
custom endpoint
path-style
SSL
region
MinIO
```

MinIO 전용 hack은 만들지 않는다.

## Agent C — Vended Credentials

Generic RESTCATALOG에서:

```text
isUsingVendedCredentials=true
```

경로를 검증한다.

Iceberg library에서 이미 처리한다면 재구현하지 않는다.

Polaris OSS vended credential은 기본 성공조건이 아니라 별도 compatibility 결과로 기록한다.

## Agent D — 보안

다음 노출 여부 점검:

```text
client secret
S3 secret
session token
OAuth token
credential property
```

Phase 완료 후:

```text
docs/polaris/storage.md
docs/polaris/security.md
```

작성 → commit → remote push.

---

# Phase 5 — 실제 E2E / Regression

가능하면 다음 구조로 실제 검증한다.

```text
Dremio OSS
   ↓
Iceberg REST
   ↓
Apache Polaris OSS
   ↓
S3 / MinIO
```

병렬 수행한다.

## Agent A — Source Lifecycle

```text
Create
Get
Update
Restart
Reload
Delete
```

재시작 후 config/secret 유지 확인.

## Agent B — Read

```sql
SHOW SCHEMAS;
SHOW TABLES;
SELECT * FROM ...
```

추가:

```text
metadata discovery
schema refresh
query planning
Parquet scan
```

## Agent C — Write

지원되는 범위에서:

```sql
CREATE TABLE
CTAS
INSERT
ALTER TABLE
DROP TABLE
```

기존 지원 시:

```sql
UPDATE
DELETE
```

도 검증한다.

## Agent D — Namespace/View

지원 범위에 따라:

```text
namespace CRUD
view list/load/create/replace/drop
```

## Agent E — Cache/Metadata

검증:

```text
enableAsync
isCachingEnabled
maxCacheSpacePct
metadata refresh
dataset expiration
```

기존 공통 Source framework가 담당하는 기능은 재구현하지 않는다.

## Agent F — 장애/Regression

검증:

```text
Polaris down
Storage down
invalid OAuth credential
invalid S3 credential
expired token
401/403/429/5xx
timeout
restart
```

그리고:

```text
plugins/icebergcatalog
sabot/kernel
dac/backend
관련 UI test
Generic RESTCATALOG
```

회귀검증 수행.

Phase 완료 후 commit + remote push.

---

# Phase 6 — 최종 통합 / 문서 / Release Readiness

전체 병렬 결과를 Integration Agent가 최종 정리한다.

확인:

```text
Generic REST Catalog 구조 유지
Polaris-specific hack 제거
공통 코드 중복 제거
Secret masking
UI/API/Backend property 일치
plugin packaging
Maven dependency
frontend build
backend tests
RESTCATALOG regression
```

문서 생성:

```text
docs/polaris/design.md
docs/polaris/configuration.md
docs/polaris/storage.md
docs/polaris/security.md
docs/polaris/test-results.md
docs/polaris/known-limitations.md
docs/polaris/progress.md
```

`configuration.md`에는 반드시:

```text
1. Dremio UI 등록 방법
2. REST API 등록 방법
3. Polaris OSS + S3 예제
4. MinIO 사용 예제
```

를 포함한다.

Secret은 placeholder만 사용한다.

최종 commit 후 반드시 remote push한다.

---

# Phase 공통 완료 Gate

각 Phase마다 다음을 모두 확인한다.

```text
[ ] 병렬 Agent 작업 완료
[ ] Integration 완료
[ ] 신규 테스트 통과
[ ] 관련 regression 통과
[ ] secret/log 검토
[ ] git diff/status 검토
[ ] 문서 갱신
[ ] commit 완료
[ ] origin push 완료
[ ] commit SHA 기록
```

결과 상태는 명확히:

```text
PASS
FAIL
ENVIRONMENT_BLOCKED
NOT_SUPPORTED
```

중 하나로 기록한다.

환경 문제 때문에 일부 E2E가 불가능하더라도 구현 가능한 다른 Phase는 계속 진행한다.

---

# 완료 기준

최소 다음이 충족되어야 한다.

```text
[ ] Dremio OSS에서 RESTCATALOG Source 노출
[ ] UI Source 생성 성공
[ ] REST API Source 생성 성공
[ ] Polaris OAuth2 인증 성공
[ ] Namespace 탐색 성공
[ ] Table 탐색 성공
[ ] SELECT 성공
[ ] CREATE TABLE 또는 CTAS 성공
[ ] INSERT 성공
[ ] Polaris + S3/MinIO 접근 성공
[ ] Source restart/reload 성공
[ ] allowedNamespaces 동작
[ ] secret masking 정상
[ ] Generic RESTCATALOG 회귀 없음
[ ] 관련 unit/integration/E2E 검증
[ ] 문서 완료
[ ] 모든 Phase commit/push 완료
```

---

# 범위 제외

이번 단계에서는 Polaris Management API는 구현하지 않는다.

제외:

```text
Polaris Catalog 자체 생성
Principal 관리
Principal Role
Catalog Role
Grant/Revoke
Polaris Admin Console
```

현재 목표는 오직:

```text
Dremio OSS
 ↕ Iceberg REST
Apache Polaris
 ↕
Object Storage
```

통합이다.

**분석만 하지 말고 실제 코드 수정 → 테스트 → 문서 → commit → remote push까지 수행한다. 각 Phase 내부 작업은 가능한 최대한 병렬화하고, Phase를 순차적으로 완료하면서 진행한다.**