# Phase 1 — 구조 분석 / Gap Analysis

- 대상: Dremio OSS 26.0.5 (`799ccbda4 Release 26.0.5`), branch `feature/polaris-restcatalog`
- 작성일: 2026-10-03
- 기준 문서: [`docs/reference_docs/catalog-support/polaris-catalog-support.md`](../reference_docs/catalog-support/polaris-catalog-support.md), Dremio 26.x 공식 문서 [Lakehouse Catalogs](https://docs.dremio.com/current/data-sources/lakehouse-catalogs/) / Iceberg REST Catalog / API Catalog > Source Configuration
- 분석 방식: Agent A~E 병렬 분석 → 각각 Verifier 재검증(정정/누락 반영) → Integration
- 이 Phase에서는 repo 코드를 수정하지 않았다. 산출물은 `docs/polaris/` 아래 문서 3개뿐이다.

관련 문서: [compatibility-matrix.md](compatibility-matrix.md), [progress.md](progress.md)

---

## 1. 개요 / 목표

`plugins/icebergcatalog`와 Iceberg `RESTCatalog`를 재사용해서, Dremio OSS가 Apache Polaris OSS를 **Generic Iceberg REST Catalog(`RESTCATALOG`) + Polaris 호환 구성**으로 사용하도록 만든다. Polaris 전용 Source type이나 엔진은 만들지 않는다.

### 핵심 결론

1. **Engine 쪽은 이미 거의 완성되어 있다.** 아래 기능이 OSS tree에 모두 들어 있다.
   - `RestIcebergCatalogPlugin`, `IcebergRestCatalogAccessor`, `DremioRESTTableOperations`, View/Folder, DML 경로
   - Iceberg 1.7.0 fork의 OAuth2 client
   - 모든 `plugins.restcatalog.*` option. 기본값은 모두 enabled다.
2. **유일한 blocker는 Source type 등록이 빠진 것이다.**
   - `RestIcebergCatalogPluginConfig`에 `@SourceType`이 없다. 그래서 OSS 배포판에서 `RESTCATALOG` Source를 만들 수 없다.
   - 25.2.0까지는 annotation과 `restcatalog-layout.json`이 있었는데, 26.0.0(`cb71ea5b1`)에서 둘 다 제거되었다.
   - Agent B가 빌드된 tarball에 annotation을 붙인 out-of-tree subclass와 layout을 넣어 prototype을 돌렸다. Polaris 1.1.0 상대로 Source create/get/update, namespace 노출, secret masking이 모두 동작했다.
3. **Polaris 사용에 필요한 조건 (live probe로 확인)**
   - `warehouse=<catalog>`와 `scope=PRINCIPAL_ROLE:ALL`이 필수다. Iceberg 기본 scope `catalog`는 Polaris가 400 `invalid_scope`로 거부한다.
   - Endpoint는 `http://<host>:8181/api/catalog`이다.
   - DROP VIEW를 쓰려면 Polaris catalog property `polaris.config.drop-with-purge.enabled=true`가 필요하다.
4. **Storage는 static credential(`fs.s3a.*`)만 동작한다.**
   - Dremio는 REST FileIO를 버리고 자체 `DremioFileIO`/Hadoop FS로 data를 읽고 쓴다. 그래서 vended credentials는 현재 사용되지 않는다.
   - 공식 Polaris OSS recipe도 `Use vended credentials = Unchecked`이므로 이 동작과 일치한다.

---

## 2. 현재 아키텍처 (code path)

```text
[REST API / UI]  PUT /apiv2/source/{name}  |  POST /api/v3/catalog  (type=RESTCATALOG)
      │  Jackson subtype ← ConnectionConf.registerSubTypes (@SourceType 필요)
      ▼
RestIcebergCatalogPluginConfig (tags 10-12)  ⊂  IcebergCatalogPluginConfig (abstract, tags 1-5)
      │  newPlugin()
      ▼
ManagedStoragePlugin.initPlugin → plugin.start() → getState()
      ▼
RestIcebergCatalogPlugin  (extends IcebergCatalogPlugin)
      │  createCatalog(fsConf) → createRestCatalog(): Supplier<Catalog> (lazy)
      ▼
IcebergRestCatalogAccessor  (extends AbstractRestCatalogAccessor)
      │  ExpiringCatalogCache (TTL plugins.restcatalog.catalog.expire_seconds=1800)
      ▼
CatalogUtil.loadCatalog(RESTCatalog, name=null, props, hadoopConf)
      │  Iceberg 1.7.0-5f7c992 fork: RESTSessionCatalog / OAuth2Util / HTTPClient
      │  POST {uri}/v1/oauth/tokens → GET {uri}/v1/config?warehouse=… → prefix
      ▼
Apache Polaris  /api/catalog/v1/{prefix}/namespaces|tables|views …
      │
      │ (data path) loadTable → BaseTable(RESTTableOperations, ResolvingFileIO)
      ▼
AbstractRestCatalogAccessor.getTableHandleInternal: ResolvingFileIO.close() → DremioFileIO
      ▼
DremioRESTTableOperations(DremioFileIO, RESTTableOperations)
      ▼
IcebergCatalogPlugin.createFS → DatasetFileSystemCache (s3/s3a/s3n → dremioS3)
      ▼
FileSystemConfUtil.initializeConfiguration → S3FileSystem (fs.s3a.* from propertyList/secretPropertyList)
```

### 2.1 단계별 근거 (repo-relative path:line)

| # | 단계 | 근거 |
|---|---|---|
| 1 | Config class. `RESTCATALOG` 전용 필드는 tags 10-12다. `@SourceType`이 없다. | `plugins/icebergcatalog/src/main/java/com/dremio/plugins/icebergcatalog/store/RestIcebergCatalogPluginConfig.java:26-52` |
| 2 | 공통 필드 tags 1-5. 부모 class는 abstract다. | `plugins/icebergcatalog/src/main/java/com/dremio/plugins/icebergcatalog/store/IcebergCatalogPluginConfig.java:33,41-66` |
| 3 | Source type registry. `@SourceType`이 붙은 concrete class만 등록된다. | `sabot/kernel/src/main/java/com/dremio/exec/catalog/ConnectionReaderImpl.java:71-86,92-114,120-130` |
| 4 | Jackson subtype 등록. `getType()`은 annotation을 읽으므로 annotation이 없으면 NPE가 난다. | `sabot/kernel/src/main/java/com/dremio/exec/catalog/conf/ConnectionConf.java:443-454,517-519` |
| 5 | Classpath scanning은 이미 package를 포함한다. | `sabot/kernel/src/main/resources/sabot-module.conf:38-39,52`, `plugins/icebergcatalog/src/main/resources/sabot-module.conf:21` |
| 6 | Plugin 초기화 시 `getState()`가 호출된다. | `sabot/kernel/src/main/java/com/dremio/exec/catalog/ManagedStoragePlugin.java:1811-1817` |
| 7 | Property 병합 순서는 `propertyList` 다음 `secretPropertyList`다. 같은 key면 secret 값이 이긴다. | `plugins/icebergcatalog/src/main/java/com/dremio/plugins/icebergcatalog/store/RestIcebergCatalogPlugin.java:134-143` |
| 8 | `createCatalog`는 lazy Supplier를 감싸기만 한다. 그래서 이 안의 try/catch는 REST 오류를 잡지 못한다. | `RestIcebergCatalogPlugin.java:145-157` |
| 9 | Option gate는 `plugins.restcatalog.enabled`다. | `RestIcebergCatalogPlugin.java:160-163`, `IcebergCatalogPlugin.java:492-507` |
| 10 | `buildCatalogProperties`는 `catalog-impl`, `uri`, `dremio.enable_azure_abfss_scheme`, 모든 property를 **Iceberg props와 Hadoop conf 양쪽에** 복사한다. | `RestIcebergCatalogPlugin.java:296-311` |
| 11 | `loadCatalog`를 catalog name `null`로 호출한다. | `RestIcebergCatalogPlugin.java:313-322` |
| 12 | `start()`는 live fsConf를 넘기고 Supplier만 저장한다. FS cache는 conf를 복사한다. | `plugins/icebergcatalog/src/main/java/com/dremio/plugins/icebergcatalog/store/IcebergCatalogPlugin.java:155-157,272-278,320-322` |
| 13 | `getState`는 실패하면 `badState("Failure connecting to source: …")`를 반환한다. | `IcebergCatalogPlugin.java:232-257` |
| 14 | `checkStateInternal`은 매번 새 RESTCatalog를 만들고 바로 닫는다. `getDefaultBaseLocation`은 만든 catalog를 닫지 않는다. | `plugins/icebergcatalog/src/main/java/com/dremio/plugins/icebergcatalog/store/IcebergRestCatalogAccessor.java:54-67` |
| 15 | TTL cache. TTL이 만료되면 이전 catalog를 동기적으로 close한다. | `plugins/icebergcatalog/src/main/java/com/dremio/plugins/icebergcatalog/store/ExpiringCatalogCache.java:35-79` |
| 16 | Allowed namespace는 regex separator로 split한다. Per-user table/view cache가 있다. | `plugins/icebergcatalog/src/main/java/com/dremio/plugins/icebergcatalog/store/AbstractRestCatalogAccessor.java:112-161,164-205` |
| 17 | REST FileIO를 close하고 `DremioFileIO`로 교체한다. | `AbstractRestCatalogAccessor.java:376-407` |
| 18 | `DremioRESTTableOperations`는 `io()`로 `DremioFileIO`를 반환한다. | `plugins/icebergcatalog/src/main/java/org/apache/iceberg/rest/DremioRESTTableOperations.java:29-58` |
| 19 | URI scheme을 `dremioS3`로 rewrite한다. dataset별 conf hook은 쓰이지 않는다. | `plugins/icebergcatalog/src/main/java/com/dremio/plugins/icebergcatalog/dfs/DatasetFileSystemCache.java:62-69,109,157-200,230-232` |
| 20 | S3 credential provider 결정. `fs.s3a.access.key`가 있으면 `SimpleAWSCredentialsProvider`, 없으면 env, 그다음 InstanceProfile 순이다. | `plugins/hive/src/main/java/com/dremio/exec/store/hive/exec/FileSystemConfUtil.java:102-121,224-250` |
| 21 | S3 endpoint 앞에 scheme을 자동으로 붙인다. | `plugins/s3/src/main/java/com/dremio/plugins/s3/store/S3FileSystem.java:788-791,806` |
| 22 | CTAS/INSERT 경로. CTAS는 namespace `location`이 있어야 한다. | `RestIcebergCatalogPlugin.java:422-481,779-787`, `AbstractRestCatalogAccessor.java:798-832` |
| 23 | View. dialect는 `DremioSQL`이고, 읽을 때는 첫 SQL representation을 쓴다. | `AbstractRestCatalogAccessor.java:672-767`, `plugins/icebergcatalog/src/main/java/com/dremio/plugins/icebergcatalog/store/IcebergCatalogViewProvider.java:83-93,130-140` |
| 24 | DROP TABLE은 `purge=false`로 호출한다. | `AbstractRestCatalogAccessor.java:768-773` |
| 25 | DML/OPTIMIZE/VACUUM prule이 `SupportsIcebergRestApi`를 허용한다. | `sabot/kernel/src/main/java/com/dremio/exec/planner/physical/FileSystemTableModifyPrule.java:44`, `FileSystemTableOptimizePrule.java:44`, `FileSystemVacuumTablePrule.java:43` |
| 26 | Iceberg 버전은 Dremio fork `1.7.0-5f7c992-20250730084652-3bf8b99`이다. AuthManager SPI는 없다. | `pom.xml:82,2682-2686` |

### 2.2 Option 기본값

출처: `sabot/kernel/src/main/java/com/dremio/exec/store/IcebergCatalogPluginOptions.java:24-62`, `sabot/kernel/src/main/java/com/dremio/exec/catalog/CatalogOptions.java:134-139`. `PositiveLongValidator`의 인자 순서는 `(name, max, def)`이다(`services/options/src/main/java/com/dremio/options/TypeValidators.java:37`).

| Option | Default | 비고 |
|---|---|---|
| `plugins.restcatalog.enabled` | true | type 목록 노출과 plugin start를 gate한다 |
| `plugins.restcatalog.mutable.enabled` | true | 모든 write를 gate한다. false면 `getId()`가 throw한다 |
| `plugins.restcatalog.views_supported` | true | View CRUD |
| `plugins.restcatalog.folders_supported` | true | Folder ↔ namespace |
| `plugins.restcatalog.table_cache.enabled` / `view_cache.enabled` | true / true | per-user cache |
| `plugins.restcatalog.table_cache.size_items` | 10000 (max 1e9) | |
| `plugins.restcatalog.table_cache.expire_after_write_seconds` | 3 (max 120) | |
| `plugins.restcatalog.file_system.expire_after_write_minutes` | 5 (max 20) | |
| `plugins.restcatalog.file_system.optimistic_locking` | false | |
| `plugins.restcatalog.catalog.expire_seconds` | 1800 (max 3600) | Polaris token TTL 3600s보다 짧다 |
| `plugins.restcatalog.allowed.ns.separator` | `\\.` (regex) | 이름에 `.`이 들어간 namespace는 표현할 수 없다 |
| `plugins.restcatalog.lineage_calculation` | true | |

---

## 3. Agent별 분석 결과

### 3.1 Agent A — Backend (`plugins/icebergcatalog`)

- **REST 호환성**: Iceberg 1.7.0 fork의 `RESTSessionCatalog`/`OAuth2Util`이 그대로 쓰인다. Dremio 쪽에 자체 OAuth 코드는 없다.
  - pass-through되는 key: `credential`, `token`, `scope`, `oauth2-server-uri`, `token-refresh-enabled`, `token-expires-in-ms`, `audience`, `resource`, `header.*`, `warehouse`, `rest.sigv4-enabled`, `view-endpoints-supported`, `rest-page-size`, `snapshot-loading-mode`, `io-impl`, `rest.client.*` (timeout, retry 등)
  - 근거: `RestIcebergCatalogPlugin.java:296-311`, iceberg-core jar의 `OAuth2Properties` javap 결과
- **SQL 지원 (코드 기준)**
  - SELECT (time travel 포함), CREATE TABLE, CTAS, INSERT, UPDATE/DELETE/MERGE, OPTIMIZE, VACUUM
  - ALTER: column / PK / sort order / TBLPROPERTIES. TRUNCATE, ROLLBACK
  - DROP TABLE은 purge=false라서 data file이 남는다
  - CREATE/REPLACE/DROP VIEW, Folder ↔ namespace
  - table/view rename은 Dremio API가 없어서 지원하지 않는다
  - 근거: `RestIcebergCatalogPlugin.java:175-290,336-773,793-888`, `IcebergCatalogModel.java:62-113`
- **Live probe (Dremio runtime classpath의 Iceberg jar로 Polaris 1.1.0 호출)**
  - 성공: create table, staged-create transaction (CTAS 경로), create view, renameTable, dropTable(purge=false)
  - DROP VIEW는 403이다 (G-06)
  - `header.X-Iceberg-Access-Delegation`을 설정하면 root도 403 `LOAD_TABLE_WITH_READ_DELEGATION`을 받는다
- **Verifier 정정**
  - MinIO의 `fs.s3a.endpoint`는 **scheme 없이 `host:port`**로 써야 한다. `S3FileSystem.getEndpoint`가 scheme을 직접 붙이기 때문에, scheme을 넣으면 `http://http://minio:9000`이 된다 (`S3FileSystem.java:788-791`).
  - "static key만 가능"은 정확하지 않다. env var, InstanceProfile fallback, 그리고 명시적인 `fs.s3a.aws.credentials.provider`(assumed role `STSCredentialProviderV1` 포함)도 쓸 수 있다.
  - Vended 403 응답은 bootstrap root(`service_admin`, `catalog_admin`)에게도 발생한다.
  - Forbidden 매핑은 기존 패턴을 따르면 된다. `createFolder`/`updateFolder`가 이미 `ForbiddenException`을 `CatalogEntityForbiddenException`으로 바꾸고 있다 (`RestIcebergCatalogPlugin.java:200-205,240-245`).

### 3.2 Agent B — OSS Source 등록

- **Baseline (빌드된 26.0.5 tarball에서 실측)**
  - `GET /api/v3/source/type`이 24개 type을 반환하고, 그중 RESTCATALOG는 없다
  - `GET /api/v3/source/type/RESTCATALOG`는 404를 반환한다
  - `POST /api/v3/catalog`와 `PUT /apiv2/source/{name}`는 400 `An invalid value was found: RESTCATALOG`를 반환한다 (Jackson unknown type id)
- **Prototype**: annotation만 붙인 subclass와 25.2.0 layout을 `DREMIO_EXTRA_CLASSPATH`로 넣었다.
  - 목록 노출, template(uiConfig와 8개 element) 반환
  - Polaris 상대로 Source 생성 결과 HTTP 200, state `good`, child folder `ns1` 표시
  - secret은 `$DREMIO_EXISTING_VALUE$`로 masking되었고, masked 값으로 PUT update를 보내도 유지되었다
  - 잘못된 credential에는 400 `Could not connect…`이 나왔다. 로그에 secret은 없었다.
- **이력**: 25.2.0(`c7fee0bcd`)에는 `@SourceType(value = "RESTCATALOG", label = "REST Iceberg Catalog", uiConfig = "restcatalog-layout.json")`과 layout 파일이 있었다. 26.0.0(`cb71ea5b1`)에서 둘 다 제거되었고, 주석은 "20-109 - Reserved by other plugins"로 바뀌었다. Closed-source 쪽 subclass로 옮겨간 것으로 추정한다.
- **이미 갖춰진 것**
  - classpath scan
  - packaging: `plugins/pom.xml:48`, `dac/daemon/pom.xml:198-202`, `distribution/resources/src/main/resources/assemblies/core-component.xml:21-41`
  - `SourceVerifier.NO_OP`: `dac/backend/src/main/java/com/dremio/dac/daemon/DACDaemonModule.java:812`, `sabot/kernel/src/main/java/com/dremio/exec/server/SourceVerifier.java:25-28`
  - proto enum `RESTCATALOG=26`: `services/namespace/src/main/proto/source.proto:132`
  - UI icon과 lakehouse 분류
- **제약**
  - 같은 `@SourceType` value를 가진 class가 둘이면 `ImmutableMap` 중복 key 때문에 startup이 실패한다 (`ConnectionReaderImpl.java:74-85`). Polaris용 별도 class나 type을 만들면 안 된다. prototype jar를 classpath에 남겨둬도 안 된다.
  - `@SourceType`은 `@Inherited`가 아니다.
- **Verifier 정정**
  - Layout이 없어도 auto form에서 secret masking은 동작한다 (`SourceFormJsonPolicy.js:227-229`). Layout이 실제로 더하는 것은 tab/section 구성, label, placeholder다.
  - hadoop-azure `provided` scope 위치는 `plugins/icebergcatalog/pom.xml:39-40`이다.
  - "option=false여도 create가 되고 start만 실패한다"는 주장은 검증되지 않았다. `start()`가 create 과정에서 동기적으로 실행되므로 create 자체가 오류로 거부될 가능성이 높다.
  - UI의 `sourceTypes.js:71` label은 runtime에서 쓰이지 않는다. 화면에 보이는 label은 `@SourceType.label`에서 온다.
  - `dac/backend`는 icebergcatalog에 의존하지 않는다. 그래서 registration test는 `plugins/icebergcatalog`에 둬야 한다.

### 3.3 Agent C — 공식 Dremio 설정 비교

- 공식 문서에는 standalone "Apache Polaris" 페이지가 없다(404). Polaris OSS는 Iceberg REST Catalog의 "Supported Configuration"으로만 설명되어 있다. 이 프로젝트의 generic 접근과 같다.
- **필드 일치**: tags 1-5, 10-12의 이름, type, 기본값, UI label이 공식 문서와 정확히 일치한다 (`IcebergCatalogPluginConfig.java:41-66`, `RestIcebergCatalogPluginConfig.java:32-52`).
- **누락**
  - `isUsingVendedCredentials`가 없다. 공식 API 문서에서 Boolean이고 Optional 표시가 없다. UI에서는 "Use vended credentials"이고 기본값은 checked다.
  - `@SourceType`이 없다
  - layout이 없다
  - `restEndpointUri` 검증이 없다
  - label이 다르다: `REST Iceberg Catalog`와 공식 `Iceberg REST Catalog`
- **Strict Jackson**: `JSONUtil`이 `FAIL_ON_UNKNOWN_PROPERTIES`를 끄지 않는다. `CatalogEntity`의 `@JsonIgnoreProperties(ignoreUnknown=true)`는 top-level에만 적용된다. 그래서 공식 예제 JSON(`isUsingVendedCredentials` 포함)은 필드가 없으면 400이 날 가능성이 높다. 정적 분석 결과이며 runtime 검증은 아직 TBD다.
- **공식 Polaris OSS recipe**: [compatibility-matrix.md](compatibility-matrix.md) §a 참고.
- **S3 호환 storage (MinIO 등) 추가 property**: 공식 문서는 AWS를 전제로 한다.
  - `fs.s3a.endpoint=<host>:<port>`, `fs.s3a.path.style.access=true`, `dremio.s3.compat=true`, `fs.s3a.connection.ssl.enabled=false`, `dremio.bucket.discovery.enabled=false`, `dremio.s3.region=us-east-1`, `fs.s3a.requester.pays.enabled=false`
  - 26.0.5에는 `REQUESTER_PAYS_DEFAULT = true`가 있다 (`plugins/s3/src/main/java/com/dremio/plugins/s3/store/S3ClientProperties.java:129`). 26.1.8 release note에서 RESTCATALOG의 기본값이 바뀌었다.
- **Verifier 정정**
  - URI가 잘못되었을 때 실패는 `createCatalog`에서 나지 않는다. lazy이기 때문이다. 실제로는 `getState`의 `badState`로 나타난다.
  - release note 매핑 정정:
    - "vended INSERT hang" fix는 26.0.2에도 있으므로 5 이전에 이미 포함되었을 수 있다
    - "DML commit … starting snapshot id" fix는 IRC가 아니라 Snowflake Open Catalog 대상이다
    - 다음 IRC fix가 누락되어 있었다: 26.1.6 positional delete DML, 26.1.10 CTAS failure
  - 공식 문서 예제의 `secretPropertyList`에는 `credential` 값이 평문으로 적혀 있다.

### 3.4 Agent D — UI

- **노출 방식**: Add Source tile 목록은 `/api/v3/source/type` 응답에 `vlhfList.json` decoration을 더한 것이다. 근거: `dac/ui/src/pages/HomePage/components/modals/AddSourceModal/AddSourceModal.jsx:116-131`, `dac/ui/src/utils/FormUtils/SourceFormJsonPolicy.js:65-86`. UI가 직접 type을 추가하지 않으므로 backend 등록만 되면 tile이 나타난다.
- **이미 갖춰진 것**
  - lakehouse/metastore/dataLake 분류: `dac/ui/src/constants/sourceTypes.js:91-135`
  - icon: `dac/ui-lib/icons/dremio{,-dark}/sources/RESTCATALOG.svg`
  - `isIcebergSource`: `dac/ui/src/utils/sourceUtils.ts:67-82`
- **Layout 필수 요소**
  - `restcatalog-layout.json`에는 반드시 `"sourceType": "RESTCATALOG"`와 `"metadataRefresh": {"datasetDiscovery": true}`가 있어야 한다.
  - `metadataRefresh`가 없으면 `AddSourceModal.jsx:158`와 `EditSourceView.jsx:148`에서 TypeError가 난다.
  - `sourceType`이 없으면 create 요청이 `type: undefined`로 나가고, header icon은 `undefined.svg`가 된다 (`SourceFormJsonPolicy.js:316-328,448`).
- **Verifier 정정 (D-02)**: Add flow에서 form은 열린다. 실패하는 것은 그 뒤의 `passDataBetweenTabs` dispatch이고, 오류는 unhandled rejection으로 조용히 묻힌다. Edit flow는 `didLoadFail` 화면을 보여준다.
- **Preset**: layout JSON으로는 add mode에서 `propertyList`를 미리 채울 수 없다 (`PropertyListConfig.js:35-41`). Preset은 client-side에서 구현해야 한다.
  - 권장(Option A): `AddSourceModal`에 `RESTCATALOG`를 기반으로 하는 "Apache Polaris" virtual tile을 추가하고, `initialValues`로 값을 주입한다. 저장되는 backend type은 `RESTCATALOG` 그대로다.
  - 대안(Option B): `FormElementConfig.getRendererOverride`로 form 안에 preset selector를 둔다.
  - Option A 주의점:
    - 빈 값 row는 `Property.validate`에서 submit을 막는다 (`dac/ui/src/components/Forms/Property.jsx:41-43`)
    - deep-link(`location.state.selectedSourceType`) 경로에서는 preset이 우회된다
- **Edit round-trip**: masked 값 `$DREMIO_EXISTING_VALUE$`은 비어 있지 않아서 validate를 통과한다. 단, secret key 이름을 바꾸면 `applySecretsFrom`이 null 원소를 넣는다 (`sabot/kernel/src/main/java/com/dremio/exec/catalog/conf/ConnectionConf.java:183-205`).
- **UI test 현황**
  - `AddSourceModal-spec.js`는 `describe.skip`이다
  - `SelectSourceType-spec.js`는 smoke test 1개뿐이다
  - background `dac/ui` build가 돌고 있어서 UI test는 실행하지 않았다

### 3.5 Agent E — Polaris

- **Probe 방법**: Dremio의 Iceberg jar(및 pinned dependency)로 standalone client를 만들어 JDK 11에서 Polaris 1.1.0-incubating을 호출했다.
- **동작 확인**
  - initialize: token 발급 → `/v1/config?warehouse=` → `prefix`
  - nested namespace(`%1F`) list
  - createNamespace: Polaris가 `location`을 자동으로 채운다
  - table create/list/load
  - cross-namespace rename
  - view create/list/load/rename (`DremioSQL`)
  - dropTable(purge=false)
  - token refresh: 20s TTL token으로 검증
- **Verifier 정정**
  - 이 probe는 `io-impl=InMemoryFileIO`와 null Hadoop conf를 사용했다. 따라서 Dremio의 실제 data path(`DremioFileIO`, S3)는 **검증되지 않았다**.
  - 오류 hint를 `createCatalog`에 넣는 권고는 효과가 없다. lazy Supplier라서 오류가 그 위치를 지나가지 않는다. hint는 `getState`(`IcebergCatalogPlugin.java:243-256`)와 `getCatalog()` wrapper에 넣어야 한다.
  - CTAS는 `newCreateTableTransaction`을 location 없이 호출한다 (`AbstractRestCatalogAccessor.java:798-805`). 그래서 out-of-tree LOCATION은 commit 시점에 거부된다.
  - `IcebergCatalogPluginConfig.java:106-113` 인용은 잘못되었다. 파일은 87줄이며 올바른 위치는 `:41-48`이다.
- **Polaris 특이 동작**: [compatibility-matrix.md](compatibility-matrix.md) §b 참고.

---

## 4. Gap 목록

심각도: blocker > high > medium > low. 대상 Phase는 `polaris-catalog-support.md`의 Phase 정의를 따른다.

| ID | 영역 | Gap | 심각도 | 권장 조치 | 대상 Phase | 출처 |
|---|---|---|---|---|---|---|
| G-01 | Backend 등록 | `RestIcebergCatalogPluginConfig`에 `@SourceType`이 없다. `RESTCATALOG`가 등록되지 않아 목록에 없고 create는 400이 나며, 25.x에서 만든 source도 load되지 않는다 (`RestIcebergCatalogPluginConfig.java:26`) | blocker | `@SourceType(value = "RESTCATALOG", label = "Iceberg REST Catalog", uiConfig = "restcatalog-layout.json")`를 추가한다. 기존 class에 직접 붙이고 subclass는 만들지 않는다 | 2 | A-01, B-01, C-01, D-01 |
| G-02 | UI layout | `restcatalog-layout.json`이 없다. `metadataRefresh`나 `sourceType`이 빠지면 UI TypeError와 `type: undefined` 문제가 생긴다 | blocker | `plugins/icebergcatalog/src/main/resources/restcatalog-layout.json`을 만든다. 구조는 25.2.0 기반으로 하고 §7.3 contract를 따른다 | 2 | B-02, C-05, D-02, D-03 |
| G-03 | Config/API | `isUsingVendedCredentials` 필드가 없다. 공식 JSON이 strict Jackson에서 400이 될 가능성이 높다 | high | `@Tag(13) @DisplayMetadata(label = "Use vended credentials") public boolean isUsingVendedCredentials = false;`를 추가한다. 기본값 false는 공식 UI 기본값과 다르며 이유를 문서화한다 | 2 | C-02, C-03, E-05, B-08 |
| G-04 | Storage | Vended credentials를 runtime에서 쓰지 않는다. REST FileIO를 close하고 static Hadoop conf만 사용한다 (`AbstractRestCatalogAccessor.java:376-407`) | high | Phase 2에서는 true일 때 `header.X-Iceberg-Access-Delegation=vended-credentials`만 주입하고 "OSS 미지원"으로 문서화한다. Phase 4에서 loadTable config(`s3.*`)를 per-dataset FS conf에 매핑한다 (`DatasetFileSystemCache` hook, `isCachingPerDataset`) | 2 (flag) / 4 (runtime) | A-04, C-04, E-06 |
| G-05 | Polaris 설정 | Polaris는 `warehouse`와 `scope`를 반드시 요구한다. 없으면 각각 400 `Please specify a warehouse`, `invalid_scope`가 난다. Typed field도 검증도 없다 | high | layout help text, preset, 문서에 필수로 명시한다. Phase 3에서 `getState` 오류 hint를 추가한다. Polaris 전용 field는 추가하지 않는다 | 2 (docs/UI) / 3 (hint) | A-03, E-02, E-09 |
| G-06 | View | Polaris 기본 설정에서 DROP VIEW가 403 (purge)이다. `dropView`가 `NoSuchViewException`만 잡아서 raw RuntimeException이 그대로 나온다 (`RestIcebergCatalogPlugin.java:859-873`) | high | Polaris catalog property `polaris.config.drop-with-purge.enabled=true`를 문서화한다. `ForbiddenException`은 `CatalogEntityForbiddenException` 또는 `UserException.permissionError`로 매핑한다 (dropView, createView, updateView, dropTable) | 2 (docs) / 3 (code) | A-06, E-07 |
| G-07 | Storage | `fs.s3a.*`가 Hadoop conf에 들어가는 것이 lazy catalog build의 부수효과다. executor는 Polaris에 접근할 수 있어야 S3 설정을 얻는다 | high | `RestIcebergCatalogPlugin`에서 `start()` 시점에 property를 fsConf로 미리 복사한다 (`createFSCache` 전). unit test로 `getFsConfCopy()`에 key가 있는지 확인한다 | 2 | A-05, C(missed), E(missed) |
| G-08 | Storage (MinIO) | 공식 문서가 다루지 않는 S3-compatible 설정이 있다. endpoint에 scheme을 넣으면 실패하고, requester-pays 기본값이 true다 | high | 문서와 help text에 `fs.s3a.endpoint=<host>:<port>` (scheme 없음), `fs.s3a.connection.ssl.enabled=false`, `fs.s3a.path.style.access=true`, `dremio.s3.compat=true`, `fs.s3a.requester.pays.enabled=false`, `dremio.bucket.discovery.enabled=false`, `dremio.s3.region`을 명시한다. Phase 4에서 실측한다 | 2 (docs) / 4 (verify) | A(verifier), C-10 |
| G-09 | 오류 처리 | `createCatalog`의 catch는 dead code다. `invalid_scope`, `unauthorized_client`(HTTP 401이 `BadRequestException`으로 변환됨), warehouse 404 같은 오류가 raw message로 노출된다 | medium | `IcebergCatalogPlugin.getState`와 `getCatalog()` wrapper에서 Iceberg exception을 `UserException`으로 매핑하고 hint를 붙인다. secret은 포함하지 않는다 | 3 | E-02, E-10, E(missed) |
| G-10 | Namespace | Polaris에서 비어 있지 않은 namespace를 drop하면 `BadRequestException("… is not empty")`이 온다. `deleteFolder`는 `NamespaceNotEmptyException`만 잡는다 (`RestIcebergCatalogPlugin.java:277`) | medium | `BadRequestException`의 메시지가 "not empty"이면 `CatalogFolderNotEmptyException`으로 매핑한다 | 3 | E-08 |
| G-11 | Lifecycle | `ExpiringCatalogCache`가 사용 중인 catalog를 close한다. `checkState`는 매번 token과 `/config`를 호출하고 deprecation WARN을 남긴다. `getDefaultBaseLocation`에는 leak이 있다 | medium | 이전 catalog를 지연 close하거나 swap 시 close한다. `checkState`는 cached catalog를 재사용한다. try-with-resources를 쓴다 | 3 | A-09, E-03, E-19 |
| G-12 | Security | `secretPropertyList`는 at-rest에서 평문이다 (`encryptSecrets`는 `SecretRef` 전용). Hadoop conf와 `RESTCatalog.properties()`에 복사되고, `Property.toString()`은 값을 출력한다 | medium | 문서에 limitation으로 명시한다. 로그 금지 test를 둔다. REST auth key는 Hadoop conf에서 걸러낸다(선택). kernel 차원의 암호화는 open item이다 | 3 / 4 | A-11, B(missed), E(missed) |
| G-13 | Security/UI | `propertyList`(비밀이 아닌 목록)에 `credential`이나 `fs.s3a.secret.key`를 넣어도 막지 않는다. 이 값은 masking되지 않는다 | medium | UI validator 또는 경고, backend WARN을 둔다 (key 이름만 로그) | 2 (UI) / 3 (backend) | C(missed), D(missed) |
| G-14 | Config/API | masked secret의 key 이름을 바꾸면 `applySecretsFrom`이 null 원소를 넣고, 이후 NPE가 난다 (`ConnectionConf.java:183-205`, `RestIcebergCatalogPlugin.java:134-143`) | low | `getConfigPropertyList`에서 null을 건너뛰거나 명확한 오류를 낸다. UI에서는 이름이 바뀌면 값을 비운다 | 2 | A(missed), B(missed), D(missed) |
| G-15 | Polaris preset | "Apache Polaris" preset이 없다 | medium | client-side preset tile (Option A). backend type은 `RESTCATALOG` 그대로 | 2 | B-07, D-08, D-09 |
| G-16 | Test | registration, protostuff/Jackson round-trip, layout-필드 일치, property pass-through, fs.s3a 전파에 대한 test가 없다 | medium | §7 Agent D 작업 | 2 | A-14, B-10, C(missed), D-11 |
| G-17 | Config | `restEndpointUri`에 검증이 없다 | low | `@NotBlank`를 추가한다 (`Source.config`는 `@Valid`, `dac/backend/src/main/java/com/dremio/dac/api/Source.java:43`) | 2 | C-07 |
| G-18 | Label | `REST Iceberg Catalog`과 공식 `Iceberg REST Catalog`이 다르다 | low | `@SourceType.label`을 공식 명칭으로 한다. `sourceTypes.js:71`은 cosmetic이다 | 2 | B-11, C-06, D-06 |
| G-19 | UI | `AddSourceModal` errorHandler가 Response가 아닌 오류에서 `e.json()`을 호출해 rejection이 처리되지 않는다 | low | `metadataRefresh?.`와 safe errorHandler를 적용한다 | 2 | D-02, D(missed) |
| G-20 | Gating | `plugins.restcatalog.enabled=false`가 목록에서만 숨긴다. `GET /source/type/RESTCATALOG`와 create는 gate하지 않는다 | low | `getSourceByType`에도 `isSourceTypeVisible`을 적용한다(선택) | 3 | B-06, D-07 |
| G-21 | 이름 | `catalogName()`이 null이어서 table 이름이 `null.ns.t`가 된다 | low | source name을 반환한다 | 3 | A-10, E-01 |
| G-22 | DDL | CTAS/CREATE에 out-of-tree LOCATION을 주면 Polaris가 403을 commit 시점에 반환하고, raw로 노출된다 | medium | `ForbiddenException` 매핑, test 추가 | 3 / 5 | E-13 |
| G-23 | Read-only | `mutable.enabled=false`일 때 `getId()`가 throw한다. read 경로에 영향이 있는지는 미검증이다 | low | smoke test 후 필요하면 무조건 반환하도록 바꾼다 | 5 | A-16 |
| G-24 | Namespace | allowedNamespaces separator가 `.`이라 이름에 `.`이 들어간 namespace를 표현할 수 없다 | low | 문서화 (option 변경 방법) | 3 | A-17, E-11 |
| G-25 | View | Spark에서 만든 view도 첫 SQL representation을 사용한다 (dialect 우선순위를 무시) | low | 문서화 (engine별 view) | 6 | A-13 |
| G-26 | Upstream | 26.0.5 이후 IRC fix가 없다: 26.0.8 CTAS unpartitioned, 26.1.6 positional delete DML, 26.1.8 requester-pays, 26.1.10 CTAS, 26.1.11 stuck jobs / HTTP 405 partitioned | low | known-limitations에 기록하고 E2E case를 추가한다 | 5 / 6 | C-13 (정정) |
| G-27 | Auth model | `hasAccessPermission()`이 항상 true다. 모든 사용자가 하나의 Polaris principal을 쓴다 | low | 문서화. 최소 권한 principal 예시를 제공한다 | 3 (docs) | A-12, E-18 |
| G-28 | Data path | Dremio 내부 data path(DremioFileIO, S3, CTAS commit)는 실제 Polaris를 대상으로 한 번도 실행되지 않았다 | high | Phase 4/5 E2E (opt-in IT) | 4 / 5 | E(missed), B(missed) |

---

## 5. Polaris live probe 결과

### 5.1 실행 환경

- Polaris: `apache/polaris:1.1.0-incubating`. 0.10.0-beta는 미검증이다.
- 사용한 container와 port: `p1-polaris-a`:41128, `p1-vfy-polaris`:47391, `p1-b-polaris`:38181, `p1-polaris*`:18181/18171/18161, `p1-minio`:19000. 모두 제거했다.
- Client: Dremio Iceberg fork `1.7.0-5f7c992-20250730084652-3bf8b99`. Agent B는 빌드된 Dremio tarball에서 prototype source로도 검증했다.

### 5.2 결과 요약

| 항목 | 결과 |
|---|---|
| `scope=PRINCIPAL_ROLE:ALL` token | 200, `expires_in=3600` |
| `scope=catalog` (Iceberg 기본값) 또는 scope 없음 | 400 `{"error":"invalid_scope"}` |
| 잘못된 secret | HTTP 401 `unauthorized_client`. Iceberg는 이를 `BadRequestException: Malformed request: unauthorized_client`로 바꾼다 |
| credential 없음 | `NotAuthorizedException: Not authorized:` |
| `/v1/config` warehouse 없음 | 400 `Please specify a warehouse` |
| `/v1/config` warehouse 틀림 (대소문자 포함) | 404 `Unable to find warehouse X` |
| `/v1/config?warehouse=<cat>` | `overrides.prefix=<cat>`, `defaults.default-base-location`, `endpoints`(views 포함) |
| uri에 `/api/catalog` 누락 | 404 |
| `oauth2-server-uri` 미설정 | `<uri>/v1/oauth/tokens` 사용. 동작하지만 initialize마다 deprecation WARN |
| namespace 생성 (location 없음) | Polaris가 `location=<base>/<ns>/`를 자동으로 채운다. CTAS 조건을 만족한다 |
| nested namespace | 지원 (`%1F`). parent가 없으면 404 |
| create table / staged create (CTAS) / load | 성공 (root, data grant 없이) |
| table location이 namespace 밖 | 403 `Invalid locations … not in the list of allowed locations` |
| rename table/view (cross-namespace) | 성공. Dremio에는 rename 기능이 없다 |
| dropTable(purge=false) | 204 |
| DROP VIEW (기본 catalog) | 403 `Unable to purge entity … set … polaris.config.drop-with-purge.enabled` |
| DROP VIEW (`polaris.config.drop-with-purge.enabled=true`) | 204 |
| 비어 있지 않은 namespace drop | 400 `NamespaceNotEmptyException`. Iceberg는 `BadRequestException`으로 바꾼다 |
| `header.X-Iceberg-Access-Delegation=vended-credentials` | root에게도 403 `LOAD_TABLE_WITH_READ_DELEGATION`. `TABLE_READ_DATA`/`TABLE_WRITE_DATA` grant 후에는 `s3.*` credential 반환 |
| token refresh (TTL PT20S) | 기본값(`token-refresh-enabled=true`)에서 만료 이후에도 계속 동작. false면 만료 후 401 |
| Dremio source 생성 (Agent B prototype, annotation 추가) | 200, state `good`, namespace 표시, secret masking 정상 |
| `/v1/config`의 비표준 endpoint 항목 (`polaris/v1/...generic-tables`, `policies`) | Iceberg 1.7이 문제 없이 parse |
| Polaris FILE storage | feature flag 2개와 `polaris.readiness.ignore-severe-issues=true` 필요 |
| Polaris + MinIO S3 | AWS_* env와 storageConfigInfo의 `endpoint`/`pathStyleAccess` 사용(roleArn 없음). 1.1.0에서 `stsUnavailable`은 무시된다. `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION`을 켜면 실제 AWS로 요청한다(301) |

### 5.3 재사용 가능한 명령

> 아래 secret은 **로컬 테스트 전용 값**이다: Polaris root `s3cr3t`, MinIO `minioadmin`. 실제 환경에서는 `<client_id>:<client_secret>`, `<s3AccessKey>`, `<s3SecretKey>` placeholder 자리에 각자의 값을 넣는다.
> Container 이름은 `p1-` prefix로 짓고, 사용 후 `docker rm -f`로 제거한다.

```bash
# 공통 변수 (로컬 테스트 전용)
POLARIS_REALM=POLARIS
POLARIS_ROOT_ID=root
POLARIS_ROOT_SECRET=s3cr3t          # 로컬 테스트 전용
```

**(1) FILE storage Polaris (container 내부 storage, 가장 단순한 형태)**

```bash
docker run -d --name p1-polaris -p 18181:8181 -p 18182:8182 \
  -e POLARIS_BOOTSTRAP_CREDENTIALS=${POLARIS_REALM},${POLARIS_ROOT_ID},${POLARIS_ROOT_SECRET} \
  -e polaris.realm-context.realms=${POLARIS_REALM} \
  -e quarkus.otel.sdk.disabled=true \
  -e 'polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["FILE","S3"]' \
  -e 'polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true' \
  -e polaris.readiness.ignore-severe-issues=true \
  apache/polaris:1.1.0-incubating

curl -s http://localhost:18182/q/health            # {"status":"UP"} (~6s)

T=$(curl -s -X POST http://localhost:18181/api/catalog/v1/oauth/tokens \
  -d grant_type=client_credentials -d client_id=${POLARIS_ROOT_ID} \
  -d client_secret=${POLARIS_ROOT_SECRET} -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)

curl -s -X POST http://localhost:18181/api/management/v1/catalogs \
  -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  -d '{"catalog":{"name":"p1cat","type":"INTERNAL",
       "properties":{"default-base-location":"file:///tmp/polaris/p1cat",
                     "polaris.config.drop-with-purge.enabled":"true"},
       "storageConfigInfo":{"storageType":"FILE","allowedLocations":["file:///tmp/polaris"]}}}'   # 201

curl -s "http://localhost:18181/api/catalog/v1/config?warehouse=p1cat" -H "Authorization: Bearer $T"
curl -s http://localhost:18181/api/catalog/v1/p1cat/namespaces -H "Authorization: Bearer $T"
```

**(2) Host와 FILE warehouse를 공유하는 Polaris (Dremio가 host에서 같은 경로를 읽고 쓰는 경우)**

```bash
WH=/abs/path/to/warehouse   # Dremio host와 같은 절대경로
docker run -d --name p1-polaris-b --user $(id -u):$(id -g) \
  -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro -v $WH:$WH \
  -p 18171:8181 -p 18172:8182 <(1)과 같은 -e 옵션> apache/polaris:1.1.0-incubating
# catalog: storageConfigInfo {"storageType":"FILE","allowedLocations":["file://$WH"]},
#          default-base-location file://$WH/<cat>
# /etc/passwd와 /etc/group mount가 없으면 Hadoop UGI가 503 "Failed to get file system for path"로 실패한다
```

**(3) MinIO(S3) 기반 Polaris**

```bash
docker run -d --name p1-minio -p 19000:9000 -p 19001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  -e MINIO_DEFAULT_BUCKETS=warehouse bitnamilegacy/minio:latest       # 로컬 테스트 전용 credential

docker run -d --name p1-polaris-s --add-host=host.docker.internal:host-gateway \
  -p 18161:8181 -p 18162:8182 \
  -e POLARIS_BOOTSTRAP_CREDENTIALS=${POLARIS_REALM},${POLARIS_ROOT_ID},${POLARIS_ROOT_SECRET} \
  -e polaris.realm-context.realms=${POLARIS_REALM} -e quarkus.otel.sdk.disabled=true \
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e AWS_REGION=us-east-1 \
  -e polaris.readiness.ignore-severe-issues=true \
  apache/polaris:1.1.0-incubating

# catalog 생성 body (Polaris가 보는 endpoint에는 scheme이 있어야 한다)
{"catalog":{"name":"s3cat","type":"INTERNAL",
  "properties":{"default-base-location":"s3://warehouse/s3cat",
                "polaris.config.drop-with-purge.enabled":"true"},
  "storageConfigInfo":{"storageType":"S3","allowedLocations":["s3://warehouse/"],
                       "endpoint":"http://host.docker.internal:19000","pathStyleAccess":true}}}
# 금지: polaris.features."SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION"=true (endpoint를 무시하고 AWS로 요청, 301)
# vended credential을 시험할 때: Dremio에서 접근 가능한 주소를 storageConfigInfo.endpointInternal에 넣는 것을 검토
```

**(4) Dremio 전용 principal (root가 아닌 계정, Polaris Management API)**

이 프로젝트는 Polaris Management API를 구현하지 않는다(범위 제외). 아래는 테스트 fixture를 준비하기 위한 호출이다.

```text
POST /api/management/v1/principals        {"principal":{"name":"dremio","properties":{}},"credentialRotationRequired":false}
     → credentials.clientId / clientSecret
POST /api/management/v1/principal-roles   {"principalRole":{"name":"dremio_role"}}
PUT  /api/management/v1/principals/dremio/principal-roles {"principalRole":{"name":"dremio_role"}}
POST /api/management/v1/catalogs/p1cat/catalog-roles      {"catalogRole":{"name":"dremio_cr"}}
PUT  /api/management/v1/catalogs/p1cat/catalog-roles/dremio_cr/grants {"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}
PUT  /api/management/v1/principal-roles/dremio_role/catalog-roles/p1cat {"catalogRole":{"name":"dremio_cr"}}
# scope는 PRINCIPAL_ROLE:ALL 또는 PRINCIPAL_ROLE:dremio_role.
# CATALOG_MANAGE_CONTENT에는 read/write delegation 권한이 없다 (vended 사용 시 TABLE_READ_DATA / TABLE_WRITE_DATA 추가 필요)
```

**(5) 대응하는 Dremio RESTCATALOG source config (static credential, MinIO 기준)**

> `isUsingVendedCredentials`는 Phase 2에서 필드가 추가된 뒤에만 넣는다. 지금(26.0.5 HEAD) 넣으면 400이 날 가능성이 있다.
> MinIO endpoint는 Dremio 쪽에서는 **scheme 없이** `host:port`로 쓰고, Polaris 쪽에서는 `http://host:port`로 쓴다.

```json
{
  "entityType": "source",
  "type": "RESTCATALOG",
  "name": "polaris",
  "config": {
    "restEndpointUri": "http://localhost:18161/api/catalog",
    "isUsingVendedCredentials": false,
    "allowedNamespaces": [],
    "isRecursiveAllowedNamespaces": true,
    "propertyList": [
      {"name": "warehouse", "value": "s3cat"},
      {"name": "scope", "value": "PRINCIPAL_ROLE:ALL"},
      {"name": "oauth2-server-uri", "value": "http://localhost:18161/api/catalog/v1/oauth/tokens"},
      {"name": "fs.s3a.aws.credentials.provider", "value": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"},
      {"name": "fs.s3a.endpoint", "value": "localhost:19000"},
      {"name": "fs.s3a.connection.ssl.enabled", "value": "false"},
      {"name": "fs.s3a.path.style.access", "value": "true"},
      {"name": "dremio.s3.compat", "value": "true"},
      {"name": "dremio.bucket.discovery.enabled", "value": "false"},
      {"name": "dremio.s3.region", "value": "us-east-1"},
      {"name": "fs.s3a.requester.pays.enabled", "value": "false"}
    ],
    "secretPropertyList": [
      {"name": "credential", "value": "<client_id>:<client_secret>"},
      {"name": "fs.s3a.access.key", "value": "<s3AccessKey>"},
      {"name": "fs.s3a.secret.key", "value": "<s3SecretKey>"}
    ],
    "enableAsync": true,
    "isCachingEnabled": true,
    "maxCacheSpacePct": 100
  }
}
```

MinIO 관련 key 묶음은 정적 분석으로 도출한 것이며 Phase 4에서 실측한다. 상태: TBD (Phase 4).

---

## 6. 리스크 / 오픈 이슈

1. **`isUsingVendedCredentials` 기본값**
   - 공식 UI 기본값은 true다. 그러나 OSS는 vended credentials를 소비하지 못하고, Polaris는 root에게도 delegation 요청에 403을 준다.
   - 권장: Java 기본값을 `false`로 두고 문서화한다. 공식 문서와의 차이는 known limitation으로 기록한다.
   - Tag 13은 Enterprise와 호환되는지 확인할 수 없다. 10-19 범위 안이므로 OSS 내부에서는 안전하다.
2. **Label**: `@SourceType.label = "Iceberg REST Catalog"`(공식 명칭)을 권장한다. 25.2.0 값은 "REST Iceberg Catalog"였다.
3. **Preset UX**: Option A(별도 tile) 또는 Option B(form 안의 selector) 중 하나를 정해야 한다. 권장은 A다. 빈 `warehouse` row가 validate에서 submit을 막는 문제는 preset에 placeholder 값을 넣거나 help text로 대체해서 해결한다.
4. **Background `dac/ui` build**: 진행 중이다. Phase 2 Agent C는 build가 끝난 뒤에 UI test를 실행해야 하고, `dac/ui` Maven build를 새로 시작하지 않는다.
5. **Multi-node / executor**: G-07을 해결하기 전까지는 executor도 Polaris에 접근할 수 있어야 한다. Phase 2~5의 검증은 single-node 기준이다.
6. **Secret at-rest 암호화**: `List<Property>` secret을 at-rest로 암호화하는 일은 kernel 전체에 걸친 변경이다. Glue와 Hive에도 영향이 있으므로 이번 범위에서는 open item으로 둔다.
7. **DROP VIEW**: 문서로 안내하는 것(Polaris catalog property 설정)과 오류 메시지를 개선하는 것을 함께 진행한다. 0.10.0-beta에서는 미검증이다.
8. **Polaris 1.1.0에서만 관찰한 동작**: `stsUnavailable`이 무시되는 것, `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION`의 부작용.
9. **Upstream fix 누락 (G-26)**: 26.0.5 이후의 IRC 관련 fix가 이 tree에 없다. E2E에서 재현될 수 있다.
10. **Option gate**: `plugins.restcatalog.enabled=false`일 때 create가 어떻게 동작하는지 runtime으로 확인하지 않았다.
11. **Catalog rotation 중 commit 실패 (G-11)**: 가능성은 있지만 재현하지 않았다.

---

## 7. Phase 2 구현 계획

목표: `RESTCATALOG`를 OSS Source로 완성한다. 노출, create/get/update, layout, preset, vended flag, test까지 포함한다.

원칙:
- **파일 소유권을 겹치지 않게 나눈다.**
- Agent끼리는 아래 §7.3 contract로만 의존한다.
- commit과 push는 Lead만 한다.

### 7.1 파일 소유권

| Agent | 소유 파일 (이 Agent만 수정) | 작업 |
|---|---|---|
| **A — Backend 등록** | `plugins/icebergcatalog/src/main/java/com/dremio/plugins/icebergcatalog/store/RestIcebergCatalogPluginConfig.java` | ① `@SourceType(value = "RESTCATALOG", label = "Iceberg REST Catalog", uiConfig = "restcatalog-layout.json")`과 import 추가 (G-01, G-18) ② `@Tag(13) @DisplayMetadata(label = "Use vended credentials") public boolean isUsingVendedCredentials = false;` (G-03) ③ `restEndpointUri`에 `@NotBlank` (G-17) ④ tags 1-12는 그대로 둔다. 20-109는 사용하지 않는다 ⑤ `scripts/dev build plugins/icebergcatalog`을 실행하고 jar에 layout이 들어 있는지 확인. 이후 `scripts/dev build distribution/server` (Lead와 시점 조율) |
| **B — Config/API** | `plugins/icebergcatalog/src/main/java/com/dremio/plugins/icebergcatalog/store/RestIcebergCatalogPlugin.java` | ① `buildCatalogProperties`: `isUsingVendedCredentials=true`이고 사용자가 지정하지 않았으면 catalog props에만 `header.X-Iceberg-Access-Delegation=vended-credentials`를 추가한다. Hadoop conf에는 넣지 않는다 (G-04 flag) ② `getConfigPropertyList`에서 null 원소와 null name을 건너뛴다 (G-14) ③ `start()`(또는 `createFSCache` 이전)에서 `configPropertyList`를 fsConf로 미리 복사한다 (G-07) ④ 비밀 key(`credential`, `token`, `fs.s3a.secret.key`, `fs.s3a.access.key`, `fs.s3a.session.token`, `s3.secret-access-key`)가 `propertyList`에 있으면 WARN을 남긴다. 로그에는 key 이름만 쓴다 (G-13) ⑤ 빌드된 tarball로 `POST /api/v3/catalog`(공식 문서 JSON)와 `PUT /apiv2/source/{name}` round-trip을 수동 검증한다. 스크립트는 scratchpad에 둔다 |
| **C — UI** | `plugins/icebergcatalog/src/main/resources/restcatalog-layout.json` (신규), `dac/ui/src/pages/HomePage/components/modals/AddSourceModal/AddSourceModal.jsx`, `dac/ui/src/pages/HomePage/components/modals/EditSourceView.jsx`, `dac/ui/src/pages/HomePage/components/modals/AddSourceModal/SelectSourceType.jsx`, `dac/ui/src/utils/sourceUtils.ts`, `dac/ui/src/constants/sourceTypes.js`, UI spec (`SourceFormJsonPolicy-spec.js`, `SelectSourceType-spec.js`, `dac/ui/src/utils/mappers/sourcesMapper-spec.js`, 신규 `sourceUtils` preset spec) | ① layout 작성 (§7.3) (G-02) ② `metadataRefresh?.` optional chaining과 safe errorHandler (G-19) ③ "Apache Polaris" preset tile: `presetOf: 'RESTCATALOG'`. initialValues는 `scope=PRINCIPAL_ROLE:ALL`, `isUsingVendedCredentials=false`이고 warehouse/credential은 help text로 안내한다 (G-15) ④ `propertyList`에 민감한 key가 있으면 경고 (G-13) ⑤ secret key 이름이 바뀌면 값을 비운다 (G-14) ⑥ `sourceTypes.js:71`의 label을 맞추고 preset을 lakehouse로 분류한다 ⑦ background `dac/ui` build가 끝난 뒤 `DREMIO_UI_TESTS=… corepack pnpm run test:only`. dac/ui Maven build는 시작하지 않는다 |
| **D — Test** | `plugins/icebergcatalog/src/test/java/com/dremio/plugins/icebergcatalog/store/TestRestCatalogSourceRegistration.java` (신규), `.../store/TestRestCatalogLayout.java` (신규), `.../store/TestRestIcebergCatalogPluginConfig.java` (확장), `.../store/TestRestIcebergCatalogPlugin.java` (확장) | ① registration: `ConnectionReader.of(scan, ConnectionReaderImpl.class).getAllConnectionConfs().get("RESTCATALOG") == RestIcebergCatalogPluginConfig.class`, `getType()=="RESTCATALOG"` ② protostuff round-trip (tag 13 포함)과 25.2.0 tag layout bytes의 backward compatibility ③ `ConnectionConf.registerSubTypes`를 등록한 mapper로 공식 문서 JSON을 deserialize ④ secret masking: `clearSecrets`면 `$DREMIO_EXISTING_VALUE$`, `applySecretsFrom`이면 복원, 이름이 바뀐 경우 처리 ⑤ layout: classpath에 존재, JSON parse 가능, `sourceType`과 `metadataRefresh` 존재, 모든 `config.X` propName이 `@Tag` 필드와 일치 ⑥ `loadCatalog` props에 `warehouse`, `scope`, `credential`, `header.*`가 전달되고 vended header는 true일 때만 존재 ⑦ `start()` 직후 catalog 호출 없이 `getFsConfCopy()`에 `fs.s3a.*`가 있는지 ⑧ option false면 `validateOnStart`가 throw ⑨ 오류 메시지에 secret 값이 없는지. 실행: `scripts/dev test TestRestCatalogSourceRegistration TestRestCatalogLayout TestRestIcebergCatalogPluginConfig TestRestIcebergCatalogPlugin` |
| **Lead** | `docs/polaris/*.md`, commit/push | 통합, `scripts/dev fmt`, `scripts/dev lint`, regression(`scripts/dev test` 대상: icebergcatalog module의 test class), progress.md 갱신 |

### 7.2 의존 순서

```text
A (config field/annotation) ─┬─> B (plugin uses isUsingVendedCredentials)
                             ├─> C (layout propNames)
                             └─> D (registration/round-trip tests)
B ─> D (header/eager-conf tests)    C(layout) ─> D (layout consistency test)
```

모든 Agent는 §7.3 contract를 기준으로 **동시에 시작**한다. 컴파일과 test는 A가 끝난 뒤 통합 단계에서 실행한다.

### 7.3 Agent 간 Contract

- **Config 필드**: tags 1-5, 10-12는 이름과 의미를 바꾸지 않는다. 신규 필드는 다음 하나뿐이다.
  ```java
  @Tag(13) @DisplayMetadata(label = "Use vended credentials")
  public boolean isUsingVendedCredentials = false;
  ```
- **Vended header**: key는 `header.X-Iceberg-Access-Delegation`, 값은 `vended-credentials`. 사용자가 같은 key를 직접 지정했으면 그 값을 우선한다.
- **Layout**: `restcatalog-layout.json`의 최소 구조.
  ```json
  {
    "sourceType": "RESTCATALOG",
    "metadataRefresh": {"datasetDiscovery": true, "authorization": false},
    "form": {"tabs": [
      {"name": "General", "isGeneral": true, "sections": [
        {"elements": [
          {"propName": "config.restEndpointUri", "placeholder": "e.g. http://<polaris-host>:8181/api/catalog", "validate": {"isRequired": true}},
          {"propName": "config.isUsingVendedCredentials"}]},
        {"name": "Allowed Namespaces", "elements": [
          {"propName": "config.allowedNamespaces[]", "emptyLabel": "All namespaces are visible", "addLabel": "Add namespace"},
          {"propName": "config.isRecursiveAllowedNamespaces"}]}]},
      {"name": "Advanced Options", "sections": [
        {"name": "Catalog Properties", "elements": [{"propName": "config.propertyList", "emptyLabel": "No properties added", "addLabel": "Add property"}]},
        {"name": "Catalog Credentials", "elements": [{"propName": "config.secretPropertyList", "emptyLabel": "No credentials added", "addLabel": "Add credential", "secure": true}]},
        {"elements": [{"propName": "config.enableAsync"}]},
        {"name": "Cache Options", "checkboxController": "enableAsync", "elements": [
          {"propName": "config.isCachingEnabled"}, {"propName": "config.maxCacheSpacePct"}]}]}]}
  }
  ```
  - Tab 이름은 정확히 `"Advanced Options"`여야 한다 (`addAlwaysPresent`가 이 이름으로 찾는다).
  - Help text에는 Polaris 필수 key를 적는다: `warehouse`, `scope=PRINCIPAL_ROLE:ALL`, `credential`(Catalog Credentials에 넣음), `oauth2-server-uri`. MinIO key도 함께 적는다.
  - 정확한 layout DSL은 `plugins/awsglue/src/main/resources/awsglue-layout.json`과 `plugins/hive3/launcher/src/main/resources/hive3-layout.json:72-84`를 참고한다. `property_list`라는 uiType은 없다.
- **Preset**: 저장되는 backend type은 언제나 `RESTCATALOG`다. 별도 `@SourceType`은 만들지 않는다. 두 번째 `"RESTCATALOG"` class가 생기면 startup이 깨진다.

### 7.4 Phase 2 완료 기준 (Gate 세부)

- 빌드된 tarball에서 다음이 통과한다.
  - `GET /api/v3/source/type`에 RESTCATALOG가 있다
  - `GET /api/v3/source/type/RESTCATALOG`가 uiConfig를 포함해 반환된다
  - 공식 문서 JSON으로 `POST /api/v3/catalog`가 200이다
  - GET 응답이 masking되어 있다
  - masked 값으로 PUT해도 secret이 유지된다
  - Polaris(FILE 또는 MinIO)를 대상으로 state가 `good`이다
- §7.1 D의 test가 모두 PASS다. icebergcatalog module의 기존 test에 regression이 없다.
- UI spec이 PASS다. 이건 background build가 끝난 뒤에 실행한다. 실행할 수 없으면 `ENVIRONMENT_BLOCKED`로 기록한다.
- `scripts/dev fmt`와 `scripts/dev lint`가 clean이다.
