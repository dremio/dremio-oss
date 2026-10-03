# Polaris RESTCATALOG Object Storage (Phase 4)

- 작업 정의: [polaris-catalog-support.md](../reference_docs/catalog-support/polaris-catalog-support.md) Phase 4 (Agent A Polaris + S3 기본 경로, B S3-compatible / MinIO, C Vended Credentials, D 보안)
- 기준: `feature/polaris-restcatalog` Phase 3 `3b85d3619` + Phase 4 작업 트리 (review 반영 포함, §14.5). Apache Polaris `1.1.0-incubating`, Iceberg `1.7.0-5f7c992-20250730084652-3bf8b99` (Dremio fork), 로컬 MinIO (plain HTTP, region `us-east-1`), TLS/region 검증용 throwaway MinIO (`bitnamilegacy/minio`).
- 원칙: Dremio의 기존 S3 plugin(`plugins/s3` `S3FileSystem`)을 그대로 쓴다. MinIO 전용 code나 Polaris 전용 engine은 없다. Vended credential은 Iceberg REST client가 table FileIO에 합쳐 준 property를 읽어서 쓴다 (응답을 직접 parsing하지 않는다).
- 상태 값: `PASS` / `FAIL` / `ENVIRONMENT_BLOCKED` / `NOT_SUPPORTED`. 실제 AWS 계정이 없으므로 **AWS S3 전용 항목은 모두 `ENVIRONMENT_BLOCKED`** 다.
- 모든 secret은 placeholder다: `<client_id>:<client_secret>`, `<access-key>`, `<secret-key>`.
- 관련 문서: [security.md](security.md) (secret 노출 matrix), [compatibility-matrix.md](compatibility-matrix.md) (d) Storage, [progress.md](progress.md).

## 1. 요약

| 항목 | 결과 | Status |
|---|---|---|
| 공식 baseline (`isUsingVendedCredentials=false`, `SimpleAWSCredentialsProvider`, secret에 S3 key) + MinIO endpoint 설정 | CREATE TABLE, INSERT, CTAS, INSERT…SELECT, UPDATE, DELETE, metadata table, `AT SNAPSHOT`, Dremio 재시작 후 SELECT/INSERT | **PASS** (MinIO) |
| 공식 baseline을 실제 AWS S3에서 | AWS 계정 없음 | ENVIRONMENT_BLOCKED |
| `fs.s3a.aws.credentials.provider` 생략 | Phase 4 이전: 첫 S3 접근에서 `Invalid AWSCredentialsProvider provided`. 수정 후: Hadoop 기본 chain 대신 `SimpleAWSCredentialsProvider`로 access key를 쓴다. key가 없으면 host identity로 넘어가지 않고 실패한다 | FAIL → **PASS** (수정, §10) |
| `fs.s3a.endpoint`에 scheme (`http://host:port`) | Phase 4 이전: `http://http://…`로 요청, INSERT/SELECT hang. 수정 후: scheme 유지 | FAIL → **PASS** (수정, §11) |
| MinIO recipe (custom endpoint, path-style, SSL/TLS, region, requester-pays) | §5–§9 | PASS (항목별 matrix §14) |
| Vended credentials (`isUsingVendedCredentials=true`, Polaris OSS + MinIO STS) | static S3 key 없이 SELECT/INSERT/CTAS/UPDATE/DELETE/OPTIMIZE, credential 만료 후 자동 갱신. static key도 있는 source는 새 table을 static key로 쓴다 (review 반영, §12.3) | **PASS** (compatibility 결과, single node) |
| Secret 노출 (log, REST, profile, system table) | 0건. HttpClient 4 header logger와 Netty `LoggingHandler`(SDK v2 async 읽기)는 `logback.xml`에서 INFO로 고정 (§13, [security.md](security.md)) | PASS |

## 2. Storage 구조: 누가 무엇을 읽고 쓰나

### 2.1 설정이 S3까지 가는 경로

1. Source의 `propertyList`(Catalog Properties)와 `secretPropertyList`(Catalog Credentials)를 이 순서로 합친다 (`RestIcebergCatalogPlugin.getConfigPropertyList`). 같은 key가 둘 다 있으면 secret 쪽 값이 이긴다.
2. `applyConfigPropertiesToFsConf`가 REST client 전용 key(`credential`, `token`, `scope`, `oauth2-server-uri`, `audience`, `resource`, `token-exchange-enabled`/`token-refresh-enabled`/`token-expires-in-ms`, `header.*`, `rest.auth.*`, `rest.client.*`, SigV4용 `rest.access-key-id`/`rest.secret-access-key`/`rest.session-token`, token-exchange type key)를 뺀 모든 key를 Hadoop conf로 복사한다. 그 밖의 `rest.*` key는 복사된다 (secret 아님). `start()`의 `createCatalog`에서 catalog를 부르기 전에 한 번, 그리고 `getFsConfCopy()`마다 실행된다 (G-07, executor는 catalog에 접근하지 않아도 S3 설정을 갖는다).
3. Table 파일 접근은 `IcebergCatalogPlugin.createFS` → `DatasetFileSystemCache`다. `s3://`를 `dremioS3://`로 바꾸고 `FileSystemConfUtil.initializeConfiguration`(provider 결정, bucket별 key 반영)을 거쳐 Dremio `S3FileSystem`을 만든다.
4. `S3FileSystem`
   - `setup()`: `dremio.s3.compat=true`가 아니면 AWS STS `GetCallerIdentity`로 credential을 검증한다 (MinIO key로 실제 AWS를 부른다).
   - Bucket discovery(기본 on)는 FS 시작 시 `ListBuckets`를 부르고, 처음 보는 bucket은 root listing으로 확인한다. Vended credential을 적용한 table FS는 discovery를 끈다 (§12.2).
   - 쓰기, list, head는 Hadoop S3A(AWS SDK v1), 읽기는 SDK v2 async reader다. 두 client가 읽는 key가 다르다 (§4 "읽는 client" 열).
5. `AbstractRestCatalogAccessor`가 table의 FileIO를 `DremioFileIO`로 바꾼다 (G-04). Commit은 REST `updateTable`이므로 새 `metadata.json`은 **Polaris가** 쓴다.

### 2.2 읽기/쓰기 주체 (B의 throwaway MinIO trace, Polaris와 Dremio에 서로 다른 S3 user)

| Object | 쓰는 쪽 | 읽는 쪽 | 필요한 credential |
|---|---|---|---|
| `metadata/*.metadata.json` | **Polaris** (create, commit) | Polaris, Dremio | Polaris의 storage credential (container의 `AWS_*` env 또는 IAM role). Polaris는 vended를 요청받지 않아도 STS로 subscope된 임시 key를 만들어 쓴다 |
| `data/*.parquet` | Dremio | Dremio | Source의 S3 설정 (static key 또는 vended) |
| Manifest (`*-m0.avro`), manifest list (`snap-*.avro`) | Dremio | Dremio | 위와 같음 |

- 그래서 **양쪽 모두 자기 storage credential이 필요하다.** Dremio S3 user에 필요한 권한: `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`. AWS에서 bucket discovery를 켜 두면 `s3:ListAllMyBuckets`도 필요하다 (code 기준, AWS 미검증). Vended credential에는 이 권한이 없으므로 vended table FS는 discovery를 자동으로 끈다.
- Polaris storage credential이 틀리면: CREATE TABLE이 `Failed to get subscoped credentials … Sts 403`으로 실패한다. CTAS는 매핑되지 않은 `SYSTEM ERROR: RESTException`이 되고 Dremio가 이미 쓴 data file을 지운다.
- `DROP TABLE`은 Polaris에서 table만 지우고 S3 object는 남긴다 (purge 없음).
- `SELECT count(*)`(filter 없음)는 Polaris `loadTable` 응답의 snapshot summary로 답할 수 있어 S3를 읽지 않는다. S3 설정 확인에는 `SELECT *`를 쓴다 (Integration 관찰).

## 3. 공식 baseline 설정

### 3.1 UI (Add Source → Iceberg REST Catalog, 또는 "Apache Polaris OSS" preset)

| Tab / 필드 | 값 | 비고 |
|---|---|---|
| General / Name | `polaris` | |
| General / Endpoint URI | `http://<polaris-host>:8181/api/catalog` | |
| General / Use vended credentials | **해제** (`isUsingVendedCredentials=false`) | 켜는 경우는 §12 |
| Advanced Options / Catalog Properties | `warehouse` = `<catalog>` | Polaris catalog 이름 (대소문자 일치) |
| | `scope` = `PRINCIPAL_ROLE:ALL` | |
| | `fs.s3a.aws.credentials.provider` = `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` | 공식 값. Phase 4 수정 후에는 생략해도 된다 (§10) |
| | (MinIO 등 S3-compatible이면) `fs.s3a.endpoint` = `<host>:<port>`, `fs.s3a.path.style.access` = `true`, `dremio.s3.compat` = `true`, (HTTP이면) `fs.s3a.connection.ssl.enabled` = `false` | §5 |
| Advanced Options / Catalog Credentials | `credential` = `<client_id>:<client_secret>` | |
| | `fs.s3a.access.key` = `<access-key>` | |
| | `fs.s3a.secret.key` = `<secret-key>` | |

### 3.2 REST API (`POST /api/v3/catalog`)

```json
{
  "entityType": "source",
  "type": "RESTCATALOG",
  "name": "polaris",
  "config": {
    "restEndpointUri": "http://<polaris-host>:8181/api/catalog",
    "isUsingVendedCredentials": false,
    "propertyList": [
      {"name": "warehouse", "value": "<catalog>"},
      {"name": "scope", "value": "PRINCIPAL_ROLE:ALL"},
      {"name": "fs.s3a.aws.credentials.provider", "value": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"},
      {"name": "fs.s3a.endpoint", "value": "<minio-host>:9000"},
      {"name": "fs.s3a.connection.ssl.enabled", "value": "false"},
      {"name": "fs.s3a.path.style.access", "value": "true"},
      {"name": "dremio.s3.compat", "value": "true"}
    ],
    "secretPropertyList": [
      {"name": "credential", "value": "<client_id>:<client_secret>"},
      {"name": "fs.s3a.access.key", "value": "<access-key>"},
      {"name": "fs.s3a.secret.key", "value": "<secret-key>"}
    ]
  }
}
```

- AWS S3에서는 `fs.s3a.endpoint`, `fs.s3a.connection.ssl.enabled`, `fs.s3a.path.style.access`, `dremio.s3.compat` 4개를 뺀다 (`dremio.s3.compat=true`를 AWS에서 쓰면 안 된다).
- `oauth2-server-uri`는 선택이다. 생략하면 `<uri>/v1/oauth/tokens`를 쓰고 Iceberg가 deprecation WARN을 남긴다. Integration baseline(`base`)은 생략한 상태로 PASS.
- S3 key를 `propertyList`에 넣어도 동작하지만 (A C2) GET 응답에 값이 그대로 나오고 UI가 저장을 막는다. 반드시 Catalog Credentials에 넣는다.

## 4. Property reference

"AWS" / "MinIO" 열: 필수 / 권장 / 불필요 / 금지. "읽는 client": S3A = Hadoop S3A(SDK v1, 쓰기/list), v2 = SDK v2 (읽기, bucket 확인).

| Key | 용도 | 기본값 | AWS S3 | MinIO (HTTP, IP endpoint) | 읽는 client | 근거 |
|---|---|---|---|---|---|---|
| `warehouse`, `scope`, `credential` | Polaris REST/OAuth2 | — | 필수 | 필수 | REST | Phase 3 |
| `fs.s3a.access.key` / `fs.s3a.secret.key` (Catalog Credentials) | static S3 key | — | 필수 (static) | 필수 (static) | 둘 다 | A C1, C3 |
| `fs.s3a.aws.credentials.provider` | S3 credential provider (값 하나) | Hadoop 기본 chain → Phase 4부터 `SimpleAWSCredentialsProvider`로 바꾼다. 빈 값을 명시하면 key / `AWS_*` env / instance profile 순으로 결정 | 권장 (공식 값) | 권장 | 둘 다 | §10 |
| `fs.s3a.endpoint` | custom endpoint. `host:port` 권장, Phase 4부터 `http(s)://host:port`도 가능 | AWS | 불필요 | **필수** | 둘 다 | A E9, B (a), §11 |
| `fs.s3a.connection.ssl.enabled` | endpoint에 scheme이 없을 때 http/https 선택 | `true` | 불필요 | HTTP이면 **필수** (`false`) | 둘 다 | A E4, B (c) |
| `dremio.s3.compat` | AWS STS 검증 생략, endpoint 그대로 사용 | `false` | **금지** | **필수** (`true`) | Dremio | A E8, B (f) |
| `fs.s3a.path.style.access` | path-style 요청 | `false` | 불필요 | 권장. hostname endpoint면 **필수** | 둘 다 | A E5–E7, B (b) |
| `fs.s3a.endpoint.region` | signing region (S3A와 v2 모두) | — | 선택 | MinIO region이 `us-east-1`이 아니면 **필수** | 둘 다 | B (d) |
| `dremio.s3.region` | v2 client region (우선순위 1위). S3A는 읽지 않는다 | — | 선택 | 불필요 (단독으로는 non-default region에서 FAIL) | v2 | B (d) |
| `dremio.bucket.discovery.enabled` | FS 시작 시 `ListBuckets` | `true` | S3 user에 `s3:ListAllMyBuckets`가 없으면 `false` (code 기준). Vended credential table은 자동으로 `false` | 불필요 (MinIO는 거부 대신 filtering) | Dremio | A E11, B (f), review |
| `fs.s3a.requester.pays.enabled` | `x-amz-request-payer: requester` header | `true` | 권장 `false` (requester-pays bucket이 아니면) | 불필요 (MinIO는 무시) | v2 | A E12, B (e) |
| `fs.s3a.attempts.maximum`, `fs.s3a.retry.limit` | S3A 재시도 | Dremio/Hadoop 기본 | 선택 | 선택. 잘못된 endpoint 진단 시 `1`이면 빠르게 실패 (기본값이면 약 8분) | S3A | B |
| `fs.s3a.bucket.<bucket>.*` | bucket별 endpoint/key/provider/SSL. Dremio(`FileSystemConfUtil`)와 S3A가 전역 값 위에 덮어쓴다 | — | 선택 | 선택 | 둘 다 | B unit |
| (vended) `fs.s3a.bucket.<bucket>.access.key` / `.secret.key` / `.session.token` / `.aws.credentials.provider` | 위와 같음 | — | vended credential을 받은 table에서는 무시된다 (적용 시 제거). 받지 못한 table에서는 그대로 쓰인다 | 같음 | 둘 다 | review (unit, live `vbp`) |
| `s3.endpoint`, `s3.path-style-access` (Iceberg FileIO key) | Iceberg S3FileIO용 | — | 쓰지 않는다 | 쓰지 않는다 (S3A는 무시해 실제 AWS로 간다) | v2만 | B (a) |
| `oauth2-server-uri` | token endpoint | `<uri>/v1/oauth/tokens` | 선택 | 선택 | REST | A |

## 5. MinIO recipe (최소 설정)

Catalog Properties:

```text
warehouse=<catalog>
scope=PRINCIPAL_ROLE:ALL
fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
fs.s3a.endpoint=<host>:<port>
fs.s3a.connection.ssl.enabled=false        # HTTP MinIO일 때만
fs.s3a.path.style.access=true
dremio.s3.compat=true
fs.s3a.endpoint.region=<minio-region>      # MinIO region이 us-east-1이 아닐 때만. AWS region id여야 한다
```

Catalog Credentials: `credential=<client_id>:<client_secret>`, `fs.s3a.access.key=<access-key>`, `fs.s3a.secret.key=<secret-key>`.

- 없어도 되지만 넣어도 무해: `dremio.bucket.discovery.enabled=false`, `dremio.s3.region`, `fs.s3a.requester.pays.enabled=false`. Phase 2의 recipe(이 key 포함, harness 기본값)도 그대로 PASS다 (Integration `rec`).
- Polaris catalog storage config (Management API `storageConfigInfo`): `storageType=S3`, `endpoint=http://<host>:<port>` (**scheme 필수**, 없으면 "Illegal character in scheme name"), `pathStyleAccess=true`, `region=<minio-region>` (생략하면 container의 `AWS_REGION`), `allowedLocations=["s3://<bucket>/<prefix>/"]`. Polaris container에는 `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION`을 env로 준다 (harness `polaris-up.sh` 참고).

## 6. SSL / TLS

| 경우 | 설정 | 결과 (B) |
|---|---|---|
| HTTP MinIO | `fs.s3a.connection.ssl.enabled=false` (또는 endpoint `http://…`) | PASS |
| HTTP MinIO, SSL 설정 생략 | 기본 `true` → `SSLException: Unsupported or unrecognized SSL message` | FAIL (예상) |
| TLS MinIO (공인 CA 또는 truststore 등록) | `ssl.enabled=true` 또는 생략, endpoint IP/hostname | PASS |
| TLS MinIO, self-signed CA를 신뢰하지 않음 | `PKIX path building failed`. Polaris도 같은 원인으로 422 | FAIL (예상) |
| TLS endpoint에 `ssl.enabled=false` | `AmazonS3Exception: Bad Request (400)` | FAIL (예상) |

Self-signed CA 사용 시:
- JDK `cacerts` **복사본**에 CA를 추가한 truststore를 만든다 (공인 CA도 계속 신뢰하도록).
- Dremio: coordinator와 모든 executor의 `conf/dremio-env`에 `DREMIO_JAVA_SERVER_EXTRA_OPTS="-Djavax.net.ssl.trustStore=<path> -Djavax.net.ssl.trustStorePassword=<password> -Djavax.net.ssl.trustStoreType=JKS"`.
- Polaris: 같은 truststore를 mount하고 `JAVA_OPTS_APPEND`에 같은 option을 준다.
- Harness에는 truststore mount option이 아직 없다 (B 제안 P5, Phase 5).

## 7. Region

- MinIO 기본 region(`us-east-1`)이면 region key가 필요 없다.
- MinIO region이 다르면 `fs.s3a.endpoint.region=<region>`을 쓴다. `dremio.s3.region`만 주면 S3A(SDK v1) 경로가 무시하고 `us-east-1`로 서명해 `AuthorizationHeaderMalformed`가 난다 (B).
- 잘못된 region(`dremio.s3.region=ap-northeast-2`, MinIO는 `us-east-1`): "region is wrong; expecting 'us-east-1'". INSERT에서는 원래 오류 대신 "Memory was leaked by query"가 보인다 (오류 표시 문제, Phase 5 확인).
- AWS region id가 아닌 이름(MinIO `minio-local` 등)은 `S3PluginUtils`의 검사에 걸려 "minio-local is not a valid AWS region."이 된다 → **NOT_SUPPORTED** (B 제안 P3: compat mode에서 허용, 미구현).

## 8. Path-style

- IP endpoint(`127.0.0.1:9000`)는 `path.style.access=false`여도 두 SDK가 자동으로 path-style을 쓴다 (A E5, B).
- Hostname endpoint는 `fs.s3a.path.style.access=true`가 필수다. 없으면 `dremiodev.<host>`로 virtual-host 요청을 보낸다: A는 INSERT가 `UnknownHost`로 300초 이상 hang, B는 `InvalidBucketName`/400.
- Iceberg key `s3.path-style-access`는 S3A에 효과가 없고, Hadoop 기본값 `fs.s3a.path.style.access=false`가 항상 있어서 v2 client에서도 이기지 못한다 (B unit).

## 9. Requester-pays

- Dremio 기본값은 `true`다 (`S3ClientProperties.REQUESTER_PAYS_DEFAULT`). v2 client가 `x-amz-request-payer: requester`를 보낸다 (B MinIO trace: 22건).
- MinIO는 header를 무시하므로 `false`가 필요 없다 (PASS).
- AWS의 requester-pays bucket과 일반 bucket에서의 동작: AWS 계정 없음 → **ENVIRONMENT_BLOCKED**. 일반 bucket이면 `fs.s3a.requester.pays.enabled=false`를 권장한다.

## 10. Credential provider (Phase 4 수정)

- 원인: Hadoop `core-default.xml`의 `fs.s3a.aws.credentials.provider` 기본값은 provider 4개(`TemporaryAWSCredentialsProvider`, `SimpleAWSCredentialsProvider`, `EnvironmentVariableCredentialsProvider`, `IAMInstanceCredentialsProvider`)의 chain이다. Dremio는 `fs.*` 기본값을 모든 FS conf에 복사하므로 source가 key를 주지 않으면 이 chain이 남는다.
  - Dremio의 S3 client(`AwsCredentialProviderUtils`, SDK v2)는 provider 하나만 받으므로 `Invalid AWSCredentialsProvider provided`로 실패했다 (A E2, B).
  - Hadoop S3A(SDK v1, metadata와 쓰기)는 chain을 차례로 시도한다. key가 없으면 Dremio host의 `AWS_*` env와 EC2 instance profile까지 간다 (review live 1차 `snp`: CTAS가 "Unable to load AWS credentials from environment variables"로 실패. env를 지운 harness라서 실패했고, env나 EC2 role이 있는 host였다면 그 identity로 썼다).
- 수정: `RestIcebergCatalogPlugin.replaceHadoopDefaultCredentialsProvider`가 값이 **Hadoop 기본 chain과 정확히 같을 때만** `SimpleAWSCredentialsProvider`로 바꾼다. Source에서 지정한 provider(빈 값 포함)는 그 뒤에 복사되므로 이기고, `core-site.xml`의 다른 값은 그대로 둔다. Generic 동작이며 MinIO 전용이 아니다.
  - access key가 있으면 공식 baseline과 같은 설정이 된다 (provider 생략 = Simple).
  - access key가 없으면 두 client 모두 실패한다 (**fail closed**): 읽기는 `PERMISSION ERROR: Access denied on …metadata.json`, 쓰기는 `NoAwsCredentialsException: SimpleAWSCredentialsProvider: No AWS credentials in the Hadoop configuration`. host identity로 넘어가지 않는다.
  - Review 이전 Phase 4 작업 트리는 chain을 빈 값으로 바꿨다. 그러면 key가 없을 때 `FileSystemConfUtil`이 `AWS_*` env(→ Simple + env key) 또는 instance profile을 골라 host identity를 조용히 썼다 (review finding). 지금은 이 동작을 원하면 provider를 **빈 값으로 명시**해야 한다 (opt-in, 아래).
- Host identity opt-in: `fs.s3a.aws.credentials.provider=`(빈 값)를 명시하고 key를 넣지 않으면 `FileSystemConfUtil`이 `AWS_ACCESS_KEY_ID`/`AWS_ACCESS_KEY` + `AWS_SECRET_ACCESS_KEY`/`AWS_SECRET_KEY` env(Hadoop conf로 복사), 없으면 EC2 instance profile을 쓴다. Vended source에서 credential을 받지 못한 table(조회 실패, 403, vending 없음)도 이 경로를 탄다. Live `sne`(빈 값, env 없음, EC2 아님): `NoAuthWithAWSException: No AWS Credentials provided by InstanceProfileCredentialsProvider` → instance profile 자체는 ENVIRONMENT_BLOCKED, env 경로는 미검증.
- 검증: unit `TestRestCatalogStorageConfig#testMissingProviderUsesTheAccessKey`, `#testOnlyHadoopBuiltInProviderIsReplaced`, `#testSourceWithoutS3CredentialsDoesNotFallBackToTheHostIdentity`, `#testSourceProviderIsKept`, `#testBlankProviderIsDerivedFromAccessKey`. Live(Integration `noprov`, review INSTANCE=6 `noprov` 재시작 후 포함): provider key 없이 SELECT/CREATE/INSERT/CTAS PASS. Review `snp`(key·provider 없음): fail closed. `sne`: opt-in 경로.
- Vended credential을 받은 table은 provider를 명시적으로 덮는다 (`TemporaryAWSCredentialsProvider`, session token이 없으면 Simple). 받지 못한 table은 source 설정을 그대로 쓴다: static key가 있으면 그 key, 없으면 fail closed, provider를 빈 값으로 명시했으면 host identity (§12.4).

## 11. Endpoint scheme (Phase 4 수정)

- 원인: `S3FileSystem.getEndpoint()`가 endpoint 앞에 무조건 `http(s)://`를 붙였다. `http://127.0.0.1:9000`이면 S3A가 `http://http://127.0.0.1:9000`을 받아 "Unable to execute HTTP request: http"(UnknownHost)로 재시도하다 약 8분 뒤 실패했다 (G-08, A E10, B). SDK v2 쪽 `S3ClientProperties.createUriFromEndpoint`는 이미 scheme을 유지했다.
- 수정 (`plugins/s3`, 1줄): endpoint에 `://`가 있으면 그대로 쓰고, 없을 때만 `fs.s3a.connection.ssl.enabled`에 따라 scheme을 붙인다. S3 source와 다른 catalog plugin에도 같은 효과다. 기존 `host:port` 설정의 결과는 바뀌지 않는다.
- 검증: unit `TestS3FileSystem#testGetEndpointAddsSchemeOnlyWhenMissing`, `#testGetEndpointKeepsScheme`, `plugins/s3` 112 tests. Live(Integration `scheme`, `scheme2`): `fs.s3a.endpoint=http://127.0.0.1:9000`으로 SSL key가 있든 없든 SELECT/CREATE/INSERT/CTAS PASS. B는 같은 patch로 `https://` TLS MinIO도 PASS를 확인했다.
- 문서 권장 형식은 계속 `host:port` + `fs.s3a.connection.ssl.enabled`다.

## 12. Vended credentials (compatibility 결과)

### 12.1 Polaris OSS가 주는 것 (C 분석)

- `X-Iceberg-Access-Delegation: vended-credentials` header가 있으면 loadTable 응답의 `config`에 임시 S3 credential이 온다: `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`(STS session token), `s3.session-token-expires-at-ms`, `expiration-time`(기본 수명 3600초), `s3.endpoint`, `client.region`, `s3.path-style-access`, `client.refresh-credentials-endpoint`. 같은 값이 `storage-credentials[]`(prefix = table location)에도 온다.
- Polaris는 MinIO STS `AssumeRole`(catalog의 storage user + session policy)로 만든다. `roleArn`/`stsEndpoint` 없이 harness catalog(`endpoint`, `pathStyleAccess`, `region`)와 `CATALOG_MANAGE_CONTENT`만으로 동작했다.
- Scope: table prefix만 list/read/write 가능, 부모 prefix, bucket root, 다른 prefix는 거부. Read-only principal에는 read-only credential. 만료 후 MinIO가 거부 ("The Access Key Id you provided does not exist").
- Staged create(`stage-create=true`)도 기본 table location용 credential을 준다 (commit하지 않으면 table은 생기지 않는다).

### 12.2 Iceberg library와 Dremio

- Iceberg 1.7 `RESTSessionCatalog`는 `config`를 table FileIO property에 합친다 (S3FileIO라면 그대로 쓴다). `storage-credentials`는 FileIO에 넘기지 않는다.
- Dremio는 그 FileIO를 `DremioFileIO`(Hadoop `S3FileSystem`)로 바꾸므로 (G-04) library만으로는 vended credential이 쓰이지 않는다. Phase 4 이전에는 무시되었다.
- **구현 (C)**: generic RESTCATALOG 기능이다. 기존 `DatasetFileSystemCache`의 per-dataset hook과 Dremio S3 plugin을 쓴다.
  - `CatalogAccessor.loadTableStorageProperties(table, stageNewTable)`: `catalog.loadTable(...).io().properties()`. Table이 아직 없으면(CTAS는 commit 전에 data를 쓴다) `stageNewTable`일 때만 기본 location의 staged create를 만들어 property만 읽고 commit하지 않는다. Plugin은 source에 자기 S3 credential(access key, 또는 key가 필요 없는 provider)이 **없을 때만** staged create를 쓴다: static key가 있는 source는 새 table을 static key로 쓴다 (Phase 4 이전과 같은 동작, 기본 location 밖 `LOCATION`도 가능). 403/401은 redact된 permission error.
  - Plugin은 FileIO property에서 source 자신의 catalog property(같은 key, 같은 값)를 뺀다. Iceberg REST client가 catalog property(= 모든 source property)를 table FileIO property에 합치므로, source에 넣은 `s3.access-key-id` 등을 vended credential로 오인하지 않기 위해서다 (review).
  - `VendedStorageCredentials`: `s3.access-key-id` / `s3.secret-access-key` / `s3.session-token`을 `fs.s3a.access.key` / `fs.s3a.secret.key` / `fs.s3a.session.token`으로 옮기고 provider를 `TemporaryAWSCredentialsProvider`(session token이 없으면 `SimpleAWSCredentialsProvider`)로 정한다. 만료는 `s3.session-token-expires-at-ms`와 `expiration-time` 중 이른 값. Credential만 옮긴다: endpoint, region, path-style, TLS는 source 설정을 쓴다. 예외 두 가지 (review): bucket별 credential 설정(`fs.s3a.bucket.<bucket>.access.key`/`.secret.key`/`.session.token`/`.aws.credentials.provider`)을 지우고(그대로 두면 전역 값 위에 복사되어 source key와 vended session token이 섞인다), `dremio.bucket.discovery.enabled=false`로 둔다(table prefix로 scope된 credential은 `ListAllMyBuckets`와 bucket root listing 권한이 없다). `toString()`은 값을 보여 주지 않는다.
  - `VendedCredentialsCache`(node별 Caffeine): 만료 min(5분, 남은 수명의 절반) 전에 갱신. "credential 없음"은 5분, 조회 실패는 30초 cache하고 그동안 source의 storage 설정을 쓴다 (redact된 WARN). Credential은 plan/fragment로 전달되지 않고 각 node가 catalog에 직접 묻는다. Entry는 monotonic ticker로 만료되므로, wall clock이 앞으로 뛰어 유효 시각이 지난 결과는 `get()`에서 다시 조회한다 (review). 이 node에서 실행한 CREATE TABLE/CTAS/DROP TABLE은 그 table의 credential과 FS를 비운다 (같은 이름으로 다른 location에 다시 만든 table이 이전 credential을 쓰지 않도록. 다른 node의 cache는 만료로 갱신).
  - `DatasetFileSystemCache`: per-dataset caching flag, conf marker `dremio.icebergcatalog.fs.expires-at-millis`로 entry별 만료 (FS 생성 전에 제거). 이미 지난 만료 시각이어도 entry 수명은 최소 1초다 (수명 0이면 매 조회마다 새 FS가 생겨 `Unable to acquire lock on freshly produced FS instance`. review). Dataset별 무효화 (`invalidateDatasets`). Test용 ticker/clock 주입. 미사용 cache의 `close()` NPE(Phase 2 known WARN)도 수정.
- Source가 `isUsingVendedCredentials=false`면 이 경로는 전혀 동작하지 않는다 (unit: credential 조회 0회).

### 12.3 결과 (C INSTANCE=3, Integration INSTANCE=5 재확인)

| Scenario | 결과 | Status |
|---|---|---|
| `vn`: vended=true, static S3 key **없음**: SELECT(Polaris API로 만든 table), INSERT, CTAS, CTAS table에 INSERT/SELECT, UPDATE, DELETE, OPTIMIZE, CREATE FOLDER/TABLE, DROP TABLE | 모두 COMPLETED, row 정확 (Integration: SELECT/CREATE/INSERT/CTAS/INSERT…SELECT/UPDATE/DELETE, 재시작 후 SELECT) | PASS |
| `vk`: vended=true + static key | SELECT, INSERT, CTAS | PASS |
| `vk` 새 table, 기본 location 밖 `LOCATION` (namespace 안: `ns1/custom/<t>`) (review INSTANCE=6) | CTAS, CREATE TABLE + INSERT, SELECT, 재시작 후 CTAS/SELECT. 새 table은 static key로 쓰고 staged create를 하지 않는다. 존재하게 된 뒤에는 vended credential (`vn`에서 같은 table INSERT/SELECT PASS) | PASS (review 이전 FAIL: AccessDenied) |
| `vk` DROP 후 같은 이름을 다른 location으로 CTAS (vended credential이 cache된 table) (review INSTANCE=6) | SELECT(vended cache) → DROP → `LOCATION 'ns1/custom/t9'` CTAS → SELECT | PASS (DROP/CTAS가 cache를 비운다) |
| `vbp`: vended=true, static key 없음, `fs.s3a.bucket.<bucket>.aws.credentials.provider=SimpleAWSCredentialsProvider`, discovery 기본값(true) (review INSTANCE=6) | SELECT, INSERT, CTAS, 재시작 후 SELECT/INSERT | PASS (bucket별 provider 제거, discovery 끔) |
| `sn`: vended=false, key 없음 (대조군) | `PERMISSION ERROR: Access denied on …metadata.json` | PASS (대조군) |
| Credential 만료 (Polaris credential 수명 900초, 20분간 3분마다 INSERT+SELECT) | 첫 credential 만료 확인 후에도 성공, 투명하게 갱신 | PASS |
| Read-only principal + vended: SELECT | COMPLETED | PASS |
| Read-only principal + vended: INSERT | S3가 거부: `SYSTEM ERROR: AmazonS3Exception: Access Denied` (static 경로는 Polaris PERMISSION ERROR) | PASS (거부됨). 오류 type은 known limitation |
| Read-only principal + vended: CTAS | PERMISSION ERROR `CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION` | PASS |
| 기본 location이 아닌 `LOCATION`의 CREATE TABLE/CTAS (`vn`, static key 없음) | AccessDenied: staged create credential이 기본 location만 덮는다 (review INSTANCE=6 재확인: `ns1/custom/n2` CTAS `SYSTEM ERROR: AmazonS3Exception: Access Denied`) | NOT_SUPPORTED |
| Namespace location 밖 `LOCATION` (`<base>/custom/<t>`) | 모든 source에서 Polaris가 commit을 거부: `Forbidden: Invalid locations … not in the list of allowed locations` (Polaris unstructured table location 기본 off) | Polaris 정책 (Dremio는 permission error로 매핑) |
| Multi-node executor | single node 환경 | ENVIRONMENT_BLOCKED |
| 실제 AWS S3 vending | AWS 계정 없음 | ENVIRONMENT_BLOCKED |

### 12.4 Known limitations

- 기본 table location 밖의 explicit `LOCATION`은 vended credential만으로는 쓸 수 없다 (`vn`). Static key도 있는 source(`vk`)는 아직 없는 table을 static key로 쓰므로 성공한다 (review 반영. 그 static key에 그 location 권한이 있어야 한다).
- Vended credential을 받은 table은 source의 static key와 bucket별 credential 설정 대신 vended credential을 쓰고, bucket discovery를 끈다. 받지 못한 table(vending 없음, 조회 실패, 403)은 source 설정을 쓴다: static key, 없으면 fail closed, provider를 빈 값으로 명시했으면 Dremio host의 `AWS_*` env 또는 EC2 instance profile (§10, [security.md §7.4](security.md#74-phase-4-storage-secret-처리-원칙)).
- CREATE/DROP 후 cache 무효화는 그 문장을 실행한 node에만 적용된다. 다른 node는 cache 만료(최대 credential 수명 - 갱신 margin, 또는 5분/30초) 후 다시 조회한다 (multi-node 미검증).
- Read-only principal의 INSERT가 PERMISSION ERROR 대신 SYSTEM `AmazonS3Exception: Access Denied`로 보인다.
- Vended credential을 켜면 executor도 REST catalog에 접근할 수 있어야 한다 (credential을 node마다 직접 받는다).
- S3 credential만 옮긴다. ADLS/GCS vended credential, `config` 없이 `storage-credentials`만 보내는 catalog는 source의 storage 설정으로 fallback한다.
- Catalog가 vending하는 endpoint, region, path-style은 무시하고 source 값을 쓴다. 그래서 vended source에도 §5의 endpoint 설정이 필요하다.

## 13. Troubleshooting

| 증상 (오류 메시지) | 원인 | 조치 |
|---|---|---|
| `IllegalStateException: Invalid AWSCredentialsProvider provided: …TemporaryAWSCredentialsProvider, …` | Phase 4 이전 build에서 `fs.s3a.aws.credentials.provider` 생략 | `SimpleAWSCredentialsProvider`를 넣거나 Phase 4 이후 build 사용 |
| `Access key ID cannot be blank` | provider는 Simple인데 `fs.s3a.access.key` 없음 | Catalog Credentials에 S3 key 추가 (또는 vended 사용) |
| `SimpleAWSCredentialsProvider: No AWS credentials in the Hadoop configuration` (쓰기), `PERMISSION ERROR: Access denied on …` (읽기) | key도 provider도 없음 (Phase 4 이후 fail closed) | Catalog Credentials에 S3 key 추가, vended 사용, 또는 host identity를 쓰려면 provider를 빈 값/명시 provider로 지정 |
| `Failed to load credentials from IMDS`, `No AWS Credentials provided by InstanceProfileCredentialsProvider` | provider 빈 값(명시) + key 없음 → instance profile | key 추가. EC2 instance profile을 쓰려면 EC2에서 실행 |
| `Credentials for the Storage Provider must be valid…` (STS 403) | S3-compatible인데 `dremio.s3.compat` 없음 → MinIO key로 AWS STS 호출 (access key ID가 AWS로 전송됨) | `dremio.s3.compat=true` |
| `InvalidAccessKeyId` / "The AWS Access Key Id you provided does not exist" (endpoint 없음) | `fs.s3a.endpoint` 없음 (Iceberg `s3.endpoint`만 있음) → 실제 AWS S3로 요청 | `fs.s3a.endpoint=<host>:<port>` |
| `Unable to execute HTTP request: http` / `UnknownHostException: https`, 수 분 hang | Phase 4 이전 build에서 endpoint에 scheme | `host:port` + `ssl.enabled`, 또는 Phase 4 이후 build |
| `SSLException: Unsupported or unrecognized SSL message` | HTTP endpoint인데 SSL 기본값 `true` | `fs.s3a.connection.ssl.enabled=false` |
| `PKIX path building failed` (Dremio), Polaris 422 | self-signed CA 미신뢰 | truststore (§6) |
| `AmazonS3Exception: Bad Request (400)` | TLS endpoint에 `ssl.enabled=false` | `ssl.enabled=true` 또는 생략 |
| `UnknownHostException: <bucket>.<host>` hang, `InvalidBucketName` | hostname endpoint에 path-style 없음 | `fs.s3a.path.style.access=true` |
| `AuthorizationHeaderMalformed`, "region is wrong; expecting '…'" | MinIO region과 signing region 불일치 | `fs.s3a.endpoint.region=<minio-region>` |
| "`<name>` is not a valid AWS region." | MinIO custom region 이름 | MinIO region을 AWS region id로 설정 (NOT_SUPPORTED) |
| `SignatureDoesNotMatch` / S3 403 (source state는 `good`) | 잘못된 S3 secret key. State check는 storage를 검사하지 않는다 | Catalog Credentials의 S3 key 확인 |
| CREATE TABLE 실패 후 재시도 시 "already exists" | Polaris에는 table이 생성되었고 Dremio 쪽 S3 오류로 실패 (A C6) | Polaris에서 table drop 후 재시도 |
| `Failed to get subscoped credentials … Sts 403` (CREATE TABLE), CTAS `SYSTEM ERROR: RESTException` | **Polaris** 쪽 storage credential 오류 | Polaris container/IAM의 storage credential 확인 |
| `PERMISSION ERROR: Access denied on …metadata.json` | Dremio에 S3 credential 없음 (static key도 vended도 없음) | key 추가 또는 vended 켜기 |
| vended source(static key 없음)에서 explicit `LOCATION` CTAS가 AccessDenied | staged create credential은 기본 location만 허용 | `LOCATION` 생략, 또는 그 location 권한이 있는 static key 추가 (§12.4) |
| `Forbidden: Invalid locations '[…]' … not in the list of allowed locations` | `LOCATION`이 namespace location 밖 (Polaris unstructured table location 기본 off) | namespace location 아래로 지정 |
| 잘못된 endpoint에서 query가 약 8분 뒤 실패 | S3A 기본 재시도 | 진단 시 `fs.s3a.attempts.maximum=1`, `fs.s3a.retry.limit=1` |

## 14. 검증 matrix

### 14.1 Agent A — 공식 baseline (INSTANCE=1, user MinIO)

Baseline lifecycle (source `polaris`, namespace `p4base`): source `good`, CREATE TABLE, INSERT 3 rows, CTAS 2 rows, INSERT…SELECT 3 rows, SELECT, `table_snapshot`/`table_history`/`table_files`/`table_manifests`, `AT SNAPSHOT` 모두 PASS. Polaris `loadTable`의 snapshot id가 Dremio와 같고 `config`는 비어 있었다 (vended 없음). S3 object 13개(parquet 3, `metadata.json` 4, manifest 3, manifest list 3). Secret scan 0.

| # | Variant | 결과 | Status (Phase 4 이전 build) | 현재 |
|---|---|---|---|---|
| E1 | 최소 설정 (provider + MinIO endpoint/ssl/compat) | 쓰기/읽기 성공 | PASS | PASS |
| E2 | provider 생략 | `Invalid AWSCredentialsProvider provided` | FAIL | PASS (§10, Integration `noprov`) |
| E3 | provider 빈 값 | 성공 (access key로 결정) | PASS | PASS |
| E4 | `ssl.enabled=false` 없음 (HTTP) | "Unsupported or unrecognized SSL message" | FAIL | FAIL (설정 필요) |
| E5 | path-style 없음, IP endpoint | 성공 | PASS | PASS |
| E6 / E7 | hostname endpoint, path-style 없음 / 있음 | INSERT가 `UnknownHost dremiodev.<host>`로 300초 이상 hang / 성공 | FAIL / PASS | 동일 |
| E8 | compat 없음 | AWS STS 403 | FAIL | FAIL (설정 필요) |
| E9 | endpoint 없음 | AWS S3 403 InvalidAccessKeyId | FAIL | FAIL (설정 필요) |
| E10 | endpoint에 scheme | `http://http://…`, INSERT hang | FAIL | PASS (§11, Integration `scheme`) |
| E11 | discovery 기본값, bucket 제한 user (throwaway MinIO) | 성공 (MinIO가 ListBuckets를 filtering) | PASS | PASS |
| C1 | 잘못된 S3 secret | Source는 `good`, query는 S3 403 SignatureDoesNotMatch. secret 노출 없음 | PASS | PASS |
| C2 | S3 key를 `propertyList`에 | 동작, WARN은 key 이름만, GET에 값이 그대로 | PASS (비권장) | 동일 |
| C3 | key 없음, provider Simple | "Access key ID cannot be blank" | FAIL (예상) | 동일 |
| C4 | key 없음, provider 없음 | E2와 같은 오류 | FAIL | fail closed: 읽기 PERMISSION ERROR, 쓰기 `SimpleAWSCredentialsProvider: No AWS credentials …` (review `snp`). host identity로 가지 않는다 |
| C5 | key 없음, provider 빈 값 | "Failed to load credentials from IMDS" | ENVIRONMENT_BLOCKED (EC2 아님) | 동일 (opt-in 경로, review `sne`) |
| C6 | Dremio 쪽 S3 오류로 CREATE TABLE 실패 | Polaris에는 table이 남는다 | 관찰 | Known limitation |
| — | 실제 AWS S3, requester-pays bucket, `ListAllMyBuckets` 없는 AWS user | 검증 불가 | ENVIRONMENT_BLOCKED | 동일 |

### 14.2 Agent B — S3-compatible / MinIO (INSTANCE=2, user MinIO + throwaway TLS/region MinIO)

| Variant | 설정 | 결과 | Status |
|---|---|---|---|
| (a) endpoint `host:port` | `127.0.0.1:9000`, `localhost:9000` | 4단계(공유 table SELECT, CREATE, INSERT, SELECT) 성공 | PASS |
| (a) endpoint scheme | `http://127.0.0.1:9000`, `https://127.0.0.1:29402` | 이전: "Unable to execute HTTP request: http" / "UnknownHostException: https", 기본 재시도 7분 50초 | FAIL → PASS (P1 = §11) |
| (a) Iceberg key만 | `s3.endpoint`, `s3.path-style-access` | 실제 AWS로 요청, "InvalidAccessKeyId" | FAIL (예상) |
| (b) path-style off, IP | | 두 SDK가 IP endpoint에서 자동 path-style | PASS |
| (b) path-style off, hostname | `localhost:9000` | "The specified bucket is not valid" / 400 | FAIL (예상) |
| (c) SSL 생략, HTTP MinIO | 기본 `true` | `SSLException` | FAIL (예상) |
| (c) TLS MinIO, truststore 없음 | | PKIX, Polaris 422 | FAIL (예상) |
| (c) TLS MinIO + truststore | `ssl=true`/생략, IP/hostname | 성공 | PASS |
| (c) TLS endpoint에 `ssl=false` | | 400 Bad Request | FAIL (예상) |
| (d) region us-east-1 | 없음 또는 `fs.s3a.endpoint.region=us-east-1` | 성공 | PASS |
| (d) 잘못된 region | `dremio.s3.region=ap-northeast-2` | "region is wrong"; INSERT는 "Memory was leaked by query" | FAIL (예상) |
| (d) MinIO ap-northeast-2, `dremio.s3.region`만 | | S3A가 us-east-1로 서명 → AuthorizationHeaderMalformed | FAIL |
| (d) MinIO ap-northeast-2, `fs.s3a.endpoint.region` | 단독 또는 `dremio.s3.region`과 함께 | 성공 | PASS |
| (d) AWS region이 아닌 이름 | `minio-local` | "minio-local is not a valid AWS region." | NOT_SUPPORTED |
| (e) requester-pays 기본(true) | | 성공. header 22건, MinIO 무시 | PASS |
| (e) AWS requester-pays bucket | | AWS 계정 없음 | ENVIRONMENT_BLOCKED |
| (f) compat 생략 | | AWS STS 403 → "Credentials for the Storage Provider must be valid…" | FAIL → **필수** |
| (f) bucket discovery 기본(true) | root user, ListAllMyBuckets deny user | 성공 (MinIO filtering) | PASS (불필요) |
| (f) ListAllMyBuckets 없는 AWS IAM user | | AWS 계정 없음 | ENVIRONMENT_BLOCKED |
| provider 생략 | | `Invalid AWSCredentialsProvider provided` | FAIL → PASS (P2 = §10) |

공통 관찰: source state는 storage 설정과 관계없이 항상 `good`이다. CREATE TABLE은 storage 설정이 틀려도 성공한다 (Polaris가 첫 `metadata.json`을 쓴다). 오류는 첫 INSERT/SELECT에서 나온다.

### 14.3 Agent C — Vended credentials (INSTANCE=3)

§12.3. Module regression 366 tests 0 failures (C 시점), `TestRestCatalogVendedCredentials` 22 tests (review 반영 후 33).

### 14.4 Integration 재검증 (INSTANCE=5, Phase 4 최종 tarball)

환경: `scripts/dev build plugins/s3 plugins/icebergcatalog distribution/resources distribution/server` 후 tarball을 새로 풀어 실행 (plugin jar 2개의 class와 `conf/logback.xml`의 새 logger block 확인). Polaris 1.1.0 `p4-polaris-e2e-5`, user MinIO prefix `polaris-p4-int`.

| # | Source / 항목 | 설정 | 결과 | Status |
|---|---|---|---|---|
| 1 | `base` (**공식 baseline + MinIO 최소 설정**, `oauth2-server-uri` 없음) | provider Simple, endpoint `127.0.0.1:9000`, ssl false, path-style, compat; secret `credential`/S3 key 2개 | state `good`. CREATE TABLE, INSERT 3, SELECT, CTAS 2, INSERT…SELECT 2, UPDATE 1, DELETE 1, count/min/max, `table_snapshot` 4, `table_history` 4, `table_files` 2, `AT SNAPSHOT`(첫 snapshot → 3 rows) | **PASS** |
| 2 | Polaris ↔ Dremio 일치 | Polaris `loadTable` | current snapshot id가 Dremio 최신 snapshot과 같다. snapshot 4개, `config` 비어 있음 | PASS |
| 3 | `noprov` | 1에서 provider key 제거 | SELECT(공유 table), CREATE, INSERT, CTAS, SELECT | PASS (§10) |
| 4 | `scheme` | endpoint `http://127.0.0.1:9000` + ssl false | 같은 5단계 | PASS (§11) |
| 5 | `scheme2` | endpoint `http://127.0.0.1:9000`, ssl key 없음(기본 true) | 같은 5단계 (scheme이 SSL 기본값보다 우선) | PASS (§11) |
| 6 | `rec` | Phase 2/harness 기본 recipe (discovery off, `dremio.s3.region`, `fs.s3a.endpoint.region`, requester-pays false, `oauth2-server-uri`) | 같은 5단계 | PASS |
| 7 | `vn` | vended=true, static S3 key 없음 | SELECT, CREATE, INSERT, CTAS, INSERT…SELECT, UPDATE, DELETE, SELECT | PASS (§12) |
| 8 | `sn` | vended=false, key 없음 (대조군) | `SELECT *` → PERMISSION ERROR Access denied on `metadata.json`. `count(*)`는 snapshot summary로 성공 (S3 미접근) | PASS (대조군) |
| 9 | Dremio 재시작 (`--reuse`) | | `base`/`noprov`/`scheme`/`vn` state `good`, SELECT 4건과 `base` INSERT 성공 | PASS |
| 10 | Secret scan | `scan-secrets.sh`(MinIO secret, Polaris root secret, `Bearer`, `access_token`, `s3.secret-access-key`, `s3.session-token`, `client_secret`; Dremio log 전체(.gz 포함 대상, 이번 실행은 회전 없음) + Polaris container log), 추가 marker `X-Amz-Security-Token`(대소문자), `eyJ`, `fs.s3a.session.token`, v3 GET 7개 source의 S3 secret | 모두 0. `Invalid AWSCredentialsProvider` log 0 | PASS |
| 11 | 정리 | source 7개 삭제, Dremio `--purge`, `p4-polaris-e2e-5` 삭제, MinIO object 86개 삭제(남은 0), scratch tarball/env 삭제 | | PASS |

### 14.5 Review 반영 재검증 (INSTANCE=6)

환경: Phase 4 tarball(19:46, `plugins/s3` class가 작업 트리 build와 같음)을 새로 풀고 review 반영 `dremio-icebergcatalog-plugin` jar와 `conf/logback.xml`(Netty logger 포함)로 교체. Polaris 1.1.0 `p4-polaris-e2e-6`, user MinIO prefix `polaris-p4-fix6`. Dremio process env에 `AWS_*`/`S3_*`/`MINIO_*` 0개 (`AWS_ACCESS_KEY_ID`를 넘겨 시작해도 `dremio-up.sh`가 제거).

| Source | 설정 | 결과 | Status |
|---|---|---|---|
| `base` | 공식 baseline + MinIO | CREATE, INSERT, SELECT, CTAS, Polaris table SELECT, 재시작 후 SELECT/INSERT | PASS |
| `noprov` | provider 생략 (key 있음) | CTAS, SELECT, 재시작 후 SELECT/CTAS | PASS |
| `vk` | vended + static key | vended SELECT/INSERT, 기본 location CTAS, `LOCATION 'ns1/custom/k2'` CTAS + SELECT/INSERT, `LOCATION 'ns1/custom/k3'` CREATE + INSERT/SELECT, vended cache된 `t9` DROP 후 `ns1/custom/t9` CTAS + SELECT, 재시작 후 `ns1/custom/k4` CTAS | PASS (Polaris metadata `ns1/custom/k2/metadata/…` 확인) |
| `vn` | vended, key 없음 | vended SELECT/INSERT, 기본 location CTAS(staged) + SELECT/UPDATE/DELETE, `vk`가 만든 custom location table INSERT/SELECT, 재시작 후 SELECT/CTAS/INSERT | PASS |
| `vn` | `LOCATION 'ns1/custom/n2'` CTAS | `SYSTEM ERROR: AmazonS3Exception: Access Denied` | NOT_SUPPORTED (§12.4) |
| `vbp` | vended, key 없음, bucket별 provider Simple, discovery 기본값 | SELECT, INSERT, CTAS, 재시작 후 SELECT/INSERT | PASS |
| `snp` | vended=false, key·provider 없음 | 1차(chain 유지 build): CTAS `Unable to load AWS credentials from environment variables` (S3A가 chain의 env provider 시도). 최종: 읽기 PERMISSION ERROR, CTAS `NoAwsCredentialsException: SimpleAWSCredentialsProvider: No AWS credentials in the Hadoop configuration` | PASS (fail closed) |
| `sne` | key 없음, provider 빈 값 명시 (opt-in) | `NoAuthWithAWSException: No AWS Credentials provided by InstanceProfileCredentialsProvider` (IMDS 연결 실패) | ENVIRONMENT_BLOCKED (EC2 아님) |
| (공통) | `<base>/custom/<t>` (namespace 밖) `LOCATION` | Polaris `Invalid locations … not in the list of allowed locations` | Polaris 정책 |
| Secret scan | `scan-secrets.sh` + `X-Amz-Security-Token`(대소문자), `eyJ`, `fs.s3a.session.token`, `fs.s3a.secret.key=` | 모두 0 | PASS |
| 정리 | source 7개 삭제, Dremio `--purge`, `p4-polaris-e2e-6` 삭제, MinIO object 94개 삭제(남은 0), scratch tarball 삭제 | | PASS |

## 15. Phase 4 코드 변경

| File | 변경 | 출처 |
|---|---|---|
| `plugins/icebergcatalog/.../store/VendedStorageCredentials.java` (신규) | vended `s3.*` → `fs.s3a.*` 매핑, provider 선택, 만료, 값 없는 `toString()` | C |
| `plugins/icebergcatalog/.../store/VendedCredentialsCache.java` (신규) | table별 credential cache, 갱신/실패/없음 수명, redact된 WARN | C |
| `.../store/CatalogAccessor.java`, `AbstractRestCatalogAccessor.java` | `loadTableStorageProperties(table, stageNewTable)` (loadTable, 없으면 요청 시에만 staged create, 403/401 매핑) | C, review |
| `.../store/RestIcebergCatalogPlugin.java` | vended이면 per-dataset FS cache (`createFSCache`, `getFsConfForDataset`, `tableIdentifierOf`), `close()`에서 credential 비움, start INFO 문구. **Hadoop 기본 provider chain을 `SimpleAWSCredentialsProvider`로 교체** (`replaceHadoopDefaultCredentialsProvider`). Review: 자기 S3 credential이 있는 source는 staged create 안 함 (`hasOwnS3Credentials`), source property를 vended로 오인하지 않음 (`withoutSourceProperties`), CREATE/CTAS/DROP 시 table credential·FS 무효화 (`forgetTableStorage`), SigV4 REST key 3개를 Hadoop conf에서 제외 | C, Integration (A P-A1 + B P2), review |
| `.../store/VendedStorageCredentials.java`, `VendedCredentialsCache.java` (review) | bucket별 credential 설정 제거, bucket discovery 끔, wall clock 기준 재조회, table별 `invalidate` | review |
| `.../dfs/DatasetFileSystemCache.java`, `LockableHadoopFileSystem.java` | per-dataset flag, entry별 만료 marker, 미사용 cache `close()` NPE 수정. Review: 만료 entry 최소 수명 1초, `invalidateDatasets`, test용 ticker/clock 주입 | C, review |
| `plugins/icebergcatalog/src/main/resources/restcatalog-layout.json` | `isUsingVendedCredentials` tooltip | C |
| `plugins/s3/.../S3FileSystem.java` | `getEndpoint()`: scheme이 있으면 유지 | Integration (B P1) |
| `distribution/resources/.../conf/logback.xml` | `org.apache.http.wire`, `org.apache.http.headers`, `com.amazonaws.auth.AWS4Signer`, `software.amazon.awssdk.auth.signer`, `software.amazon.awssdk.http.auth.aws.internal.signer`, (review) `io.netty.handler.logging.LoggingHandler`, `io.netty.handler.codec.http2.Http2FrameLogger`를 INFO로 고정 | Integration (D patch), review |
| `scripts/polaris-e2e/source.sh` | 기본 source body에 `fs.s3a.endpoint.region=$S3_REGION` 추가 (`dremio.s3.region`만으로는 non-default region MinIO에서 S3A가 실패) | Integration (B P5 일부) |
| `scripts/polaris-e2e/dremio-up.sh` (review) | Dremio process에서 `AWS_ACCESS_KEY_ID`/`AWS_ACCESS_KEY`/`AWS_SECRET_ACCESS_KEY`/`AWS_SECRET_KEY`/`AWS_SESSION_TOKEN`/`AWS_PROFILE`/`S3_ACCESS_KEY`도 제거 (credential 없음 대조군이 env key를 쓰지 않도록) | review |
| `.localci.yaml` (review) | `plugins/s3/src`를 archive에 포함, `s3-test` step 추가(fast, full), lint/static에 `plugins/s3` 추가 | review |
| Test | `TestRestCatalogStorageConfig`(A, 13), `TestRestCatalogS3CompatibleProps`(B, 9, reflection 제거: forbiddenapis `setAccessible`), `TestRestCatalogVendedCredentials`(C, 33. FS 만료 test는 fake ticker/clock), `TestRestCatalogSecretExposure`(D, 7. SDK v2 async(Netty) 읽기와 `io.netty` DEBUG 포함), `TestS3FileSystem` +2 | A–D, Integration, review |

## 16. 남은 항목 (Phase 5/6)

Phase 5 결과(같은 MinIO recipe로 lifecycle/read/write/cache/장애 E2E, storage down과 잘못된 S3 secret 포함)는 [test-results.md §5](test-results.md#5-phase-5--실제-e2e--regression)에 있다. 아래 항목 중 multi-node와 P-A3은 Phase 5에서도 실행/구현하지 않았다. Storage down 시 기본 S3A 재시도로 SELECT가 오래 붙잡히는 문제(known-limitations K-15)가 Phase 5에서 추가로 확인되었다.

- AWS S3 실측 전부 (static baseline, requester-pays, `ListAllMyBuckets` 없는 IAM user, instance profile, assumed role, AWS STS vending): ENVIRONMENT_BLOCKED. AWS 계정이 생기면 §14.1 E1, §14.2 (e)/(f), §12.3을 다시 실행한다.
- Multi-node executor에서의 static/vended 경로 (Phase 5).
- B 제안 P3 (compat mode에서 AWS 외 region 이름 허용), P4 (amazonaws.com이 아닌 endpoint에 compat이 꺼져 있으면 WARN: 지금은 MinIO access key ID가 AWS STS로 간다), P5 나머지 (harness truststore mount option): 미적용.
- A 제안 P-A3: commit 시 403 외 `RESTException`(예: Polaris storage credential 오류)을 redact된 server message가 있는 `UserException`으로 매핑 (Phase 5).
- Known limitations (Phase 6 `known-limitations.md`): source state check가 storage를 검사하지 않음 (A C1, B), Dremio 쪽 S3 오류로 실패한 CREATE TABLE이 Polaris에 남음 (A C6), vended credential limitation (§12.4), 잘못된 region INSERT의 "Memory was leaked" 표시, `DROP TABLE`이 S3 object를 남김.
- Polaris FILE storage를 Dremio에서 읽는 경로, Azure/GCS: 미검증 (범위 밖).
- Phase 6: 위 목록은 그대로 남는다 (AWS/multi-node는 ENVIRONMENT_BLOCKED, P-A3은 C-06 known). Phase 6 release tarball 최종 smoke(INSTANCE=3, 같은 MinIO recipe)는 PASS ([test-results.md §7](test-results.md#7-phase-6--최종-통합--release-readiness)).
