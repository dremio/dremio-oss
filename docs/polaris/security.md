# Polaris RESTCATALOG 보안 (secret, 로그, 오류 메시지)

- 이 문서는 RESTCATALOG source가 다루는 secret이 어디로 흘러가고 어디에 남지 않아야 하는지 정리한다.
- Phase 3에서 처음 작성했고 Phase 4(Object Storage, vended credentials)에서 storage credential 항목을 보강했다 (§1, §2, §3의 Phase 4 행과 §7). Storage 설정 자체는 [storage.md](storage.md).
- Phase 6에서 동작 중 오류 매핑(C-08)과 log redaction을 넓혀 §2, §3, §6을 갱신했다.
- 기준 코드: `feature/polaris-restcatalog` Phase 3 `3b85d3619` + Phase 4 `691f2b5a9` + Phase 5 `6d2f6f4e9` + Phase 6 작업 트리. Iceberg `1.7.0-5f7c992-20250730084652-3bf8b99` (Dremio fork). `HTTPClient`는 Apache HttpClient `5.3.1`을 relocation 없이 쓴다.
- 관련 문서: [phase3-oauth-catalog.md](phase3-oauth-catalog.md) (Phase 3 시나리오 결과), [progress.md](progress.md), [compatibility-matrix.md](compatibility-matrix.md).
- 이 문서의 모든 secret은 placeholder(`<client_id>:<client_secret>`, `<s3AccessKey>`, `<s3SecretKey>`)다. Polaris bootstrap root secret `s3cr3t`는 throwaway container용 로컬 테스트 값이다.

## 1. 보호 대상

로그, API 응답, 오류 메시지에 절대 나오면 안 되는 값 (polaris-catalog-support.md Phase 3 Agent D):

| 값 | 설정 위치 | 비고 |
|---|---|---|
| OAuth2 `client_secret` / `credential` (`<client_id>:<client_secret>`) | `secretPropertyList` (Catalog Credentials) | Iceberg `RESTCatalog`의 OAuth2 client_credentials grant로만 전송된다 |
| OAuth2 access token (`token`, 발급받은 Bearer token) | `secretPropertyList` 또는 runtime | Bearer token은 Iceberg client 내부에만 있다 |
| S3 secret key / access key | `secretPropertyList` (`fs.s3a.secret.key`, `fs.s3a.access.key`) | Hadoop conf로 S3 FileSystem에 전달된다 |
| S3 session token | `fs.s3a.session.token`, vended `s3.session-token` | Phase 4: vended credential을 runtime에서 쓴다 (G-04, [storage.md §12](storage.md#12-vended-credentials-compatibility-결과)) |
| Vended S3 credential (`s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`) (Phase 4) | 실행 중 catalog `loadTable`/staged create 응답의 `config` (Polaris 1.1은 `storage-credentials`에도 넣는다) | source config에 없다. node마다 memory(`VendedCredentialsCache`)와 table별 S3 FileSystem의 Hadoop conf에만 있다. MinIO STS가 발급한 값은 access key 20자, secret 40자, session token 약 1,200자 JWT(`eyJ…`) |
| `dremio-admin encrypt` 결과 (`secret:1.…`) (Phase 4) | `secretPropertyList` 값 | 평문은 아니지만 같은 data dir의 local keystore(`data/security`)로 복호화된다 (§7.3) |

## 2. 경로별 처리

| 경로 | 처리 | 근거 |
|---|---|---|
| REST API GET (`/api/v3/catalog/{id}`, `/api/v3/source/{id}`, `/apiv2/source/{name}`) | secret은 `$DREMIO_EXISTING_VALUE$`로 masking. masked 값 그대로 PUT하면 기존 값 유지 | Phase 2 live gate #4/#5, Phase 3 D live L3/L4 |
| Source state 메시지 (`getState`) | `redactSecrets`: source config에 있는 secret 값과 그 `:` 분할 부분을 `****`로 바꾼다. 긴 값부터 치환하고, 4자 미만 값은 건너뛰며, 8자 미만 값은 앞뒤가 영숫자가 아닐 때만 치환한다 | unit (`TestRestCatalogHttpErrors`, `TestRestCatalogOAuth2`, `TestRestIcebergCatalogPluginConfig`) |
| Source state 메시지의 server 응답 부분 | redact → HTML tag 제거, 공백 정리, 300자 제한 → 다시 redact (Phase 3 integration). 자른 뒤에 secret 일부가 남지 않도록 자르기 전에 먼저 redact한다 | unit `testHtmlErrorPageIsStrippedFromTheMessage`, `testLongServerMessagesAreAbbreviated` |
| 실패 기록 cache (`RECENT_FAILURES`) | redact된 `SourceState`만 저장한다. 2분 후 만료, 성공하면 삭제 | Agent A |
| Endpoint 표시 | user info, query, fragment를 뺀 URI만 메시지에 넣는다 (`sanitizeEndpoint`) | unit |
| Hadoop conf | REST client 전용 key(`credential`, `token`, `scope`, `oauth2-server-uri`, `audience`, `resource`, `token-exchange-enabled`/`token-refresh-enabled`/`token-expires-in-ms`, `header.*`, `rest.auth.*`, `rest.client.*`, AWS SigV4 REST signing의 `rest.access-key-id`/`rest.secret-access-key`/`rest.session-token`, token-exchange type key)는 Hadoop conf로 복사하지 않는다. `rest.client.*`는 Phase 3 integration에서(secret은 아니지만 HTTP client 전용), SigV4 key 3개는 Phase 4 review에서 추가했다. 그 밖의 `rest.*` key(secret 아님)는 복사된다 | unit |
| `propertyList`(plain)에 secret key | UI validator가 저장을 막고 backend는 key 이름만 WARN한다 (G-13) | Phase 2 |
| Config 객체 | `RestIcebergCatalogPluginConfig.toString()`은 `Object.toString()`이다 (값 없음). `clearSecrets()` 후 JSON은 secret 4개 모두 `$DREMIO_EXISTING_VALUE$` | D unit #21/#22 |
| Vended credential 보관 (Phase 4) | `VendedCredentialsCache`(node별, 만료 5분 전 또는 남은 수명의 절반 전에 갱신)와 table별 FileSystem conf에만 둔다. Query plan, fragment, KV store, profile로 가지 않는다. Source `close()`에서 cache를 비우고, 이 node에서 실행한 CREATE/CTAS/DROP TABLE은 그 table의 credential과 FileSystem을 비운다 (review) | C 설계. live: KV/profile/log에서 vended 값 0건 (§7.2) |
| Vended credential 판별 (Phase 4 review) | Iceberg REST client가 source property를 table FileIO property에 합치므로, source에 설정된 값과 같은 key/값은 vended credential로 보지 않는다 (source의 `s3.*` key가 source의 `fs.s3a.*` 설정을 덮지 않는다). Vended credential을 적용할 때 bucket별 credential 설정(`fs.s3a.bucket.<b>.access.key`/`.secret.key`/`.session.token`/`.aws.credentials.provider`)을 지운다 (source key와 vended session token이 섞여 서명되지 않도록) | unit `testSourceS3PropertiesAreNotTakenForVendedCredentials`, `testVendedCredentialsReplacePerBucketCredentials`, live `vbp` |
| Vended credential 문자열화 (Phase 4) | `VendedStorageCredentials.toString()`과 `Result.toString()`은 종류와 만료 시각만 보여 준다 (`temporary S3 credentials expiring at …`). DEBUG log도 이 문자열만 찍는다 | C unit, D live DEBUG |
| Vended credential 조회 실패 (Phase 4) | WARN 한 줄. catalog 메시지는 `redactSecrets` + `redactedServerMessage`로 줄인다 | C `VendedCredentialsCache.describe` |
| Vended key 이름 (Phase 4) | `isSensitivePropertyKey`는 `s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`, `fs.s3a.session.token`, `gcs.oauth2.token`, `adls.sas-token.*`, `credential`, `token`, `header.Authorization`을 secret으로 본다 (대소문자 무시). `s3.session-token-expires-at-ms`, `expiration-time`, `client.refresh-credentials-endpoint`, `client.region`, `s3.endpoint`, `s3.path-style-access`, `gcs.oauth2.token-expires-at`은 secret이 아니다 | D unit `testVendedCredentialKeysAreSensitive` |
| S3 요청 (Phase 4) | SigV4는 secret key를 보내지 않는다. Session token은 `X-Amz-Security-Token` header로만 간다. S3 오류(`AccessDeniedException`/`AmazonS3Exception`, MinIO `Forbidden`/`SignatureDoesNotMatch`)에 key 값이 없다 | D unit (fake S3가 받은 raw request), live (잘못된 S3 secret source) |
| 동작 중 오류 (`RestCatalogExceptionMapper`) | Review 반영: plugin이 `redactSecrets`를 accessor와 mapper에 넘긴다. 401/403 permission error의 server message는 redact → 300자 제한 → redact. cause로 남기는 Iceberg 예외도 message에 secret이 있으면 redact된 같은 type의 예외(원래 stack trace)로 바꾼다. job profile과 server.log에 찍히는 cause chain에도 secret이 남지 않는다. Phase 6: table lookup/load 경로와 DDL 경로(create/drop table, CTAS staging, create/drop/replace view, create/drop folder)의 429/5xx/timeout/network/TLS(`requestFailed`, CONNECTION ERROR)와 400/422(VALIDATION ERROR, drop table은 UNSUPPORTED_OPERATION ERROR)도 같은 방식으로 redact한다 (C-08). HTML만 있는 message는 예외 type 이름으로 바꾼다 | unit `testTableExists403/401IsRaisedWithoutSecrets`(server가 secret echo), `testServerMessageIsRedactedInMessageAndCause`, Phase 6 `testRequestFailedRedactsServerTextAndCause`, `TestRestCatalogHttpErrors`(fake HTTP catalog의 500/429/503/502 HTML/timeout/연결 끊김) |
| 오류 log의 stack trace (Phase 6) | namespace/table/view listing 실패는 WARN 한 줄(redact된 server message)이고 stack trace는 DEBUG에만 남긴다. DEBUG로 넘기는 예외는 cause chain의 어느 message에 secret 값이 있으면 redact된 message와 원래 stack trace만 가진 예외로 바꾼다 (`RestCatalogExceptionMapper.redactedForLogging`) | unit `testRedactedForLoggingDropsSecretsFromTheCauseChain` |

## 3. Log 설정

- **HttpClient wire/headers logger.** `org.apache.hc.client5.http.wire`와 `org.apache.hc.client5.http.headers`를 DEBUG로 두면 `Authorization: Bearer <token>`과 token 요청 body의 `client_secret`이 그대로 찍힌다 (D의 임시 probe로 확인, 제출하지 않음).
  - `distribution/resources/src/main/resources/conf/logback.xml`이 두 logger를 INFO로 고정한다 (commit `ffd1cc1b2`). root level을 올려도 이 두 logger는 DEBUG가 되지 않는다.
  - Phase 3 integration에서 `distribution/resources`와 `distribution/server`를 다시 build해 tarball의 `conf/logback.xml`에 이 block이 들어간 것을 확인했다.
  - 주의: `distribution/server/target/dremio-community-*/dremio-community-*/` 압축 해제 디렉터리는 rebuild 후에도 `conf/logback.xml`이 갱신되지 않았다. Live 검증은 tarball(`*.tar.gz`)을 새로 풀어서 했다.
- **HttpClient 4 headers/wire logger와 SigV4 signer (Phase 4).** Hadoop S3A(AWS SDK v1)는 Apache HttpClient 4를 쓴다. `org.apache.http.headers`를 DEBUG로 두면 S3 요청의 `X-Amz-Security-Token: <session token>`(static `fs.s3a.session.token`과 vended `s3.session-token` 모두)이 그대로 찍힌다 (D unit probe로 확인, probe는 제출하지 않음). Secret key 자체는 전송되지 않으므로 찍히지 않는다.
  - Phase 4 integration에서 `conf/logback.xml`에 `org.apache.http.wire`, `org.apache.http.headers`, `com.amazonaws.auth.AWS4Signer`, `software.amazon.awssdk.auth.signer`, `software.amazon.awssdk.http.auth.aws.internal.signer`를 INFO로 고정했다. headers 외 4개는 예방 조치다 (wire는 header와 body를, signer는 canonical request에 session token을 넣는다. live 확인은 하지 않음).
  - 새 tarball의 `conf/logback.xml`에 block이 들어간 것을 확인했다 (Integration INSTANCE=5). `TestRestCatalogSecretExposure.LogCapture`도 같은 logger 목록을 INFO로 고정한다.
  - `com.amazonaws.request` DEBUG(6 event)와, `org.apache.hadoop.fs.s3a` + `com.amazonaws`(S3A + AWS SDK v1) DEBUG를 합친 102 event에서 secret key와 session token이 나오지 않았다 (D unit). S3A DEBUG는 provider 이름만 찍는다.
- **Netty logger (AWS SDK v2 async 읽기, Phase 4 review).** Dremio는 data file을 SDK v2 async client(`S3AsyncByteReader`, `NettyNioAsyncHttpClient`)로 읽는다. netty-nio-client 2.30.27의 `ChannelPipelineInitializer`는 모든 channel에 `LoggingHandler(LogLevel.DEBUG)`(HTTP/1.1)와 `Http2FrameLogger(LogLevel.DEBUG)`(HTTP/2)를 항상 넣는다 (bytecode 확인). `io.netty.handler.logging.LoggingHandler`가 DEBUG면 요청을 header째로 찍는다.
  - Unit 확인: `TestRestCatalogSecretExposure`에서 이 logger 고정을 빼고 `io.netty`를 DEBUG로 두면 async 읽기 한 번에 S3 session token(`x-amz-security-token`)이 log event에 나왔다 (test 전용 dummy 값, 일회성 확인 후 원복).
  - Review 반영으로 `conf/logback.xml`과 `LogCapture`에 `io.netty.handler.logging.LoggingHandler`와 `io.netty.handler.codec.http2.Http2FrameLogger`를 INFO로 고정했다. 같은 test가 `io.netty` DEBUG 46 event, SDK v2 DEBUG 12 event에서 secret 0건을 확인한다 (positive control: fake S3가 `md/http#NettyNio` user agent 요청을 받는다).
- **관찰 (후속 확인 필요, Phase 4 D).** logback hot reload(`scan="true"`) 때 `server.out`에 `no applicable action for [turboFilter]`가 찍혔다. reload 후 `BlockLogLevelTurboFilter`가 빠진다면 root level만 올려도 third-party logger가 DEBUG가 될 수 있다. Logger 단위 고정은 **위에 나열한 logger만** 막는다 (HttpClient 4/5 wire·headers, SigV4 signer, Netty `LoggingHandler`/`Http2FrameLogger`). 다른 third-party logger가 request header를 찍는 경로가 새로 생기면 따로 고정해야 한다.
- **`BlockLogLevelTurboFilter`.** 기본 설정은 `com.dremio` 외의 DEBUG를 막는다. D는 live 검증에서 `org.apache.iceberg,DEBUG`와 `org.apache.hc.client5,DEBUG`를 추가해 1,458줄의 DEBUG log를 만들었고, 이때도 secret 노출은 0건이었다 (wire/headers는 INFO 유지).
- **`Property.toString()`.** `sabot/kernel`의 `com.dremio.exec.catalog.conf.Property`는 `toString()`에 value를 넣는다. `secretPropertyList`를 그대로 log에 넘기면 secret이 찍힌다. 현재 plugin code는 key 이름만 log에 남긴다. 새 log 문을 쓸 때 주의한다.
- **namespace listing 실패 로그.** Phase 3 integration에서 `NoSuchNamespaceException`/`ForbiddenException`은 stack trace 없는 WARN 한 줄로 바꿨다 (`Skipping namespace … : <server message>`, `Namespace … does not exist …`). Review 반영으로 `Skipping namespace`의 server message도 redact하고 300자로 줄인다 (unit `testListNamespaces403IsReportedWithoutSecrets`가 secret을 echo하는 403으로 확인). Phase 6부터 그 밖의 listing 예외도 ERROR + stack trace 대신 WARN 한 줄(`Error listing namespace …: <redact된 message>`)이고, stack trace는 DEBUG에서 redact된 예외로만 찍는다.
- **E2E harness.** `common.sh`의 `redact()`는 `S3_SECRET_KEY`, `AWS_SECRET_ACCESS_KEY`, `MINIO_SECRET_KEY`, `POLARIS_ROOT_SECRET`, `E2E_SRC_CREDENTIAL`, `SOURCE_CREDENTIAL`, `E2E_PRINCIPAL_SECRET`, `E2E_EXTRA_SECRET`와 `$WORK/principal-*.cred`의 secret을 가린다. `id:secret` 값은 secret 부분도 따로 가린다. `scan-secrets.sh`는 rotate된 `log/archive/*.gz`도 풀어서 센다 (Review 반영).

## 4. Secret 노출 검증 결과

### 4.1 Unit

`TestRestCatalogHttpErrors`(D)와 `TestRestCatalogOAuth2`(A)는 fake REST catalog server가 error message 안에 credential과 S3 key를 일부러 되돌려 보내게(echo) 해서 redaction을 실제로 확인한다.

- 검사 위치: state message, `SourceState.toString()`, 예외 chain의 message/`toString()`, DEBUG로 수집한 log event (`com.dremio.plugins.icebergcatalog`, `org.apache.iceberg`, `org.apache.hc.client5`. wire/headers는 INFO).
- 검사 값: client secret, S3 access key, S3 secret key, S3 session token, fake server가 발급한 access token, `Bearer`, `access_token`, `client_secret` marker.
- 결과: 모든 시나리오에서 0건. echo된 값은 `****`로 바뀌었다.
- A의 첫 scan에서 `client_secret` 46건이 나왔는데, 모두 hint 문구의 placeholder `<client_id>:<client_secret>`였다. 문구를 "client ID and client secret separated by ':'"로 바꾼 뒤 0건이 되었다.

### 4.2 Live

| 실행 | 대상 | 검사 값 / marker | 결과 |
|---|---|---|---|
| Harness smoke (INSTANCE=0) | Dremio `log/` 전체, Polaris container log | MinIO secret, Polaris root secret, `Bearer`, `access_token`, `s3.secret-access-key`, `s3.session-token`, `client_secret` | 0 |
| A (INSTANCE=1, DEBUG 포함) | 동일 | 위 항목 + 잘못 넣은 secret | 0 |
| B (INSTANCE=2) | 동일 | 위 항목 | 0 |
| C (INSTANCE=3) | 동일 | 위 항목 | 0 |
| D (INSTANCE=4, iceberg/hc DEBUG) | `server.log`, `server.out`, `json/server.json`, `queries.json`, `metadata_refresh.log`, `access.log`, Polaris `docker logs` | MinIO secret, Polaris root secret, read-only principal secret, 잘못 넣은 credential, `Bearer`/`Authorization`, `access_token`/`client_secret`, JWT `eyJ`, `s3.secret-access-key`/`s3.session-token`, `fs.s3a.secret.key=`/`fs.s3a.access.key=`/`credential=` | 0 |
| Integration 재검증 (INSTANCE=5, Phase 3 최종 tarball) | Dremio `log/` 전체, Polaris container log | MinIO secret, Polaris root secret, 잘못 넣은 secret, `reader`/`nolist` principal secret, `Bearer`, `access_token`, `s3.secret-access-key`, `s3.session-token`, `client_secret`, JWT `eyJ` | 0 ([phase3-oauth-catalog.md §5](phase3-oauth-catalog.md#5-integration-재검증-instance5) #19) |
| Review 반영 재검증 (INSTANCE=6) | Dremio `log/` 전체(.gz 포함), Polaris container log | MinIO secret, Polaris root secret, `Bearer`, `access_token`, `s3.secret-access-key`, `s3.session-token`, `client_secret` | 0 ([phase3-oauth-catalog.md §9](phase3-oauth-catalog.md#9-review-반영-instance6)) |
| Phase 4 D (INSTANCE=4, static + vended, DEBUG, 회전 `.gz`) | Dremio `log/` 전체, REST/job/profile/system table, KV, data dir, Polaris log, harness 출력 | 위 항목 + vended access key/secret/session token 값, `X-Amz-Security-Token`, `fs.s3a.session.token` | 0 (KV 평문 G-12 제외, §7.2) |
| Phase 4 Integration (INSTANCE=5, Phase 4 최종 tarball) | Dremio `log/` 전체(.gz 대상), Polaris container log, v3 GET | MinIO secret, Polaris root secret, `Bearer`, `access_token`, `s3.secret-access-key`, `s3.session-token`, `client_secret`, `X-Amz-Security-Token`, `eyJ`, `fs.s3a.session.token` | 0 ([storage.md §14.4](storage.md#144-integration-재검증-instance5-phase-4-최종-tarball)) |
| Phase 4 review 반영 (INSTANCE=6, review jar + `logback.xml`) | Dremio `log/` 전체, Polaris container log | 위 항목 + `fs.s3a.secret.key=` | 0 ([storage.md §14.5](storage.md#145-review-반영-재검증-instance6)) |

## 5. 재시도와 timeout (Iceberg REST client)

Dremio fork 1.7 `org.apache.iceberg.rest.HTTPClient`와 `ExponentialHttpRequestRetryStrategy`의 동작 (D가 bytecode와 unit test로 확인):

| 항목 | 동작 |
|---|---|
| 재시도 횟수 | `rest.client.max-retries`. 기본값 5 |
| 재시도하는 status | 429, 502, 503, 504. Method와 관계없이 재시도한다. Token `POST`도 포함된다 |
| 재시도하지 않는 I/O 예외 | `InterruptedIOException`(read/connect timeout 포함), `UnknownHostException`, `ConnectException`, `ConnectionClosedException`, `NoRouteToHostException`, `SSLException`. 그 밖의 I/O 예외는 idempotent method만 재시도한다 |
| 대기 시간 | `Retry-After`가 있으면 그 값(초 또는 HTTP-date)을 상한 없이 쓴다. 없으면 `1000ms × 2^(n-1)`(최대 64배)에 최대 10% jitter. 기본 5회면 약 31초 |
| timeout | `rest.client.connection-timeout-ms`, `rest.client.socket-timeout-ms`. 설정하지 않으면 HttpClient 기본값 3분 |

영향과 권장:
- 응답하지 않는 catalog는 요청 하나를 최대 3분 붙잡는다. Dremio의 source state check는 10초에서 끊기므로 state는 일반 메시지("Source is not currently available")가 된다. timeout을 설정하면 state에 "did not respond in time" hint가 나온다 (A live).
- 운영 권장값 (기본값으로 넣지는 않았다. 기존 generic RESTCATALOG 동작을 바꾸기 때문이다): `rest.client.connection-timeout-ms=10000`, `rest.client.socket-timeout-ms=60000`. 큰 `Retry-After`를 주는 server라면 `rest.client.max-retries`를 낮춘다.
- TLS handshake 실패는 요청을 보내기 전에 일어나므로 credential은 전송되지 않는다 (D unit).

## 6. 남은 위험과 다음 Phase

| 항목 | 내용 | 다음 단계 |
|---|---|---|
| G-12 at-rest 평문 | `secretPropertyList`는 KV store에 평문이다 (`encryptSecrets`는 `SecretRef` 전용). `RESTCatalog.properties()`에도 남는다. Phase 4 live 재확인: client secret과 S3 secret key가 RocksDB(`data/db`)에 평문. 실패한 생성/update 요청의 값은 저장되지 않는다 | 완화책 `dremio-admin encrypt` (§7.3, live PASS). kernel 범위 open item. Phase 6 known-limitations |
| 동작 중 오류의 redaction | Phase 6: table lookup/load, view load, storage credential 조회와 DDL 경로(create table, CTAS staging, drop table, create/drop view, view replace, folder create/drop)의 모든 `RESTException`(401/403/400/422/429/5xx/timeout/network)을 매핑하고 redact한다 (C-08. DDL 경로의 403/401 외 예외는 Phase 6 review에서 추가). Listing 오류 log와 DEBUG stack trace도 redact한다. 남은 경로: commit 시 403/401 외 `RESTException`(C-06), `updateFolder`(Dremio에서 쓰는 경로 없음), Iceberg library 내부 log | C-06은 Iceberg commit 경로의 5xx 처리(`CommitStateUnknownException`)를 바꾸지 않기 위해 그대로 둔다 |
| 실행 중 받은 값 | OAuth2 access token, vended S3 credential은 config에 없으므로 `redactSecrets` 대상이 아니다 | Phase 4: vended credential은 log/profile/KV에 남지 않는다 (live 0건). memory에는 만료 직전까지 남는다 (node별 cache, FileSystem conf). heap dump는 범위 밖 |
| G-27 단일 principal | 모든 Dremio 사용자가 source의 principal 하나로 Polaris에 접근한다 (`hasAccessPermission()`은 항상 true) | 최소 권한 principal 예시는 [phase3-oauth-catalog.md](phase3-oauth-catalog.md) §4.3. Phase 6 문서 |
| allowedNamespaces는 접근 제어가 아님 | 직접 경로로 SELECT/CREATE하면 allow list 밖도 접근된다 (C B-2) | Polaris grant로 권한을 제한한다. 강제 여부는 Phase 5/6 결정 |
| Vended credential | Phase 4에서 runtime 사용 (§7). config, KV, profile, log에 남지 않는다 | multi-node는 Phase 5 |
| HttpClient 4 header logger, Netty `LoggingHandler` | DEBUG에서 `X-Amz-Security-Token`이 찍힌다 | Phase 4에서 `logback.xml`에 INFO 고정 (§3. Netty는 review). 나열하지 않은 logger는 막지 않는다 |
| S3 credential이 없는 source/table의 host identity | Phase 4 review 이전 작업 트리는 provider chain을 빈 값으로 바꿔, key가 없으면 Dremio host의 `AWS_*` env 또는 EC2 instance profile을 조용히 썼다 (vended 조회 실패/403/vending 없음 table 포함). Hadoop 기본 chain을 그대로 둬도 S3A가 env/instance profile까지 시도한다 | Review 반영: 기본 chain을 `SimpleAWSCredentialsProvider`로 바꿔 fail closed (§7.4). host identity는 provider를 빈 값/명시 provider로 지정할 때만 |
| Harness `docker inspect` | Polaris container env에 MinIO secret과 bootstrap credential이 보인다 (harness가 env로 넘긴다) | 로컬 test 전용. 운영 Polaris는 secret store/IAM role을 쓴다 |
| Client secret 회수/rotate (Phase 5 F-7) | Polaris principal secret을 바꿔도 실행 중 source는 계속 동작한다. Iceberg OAuth2 session이 client secret 없이 token을 갱신하고, 새 client(Dremio 재시작, 설정 변경 update)를 만들 때만 실패한다. `token-exchange-enabled=false`도 차이 없음 | 회수를 즉시 반영하려면 Polaris grant/principal role도 회수하고 source를 update하거나 Dremio를 재시작한다 (known-limitations X-09) |
| Phase 5 secret scan | 7개 instance의 Dremio log, Polaris/proxy/fixture/MinIO container log, UI HTTP 응답 150개와 page에서 secret, `Bearer`, `access_token`, `client_secret`, `s3.secret-access-key`, `s3.session-token`, `eyJ` 0건 | [test-results.md §5](test-results.md#5-phase-5--실제-e2e--regression) |

## 7. Phase 4: storage secret 처리 결과

Storage 설정과 vended credential 동작은 [storage.md](storage.md). 이 절은 Agent D(INSTANCE=4)와 Integration(INSTANCE=5)의 secret 검증 결과다.

### 7.1 Unit: `TestRestCatalogSecretExposure` (7 tests, PASS)

| Test | 확인 내용 |
|---|---|
| `testVendedCredentialKeysAreSensitive` | vended/S3A/REST secret key 11개는 sensitive(대문자 포함), vended 부가 key 7개는 non-sensitive |
| `testVendedKeysConfiguredAsSecretsAreRedactedAndMasked` | `secretPropertyList`의 `token`, `s3.*` vended key 값과 `credential`의 `:` 뒤 부분이 `redactSecrets`/`describeConnectionFailure`에서 가려진다. `clearSecrets()` 후 값은 모두 `$DREMIO_EXISTING_VALUE$` |
| `testVendedKeysInPlainPropertiesAreRedactedAndLoggedByNameOnly` | plain `propertyList`의 vended key: 값은 redact, log는 key 이름만 |
| `testSourceConfigAndPluginIdRenderingDoesNotExposeSecrets` | `RestIcebergCatalogPluginConfig`, `SourceConfig`, `StoragePluginId`, plugin의 `toString()`에 secret 없음 |
| `testMaskedSourceJsonHidesSessionTokensAndVendedKeys` | masking한 source JSON에 session token/vended key 값이 없다. masking 전 JSON(KV 저장 형태)에는 있다 (G-12 positive control) |
| `testRestClientSecretsNeverReachTheHadoopConfiguration` | OAuth2 client secret, token, `header.Authorization`은 Hadoop conf에 없다 |
| `testS3AccessDeniedDoesNotExposeSecretKeyOrSessionToken` | fake S3(403)에 `TemporaryAWSCredentialsProvider`로 요청: S3A(SDK v1)의 head/open/list와 data file용 SDK v2 async(Netty) 읽기. secret key는 전송되지 않고 session token은 `X-Amz-Security-Token` header에만 있다. 예외 chain과 DEBUG log event(Dremio, Iceberg, S3A, AWS SDK v1/v2, Netty)에 secret 없음. Positive control: S3A + AWS SDK v1 DEBUG 102개, `com.amazonaws.request` DEBUG 6개, SDK v2 DEBUG 12개, Netty DEBUG 46개, Netty client 요청 수신 |

`TestRestCatalogVendedCredentials`(C, review 반영 후 33 tests)는 모든 test에서 `com.dremio.plugins.icebergcatalog`와 `org.apache.iceberg` logger의 log event(DEBUG까지)에 secret이 없는지 확인한다. `toString()`과 오류 메시지는 두 test(`testToStringNeverShowsCredentials`, `testForbiddenIsAPermissionErrorWithoutSecrets`)가 확인한다.

### 7.2 Live 노출 matrix (D INSTANCE=4; 건수만 셌고 값은 출력하지 않음)

Workload: static baseline source(CREATE/INSERT/SELECT), 잘못된 client secret으로 생성(v3 POST, v2 PUT)과 update, 잘못된 S3 secret source(SELECT 403 `Forbidden`, CTAS 403 `SignatureDoesNotMatch`), `dremio-admin encrypt` 값 source, vended source 2개(static key 있음/없음)로 SELECT/INSERT/CTAS. 검사한 vended 값은 같은 principal로 Polaris `loadTable`을 호출해 얻었고, 만료 시각이 Dremio DEBUG log와 같아 Dremio가 받은 값과 같음을 확인했다 (mode 600 file, 끝난 뒤 삭제).

Marker: `Bearer `, `access_token`, `client_secret`, `s3.secret-access-key`, `s3.session-token`, `s3.access-key-id`, `fs.s3a.secret.key`, `fs.s3a.session.token`, `X-Amz-Security-Token`(대소문자), JWT `eyJ`, `Authorization:`.

| 표면 | client secret | S3 secret key | 잘못 넣은 secret | vended access key / secret / session token | OAuth token (`Bearer`/`access_token`/`eyJ`) | Status |
|---|---|---|---|---|---|---|
| REST GET: v3 catalog (list, id, by-path), v3 source, `/apiv2/sources`, `/apiv2/source/{name}` (source 7개, vended 포함) | 0 | 0 | 0 | 0/0/0 | 0 | PASS (모두 `$DREMIO_EXISTING_VALUE$`) |
| REST 오류 응답 (생성 실패 v3/v2, update 실패) | 0 | 0 | 0 | – | 0 | PASS |
| Job v3, `/apiv2/job/{id}/details`, jobs-listing, profile JSON (job 30개, 실패 S3 job 포함) | 0 | 0 | 0 | 0/0/0 | 0 | PASS |
| `sys.jobs`, `sys.jobs_recent`(query text, error), `INFORMATION_SCHEMA`, `sys.options` | 0 | 0 | 0 | 0/0/0 | 0 | PASS (`sys.sources`는 OSS에 없음) |
| Dremio log, 기본 level | 0 | 0 | 0 | – | 0 | PASS |
| Dremio log, DEBUG(icebergcatalog, iceberg, s3, s3a) + 회전 `.gz` + `admin_encrypt_*.log` | 0 | 0 | 0 | 0/0/0 | 0 | PASS |
| KV store `data/db` | **평문** | **평문** | 잘못된 S3 secret 평문 2 (저장된 source) | 0/0/0 | 0 | NOT_SUPPORTED (G-12/X-01: at-rest 암호화는 기본 미지원, `dremio-admin encrypt`로 완화). vended 값은 저장되지 않는다 |
| KV store, `dremio-admin encrypt` 값 source | 암호문만 | 암호문만 | – | – | – | PASS |
| `data/pdfs`, `data/cm`, `data/zk`, `data/spill` | 0 | 0 | 0 | 0/0/0 | 0 | PASS |
| Polaris container log | 0 | 0 | 0 | 0/0/0 | 0 | PASS |
| Harness 출력 | 0 | 0 | 0 | 0/0/0 | 0 | PASS |
| Dremio process (`/proc/<pid>/environ`, cmdline) | 0 | 0 | – | – | – | PASS (`dremio-up.sh`가 `env -u`로 뺀다) |
| `docker inspect` (Polaris container) | root secret 1 | 1 | – | – | – | NOT_SUPPORTED (X-08: 운영 경로 아님. harness가 env로 전달, 로컬 전용) |
| HttpClient 4 `org.apache.http.headers` DEBUG (unit probe) | – | 0 | – | session token **노출** | – | FAIL → Phase 4 integration에서 `logback.xml` 고정 (§3) |
| Netty `io.netty.handler.logging.LoggingHandler` DEBUG (SDK v2 async 읽기, unit, review) | – | 0 | – | session token **노출** | – | FAIL → review에서 `logback.xml` 고정 (§3) |

Integration 재검증 (INSTANCE=5, Phase 4 최종 tarball): source 7개(static baseline, provider 생략, endpoint scheme 2개, harness recipe, vended, 대조군)와 Dremio 재시작 후 `scan-secrets.sh` 0건, 추가 marker `X-Amz-Security-Token`/`eyJ`/`fs.s3a.session.token`/`s3.session-token` 0건, v3 GET 7개 source의 S3 secret 0건 ([storage.md §14.4](storage.md#144-integration-재검증-instance5-phase-4-최종-tarball) #10).

### 7.3 G-12 완화책: `dremio-admin encrypt` (D live PASS)

- `dremio-admin encrypt`로 만든 `secret:1.…` 값을 `secretPropertyList`에 넣으면 source가 정상 동작하고(SELECT/INSERT) KV에는 암호문만 남는다. `ManagedStoragePlugin.resolveConnectionConf`가 `List<Property>` secret 중 암호화된 URI(`system:`/`secret:`)를 풀어서 plugin에 넘기기 때문이다.
- Secret은 stdin으로 입력한다. argv로 넘기면 process list에 보인다. `log/admin_encrypt_<ts>.log`에 입력 값은 없었다.
- 한계: key가 같은 data dir(`data/security`)에 있어 data dir 전체 backup에는 효과가 없다. `env:`/`file:` URI는 property list에서 풀리지 않는다 (`CredentialsServiceUtils::isEncryptedCredentials` filter).

### 7.4 Phase 4 storage secret 처리 원칙

- S3 key는 Catalog Credentials(`secretPropertyList`)에만 넣는다. `propertyList`에 넣으면 동작은 하지만 GET 응답에 값이 그대로 나온다 (UI는 저장을 막고 backend는 key 이름만 WARN).
- Source state가 `good`이어도 storage credential이 맞다는 뜻은 아니다 (state check는 catalog만 본다). 잘못된 S3 key의 오류 메시지(S3 403)에는 값이 없다.
- `dremio.s3.compat=true` 없이 S3-compatible storage를 쓰면 Dremio가 MinIO access key ID(secret은 아님)로 실제 AWS STS를 호출한다. 이 key를 AWS에 보내지 않으려면 compat를 켠다 ([storage.md §13](storage.md#13-troubleshooting)).
- Vended credential은 table prefix로 scope된 임시 key다. Static key보다 노출 범위가 작지만, 켜면 각 node가 catalog에 접근해야 한다.
- **S3 credential이 없을 때 (review 반영).** Source에 S3 key도 provider도 없으면 table 파일 접근은 실패한다 (fail closed: Hadoop 기본 provider chain을 `SimpleAWSCredentialsProvider`로 바꾼다. [storage.md §10](storage.md#10-credential-provider-phase-4-수정)). Dremio host의 identity는 provider를 **빈 값으로 명시**(또는 instance profile 등 provider를 명시)했을 때만 쓰인다: `AWS_ACCESS_KEY_ID`/`AWS_ACCESS_KEY` + `AWS_SECRET_ACCESS_KEY`/`AWS_SECRET_KEY` env(Hadoop conf로 복사됨), 없으면 EC2 instance profile.
  - Vended source에서 credential을 받지 못한 table(조회 실패, 401/403, catalog가 vending하지 않음)은 source 설정을 쓴다. Static key가 있으면 그 key, 없으면 fail closed, 위 opt-in이 있으면 host identity. 이때 Polaris의 table별 scope는 적용되지 않는다. Vended만 쓰는 source(`vn`)는 provider를 비워 두거나 `SimpleAWSCredentialsProvider`로 둔다 (빈 값 명시 금지).
  - Live (review INSTANCE=6): `snp`(key·provider 없음) 읽기 PERMISSION ERROR, 쓰기 `SimpleAWSCredentialsProvider: No AWS credentials in the Hadoop configuration`. `sne`(빈 값 명시)는 instance profile로 가서 IMDS 연결 실패. Harness `dremio-up.sh`는 Dremio process에서 `AWS_*` credential env를 모두 지운다.
