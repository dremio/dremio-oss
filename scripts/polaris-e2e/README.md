# Polaris RESTCATALOG E2E harness

Dremio tarball, Apache Polaris(`apache/polaris:1.1.0-incubating`), S3 호환 storage(MinIO)로 E2E stack을 띄우고 검증하는 bash script 모음이다.
모든 값은 `INSTANCE`(0..9)에서 나온다. 그래서 여러 Agent가 같은 host에서 서로 겹치지 않는 stack을 동시에 띄울 수 있다.

## 빠른 시작

```bash
cd scripts/polaris-e2e
export INSTANCE=3                      # 0..9, Agent마다 다르게
export S3_ACCESS_KEY=<s3AccessKey>     # 필수. 기본값 없음
export S3_SECRET_KEY=<s3SecretKey>     # 필수. 기본값 없음 (AWS_SECRET_ACCESS_KEY, MINIO_SECRET_KEY도 읽는다)
export S3_PREFIX=polaris-p3-$INSTANCE  # 선택. 기본값 polaris-e2e-$INSTANCE

./smoke.sh                             # up -> source -> namespace/table -> SQL -> secret scan -> teardown
```

단계별로 실행하려면 아래 순서를 따른다.

```bash
./polaris-up.sh                        # Polaris container + catalog e2ecat + 권한
./dremio-up.sh                         # tarball 복사, dremio.conf 생성, 시작, 첫 사용자, token 저장
./source.sh create                     # RESTCATALOG source "polaris" 생성 (state 출력)
./polaris-api.sh ns-create ns1         # Polaris에 namespace 생성
./polaris-api.sh table-create ns1 t0   # Polaris에 table 생성 (id long, data string)
./source.sh wait polaris polaris.ns1   # Dremio catalog tree에 보일 때까지 대기
./sql.sh "CREATE TABLE polaris.ns1.t1 (id INT, name VARCHAR)"
./sql.sh "INSERT INTO polaris.ns1.t1 VALUES (1, 'a')"
./sql.sh "SELECT * FROM polaris.ns1.t1"
./scan-secrets.sh                      # 로그의 secret 노출 건수 (0이어야 한다)
./source.sh delete; ./dremio-down.sh --purge; ./polaris-down.sh; ./s3.sh clean
```

## Script 목록

| Script | 역할 |
|---|---|
| `common.sh` | port, 경로, 환경 변수, 공통 함수 (`source`해서 쓴다) |
| `polaris-up.sh` / `polaris-down.sh` | Polaris container 시작/삭제. catalog, catalog role(`CATALOG_MANAGE_CONTENT`), `service_admin` binding까지 만든다 |
| `polaris-principal.sh <name> [PRIV...]` | 권한이 제한된 principal을 만든다 (403 test용). 기본 권한은 read-only. `--delete <name>`으로 삭제 |
| `polaris-api.sh` | Polaris token을 발급받아 Iceberg REST / Management API를 호출한다 (`raw`, `ns-list`, `ns-create`, `ns-drop`, `tables`, `views`, `table-create`, `table-get`, `table-drop`, `--as <principal>`) |
| `dremio-up.sh [--reuse]` / `dremio-down.sh [--purge]` | Dremio 시작/중지. `--reuse`는 기존 복사본과 data를 유지한다 (재시작 test). `--purge`는 `$WORK/dremio`를 삭제한다 |
| `source.sh` | `create`, `update`, `get`, `ls <path>`, `wait <path> <child> [sec]`, `delete`, `json` (`json`은 secret을 placeholder로 바꿔 출력만 한다). `delete`는 root listing에서 id를 찾으므로 state가 `bad`인 source도 지운다 |
| `sql.sh` | `/api/v3/sql`로 실행하고 job이 끝날 때까지 기다린 뒤 state와 결과 row를 출력한다. COMPLETED면 항상 `/results`를 읽는다 (`SHOW`/`DESCRIBE`처럼 coordinator에서 답하는 문장은 job의 `rowCount`가 0이어도 row가 있다). COMPLETED가 아니면 exit 1 |
| `scan-secrets.sh [VAR...]` | Dremio log와 Polaris container log에서 secret 값과 token marker의 건수만 출력한다. 하나라도 있으면 exit 1 |
| `s3.sh ls\|count\|clean` | `s3://$S3_BUCKET/$S3_PREFIX/` 아래 object를 조회하거나 삭제한다 (prefix는 `polaris-`로 시작해야 한다) |
| `smoke.sh` | 위 script를 순서대로 실행하는 smoke test. `SMOKE_KEEP=1`이면 teardown하지 않는다 |

## Port (offset = `INSTANCE * 10`)

| 항목 | 계산식 | INSTANCE=0 | INSTANCE=9 |
|---|---|---|---|
| Polaris HTTP (`QUARKUS_HTTP_PORT`) | 18181 + offset | 18181 | 18271 |
| Polaris management/health (`QUARKUS_MANAGEMENT_PORT`) | 18182 + offset | 18182 | 18272 |
| Dremio web (`services.coordinator.web.port`) | 19047 + offset | 19047 | 19137 |
| Dremio client (`services.coordinator.client-endpoint.port`) | 31110 + offset | 31110 | 31200 |
| Dremio flight (`services.flight.port`) | 32110 + offset | 32110 | 32200 |
| Dremio fabric (`services.fabric.port`) | 27710 + offset | 27710 | 27800 |
| Embedded ZooKeeper (`services.coordinator.master.embedded-zookeeper.port`) | 12181 + offset | 12181 | 12271 |

- Dremio port는 제품 기본값(9047/31010/32010/45678/2181)과 겹치지 않게 골랐다. 그래서 host에 다른 Dremio가 떠 있어도 충돌하지 않는다.
- conduit port와 web-admin(liveness) port는 기본값 0(자동 할당)을 그대로 쓴다.
- Container는 `--network host`로 실행한다. 이름은 `${E2E_NAME_PREFIX:-p3-}polaris-e2e-<INSTANCE>`이다.
- `*-up.sh`는 시작하기 전에 port가 비어 있는지 확인하고, 사용 중이면 실패한다.

## 환경 변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `INSTANCE` | `0` | 0..9 |
| `E2E_WORK` | `/tmp/polaris-e2e` | 작업 디렉터리 root. 실제 경로는 `WORK=$E2E_WORK/<INSTANCE>` |
| `E2E_NAME_PREFIX` | `p3-` | container 이름 prefix |
| `S3_ENDPOINT` | `http://127.0.0.1:9000` | Polaris에는 그대로, Dremio `fs.s3a.endpoint`에는 scheme을 뺀 `host:port`로 넣는다 |
| `S3_BUCKET` / `S3_PREFIX` / `S3_REGION` | `dremiodev` / `polaris-e2e-<INSTANCE>` / `us-east-1` | catalog base location은 `s3://$S3_BUCKET/$S3_PREFIX` |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | 없음 (필수) | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`(또는 `MINIO_SECRET_KEY`)도 읽는다 |
| `POLARIS_IMAGE` | `apache/polaris:1.1.0-incubating` | |
| `POLARIS_ROOT_SECRET` | `s3cr3t` | 로컬 test 전용 bootstrap 값 (in-memory Polaris) |
| `POLARIS_CATALOG` / `POLARIS_CATALOG_ROLE` | `e2ecat` / `e2e_admin` | |
| `POLARIS_EXTRA_ENV` | 없음 | `;`로 구분한 Polaris/Quarkus 설정 (예: token 수명 test) |
| `DREMIO_TARBALL_DIR` | `distribution/server/target/dremio-community-*/dremio-community-*` | `bin/dremio`가 있는 디렉터리 |
| `DREMIO_JAVA_HOME` | `~/jvm/jdk-17.0.9` | |
| `DREMIO_HEAP_MB` / `DREMIO_DIRECT_MB` | `2048` / `2048` | |
| `DREMIO_START_TIMEOUT` / `POLARIS_START_TIMEOUT` | `300` / `120` | 초 |

`source.sh` 설정(`SOURCE_URI`, `SOURCE_WAREHOUSE`, `SOURCE_SCOPE`, `SOURCE_OAUTH2_URI`, `SOURCE_PRINCIPAL`, `SOURCE_CREDENTIAL`, `SOURCE_ALLOWED_NS`, `SOURCE_RECURSIVE`, `SOURCE_VENDED`, `SOURCE_NO_S3_KEYS`, `SOURCE_OMIT_PROPS`, `SOURCE_EXTRA_PROPS`, `SOURCE_KEEP_SECRETS`, `SOURCE_NAMES_REFRESH_MS`, `SOURCE_VERBOSE`)은 script 머리말 주석에 설명되어 있다. 예시는 다음과 같다.

```bash
SOURCE_ALLOWED_NS="ns1,ns2.child" SOURCE_RECURSIVE=false ./source.sh create scoped
./polaris-principal.sh reader && SOURCE_PRINCIPAL=reader ./source.sh create ro
SOURCE_CREDENTIAL='root:<wrong>' ./source.sh create badcred      # 오류 경로 확인
SOURCE_OMIT_PROPS=warehouse ./source.sh create nowh
SOURCE_KEEP_SECRETS=1 SOURCE_VENDED=true ./source.sh update      # masked 값으로 PUT
```

## Secret 정책

- S3 secret에는 기본값이 없다. 실행할 때 환경 변수로만 넘긴다. Repo의 파일, 문서, test에는 `<s3AccessKey>`, `<s3SecretKey>`, `<client_id>:<client_secret>` placeholder만 쓴다.
- Script는 secret을 command line에 올리지 않는다.
  - curl body는 stdin으로 넘긴다.
  - docker에는 `-e NAME`처럼 이름만 넘긴다.
  - mc에는 `MC_HOST_<alias>` 환경 변수로 넘긴다. 설정 디렉터리는 임시 `MC_CONFIG_DIR`을 쓰고 `~/.mc`는 쓰지 않는다.
- Script는 secret을 출력하지 않는다. 오류 본문과 log tail은 알려진 secret 값을 `<redacted>`로 바꾼 뒤 출력한다.
- `$WORK`의 token, header, principal credential 파일은 mode 600으로 만든다 (`umask 077`). `polaris-down.sh`와 `dremio-down.sh`가 지운다.
- `scan-secrets.sh`는 건수만 출력한다. 값이나 일치한 줄은 출력하지 않는다.
- `bash -x`로 실행하지 않는다. trace에 secret이 그대로 찍힌다.

## 주의 사항

- 먼저 `scripts/dev build distribution/server`로 tarball을 만들어야 한다. 이 tarball의 `dremio-dac-ui` jar는 `dac/ui`를 다시 build하지 않으면 예전 UI다. REST API test에는 영향이 없다.
- 시간(참고 값): Polaris health 약 5초, Dremio tarball 복사 및 기동 약 20~25초, `smoke.sh` 전체 약 2분 40초. 이 중 2분은 namespace가 나타나기를 기다리는 시간이다.
- 메모리: Dremio 1개당 heap 2 GB + direct 2 GB, Polaris container 약 0.5 GB. 동시에 여러 instance를 띄울 때 고려한다.
- Source를 만든 **뒤에** Polaris에서 만든 namespace는 다음 names refresh 때 Dremio catalog tree(`/api/v3/catalog/by-path`)에 나타난다.
  - 기본 주기는 1시간이다. `SOURCE_NAMES_REFRESH_MS=60000`(최소값)으로 줄여도 실제로는 약 60~120초가 걸린다.
  - 그 table을 SQL로 직접 조회해도 바로 등록된다.
  - Source를 만들기 **전에** 있던 namespace는 생성 직후부터 보인다.
- `/api/v3/catalog/{id}/refresh`는 source metadata refresh가 아니다 (dataset reflection refresh). Source에는 404를 반환한다.
- Dremio `DROP TABLE`을 실행해도 S3 object는 남는다. 정리는 `s3.sh clean`으로 한다.
- 잘못된 credential이나 `warehouse`가 빠진 source를 만들면 API는 HTTP 400을 돌려주고, Phase 3부터 `errorMessage`에 원인 hint와 redact된 detail이 들어 있다 (G-09). Dremio 재시작 때 시작에 실패한 source는 일반 메시지("Source is not currently available.")만 보이고 hint는 server.log에 있다.
- `s3.sh`는 `@`나 `/`가 들어간 S3 credential을 지원하지 않는다. mc가 `MC_HOST`의 credential을 percent-decode하지 않기 때문이다.
- `shellcheck`가 host에 없으면 `docker run --rm -v "$PWD:/mnt:ro" -w /mnt koalaman/shellcheck:stable -x *.sh`로 검사한다.
