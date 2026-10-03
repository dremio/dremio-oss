# Polaris RESTCATALOG 문서

Dremio OSS 26.0.5의 generic Iceberg REST Catalog source(`RESTCATALOG`)로 Apache Polaris OSS와 S3/S3-compatible storage를 쓰는 작업(`feature/polaris-restcatalog`)의 문서 모음이다. 작업 정의는 [polaris-catalog-support.md](../reference_docs/catalog-support/polaris-catalog-support.md)다. 문서의 secret은 모두 placeholder(`<client_id>:<client_secret>`, `<access-key>`, `<secret-key>`)다.

## 빠른 시작

1. Polaris 쪽 준비: catalog, principal, principal role, catalog role과 grant를 만든다 (Dremio 밖의 작업, [configuration.md §6](configuration.md#6-polaris-쪽-사전-준비-외부-작업)).
2. Dremio에서 source를 등록한다.
   - UI: Add Source → Lakehouse Catalogs → **Apache Polaris OSS** preset 또는 **Iceberg REST Catalog** ([configuration.md §1](configuration.md#1-dremio-ui-등록-방법)).
   - REST API: `POST /api/v3/catalog`, `type=RESTCATALOG` ([configuration.md §2](configuration.md#2-rest-api-등록-방법)).
3. Storage 설정을 넣는다: AWS S3는 [configuration.md §3](configuration.md#3-polaris-oss--s3-예제-aws-s3-공식-baseline), MinIO는 [§4](configuration.md#4-minio-사용-예제). Vended credentials는 [§5](configuration.md#5-vended-credentials-선택).
4. 문제가 생기면 [configuration.md §9](configuration.md#9-troubleshooting)와 [known-limitations.md](known-limitations.md)를 본다.

로컬 E2E stack(Polaris container + Dremio tarball + MinIO)은 [`scripts/polaris-e2e`](../../scripts/polaris-e2e/README.md)로 띄운다 (`smoke.sh`).

## 문서 목록

| 문서 | 내용 |
|---|---|
| [design.md](design.md) | 목표와 범위, 구조(control/data/vended path), 주요 class, 설정 필드, 오류 매핑, 설계 결정(D-1–D-17), 변경 이력 |
| [configuration.md](configuration.md) | 등록 방법(UI, REST API), Polaris OSS + AWS S3 예제, MinIO 예제, vended credentials, allowedNamespaces, 추가 설정, troubleshooting |
| [storage.md](storage.md) | Object storage 설정 reference, MinIO recipe, SSL/region/path-style/provider, vended credentials 동작, Phase 4 검증 matrix |
| [security.md](security.md) | Secret 경로별 처리, masking, redaction, log 설정, 노출 검증 결과, 남은 위험 |
| [test-results.md](test-results.md) | Phase별 unit, Local CI, live 결과. Phase 5 E2E(§5), Phase 6 release readiness(§7) |
| [known-limitations.md](known-limitations.md) | 알려진 제약과 결함, 우회책, 상태 (catalog/DDL, namespace, lifecycle, storage, security, UI/packaging) |
| [compatibility-matrix.md](compatibility-matrix.md) | 공식 Dremio RESTCATALOG 설정, Iceberg REST operation, SQL 기능, storage/auth 모드별 지원 상태 |
| [progress.md](progress.md) | Phase 현황, Phase별 완료 gate, 완료 기준 현황, 환경 메모 |
| [phase1-analysis.md](phase1-analysis.md) | Phase 1 구조 분석과 Gap 목록(G-01–G-28) |
| [phase3-oauth-catalog.md](phase3-oauth-catalog.md) | Phase 3 OAuth2/catalog 연동 상세 결과 |
| [img/](img/) | UI 화면 (Phase 5) |

## 검증 범위 요약

- 검증 환경: Apache Polaris `1.1.0-incubating`, 로컬 MinIO, Dremio tarball 단일 node.
- AWS S3, multi-node executor, 외부 IdP, Azure/GCS는 계정이나 환경이 없어 `ENVIRONMENT_BLOCKED`다.
- Polaris Management API(catalog, principal, role, grant 관리)는 범위 밖이다.
