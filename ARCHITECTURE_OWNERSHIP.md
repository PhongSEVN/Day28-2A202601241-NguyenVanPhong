# Kiến trúc & phân vai — Day 28 Track 2

Nộp cho: Nguyễn Văn Phong — 2A202601241. Làm **cá nhân**, một người đi qua đủ 5 vai.

## Sơ đồ kiến trúc

![Sơ đồ kiến trúc 10 điểm kết nối](docs/images/lab28-architecture-overview.png)

Nguồn vector: [`docs/images/lab28-architecture-overview.svg`](docs/images/lab28-architecture-overview.svg).
Định nghĩa máy đọc được: [`contracts/integration-matrix.yaml`](contracts/integration-matrix.yaml).

Đọc theo ba vùng:

1. **Luồng chính** — client → Envoy gateway → FastAPI → Kafka `data.raw` → Airflow DAG
   `lab28_ingestion_pipeline` → Spark Connect → Delta Lake.
2. **Dữ liệu & mô hình** — Delta cấp dữ liệu cho Feast (online features), Qdrant (vector),
   MLflow (registry + champion alias); FastAPI gọi vLLM để sinh câu trả lời có grounding.
3. **Giám sát** — mọi service expose `/metrics` cho Prometheus/Grafana; OTLP span đẩy về
   collector → Jaeger, một trace ID xuyên toàn bộ boundary.

## Phân vai theo integration point

Làm cá nhân nên "ownership" ở đây là **thứ tự em thực hiện từng vai**, không phải chia người.

| Vai | Integration point | Việc đã làm | Bằng chứng |
|---|---|---|---|
| Ingestion & Orchestration | IP01, IP02 | `event_headers` (traceparent + idempotency-key qua Kafka); kiểm DAG `lab28_ingestion_pipeline`, task states, asset events; replay/idempotency qua J2 | `evidence/ip01-kafka-consume.json`, `evidence/ip02-airflow-run.json` |
| Data & ML | IP03, IP04, IP06 | `dedupe_latest` (MERGE source replay-safe); `feast_online_request` (khớp `FEATURE_REFS`); `lab28 release` chuỗi `v1 → v2 → v3` + champion alias; Delta time travel | `evidence/ip03-delta-history.json`, `evidence/ip04-feast-online.json`, `evidence/ip06-mlflow-release.json` |
| Serving & Retrieval | IP05, IP07 | `lab28 index` (Qdrant ID tất định từ `doc_id`); dựng vLLM `0.10.1.1` thật trên Kaggle T4 (`Qwen/Qwen3-1.7B`) qua Cloudflare tunnel, verify `/version` + `/v1/models` + 61 metric `vllm:` | `evidence/ip05-qdrant-search.json`, `evidence/ip07-vllm-identity.json` |
| Platform & Observability | IP08, IP09, IP10 | `readiness_status` (`ready`/`degraded`/`not_ready`); gateway 200 + 429 kèm `x-request-id`; Prometheus targets + Grafana dashboards; một trace 25 span đủ 11 span bắt buộc; validate manifest K8s/GitOps | `evidence/ip08-gateway.json`, `evidence/ip09-*.json`, `evidence/ip10-trace.json` |
| Presenter / Incident Commander | — | Evidence index (`evidence/integration-report.json`), kịch bản demo, tường thuật sự cố (`ANSWERS.md` §5), load profile + bottleneck (§6) | `evidence/integration-report.json`, `evidence/load-profile.json` |

## Ownership hạ tầng (service → vai)

Theo `contracts/integration-matrix.yaml` (`SERVICE_OWNERS`):

| Service | Vai chịu trách nhiệm |
|---|---|
| `gateway`, `otel-collector`, `prometheus`, `grafana` | Platform & Observability |
| `lab28-api`, `qdrant`, `vllm` | Serving & Retrieval |
| `kafka`, `airflow` | Ingestion & Orchestration |
| `spark-delta`, `feast`, `mlflow` | Data & ML |

## Trạng thái tổng

`lab28 integration`: score **100**, `ready: true`. IP01–IP07 verified; IP08/IP09/IP10 chứng minh
bằng evidence file + integration-test suite (lệnh `integration` không probe in-process nên
đánh dấu `unverified` theo thiết kế). Chi tiết ở [`ANSWERS.md`](ANSWERS.md).
