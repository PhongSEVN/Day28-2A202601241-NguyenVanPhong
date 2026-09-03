# ANSWERS — Day 28 Track 2

Người làm: Nguyễn Văn Phong — làm **cá nhân**, đi qua đủ 5 vai trò.
Nhánh: `ca-nhan-NguyenVanPhong`.

---

## 1. Bốn boundary đã hoàn thiện

`src/lab28_platform/integration_tasks.py`:

| Hàm | IP | Quyết định |
|---|---|---|
| `event_headers` | IP01 + IP10 | `idempotency-key` luôn có; `traceparent` chỉ thêm khi trace đang chạy — không gửi header W3C rỗng/không hợp lệ. Giá trị encode từ tham số, không hardcode. |
| `dedupe_latest` | IP03 | Gom theo `idempotency_key`, giữ bản có `(occurred_at, event_id)` lớn nhất → không phụ thuộc thứ tự giao của Kafka. Sort theo key để MERGE source tất định. Duyệt iterable đúng 1 lần. |
| `feast_online_request` | IP04 | Lấy `FEATURE_REFS` từ `contracts.py` (single source of truth), không chép lại danh sách feature. `full_feature_names=False` khớp registry. |
| `readiness_status` | IP07 + IP08 | Ưu tiên: mandatory fail → `not_ready`; optional fail → `degraded`; còn lại → `ready`. List-hóa iterable vì caller truyền generator. |

Kiểm chứng: `starter-tests` + `tests` 83 passed, `ruff` sạch.

---

## 2. Kết quả 10 integration point

| IP | Trạng thái | Bằng chứng |
|---|---|---|
| IP01 HTTP → Kafka | ✅ | `evidence/ip01-kafka-consume.json` — event trên `data.raw` có `traceparent` |
| IP02 Kafka → Airflow | ✅ | `evidence/ip02-airflow-run.json` — DAG run `it-0a60aa6d`, 4 task success, 4 asset event |
| IP03 Airflow/Spark → Delta | ✅ | `evidence/ip03-delta-history.json` — `feedback` v9, MERGE, 1 inserted / 0 copied (idempotent) |
| IP04 Delta → Feast | ✅ | `evidence/ip04-feast-online.json` — online row có `delta_version` + freshness |
| IP05 Delta → Qdrant | ✅ | `evidence/ip05-qdrant-search.json` — hybrid query, `points_upserted=13`, ID tất định từ `doc_id` |
| IP06 Eval → MLflow Registry | ✅ | `evidence/ip06-mlflow-release.json` — `lab28-rag-release` champion, signature + provenance |
| IP07 Prompt → vLLM thật | ❌ **UNVERIFIED** | Không có endpoint GPU. `probe_vllm` = `unreachable: ConnectError`. Không giả lập theo yêu cầu đề. |
| IP08 Client → Envoy gateway | ✅ | `evidence/ip08-gateway.json` — 200 + 429 (`local_rate_limited`) kèm `x-request-id` |
| IP09 Components → Prometheus/Grafana | ✅ | `evidence/ip09-prometheus-targets.json`, `ip09-grafana-dashboards.json` |
| IP10 Components → OTLP trace | ⚠️ **một phần** | `evidence/ip10-trace.json` — 6/11 span (nhánh ingestion đủ: gateway/api.ingest/kafka.produce/kafka.consume/airflow.dag/spark.delta_merge). Thiếu 5 span serving (`api.ask`, `feast.get_online_features`, `mlflow.resolve_release`, `qdrant.query`, `vllm.chat_completion`) vì cần 1 request `/ask` qua vLLM thật trong cùng trace. LangSmith leg: `UNVERIFIED` (không có `LANGSMITH_API_KEY`). |

`lab28 integration`: score 83, 5/6 verified passing, `ready:false` (do IP07).

Journeys: J1 12 passed / 3 skip (gpu), J2 9 passed, suite non-gpu 56 passed.

---

## 3. Luồng đúng (happy path) — IDs để đối chiếu

- DAG run: `it-0a60aa6d` — state `success`
- Trace ID: `ee805d08234d4a37ae18512a79babc01`
- Delta `feedback` version: `9` (MERGE, 1 inserted, 0 copied → replay-safe)
- MLflow: `lab28-rag-release`, champion đã chạy `v1 → v2 → v3`, run mới nhất `0d10c5c9136b4c0d82bd3f2a44206c4e`, `delta_version=9`
- Embedding model: `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2@faf4aa4225822f3bc6376869cb1164e8e3feedd0`

---

## 4. Replay-safe (IP03)

J2 `test_j2_idempotent_replay.py` — 9 passed. Gửi lại cùng lô: Delta table version có thể tăng nhưng số row không đổi, Feast đếm fact 1 lần, Qdrant giữ đúng 1 point/doc. `dedupe_latest` gộp trùng trước khi MERGE; `stable_point_id` cho Qdrant; MERGE key = `idempotency_key`.

---

## 5. Sự cố đã tạo — dấu hiệu → quan sát → khôi phục → không mất dữ liệu

### 5a. Sự cố có chủ đích (J4 degraded-recovery)

- **Tạo:** cho một dependency **không bắt buộc** (Feast) lỗi.
- **Dự đoán dấu hiệu:** `/ready` = `degraded` (không `not_ready`), `lab28_component_ready{name="feast"}=0`, câu trả lời vẫn trả về nhưng `evidence.degraded=true`.
- **Quan sát:** `test_j4_degraded_recovery.py` pass — hệ thống trả lời ở chế độ degraded, không 5xx.
- **Khôi phục:** dependency lên lại → `/ready` = `ready`, không thao tác thủ công.
- **Không mất dữ liệu:** event trong lúc lỗi vẫn nằm ở Kafka (`data.raw` retention 7 ngày); DAG chạy lại drain vào Delta; số row đúng.

### 5b. Sự cố thật gặp khi dựng lab (bonus)

- **Triệu chứng:** `docker build` airflow fail — `pip install` tải `pyspark==4.2.0` (sdist 450 MB) trên mạng yếu, `IncompleteRead` / `ProtocolError`, mỗi lần fail tải lại từ 0.
- **Nguyên nhân:** `--no-cache-dir` + link chậm → không bao giờ xong.
- **Khắc phục:** `scripts/fetch_airflow_offline.sh` tải sdist 1 lần trên host có resume + verify `gzip -t`; Dockerfile đổi sang `pip install --find-links docker/airflow/offline` + `--retries/--timeout`. `.gitignore` chặn blob 450 MB. Không đụng logic DAG/test. Commit `9da8671`.

---

## 6. Load profile & bottleneck

`load-tests/run_profile.py`, GET `/ready`, 200 request / 8 worker. Xem `evidence/load-profile.json`.

| Đường | 200 | Lỗi | p50 | p95 | p99 |
|---|---|---|---|---|---|
| Qua gateway `:8080` | 32 | 168 (HTTP 429) | 5.95 ms | 506 ms | 725 ms |
| Direct API `:8000` | 200 | 0 | 487 ms | 755 ms | 1168 ms |

- Qua gateway: **rate-limit bound** — Envoy `local_rate_limit` (IP08) đặt thấp có chủ đích để demo 429. `urllib` raise trên 429 nên script ghi status `0`.
- Direct API: 100% thành công. Latency bị chi phối bởi `/ready` **probe mọi dependency đồng bộ mỗi lần gọi**; probe vLLM thêm đuôi latency vì đang `ConnectError`. Có vLLM thật hoặc readiness async/cache → p50 tụt sâu dưới 487 ms.
- Không thấy memory leak khi chạy J1–J5 + load nhiều lần; container giữ `healthy`.

---

## 7. Kubernetes / GitOps

`scripts/validate_manifests.py` → "Kubernetes and GitOps manifest contracts passed".
`scripts/check_portability.py`, `scripts/verify_matrix.py` (245 checks) → OK.

- **Triển khai:** manifest trong `gitops/` + `deploy/`; Argo CD sync theo Git là source of truth.
- **Rollback:** revert commit GitOps → Argo CD tự sync về manifest cũ. Với model: `lab28 rollback` chuyển alias `champion` về version trước (đã có v1→v2→v3, rollback = v3→v2) — J3 `test_j3_promotion_rollback.py` pass, không sửa mã.
- **Probe/security:** `/health` liveness (không chạm dependency), `/ready` readiness (503 khi `not_ready`, gateway rút pod khỏi rotation); gateway có rate limit + `x-request-id`.

---

## 8. ready / degraded / not_ready

| Trạng thái | Điều kiện | HTTP | Hành vi gateway |
|---|---|---|---|
| `ready` | mọi probe (bắt buộc + tùy chọn) OK | 200 | giữ pod |
| `degraded` | probe bắt buộc OK, ít nhất 1 probe tùy chọn lỗi | 200 | giữ pod, câu trả lời gắn `degraded=true` |
| `not_ready` | ít nhất 1 probe **bắt buộc** lỗi | 503 | rút pod khỏi rotation |

Feast là tùy chọn (feature nguội làm giảm chất lượng, không làm sai câu trả lời). vLLM là bắt buộc **chỉ khi** `LAB28_VLLM_REQUIRE_REAL=true` (mặc định container = `false` → stack cơ bản báo `degraded`, đúng thiết kế).

---

## 9. Trade-off đã chọn

1. **Feast non-mandatory trong `/ready`.** Được: pod không bị rút khỏi rotation vì feature store nguội. Mất: câu trả lời có thể thiếu ngữ cảnh asker mà client không thấy rõ nếu bỏ qua `degraded_reasons`.
2. **`dedupe_latest` sort toàn bộ theo key.** Được: MERGE source tất định, dễ test. Mất: O(n log n) + giữ cả batch trong RAM — batch rất lớn cần streaming/partition.
3. **ID tất định (`uuid5`) cho Qdrant/Delta thay vì hash nội dung.** Được: replay ghi đè đúng 1 điểm. Mất: sửa nội dung cùng `doc_id` vẫn là 1 điểm — cần bump khóa khi đổi nghĩa.
4. **Pre-fetch pyspark vào build context.** Được: build airflow chịu được mạng yếu. Mất: thêm 1 bước host + blob 450 MB ngoài repo (phải chạy `scripts/fetch_airflow_offline.sh` trước khi build ở máy mới).

---

## 10. Production gaps — sẽ cải tiến khi triển khai thật

1. **IP07 chưa verify** — chưa có vLLM GPU thật. Prod: endpoint vLLM có health `/version` + metric `vllm:` thật, gắn vào `/ready` khi `require_real=true`.
2. **IP10 trace chưa trọn** — trace hiện chỉ phủ nhánh ingestion. Cần 1 trace mang đủ 11 span gồm nhánh `/ask` (feast/qdrant/mlflow/vllm). Phụ thuộc (1).
3. **`/ready` probe đồng bộ mọi lần gọi** → p50 ~487 ms. Prod: cache kết quả probe (TTL ngắn) hoặc probe nền, `/ready` chỉ đọc trạng thái.
4. **Kafka RF=1, 1 broker; Spark driver 1g** — cấu hình lab. Prod: RF≥3, resource thật, `MAX_ACTIVE_RUNS_PER_DAG` cân theo throughput.
5. **CLI lỗi trên Windows** — `lab28 integration` in JSON có ký tự `→` ra stdout cp1252 → `UnicodeEncodeError`. Workaround: `$env:PYTHONUTF8=1`. Fix thật: CLI ép `sys.stdout` reconfigure UTF-8.
6. **Envoy local rate limit rất thấp** — tốt để demo 429, nhưng che mất số liệu throughput thật của app. Prod: tách limit demo và limit thật.
7. **`evidence/` bị `.gitignore`** — nộp evidence bundle riêng (không commit vào repo).

---

## 11. Điều khó nhất

_(viết 3–5 câu: phần nào tốn thời gian nhất, vì sao — ví dụ: hiểu vì sao `dedupe_latest` pass test riêng nhưng fail test Delta / phân biệt `not_ready` vs `degraded` / dựng full stack trên mạng yếu.)_

---

## 12. Các vai trò đã đi qua (làm cá nhân)

- **Ingestion & Orchestration** (IP01–IP02): `event_headers`, kiểm DAG `lab28_ingestion_pipeline`, replay/DLQ qua J2.
- **Data & ML** (IP03–IP04–IP06): `dedupe_latest`, `feast_online_request`, `lab28 release` (v1→v3), Delta time travel.
- **Serving & Retrieval** (IP05–IP07): `lab28 index`, grounding path, degraded behavior khi vLLM `ConnectError`.
- **Platform & Observability** (IP08–IP10): `readiness_status`, gateway 200/429, Prometheus targets, OTLP trace, validate manifest K8s/GitOps.
- **Presenter / Incident Commander**: evidence pack, kịch bản demo, tường thuật sự cố (mục 5).
