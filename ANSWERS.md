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
| IP07 Prompt → vLLM thật | ✅ **VERIFIED** | vLLM 0.10.1.1 thật trên Kaggle T4 (`Qwen/Qwen3-1.7B`), gọi qua Cloudflare quick tunnel. `evidence/ip07-vllm-identity.json`: `/version` = `0.10.1.1`, `/v1/models` = `Qwen/Qwen3-1.7B`, 61 metric `vllm:`, `is_real_vllm: true`. Completion thật trong J1/J3 (`llm_ms ≈ 9000`). Không giả lập. |
| IP08 Client → Envoy gateway | ✅ | `evidence/ip08-gateway.json` — 200 + 429 (`local_rate_limited`) kèm `x-request-id` |
| IP09 Components → Prometheus/Grafana | ✅ | `evidence/ip09-prometheus-targets.json`, `ip09-grafana-dashboards.json`. Lưu ý: target vLLM `host.docker.internal:8001` báo `down` khi dùng endpoint Kaggle remote — không cấu scrape proxy cho tunnel, không commit URL. |
| IP10 Components → OTLP trace | ✅ **đủ span** | `evidence/ip10-trace.json` — 25 span, `missing: []`, đủ 11 span bắt buộc gồm `lab28.vllm.chat_completion`, `lab28.api.ask`, `lab28.feast.get_online_features`, `lab28.qdrant.query`, `lab28.mlflow.resolve_release`. LangSmith leg: `UNVERIFIED` (không có `LANGSMITH_API_KEY`). |

`lab28 integration`: **score 100, `ready: true`** — 6/6 verified passing (IP01/03/04/05/06/07). IP02/08/09/10 lệnh này đánh dấu `unverified` theo thiết kế (chỉ probe từ serving process), chứng minh bằng evidence file + test suite.

Journeys với vLLM thật: **68 passed / 3 failed** (`pytest integration-tests -m "not langsmith"`). 3 fail đều do topology tunnel remote, không phải lỗi code:

| Test fail | Nguyên nhân |
|---|---|
| `j4 ...gateway_stops_routing_to_a_pod_that_is_not_ready` | J4 inject lỗi bằng cách dừng container vLLM local — vLLM ở Kaggle nên không tái hiện được failure đó |
| `prometheus ...inference_endpoint_is_scraped` | Prometheus scrape `host.docker.internal:8001` (target local) — vLLM ở Kaggle → target `down` |
| `trace ...spans_the_processes` | Đòi ≥4 tên service trong trace, được 3 (`airflow/api/gateway`) — vLLM remote không tự phát span OTLP qua tunnel; span `lab28.vllm.chat_completion` vẫn có (client span của `api`) |

Baseline sạch không tunnel: `pytest integration-tests -m "not gpu and not langsmith"` = 56 passed. J1 12 / J2 9 (không GPU).

---

## 3. Luồng đúng (happy path) — IDs để đối chiếu

Từ lần chạy `pytest integration-tests -m "not langsmith"` với vLLM thật:

- Trace ID: `d55a93329910402595452612d3263b3b` — `evidence/ip10-trace.json`, 25 span, `required_spans_missing: []`, đủ 11 span bắt buộc gồm nhánh serving (`lab28.api.ask`, `lab28.feast.get_online_features`, `lab28.qdrant.query`, `lab28.mlflow.resolve_release`, `lab28.vllm.chat_completion`)
- DAG run: `it-9128c41e` — state `success`, 4 task success (`evidence/ip02-airflow-run.json`); DAG run trong trace đầy đủ: `it-35f3f21e`
- Delta version: `feedback` v17, `documents` v10 — MERGE, replay-safe (`evidence/ip03-delta-history.json`)
- MLflow: `lab28-rag-release`, champion `v3` (chuỗi `v1 → v2 → v3`), run `0d10c5c9136b4c0d82bd3f2a44206c4e` (`evidence/ip06-mlflow-release.json`)
- vLLM: `Qwen/Qwen3-1.7B`, vLLM `0.10.1.1`, 61 metric `vllm:` (`evidence/ip07-vllm-identity.json`)
- Embedding model: `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2@faf4aa4225822f3bc6376869cb1164e8e3feedd0`

---

## 4. Replay-safe (IP03)

J2 `test_j2_idempotent_replay.py` — 9 passed. Gửi lại cùng lô: Delta table version có thể tăng nhưng số row không đổi, Feast đếm fact 1 lần, Qdrant giữ đúng 1 point/doc. `dedupe_latest` gộp trùng trước khi MERGE; `stable_point_id` cho Qdrant; MERGE key = `idempotency_key`.

---

## 5. Sự cố đã tạo — dấu hiệu → quan sát → khôi phục → không mất dữ liệu

### 5a. Sự cố có chủ đích (J4 degraded-recovery)

- **Tạo:** cho một dependency **không bắt buộc** (Feast) lỗi.
- **Dự đoán dấu hiệu:** `/ready` = `degraded` (không `not_ready`), `lab28_component_ready{name="feast"}=0`, câu trả lời vẫn trả về nhưng `evidence.degraded=true`.
- **Quan sát:** `test_j4_degraded_recovery.py` pass ở baseline non-gpu (56 passed) — hệ thống trả lời ở chế độ degraded, không 5xx.
- **Khôi phục:** dependency lên lại → `/ready` = `ready`, không thao tác thủ công.
- **Không mất dữ liệu:** event trong lúc lỗi vẫn nằm ở Kafka (`data.raw` retention 7 ngày); DAG chạy lại drain vào Delta; số row đúng.
- **Lưu ý tunnel:** biến thể `test_the_gateway_stops_routing_to_a_pod_that_is_not_ready` fail khi vLLM ở Kaggle vì nó inject lỗi bằng cách dừng container vLLM local. Kịch bản degraded/recovery vẫn chứng minh đầy đủ ở baseline non-gpu.

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
- Direct API: 100% thành công. Latency bị chi phối bởi `/ready` **probe mọi dependency đồng bộ mỗi lần gọi**; probe vLLM (giờ là tunnel Kaggle) đóng góp đuôi latency ~1 hop qua Cloudflare. Readiness async/cache → p50 tụt sâu dưới 487 ms.
- Latency `/api/v1/ask` với vLLM thật: `llm_ms ≈ 9000`, `total_ms ≈ 9300` (trace J3) — chi phối bởi generation trên T4 + hop tunnel.
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
5. **vLLM inference đẩy sang Kaggle T4 qua tunnel, không chạy local.** Được: verify IP07 bằng vLLM thật dù laptop chỉ có RTX 3050 4 GB (không đủ cho `Qwen3-1.7B` fp16). Mất: phụ thuộc session/quota Kaggle, latency thêm 1 hop, 3 test topology fail (mục 10). `compose.gpu.yaml` cho local vẫn giữ nguyên để chạy được khi có GPU đủ VRAM.

---

## 10. Production gaps — sẽ cải tiến khi triển khai thật

1. **vLLM qua Cloudflare quick tunnel** — IP07 đã verify nhưng endpoint là tunnel tạm: session Kaggle ≤ 12h, quota GPU 30h/tuần, URL `trycloudflare` đổi mỗi lần restart (để `ports.local`, không commit). Hết session = IP07 `not_ready`. Prod: vLLM co-located hoặc named tunnel + service mesh.
2. **3 test fail do topology tunnel** (không phải lỗi code):
   - `j4 ...pod_that_is_not_ready`: J4 dừng container vLLM local để inject lỗi — vLLM remote không tái hiện được.
   - `prometheus ...inference_endpoint_is_scraped`: Prometheus scrape target local `host.docker.internal:8001` → `down` với endpoint Kaggle. Prod: scrape proxy/federation tới endpoint remote.
   - `trace ...spans_the_processes`: đòi ≥4 tên service — vLLM remote không phát span OTLP riêng qua tunnel (span `lab28.vllm.chat_completion` vẫn có, là client span của `api`). Prod: vLLM export OTLP về collector chung.
3. **`LAB28_VLLM_TIMEOUT` mặc định 30s** — không đủ cho endpoint tunnel (RTT laptop→Cloudflare→Kaggle). Đã thêm passthrough vào `compose.yaml` api service + set `120` ở `ports.local`. Prod: timeout theo SLO endpoint thật, thêm retry/circuit-breaker.
4. **`/ready` probe đồng bộ mọi lần gọi** → p50 ~487 ms. Prod: cache kết quả probe (TTL ngắn) hoặc probe nền, `/ready` chỉ đọc trạng thái.
5. **Kafka RF=1, 1 broker; Spark driver 1g** — cấu hình lab. Prod: RF≥3, resource thật, `MAX_ACTIVE_RUNS_PER_DAG` cân theo throughput.
6. **CLI lỗi trên Windows** — `lab28 integration` in JSON có ký tự `→` ra stdout cp1252 → `UnicodeEncodeError`. Workaround: `$env:PYTHONUTF8=1`. Fix thật: CLI ép `sys.stdout` reconfigure UTF-8.
7. **Envoy local rate limit rất thấp** — tốt để demo 429, nhưng che mất số liệu throughput thật của app. Prod: tách limit demo và limit thật.
8. **`evidence/` bị `.gitignore`** — nộp evidence bundle riêng (không commit vào repo).

---

## 11. Điều khó nhất

Bốn hàm starter thì em viết khá nhanh. Chỗ duy nhất phải dừng lại suy nghĩ là `dedupe_latest`: lúc đầu test riêng của nó pass nhưng test Delta lại đỏ, mãi em mới nhận ra mình đang coi `IngestionEvent` như dict trong khi nó là object, và em chỉ so `occurred_at` nên khi hai bản tin trùng thời điểm thì kết quả phụ thuộc vào thứ tự Kafka trả về. Sửa lại thành so cặp `(occurred_at, event_id)` rồi sort theo key thì mới ổn định.

Phần thật sự mất thời gian là dựng full stack và nối vLLM. Máy em mạng yếu nên build image Airflow cứ chết giữa chừng vì pip tải `pyspark` bản nguồn 450 MB không bao giờ xong, tải lại từ đầu mỗi lần. Có lần em lỡ chạy hai tiến trình tải cùng lúc, file phình to hơn cả bản gốc và hỏng luôn. Cuối cùng em phải tải sẵn một lần trên máy có resume rồi cho Dockerfile đọc file local thay vì kéo từ mạng.

Nối vLLM trên Kaggle còn rối hơn. RTX 3050 của em chỉ 4 GB VRAM nên không chạy nổi model, phải đẩy sang GPU T4 của Kaggle. Lúc đó mới dính một chuỗi lệch phiên bản: bản vllm mới kéo về torch dựng cho CUDA 13 trong khi Kaggle đang là CUDA 12, hạ vllm xuống thì lại vỡ ở tokenizer vì thư viện transformers quá mới, phải ghim đúng transformers 4.55.4. T4 cũng cũ nên vllm tự lùi về engine V0. Đến khi endpoint chạy được rồi thì request hỏi đáp vẫn timeout, vì client trong container để mặc định 30 giây, không đủ cho một request đi vòng qua Cloudflare tunnel tới Kaggle rồi mới sinh chữ trên T4; em phải cho compose truyền `LAB28_VLLM_TIMEOUT` vào service api và nâng lên 120 giây.

Điều em rút ra là các tầng phải khớp nhau về phiên bản và cả về độ trễ: torch với CUDA, vllm với transformers, timeout với đường mạng thực tế. Một tầng báo chạy được không có nghĩa là tầng kế tiếp sẽ chạy.

---

## 12. Các vai trò đã đi qua (làm cá nhân)

- **Ingestion & Orchestration** (IP01–IP02): `event_headers`, kiểm DAG `lab28_ingestion_pipeline`, replay/DLQ qua J2.
- **Data & ML** (IP03–IP04–IP06): `dedupe_latest`, `feast_online_request`, `lab28 release` (v1→v3), Delta time travel.
- **Serving & Retrieval** (IP05–IP07): `lab28 index`, grounding path, dựng vLLM 0.10.1.1 thật trên Kaggle T4 + Cloudflare tunnel, verify identity (`/version`, `/v1/models`, 61 metric `vllm:`), chạy J1/J3 với completion thật.
- **Platform & Observability** (IP08–IP10): `readiness_status`, gateway 200/429, Prometheus targets, OTLP trace, validate manifest K8s/GitOps.
- **Presenter / Incident Commander**: evidence pack, kịch bản demo, tường thuật sự cố (mục 5).
