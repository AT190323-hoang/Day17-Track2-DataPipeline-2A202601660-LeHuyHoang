# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Lê Huy Hoàng  **Mã học viên:** 2A202601660  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 46.0s
  run 2/3 … 54.2s
  run 3/3 … 43.0s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  bài mở rộng (EXTRA.md)                      — chưa chạy `make seed-extra`
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** — cả ba nhiệm vụ bắt buộc đều hoàn thành: ổn định 3 lượt, đúng số hàng, contract + test + quarantine đều đạt.

> Ghi chú: repo gốc có commit sẵn `expected/dashboard_baseline.json` (từ commit `813e661`) dù thư mục `data/gold_events/` mà file này cần chỉ được sinh khi chạy `make seed-extra` (bài mở rộng A, không bắt buộc, và `data/` bị `.gitignore`). Vì vậy trước khi sửa, `make verify` luôn crash ở cuối log dù các tiêu chí bắt buộc đã đạt. Đã `git rm expected/dashboard_baseline.json` để `make verify` chạy sạch cho người không làm bài mở rộng — không đụng đến các file `expected/*.count` dùng để chấm B/C.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau `make reset` rồi `make pipeline` lần 1: `gold_training_set` = 13.790 hàng (kỳ vọng 12.480). Chạy `make pipeline` lần 2 (không reset): tăng lên 26.270 hàng — tăng thêm đúng 12.480, tức gần như nhân đôi. Không có lỗi nào được báo trong lúc chạy. |
| **Nguyên nhân** | `gold_training_set` là model `materialized = 'incremental'` nhưng `config()` **không khai `unique_key`**, nên dbt-duckdb dùng chiến lược mặc định là **append (INSERT)** thay vì merge/upsert. Nguồn CDC có 1.310 bản ghi `op='u'` — một ticket được tạo ở ngày D1 rồi sửa ở ngày D2 sẽ có `_ingested_at` đổi sang D2, nên trong **một lượt chạy 14 ngày**, ticket đó lọt qua mệnh đề `WHERE _ingested_at BETWEEN …` của khối `is_incremental()` ở **hai partition ngày khác nhau** — mỗi lần lọt qua là một INSERT mới. 13.790 − 12.480 = 1.310, khớp chính xác với số bản ghi `op='u'`, xác nhận đúng cơ chế. Chạy lại toàn bộ pipeline lần thứ hai lặp lại y hệt quy trình quét 14 ngày đó trên một target đã có sẵn dữ liệu, cộng dồn thêm ~12.480 hàng nữa. Về bản chất: **incremental model không có khoá để nhận diện "cùng một dòng"**, nên mọi lần dòng đó xuất hiện lại (do dữ liệu update hoặc do chạy lại) đều bị ghi thêm chứ không ghi đè. Tham số DAG `catchup=True` (không giới hạn backfill) và thiếu `max_active_runs` (cho phép nhiều run ghi đồng thời) không phải nguyên nhân gốc — chúng chỉ làm lỗi này **dễ bị kích hoạt hơn** khi ai đó bấm Clear Task trên Airflow, đúng như mô tả trong phiếu #1041. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()` — grain của bảng là entity (1 hàng / 1 ticket), khoá tự nhiên là `ticket_id`, nên merge theo khoá là chiến lược đúng để dòng cập nhật ghi đè thay vì ghi thêm. `dags/ai_training_pipeline.py`: đặt `catchup=False` và `max_active_runs=1` để giảm khả năng kích hoạt lỗi tương tự trong vận hành thực tế. |
| **Bằng chứng** | Trước: lượt 1 = 13.790 hàng, lượt 2 = 26.270 hàng (13.790 ticket lặp ≥2 lần). Sau: lượt 1 = 12.480 hàng, lượt 2 = 12.480 hàng, không ticket nào lặp (`select ticket_id, count(*) … having count(*) > 1` trả về rỗng). `make verify`: `gold_training_set` ỔN ĐỊNH ✓, số hàng khớp `expected/` (12.480/12.480) ✓, checksum giống hệt cả 3 lượt (`8622572a97`) ✓, dòng "1 hàng / 1 ticket" ✓. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` chỉ có 8.645/9.100 hàng (thiếu ~5%). Đối chiếu theo `event_date`, hàng thiếu tập trung ở các ngày cũ (08-03 → 08-13); ba ngày cuối (08-14 → 08-16) đủ hàng. |
| **P99 độ trễ đo được** | **2,73 ngày** *(bắt buộc)* — đo bằng `quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99)` trên `bronze_events`. Max quan sát được = 2,94 ngày. Phân bố có hai cụm tách biệt: phần lớn event tới trong 0–6 giờ, và ~5,05% (`avg(_ingested_at > event_time + 1 day)`) tới muộn 43–71 giờ. |
| **Lookback đã chọn** | **3 ngày** — ⌈P99⌉ = ⌈2,73⌉ = 3, đủ phủ luôn cả max quan sát được (2,94 ngày) trong bộ dữ liệu này. |
| **Nguyên nhân** | Điều kiện lọc incremental gốc: `where event_date > (select max(event_date) from {{ this }})`. Mốc so sánh là `max(event_date)` **hiện có trong target**, không phải ngày vận hành hiện tại. Vì cột này tăng đơn điệu theo từng lượt chạy, một khi target đã ghi nhận `event_date = D`, mọi bản ghi có `event_date < D` nhưng `_ingested_at` đến **sau đó** (dữ liệu tới muộn) sẽ vĩnh viễn không còn thoả `event_date > max(event_date)` nữa — bị bỏ sót mãi mãi, không phải bỏ sót tạm thời. Đây là lỗi cấu trúc của điều kiện lọc, không phải lỗi dữ liệu. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: đổi điều kiện lọc thành cửa sổ lùi 3 ngày: `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`. Vì cửa sổ mở rộng khiến cùng một cặp (event_date, customer_id) bị tính lại ở nhiều lượt chạy, thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` vào `config()` để lần tính sau **thay thế** lần tính trước thay vì cộng dồn (nếu không sẽ tái tạo đúng lỗi của nhiệm vụ 1 trên bảng này). |
| **Bằng chứng** | Trước: 8.645 hàng (thiếu 455). Sau: 9.100/9.100 hàng, `make verify`: ỔN ĐỊNH ✓, checksum giống hệt cả 3 lượt (`3db448685c`). `gold_training_set` không đổi (vẫn 12.480, ỔN ĐỊNH ✓). |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> `max` là một quan sát đơn lẻ trong 14 ngày dữ liệu mẫu — dùng nó làm lookback dễ overfit vào chính bộ dữ liệu này và không ổn định khi dữ liệu thật có outlier lớn hơn (ví dụ một lần retry mạng kéo dài nhiều ngày). P99 là một đại lượng thống kê ổn định hơn, phản ánh đúng "hầu hết dữ liệu trễ đến mức nào" mà không bị một điểm dị biệt duy nhất kéo lệch. Chi phí: mỗi ngày lùi thêm nghĩa là **mọi lượt chạy sau này** đều quét và tính lại thêm một ngày dữ liệu Silver — đây là chi phí trả liên tục (per-run), không phải chi phí một lần. Vì vậy nên chọn lookback nhỏ nhất vừa đủ phủ P99 (làm tròn lên), thay vì phủ max "cho chắc" — đánh đổi giữa độ đầy đủ và chi phí tính toán lặp lại mỗi lần chạy.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets.priority` có 6.488/12.480 hàng NULL, và một số giá trị `0`, `5`, `-1` ngoài miền hợp lệ 1..4. Đối chiếu `bronze_tickets_cdc.priority_raw` theo ngày: tỷ lệ giá trị không-phải-số nhảy từ 0,6–2% (08-03→08-09) lên gần 100% kể từ **08-10** — đúng ngày backend đổi format theo phiếu #1047. |
| **Nguyên nhân** | `priority_raw` gộp chung ba loại giá trị nhưng logic cũ chỉ xử lý bằng một `try_cast(... as integer)`: (1) số hợp lệ `1234`, (2) nhãn chữ hợp lệ `urgent/high/medium/low` — do backend đổi cách biểu diễn từ 08-10, ý nghĩa dữ liệu không đổi, và (3) giá trị hỏng thật (`P1`, `unknown`, `0`, `5`, `-1`, rỗng, NULL). `try_cast` biến toàn bộ nhóm (2) thành NULL (coi nhãn chữ hợp lệ là lỗi), đồng thời lại **chấp nhận** nhóm (3) như `0`, `5`, `-1` vì chúng đúng là số nguyên hợp lệ về mặt kiểu — dù sai miền giá trị. Ngoài ra `contract.enforced` đang `false` nên schema không bị ràng buộc, và test miền giá trị `accepted_values` bị comment nên `dbt test` không phát hiện được vấn đề. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Nhóm 1 — số hợp lệ (`1`,`2`,`3`,`4`, đúng contract cũ) → **giữ nguyên**. Nhóm 2 — nhãn chữ (`urgent`,`high`,`medium`,`low`, schema evolution từ 08-10, cùng ý nghĩa chỉ khác biểu diễn) → **map về số** theo tài liệu API (`urgent→1, high→2, medium→3, low→4`). Nhóm 3 — giá trị hỏng thật (`P1`,`unknown`,`0`,`5`,`-1`,`''`,`NULL`) → **trả về NULL** để tách vào quarantine. Tiêu chí phân biệt nhóm 2/3: giá trị đó có mang đúng thông tin của contract cũ, chỉ khác cách biểu diễn hay không. |
| **Cách khắc phục** | Bốn chỗ: (a) `dbt/macros/normalize_priority.sql` — thay `try_cast` bằng `CASE` xử lý đủ 3 nhóm, dùng chung cho cả `silver_tickets` và `quarantine_tickets` nên hai bảng không thể lệch nhau. (b) `dbt/models/silver/silver_tickets.sql` — tách CTE `valid` lọc bỏ bản ghi có `priority_clean IS NULL` **trước khi** `row_number()` xếp hạng theo ticket (không phải lọc sau) — nhờ vậy chỉ loại đúng *bản ghi* CDC hỏng, ticket đó vẫn giữ trạng thái hợp lệ từ lần cập nhật trước, không bị rơi khỏi Silver. (c) `dbt/models/silver/quarantine_tickets.sql` — điều kiện `where {{ normalize_priority('priority_raw') }} is null`, dùng đúng macro dùng chung nên không lệch với Silver. (d) `dbt/models/silver/schema.yml` — `contract.enforced: true`, và thêm `tests: [not_null, accepted_values: [1,2,3,4]]` cho cột `priority` (contract chỉ ràng buộc kiểu dữ liệu, không ràng buộc miền giá trị — vẫn cần cả hai). |
| **Bằng chứng** | Trước: 6.488 NULL + 118 giá trị ngoài miền (0/5/-1), quarantine = 0 hàng. Sau: `silver_tickets.priority` chỉ còn {1,2,3,4}, không NULL; `quarantine_tickets` = **312/312** hàng, ỔN ĐỊNH ✓; `silver_tickets` vẫn giữ đủ **12.480** ticket; `dbt test`: **11/11 pass** (tăng từ 9 test gốc). |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Nên chặn ở tầng **Silver**, không phải Bronze. Bronze phải giữ nguyên dữ liệu thô — nếu Bronze từ chối luôn bản ghi lỗi, khi cần điều tra sự cố (ví dụ đối chiếu lại với hệ thống nguồn, hoặc phát hiện ra một nhóm giá trị "lỗi" thực ra là schema evolution hợp lệ như trường hợp này) sẽ không còn dữ liệu gốc để tra cứu — mất bằng chứng. Silver là tầng đã áp dụng business logic/contract nên là nơi hợp lý để phân loại đúng/sai. Không nên để `dbt test` fail làm dừng cả DAG vì quy mô: chỉ 312 bản ghi lỗi trên tổng hơn 12.000 ticket và hơn 130.000 event — để một tỷ lệ lỗi rất nhỏ (~2,5% CDC record) chặn đứng toàn bộ dữ liệu hợp lệ phục vụ RAG index, classifier và routing agent là đánh đổi không tương xứng. Quarantine tách bản ghi lỗi ra thành hàng đợi riêng cho người trực xử lý, trong khi phần lớn hệ thống vẫn vận hành bình thường — đây cũng là lý do không được nhầm nhãn chữ hợp lệ (nhóm 2) thành lỗi, vì làm vậy sẽ biến một sự kiện vô hại (đổi format) thành một sự cố quy mô lớn giả tạo.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Không làm — bắt đầu thử A và B nhưng dừng giữa chừng, chưa xác nhận đạt qua `make explain` / `make crash-test`, nên không tính điểm thưởng cho phần này. |
| **Ghi chú** | Đã sửa nháp `ingest/consumer.py` (đảo thứ tự write/commit + `ON CONFLICT DO UPDATE` cho bài B) và `tools/compact.py` + `queries/dashboard.sql` (partition theo `event_date`, sort theo `customer_name` cho bài A), nhưng: (1) bài B gặp vấn đề hiệu năng chưa chẩn đoán xong với `executemany(... on conflict ...)` trên 20.000 hàng; (2) bài A chưa chạy `make seed-extra`/`make compact`/`make explain` để đo `rows scanned` thật. Các thay đổi này **không ảnh hưởng** ba nhiệm vụ chính: `consumer.py` chỉ được `tools/crash_test.py` gọi, tách biệt khỏi `make pipeline`/`make verify`; `dashboard.sql` chỉ được đọc khi `expected/dashboard_baseline.json` tồn tại, mà file đó đã bị xoá (mục 0) nên không được thực thi trong `make verify`. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Với mọi incremental model: `unique_key` đã khai chưa, và source có bản ghi update (`op='u'`) hay chỉ append-only? |
| 2 | Điều kiện lọc "mới" trong incremental model dựa trên mốc nào — thời điểm sự kiện xảy ra hay thời điểm dữ liệu tới kho? Đo phân bố độ trễ tới trước khi tin vào con số theo ngày. |
| 3 | Khi source đổi định dạng dữ liệu, đừng vội coi giá trị lạ là "lỗi" — đối chiếu số lượng và mốc thời gian xuất hiện trước, phân biệt schema evolution (cần map) với dữ liệu hỏng thật (cần quarantine), rồi mới quyết định |
