# Medallion Lakehouse Pipeline — PR #4
## Structured Streaming, SCD Type 2, Skew-Aware Aggregation, and MLOps

**Stack:** PySpark, Delta Lake, Spark Structured Streaming, MLflow, Spark UI (via ngrok)

### Overview

This pull request extends the medallion pipeline from PR1–PR3 with three capabilities that
weren't present before: an ingestion and change-data-capture path built on **Structured
Streaming**, a **salted two-stage aggregation** to defeat a deliberately engineered 300x+
customer skew, and an **MLOps layer** (RandomForestRegressor, MLflow tracking) that trains on
historical Gold data and scores new incoming orders.

The central design decision running through this PR: **streaming is used only where the
computation is genuinely incremental** (Bronze ingestion, Silver's SCD2 merge). Gold's
customer-lifetime rollup and the ML training/scoring step are **full-dataset batch
computations by design** — they need to see every row to be correct, so they read a
completed Delta snapshot rather than reacting to a live stream. Applying streaming uniformly
across every layer was tried and reverted after it produced a real correctness bug (see
"Bugs found and fixed" below); the final architecture reflects that lesson.

---

### Data

Three synthetic CSVs (Bronze sources), generated with fixed seed:

| File | Rows | Shape |
|---|---|---|
| `orders_day1_skewed.csv` | 200,005 | Day-1 bulk load. 5 VIP `customer_id`s deliberately dominate (~40% of orders) — the skew source for Gold. |
| `orders_day2.csv` | 70,005 | Day-2 CDC arrival: ~30% status-progression updates to day-1 orders, ~70% brand-new orders. No VIP customers — skew is isolated to the historical bulk load, not the incremental stream. |
| `orders_prediction.csv` | 10,002 | Unlabeled new-arrival data scored by the trained model. |

Each file also carries deliberately injected DQ violations (nulls, non-positive
quantity/price, invalid status, exact-duplicate row pairs) spanning every rule the Silver
DQ layer checks, so the quarantine path is exercised with real evidence rather than staying
empty.

---

### Bronze Layer — Structured Streaming Ingestion

- `spark.readStream.format("csv")`, schema-on-read (all `StringType` — Bronze never
  coerces or rejects raw data), `maxFilesPerTrigger=1`, `trigger(availableNow=True)`.
- `foreachBatch` hands each micro-batch to `process_batch()` as a plain, static DataFrame —
  the mechanism every downstream `MERGE`/DQ operation in this pipeline depends on. Structured
  Streaming's file source discovers new files and reduces each increment to an ordinary batch
  DataFrame; nothing inside `foreachBatch` is streaming-aware.
- First batch: plain `write().mode("append")`. Subsequent batches: `MERGE` on
  `(order_id, order_date)`.
- Z-ORDER (`order_id`) runs **once, after `awaitTermination()`** — not per batch (see bug #3).

Row counts landed exactly matching source files (200,005 / 70,005 / 10,002) across every run.

---

### Silver Layer — DQ Quarantine + SCD Type 2 (Structured Streaming)

**DQ:** nine explicit `(rule_name, condition, description)` rules — not-null checks,
positive-value checks, valid-status check, and an exact-duplicate check via
`row_signature_count == 1` computed over the full business-column signature. DQ runs as a
**batch** step against Bronze's output before anything touches `readStream` again, since
window functions (`Window.partitionBy(...).count()`) are not supported on genuine streaming
DataFrames — they only run correctly against the static DataFrame `foreachBatch` provides.

Quarantine counts, verified against the injected violations:

| Source | Good rows | Quarantined |
|---|---|---|
| Day 1 | 199,940 | 65 |
| Day 2 | 69,940 | 65 |
| Prediction | 9,976 | 26 |

**SCD2:** DQ-passed rows are staged to a Delta table (one file per `order_date`, forced via
`.coalesce(1)`), then consumed by a second streaming query
(`readStream.format("delta")`, `maxFilesPerTrigger=1`) whose `foreachBatch` runs the
two-step `MERGE`:
1. **Expire** — `whenMatchedUpdate` sets `is_current=false`, `valid_to=current_timestamp()`
   where `status` changed.
2. **Insert** — `whenNotMatchedInsertAll` adds the new current version.

**Correctness proof, not assumption:** naive expectation was
`199,940 (day1) + 69,940 (day2) = 269,880` rows in Silver. Actual: **260,316** — a gap of
9,564. Verified via Delta time travel (`versionAsOf` the pre-day2 snapshot) that exactly
9,564 day-2 rows had a status identical to the existing current row — correctly classified
as "matched, no insert" by `whenNotMatchedInsertAll`, not lost or duplicated. Arithmetic and
row-level evidence agree exactly.

---

### Gold Layer — Salted Aggregation (Batch)

`indicate_skew()` (adapted from PR3's `indicate_skew`) measured `customer_id` at
**315.8x–411.1x skew ratio** (5 VIP customers, ~16,000 rows each vs. an average of ~52) —
well past the "salt this" threshold. Two-stage salted aggregation:

```python
salted_customer_id = customer_id + "_" + floor(rand() * 10)   # only for the 5 VIP keys
# Stage 1: partial aggregates per salted key (spreads hot rows across 10 synthetic buckets)
# Stage 2: re-group by real customer_id, sum the 10 partials back into one row
```

**Bucket count (10, not 100) was a measured decision, not a guess.** With the VIP max at
16,423 rows, 10 buckets yields ~1,643 rows/bucket — small enough that task-scheduling
overhead already dominates processing cost, matching the same "absolute row count over
ratio" lesson from PR3's skew audit. Raising to 100 buckets would fragment every *normal*
customer's already-small row count for no measured benefit, while increasing the total
number of shuffle keys Stage 1 has to track.

Gold correctly computes `order_count` (`count(distinct order_id)`, not row count — avoids
overcounting multi-item orders), `total_order_value`, `avg_order_value`, and `primary_region`
(mode region per customer via a ranked window).

---

### MLOps Layer

**Model:** `RandomForestRegressor` (100 trees, max depth 10), predicting `total_order_value`
from `order_count`, `total_quantity`, `avg_order_value`, and one-hot-encoded `primary_region`.
MLflow tracks the run and logs the pipeline model.

**Two fixes were required to get a trustworthy result, addressing two separate failure modes:**

1. **Stratified split.** With only 5 VIP-scale rows in ~5,000 total, a plain
   `randomSplit` risked a catastrophic-outlier row landing in the 20% test set — and did,
   producing `R² = -78.22` (worse than predicting the mean). Fix: VIP rows are always routed
   to training (`union`), never into the held-out test set, since there's too little VIP data
   for a meaningful held-out evaluation of that segment anyway.
2. **Log-transform the target.** `total_order_value` is heavily right-skewed. Training on
   `log1p(total_order_value)` and inverting with `expm1` at prediction time compresses the
   scale the model has to fit.

**Result after both fixes: RMSE = 3,147.19, R² = 0.97**, confirmed identically on a
normal-customer-only holdout slice — the two fixes address different problems
(evaluation-set variance vs. target-distribution skew) and both were needed.

The trained model then scores `orders_prediction.csv` (no retraining) after being run through
the identical feature-engineering path used for training.

---

### Bugs found and fixed during development

Documented here because they were instructive, not just corrected in place.

1. **`pathGlobFilter` scoped to a full path instead of a filename** — silently discovered
   zero files, so `foreachBatch` never ran even once; `"Bronze Stream is successful"` printed
   anyway because zero batches isn't an error. Root cause of a `PATH_NOT_FOUND` two steps
   later when reading a Delta table that had never been created.
2. **Streaming file source requires a directory, not a single file** —
   `Option 'basePath' must be a directory`. Fixed by loading the parent directory and
   narrowing with `pathGlobFilter` (the two options work together, not redundantly).
3. **Z-ORDER placed inside `foreachBatch` instead of after `awaitTermination()`** — rewrote
   the *entire* Silver table on every one of 13 micro-batches (O(n²)-shaped cost). Moving it
   outside the batch function cut Silver's runtime from **341.72s to 158.46s (−54%)**, with
   `Silver Count: 260316` unchanged — proof the fix was purely a performance change, not a
   correctness one.
4. **Gold's `.write().mode("overwrite")` running inside `foreachBatch`** — a full-dataset
   rollup was (incorrectly) implemented as a streaming aggregation, so every micro-batch's
   partial result overwrote the *entire* Gold table; only the last batch processed survived.
   This silently fed the ML model a non-representative training set. Fixed by making Gold a
   plain batch computation over the complete Silver snapshot — the correct shape for a
   "sum everything ever, per customer" calculation, which cannot be correctly computed
   incrementally one arrival at a time.
5. **`quarantine()`'s print statement fired regardless of whether the write executed** —
   masked that zero-violation runs were writing nothing, and a copy-paste bug elsewhere
   loaded `quarantine_path2` twice instead of `quarantine_ml_path`.

---

### Performance evidence (measured, not assumed)

| Layer | Run time | Notes |
|---|---|---|
| Bronze | ~48–50s | 3 streaming ingests, `availableNow` |
| Silver (DQ) | ~57–67s | batch, includes quarantine writes |
| Silver (SCD2 MERGE) | **158.46s** (was 341.72s before fix #3) | 13 micro-batches, Z-ORDER once |
| Gold (salted agg) | ~32–36s | batch, full 260,316-row snapshot |
| MLOps | ~90–118s | train + evaluate + score |

Executor sizing was tuned for an 8GB / 6-core laptop (`local[6]`, `driver.memory=3g`,
`shuffle.partitions=12`) — deliberately smaller than PR3's defaults to reflect the actual
development environment; absolute run times will vary with network conditions (ngrok
tunnel) and background load, so relative before/after comparisons (bug #3) are the more
meaningful evidence than absolute wall-clock numbers alone.

---

### File counts after Z-ORDER

| Table | Files | Size |
|---|---|---|
| Bronze (day1) | 6 | 2,765.0 KiB |
| Bronze (day2) | 9 | 1,085.1 KiB |
| Bronze (prediction) | 3 | 155.3 KiB |
| Silver | 8 | 3,516.5 KiB |
| Gold | 5 | 101.7 KiB |
| ML scored output | 5 | 134.7 KiB |

---

### What this demonstrates

- Structured Streaming's `foreachBatch` mechanism, and why it's the only place `MERGE`,
  window functions, and other batch-only operations can run inside a streaming pipeline.
- SCD Type 2 implemented and *proven* correct via Delta time-travel arithmetic, not just a
  "the count looks right" assertion.
- Skew diagnosis and mitigation backed by a measured threshold (absolute row count, not
  raw ratio) — the same discipline as PR3, applied to a new mechanism (salting instead of
  broadcast joins).
- Recognizing when NOT to stream: a full-dataset aggregation forced into a streaming shape
  produced a real, silent correctness bug: catching and fixing it, and being able to explain
  why the fix was structural rather than a tuning knob, is itself part of the evidence this
  pipeline is trying to provide.
