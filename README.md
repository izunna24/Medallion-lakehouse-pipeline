# Pull Request #3 — Performance Tuning: Medallion Architecture Lakehouse Pipeline

**Stack:** PySpark, Delta Lake, Spark UI (via ngrok), `time.time()`, Z-ORDER, `.explain("formatted")`, `.history()`, `.cache()`, explicit tuple-based DQ rules

## Overview

This pull request builds on the correctness-proven pipeline from PR 1 and PR 2 and focuses on **measurable performance tuning**: partitioning strategy, broadcast joins, caching, and skew handling — each verified with real evidence (Spark UI physical plans, Delta transaction history, file counts, and wall-clock timing) rather than assumed.

The pipeline ingests raw daily transactional order data, enforces data quality with an auditable quarantine mechanism, maintains full historical lineage via SCD Type 2, and produces two ready-to-query Gold-layer summary tables.

## Environment & Requirements

- **Frameworks**: `pyspark==4.0.3`, `delta-spark==4.0.0` (auto-resolves `io.delta:delta-spark_2.13:4.0.0`)
- **Execution environment**: Google Colab
- **Monitoring**: `pyngrok` (tunnels the local Spark UI at `http://localhost:4050` to a public URL for physical-plan and job inspection)

## Environment Setup & Monitoring

- Initializes a local `SparkSession` with Delta Lake extensions, Kryo serialization, and Adaptive Query Execution (AQE — `spark.sql.adaptive.enabled`, `skewJoin.enabled`, `coalescePartitions.enabled`) all turned on.
- Tunnels the Spark UI through ngrok for live inspection of jobs, stages, and SQL execution plans during development.
- Calls `spark.stop()` at the top of the pipeline cell (wrapped in `try/except NameError`) before rebuilding the session. Spark's job/stage IDs are counters scoped to the running `SparkContext`, not to a single script execution — without an explicit restart, re-running the same cell just keeps incrementing IDs from the previous run and can carry over warm caches, which would make timing comparisons across runs misleading.

## Bronze Layer (Raw Ingestion)

- Ingests raw CSV batches with **schema-on-read as all strings** — Bronze never rejects or silently coerces raw data, preserving a faithful, replayable record of exactly what arrived.
- Idempotent ingestion via Delta `MERGE` on `(order_id, ingest_date)`.
- Partitioned by `ingest_date`; optimized with `OPTIMIZE ... ZORDER BY order_id`.

## Silver Layer (Data Quality & SCD2)

**Cleansing & casting**: whitespace trimmed on every logic-bearing column (any column that gets cast, compared, joined, or partitioned on), then explicitly typed — `try_cast` on `order_date` converts malformed date strings to `NULL` rather than failing the batch.

**Data Quality — explicit, auditable rules**: each rule is a `(rule_name, condition, description)` tuple, evaluated per row, with failures collected into a `_failed_rules` array column. This was a deliberate change from the `dropDuplicates()` approach used in PR 1/PR 2: `dropDuplicates()` silently removes rows with no record of what was dropped or why, while the explicit rule-based approach quarantines with a fully auditable reason per row — a small overhead traded for production-level traceability. PR 1/PR 2 were left unchanged, since the point of this PR is to show the evolution, not retrofit prior work.

Nine rules are enforced: not-null checks on `order_id`, `customer_id`, `product_id`, `region`, `order_date`; positive-value checks on `quantity` and `unit_price`; a valid-status check (`PLACED`, `PROCESSING`, `SHIPPED`, `DELIVERED`, `CANCELLED`); and an exact-duplicate check (`row_signature_count == 1`) computed by grouping on the **full business-column signature**, not just `order_id` — this is what correctly distinguishes a genuine multi-item order (many rows, same `order_id`, different line-item data) from a true duplicate (an identical row repeated).

**Quarantine**: failing rows are routed to a dedicated Delta path with their `_failed_rules` and `_quarantined_at` timestamp preserved.

**SCD Type 2**: a two-stage MERGE expires the current row (`is_current = false`, `valid_to` set) when a line item's status changes, then inserts the new current version (`is_current = true`, `valid_from` set to `ingest_date`). The merge key is `(order_id, line_item_seq)` — `line_item_seq` is a stable identifier baked into the source data at generation time (not computed via `monotonically_increasing_id()` at read time, which would assign unrelated IDs across separate day1/day2 reads and silently break CDC matching).

Maintained via `OPTIMIZE ... ZORDER BY order_id, region`.

## Gold Layer (Aggregated Business Metrics)

Filters to `is_current = true` only, then computes a per-line `line_value = quantity * unit_price` (summed, not derived from separately-summed `quantity` and `unit_price` — the latter is mathematically wrong for any group with more than one row).

- **`order_summary`** — one row per `order_id`: `line_item_count`, `total_quantity`, `total_order_value`, and `status`. Since a single order can span multiple line items with independent statuses (a CDC update can change some lines but not others), `status` is derived via a **worst-status-wins** rule: each status is ranked (`CANCELLED=0`, `PLACED=1`, `PROCESSING=2`, `SHIPPED=3`, `DELIVERED=4`), the minimum rank is taken per order, then translated back to a label. Partitioned by `order_date`.
- **`region_daily_metrics`** — grouped by `(region, order_date)`: `order_count` (`count(distinct order_id)`, not row count, so multi-item orders aren't overcounted), `total_revenue`, `avg_order_value`.

## Performance Tuning — Verified Evidence

### Broadcast join: confirmed, not assumed
`explain("formatted")` was run on the Silver DQ join and inspected in the Spark UI SQL tab. The physical plan shows `BroadcastHashJoin LeftOuter BuildRight` with a `BroadcastExchange` on the small side — confirming Spark actually chose the broadcast strategy rather than a full shuffle join.

### Adaptive Query Execution: active and doing real work
The Silver CDC-insert explain output shows `AQEShuffleRead ... Arguments: coalesced` — AQE adaptively coalescing shuffle partitions at runtime based on actual data size, not static configuration.

### Small-files fix: `region_daily_metrics`
`region_daily_metrics` has only 40 rows total. Partitioning it by `order_date` (10 distinct dates) crossed with 4 regions produced a textbook small-files problem:

| | Files | Size |
|---|---|---|
| Before (partitioned by `order_date`) | 10 | 17.2 KiB |
| After (no partitioning) | 1 | 2.9 KiB |

Removing the partition column collapsed the table to a single file with no loss of query capability, given the table's size.

### Z-ORDER consolidation: Silver
`OPTIMIZE ... ZORDER BY order_id, region` on Silver after both MERGE operations: **15 files removed → 10 files added**, confirmed via `.history()` (`OPTIMIZE` operation metrics).

### Skew handling — measured, not assumed

Eight `order_id`s were deliberately engineered as multi-item orders (300–640 line items each, generated with varying `product_id`/`quantity`/`unit_price` per line — genuine line items, not exact duplicates) so the pipeline would have real imbalanced keys to audit, rather than discussing skew only in the abstract. This required moving off `monotonically_increasing_id()` in favor of a source-baked `line_item_seq` for the SCD2 merge key, since Spark-computed IDs aren't stable across the separate day1/day2 reads that CDC needs to match against.

A per-column skew audit (`indicate_skew()`) runs on the full Silver table, computing `max/avg` ratio and top-N heaviest keys for every column. Results from an actual run:

| Column | Max count | Avg count | Skew ratio | Flag |
|---|---|---|---|---|
| `order_id` | 640 | 1.18 | 541.9x | HIGH |
| `customer_id` | 652 | 11.08 | 58.9x | HIGH |
| `line_item_seq` | 85,194 | 197.27 | 431.9x | HIGH |
| `product_id` | 221 | 177.50 | 1.2x | OK |
| `unit_price` | 15 | 3.77 | 4.0x | MODERATE |

Reading these ratios at face value would suggest three columns need salting. They don't — the ratio alone is misleading without two further checks:

**Absolute row count, not just ratio, determines whether skew is actually a problem.** `order_id 8419`, the heaviest key at 640 rows, is still small enough that a single task processes it in a few milliseconds — task-scheduling overhead alone is often larger than the cost of 640 rows of simple data. Salting exists to fix tasks that take meaningfully longer than the rest of the stage; at this scale, none do. A high ratio on a small absolute count is a candidate worth watching if the dataset grows, not a present bottleneck.

**A column's skew is only relevant if that column is actually used as a shuffle/join/groupBy key in the stage being measured.** `line_item_seq`'s 431.9x ratio is a structural artifact, not a hot key: every order's first line item is numbered 1, so the value `1` is guaranteed to be the most common value in the column regardless of any real imbalance (85,194 of 88,574 rows are single-item orders). `line_item_seq` is never a standalone shuffle key anywhere in this pipeline — Silver's SCD2 merge uses it only as part of the composite key `(order_id, line_item_seq)`, where `order_id` already differentiates most rows, and neither Gold aggregation (`order_summary` groups by `order_id, customer_id, region, order_date`; `region_daily_metrics` groups by `region, order_date, order_id`) references it at all. Its ratio is real but not actionable — a case for reading skew audits with the join/groupBy keys in view, not evaluating a column in isolation.

`customer_id`'s 58.9x ratio (max 652, from `customer_id 7007`) sits in the same category as `order_id`: real but small in absolute terms, and not used as a shuffle key in this pipeline's current aggregations.

**Conclusion**: no salting was applied. At this dataset's scale, AQE's built-in `spark.sql.adaptive.skewJoin.enabled` is sufficient to absorb the imbalance that does exist, and the audit gives a concrete, reproducible basis for that decision rather than a guess — plus a clear trigger (absolute row counts climbing into the thousands+ range on a real shuffle key) for when salting would become worth revisiting.

### Timing — captured, isolated to real work only

Each checkpoint below times only the actual layer function call (`bronze_layer()`, the DQ/merge/aggregation logic) — not the `.show()`, `.count()`, `.explain()`, or `.history()` diagnostic calls that surround them in the notebook, since those are inspection overhead added for reviewability, not pipeline work.

```
Bronze layer:            36.18s   (was 47.20s with diagnostics included)
Silver DQ + quarantine:  61.78s   (was 70.07s with diagnostics included)
Silver MERGE (SCD2):     38.98s   (was 47.17s with diagnostics included)
Gold aggregation:        24.49s   (was 36.22s with diagnostics included)
Total (isolated):       161.43s
Total (with diagnostics): 200.66s
```

Isolating each stage to just its real work consistently reduced every checkpoint, confirming that a meaningful share of the earlier, diagnostic-inflated numbers was genuinely coming from `.show()`, `.count()`, `.explain()`, and `.history()` calls rather than pipeline work. Silver DQ remains the most expensive stage even in isolation, consistent with it doing the most real work: a self-join on an 8-column broadcast key plus 9 rule evaluations per row across both day1 and day2.

## Verified Chain (from an actual run)

| Stage | Row count |
|---|---|
| Bronze day1 raw | 63,126 |
| Bronze day2 raw | 27,384 |
| Quarantine (day1) | 65 |
| Quarantine (day2) | 65 |
| Silver (post SCD2 MERGE) | 88,574 |
| Gold `order_summary` | 75,000 |
| Gold `region_daily_metrics` | 40 |

Each file's 65 quarantined rows were deliberately injected and span every DQ rule: 5 null `order_id`, 5 null `customer_id`, 5 null `product_id`, 10 non-positive `quantity`, 10 non-positive `unit_price`, 5 null `region`, 10 invalid `status` values, 5 null `order_date`, and 5 exact-duplicate row pairs (10 rows) — confirming the Gatekeeper catches every category it's designed for, not just the ones that happen to occur naturally.

## Verification & Testing

- **Execution logging**: `explain_and_log()` wraps key DataFrames to print `df.explain("formatted")`, cross-referenced against the Spark UI SQL tab.
- **Table health audits**: file counts, byte sizes, and full Delta transaction history (`.history()`) printed for every layer at the end of the run.

## Running the Pipeline

This pipeline was developed and run as a Google Colab notebook, not a standalone script. To reproduce:

1. Upload `orders.csv` and `orders_day2.csv` to `data/raw/` in the Colab environment.
2. Run the notebook cells top to bottom.
3. Open the printed ngrok URL to inspect the Spark UI during/after the run.
