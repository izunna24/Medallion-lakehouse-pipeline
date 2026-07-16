
# PR #1
PySpark Delta Lake pipeline progression from simple (PR #1) to advanced

**Notebook:** Google Colab (also runs locally — see Requirements below)

## Requirements
pip install pyspark==4.0.3 delta-spark==4.0.0

The script pulls the matching Delta JAR (`delta-spark_2.13:4.0.0`) automatically via
`spark.jars.packages` on first run — no manual JAR download needed.

## Data source
Sample data is committed at `data/raw/orders.csv` — anyone cloning this repo can
run the pipeline immediately, no external file access required.

## Run it
python medallion/pipeline/pr1_medallion.py


## 1. Ingestion Cleanup & Workspace Setup
Integrated dynamic cleaning logic utilizing `shutil.rmtree` at
runtime to clear old stale tables, neutralizing `AnalysisException`
errors caused by schema drift.

## 2. Bronze Layer (Raw Ingestion)
Ingests the raw orders CSV.
Enriches the records by adding ingestion metadata
(`ingest_date`, `source_filename`, `ingest_at`).
Performs an upsert (Delta Merge) on composite keys
(`order_id` + `source_filename`) to support idempotency — re-running
the pipeline against the same source file does not create duplicate rows.

## 3. Silver Layer (Cleanse & Quality Gate Routing)
Implements casting and strict schema validation
(e.g., using `try_cast` on dates to map garbage values to `NULL` rather
than silently dropping or erroring on bad input).

Deploys three **independent** rule-check flags:
`is_conflicting`, `is_invalid_quantity`, `is_invalid_date`.

**The Gatekeeper Pattern:**
- **Good data:** formatted, trimmed, and merged into `data/silver/orders/` by unique `order_id`.
- **Bad data:** routed into a dedicated quarantine path `data/quarantine/orders/`
  with automated, concatenated `quarantine_reason` tags (e.g. `invalid_quantity+invalid_date`).

**Note on ordering:** DQ flags are evaluated *before* deduplication. This means
quarantine counts reflect raw violations prior to dedup, not post-dedup counts —
deduplication should never mask or reduce visibility into rows that failed a
validity check.

**Known scope limitation (deferred to a later PR):** `is_conflicting` currently
flags rows sharing an `order_id` with genuinely differing business values. These
rows are quarantined rather than auto-resolved — deciding which version is
"correct" is intentionally left to human review, not the pipeline.

## 4. Gold Layer (Business Aggregations)
Computes analytical outputs from the cleansed Silver layer:
- **Aggregate 1 (Revenue Analysis):** Daily total revenue per Product ID (`order_date`, `product_id`).
- **Aggregate 2 (Operations KPI):** Total order counts grouped by transaction lifecycle status.

## Verification and Operational Metrics
Running the pipeline demonstrates operational accountability:
- **Null check integrity:** explicit validation of null counts across all Silver columns.
- **Audit footprints:** row counts are logged and verified to reconcile across every stage.

Example verified chain from a real run:

Raw CSV rows:        312
Quarantined rows:     15   (10 invalid_quantity/date + 5 with an overlap in reason)
Silver rows:         281   (312 - 15 quarantined - remaining dedup/null drops)
Gold (status counts): 76 (shipped) + 59 (cancelled) + 76 (delivered) + 70 (placed) = 281

Gold aggregates sum back exactly to the Silver row count — confirming the
pipeline is lossless: every row is either present in Silver, or explicitly
quarantined with a reason, never silently dropped.
