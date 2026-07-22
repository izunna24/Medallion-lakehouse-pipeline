
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

PR #2:

Medallion Architecture Pipeline with Data Quality Gatekeeper & SCD Type 2 History

## Overview
This PR implements an end-to-end Medallion Architecture data pipeline using
PySpark (4.0.3) and Delta Lake (4.0.0). The pipeline ingests multi-day raw e-commerce
orders, enforces schema integrity, isolates data anomalies using a
**Gatekeeper Pattern**, Delta Lake History(Transaction Log), tracks order lifecycle status updates via
**Slowly Changing Dimensions (SCD) Type 2**, and surfaces audit-ready
historical logs in the Gold layer.

## Technical Features & Architectural Highlights

### 1. Bronze Layer: Ingestion Cleanup & Idempotency
- **Runtime Path Cleansing**: Dynamically resets local testing paths
 (`shutil.rmtree`) to prevent schema drift issues during local iterations.
- **Lineage Metadata**: Enriches landing CSV records with operational
columns (`ingest_date`, `source_filename`).
- **Idempotency**: Utilizes Delta Merge operations based on composite keys
 (`order_id` + `source_filename`) to guarantee safe re-runs without record duplication.

### 2. Silver Layer: Data Quality Gatekeeper Pattern
- **Type Safety**: Enforces schema constraints using `try_cast` for dates to trap
malformed values as `NULL` without breaking batch processing.
- **Independent Anomaly Detection**: Evaluates multi-flag validation rules
 (`is_conflicting`, `is_invalid_quantity`, `is_invalid_date`).
- **Quarantine Routing**: Automatically diverts invalid records into `data/quarantine/orders/`
with concatenated, audit-friendly `quarantine_reason` tags (e.g., `invalid_quantity+invalid_date`).
- **Pre-Deduplication Evaluation**: Data Quality flags are evaluated *prior* to
deduplication to ensure raw anomaly counts are fully preserved and accounted for.

### 3. SCD Type 2 Merge Strategy
- **Two-Step State Updates**:
  1. Expire existing records (`is_current = false`, set `valid_to = current_timestamp()`)
  when a status update is detected.
  2. Insert updated status records as active
   (`is_current = true`, `valid_from = current_timestamp()`, `valid_to = NULL`).
- Unchanged orders bypass updates, preserving state without generating redundant version rows.

### 4. Gold Layer: History Audit Log
- Extracts full lifecycle status transitions for orders experiencing changes
into `data/gold/scd/orders`.
- Confirms state consistency: each updated order reflects exactly two state records
 (1 historical closed version + 1 active current version).

## Example verified chain from a real run:
 day1 raw data content : 312 rows
 day2 raw data content : 38 rows
 day2 manifest(used to verify successful merge) : 38 rows
 Quarantined_path1 :  15 rows
 Quarantined_path2 :  0 row (clean data)
 rows with history : 20 history + 20 current = 40 rows
 order_id mismatch_count != 2 on rows with history = 0 row
 

## Environment & Requirements
- **Frameworks**: `pyspark==4.0.3`, `delta-spark==4.0.0`
- **Execution Environment**: Tested on Google Colab and local Spark runtime.
- **Delta Package Dependency**: Auto-resolves `io.delta:delta-spark_2.13:4.0.0`.

## Execution Command
To run the full end-to-end pipeline locally:
python medallion/pipeline/pr2_medallion.py
