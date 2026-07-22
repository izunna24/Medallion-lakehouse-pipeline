
# Import core Spark components, Delta Lake integration, and standard utility modules.
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from datetime import datetime

# Clean up any stale directory paths on the local file system.
# This prevents schema drift/conflicts when testing changes to the pipeline.
import shutil
shutil.rmtree("data/bronze/scd/orders1/", ignore_errors=True)
shutil.rmtree("data/bronze/scd/orders_day2/", ignore_errors=True)
shutil.rmtree("data/silver/scd/orders/", ignore_errors=True)
shutil.rmtree("data/quarantine/orders/1/", ignore_errors=True)
shutil.rmtree("data/quarantine/orders/2/", ignore_errors=True)
shutil.rmtree("data/gold/scd/orders/", ignore_errors=True)

print("Local stale data cleared!")

# Initialize SparkSession with Delta Lake package configurations and extensions.
spark = SparkSession.builder \
    .appName("medallion_arch") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.ui.port", "4050") \
    .getOrCreate()

# Suppress excessively verbose logs; show warning and error logs only.
spark.sparkContext.setLogLevel("WARN")

# Quick Delta Lake connectivity test
spark.range(5).write.format("delta").mode("overwrite").save("/tmp/delata-test")
print("*** delta write is successful ***")

# Define the path to the raw input CSV file, relative to the repo root, so
# this script can be cloned and run by anyone without needing Google Drive access.
csv_path1 = "data/raw/orders.csv"
csv_path2 = "data/raw/orders_day2.csv"
day2_manifest = "data/raw/day2_manifest.csv"

# Load the raw CSV datasets with headers enabled.
df1 = spark.read.option("header", "true").csv(csv_path1)
day2_df = spark.read.option("header", "true").csv(csv_path2)
manifest_df = spark.read.option("header", "true").csv(day2_manifest)

# verify row count
print(df1.count())
print(day2_df.count())
print(manifest_df.count())
print("*** orders read is successful ***")

# Print inferred raw schemas to verify initial column structural types
df1.printSchema()
day2_df.printSchema()

# Display sample rows to preview landing data content.
df1.show(5)
day2_df.show(5)


# === Bronze Layer (Raw Ingestion & Lineage Enrichment) ====

# Define target directory for the 2 raw Bronze Delta Tables.
bronze_path1 = "data/bronze/scd/orders1/"
bronze_path2 = "data/bronze/scd/orders_day2/"

def bronze_day1(source_day1, target_path1):
  # Enriched Day 1 CSV batch with timestamp and ingestion file origin metadata.
  enriched_day1 = source_day1.withColumn("ingest_date", to_date(current_timestamp())) \
                           .withColumn("source_filename", input_file_name())

  # Upsert data using Delta Merge if the target Delta Table already exists.
  if DeltaTable.isDeltaTable(spark, target_path1):
        delta_table = DeltaTable.forPath(spark, target_path1)
        delta_table.alias("target").merge(
            enriched_day1.alias("source"),
            """target.order_id = source.order_id
            AND target.source_filename = source.source_filename"""
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

  # Initialize the Delta Table on the very first run.
  else:
      enriched_day1.write.format("delta").mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("ingest_date").save(target_path1)


def bronze_day2(source_day2, target_path2):
  # Enriched Day 2 CSV batch with timestamp and ingestion file origin metadata.
  enriched_day2 = source_day2.withColumn("ingest_date", to_date(current_timestamp())) \
                           .withColumn("source_filename", input_file_name())

  # Upsert data using Delta Merge if the target Delta Table already exists.
  if DeltaTable.isDeltaTable(spark, target_path2):
        delta_table = DeltaTable.forPath(spark, target_path2)
        delta_table.alias("target").merge(
            enriched_day2.alias("source"),
            """target.order_id = source.order_id
            AND target.source_filename = source.source_filename"""
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

  # Initialize the Delta Table on the very first run.
  else:
    enriched_day2.write.format("delta").mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("ingest_date").save(target_path2)
  print("Bronze day2 Delta history:")


# Trigger Bronze ingestion processes for Day 1 and Day 2 datasets
bronze_day1(df1, bronze_path1)
bronze_day2(day2_df, bronze_path2)
print("Bronze day1 Delta history:")
DeltaTable.forPath(spark, bronze_path1).history().show(truncate=False)
print("Bronze day2 Delta history:")
DeltaTable.forPath(spark, bronze_path2).history().show(truncate=False)

print(f"csv:{csv_path1} has been written to delta:{bronze_path1}")
print(f"csv:{csv_path2} has been written to delta:{bronze_path2}")

# Read landed Bronze tables back into DataFrames to evaluate raw outputs.
bronze_df1 = spark.read.format("delta").load(bronze_path1)
bronze_df2 = spark.read.format("delta").load(bronze_path2)
bronze_df1.show(5, truncate=False)
bronze_df2.show(5, truncate=False)

# === Silver Layer (Type Casting, Data Quality Gatekeeper & SCD Type 2 Merge) ===

# Target directory destinations for refined Silver data and quarantine targets.
silver_path = "data/silver/scd/orders/"
quarantine_path1 = "data/quarantine/orders/1/"
quarantine_path2 = "data/quarantine/orders/2/"

from pyspark.sql.types import *

# Explicit type casting and schema standardization for Day 1
silver_day1 = bronze_df1.select(
    col("order_id").cast(StringType()),
    col("customer_id").cast(StringType()),
    col("order_date").try_cast(DateType()),  # covert malformed date formats to NULLs
    col("product_id").cast(StringType()),
    col("quantity").cast(IntegerType()),
    col("unit_price").cast(DoubleType()),
    col("status").cast(StringType()),
    col("payment_method").cast(StringType()),
    col("ingest_date").cast(DateType()),
    col("source_filename").cast(StringType()),
)

# Explicit type casting and schema standardization for Day 2
silver_day2 = bronze_df2.select(
    col("order_id").cast(StringType()),
    col("customer_id").cast(StringType()),
    col("order_date").try_cast(DateType()),  # covert malformed date formats to NULLs
    col("product_id").cast(StringType()),
    col("quantity").cast(IntegerType()),
    col("unit_price").cast(DoubleType()),
    col("status").cast(StringType()),
    col("payment_method").cast(StringType()),
    col("ingest_date").cast(DateType()),
    col("source_filename").cast(StringType()),
)

# Run quick audit check across all schema columns for day 2
silver_day2.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in day2_df.columns
    ]).show()

silver_day2.filter(col("quantity") <= 0).show()

silver_day2.groupBy("order_id") \
        .agg(count("*").alias("order_count")) \
        .filter(col("order_count") > 1) \
        .show()


def silver_layer(scd_day1, scd_day2, merged_path, dup_path1, dup_path2):
    # Define primary business key columns and unique transactional event
    business_cols = ["order_id", "customer_id", "order_date", "product_id",
                      "quantity", "unit_price", "status", "payment_method"]

    # -- Day 1 Data Quality Processing ---
    # Calculate order duplication frequency and distinct row signature counts.
    summary_day1 = scd_day1.groupBy("order_id").agg(
        count("*").alias("dup_count"),
        count_distinct(struct(*business_cols)).alias("distinct_row_count")
    )

    # Join duplication metrics back onto the main Day 1  using a broadcast join.
    join_day1 = scd_day1.join(broadcast(summary_day1), on="order_id", how="left")

   # Evaluate three independent flags to locate data anomalies.
    flagged_day1 = join_day1.withColumn(
        "is_conflicting",
        (col("dup_count") > 1) & (col("distinct_row_count") > 1)
    ).withColumn(
            "is_invalid_date",
        col("order_date").isNull()
    ).withColumn(
        "is_invalid_quantity",
        col("quantity").isNull() | (col("quantity") <= 0)
    )

    # Good records extracted
    good_day1 = flagged_day1.filter(
        (~col("is_conflicting")) & (~col("is_invalid_quantity")) & (~col("is_invalid_date"))
    ).dropDuplicates(business_cols) \
     .drop("dup_count", "distinct_row_count", "is_conflicting", "is_invalid_quantity", "is_invalid_date")

    # Bad records -> Quarantine
    bad_day1 = flagged_day1.filter(
        col("is_conflicting") | col("is_invalid_quantity") | col("is_invalid_date")
    ).withColumn(
        "quarantine_reason",
        when(col("is_conflicting") & col("is_invalid_quantity") & col("is_invalid_date"),
             lit("conflicting_order_id+invalid_quantity+invalid_date"))
        .when(col("is_conflicting") & col("is_invalid_quantity"),
             lit("conflicting_order_id+invalid_quantity"))
        .when(col("is_conflicting") & col("is_invalid_date"),
             lit("conflicting_order_id+invalid_date"))
        .when(col("is_invalid_quantity") & col("is_invalid_date"),
             lit("invalid_quantity+invalid_date"))
        .when(col("is_conflicting"), lit("conflicting_order_id"))
        .when(col("is_invalid_quantity"), lit("invalid_quantity"))
        .when(col("is_invalid_date"), lit("invalid_date"))
        .otherwise(lit("unknown"))
    ).withColumn("quarantine_at", current_timestamp()) \
     .drop("dup_count", "distinct_row_count", "is_conflicting", "is_invalid_quantity", "is_invalid_date")
    bad_day1.show()

    # Route quarantined records to the designated Day 1 quarantine location
    if bad_day1.count() > 0:
        bad_day1.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("ingest_date") \
            .save(dup_path1)
        print(f"Quarantined {bad_day1.count()} rows to {dup_path1}")

    # Standardize and save clean Day 1 records with initial SCD Type 2 tracking attributes.
    if good_day1.count() > 0:
        clean_day1 = good_day1.withColumn("status", lower(trim(col("status")))) \
            .withColumn("payment_method", lower(trim(col("payment_method")))) \
            .dropDuplicates() \
            .dropna(subset=["quantity", "unit_price"]) \
            .withColumn("payment_method",
                        when(col("payment_method").isNull(), lit("unknown"))
                        .otherwise(col("payment_method"))) \
                        .withColumn("is_current",     lit(True)) \
                        .withColumn("valid_from", col("ingest_date").cast(DateType())) \
                        .withColumn("valid_to", lit(None).cast("timestamp"))

        clean_day1.write.format("delta").mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("ingest_date").save(merged_path)

    # --- Day 2 Data Quality Processing ---
    # Calculate order duplication frequency and distinct row signature counts for Day 2.
    summary_day2 = scd_day2.groupBy("order_id").agg(
        count("*").alias("dup_count"),
        count_distinct(struct(*business_cols)).alias("distinct_row_count")
    )

    # Join duplication metrics back onto the main Day 2
    join_day2 = scd_day2.join(broadcast(summary_day2), on="order_id", how="left")

   # Evaluate three independent flags to locate data anomalies.
    flagged_day2 = join_day2.withColumn(
        "is_conflicting",
        (col("dup_count") > 1) & (col("distinct_row_count") > 1)
    ).withColumn(
            "is_invalid_date",
        col("order_date").isNull()
    ).withColumn(
        "is_invalid_quantity",
        col("quantity").isNull() | (col("quantity") <= 0)
    )

    # Good records extracted
    good_day2 = flagged_day2.filter(
        (~col("is_conflicting")) & (~col("is_invalid_quantity")) & (~col("is_invalid_date"))
    ).dropDuplicates(business_cols) \
     .drop("dup_count", "distinct_row_count", "is_conflicting", "is_invalid_quantity", "is_invalid_date")

    # Bad records -> Quarantine
    bad_day2 = flagged_day2.filter(
        col("is_conflicting") | col("is_invalid_quantity") | col("is_invalid_date")
    ).withColumn(
        "quarantine_reason",
        when(col("is_conflicting") & col("is_invalid_quantity") & col("is_invalid_date"),
             lit("conflicting_order_id+invalid_quantity+invalid_date"))
        .when(col("is_conflicting") & col("is_invalid_quantity"),
             lit("conflicting_order_id+invalid_quantity"))
        .when(col("is_conflicting") & col("is_invalid_date"),
             lit("conflicting_order_id+invalid_date"))
        .when(col("is_invalid_quantity") & col("is_invalid_date"),
             lit("invalid_quantity+invalid_date"))
        .when(col("is_conflicting"), lit("conflicting_order_id"))
        .when(col("is_invalid_quantity"), lit("invalid_quantity"))
        .when(col("is_invalid_date"), lit("invalid_date"))
        .otherwise(lit("unknown"))
    ).withColumn("quarantine_at", current_timestamp()) \
     .drop("dup_count", "distinct_row_count", "is_conflicting", "is_invalid_quantity", "is_invalid_date")
    bad_day2.show()

    # Route quarantined records to the designated Day 2 quarantine location
    if bad_day2.count() > 0:
        bad_day2.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("ingest_date") \
            .save(dup_path2)
        print(f"Quarantined {bad_day2.count()} rows to {dup_path2}")

    # Standardize and save clean Day 2 records.
    if good_day2.count() > 0:
        clean_day2 = good_day2.withColumn("status", lower(trim(col("status")))) \
            .withColumn("payment_method", lower(trim(col("payment_method")))) \
            .dropDuplicates() \
            .dropna(subset=["quantity", "unit_price"]) \
            .withColumn("payment_method",
                        when(col("payment_method").isNull(), lit("unknown"))
                        .otherwise(col("payment_method")))

        # Merge updates if the target Silver Delta table already exists.
        if DeltaTable.isDeltaTable(spark, merged_path):
            target = DeltaTable.forPath(spark, merged_path)

            # Step 1: Expire (soft-delete) the active version for orders whose status changed.
            target.alias("tgt").merge(
                clean_day2.alias("src"),
                condition="""
                    tgt.order_id = src.order_id
                    AND tgt.is_current = true
                    AND tgt.status != src.status
                """
            ).whenMatchedUpdate(set={
                "is_current": lit(False),
                "valid_to": current_timestamp()
            }).execute()

            # # Step 2: Insert the new updated state as the current active version
            new_rows = clean_day2.withColumn("is_current", lit(True)) \
                                 .withColumn("valid_from", current_timestamp()) \
                                 .withColumn("valid_to", lit(None).cast("timestamp"))

            target.alias("tgt").merge(
                new_rows.alias("src"),
                condition="tgt.order_id = src.order_id AND tgt.is_current = true"
            ).whenNotMatchedInsert(values={
                "order_id": "src.order_id",
                "customer_id": "src.customer_id",
                "order_date": "src.order_date",
                "product_id": "src.product_id",
                "quantity": "src.quantity",
                "payment_method": "src.payment_method",
                "status": "src.status",
                "unit_price": "src.unit_price",
                "ingest_date": "src.ingest_date",
                "source_filename": "src.source_filename",
                "is_current": "src.is_current",
                "valid_from": "src.valid_from",
                "valid_to": "src.valid_to"
            }).execute()

        else:
            # Append Day 2 dataset if no Day 1 table was previously initialized.
            clean_day2.withColumn("is_current", lit(True)) \
                      .withColumn("valid_from", col("ingest_date").cast("timestamp")) \
                      .withColumn("valid_to", lit(None).cast("timestamp")) \
                      .write.format("delta").mode("append") \
                      .option("mergeSchema", "true") \
                      .partitionBy("ingest_date").save(merged_path)


# Execute the Silver transformation pipeline function.
silver_layer(silver_day1, silver_day2, silver_path, quarantine_path1, quarantine_path2)
print("silver Delta history:")
DeltaTable.forPath(spark, silver_path).history().show(truncate=False)

silver_df = spark.read.format("delta").load(silver_path) \
              .orderBy("valid_from")

print(f"content of{quarantine_path2}")
quarantine_df2 = spark.read.format("delta").load(quarantine_path1).show(5)
silver_df.show(truncate=False)

# Audit check: Verify dynamic SCD Type 2 history tracking on a specific order.
silver_df.filter(col("order_id") == "ORD10200").show(truncate=False)
print(silver_df.filter(col("valid_to").isNotNull()).count())

# Report summary metrics across current and historical records.
print("Total Silver rows:", silver_df.count())
print("Current rows (is_current=true):", silver_df.filter(col("is_current") == True).count())
print("Historical rows (is_current=false):", silver_df.filter(col("is_current") == False).count())

# Reconcile historical transitions against expected manifest references.
historical_ids = set(row["order_id"] for row in silver_df.filter(col("is_current") == False).select("order_id").collect())
manifest_progressed = set(row["order_id"] for row in spark.read.option("header","true") \
          .csv(day2_manifest).filter(col("expected_category") == 
          "progressed_status_should_create_new_scd2_version").select("order_id").collect())
print("Match:", historical_ids == manifest_progressed)
print("Missing from historical:", manifest_progressed - historical_ids)
print("Unexpected in historical:", historical_ids - manifest_progressed)

# Reconcile unchanged order behaviors against manifest references.
manifest_unchanged = set(row["order_id"] for row in spark.read.option("header","true")
    .csv(day2_manifest)
    .filter(col("expected_category") == "unchanged_status_should_NOT_create_new_version")
    .select("order_id").collect())
print("Unchanged orders that WRONGLY created history:", manifest_unchanged & historical_ids)


# === Gold Layer (SCD Type 2 Audit History & Status Change Aggregations) ===

gold_path = "data/gold/scd/orders"

# Enrich DataFrame with Gold layer processing timestamp.
gold_df = silver_df.withColumn("gold_at", current_timestamp())

def gold_layer(history_df, history_path):
  historical_ids = history_df.filter(col("is_current") == False) \
    .select("order_id").distinct()

  # Inner join: keep only orders that DO appear in historical_ids
  status_change_log = history_df.join(
    broadcast(historical_ids),
    on="order_id",
    how="inner"
  )

  status_change_log.write \
  .format("delta").mode("overwrite") \
  .option("overwriteSchema", "true").save(history_path)

  status_change_log.orderBy("order_id", "valid_to").show(truncate=False)
  print(f"Total history rows: {status_change_log.count()}")

# Execute Gold layer workflow.
gold_layer(gold_df, gold_path)

print("gold Delta history:")
DeltaTable.forPath(spark, gold_path).history().show(truncate=False)

# Verify that all historical entries maintain two state records (expired + active).
my_gold_df = spark.read.format("delta").load(gold_path)
mismatch_count = my_gold_df.groupBy("order_id").count().filter(col("count") != 2).count()
print(f"Orders with incorrect version count (should be 0): {mismatch_count}")
