
# Import core Spark components, Delta Lake integration, and standard utility modules.
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from datetime import datetime

# Clean up any stale directory paths on the local file system.
# This prevents schema drift/conflicts when testing changes to the pipeline.
import shutil
shutil.rmtree("data/bronze/orders/", ignore_errors=True)
shutil.rmtree("data/silver/orders/", ignore_errors=True)
shutil.rmtree("data/quarantine/orders/", ignore_errors=True)
shutil.rmtree("data/gold/orders/1/", ignore_errors=True)
shutil.rmtree("data/gold/orders/2/", ignore_errors=True)

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
csv_path = "data/raw/orders.csv"
df = spark.read.option("header", "true") \
               .csv(csv_path)

print("*** orders read is successfully ***")
df.printSchema()
df.show(5)

# ============ Bronze Layer ============
# Define target directory for the raw Bronze Delta Table.
bronze_path = "data/bronze/orders/"

def bronze_layer(source_df, target_path):
  # Enriched DataFrame with timestamp and ingestion file origin metadata.
  enriched_df = source_df.withColumn("ingest_date", to_date(current_timestamp())) \
                           .withColumn("source_filename", input_file_name()) \
                           .withColumn("ingest_at", current_timestamp())

  # Upsert data using Delta Merge if the target Delta Table already exists.
  if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)
        delta_table.alias("target").merge(
            enriched_df.alias("source"),
            """target.order_id = source.order_id
            AND target.source_filename = source.source_filename"""
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

  # Initialize the Delta Table on the very first run.
  else:
      enriched_df.write.format("delta").mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("ingest_date").save(target_path)

# Execute the Bronze pipeline stage.
bronze_layer(df, bronze_path)
print(f"csv:{csv_path} has been written to delta:{bronze_path}")

# Load and inspect the written Bronze Delta dataset.
bronze_df = spark.read.format("delta").load(bronze_path)
bronze_df.show(5, truncate=False)

# ============ Silver Layer ============
# Set directory destinations for the refined Silver and data quality Quarantine areas.
silver_path = "data/silver/orders/"
quarantine_path = "data/quarantine/orders/"

from pyspark.sql.types import *

# Type casting and basic schema enforcement
silver_df = bronze_df.select(
    col("order_id").cast(StringType()),
    col("customer_id").cast(StringType()),
    col("order_date").try_cast(DateType()),  # covert garbage values to NULLs
    col("product_id").cast(StringType()),
    col("quantity").cast(IntegerType()),
    col("unit_price").cast(DoubleType()),
    col("status").cast(StringType()),
    col("payment_method").cast(StringType()),
    col("ingest_date").cast(DateType()),
    col("source_filename").cast(StringType()),
    col("ingest_at").cast(TimestampType())
)

silver_df.printSchema()

# Run a quick audit check displaying null counts across all schema columns.
silver_df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]).show()

def silver_layer(transform_df, clean_path, dup_path):
    # Define primary business key parameters and unique transactional event
    business_cols = ["order_id", "customer_id", "order_date", "product_id",
                      "quantity", "unit_price", "status", "payment_method"]

    # Detect duplicates and conflicts
    summary_df = transform_df.groupBy("order_id").agg(
        count("*").alias("dup_count"),
        count_distinct(struct(*business_cols)).alias("distinct_row_count")
    )

    # Rejoin metrics back to main stream.
    join_df = transform_df.join(broadcast(summary_df), on="order_id", how="left")

   # Evaluate three independent flags to locate data anomalies.
    flagged_df = join_df.withColumn(
        "is_conflicting",
        (col("dup_count") > 1) & (col("distinct_row_count") > 1)
    ).withColumn(
        "is_invalid_quantity",
        col("quantity").isNull() | (col("quantity") <= 0)
    ).withColumn(
        "is_invalid_date",
        col("order_date").isNull()
    )

    # Good records extracted
    good_df = flagged_df.filter(
        (~col("is_conflicting")) & (~col("is_invalid_quantity")) & (~col("is_invalid_date"))
    ).dropDuplicates(business_cols) \
     .drop("dup_count", "distinct_row_count", "is_conflicting", "is_invalid_quantity", "is_invalid_date")

    # Bad records -> Quarantine
    bad_df = flagged_df.filter(
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

    # Write anomalies to the quarantine zone for business review.
    if bad_df.count() > 0:
        bad_df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .partitionBy("ingest_date") \
            .save(dup_path)
        print(f"Quarantined {bad_df.count()} rows to {dup_path}")

    # Standardize and upsert clean records to the Silver layer.
    if good_df.count() > 0:
        clean_df = good_df.withColumn("status", lower(trim(col("status")))) \
            .withColumn("payment_method", lower(trim(col("payment_method")))) \
            .dropDuplicates() \
            .dropna(subset=["quantity", "unit_price"]) \
            .withColumn("payment_method",
                        when(col("payment_method").isNull(), lit("unknown"))
                        .otherwise(col("payment_method")))

        # Upsert: If the target Silver Delta table already exists
        if DeltaTable.isDeltaTable(spark, clean_path):
            delta_table = DeltaTable.forPath(spark, clean_path)
            delta_table.alias("target").merge(
                clean_df.alias("source"),
                "target.order_id = source.order_id"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            # Create a new Silver partition on order_date if no pre-existing table is found.
            clean_df.write.format("delta").partitionBy("order_date") \
                .mode("append").option("mergeSchema", "true") \
                .save(clean_path)

# Execute the Silver pipeline stage.
silver_layer(silver_df, silver_path, quarantine_path)
print(f"delta:{bronze_path} has been written to delta:{silver_path}")

# Load and audit the processed Silver layer.
my_silver_df = spark.read.format("delta").load(silver_path)
my_silver_df.show(5, truncate=False)

my_silver_df.select([
        sum(when(col(b).isNull(), 1).otherwise(0)).alias(b)
        for b in df.columns
    ]).show()

# Output total record counts across all processing stages to verify processing consistency.
print("Raw CSV rows:", df.count())
print("Bronze rows:", spark.read.format("delta").load(bronze_path).count())
print("Silver rows:", spark.read.format("delta").load(silver_path).count())
print("Quarantine rows:", spark.read.format("delta").load(quarantine_path).count())

# ============ Gold Layer ============
# Define target paths for Gold aggregate targets
gold_path_revenue = "data/gold/orders/1/"
gold_path_status_counts = "data/gold/orders/2/"

# Append processing timestamp context metadata to current dataset.
gold_df = my_silver_df.withColumn("gold_at", current_timestamp())

def gold_layer(agg_df, gold_pathA, gold_pathB):
  # Daily revenue by date
  agg_df.withColumn("daily_rev", round(coalesce(col("quantity"), lit(0))
                            * coalesce(col("unit_price"), lit(0)), 2)) \
        .groupBy("order_date", "product_id") \
        .agg(sum(col("daily_rev")).alias("daily_sum")) \
             .write.format("delta").mode("overwrite") \
             .option("overwriteSchema", "true") \
             .save(gold_pathA)

  # Order count by status
  agg_df.groupBy("status") \
        .agg(count(col("order_id")).alias("order_count")) \
             .write.format("delta").mode("overwrite") \
             .option("overwriteSchema", "true") \
             .save(gold_pathB)

# Execute the Gold pipeline stage.
gold_layer(gold_df, gold_path_revenue, gold_path_status_counts)
print(f"delta:{silver_path} has been written to delta:{gold_path_revenue}")
print(f"delta:{silver_path} has been written to delta:{gold_path_status_counts}")

# Load and display finalized business analytical Gold views.
my_gold_df1 = spark.read.format("delta").load(gold_path_revenue)
my_gold_df2 = spark.read.format("delta").load(gold_path_status_counts)

my_gold_df1.show(5, truncate=False)
my_gold_df2.show(truncate=False)

print("Gold rows No.1:", my_gold_df1.count())
print("Gold rows No.2:", my_gold_df2.count())
