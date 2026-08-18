
# {Import core Spark components,
# Delta Lake integration,
# and standard utility modules.}
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.types import *
from datetime import datetime
from delta import configure_spark_with_delta_pip
import getpass, time, os,  shutil
from pyngrok import ngrok

# --- Stop any existing Spark session first, so job/stage counters
#     and cached state genuinely reset for a clean before/after run ---
try:
    spark.stop()
    print("Previous Spark session stopped.")
except NameError:
    print("No previous Spark session found — starting fresh.")

# Clean up any stale directory paths on the local file system.
# This prevents schema drift/conflicts when testing changes to the pipeline.
paths_to_clean = [
    "/content/lakehouse/bronze/1/orders",
    "/content/lakehouse/bronze/2/orders",
    "/content/lakehouse/silver/1/orders_quarantine",
    "/content/lakehouse/silver/2/orders_quarantine",
    "/content/lakehouse/silver/orders",
    "/content/lakehouse/gold/order_summary",
    "/content/lakehouse/gold/region_daily_metrics",
]

for p in paths_to_clean:
    shutil.rmtree(p, ignore_errors=True)
print("Local stale data cleared!")


builder = (
    SparkSession.builder
    .appName("MedallionLakehousePipeline")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.ui.port", "4050")
    # ---- extra useful configs ----
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.files.maxPartitionBytes", "128m")
    .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    .config("spark.databricks.delta.autoCompact.enabled", "true")
)
spark = configure_spark_with_delta_pip(
    builder, extra_packages=["io.delta:delta-spark_2.13:4.0.0"]
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Check if SparkSession exists and what UI port it is using
try:
    print("Spark UI port:", spark.sparkContext.getConf().get("spark.ui.port"))
    print("Spark UI enabled:", spark.sparkContext.getConf().get("spark.ui.enabled", "true"))
    print("SparkSession is active")
except Exception as e:
    print("SparkSession is NOT active:", e)

# 1. Gracefully disconnect every active tunnel

ngrok.kill()
time.sleep(1)
print("ngrok initial tunnels killed.")


authtoken = getpass.getpass()
ngrok.set_auth_token(authtoken)

public_url = ngrok.connect(4050).public_url

print(f"\nSpark UI: http://localhost:4050")
print("-" * 10)
print(f"👉 Click this link to open your Spark UI: \n{public_url}")
print("-" * 10)
print("Keep this cell running while you use the Spark UI.")

print(f"Spark UI: http://localhost:4050")

bronze_path1 = "/content/lakehouse/bronze/1/orders"
bronze_path2 = "/content/lakehouse/bronze/2/orders"
quarantine_path1 = "/content/lakehouse/silver/1/orders_quarantine"
quarantine_path2 = "/content/lakehouse/silver/2/orders_quarantine"
silver_path = "/content/lakehouse/silver/orders"
gold_order_path = "/content/lakehouse/gold/order_summary"
gold_region_path = "/content/lakehouse/gold/region_daily_metrics"

raw_day1 = "data/raw/orders.csv"
raw_day2 = "data/raw/orders_day2.csv"

def explain_and_log(df, name: str):
    print(f"\n===== EXPLAIN: {name} =====")
    df.explain("formatted")
    print(f"→ Check Spark UI (SQL tab) for '{name}'\n")

# Bronze Layer
def bronze_layer(raw_path: str, ingest_path: str):
    """Read a raw CSV batch and idempotently MERGE it into the Bronze Delta table."""
    raw_df = (
        spark.read.option("header", True)
        .csv(raw_path)
        .withColumn("_source_file", lit(os.path.basename(raw_path)))
    )

    raw_df.printSchema()

    if not DeltaTable.isDeltaTable(spark, ingest_path):
        (
            raw_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("ingest_date")
            .save(ingest_path)
        )

    else:
      bronze_tbl = DeltaTable.forPath(spark, ingest_path)
      (
        bronze_tbl.alias("target")
        .merge(
            raw_df.alias("source"),
            "target.order_id = source.order_id AND target.ingest_date = source.ingest_date",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Bronze: merged {raw_path}")

    # Optional Z-ORDER (helps later reads)
    DeltaTable.forPath(spark, ingest_path).optimize().executeZOrderBy("order_id")

t0 = time.time()
bronze_layer(raw_day1, bronze_path1)
bronze_layer(raw_day2, bronze_path2)
t1 = time.time()
print(f"Bronze layer Run_time: {t1 - t0:.2f}s")

bronze_day1 = spark.read.format("delta").load(bronze_path1)
print(f"Bronze Day1 total rows: {bronze_day1.count():,}")
bronze_day1.show(5, truncate=False)

bronze_day2 = spark.read.format("delta").load(bronze_path2)
print(f"Bronze Day2 total rows: {bronze_day2.count():,}")
bronze_day2.show(5, truncate=False)
print("Bronze day1 Delta history:")
DeltaTable.forPath(spark, bronze_path1).history().show(truncate=False)
print("Bronze day2 Delta history:")
DeltaTable.forPath(spark, bronze_path2).history().show(truncate=False)



# Silver Layer

def schema_def(ingest_df):
  # Audit for not trimed values
  string_cols = [
    f.name for f in ingest_df.schema.fields
    if isinstance(f.dataType, StringType)
  ]

  # Count of rows that have leading or trailing spaces per string column
  whitespace_counts_df = ingest_df.select([
    sum(
        when(length(col(c)) != length(trim(col(c))), 1).otherwise(0)
    ).alias(c)
    for c in string_cols
  ])

  print("whitespace content")
  whitespace_counts_df.show()

  # trim() before casting explicit schema
  for c in ingest_df.columns:
    ingest_df = ingest_df.withColumn(c, trim(col(c)))
  print("all columns trimed successfully")

  # Explicit type casting and schema standardization
  schema_set = ingest_df.select(
    col("order_id").cast(StringType()),
    col("customer_id").cast(StringType()),
    col("product_id").cast(StringType()),
    col("quantity").cast(IntegerType()),
    col("unit_price").cast(DoubleType()),
    col("region").cast(StringType()),
    col("status").cast(StringType()),
    col("order_date").try_cast(DateType()),  # convert malformed date formats to NULLs
    col("line_item_seq").cast(IntegerType()),
    col("ingest_date").cast(DateType()),
    col("_source_file").cast(StringType()),
  )

  schema_set.printSchema()

  # Audit for null values
  schema_set.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in schema_set.columns
    ]).show()

  # Audit for numerics less than and/or equal zero
  numeric_types = (IntegerType, DoubleType)

  # 1. Collect numeric column names (small metadata list – unavoidable)
  numeric_cols = [
    field.name
    for field in schema_set.schema.fields
    if isinstance(field.dataType, numeric_types)
  ]

  print("Numeric columns:", numeric_cols)

  # One-row DataFrame with the count of ≤ 0 values per numeric column
  counts_df = schema_set.select([
    sum(when(col(c) <= 0, 1).otherwise(0)).alias(c)
    for c in numeric_cols
  ])

  counts_df.show()

  # EXPLAIN
  explain_and_log(schema_set, "schema_def result")

  return schema_set

bronze_day1.cache()
bronze_day2.cache()
schema_def(bronze_day1)
schema_def(bronze_day2)
bronze_day1.unpersist()
bronze_day2.unpersist()

def quality_checks(df):
    # Define primary business key columns and unique transactional event
    business_cols = ["order_id", "customer_id", "product_id", "quantity",
                      "unit_price", "region", "status", "order_date",]


    # Calculate order duplication frequency and distinct row signature counts.
    summary = df.groupBy(*business_cols).agg(
        count("*").alias("row_signature_count"),
    )

    # Join duplication metrics using a broadcast join.
    join_df = df.join(broadcast(summary), on=business_cols, how="left")

    #Explain
    explain_and_log(join_df, "quality_checks – after broadcast join")

    # Each rule is: (rule_name, condition_column_expr, description)
    q_rules= [
    (
        "order_id_not_null",
        col("order_id").isNotNull(),
        "order_id must not be null"
    ),
    (
         "customer_id_not_null",
        col("customer_id").isNotNull(),
        "customer_id must not be null"
    ),
    (
         "product_id_not_null",
        col("product_id").isNotNull(),
        "product_id must not be null"
    ),
    (
        "quantity_greater_than_zero_&_not_null",
        col("quantity").isNotNull() & (col("quantity") > 0),
        "quantity must be greatter than zero and not null"
    ),
    (
        "unit_price_greater_than_zero_&_not_null",
        col("unit_price").isNotNull() & (col("unit_price") > 0),
        "unit_price must be greatter than zero and not null"
    ),
    (
         "region_not_null",
        col("region").isNotNull(),
        "region must not be null"
    ),
    (
        "status_valid",
        col("status").isin("PLACED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"),
        "Status must be PLACED, PROCESSING, SHIPPED, DELIVERED or CANCELLED"
    ),
    (
        "order_date_not_null",
        col("order_date").isNotNull(),
        "order_date must not be null"
    ),
    (
        "row_signature_count_equals_1",
        (col("row_signature_count") == 1),
        "row_signature_count must be 1"
    ),
    ]

    # Build a column for each rule — True if passed, False if failed
    df_checked = join_df
    for rule_name, condition, _ in q_rules:
        df_checked = df_checked.withColumn(
            f"_check_{rule_name}",
            when(condition, lit(True)).otherwise(lit(False))
        )

    # Collect names of failed checks into an array column
    failed_checks_col = array(*[
        when(col(f"_check_{rule_name}") == False, lit(rule_name))
        .otherwise(lit(None))
        for rule_name, _, _ in q_rules
    ])

    df_checked = df_checked.withColumn(
        "_failed_rules_raw", failed_checks_col
    )

    # Filter nulls out of the array to get only failed rule names
    from pyspark.sql.functions import array_compact
    df_checked = df_checked.withColumn(
        "_failed_rules",
        array_compact(col("_failed_rules_raw"))
    )

    # Drop intermediate columns
    check_cols = [f"_check_{r}" for r, _, _ in q_rules] + ["_failed_rules_raw"]
    df_checked = df_checked.drop(*check_cols)

    # Split into good and bad
    from pyspark.sql.functions import size
    good_df = df_checked \
        .filter(size(col("_failed_rules")) == 0) \
        .drop("_failed_rules", "row_signature_count")

    good_df.show(5, truncate=False)

    bad_df = df_checked \
        .filter(size(col("_failed_rules")) > 0) \
        .withColumn("_quarantined_at", current_timestamp()) \
        .drop("row_signature_count")

    bad_df.show(5)

    #Explain
    explain_and_log(good_df, "good_df (passed all rules)")

    return good_df, bad_df

t2 = time.time()
check_day1 = schema_def(bronze_day1).cache()
check_day2 = schema_def(bronze_day2).cache()

good_day1, bad_day1 = quality_checks(check_day1)
good_day2, bad_day2 = quality_checks(check_day2)


check_day1.unpersist()
check_day2.unpersist()

good_day1.show(5, truncate=False)
bad_day1.show(5, truncate=False)
good_day2.show(5, truncate=False)
bad_day2.show(5, truncate=False)

def quarantine(qua_df, qua_path):
  if qua_df.limit(1).count() > 0:
    qua_df.write.format("delta") \
    .mode("append").option("mergeSchema", "true") \
    .partitionBy("order_date") \
    .save(qua_path)
  print(f"quarantine writen to {qua_path}")

quarantine(bad_day1, quarantine_path1)
quarantine(bad_day2, quarantine_path2)
t3 = time.time()
print(f"Silver DQ + quarantine Run_time: {t3 - t2:.2f}s")

q1_df = spark.read.format("delta").load(quarantine_path1)
q2_df = spark.read.format("delta").load(quarantine_path2)
print(f"quarantine count: q1={q1_df.count()}, q2={q2_df.count()}")

def silver_layer(clean_df1, clean_df2, merge_path):
  if clean_df1.limit(1).count() > 0:
    target_df = clean_df1 \
               .withColumn("is_current",     lit(True)) \
               .withColumn("valid_from", col("ingest_date").cast(DateType())) \
               .withColumn("valid_to", lit(None).cast("timestamp"))



    target_df.write.format("delta").mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("order_date").save(merge_path) \

  if clean_df2.limit(1).count() > 0:
    source_df = clean_df2


    if not DeltaTable.isDeltaTable(spark, merge_path):
      source_df.withColumn("is_current", lit(True)) \
                      .withColumn("valid_from", col("ingest_date").cast(DateType())) \
                      .withColumn("valid_to", lit(None).cast("timestamp")) \
                      .write.format("delta").mode("append") \
                      .option("mergeSchema", "true") \
                      .partitionBy("order_date").save(merge_path)

    else:
      target = DeltaTable.forPath(spark, merge_path)
      # Step 1: Expire (soft-delete) the active version for orders whose status changed.
      target.alias("tgt").merge(
                source_df.alias("src"),
                condition="""
                    tgt.order_id = src.order_id
                    AND tgt.line_item_seq = src.line_item_seq
                    AND tgt.is_current = true
                    AND tgt.status != src.status
                """
            ).whenMatchedUpdate(set={
                "is_current": lit(False),
                "valid_to": current_timestamp()
            }).execute()

      # Step 2: Insert the new updated state as the current active version
      incoming_rows = source_df.withColumn("is_current", lit(True)) \
                                 .withColumn("valid_from", col("ingest_date").cast(DateType())) \
                                 .withColumn("valid_to", lit(None).cast("timestamp"))

      # Explain (Log and display the physical execution plan)
      explain_and_log(incoming_rows, "Silver incoming rows (before 2nd MERGE)")

      target.alias("tgt").merge(
                incoming_rows.alias("src"),
                condition="""
                tgt.order_id = src.order_id
                AND tgt.line_item_seq = src.line_item_seq
                AND tgt.is_current = true
                """
            ).whenNotMatchedInsertAll() \
            .execute()

    print("merged successfully")

    # Z-ORDER after all MERGEs
    DeltaTable.forPath(spark, merge_path).optimize().executeZOrderBy(
        "order_id", "region"
    )
    print("Silver Z-ORDER completed")

good_day1 = good_day1.cache()
good_day2 = good_day2.cache()

t4 = time.time()

silver_layer(good_day1, good_day2, silver_path)
t5 = time.time()
print(f"Silver MERGE (SCD2) Run_time: {t5 - t4:.2f}s")

good_day1.unpersist()
good_day2.unpersist()

silver_df = spark.read.format("delta").load(silver_path)
print(f"Silver Layer count: {silver_df.count()}")
print("silver layer Delta history:")
DeltaTable.forPath(spark, silver_path).history().show(truncate=False)

# Inspect each of the columns to indicate skew
def indicate_skew(skew_df, columns=None, top_n=10):
  if columns is None:
    columns = skew_df.columns

  for c in columns:
    print(f"\n===== Column: {c} =====")
    counts_df = skew_df.groupBy(c) \
          .count()

    # Get max, avg, min for skew detection
    stats = counts_df.agg(
            max("count").alias("max_count"),
            avg("count").alias("avg_count"),
            min("count").alias("min_count"),
            count("*").alias("distinct_keys")
        ).collect()[0]

    max_c = stats["max_count"]
    avg_c = stats["avg_count"]
    min_c = stats["min_count"]

    skew_ratio = max_c / avg_c if avg_c else 0

    print(f"max(count) = {max_c:,} | avg(count) = {avg_c:,.2f} | min(count) = {min_c:,} | distinct = {stats['distinct_keys']:,}")
    print(f"Skew Ratio (max/avg) = {skew_ratio:,.1f}x -> {'HIGH SKEW - SALT THIS' if skew_ratio > 10 else 'OK' if skew_ratio < 3 else 'MODERATE'}")

    # Top N heaviest keys
    counts_df.orderBy(desc("count")).show(top_n, truncate=False)

    # Explain (Log and display the physical execution plan)
    explain_and_log(counts_df, "Skew Audit")

silver_df.cache()

t6 = time.time()
indicate_skew(silver_df)

# Gold_layer

def gold_layer(transformed_df, order_path, region_path):
  # 1. Filter for only active/current SCD2 records and calculate basic line item values
  # Assign an integer priority/rank to each order status (used later to determine overall order status)
  current_df = transformed_df.filter(col("is_current") == lit(True)) \
               .withColumn("line_value", round(col("quantity") * col("unit_price"), 2)) \
               .withColumn(
                   "status_rank",
                    when(col("status") == "CANCELLED", 0)
                    .when(col("status") == "PLACED", 1)
                    .when(col("status") == "PROCESSING", 2)
                    .when(col("status") == "SHIPPED", 3)
                    .when(col("status") == "DELIVERED", 4)
                    .otherwise(99)
               )
  
  # 2. Aggregate line-item level data into a single summary record per order
  # Takes the minimum status_rank to resolve overall order status (e.g., CANCELLED takes precedence)
  order_summary = current_df.groupBy("order_id", "customer_id", "region", "order_date") \
            .agg(
                count("*").alias("line_item_count"),
                sum("quantity").alias("total_quantity"),
                sum("line_value").alias("total_order_value"),
                min("status_rank").alias("status_rank")
            ) \
            .withColumn(
                "status",
                 when(col("status_rank") == 0, "CANCELLED")
                 .when(col("status_rank") == 1, "PLACED")
                 .when(col("status_rank") == 2, "PROCESSING")
                 .when(col("status_rank") == 3, "SHIPPED")
                 .when(col("status_rank") == 4, "DELIVERED")
                 .otherwise("UNKNOWN")
            ) \
            .drop("status_rank") # Drop helper column after mapping back to status label

  # Log and display the physical execution plan for the order summary aggregation
  explain_and_log(order_summary, "Gold – order_summary aggregation")

  order_summary.write.format("delta") \
               .mode("overwrite") \
               .option("overwriteSchema", "true") \
               .partitionBy("order_date") \
               .save(order_path)
  print(f"order_summary writen to {order_path}")

  DeltaTable.forPath(spark, order_path).optimize().executeZOrderBy("order_id")


  region_daily_metrics = current_df.groupBy("region", "order_date") \
                      .agg(count_distinct("order_id").alias("order_count"),
                          sum(col("quantity")).alias("_total_quantity"),
                          sum(col("line_value")).alias("total_revenue")
                     ).withColumn("avg_order_value", round(col("total_revenue")
                                              / col("order_count"), 2))

  # Log and display the physical execution plan for the regional metrics aggregation
  explain_and_log(region_daily_metrics, "Gold – region_daily_metrics aggregation")

  region_daily_metrics.write.format("delta") \
               .mode("overwrite") \
               .option("overwriteSchema", "true") \
               .save(region_path)

  # Optimize storage layout of region_daily_metrics using Z-Ordering 
  DeltaTable.forPath(spark, region_path).optimize().executeZOrderBy("region")

  # comparing relations for order_id-4338
  current_df.filter(col("order_id") == "4338").groupBy("status").count().show()
  # then compare against:
  order_summary.filter(col("order_id") == "4338").select("status").show()


gold_layer(silver_df, gold_order_path, gold_region_path)
t7 = time.time()
print(f"Gold-aggregation & Skew-Audit Run_time: {t7 - t6:.2f}s")

# verify successful agg gold_layer
gold_order_df = spark.read.format("delta").load(gold_order_path)
gold_region_df = spark.read.format("delta").load(gold_region_path)
print(f"Gold order_summar count: {gold_order_df.count()}")
print(f"Gold region_daily_metrics count: {gold_region_df.count()}")
print("Gold order_summary history:")
DeltaTable.forPath(spark, gold_order_path).history().show(truncate=False)
print("Gold region_daily_metrics history:")
DeltaTable.forPath(spark, gold_region_path).history().show(truncate=False)

silver_df.unpersist()

# 1. Output pipeline completion status and inform user of Spark UI access for job profiling
print("\nPipeline finished. All caches released.")
print("Open Spark UI → http://localhost:4050 to review jobs, stages, SQL plans and storage.")

# 2. Print table storage metadata headers to audit physical file compaction
print("=== File counts after Z-ORDER ===")

# 3. Iterate through every Delta table layer across Bronze, Silver, and Gold paths
for path, name in [
    (bronze_path1, "Bronze Day1"),
    (bronze_path2, "Bronze Day2"),
    (silver_path, "Silver"),
    (gold_order_path, "Gold Order"),
    (gold_region_path, "Gold Region")
]:
    # Query Delta table details metadata (file count and byte size) and retrieve the primary row
    detail = DeltaTable.forPath(spark, path).detail().select("numFiles", "sizeInBytes").collect()[0]
    
    # Print table file compaction metrics and formatted size in Kibibytes (KiB)
    print(f"{name}: {detail['numFiles']} files, {detail['sizeInBytes']/1024:.1f} KiB")


