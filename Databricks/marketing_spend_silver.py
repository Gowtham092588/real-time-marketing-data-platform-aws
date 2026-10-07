# Databricks notebook source
# MAGIC %md
# MAGIC ======== Market Spend Silver =========

# COMMAND ----------

# ============================================
# 1. Imports
# ============================================
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)
from pyspark.sql.window import Window as W

from delta.tables import DeltaTable

# COMMAND ----------

# ============================================
# 2. PATH
# ============================================

BRONZE_PATH = (
    "s3://calendly-data-bkt/bronze/marketing_spend/"
)

SILVER_PATH = (
    "s3://calendly-data-bkt/silver/marketing_spend/"
)

CHECKPOINT_PATH = (
    "s3://calendly-data-bkt/silver/checkpoints/"
    "marketing_spend_silver/"
)

DQ_ERROR_PATH = ("s3://calendly-data-bkt/silver/dq_errors/marketing_spend/")

# COMMAND ----------

# ============================================
# 3. Bronze Schema
# ============================================
bronze_schema = (
    spark.read
    .option("multiLine", "true")
    .json(BRONZE_PATH)
    .schema
)

print(bronze_schema)

# COMMAND ----------

# ============================================
# 4. Read Bronze incrementally with Auto Loader
# ============================================

bronze_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option(
        "cloudFiles.format",
        "json"
    )
    .option(
        "multiLine",
        "true"
    )
    .schema(
        bronze_schema
    )
    .load(
        BRONZE_PATH
    )
)

bronze_stream_df.printSchema()


# COMMAND ----------

# ===============================================
# 4. Add Metadata column to identify source file.
# ===============================================
bronze_stream_df = (
    bronze_stream_df
    .withColumn(
        "source_file",
        F.col("_metadata.file_path")
    )
)

bronze_stream_df.printSchema()

# COMMAND ----------

# ===============================================
# 5. Transformation - Marketing Spend
# ===============================================
def transform_marketing_spend(bronze_df):

    #Schema Drift Detection

    expected_columns = {
            "date",
            "channel",
            "spend"
        }

    actual_columns = set(bronze_df.columns)

    missing_columns = (
            expected_columns
            - actual_columns
        )

    new_columns = (
            actual_columns
            - expected_columns
        )
    
    if missing_columns:
        raise ValueError(
        f"Missing required columns: {missing_columns}"
    )

    if new_columns:
        print(
        f"WARNING: New source columns detected: {new_columns}"
    )

    silver_df = (
        bronze_df
        .withColumn(
            "channel",
            F.lower(F.trim(F.col("channel")))
        )
        .withColumn(
            "spend_date",
            F.to_date(F.col("date"), "yyyy-MM-dd")
        )
        .withColumn(
            "spend_amount",
            F.col("spend").cast("decimal(12, 2)")
        )
        .withColumn(
            "source_file_date",
            F.to_date(
                F.regexp_extract(
                    F.col("source_file"),
                    r"spend_data_(\d{4}-\d{2}-\d{2})\.json",
                    1
                ),
                "yyyy-MM-dd"
            )
        )
        .withColumn(
            "currency",
            F.lit("USD")
        )
        .withColumn(
            "load_timestamp",
            F.current_timestamp()
        )
        .drop("date", "spend")
    )

    # Schema Validation
    required_columns = [
        "channel",
        "spend_date",
        "spend_amount",
        "currency",
        ]

    missing_required_columns = [column_name for column_name in required_columns if column_name not in silver_df.columns]

    if missing_required_columns:
        raise  ValueError(
            f"Missing required columns: {missing_required_columns}"
        )
    
    print("Schema Validation passed")
    
    # Schema Enforcement

    required_columns = [
        "channel",
        "spend_date",
        "spend_amount",
        "currency",
        "source_file",
        "source_file_date",
        "load_timestamp"
    ]

    missing_required_columns = [
        column_name
        for column_name in required_columns
        if column_name not in silver_df.columns
    ]

    if missing_required_columns:
        raise ValueError(
            f"Missing required columns: {missing_required_columns}"
        )
    
    print("Schema Enforcement passed")


    # Schema Enforcement

    expected_types = {
        "channel": "string",
        "spend_date": "date",
        "spend_amount": "decimal(12,2)",
        "currency": "string",
        "source_file": "string",
        "source_file_date": "date",
        "load_timestamp": "timestamp"
    }

    actual_types = dict(silver_df.dtypes)

    for column_name, expected_type in expected_types.items():

        actual_type = actual_types.get(column_name)

        if actual_type is None:
            raise ValueError(
                f"Schema enforcement failed. "
                f"Missing column: {column_name}"
            )

        if actual_type != expected_type:
            raise ValueError(
                f"Schema enforcement failed for {column_name}. "
                f"Expected data type: {expected_type}. "
                f"Actual data type: {actual_type}"
            )

    print("Schema/type enforcement passed")

    # Business Key Validation
    invalid_business_key_count = (
            silver_df
            .filter(
                F.col("channel").isNull()
                | (F.trim(F.col("channel")) == "")
                | F.col("spend_date").isNull()
            )
            .count()
            )

    if invalid_business_key_count > 0:
            raise ValueError(
                f"Business key validation failed. "
                f"Invalid invitee_id count: {invalid_business_key_count}"
            )
    print("Business key validation passed")
    return silver_df

# transform_df = transform_marketing_spend(bronze_df)

# transform_df.select("channel", "spend_date", "spend_amount", "currency", "source_file", "source_file_date", "load_timestamp").show(10, truncate=False)




# COMMAND ----------

# =======================================================
# ROW LEVEL QUALITY CHECKS FOR MARKET SPEND
# =======================================================

def add_marketing_dq_errors(df):

    return (
        df.withColumn(
            "dq_error_reason",

            F.when(
                F.col("channel").isNull()
                | (F.trim(F.col("channel")) == ""),
                F.lit("INVALID_BUSINESS_KEY")
            )

            .when(
                F.col("spend_date").isNull(),
                F.lit("MISSING_SPEND_DATE")
            )

            .when(
                F.col("spend_amount").isNull()
                | (F.col("spend_amount") < 0)
                   ,
                   F.lit("INVALID_SPEND_AMOUNT")
            )

            .otherwise(F.lit(None))
        )
    )

# COMMAND ----------

# ===============================================
# 5. Data Quality Validation
# ===============================================

def validate_marketing_spend(silver_df):

    invalid_date_count = (
        silver_df
        .filter(
            F.col("spend_date").isNull()
        )
        .count()
    )

    invalid_channel_count = (
        silver_df
        .filter(
            F.col("channel").isNull()
            |
            (F.trim(F.col("channel")) == "")
        )
        .count()
    )

    invalid_spend_count = (
        silver_df
        .filter(
            F.col("spend_amount").isNull()
            |
            (F.col("spend_amount") < 0)
        )
        .count()
    )

    invalid_source_file_date_count = (
        silver_df
        .filter(
            F.col(
                "source_file_date"
            ).isNull()
        )
        .count()
    )

    print(
        f"Invalid dates: "
        f"{invalid_date_count}"
    )

    print(
        f"Invalid channels: "
        f"{invalid_channel_count}"
    )

    print(
        f"Invalid spend values: "
        f"{invalid_spend_count}"
    )

    print(
        f"Invalid source file dates: "
        f"{invalid_source_file_date_count}"
    )


    if (
        invalid_date_count > 0
        or invalid_channel_count > 0
        or invalid_spend_count > 0
        or invalid_source_file_date_count > 0
    ):

        raise ValueError(
            "Marketing spend data quality validation failed."
        )


    print(
        "Marketing spend data quality validation passed."
    )

# COMMAND ----------

# ===============================================
# 6. Detect bad duplicates in source files
# ===============================================
def detect_bad_duplicates(silver_df):
    Duplicate_count = (
        silver_df
        .groupBy("channel", "spend_date", "source_file")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if Duplicate_count > 0:
        raise ValueError(
            f"Found {Duplicate_count} duplicate "
            f"date/channel records inside the same source file."
        )
    
    print(" No duplicates found. Duplicate Check Passed")

    return silver_df


# COMMAND ----------

# ===============================================
# 7. DeDuplication
# ===============================================

def deduplication_marketing_spend(silver_df):
    
    window_spec = W.partitionBy("channel", "spend_date").orderBy(F.desc("source_file_date"))
    
    dedup_df = (
        silver_df
        .withColumn(
            "row_number",
            F.row_number().over(window_spec)
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        )
    return dedup_df


# COMMAND ----------

# ===============================================
# 7. Delta Merge Function
# ===============================================
def merge_marketing_spend_delta(silver_df):

    silver_table = DeltaTable.forPath(spark, SILVER_PATH)

    
    (
        silver_table.alias("target")
        .merge(silver_df.alias("source"),
            """
            target.spend_date = source.spend_date
            AND
            target.channel = source.channel
            """
        )
        .whenMatchedUpdateAll(
            condition="""
            source.source_file_date
            >=
            target.source_file_date
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        "Marketing spend Delta MERGE completed successfully."
    )

# COMMAND ----------

# ============================================
# 8. Process each Auto Loader batch
# ============================================

def process_batch(batch_df, batch_id):

    try:

        print(f"Starting batch: {batch_id}")

        source_rows = batch_df.count()

        if source_rows == 0:
            return

        transform_df = transform_marketing_spend(batch_df)

        validated_df = add_marketing_dq_errors(transform_df)

        good_df = (
            validated_df
            .filter(F.col("dq_error_reason").isNull())
            .drop("dq_error_reason")
        )

        bad_df = (
            validated_df
            .filter(F.col("dq_error_reason").isNotNull())
        )

        if not bad_df.isEmpty():
            (
                bad_df.write
                .format("json")
                .mode("append")
                .save(DQ_ERROR_PATH)
            )
        

        validate_marketing_spend(good_df)

        dedup_df = detect_bad_duplicates(good_df)

        silver_df = deduplication_marketing_spend(dedup_df)

        if DeltaTable.isDeltaTable(spark,SILVER_PATH):

            merge_marketing_spend_delta(silver_df)

        else:

            (
                silver_df.write
                .format("delta")
                .mode("overwrite")
                .save(SILVER_PATH)
            )

        print(f"Batch {batch_id} completed.")

    except Exception as e:

        print( f"Batch {batch_id} failed: {str(e)}")

        raise

# COMMAND ----------

query = (
    bronze_stream_df
    .writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .trigger(
        availableNow=True
    )
    .start()
)

query.awaitTermination()