# Databricks notebook source
# ============================================================
# IMPORTS
# ============================================================
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# ============================================================
# PATHS
# ============================================================

BRONZE_PATH = (
    "s3://wistia-data-bkt/bronze/wistia/engagement/"
)

SILVER_PATH = (
    "s3://wistia-data-bkt/silver/wistia_engagement/"
)

DQ_ERROR_PATH = (
    "s3://wistia-data-bkt/silver/dq_errors/wistia_engagement/"
)

CHECKPOINT_PATH = (
    "s3://wistia-data-bkt/silver/checkpoints/wistia_engagement/"
)

# COMMAND ----------

# ============================================================
# EXPLICIT BRONZE SCHEMA
# ============================================================

engagement_schema = StructType([
    StructField("engagement", DoubleType(), True),
    StructField(
        "engagement_data",
        ArrayType(LongType()),
        True
    ),
    StructField(
        "rewatch_data",
        ArrayType(LongType()),
        True
    )
])

# COMMAND ----------

# ============================================================
# AUTO LOADER
# ============================================================

engagement_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(engagement_schema)
    .load(BRONZE_PATH)
)

# COMMAND ----------

# ============================================================
# Transform + schema checks + DQ
# ============================================================

def transform_wistia_engagement(bronze_df):

    # ============================================================
    # 1. ENSURE _rescued_data EXISTS FOR BATCH TESTING
    # ============================================================

    if "_rescued_data" not in bronze_df.columns:
        bronze_df = bronze_df.withColumn(
            "_rescued_data",
            F.lit(None).cast("string")
        )


    # ============================================================
    # 2. SCHEMA DRIFT DETECTION
    # ============================================================

    known_columns = {
        "engagement",
        "engagement_data",
        "rewatch_data",
        "_rescued_data"
    }

    required_columns = {
        "engagement",
        "engagement_data",
        "rewatch_data"
    }

    actual_columns = set(bronze_df.columns)

    new_columns = (
        actual_columns - known_columns
    )

    if new_columns:
        print(
            f"WARNING: New Bronze columns detected: "
            f"{sorted(new_columns)}"
        )
    else:
        print(
            "No new top-level Bronze columns detected"
        )


    missing_required_columns = (
        required_columns - actual_columns
    )

    if missing_required_columns:
        raise ValueError(
            f"Schema drift detected. "
            f"Missing required Bronze columns: "
            f"{sorted(missing_required_columns)}"
        )


    # ============================================================
    # 3. TRANSFORM
    # ============================================================

    silver_df = bronze_df.select(

        F.regexp_extract(
            F.col("_metadata.file_path"),
            r"/([^/]+)\.json$",
            1
        ).alias("media_id"),

        F.round(
            F.col("engagement"),
            4
        ).alias("engagement"),

        F.col("engagement_data"),

        F.col("rewatch_data"),

        F.col("_rescued_data"),

        F.current_timestamp().alias(
            "load_timestamp"
        )
    )


    # ============================================================
    # 4. CLEAN BUSINESS KEY
    # ============================================================

    silver_df = silver_df.withColumn(
        "media_id",

        F.when(
            F.col("media_id").isNull()
            | (F.trim(F.col("media_id")) == ""),
            F.lit(None)
        )
        .otherwise(
            F.trim(
                F.col("media_id")
            )
        )
    )


    # ============================================================
    # 5. SCHEMA ENFORCEMENT
    # ============================================================

    expected_types = {
        "media_id": "string",
        "engagement": "double",
        "engagement_data": "array<bigint>",
        "rewatch_data": "array<bigint>",
        "_rescued_data": "string",
        "load_timestamp": "timestamp"
    }

    actual_types = dict(
        silver_df.dtypes
    )

    for column_name, expected_type in expected_types.items():

        actual_type = actual_types.get(
            column_name
        )

        if actual_type is None:
            raise ValueError(
                f"Schema enforcement failed. "
                f"Missing column: {column_name}"
            )

        if actual_type != expected_type:
            raise ValueError(
                f"Schema enforcement failed for "
                f"{column_name}. "
                f"Expected: {expected_type}. "
                f"Actual: {actual_type}"
            )

    print(
        "Schema/type enforcement passed"
    )


    # ============================================================
    # 6. ROW LEVEL DQ
    # ============================================================

    silver_df = silver_df.withColumn(
        "dq_error_reason",

        F.when(
            F.col("media_id").isNull(),
            F.lit("INVALID_BUSINESS_KEY")
        )

        .when(
            F.col("engagement").isNull()
            | (F.col("engagement") < 0)
            | (F.col("engagement") > 1),
            F.lit("INVALID_ENGAGEMENT")
        )

        .when(
            F.col("engagement_data").isNull(),
            F.lit("MISSING_ENGAGEMENT_DATA")
        )

        .when(
            F.col("rewatch_data").isNull(),
            F.lit("MISSING_REWATCH_DATA")
        )

        .when(
            F.col("_rescued_data").isNotNull(),
            F.lit("SCHEMA_DRIFT")
        )

        .otherwise(
            F.lit(None)
        )
    )

    return silver_df

# COMMAND ----------

# ============================================================
# Delta MERGE
# ============================================================

def merge_engagement_to_silver(df):

    final_df = (
        df
        .drop(
            "dq_error_reason",
            "_rescued_data"
        )
    )

    if DeltaTable.isDeltaTable(
        spark,
        SILVER_PATH
    ):

        target = DeltaTable.forPath(
            spark,
            SILVER_PATH
        )

        (
            target.alias("target")
            .merge(
                final_df.alias("source"),
                "target.media_id = source.media_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:

        (
            final_df.write
            .format("delta")
            .mode("overwrite")
            .save(SILVER_PATH)
        )

# COMMAND ----------

# ============================================================
# PROCESS BATCH
# ============================================================
def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return


    transformed_df = (
        transform_wistia_engagement(
            batch_df
        )
    )


    bad_df = (
        transformed_df
        .filter(
            F.col(
                "dq_error_reason"
            ).isNotNull()
        )
    )


    good_df = (
        transformed_df
        .filter(
            F.col(
                "dq_error_reason"
            ).isNull()
        )
    )


    # ============================================================
    # WRITE BAD ROWS
    # ============================================================

    if not bad_df.isEmpty():

        (
            bad_df.write
            .format("delta")
            .mode("append")
            .save(DQ_ERROR_PATH)
        )


    # ============================================================
    # STOP IF NO VALID ROWS
    # ============================================================

    if good_df.isEmpty():
        return


    # ============================================================
    # DEDUPLICATE
    # ============================================================

    window_spec = (
        Window
        .partitionBy("media_id")
        .orderBy(
            F.col(
                "load_timestamp"
            ).desc()
        )
    )

    good_df = (
        good_df

        .withColumn(
            "row_num",
            F.row_number().over(
                window_spec
            )
        )

        .filter(
            F.col("row_num") == 1
        )

        .drop(
            "row_num"
        )
    )


    # ============================================================
    # MERGE TO SILVER
    # ============================================================

    merge_engagement_to_silver(
        good_df
    )

# COMMAND ----------

# ============================================================
# START INCREMENTAL LOAD
# ============================================================

query = (
    engagement_stream_df.writeStream
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