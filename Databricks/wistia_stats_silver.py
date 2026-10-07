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
    "s3://wistia-data-bkt/bronze/wistia/stats/"
)

SILVER_PATH = (
    "s3://wistia-data-bkt/silver/wistia_stats/"
)

DQ_ERROR_PATH = (
    "s3://wistia-data-bkt/silver/dq_errors/wistia_stats/"
)

CHECKPOINT_PATH = (
    "s3://wistia-data-bkt/silver/checkpoints/wistia_stats/"
)

# COMMAND ----------

# ============================================================
# EXPLICIT SCHEMA
# ============================================================
stats_schema = StructType([
    StructField("engagement", DoubleType(), True),
    StructField("hours_watched", DoubleType(), True),
    StructField("load_count", LongType(), True),
    StructField("play_count", LongType(), True),
    StructField("play_rate", DoubleType(), True),
    StructField("visitors", LongType(), True)
])

# COMMAND ----------

# ============================================================
# AUTO LOADER
# ============================================================

stats_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(stats_schema)
    .load(BRONZE_PATH)
)

# COMMAND ----------

# ============================================================
# Transform + schema checks + DQ
# ============================================================
def transform_wistia_stats(bronze_df):

    # ============================================================
    # 1. SCHEMA DRIFT CHECK
    # ============================================================

    known_columns = {
        "engagement",
        "hours_watched",
        "load_count",
        "play_count",
        "play_rate",
        "visitors",
        "_rescued_data"
    }

    required_columns = {
        "engagement",
        "hours_watched",
        "load_count",
        "play_count",
        "play_rate",
        "visitors"
    }

    actual_columns = set(bronze_df.columns)

    new_columns = actual_columns - known_columns

    if new_columns:
        print(
            f"WARNING: New Bronze columns detected: "
            f"{sorted(new_columns)}"
        )
    else:
        print("No new top-level Bronze columns detected")

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
    # 2. TRANSFORM
    # ============================================================

    silver_df = bronze_df.select(

        F.regexp_extract(
            F.col("_metadata.file_path"),
            r"/([^/]+)\.json$",
            1
        ).alias("media_id"),

        F.round(F.col("engagement"),4).alias("engagement"),

        F.round(F.col("hours_watched"),2).alias("hours_watched"),

        F.col("load_count"),

        F.col("play_count"),

        F.round(F.col("play_rate"),4).alias("play_rate"),

        F.col("visitors"),

        F.col("_rescued_data"),

        F.current_timestamp().alias(
            "load_timestamp"
        )
    )


    # ============================================================
    # 3. CLEAN BUSINESS KEY
    # ============================================================

    silver_df = silver_df.withColumn(
        "media_id",

        F.when(
            F.col("media_id").isNull()
            | (F.trim(F.col("media_id")) == ""),
            F.lit(None)
        )
        .otherwise(
            F.trim(F.col("media_id"))
        )
    )


    # ============================================================
    # 4. SCHEMA ENFORCEMENT
    # ============================================================

    expected_types = {
        "media_id": "string",
        "engagement": "double",
        "hours_watched": "double",
        "load_count": "bigint",
        "play_count": "bigint",
        "play_rate": "double",
        "visitors": "bigint",
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

    print("Schema/type enforcement passed")


    # ============================================================
    # 5. ROW LEVEL DQ
    # ============================================================

    silver_df = silver_df.withColumn(
        "dq_error_reason",

        F.when(
            F.col("media_id").isNull(),
            F.lit("INVALID_BUSINESS_KEY")
        )

        .when(
            F.col("play_count").isNull()
            | (F.col("play_count") < 0),
            F.lit("INVALID_PLAY_COUNT")
        )

        .when(
            F.col("load_count").isNull()
            | (F.col("load_count") < 0),
            F.lit("INVALID_LOAD_COUNT")
        )

        .when(
            F.col("visitors").isNull()
            | (F.col("visitors") < 0),
            F.lit("INVALID_VISITOR_COUNT")
        )

        .when(
            F.col("hours_watched").isNull()
            | (F.col("hours_watched") < 0),
            F.lit("INVALID_HOURS_WATCHED")
        )

        .when(
            F.col("engagement").isNull()
            | (F.col("engagement") < 0)
            | (F.col("engagement") > 1),
            F.lit("INVALID_ENGAGEMENT")
        )

        .when(
            F.col("play_rate").isNull()
            | (F.col("play_rate") < 0)
            | (F.col("play_rate") > 1),
            F.lit("INVALID_PLAY_RATE")
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

# bronze_df = spark.read.format("json").load(BRONZE_PATH).withColumn("_rescued_data",F.lit(None).cast("string"))

# silver_df = transform_wistia_stats(bronze_df)

# silver_df.show(5, False)

# COMMAND ----------

# ============================================================
# Delta MERGE
# ============================================================
def merge_stats_to_silver(df):

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
        transform_wistia_stats(
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


    # ------------------------------------------------------------
    # Write bad rows
    # ------------------------------------------------------------

    if not bad_df.isEmpty():

        (
            bad_df.write
            .format("delta")
            .mode("append")
            .save(DQ_ERROR_PATH)
        )


    # ------------------------------------------------------------
    # Stop if no valid rows
    # ------------------------------------------------------------

    if good_df.isEmpty():
        return


    # ------------------------------------------------------------
    # Deduplicate
    # ------------------------------------------------------------

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
        .drop("row_num")
    )


    # ------------------------------------------------------------
    # Merge to Silver
    # ------------------------------------------------------------

    merge_stats_to_silver(
        good_df
    )

# COMMAND ----------

# ============================================================
# START INCREMENTAL LOAD
# ============================================================

query = (
    stats_stream_df.writeStream
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