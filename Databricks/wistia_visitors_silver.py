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
    "s3://wistia-data-bkt/bronze/wistia/visitors/"
)

SILVER_PATH = (
    "s3://wistia-data-bkt/silver/wistia_visitors/"
)

DQ_ERROR_PATH = (
    "s3://wistia-data-bkt/silver/dq_errors/wistia_visitors/"
)

CHECKPOINT_PATH = (
    "s3://wistia-data-bkt/silver/checkpoints/wistia_visitors/"
)

# COMMAND ----------

# ============================================================
# EXPLICIT BRONZE SCHEMA
# ============================================================
visitor_schema = StructType([
    StructField("created_at", StringType(),True),
    StructField("identifying_event_key", StringType(),True),
    StructField("last_active_at", StringType(),True),
    StructField("last_event_key",StringType(),True),
    StructField("load_count",LongType(),True),
    StructField("play_count",LongType(),True),
    StructField("user_agent_details",StructType(
        [
            StructField(
                "browser",
                StringType(),
                True),
            StructField(
                "browser_version",
                StringType(),
                True
            ),
            StructField(
                "mobile",
                BooleanType(),
                True
            ),
            StructField(
                "platform",
                StringType(),
                True
            )
        ]
        ),
        True
    ),
    StructField("visitor_identity",StructType(
        [
            StructField(
                "email",
                StringType(),
                True
            ),
            StructField(
                "name",
                StringType(),
                True
            ),
            StructField(
                "org",
                StructType(
                    [
                    StructField(
                        "name",
                        StringType(),
                        True
                    ),
                    StructField(
                        "title",
                        StringType(),
                        True
                    )
                ]
                    ),
                True
            )
        ]
        ),
        True
    ),

    StructField("visitor_key",StringType(),True)
])

# COMMAND ----------

# ============================================================
# AUTO LOADER
# ============================================================
visitors_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","json")
    .option("rescuedDataColumn","_rescued_data")
    .schema(visitor_schema)
    .load(BRONZE_PATH)
)

# COMMAND ----------

# ============================================================
# Transform + schema checks + DQ
# ============================================================
def transform_wistia_visitors(bronze_df):

    # ============================================================
    # 1. SUPPORT NORMAL BATCH TESTING
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
        "created_at",
        "identifying_event_key",
        "last_active_at",
        "last_event_key",
        "load_count",
        "play_count",
        "user_agent_details",
        "visitor_identity",
        "visitor_key",
        "_rescued_data"
    }

    required_columns = {
        "created_at",
        "last_active_at",
        "load_count",
        "play_count",
        "visitor_key"
    }

    actual_columns = set(bronze_df.columns)

    new_columns = (actual_columns - known_columns)

    if new_columns:

        print(
            f"WARNING: New Bronze columns detected: "
            f"{sorted(new_columns)}"
        )

    else:

        print(
            "No new top-level Bronze columns detected"
        )


    missing_required_columns = (required_columns - actual_columns)

    if missing_required_columns:

        raise ValueError(
            f"Schema drift detected. "
            f"Missing required Bronze columns: "
            f"{sorted(missing_required_columns)}"
        )


    # ============================================================
    # 3. VALIDATE REQUIRED NESTED STRUCTURE
    # ============================================================

    required_nested_columns = [
        "user_agent_details.browser",
        "user_agent_details.mobile",
        "user_agent_details.platform"
    ]

    for column_name in required_nested_columns:

        try:

            bronze_df.select(
                F.col(column_name)
            )

        except Exception:

            raise ValueError(
                f"Schema drift detected. "
                f"Required nested field is missing: "
                f"{column_name}"
            )


    # ============================================================
    # 4. SELECT / FLATTEN
    # ============================================================

    silver_df = bronze_df.select(

        F.col("visitor_key"),
        F.col("visitor_identity.name").alias("visitor_name"),
        F.col("visitor_identity.email").alias("visitor_email"),
        F.col("visitor_identity.org.name").alias("organization_name"),
        F.col("visitor_identity.org.title").alias("organization_title"),
        F.col("identifying_event_key"),
        F.col("last_event_key"),
        F.col("load_count"),
        F.col("play_count"),
        F.col("user_agent_details.browser").alias("browser"),
        F.col("user_agent_details.browser_version").alias("browser_version"),
        F.col("user_agent_details.mobile").alias("mobile"),
        F.col("user_agent_details.platform").alias("platform"),
        F.col("created_at"),
        F.col("last_active_at"),
        F.col("_rescued_data")
    )


    # ============================================================
    # 5. NULL CLEANUP / STRING STANDARDIZATION
    # ============================================================

    columns_to_clean = [
        "visitor_key",
        "visitor_name",
        "visitor_email",
        "organization_name",
        "organization_title",
        "identifying_event_key",
        "last_event_key",
        "browser",
        "browser_version",
        "platform"
    ]

    for column_name in columns_to_clean:

        silver_df = silver_df.withColumn(

            column_name,

            F.when(

                F.col(
                    column_name
                ).isNull()

                | (
                    F.trim(
                        F.col(column_name)
                    ) == ""
                )

                | (
                    F.lower(
                        F.trim(
                            F.col(column_name)
                        )
                    ) == "none"
                ),

                F.lit(None)

            ).otherwise(

                F.trim(
                    F.col(column_name)
                )
            )
        )


    # ============================================================
    # 6. STANDARDIZE EMAIL
    # ============================================================

    silver_df = silver_df.withColumn(
        "visitor_email",
        F.lower(
            F.col("visitor_email")
        )
    )


    # ============================================================
    # 7. CONVERT TIMESTAMPS
    # ============================================================

    silver_df = (
        silver_df

        .withColumn(
            "created_at",
            F.to_timestamp(
                F.col("created_at")
            )
        )

        .withColumn(
            "last_active_at",
            F.to_timestamp(
                F.col("last_active_at")
            )
        )
    )


    # ============================================================
    # 8. LOAD TIMESTAMP
    # ============================================================

    silver_df = silver_df.withColumn(
        "load_timestamp",
        F.current_timestamp()
    )


    # ============================================================
    # 9. SCHEMA ENFORCEMENT
    # ============================================================

    expected_types = {

        "visitor_key": "string",

        "visitor_name": "string",

        "visitor_email": "string",

        "organization_name": "string",

        "organization_title": "string",

        "identifying_event_key": "string",

        "last_event_key": "string",

        "load_count": "bigint",

        "play_count": "bigint",

        "browser": "string",

        "browser_version": "string",

        "mobile": "boolean",

        "platform": "string",

        "created_at": "timestamp",

        "last_active_at": "timestamp",

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
    # 10. ROW LEVEL DQ
    # ============================================================

    silver_df = silver_df.withColumn(
        "dq_error_reason",

        # Business key
        F.when(
            F.col("visitor_key").isNull()
            | (
                F.trim(
                    F.col("visitor_key")
                ) == ""
            ),
            F.lit(
                "INVALID_BUSINESS_KEY"
            )
        )

        # Created timestamp
        .when(
            F.col("created_at").isNull(),
            F.lit(
                "INVALID_CREATED_AT"
            )
        )

        # Latest activity used for dedup
        .when(
            F.col("last_active_at").isNull(),
            F.lit(
                "INVALID_LAST_ACTIVE_AT"
            )
        )

        # Invalid load count
        .when(
            F.col("load_count").isNull()
            | (
                F.col("load_count") < 0
            ),
            F.lit(
                "INVALID_LOAD_COUNT"
            )
        )

        # Invalid play count
        .when(
            F.col("play_count").isNull()
            | (
                F.col("play_count") < 0
            ),
            F.lit(
                "INVALID_PLAY_COUNT"
            )
        )

        # Schema mismatch captured by Auto Loader
        .when(
            F.col("_rescued_data").isNotNull(),
            F.lit(
                "SCHEMA_DRIFT"
            )
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
def merge_visitors_to_silver(df):

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
                """
                target.visitor_key =
                source.visitor_key
                """
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


    # ============================================================
    # TRANSFORM
    # ============================================================

    transformed_df = (
        transform_wistia_visitors(
            batch_df
        )
    )


    # ============================================================
    # BAD RECORDS
    # ============================================================

    bad_df = (
        transformed_df

        .filter(
            F.col(
                "dq_error_reason"
            ).isNotNull()
        )
    )


    # ============================================================
    # GOOD RECORDS
    # ============================================================

    good_df = (
        transformed_df

        .filter(
            F.col(
                "dq_error_reason"
            ).isNull()
        )
    )


    # ============================================================
    # WRITE DQ ERRORS
    # ============================================================

    if not bad_df.isEmpty():

        (
            bad_df.write
            .format("delta")
            .mode("append")
            .save(
                DQ_ERROR_PATH
            )
        )


    if good_df.isEmpty():
        return


    # ============================================================
    # DEDUPLICATION
    # ============================================================

    visitor_window = (
        Window.partitionBy("visitor_key").orderBy(
            F.col("last_active_at").desc(),
            F.col("load_timestamp").desc()
        )
    )

    dedup_df = (
        good_df
        .withColumn(
            "row_num",
            F.row_number().over(
                visitor_window
            )
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # ============================================================
    # MERGE
    # ============================================================
    merge_visitors_to_silver(dedup_df)

# COMMAND ----------

# ============================================================
# START INCREMENTAL LOAD
# ============================================================
query = (
    visitors_stream_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation",CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

bronze_df = (
    spark.read
    .format("json")
    .option("multiline", "true")
    .load(BRONZE_PATH)
    .withColumn(
        "_rescued_data",
        F.lit(None).cast("string")
    )
)

silver_df = transform_wistia_visitors(
    bronze_df
)

silver_df.show(
    20,
    False
)