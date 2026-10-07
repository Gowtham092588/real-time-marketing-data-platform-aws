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
    "s3://wistia-data-bkt/bronze/wistia/events/"
)

SILVER_PATH = (
    "s3://wistia-data-bkt/silver/wistia_events/"
)

DQ_ERROR_PATH = (
    "s3://wistia-data-bkt/silver/dq_errors/wistia_events/"
)

CHECKPOINT_PATH = (
    "s3://wistia-data-bkt/silver/checkpoints/wistia_events/"
)

# COMMAND ----------

# ============================================================
# EXPLICIT BRONZE SCHEMA
# ============================================================

events_schema = StructType([

    StructField("city", StringType(), True),
    StructField("conversion_type", StringType(), True),
    StructField("country", StringType(), True),
    StructField("email", StringType(), True),
    StructField("embed_url", StringType(), True),
    StructField("event_key", StringType(), True),
    StructField("iframe_heatmap_url", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("media_id", StringType(), True),
    StructField("media_name", StringType(), True),
    StructField("media_url", StringType(), True),
    StructField("org", StringType(), True),
    StructField("percent_viewed", DoubleType(), True),
    StructField("received_at", StringType(), True),
    StructField("region", StringType(), True),

    StructField(
        "thumbnail",
        StructType([
            StructField("contentType", StringType(), True),
            StructField("fileSize", LongType(), True),
            StructField("height", LongType(), True),
            StructField("type", StringType(), True),
            StructField("url", StringType(), True),
            StructField("width", LongType(), True)
        ]),
        True
    ),

    StructField(
        "user_agent_details",
        StructType([
            StructField("browser", StringType(), True),
            StructField("browser_version", StringType(), True),
            StructField("mobile", BooleanType(), True),
            StructField("platform", StringType(), True)
        ]),
        True
    ),

    StructField("visitor_key", StringType(), True),
    StructField(
    "conversion_data",
    MapType(
        StringType(),
        StringType(),
        True
    ),
    True
),

StructField(
    "_file_path",
    StringType(),
    True
)
])

# COMMAND ----------

# ============================================================
# AUTO LOADER
# ============================================================

events_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(events_schema)
    .load(BRONZE_PATH)
)

# COMMAND ----------

# ============================================================
# Transform + schema checks + DQ
# ============================================================
def transform_wistia_events(bronze_df):

    # ============================================================
    # 1. SUPPORT BATCH TESTING
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
        "city",
        "conversion_type",
        "country",
        "email",
        "embed_url",
        "event_key",
        "iframe_heatmap_url",
        "ip",
        "lat",
        "lon",
        "media_id",
        "media_name",
        "media_url",
        "org",
        "percent_viewed",
        "received_at",
        "region",
        "thumbnail",
        "user_agent_details",
        "visitor_key",
        "conversion_data",
        "_file_path",
        "_rescued_data"
    }

    required_columns = {
        "event_key",
        "media_id",
        "visitor_key",
        "percent_viewed",
        "received_at"
    }

    actual_columns = set(
        bronze_df.columns
    )

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
    # 3. SELECT / FLATTEN
    # ============================================================

    silver_df = bronze_df.select(

        F.col("event_key"),

        F.col("visitor_key"),

        F.col("media_id"),

        F.col("media_name"),

        F.col("email"),

        F.col("conversion_type"),

        F.col("percent_viewed"),

        F.col("received_at"),

        F.col("city"),

        F.col("region"),

        F.col("country"),

        F.col("ip"),

        F.col("org"),

        F.col("lat"),

        F.col("lon"),

        F.col("embed_url"),

        F.col("media_url"),

        F.col(
            "user_agent_details.browser"
        ).alias(
            "browser"
        ),

        F.col(
            "user_agent_details.browser_version"
        ).alias(
            "browser_version"
        ),

        F.col(
            "user_agent_details.mobile"
        ).alias(
            "mobile"
        ),

        F.col(
            "user_agent_details.platform"
        ).alias(
            "platform"
        ),

        F.col("_rescued_data")
    )


    # ============================================================
    # 4. NULL CLEANUP / STRING STANDARDIZATION
    # ============================================================

    columns_to_clean = [
        "event_key",
        "visitor_key",
        "media_id",
        "media_name",
        "email",
        "conversion_type",
        "city",
        "region",
        "country",
        "ip",
        "org",
        "embed_url",
        "media_url",
        "browser",
        "browser_version",
        "platform"
    ]

    for column_name in columns_to_clean:

        silver_df = silver_df.withColumn(
            column_name,

            F.when(
                F.col(column_name).isNull()
                | (F.trim(F.col(column_name)) == "")
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
    # 5. STANDARDIZE EMAIL
    # ============================================================

    silver_df = silver_df.withColumn(
        "email",
        F.lower(
            F.col("email")
        )
    )


    # ============================================================
    # 6. TIMESTAMP CONVERSION
    # ============================================================

    silver_df = silver_df.withColumn(
        "received_at",
        F.to_timestamp(
            F.col("received_at")
        )
    )


    # ============================================================
    # 7. ROUND ANALYTICAL VALUES
    # ============================================================

    silver_df = (
        silver_df

        .withColumn(
            "percent_viewed",
            F.round(
                F.col("percent_viewed"),
                2
            )
        )

        .withColumn(
            "lat",
            F.round(
                F.col("lat"),
                4
            )
        )

        .withColumn(
            "lon",
            F.round(
                F.col("lon"),
                4
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

        "event_key": "string",
        "visitor_key": "string",
        "media_id": "string",
        "media_name": "string",
        "email": "string",
        "conversion_type": "string",
        "percent_viewed": "double",
        "received_at": "timestamp",
        "city": "string",
        "region": "string",
        "country": "string",
        "ip": "string",
        "org": "string",
        "lat": "double",
        "lon": "double",
        "embed_url": "string",
        "media_url": "string",
        "browser": "string",
        "browser_version": "string",
        "mobile": "boolean",
        "platform": "string",
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
            F.col("event_key").isNull()
            | (F.trim(F.col("event_key")) == ""),
            F.lit("INVALID_BUSINESS_KEY")
        )

        # Media relationship
        .when(
            F.col("media_id").isNull()
            | (F.trim(F.col("media_id")) == ""),
            F.lit("MISSING_MEDIA_ID")
        )

        # Visitor relationship
        .when(
            F.col("visitor_key").isNull()
            | (F.trim(F.col("visitor_key")) == ""),
            F.lit("MISSING_VISITOR_KEY")
        )

        # Event timestamp
        .when(
            F.col("received_at").isNull(),
            F.lit("INVALID_RECEIVED_AT")
        )

        # Viewing percentage
        .when(
            F.col("percent_viewed").isNull()
            | (F.col("percent_viewed") < 0)
            | (F.col("percent_viewed") > 1),
            F.lit("INVALID_PERCENT_VIEWED")
        )

        # Latitude
        .when(
            F.col("lat").isNotNull()
            & (
                (F.col("lat") < -90)
                | (F.col("lat") > 90)
            ),
            F.lit("INVALID_LATITUDE")
        )

        # Longitude
        .when(
            F.col("lon").isNotNull()
            & (
                (F.col("lon") < -180)
                | (F.col("lon") > 180)
            ),
            F.lit("INVALID_LONGITUDE")
        )

        # Auto Loader schema mismatch
        .when(
            F.col("_rescued_data").isNotNull(),
            F.lit("SCHEMA_DRIFT")
        )

        .otherwise(
            F.lit(None)
        )
    )

    return silver_df

# silver_df.groupBy(
#     "media_id",
#     "dq_error_reason"
# ).count().show(truncate=False)

# COMMAND ----------

# ============================================================
# DELTA MERGE
# ============================================================
def merge_events_to_silver(df):

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
                target.event_key = source.event_key
                AND target.visitor_key = source.visitor_key
                AND target.received_at = source.received_at
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
        transform_wistia_events(
            batch_df
        )
    )


    # ============================================================
    # SPLIT GOOD / BAD
    # ============================================================

    bad_df = (
        transformed_df
        .filter(
            F.col(
                "dq_error_reason"
            ).isNotNull()
        )
    )

    
    print("Bad rows:", bad_df.count())

    good_df = (
        transformed_df
        .filter(
            F.col(
                "dq_error_reason"
            ).isNull()
        )
    )

    print("Good rows:", good_df.count())


    # ============================================================
    # QUARANTINE BAD ROWS
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
    # DEDUPLICATE
    # ============================================================

    event_window = (
        Window

        .partitionBy(
            "event_key"
        )

        .orderBy(
            F.col(
                "received_at"
            ).desc(),

            F.col(
                "load_timestamp"
            ).desc()
        )
    )


    dedup_df = (
        good_df

        .withColumn(
            "row_num",
            F.row_number().over(
                event_window
            )
        )

        .filter(
            F.col(
                "row_num"
            ) == 1
        )

        .drop(
            "row_num"
        )
    )


    # ============================================================
    # MERGE
    # ============================================================

    merge_events_to_silver(
        dedup_df
    )

# COMMAND ----------

# ============================================================
# START INCREMENTAL LOAD
# ============================================================
query = (
    events_stream_df.writeStream

    .foreachBatch(
        process_batch
    )

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

print(query.lastProgress)


# COMMAND ----------

silver_df = (
    spark.read
    .format("delta")
    .load(
        "s3://wistia-data-bkt/silver/wistia_events/"
    )
)

print(
    "Silver rows:",
    silver_df.count()
)

print(silver_df.columns)