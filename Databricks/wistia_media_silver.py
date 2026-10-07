# Databricks notebook source
# =======================================================
# Imports
# =======================================================

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# =======================================================
# Paths
# =======================================================

BRONZE_PATH = "s3://wistia-data-bkt/bronze/wistia/media/"

SILVER_PATH = "s3://wistia-data-bkt/silver/wistia_media/"

DQ_ERROR_PATH = "s3://wistia-data-bkt/silver/dq_errors/wistia_media/"

CHECKPOINT_PATH = (
    "s3://wistia-data-bkt/silver/checkpoints/wistia_media/"
)

# COMMAND ----------

# =======================================================
# Explicit Schema
# =======================================================

media_schema = StructType([
    StructField("archived", BooleanType(), True),

    StructField(
        "assets",
        ArrayType(
            StructType([
                StructField("content_type", StringType(), True),
                StructField("file_size", LongType(), True),
                StructField("height", LongType(), True),
                StructField("type", StringType(), True),
                StructField("url", StringType(), True),
                StructField("width", LongType(), True)
            ])
        ),
        True
    ),

    StructField("created", StringType(), True),
    StructField("description", StringType(), True),
    StructField("duration", DoubleType(), True),

    StructField(
        "folder",
        StructType([
            StructField("hashed_id", StringType(), True),
            StructField("id", LongType(), True),
            StructField("name", StringType(), True)
        ]),
        True
    ),

    StructField("hashed_id", StringType(), True),
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("progress", DoubleType(), True),
    StructField("protected", StringType(), True),
    StructField("section", StringType(), True),
    StructField("status", StringType(), True),

    StructField(
        "tags",
        ArrayType(StringType()),
        True
    ),

    StructField(
        "thumbnail",
        StructType([
            StructField("height", LongType(), True),
            StructField("url", StringType(), True),
            StructField("width", LongType(), True)
        ]),
        True
    ),

    StructField("type", StringType(), True),
    StructField("updated", StringType(), True)
])


# COMMAND ----------

# =======================================================
# Auto Loader read
# =======================================================

media_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(media_schema)
    .load(BRONZE_PATH)
)

# COMMAND ----------

# =======================================================
# Transformation
# =======================================================

def transform_wistia_media(bronze_df):

    # ============================================================
    # 1. SCHEMA DRIFT DETECTION
    # ============================================================

   # All columns we know can exist in Wistia Bronze
    known_columns = {
        "archived",
        "assets",
        "created",
        "description",
        "duration",
        "folder",
        "hashed_id",
        "id",
        "name",
        "progress",
        "protected",
        "section",
        "status",
        "tags",
        "thumbnail",
        "type",
        "updated"
    }

    # Columns actually required for our Silver processing
    required_columns = {
        "hashed_id",
        "id",
        "name",
        "duration",
        "status",
        "type",
        "archived",
        "created",
        "updated",
        "folder"
    }

    actual_columns = set(bronze_df.columns)

    # New fields added by Wistia
    new_columns = actual_columns - known_columns

    if new_columns:
        print(
            f"WARNING: New Bronze columns detected: "
            f"{sorted(new_columns)}"
        )
    else:
        print("No new top-level Bronze columns detected")


    # Only fail if fields we actually depend on disappear
    missing_required_columns = required_columns - actual_columns

    if missing_required_columns:
        raise ValueError(
            f"Schema drift detected. "
            f"Missing required Bronze columns: "
            f"{sorted(missing_required_columns)}"
        )

    # Validate required nested fields
    required_nested_columns = [
        "folder.hashed_id",
        "folder.name"
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
    # 2. SELECT REQUIRED COLUMNS
    # ============================================================

    silver_df = bronze_df.select(

        F.col("hashed_id").alias(
            "media_id"
        ),

        F.col("id").alias(
            "media_numeric_id"
        ),

        F.col("name").alias(
            "media_name"
        ),

        F.col("description"),

        F.col("duration").alias(
            "duration_seconds"
        ),

        F.col("status"),

        F.col("type").alias(
            "media_type"
        ),

        F.col("archived"),

        F.col("protected"),

        F.col("section"),

        F.col("folder.hashed_id").alias(
            "folder_id"
        ),

        F.col("folder.name").alias(
            "folder_name"
        ),

        F.col("created").alias(
            "created_at"
        ),

        F.col("updated").alias(
            "updated_at"
        )
    )


    # ============================================================
    # 3. NULL CLEANUP + STRING STANDARDIZATION
    # ============================================================

    columns_to_clean = [
        "media_id",
        "media_name",
        "description",
        "status",
        "media_type",
        "protected",
        "section",
        "folder_id",
        "folder_name"
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
    # 4. STANDARDIZE VALUES
    # ============================================================

    silver_df = (
        silver_df
        .withColumn(
            "media_name",
        F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.col("media_name"),
                    r"_",
                    " "
                ),
                r"\s*\(1080p\)\s*",
                " "
            ),
            r"\s*\(\d+\)\s*$",
                    ""
                )
            )
        )
        .withColumn(
            "media_name",
            F.initcap(
                F.col("media_name")
            )
        )
        .withColumn(
            "status",
            F.lower(
                F.col("status")
            )
        )

        .withColumn(
            "media_type",
            F.initcap(
                F.col("media_type")
            )
        )
    )


    # ============================================================
    # 5. CONVERT TIMESTAMPS
    # ============================================================

    timestamp_columns = [
        "created_at",
        "updated_at"
    ]

    for column_name in timestamp_columns:

        silver_df = silver_df.withColumn(
            column_name,
            F.to_timestamp(
                F.col(column_name)
            )
        )


    # ============================================================
    # 6. ADD LOAD TIMESTAMP
    # ============================================================

    silver_df = silver_df.withColumn(
        "load_timestamp",
        F.current_timestamp()
    )

    # ============================================================
    # 7. SCHEMA ENFORCEMENT
    # ============================================================

    expected_types = {

        "media_id": "string",

        "media_numeric_id": "bigint",

        "media_name": "string",

        "description": "string",

        "duration_seconds": "double",

        "status": "string",

        "media_type": "string",

        "archived": "boolean",

        "protected": "string",

        "section": "string",

        "folder_id": "string",

        "folder_name": "string",

        "created_at": "timestamp",

        "updated_at": "timestamp",

        "load_timestamp": "timestamp"
    }

    actual_types = dict(
        silver_df.dtypes
    )

    for column_name, data_type in expected_types.items():

        actual_type = actual_types.get(
            column_name
        )

        if actual_type is None:

            raise ValueError(
                f"Schema enforcement failed. "
                f"Missing column: {column_name}"
            )

        if actual_type != data_type:

            raise ValueError(
                f"Schema enforcement failed for "
                f"{column_name}. "
                f"Expected data type: {data_type}. "
                f"Actual data type: {actual_type}"
            )

    print(
        "Schema/type enforcement passed"
    )


    # ============================================================
    # 8. BUSINESS KEY VALIDATION
    # ============================================================

    invalid_business_key_count = (
        silver_df
        .filter(
            F.col("media_id").isNull()
            | (
                F.trim(
                    F.col("media_id")
                ) == ""
            )
        )
        .count()
    )

    if invalid_business_key_count > 0:
        raise ValueError(
            f"Business key validation failed. "
            f"Invalid media_id count: "
            f"{invalid_business_key_count}"
        )

    print(
        "Business key validation passed"
    )


    # ============================================================
    # 9. BASIC DATA QUALITY VALIDATION
    # ============================================================

    invalid_duration_count = (

        silver_df
        .filter(
            F.col("duration_seconds") < 0
        )
        .count()
    )

    if invalid_duration_count > 0:
        raise ValueError(
            f"Data quality validation failed. "
            f"Negative duration count: "
            f"{invalid_duration_count}"
        )


    invalid_timestamp_count = (
        silver_df
        .filter(
            F.col("created_at").isNull()
            | F.col("updated_at").isNull()
        )
        .count()
    )

    if invalid_timestamp_count > 0:

        raise ValueError(
            f"Data quality validation failed. "
            f"Invalid timestamp count: "
            f"{invalid_timestamp_count}"
        )

    print(
        "Data quality validation passed"
    )

    return silver_df

# bronze_df = spark.read.json(BRONZE_PATH)
# silver_df = transform_wistia_media(bronze_df)
# display(silver_df)

# COMMAND ----------

# =======================================================
# ROW LEVEL QUALITY CHECKS
# =======================================================

def add_media_dq_errors(df):

    return (
        df.withColumn(
            "dq_error_reason",

            F.when(
                F.col("media_id").isNull()
                | (F.trim(F.col("media_id")) == ""),
                F.lit("INVALID_BUSINESS_KEY")
            )

            .when(
                F.col("media_numeric_id").isNull(),
                F.lit("MISSING_MEDIA_NUMERIC_ID")
            )

            .when(
                F.col("media_name").isNull()
                | (F.trim(F.col("media_name")) == ""),
                F.lit("MISSING_MEDIA_NAME")
            )

            .when(
                F.col("duration_seconds") < 0,
                F.lit("INVALID_DURATION")
            )

            .when(
                F.col("created_at").isNull(),
                F.lit("INVALID_CREATED_AT")
            )

            .when(
                F.col("updated_at").isNull(),
                F.lit("INVALID_UPDATED_AT")
            )

            .when(
                F.col("updated_at") < F.col("created_at"),
                F.lit("UPDATED_AT_BEFORE_CREATED_AT")
            )

            .otherwise(F.lit(None))
        )
    )

# COMMAND ----------

# =======================================================
# DeDuplication
# =======================================================

def deduplicate_media(df):

    window_spec = (
        Window
        .partitionBy("media_id")
        .orderBy(
            F.col("updated_at").desc(),
            F.col("load_timestamp").desc()
        )
    )

    return (
        df
        .withColumn(
            "row_num",
            F.row_number().over(window_spec)
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

# COMMAND ----------

# =======================================================
# Delta Merge
# =======================================================

def merge_media_to_silver(df):

    final_df = df.drop("_rescued_data")

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

# =======================================================
# Process Media Batch
# =======================================================

def process_media_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    transformed_df = transform_wistia_media(
        batch_df
    )

    validated_df = add_media_dq_errors(transformed_df)

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
    
    silver_df = deduplicate_media(good_df)

    if DeltaTable.isDeltaTable(spark,SILVER_PATH):
        merge_media_to_silver(silver_df)
    else:
        print(
            "Silver Delta table not found. "
            "Performing initial load."
        )

        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .save(SILVER_PATH)
        )

        print(
            f"Initial Silver Delta load completed successfully. "
            f"Rows written: {silver_df.count()}"
        )
    




# COMMAND ----------

# =======================================================
# Start Incremental Load
# =======================================================

query = (
    media_stream_df.writeStream
    .foreachBatch(process_media_batch)
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

media_silver_df = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)

display(media_silver_df)