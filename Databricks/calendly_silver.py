# Databricks notebook source
# ============================================
# 1. IMPORTS
# ============================================

from pyspark.sql import functions as F
from pyspark.sql import Window as W 
from delta.tables import DeltaTable

# COMMAND ----------

# ============================================
# 2. PATHS
# ============================================

BRONZE_PATH = "s3://calendly-data-bkt/bronze/calendly/"
SILVER_PATH = "s3://calendly-data-bkt/silver/calendly_bookings/"
CHECKPOINT_PATH = "s3://calendly-data-bkt/silver/checkpoints/calendly_silver/"
SCHEMA_PATH = "s3://calendly-data-bkt/silver/checkpoints/calendly_silver_schema/"
EMPLOYEE_MEETING_SILVER_PATH = (
    "s3://calendly-data-bkt/"
    "silver/calendly_employee_meetings/"
)
DQ_CALENDLY_ERROR_PATH = ("s3://calendly-data-bkt/silver/dq_errors/calendly_bookings/")
DQ_EMPLOYEE_ERROR_PATH = ("s3://calendly-data-bkt/silver/dq_errors/employee_meeting_load/")

# COMMAND ----------

# ============================================
# 3. GET BRONZE JSON SCHEMA
# ============================================

bronze_schema = (
    spark.read
    .json(BRONZE_PATH)
    .schema
)

print(bronze_schema)

# COMMAND ----------

# ============================================
# 4. READ BRONZE DATA & AUTO LOADER
# ============================================

bronze_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(bronze_schema)
    .load(BRONZE_PATH)
)

bronze_stream_df.printSchema()



# COMMAND ----------

# ============================================
# 5. TRANSFORM FUNCTION
# ============================================

from pyspark.sql import functions as F

def transform_calendly(bronze_df):

    #Schema Drift Detection
    expected_columns = {
    "created_at",
    "created_by",
    "event",
    "payload"
    }

    actual_columns = set(bronze_df.columns)

    new_columns = (actual_columns - set(expected_columns))

    if new_columns:
        print(
            f"WARNING: New Bronze columns detected: "
            f"{sorted(new_columns)}"
        )
    else:
        print(
            "No new top-level Bronze columns detected"
        )

    missing_columns = (set(expected_columns) - actual_columns)

    if missing_columns:
        raise ValueError(
            f"Schema drift detected. "
            f"Missing required top-level columns: "
            f"{sorted(missing_columns)}"
        )

    required_nested_columns = [
        "payload.uri",
        "payload.name",
        "payload.email",
        "payload.status",
        "payload.timezone",
        "payload.rescheduled",
        "payload.created_at",
        "payload.updated_at",
        "payload.scheduled_event.uri",
        "payload.scheduled_event.event_type",
        "payload.scheduled_event.name",
        "payload.scheduled_event.status",
        "payload.scheduled_event.start_time",
        "payload.scheduled_event.end_time",
        "payload.scheduled_event.created_at",
        "payload.scheduled_event.updated_at"
    ] 

    for column_name in required_nested_columns:
        try:
            bronze_df.select(F.col(column_name))
        
        except Exception:

            raise ValueError(
                f"Schema drift detected. "
                f"Required field is missing: "
                f"{column_name}"
            )

    silver_df = bronze_df.select(

        F.col("event").alias("webhook_event"),

        F.col("payload.uri").alias("invitee_uri"),

        F.col("payload.name").alias("invitee_name"),

        F.col("payload.email").alias("invitee_email"),

        F.col("payload.status").alias("invitee_status"),

        F.col("payload.timezone").alias("invitee_timezone"),

        F.col("payload.rescheduled").alias("rescheduled"),

        F.col("payload.created_at").alias("invitee_created_at"),

        F.col("payload.updated_at").alias("invitee_updated_at"),

        F.col("payload.scheduled_event.uri").alias(
            "scheduled_event_uri"
        ),

        F.col("payload.scheduled_event.event_type").alias(
            "event_type_uri"
        ),

        F.col("payload.scheduled_event.name").alias(
            "event_name"
        ),

        F.col("payload.scheduled_event.status").alias(
            "event_status"
        ),

        F.col("payload.scheduled_event.start_time").alias(
            "event_start_time"
        ),

        F.col("payload.scheduled_event.end_time").alias(
            "event_end_time"
        ),

        F.col("payload.scheduled_event.created_at").alias(
            "event_created_at"
        ),

        F.col("payload.scheduled_event.updated_at").alias(
            "event_updated_at"
        ),

        F.col("payload.tracking.utm_source").alias(
            "utm_source"
        ),

        F.col("payload.tracking.utm_medium").alias(
            "utm_medium"
        ),

        F.col("payload.tracking.utm_campaign").alias(
            "utm_campaign"
        ),

        F.col("payload.tracking.utm_content").alias(
            "utm_content"
        ),

        F.col("payload.tracking.utm_term").alias(
            "utm_term"
        )
    )
    #Extract IDs
    silver_df = (
        silver_df
        .withColumn(
            "invitee_id",
            F.regexp_extract(F.col("invitee_uri"), r"([^/]+)$", 1)
        )
        .withColumn(
            "scheduled_event_id",
            F.regexp_extract(F.col("scheduled_event_uri"), r"([^/]+)$", 1)
        )
        .withColumn(
            "event_type_id",
            F.regexp_extract(F.col("event_type_uri"), r"([^/]+)$", 1)
        ).drop(
        "invitee_uri",
        "scheduled_event_uri",
        "event_type_uri"
        )
    )

    # Null Clean Up

    columns_to_clean = [
        "webhook_event",
        "invitee_name",
        "invitee_email",
        "invitee_status",
        "invitee_timezone",
        "event_name",
        "event_status",
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_content",
        "utm_term"]

    for column_name in columns_to_clean:
        silver_df = silver_df.withColumn(
            column_name,
            F.when(
                F.col(column_name).isNull()
                | (F.trim(F.col(column_name)) == "")
                | (F.lower(F.trim(F.col(column_name))) == "none"),
                F.lit(None)
            ).otherwise(F.trim(F.col(column_name)))
        )

    # Convert to timestamp
    timestamp_columns = [
        "event_start_time",
        "event_end_time",
        "event_created_at",
        "event_updated_at",
        "invitee_created_at",
        "invitee_updated_at"
        ]

    for column_name in timestamp_columns:
        silver_df = silver_df.withColumn(
            column_name,
            F.to_timestamp(F.col(column_name))
         )

    silver_df = silver_df.withColumn(
        "invitee_name",
        F.initcap(F.col("invitee_name"))
    )

    # Marketing channel mapping
    silver_df = silver_df.withColumn(
        "marketing_channel",
        F.when(
            F.col("event_type_id")
            == "a258c5e3-2e5f-4733-acb9-39cf9f70d537",
            "facebook_paid_ads"
        )
        .when(
            F.col("event_type_id")
            == "7fba5121-b4da-4ea6-a06e-e3191c878bb3",
            "youtube_paid_ads"
        )
        .when(
            F.col("event_type_id")
            == "3810efdf-db1e-4813-aaf7-96d9ef6173e5",
            "tiktok_paid_ads"
        )
        .otherwise("other")
        )
    
    # Schema Validation
    required_columns = [
        "invitee_id",
        "scheduled_event_id",
        "event_type_id",
        "event_start_time"
        ]

    missing_required_columns = [column_name for column_name in required_columns if column_name not in silver_df.columns]

    if missing_required_columns:
        raise  ValueError(
            f"Missing required columns: {missing_required_columns}"
        )
    
    # Schema Enforcement

    expected_types = {
        "invitee_id": "string",
        "scheduled_event_id": "string",
        "event_type_id": "string",
        "invitee_name": "string",
        "invitee_email": "string",
        "invitee_status": "string",
        "invitee_timezone": "string",
        "event_name": "string",
        "event_status": "string",
        "event_start_time": "timestamp",
        "event_end_time": "timestamp",
        "invitee_created_at": "timestamp",
        "invitee_updated_at": "timestamp",
        "event_created_at": "timestamp",
        "event_updated_at": "timestamp",
        "rescheduled": "boolean",
        "marketing_channel": "string"
        }

    actual_types = dict(silver_df.dtypes)

    for column_name, data_type in expected_types.items():
        actual_type = actual_types.get(column_name)

        if actual_type is None:
            raise ValueError(
                f"Schema enforcement failed. "
                f"Missing column: {column_name}"
            )

        if actual_type != data_type:
            raise ValueError(
                f"Schema enforcement failed for {column_name}."
                f"Expected data type: {data_type} "
                f"Actual data type: {actual_type}"
            )

    print("Schema/type enforcement passed")


    # Business Key Validation
    invalid_business_key_count = (
        silver_df
        .filter(
            F.col("invitee_id").isNull()
            | (F.trim(F.col("invitee_id")) == "")
        )
        .count()
        )

    if invalid_business_key_count > 0:
        raise ValueError(
            f"Business key validation failed. "
            f"Invalid invitee_id count: {invalid_business_key_count}"
        )
        
    return silver_df

# COMMAND ----------

# =======================================================
# ROW LEVEL QUALITY CHECKS FOR CALENDLY 
# =======================================================

def add_calendly_dq_errors(df):

    return (
        df.withColumn(
            "dq_error_reason",

            F.when(
                F.col("invitee_id").isNull()
                | (F.trim(F.col("invitee_id")) == ""),
                F.lit("INVALID_BUSINESS_KEY")
            )

            .when(
                F.col("scheduled_event_id").isNull()
                | (F.trim(F.col("scheduled_event_id")) == ""),
                F.lit("MISSING_SCHEDULED_EVENT_ID")
            )

            .when(
                F.col("event_type_id").isNull()
                | (F.trim(F.col("event_type_id")) == ""),
                F.lit("MISSING_EVENT_TYPE_ID")
            )

            .when(
                F.col("event_start_time").isNull(),
                F.lit("MISSING_EVENT_START_TIME")
            )

            .when(
                F.col("event_end_time").isNull(),
                F.lit("MISSING_EVENT_END_TIME")
            )
            .when(
                F.col("event_end_time")
                < F.col("event_start_time"),
                F.lit("EVENT_END_BEFORE_START")
            )
            .when(
                F.col("invitee_updated_at").isNull(),
                F.lit("MISSING_INVITEE_UPDATED_AT")
            )
            .otherwise(F.lit(None))
        )
    )

# COMMAND ----------

# ============================================
# 6. DEDUPLICATION FUNCTION
# ============================================

def deduplication_calendly(silver_df):

    duplicate_count = (
        silver_df
        .groupBy("invitee_id")
        .count()
        .filter(F.col("count")>1)
        .count()
    )

    if duplicate_count > 0:
        print(
            f"WARNING: Found {duplicate_count} duplicate invitee_id values. "
            f"Keeping the latest record based on invitee_updated_at."
        )
    else:
        print("No duplicate invitee_id values detected.")


    window_spec = W.partitionBy("invitee_id").orderBy(F.col("invitee_updated_at").desc_nulls_last())

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

# ============================================
# 7. DELTA MERGE FUNCTION
# ============================================

def merge_calendly_delta(silver_df):

    silver_table = DeltaTable.forPath(
        spark,
        SILVER_PATH
    )

    (
        silver_table.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.invitee_id = source.invitee_id"
        )
        .whenMatchedUpdateAll(
            condition="""
                source.invitee_updated_at >= target.invitee_updated_at
                OR target.invitee_updated_at IS NULL
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Delta MERGE completed successfully.")



# COMMAND ----------

# ============================================
# 8. Employee Meeting Function
# ============================================
def transform_employee_meetings(bronze_df):

    employee_df = (
        bronze_df
        .withColumn(
            "employee",
            F.explode(
                F.col(
                    "payload.scheduled_event.event_memberships"
                )
            )
        )

        # meeting id
        .withColumn(
            "scheduled_event_id",
            F.regexp_extract(
                F.col(
                    "payload.scheduled_event.uri"
                ),
                r"([^/]+)$",
                1
            )
        )

        # employee id
        .withColumn(
            "employee_id",
            F.regexp_extract(
                F.col("employee.user"),
                r"([^/]+)$",
                1
            )
        )

        .withColumn(
            "employee_name",
            F.trim(
                F.col("employee.user_name")
            )
        )

        .withColumn(
            "employee_email",
            F.lower(
                F.trim(
                    F.col("employee.user_email")
                )
            )
        )

        .withColumn(
            "meeting_start_time",
            F.to_timestamp(
                F.col(
                    "payload.scheduled_event.start_time"
                )
            )
        )

        .withColumn(
            "event_updated_at",
            F.to_timestamp(
                F.col(
                    "payload.scheduled_event.updated_at"
                )
            )
        )

        .select(
            "scheduled_event_id",
            "employee_id",
            "employee_name",
            "employee_email",
            "meeting_start_time",
            "event_updated_at"
        )
    )

    return employee_df

# COMMAND ----------

# =======================================================
# ROW LEVEL QUALITY CHECKS FOR EMPLOYEE_LOAD
# =======================================================

def add_employee_dq_errors(df):

    return (
        df.withColumn(
            "dq_error_reason",

            F.when(
                F.col("scheduled_event_id").isNull()
                | (F.col("scheduled_event_id") == ""),
                F.lit("INVALID_BUSINESS_KEY")
            )

            .when(
                F.col("employee_id").isNull()
                | (F.col("employee_id") == ""),
                F.lit("MISSING_EMPLOYEE_ID")
            )

            .when(
                F.col("meeting_start_time").isNull(),
                F.lit("MISSING_EVENT_START_TIME")
            )
            .when(
                F.col("event_updated_at").isNull(),
                F.lit("MISSING_EVENT_UPDATED_AT")
            )
            .otherwise(F.lit(None))
        )
    )

# COMMAND ----------

# ============================================
# 9. EMPLOYEE DEDUPLICATION FUNCTION
# ============================================

def deduplicate_employee_meetings(employee_df):

    window_spec = (
        W
        .partitionBy(
            "scheduled_event_id",
            "employee_id"
        )
        .orderBy(
            F.col(
                "event_updated_at"
            ).desc_nulls_last()
        )
    )

    return (
        employee_df
        .withColumn(
            "row_number",
            F.row_number().over(
                window_spec
            )
        )
        .filter(
            F.col("row_number") == 1
        )
        .drop(
            "row_number"
        )
    )

# COMMAND ----------

# ============================================
# 10. EMPLOYEE DELTA MERGE FUNCTION
# ============================================
def merge_employee_meetings(employee_df):

    employee_table = DeltaTable.forPath(
        spark,
        EMPLOYEE_MEETING_SILVER_PATH
    )

    (
        employee_table
        .alias("target")
        .merge(
            employee_df.alias("source"),
            """
            target.scheduled_event_id
                = source.scheduled_event_id
            AND
            target.employee_id
                = source.employee_id
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        "Employee meeting Silver MERGE completed."
    )

# COMMAND ----------


# ============================================
# 8. Process each Auto Loader batch
# ============================================

def process_batch(batch_df, batch_id):

    print(f"Processing batch: {batch_id}")

    source_rows = batch_df.count()

    print(f"Batch {batch_id} source rows: {source_rows}")

    silver_df = transform_calendly(batch_df)

    validated_df = add_calendly_dq_errors(silver_df)

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
            .save(DQ_CALENDLY_ERROR_PATH)
        )
    

    silver_df = deduplication_calendly(silver_df)

    if DeltaTable.isDeltaTable(spark,SILVER_PATH):
        merge_calendly_delta(silver_df)
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
    
    employee_df = transform_employee_meetings(batch_df)

    validated_df = add_employee_dq_errors(employee_df)

    good_emp_df = (
        validated_df
        .filter(F.col("dq_error_reason").isNull())
        .drop("dq_error_reason")
    )

    bad_emp_df = (
        validated_df
        .filter(F.col("dq_error_reason").isNotNull())
    )

    if not bad_emp_df.isEmpty():

        (
            bad_emp_df.write
            .format("json")
            .mode("append")
            .save(DQ_EMPLOYEE_ERROR_PATH)
        )
    


    employee_df = deduplicate_employee_meetings(good_emp_df)

    if DeltaTable.isDeltaTable(
        spark,
        EMPLOYEE_MEETING_SILVER_PATH
    ):

        merge_employee_meetings(
            employee_df
        )

    else:

        (
            employee_df.write
            .format("delta")
            .mode("overwrite")
            .save(
                EMPLOYEE_MEETING_SILVER_PATH
            )
        )

    print(
        f"Batch {batch_id} completed successfully."
    )

# COMMAND ----------

query = (
    bronze_stream_df.writeStream
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

# COMMAND ----------

employee_meeting_df = (
    spark.read
    .format("delta")
    .load(
        EMPLOYEE_MEETING_SILVER_PATH
    )
)

display(employee_meeting_df.orderBy("scheduled_event_id","employee_id"))

# COMMAND ----------

calendly_booking_df =(
    spark.read
    .format("delta")
    .load(
        SILVER_PATH)
)

display(calendly_booking_df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC