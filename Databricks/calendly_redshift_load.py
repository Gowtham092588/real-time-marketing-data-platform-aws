# Databricks notebook source
# MAGIC %pip install redshift_connector

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------


import redshift_connector

# COMMAND ----------

# =======================================================
# 1. REDSHIFT CONFIGURATION
# =======================================================

REDSHIFT_HOST = "marketing-data-workgroup.229378727976.us-east-2.redshift-serverless.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DATABASE = "marketingdb"

REDSHIFT_URL = (
    f"jdbc:redshift://{REDSHIFT_HOST}:"
    f"{REDSHIFT_PORT}/{REDSHIFT_DATABASE}"
)

REDSHIFT_TEMP_DIR = (
    "s3://calendly-data-bkt/redshift-temp/"
)

REDSHIFT_IAM_ROLE = (
    "arn:aws:iam::229378727976:role/Redshift-Calendly-Gold-Load-Role"
)

# COMMAND ----------

# =======================================================
# 2. GET REDSHIFT CREDENTIALS
# =======================================================

REDSHIFT_USER = dbutils.secrets.get(
    scope="marketing-platform",
    key="redshift-user"
)

REDSHIFT_PASSWORD = dbutils.secrets.get(
    scope="marketing-platform",
    key="redshift-password"
)

print("Username loaded:", bool(REDSHIFT_USER))
print("Password loaded:", bool(REDSHIFT_PASSWORD))

# COMMAND ----------

# =======================================================
# 3. Paths
# =======================================================

GOLD_CHANNEL_PATH = (
    "s3://calendly-data-bkt/gold/channel_performance/"
)

GOLD_BOOKING_TIME_PATH = (
    "s3://calendly-data-bkt/gold/booking_time_analysis/"
)

GOLD_CAMPAIGN_PATH = (
    "s3://calendly-data-bkt/gold/campaign_attribution/"
)

GOLD_EMPLOYEE_LOAD_PATH = (
    "s3://calendly-data-bkt/gold/employee_meeting_load/"
)

# COMMAND ----------

# =======================================================
# 4. Read Gold Tables
# =======================================================

bookings_df = (
    spark.read
    .format("delta")
    .load(
        "s3://calendly-data-bkt/silver/calendly_bookings/"
    )
)

employee_meetings_df = (
    spark.read
    .format("delta")
    .load(
        "s3://calendly-data-bkt/silver/calendly_employee_meetings/"
    )
)

channel_df = (
    spark.read
    .format("delta")
    .load(GOLD_CHANNEL_PATH)
)

booking_time_df = (
    spark.read
    .format("delta")
    .load(GOLD_BOOKING_TIME_PATH)
)

campaign_df = (
    spark.read
    .format("delta")
    .load(GOLD_CAMPAIGN_PATH)
)

employee_df = (
    spark.read
    .format("delta")
    .load(GOLD_EMPLOYEE_LOAD_PATH)
)

print("bookings:", bookings_df.count())
print("employee meetings:", employee_meetings_df.count())
print("channel:", channel_df.count())
print("booking time:", booking_time_df.count())
print("campaign:", campaign_df.count())
print("employee:", employee_df.count())

# COMMAND ----------

# =======================================================
# 5. LOAD STAGING TABLES INTO REDSHIFT
# =======================================================

def load_to_redshift(df, table_name):

    (
        df.write
        .format("redshift")
        .mode("append")
        .option("host", REDSHIFT_HOST)
        .option("port", REDSHIFT_PORT)
        .option("database", REDSHIFT_DATABASE)
        .option("user", REDSHIFT_USER)
        .option("password", REDSHIFT_PASSWORD)
        .option("dbtable", table_name)
        .option("batchsize","1000")
        .save()
    )

    print(
        f"Loaded successfully: {table_name}"
    )

    

# COMMAND ----------

# =======================================================
# 6. Validate Staging Tables
# =======================================================

def validate_staging_table(table_name, business_key_columns):

    conn = redshift_connector.connect(
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE,
        port=REDSHIFT_PORT,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        )

        row_count = cursor.fetchone()[0]
        if row_count == 0:
            raise ValueError(f"Table {table_name} is empty")
        
        null_condition = " OR ".join(
            [
                f"{col} IS NULL"
                for col in business_key_columns
            ]
        )
        cursor.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {null_condition}"
        )

        null_count = cursor.fetchone()[0]
        if null_count > 0:
            raise ValueError(f"Table {table_name} has null values in business key columns")
        
        key_list = ", ".join(business_key_columns)
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM ( 
                SELECT 
                    {key_list}, 
                    COUNT(*) 
                FROM {table_name} 
                GROUP BY {key_list} 
                HAVING COUNT(*) > 1
            )d"""
        )

        duplicate_count = cursor.fetchone()[0]
        if duplicate_count > 0:
            raise ValueError(f"Table {table_name} has duplicate records")

        print(f"Validation passed: {table_name}")

        print(f"Rows: {row_count}")

    except Exception as e:
        print(f"Error validating table: {table_name}")
        print(e)
        raise

    finally:
        cursor.close()
        conn.close()



        


# COMMAND ----------

# =======================================================
# 7. Merge To Final Gold Table
# =======================================================

def merge_staging_to_gold(
    staging_table,
    target_table,
    business_key_columns,
    all_columns
):

    conn = redshift_connector.connect(
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE,
        port=REDSHIFT_PORT,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

    cursor = conn.cursor()

    try:
        # Build ON condition
        merge_condition = " AND ".join(
            [
                f"{target_table}.{col} = source.{col}"
                for col in business_key_columns
            ]
        )

        # Columns that should be updated
        update_columns = [
            col
            for col in all_columns
            if col not in business_key_columns
        ]

        update_clause = ", ".join(
            [
                f"{col} = source.{col}"
                for col in update_columns
            ]
        )

        insert_columns = ", ".join(
            all_columns
        )

        insert_values = ", ".join(
            [
                f"source.{col}"
                for col in all_columns
            ]
        )

        merge_sql = f"""
        MERGE INTO {target_table} 
        USING {staging_table} AS source

        ON {merge_condition}

        WHEN MATCHED THEN
            UPDATE SET
                {update_clause}

        WHEN NOT MATCHED THEN
            INSERT (
                {insert_columns}
            )
            VALUES (
                {insert_values}
            );
        """

        cursor.execute("BEGIN;")

        cursor.execute(
            merge_sql
        )

        cursor.execute(
            f"TRUNCATE TABLE {staging_table};"
        )
     
        conn.commit()

        print(
            f"MERGE completed: "
            f"{staging_table} -> {target_table}"
        )

    except Exception as e:

        conn.rollback()

        print(
            f"MERGE failed: {target_table}"
        )

        print(e)

        raise

    finally:

        cursor.close()
        conn.close()



# COMMAND ----------

# =======================================================
# 8. PUBLISH GOLD TABLES TO REDSHIFT
# =======================================================

def publish_gold_to_redshift(
    df,
    staging_table,
    target_table,
    business_key_columns,
    all_columns
):

    print(f"Publishing {target_table}")

    # 1. Load Databricks Gold into Redshift staging
    load_to_redshift(
        df,
        staging_table
    )

    # 2. Validate staging
    validate_staging_table(
        staging_table,
        business_key_columns
    )

    # 3. MERGE into final Gold
    #    and truncate staging only if MERGE succeeds
    merge_staging_to_gold(
        staging_table,
        target_table,
        business_key_columns,
        all_columns
    )

    print(f"Completed: {target_table}")

publish_gold_to_redshift(
    df=channel_df,
    staging_table="staging.channel_performance",
    target_table="gold.channel_performance",
    business_key_columns=[
        "report_date",
        "channel"
    ],
    all_columns=[
        "report_date",
        "channel",
        "total_bookings",
        "total_spend",
        "cost_per_booking",
        "load_timestamp"
    ]
)

print("Channel Performance Published Successfully to Redshift")

publish_gold_to_redshift(
    df=booking_time_df,
    staging_table="staging.booking_time_analysis",
    target_table="gold.booking_time_analysis",
    business_key_columns=[
        "report_date",
        "booking_hour",
        "channel"],
    all_columns=[
        "report_date",
        "day_of_week",
        "booking_hour",
        "channel",
        "total_bookings",
        "load_timestamp"
    ]
)

print("Booking Time Analysis Published Successfully to Redshift")

publish_gold_to_redshift(
    df=campaign_df,
    staging_table="staging.campaign_attribution",
    target_table="gold.campaign_attribution",
    business_key_columns=[
        "report_date",
        "channel",
        "utm_source",
        "utm_campaign",
        "utm_medium"
    ],
    all_columns=[
        "report_date",
        "channel",
        "utm_source",
        "utm_campaign",
        "utm_medium",
        "total_bookings",
        "load_timestamp"
    ]
)

print("Campaign Attribution Published Successfully to Redshift")

publish_gold_to_redshift(
    df=employee_df,
    staging_table="staging.employee_meeting_load",
    target_table="gold.employee_meeting_load",
    business_key_columns=[
        "employee_id",
        "week_start_date"
    ],
    all_columns=[
        "employee_id",
        "employee_name",
        "employee_email",
        "week_start_date",
        "total_meetings",
        "avg_meetings_per_week",
        "load_timestamp"
    ]
)

print("Employee Meeting Load Published Successfully to Redshift")

# =======================================================
# 2. PUBLISH BOOKINGS DETAIL TO REDSHIFT
# =======================================================

publish_gold_to_redshift(
    df=bookings_df,
    staging_table="staging.calendly_bookings_detail",
    target_table="gold.calendly_bookings_detail",
    business_key_columns=["invitee_id"],
    all_columns=[
        "webhook_event",
        "invitee_name",
        "invitee_email",
        "invitee_status",
        "invitee_timezone",
        "rescheduled",
        "invitee_created_at",
        "invitee_updated_at",
        "event_name",
        "event_status",
        "event_start_time",
        "event_end_time",
        "event_created_at",
        "event_updated_at",
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_content",
        "utm_term",
        "invitee_id",
        "scheduled_event_id",
        "event_type_id",
        "marketing_channel"
    ]
)

print("Calendly Booking Detail Published Successfully to Redshift")

# =======================================================
# PUBLISH EMPLOYEE DETAIL TO REDSHIFT
# =======================================================

publish_gold_to_redshift(
    df=employee_meetings_df,
    staging_table="staging.calendly_employee_meetings_detail",
    target_table="gold.calendly_employee_meetings_detail",
    business_key_columns=[
        "scheduled_event_id",
        "employee_id"
    ],
    all_columns=[
        "scheduled_event_id",
        "employee_id",
        "employee_name",
        "employee_email",
        "meeting_start_time",
        "event_updated_at"
    ]
)

print("Employee Meeting Details Published Successfully to Redshift")



# COMMAND ----------

# =======================================================
# Execute Redshift Stored Procedures
# =======================================================

def execute_stored_procedures(procedure_names):

    conn = redshift_connector.connect(
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE,
        port=REDSHIFT_PORT,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

    cursor = conn.cursor()

    try:

        cursor.execute("BEGIN;")

        for procedure_name in procedure_names:

            print(
                f"Running stored procedure: "
                f"{procedure_name}"
            )

            cursor.execute(
                f"CALL {procedure_name}();"
            )

        conn.commit()

        print(
            "Stored procedures completed successfully."
        )

    except Exception as e:

        conn.rollback()

        print(
            "Stored procedure execution failed."
        )

        print(e)

        raise

    finally:

        cursor.close()
        conn.close()

# COMMAND ----------

# =======================================================
# Refresh Calendly Dimensions and Facts
# =======================================================

execute_stored_procedures(
    [
        "gold.sp_load_dim_channel",
        "gold.sp_load_dim_employee",
        "gold.sp_load_fact_marketing_spend",
        "gold.sp_load_fact_calendly_booking"
    ]
)