# Databricks notebook source
# ============================================
# 1. Imports
# ============================================
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# ============================================
# 2.Paths
# ============================================
CALENDLY_SILVER_PATH = (
    "s3://calendly-data-bkt/silver/calendly_bookings/"
)

MARKETING_SPEND_SILVER_PATH = (
    "s3://calendly-data-bkt/silver/marketing_spend/"
)

EMPLOYEE_MEETING_SILVER_PATH = (
    "s3://calendly-data-bkt/"
    "silver/calendly_employee_meetings/"
)

GOLD_CHANNEL_PERFORMANCE_PATH = (
    "s3://calendly-data-bkt/gold/channel_performance/"
)

GOLD_BOOKING_TIME_PATH = (
    "s3://calendly-data-bkt/gold/booking_time_analysis/"
)

GOLD_CAMPAIGN_ATTRIBUTION_PATH = (
    "s3://calendly-data-bkt/gold/campaign_attribution/"
)

GOLD_EMPLOYEE_LOAD_PATH = (
    "s3://calendly-data-bkt/gold/employee_meeting_load/"
)

# COMMAND ----------

# ============================================
# 3. Read Calendly and Market Spend Data
# ============================================

calendly_df = (
    spark.read
    .format("delta")
    .load(CALENDLY_SILVER_PATH)
)

marketing_spend_df = (
    spark.read
    .format("delta")
    .load(MARKETING_SPEND_SILVER_PATH)
)

employee_meeting_df = (
    spark.read
    .format("delta")
    .load(
        EMPLOYEE_MEETING_SILVER_PATH
    )
)

print("Calendly rows:", calendly_df.count())
print("Marketing Spend rows:", marketing_spend_df.count())
print("Employee Meeting rows:", employee_meeting_df.count())
calendly_df.printSchema()
marketing_spend_df.printSchema()
employee_meeting_df.printSchema()

# COMMAND ----------

# ============================================
#4. Build Gold Channel Performance
# ============================================

bookings_df = (
    calendly_df

    .filter(
        F.col("marketing_channel").isin(
            "facebook_paid_ads",
            "youtube_paid_ads",
            "tiktok_paid_ads"
        )
    )

    .withColumn(
        "report_date",
        F.to_date(
            F.col("event_start_time")
        )
    )

    .groupBy(
        "report_date",
        "marketing_channel"
    )

    .agg(
        F.countDistinct(
            "invitee_id"
        ).alias(
            "total_bookings"
        )
    )

    .withColumnRenamed(
        "marketing_channel",
        "channel"
    )
)

spend_df = (
    marketing_spend_df
    .groupBy(
        F.col("spend_date").alias("report_date"),
        F.col("channel")
        )
    .agg(
        F.sum("spend_amount").alias("total_spend")
    )
)

gold_channel_performance_df = (
    spend_df.alias("s")
    .join(
        bookings_df.alias("b"),
        (
            F.col("s.report_date") == F.col("b.report_date")
        ) 
        &
        (
            F.col("s.channel") == F.col("b.channel")
        ),
    "left"
).select(
        F.col("s.report_date"),
        F.col("s.channel"),
        F.col("b.total_bookings"),
        F.col("s.total_spend")
    )
)

gold_channel_performance_df = (
    gold_channel_performance_df
    .withColumn(
        "total_bookings",
        F.coalesce(
            F.col("total_bookings"),
            F.lit(0)
        )
    )
    .withColumn(
        "cost_per_booking",
        F.when(
            F.col("total_bookings") > 0,
            F.col("total_spend") / F.col("total_bookings")
        ).otherwise(F.lit(None))
    )
     .withColumn(
        "cost_per_booking",
        F.round(
            F.col("cost_per_booking"),
            2
        )
    )
     .withColumn(
        "load_timestamp",
        F.current_timestamp()
    )
)

gold_channel_performance_df = gold_channel_performance_df.select(
    "report_date",
    "channel",
    "total_bookings",
    "total_spend",
    "cost_per_booking",
    "load_timestamp"
)

display(gold_channel_performance_df
        .orderBy(
            F.col("report_date").desc(),
            F.col("channel")
        ))


# COMMAND ----------

# =============================================================================
#5. Reusable Write Function to write Gold Table to S3 with Delta Merge Function 
# =============================================================================

def write_gold_table(
    source_df,
    gold_path,
    merge_condition
):

    if DeltaTable.isDeltaTable(
        spark,
        gold_path
    ):

        target_table = DeltaTable.forPath(
            spark,
            gold_path
        )

        (
            target_table
            .alias("target")
            .merge(
                source_df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"MERGE completed: {gold_path}")

    else:

        (
            source_df
            .write
            .format("delta")
            .mode("overwrite")
            .save(gold_path)
        )

        print(f"Gold table created: {gold_path}")

# COMMAND ----------

# =======================================================
#6. Write Gold Channel Performance Table
# =======================================================

write_gold_table(
    gold_channel_performance_df, 
    GOLD_CHANNEL_PERFORMANCE_PATH,
    """
    target.report_date = source.report_date
    AND
    target.channel = source.channel
    """ 
    )


# COMMAND ----------

# ============================================
#7. Build Gold Booking Time Analysis
# ============================================

gold_booking_time_df = (
    calendly_df

    .filter(
        F.col("marketing_channel").isin(
            "facebook_paid_ads",
            "youtube_paid_ads",
            "tiktok_paid_ads"
        )
    )

    .withColumn(
        "report_date",
        F.to_date("event_start_time")
    )

    .withColumn(
        "booking_hour",
        F.hour("event_start_time")
    )

    .withColumn(
        "day_of_week",
        F.date_format(
            "event_start_time",
            "EEEE"
        )
    )

    .groupBy(
        "report_date",
        "day_of_week",
        "booking_hour",
        "marketing_channel"
    )

    .agg(
        F.countDistinct(
            "invitee_id"
        ).alias("total_bookings")
    )

    .withColumnRenamed(
        "marketing_channel",
        "channel"
    )

    .withColumn(
        "load_timestamp",
        F.current_timestamp()
    )
)

display(gold_booking_time_df
        .orderBy(
            F.col("report_date").desc(),
            F.col("channel"),
            F.col("day_of_week"),
            F.col("booking_hour")
        ))



# COMMAND ----------

# =======================================================
#8. Write Gold Booking Time Analysis
# =======================================================

write_gold_table(
    gold_booking_time_df, 
    GOLD_BOOKING_TIME_PATH,
    """
    target.report_date = source.report_date
    AND target.booking_hour = source.booking_hour
    AND target.channel = source.channel
    """ 
    )

# COMMAND ----------

# =======================================================
#9. Build Gold Campaign Attribution
# =======================================================
campaign_df = (
    calendly_df
    .withColumn(
        "report_date",
        F.to_date("event_start_time")
    )

    .withColumn(
        "utm_source",
        F.coalesce(
            F.col("utm_source"),
            F.lit("unattributed")
        )
    )

    .withColumn(
        "utm_campaign",
        F.coalesce(
            F.col("utm_campaign"),
            F.lit("unattributed")
        )
    )

    .withColumn(
        "utm_medium",
        F.coalesce(
            F.col("utm_medium"),
            F.lit("unattributed")
        )
    )

    .groupBy(
        "report_date",
        "marketing_channel",
        "utm_source",
        "utm_campaign",
        "utm_medium"
    )

    .agg(
        F.countDistinct(
            "invitee_id"
        ).alias("total_bookings")
    )

    .withColumnRenamed(
        "marketing_channel",
        "channel"
    )

    .withColumn(
        "load_timestamp",
        F.current_timestamp()
    )
)

display(campaign_df
        .orderBy(
            F.col("report_date").desc(),
            F.col("channel")
            )
        )


# COMMAND ----------

# =======================================================
#10. Write Gold Campaign Attribution
# =======================================================

write_gold_table(
    campaign_df,
    GOLD_CAMPAIGN_ATTRIBUTION_PATH,
    """
    target.report_date = source.report_date
    AND target.channel = source.channel
    AND target.utm_source = source.utm_source
    AND target.utm_campaign = source.utm_campaign
    AND target.utm_medium = source.utm_medium
    """
)

# COMMAND ----------

# =======================================================
#9. Build Gold Employee Load
# =======================================================


gold_employee_load_df = (
    employee_meeting_df

    .withColumn(
        "week_start_date",
        F.to_date(
            F.date_trunc(
                "week",
                F.col("meeting_start_time")
            )
        )
    )

    .groupBy(
        "employee_id",
        "employee_name",
        "employee_email",
        "week_start_date"
    )

    .agg(
        F.countDistinct(
            "scheduled_event_id"
        ).alias(
            "total_meetings"
        )
    )

    .withColumn(
        "load_timestamp",
        F.current_timestamp()
    )
)

employee_window = Window.partitionBy(
    "employee_id"
)

gold_employee_load_df = (
    gold_employee_load_df

    .withColumn(
        "avg_meetings_per_week",
        F.round(
            F.avg("total_meetings").over(
                employee_window
            ),
            2
        )
    )

    .withColumn(
        "load_timestamp",
        F.current_timestamp()
    )
)



# COMMAND ----------

# =======================================================
#10. Write Gold Employee Load
# =======================================================

write_gold_table(
    gold_employee_load_df,
    GOLD_EMPLOYEE_LOAD_PATH,
    """
    target.employee_id = source.employee_id
    AND target.week_start_date = source.week_start_date
    """)

# COMMAND ----------

# =======================================================
#11. Verify Gold Table
# =======================================================

channel_gold_df = (
    spark.read
    .format("delta")
    .load(GOLD_CHANNEL_PERFORMANCE_PATH)
)

booking_time_gold_df = (
    spark.read
    .format("delta")
    .load(GOLD_BOOKING_TIME_PATH)
)

campaign_gold_df = (
    spark.read
    .format("delta")
    .load(GOLD_CAMPAIGN_ATTRIBUTION_PATH)
)

employee_load_gold_df = (
    spark.read
    .format("delta")
    .load(GOLD_EMPLOYEE_LOAD_PATH)
)

print(
    "employee_load:",
    employee_load_gold_df.count()
)

print(
    "channel_performance:",
    channel_gold_df.count()
)

print(
    "booking_time_analysis:",
    booking_time_gold_df.count()
)

print(
    "campaign_attribution:",
    campaign_gold_df.count()
)

# COMMAND ----------

# =======================================================
#12. Business Key Verification
# =======================================================
channel_performance_duplicate_count = (
    gold_channel_performance_df
    .groupBy("report_date",
             "channel")
    .count()
    .filter(F.col("count")>1)
    .count()
)

print(f"Duplicate channel_performance business keys: {channel_performance_duplicate_count}")

booking_time_duplicate_count = (
    gold_booking_time_df
    .groupBy("report_date",
             "channel",
             "booking_hour")
    .count()
    .filter(F.col("count")>1)
    .count()
)

print(f"Duplicate booking_time business keys: {booking_time_duplicate_count}")

campaign_duplicate_count = (
    campaign_gold_df
    .groupBy("report_date",
             "channel",
             "utm_source",
             "utm_campaign",
             "utm_medium"
     )
    .count()
    .filter(F.col("count")>1)
    .count()
)

print(f"Duplicate campaign business keys: {campaign_duplicate_count}")

employee_load_duplicate_count = (
    employee_load_gold_df
    .groupBy("employee_id",
             "week_start_date")
    .count()
    .filter(F.col("count")>1)
    .count()
)

print(f"Duplicate employee_load business keys: {employee_load_duplicate_count}")    