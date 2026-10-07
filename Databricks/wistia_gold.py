# Databricks notebook source
# ============================================
# 1. Imports
# ============================================
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# ============================================
# 2.Paths
# ============================================
MEDIA_SILVER_PATH = (
    "s3://wistia-data-bkt/silver/wistia_media/"
)

STATS_SILVER_PATH = (
    "s3://wistia-data-bkt/silver/wistia_stats/"
)

ENGAGEMENT_SILVER_PATH = (
    "s3://wistia-data-bkt/silver/wistia_engagement/"
)

EVENTS_SILVER_PATH = (
    "s3://wistia-data-bkt/silver/wistia_events/"
)

VISITORS_SILVER_PATH = (
    "s3://wistia-data-bkt/silver/wistia_visitors/"
)


MEDIA_PERFORMANCE_GOLD_PATH = (
    "s3://wistia-data-bkt/gold/wistia_media_performance/"
)

VISITOR_ENGAGEMENT_GOLD_PATH = (
    "s3://wistia-data-bkt/gold/wistia_visitor_engagement/"
)

# COMMAND ----------

# ============================================
# 3. Read media, visitors, events
# ============================================
media_df = spark.read.format("delta").load(
    MEDIA_SILVER_PATH
)

stats_df = spark.read.format("delta").load(
    STATS_SILVER_PATH
)

events_df = spark.read.format("delta").load(
    EVENTS_SILVER_PATH
)

visitors_df = spark.read.format("delta").load(
    VISITORS_SILVER_PATH
)

# COMMAND ----------

# ============================================
# 4. Build Gold MEDIA Performance
# ============================================
gold_media_performance_df = (
    media_df.alias("m")

    .join(
        stats_df.alias("s"),
        F.col("m.media_id") == F.col("s.media_id"),
        "left"
    )

    .select(

        F.col("m.media_id"),

        F.col("m.media_numeric_id"),

        F.col("m.media_name"),

        F.col("m.duration_seconds"),

        F.col("m.status"),

        F.col("m.media_type"),

        F.col("m.folder_name"),

        F.col("m.created_at"),

        F.col("m.updated_at"),

        F.col("s.engagement"),

        F.col("s.hours_watched"),

        F.col("s.load_count"),

        F.col("s.play_count"),

        F.col("s.play_rate"),

        F.col("s.visitors").alias(
            "total_visitors"
        ),

        F.current_timestamp().alias(
            "load_timestamp"
        )
    )
)

print(gold_media_performance_df.show(10, False))

# COMMAND ----------

# =============================================================================
# 5. Reusable Write Function to write Gold Table to S3 with Delta Merge Function
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
# 6. Write Gold Media Performance Table
# =======================================================


write_gold_table(
    gold_media_performance_df,
    MEDIA_PERFORMANCE_GOLD_PATH,
    """
    target.media_id = source.media_id
    """
)


# COMMAND ----------

# ============================================
# 4. Build Gold Visitor Engagement
# ============================================
gold_visitor_engagement_df = (
    events_df.alias("e")

    .join(
        visitors_df.alias("v"),
        F.col("e.visitor_key")
        == F.col("v.visitor_key"),
        "left"
    )

    .join(
        media_df.alias("m"),
        F.col("e.media_id")
        == F.col("m.media_id"),
        "left"
    )

    .select(

        # Event
        F.col("e.event_key"),

        F.col("e.received_at").alias(
            "event_timestamp"
        ),

        # Visitor
        F.col("e.visitor_key"),

        F.col("v.visitor_name"),

        F.col("v.visitor_email"),

        F.col("v.organization_name"),

        # Media
        F.col("e.media_id"),

        F.col("m.media_name"),

        # Viewing behavior
        F.col("e.percent_viewed"),

        F.round(
            F.col("e.percent_viewed") * 100,
            2
        ).alias(
            "percent_viewed_pct"
        ),

        # Location
        F.col("e.city"),

        F.col("e.region"),

        F.col("e.country"),

        # Device
        F.col("e.browser"),

        F.col("e.mobile"),

        F.col("e.platform"),

        # Visitor metrics
        F.col("v.play_count").alias(
            "visitor_play_count"
        ),

        F.col("v.load_count").alias(
            "visitor_load_count"
        ),

        F.current_timestamp().alias(
            "load_timestamp"
        )
    )
)

# COMMAND ----------

# =======================================================
# 6. Write Gold Visitor Engagement Table
# =======================================================

write_gold_table(
    gold_visitor_engagement_df,
    VISITOR_ENGAGEMENT_GOLD_PATH,
    """
    target.event_key = source.event_key
    AND target.visitor_key = source.visitor_key
    """
)

# COMMAND ----------

# =======================================================
# 11. Verify Gold Table
# =======================================================

media_performance_gold_df = (
    spark.read
    .format("delta")
    .load(MEDIA_PERFORMANCE_GOLD_PATH)
)

visitor_engagement_df = (
    spark
    .read
    .format("delta")
    .load(VISITOR_ENGAGEMENT_GOLD_PATH)
)

print(
    "media_performance:",
    media_performance_gold_df.count()
)

print(
    "visitor_engagement:",
    visitor_engagement_df.count()
)


# COMMAND ----------

# =======================================================
# 12. Business Key Verification
# =======================================================
media_performance_duplicate_count = (
    media_performance_gold_df
    .groupBy("media_id",
             )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Duplicate media_performance business keys: {media_performance_duplicate_count}")


visitor_engagement_duplicate_count = (
    gold_visitor_engagement_df
    .groupBy("event_key", "visitor_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Duplicate visitor_engagement business keys: {visitor_engagement_duplicate_count}")


# COMMAND ----------

media_performance_df = (
    spark.read
    .format("delta")
    .load(
        "s3://wistia-data-bkt/gold/wistia_media_performance/"
    )
)

visitor_engagement_df = (
    spark.read
    .format("delta")
    .load(
        "s3://wistia-data-bkt/gold/wistia_visitor_engagement/"
    )
)

media_performance_df.printSchema()

visitor_engagement_df.printSchema()
