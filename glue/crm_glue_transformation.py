import sys
import logging
import json
import boto3

from datetime import datetime, timezone
from botocore.exceptions import ClientError


from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame


from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =========================================================
# GLUE SETUP
# =========================================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"]
)

sc = SparkContext()

glue_context = GlueContext(sc)

spark = glue_context.spark_session

job = Job(glue_context)

job.init(
    args["JOB_NAME"]
)


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)

logger = logging.getLogger(
    "crm-Transform-job"
)

# =========================================================
# Constraints
# =========================================================


MERGE_SQL = """
MERGE INTO staging.crm_leads 
USING staging.crm_leads_load AS source
ON staging.crm_leads.lead_id = source.lead_id

WHEN MATCHED THEN
UPDATE SET
    display_name = source.display_name,
    lead_email = source.lead_email,
    status_label = source.status_label,
    lead_owner = source.lead_owner,
    funnel = source.funnel,
    date_created = source.date_created

WHEN NOT MATCHED THEN
INSERT (
    lead_id,
    display_name,
    lead_email,
    status_label,
    lead_owner,
    funnel,
    date_created
)
VALUES (
    source.lead_id,
    source.display_name,
    source.lead_email,
    source.status_label,
    source.lead_owner,
    source.funnel,
    source.date_created
);
"""

# =========================================================
# PATHS
# =========================================================

SOURCE_PATH = "s3://crm-data-bkt/target/crm/"

REDSHIFT_CONNECTION = "crm-redshift-connection"

REDSHIFT_DATABASE = "marketingdb"

REDSHIFT_TABLE = "staging.crm_leads"

REDSHIFT_TEMP_DIR = "s3://crm-data-bkt/temp/redshift/crm/"

S3_BUCKET = "crm-data-bkt"

AWS_REGION = "us-east-2"

CRM_TARGET_PREFIX = "target/crm/"

CHECKPOINT_KEY = "checkpoint/crm_checkpoint.json"


# =========================================================
# VALIDATION PATTERNS
# =========================================================

email_pattern = (
    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
)

phone_pattern = (
    r"^\+?[0-9][0-9\-\s\(\)]{7,}$"
)


# =========================================================
# EXPECTED SOURCE SCHEMA
# =========================================================

# These columns are required for the pipeline to work.
required_columns = [
    "lead_id",
    "date_created"
]

# These columns are useful but the job can continue
# even if one of them temporarily disappears.
optional_columns = [
    "display_name",
    "lead_email",
    "status_label",
    "lead_owner",
    "funnel"
]

expected_columns = set(
    required_columns + optional_columns
)

# =========================================================
# S3 Client
# =========================================================

s3_client = boto3.client("s3", region_name=AWS_REGION)

# =====================================================
# Load Checkpoint
# =====================================================


def load_checkpoint():

    try:
        response = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key=CHECKPOINT_KEY
        )

        checkpoint_data = json.loads(response["Body"].read().decode("utf-8"))

        last_processed_time = checkpoint_data.get(
            "last_processed_time")

        if last_processed_time:
            return datetime.fromisoformat(last_processed_time)
        return None

    except ClientError as e:
        if e.response["Error"]["Code"] in ["NoSuchKey", "404"]:
            logger.info(
                "Checkpoint does not exist. "
                "This will be treated as the initial full load."
            )
            return None

        raise

# =========================================================
# Get Increment Files
# =========================================================


def get_incremental_files(last_processed_time):

    try:

        paginator = s3_client.get_paginator("list_objects_v2")

        changed_files = []

        latest_modified_time = last_processed_time

        pages = paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix=CRM_TARGET_PREFIX)

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                last_modified = obj["LastModified"]

                if key.endswith("/"):
                    continue

                if last_processed_time is None:
                    changed_files.append(
                        f"s3://{S3_BUCKET}/{key}"
                    )
                elif last_processed_time < last_modified:
                    changed_files.append(
                        f"s3://{S3_BUCKET}/{key}")

                if (
                        latest_modified_time is None
                        or last_modified > latest_modified_time):
                    latest_modified_time = last_modified
        return changed_files, latest_modified_time

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        logger.error(
            f"Failed to list crm files from s."
            f"Error code: {error_code}, Error: {e}")
        raise
    except Exception as e:
        logger.exception(
            f"Unexpected error while identifying incremental CRM files: {e}")
        raise

# =========================================================
# Save Checkpoint
# =========================================================


def save_checkpoint(last_processed_time):
    checkpoint_data = {
        "last_processed_time": last_processed_time.astimezone(timezone.utc).isoformat(),
        "checkpoint_saved_at": datetime.now(timezone.utc).isoformat()
    }

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=CHECKPOINT_KEY,
        Body=json.dumps(
            checkpoint_data,
            indent=2),
        ContentType="application/json")

    logger.info(
        "Checkpoint saved successfully: "
        f"{last_processed_time.isoformat()}"
    )

# =========================================================
# MAIN JOB
# =========================================================


def main():

    try:

        logger.info(
            "CRM Glue Transformation job started"
        )

        # -----------------------------------------
        # Checkpoint
        # -----------------------------------------

        last_processed_time = load_checkpoint()

        changed_files, latest_modified_time = (
            get_incremental_files(
                last_processed_time
            )
        )

        if not changed_files:
            logger.info("No new or modified CRM files.")
            job.commit()
            return

        # =====================================================
        # 1. READ SOURCE DATA
        # =====================================================

        logger.info(
            f"CRM files selected for processing: "
            f"{len(changed_files)}"
        )

        crm_df = spark.read.json(
            changed_files
        )

        source_row_count = crm_df.count()

        logger.info(
            f"CRM source row count: {source_row_count}"
        )

        logger.info(
            f"CRM source schema: "
            f"{crm_df.schema.simpleString()}"
        )

        # =====================================================
        # 2. SCHEMA DRIFT VALIDATION
        # =====================================================

        logger.info(
            "Starting CRM schema drift validation"
        )

        source_columns = set(
            crm_df.columns
        )

        # -----------------------------------------------------
        # Detect new columns
        # -----------------------------------------------------

        new_columns = (
            source_columns -
            expected_columns
        )

        if new_columns:

            logger.warning(
                "New source columns detected: "
                f"{sorted(new_columns)}"
            )

            logger.warning(
                "New columns will NOT automatically be "
                "added to Transform. Review them before "
                "changing the Transform schema."
            )

        else:

            logger.info(
                "No new source columns detected"
            )

        # -----------------------------------------------------
        # Detect missing required columns
        # -----------------------------------------------------

        missing_required_columns = (
            set(required_columns) -
            source_columns
        )

        if missing_required_columns:

            logger.error(
                "Missing required CRM columns: "
                f"{sorted(missing_required_columns)}"
            )

            raise ValueError(
                "Schema validation failed. "
                "Required source columns are missing: "
                f"{sorted(missing_required_columns)}"
            )

        else:

            logger.info(
                "All required CRM columns are present"
            )

        # -----------------------------------------------------
        # Detect missing optional columns
        # -----------------------------------------------------

        missing_optional_columns = (
            set(optional_columns) -
            source_columns
        )

        if missing_optional_columns:

            logger.warning(
                "Missing optional CRM columns detected: "
                f"{sorted(missing_optional_columns)}"
            )

            # Add missing optional columns as NULL
            for column_name in missing_optional_columns:

                crm_df = crm_df.withColumn(
                    column_name,
                    F.lit(None).cast("string")
                )

                logger.warning(
                    f"Optional column '{column_name}' "
                    f"was added as NULL"
                )

        else:

            logger.info(
                "All optional CRM columns are present"
            )

        logger.info(
            "Schema drift validation completed"
        )

        # =====================================================
        # 3. CLEAN AND STANDARDIZE
        # =====================================================

        logger.info(
            "Starting CRM cleaning and standardization"
        )

        crm_clean_df = crm_df.select(

            # -------------------------------------------------
            # LEAD ID
            # -------------------------------------------------

            F.trim(
                F.col("lead_id")
            ).alias(
                "lead_id"
            ),


            # -------------------------------------------------
            # DISPLAY NAME
            # -------------------------------------------------

            F.when(

                # display_name == email
                F.lower(
                    F.trim(
                        F.col("display_name")
                    )
                )
                ==
                F.lower(
                    F.trim(
                        F.col("lead_email")
                    )
                ),

                F.lit(None)

            )

            .when(

                # Phone number used as display_name
                F.trim(
                    F.col("display_name")
                ).rlike(
                    phone_pattern
                ),

                F.lit(None)

            )

            .when(

                # Placeholder values
                F.lower(
                    F.trim(
                        F.col("display_name")
                    )
                ).isin(
                    "untitled",
                    "none",
                    ""
                ),

                F.lit(None)

            )

            .otherwise(

                # Preserve actual name casing
                F.trim(
                    F.col("display_name")
                )

            )

            .alias(
                "display_name"
            ),


            # -------------------------------------------------
            # LEAD EMAIL
            # -------------------------------------------------

            F.when(

                F.col(
                    "lead_email"
                ).isNull()

                |

                (
                    F.trim(
                        F.col(
                            "lead_email"
                        )
                    )
                    == ""
                )

                |

                (
                    F.lower(
                        F.trim(
                            F.col(
                                "lead_email"
                            )
                        )
                    )
                    == "none"
                ),

                F.lit(None)

            )

            .otherwise(

                F.lower(
                    F.trim(
                        F.col(
                            "lead_email"
                        )
                    )
                )

            )

            .alias(
                "lead_email"
            ),


            # -------------------------------------------------
            # STATUS LABEL
            # -------------------------------------------------

            F.when(

                F.col(
                    "status_label"
                ).isNull()

                |

                (
                    F.trim(
                        F.col(
                            "status_label"
                        )
                    )
                    == ""
                )

                |

                (
                    F.lower(
                        F.trim(
                            F.col(
                                "status_label"
                            )
                        )
                    )
                    == "none"
                ),

                F.lit(None)

            )

            .otherwise(

                F.trim(
                    F.col(
                        "status_label"
                    )
                )

            )

            .alias(
                "status_label"
            ),


            # -------------------------------------------------
            # LEAD OWNER
            # -------------------------------------------------

            F.when(

                F.col(
                    "lead_owner"
                ).isNull()

                |

                (
                    F.trim(
                        F.col(
                            "lead_owner"
                        )
                    )
                    == ""
                )

                |

                (
                    F.lower(
                        F.trim(
                            F.col(
                                "lead_owner"
                            )
                        )
                    )
                    == "none"
                ),

                F.lit(None)

            )

            .otherwise(

                F.trim(
                    F.col(
                        "lead_owner"
                    )
                )

            )

            .alias(
                "lead_owner"
            ),


            # -------------------------------------------------
            # FUNNEL
            # -------------------------------------------------

            F.when(

                F.col(
                    "funnel"
                ).isNull()

                |

                (
                    F.trim(
                        F.col(
                            "funnel"
                        )
                    )
                    == ""
                )

                |

                (
                    F.lower(
                        F.trim(
                            F.col(
                                "funnel"
                            )
                        )
                    )
                    == "none"
                ),

                F.lit(None)

            )

            .otherwise(

                F.trim(
                    F.col(
                        "funnel"
                    )
                )

            )

            .alias(
                "funnel"
            ),


            # -------------------------------------------------
            # DATE CREATED
            # -------------------------------------------------

            F.to_timestamp(
                F.col(
                    "date_created"
                )
            ).alias(
                "date_created"
            )
        )

        logger.info(
            "CRM cleaning and standardization completed"
        )

        # =====================================================
        # 4. REMOVE INVALID BUSINESS KEYS
        # =====================================================

        logger.info(
            "Checking invalid lead_id values"
        )

        invalid_source_lead_id_count = (
            crm_clean_df
            .filter(
                F.col("lead_id").isNull()
                |
                (
                    F.trim(
                        F.col("lead_id")
                    )
                    == ""
                )
            )
            .count()
        )

        if invalid_source_lead_id_count > 0:

            logger.warning(
                f"{invalid_source_lead_id_count} "
                f"records contain invalid lead_id "
                f"and will be removed"
            )

        crm_clean_df = crm_clean_df.filter(

            F.col(
                "lead_id"
            ).isNotNull()

            &

            (
                F.trim(
                    F.col(
                        "lead_id"
                    )
                )
                != ""
            )
        )

        # =====================================================
        # 5. DEDUPLICATE
        # =====================================================

        logger.info(
            "Starting CRM lead deduplication"
        )

        window_spec = Window.partitionBy(
            "lead_id"
        ).orderBy(
            F.col(
                "date_created"
            ).desc()
        )

        crm_clean_df = (

            crm_clean_df

            .withColumn(
                "row_number",
                F.row_number().over(
                    window_spec
                )
            )

            .filter(
                F.col(
                    "row_number"
                )
                == 1
            )

            .drop(
                "row_number"
            )
        )

        logger.info(
            "CRM deduplication completed"
        )

        # =====================================================
        # 6. FINAL VALIDATION
        # =====================================================

        logger.info(
            "======================================"
        )

        logger.info(
            "FINAL CRM Transform VALIDATION"
        )

        logger.info(
            "======================================"
        )

        # -----------------------------------------------------
        # Final row count
        # -----------------------------------------------------

        final_row_count = crm_clean_df.count()

        logger.info(
            f"Final clean df row count: "
            f"{final_row_count}"
        )

        logger.info(
            f"Rows removed during cleaning/deduplication: "
            f"{source_row_count - final_row_count}"
        )

        # -----------------------------------------------------
        # Show sample
        # -----------------------------------------------------

        crm_clean_df.show(
            50,
            truncate=False
        )

        # =====================================================
        # NULL COUNTS
        # =====================================================

        logger.info(
            "----- NULL COUNTS -----"
        )

        null_counts_df = crm_clean_df.select([

            F.sum(

                F.when(
                    F.col(c).isNull(),
                    1
                )

                .otherwise(
                    0
                )

            ).alias(c)

            for c in crm_clean_df.columns
        ])

        null_counts_df.show()

        # =====================================================
        # STRING COLUMNS
        # =====================================================

        string_columns = [

            "lead_id",
            "display_name",
            "lead_email",
            "status_label",
            "lead_owner",
            "funnel"

        ]

        # =====================================================
        # BLANK STRING CHECK
        # =====================================================

        logger.info(
            "----- BLANK STRING COUNTS -----"
        )

        blank_leftover_count = 0

        for column_name in string_columns:

            blank_count = crm_clean_df.filter(

                F.col(
                    column_name
                ).isNotNull()

                &

                (
                    F.trim(
                        F.col(
                            column_name
                        )
                    )
                    == ""
                )

            ).count()

            logger.info(
                f"{column_name}: "
                f"{blank_count}"
            )

            blank_leftover_count += (
                blank_count
            )

        # =====================================================
        # STRING "NONE" CHECK
        # =====================================================

        logger.info(
            "----- STRING 'NONE' LEFTOVERS -----"
        )

        none_leftover_count = 0

        for column_name in string_columns:

            none_count = crm_clean_df.filter(

                F.col(
                    column_name
                ).isNotNull()

                &

                (
                    F.lower(
                        F.trim(
                            F.col(
                                column_name
                            )
                        )
                    )
                    == "none"
                )

            ).count()

            logger.info(
                f"{column_name}: "
                f"{none_count}"
            )

            none_leftover_count += (
                none_count
            )

        # =====================================================
        # INVALID LEAD ID
        # =====================================================

        invalid_lead_id_count = crm_clean_df.filter(

            F.col(
                "lead_id"
            ).isNull()

            |

            (
                F.trim(
                    F.col(
                        "lead_id"
                    )
                )
                == ""
            )

        ).count()

        logger.info(
            f"Invalid lead_id count after cleaning: "
            f"{invalid_lead_id_count}"
        )

        # =====================================================
        # DUPLICATE LEAD ID
        # =====================================================

        duplicate_leads_df = (

            crm_clean_df

            .groupBy(
                "lead_id"
            )

            .count()

            .filter(
                F.col(
                    "count"
                )
                > 1
            )
        )

        duplicate_lead_count = (
            duplicate_leads_df.count()
        )

        logger.info(
            f"Duplicate lead_id count: "
            f"{duplicate_lead_count}"
        )

        if duplicate_lead_count > 0:

            duplicate_leads_df.show(
                100,
                truncate=False
            )

        # =====================================================
        # DISPLAY NAME == EMAIL
        # =====================================================

        bad_name_email_df = crm_clean_df.filter(

            F.col(
                "display_name"
            ).isNotNull()

            &

            F.col(
                "lead_email"
            ).isNotNull()

            &

            (
                F.lower(
                    F.trim(
                        F.col(
                            "display_name"
                        )
                    )
                )
                ==
                F.lower(
                    F.trim(
                        F.col(
                            "lead_email"
                        )
                    )
                )
            )
        )

        bad_name_email_count = (
            bad_name_email_df.count()
        )

        logger.info(
            "display_name equal to lead_email count: "
            f"{bad_name_email_count}"
        )

        # =====================================================
        # PHONE NUMBER IN DISPLAY NAME
        # =====================================================

        phone_name_df = crm_clean_df.filter(

            F.col(
                "display_name"
            ).isNotNull()

            &

            F.trim(
                F.col(
                    "display_name"
                )
            ).rlike(
                phone_pattern
            )
        )

        phone_name_count = (
            phone_name_df.count()
        )

        logger.info(
            f"Phone-style display_name count: "
            f"{phone_name_count}"
        )

        # =====================================================
        # UNTITLED DISPLAY NAME
        # =====================================================

        untitled_count = crm_clean_df.filter(

            F.col(
                "display_name"
            ).isNotNull()

            &

            (
                F.lower(
                    F.trim(
                        F.col(
                            "display_name"
                        )
                    )
                )
                == "untitled"
            )

        ).count()

        logger.info(
            f"Untitled display_name count: "
            f"{untitled_count}"
        )

        # =====================================================
        # INVALID EMAIL
        # REPORT ONLY
        # =====================================================

        invalid_email_df = crm_clean_df.filter(

            F.col(
                "lead_email"
            ).isNotNull()

            &

            ~F.col(
                "lead_email"
            ).rlike(
                email_pattern
            )
        )

        invalid_email_count = (
            invalid_email_df.count()
        )

        if invalid_email_count > 0:

            logger.warning(
                f"Invalid email format count: "
                f"{invalid_email_count}"
            )

            invalid_email_df.select(
                "lead_id",
                "lead_email"
            ).show(
                100,
                truncate=False
            )

        else:

            logger.info(
                "Invalid email format count: 0"
            )

        # =====================================================
        # NULL DATE_CREATED
        # =====================================================

        null_date_count = crm_clean_df.filter(

            F.col(
                "date_created"
            ).isNull()

        ).count()

        logger.info(
            f"Null date_created count: "
            f"{null_date_count}"
        )

        # =====================================================
        # DUPLICATE EMAIL ACROSS LEADS
        # REPORT ONLY
        # =====================================================

        duplicate_email_df = (

            crm_clean_df

            .filter(
                F.col(
                    "lead_email"
                ).isNotNull()
            )

            .groupBy(
                "lead_email"
            )

            .agg(

                F.countDistinct(
                    "lead_id"
                ).alias(
                    "lead_count"
                )
            )

            .filter(
                F.col(
                    "lead_count"
                )
                > 1
            )
        )

        duplicate_email_count = (
            duplicate_email_df.count()
        )

        if duplicate_email_count > 0:

            logger.warning(
                "Emails associated with multiple "
                f"lead_ids: {duplicate_email_count}"
            )

            duplicate_email_df.show(
                100,
                truncate=False
            )

        else:

            logger.info(
                "No emails are associated with "
                "multiple lead_ids"
            )

        # =====================================================
        # CATEGORY DISTRIBUTIONS
        # =====================================================

        logger.info(
            "----- STATUS VALUES -----"
        )

        crm_clean_df.groupBy(
            "status_label"
        ).count().orderBy(
            F.desc(
                "count"
            )
        ).show(
            100,
            truncate=False
        )

        logger.info(
            "----- FUNNEL VALUES -----"
        )

        crm_clean_df.groupBy(
            "funnel"
        ).count().orderBy(
            F.desc(
                "count"
            )
        ).show(
            100,
            truncate=False
        )

        logger.info(
            "----- LEAD OWNER VALUES -----"
        )

        crm_clean_df.groupBy(
            "lead_owner"
        ).count().orderBy(
            F.desc(
                "count"
            )
        ).show(
            100,
            truncate=False
        )

        # =====================================================
        # 7. FINAL PASS / FAIL
        # =====================================================

        logger.info(
            "Starting final critical validation"
        )

        if invalid_lead_id_count > 0:

            raise ValueError(
                "Validation failed: "
                "invalid lead_id found"
            )

        if duplicate_lead_count > 0:

            raise ValueError(
                "Validation failed: "
                "duplicate lead_id found"
            )

        if null_date_count > 0:

            raise ValueError(
                "Validation failed: "
                "null date_created found"
            )

        if bad_name_email_count > 0:

            raise ValueError(
                "Validation failed: "
                "display_name still equals email"
            )

        if phone_name_count > 0:

            raise ValueError(
                "Validation failed: "
                "phone number still exists "
                "in display_name"
            )

        if untitled_count > 0:

            raise ValueError(
                "Validation failed: "
                "Untitled still exists "
                "in display_name"
            )

        if none_leftover_count > 0:

            raise ValueError(
                "Validation failed: "
                "string 'none' values still exist"
            )

        if blank_leftover_count > 0:

            raise ValueError(
                "Validation failed: "
                "blank strings still exist"
            )

        logger.info(
            "CRM Transform validation passed successfully"
        )

        # =====================================================
        # 8. WRITE TO RedShift
        # =====================================================

        logger.info(
            f"Writing CRM Transformed data to: "
            f"{REDSHIFT_TEMP_DIR}"
        )

        crm_dynamic_frame = DynamicFrame.fromDF(
            crm_clean_df,
            glue_context,
            "crm_dynamic_frame"
        )

        glue_context.write_dynamic_frame.from_jdbc_conf(
            frame=crm_dynamic_frame,
            catalog_connection="crm-redshift-connection",
            connection_options={
                "database": "marketingdb",
                "dbtable": "staging.crm_leads_load",
                "preactions": "DELETE FROM staging.crm_leads_load;",
                "postactions": MERGE_SQL
            },
            redshift_tmp_dir=REDSHIFT_TEMP_DIR
        )

        logger.info(
            "CRM Transform data written successfully to crm_leads_load"
        )

        if latest_modified_time:
            save_checkpoint(latest_modified_time)

        # =====================================================
        # 9. COMMIT JOB
        # =====================================================

        job.commit()

        logger.info(
            "======================================"
        )

        logger.info(
            "CRM Transform Glue job completed successfully"
        )

        logger.info(
            "======================================"
        )

    # =========================================================
    # ERROR HANDLING
    # =========================================================

    except Exception as e:

        logger.exception(
            f"CRM Transform Glue job failed: {e}"
        )

        raise


if __name__ == "__main__":
    main()
