import json
import boto3
import urllib.request
from botocore.exceptions import ClientError

# ============================================
# 1. CONFIG
# ============================================

SOURCE_INDEX_URL = (
    "https://dea-data-bucket.s3.us-east-1.amazonaws.com/"
    "calendly_spend_data/file_index.json"
)

SOURCE_BASE_URL = (
    "https://dea-data-bucket.s3.us-east-1.amazonaws.com/"
    "calendly_spend_data/"
)

TARGET_BUCKET = "calendly-data-bkt"
TARGET_PREFIX = "bronze/marketing_spend/"

# ============================================
# 2. S3 CLIENT
# ============================================

s3 = boto3.client("s3", region_name="us-east-2")

# ============================================
# 3. Get File Index
# ============================================


def get_file_index():

    with urllib.request.urlopen(
        SOURCE_INDEX_URL,
        timeout=30
    ) as response:

        index_data = json.loads(response.read().decode("utf-8"))

    print("File index loaded successfully.")

    return index_data

# ============================================
# 3. Extract file name
# ============================================


def extract_file_names(index_data):

    if isinstance(index_data, list):
        return index_data

    if isinstance(index_data, dict):

        if "files" in index_data:
            return index_data["files"]

    raise ValueError(
        "Unexpected file_index.json structure."
    )

# ============================================
# 3. File name Exists
# ============================================


def file_exists_in_bronze(file_name):

    key = TARGET_PREFIX + file_name

    try:

        s3.head_object(
            Bucket=TARGET_BUCKET,
            Key=key
        )

        return True

    except ClientError as e:

        error_code = e.response["Error"]["Code"]

        if error_code in [
            "404",
            "NoSuchKey",
            "NotFound"
        ]:
            return False

        raise

# ============================================
# 4. Download Source File
# ============================================


def download_source_file(file_name):

    source_url = (SOURCE_BASE_URL + file_name)

    with urllib.request.urlopen(
        source_url,
        timeout=30
    ) as response:

        file_content = response.read()

    return file_content

# ============================================
# 4. Write to Bronze
# ============================================


def write_to_bronze(file_name, file_content):

    key = TARGET_PREFIX + file_name

    s3.put_object(
        Bucket=TARGET_BUCKET,
        Key=key,
        Body=file_content,
        ContentType="application/json"
    )

    print(f"Copied to Bronze: {file_name}")


def lambda_handler(event, context):

    index_data = get_file_index()
    available_files = extract_file_names(index_data)

    skipped_files = []
    copied_files = []

    for file_name in available_files:

        if not file_name.startswith("spend_data_"):

            continue

        if file_exists_in_bronze(file_name):

            skipped_files.append(file_name)

            continue

        file_content = download_source_file(file_name=file_name)

        write_to_bronze(file_name, file_content)

        copied_files.append(file_name)

    result = {
        "available_files": len(available_files),
        "skipped_files": len(skipped_files),
        "copied_files": len(copied_files)
    }

    print(json.dumps(result))

    return {
        "statusCode": 200,
        "body": json.dumps(
            result
        )
    }
