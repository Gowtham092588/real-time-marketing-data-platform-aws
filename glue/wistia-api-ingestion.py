import sys
import json
import time
import boto3
import requests

from awsglue.utils import getResolvedOptions
from requests.exceptions import RequestException

args = getResolvedOptions(
    sys.argv,
    [
        "AWS_REGION",
        "BUCKET",
        "SECRET_NAME",
        "CHECKPOINT_KEY",
        "WISTIA_MEDIA_IDS"
    ]
)

AWS_REGION = args["AWS_REGION"]
BUCKET = args["BUCKET"]
SECRET_NAME = args["SECRET_NAME"]
CHECKPOINT_KEY = args["CHECKPOINT_KEY"]

MEDIA_IDS = [
    media_id.strip()
    for media_id in args["WISTIA_MEDIA_IDS"].split(",")
    if media_id.strip()
]

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION
)

secrets_client = boto3.client(
    "secretsmanager",
    region_name=AWS_REGION
)


def get_secret():

    response = secrets_client.get_secret_value(
        SecretId=SECRET_NAME
    )

    secret_dict = json.loads(
        response["SecretString"]
    )

    return secret_dict["wistia_api_token"]


def get_headers(secret):

    return {
        "Authorization": f"Bearer {secret}",
        "X-Wistia-API-Version": "2026-07",
        "User-Agent": "AWS-Glue-Wistia-Ingestion"
    }


def get_media_metadata(secret, media_id):

    url = f"https://api.wistia.com/modern/medias/{media_id}"

    try:

        response = requests.get(
            url=url,
            headers=get_headers(secret),
            timeout=30
        )

        if response.status_code == 200:
            return response.json()

        print(
            f"Metadata API error for {media_id}: "
            f"{response.status_code} {response.text}"
        )

    except RequestException as e:
        print(f"Metadata request failed: {e}")

    return None


def get_media_stats(secret, media_id):

    url = (
        f"https://api.wistia.com/modern/stats/"
        f"medias/{media_id}"
    )

    try:

        response = requests.get(
            url=url,
            headers=get_headers(secret),
            timeout=30
        )

        if response.status_code == 200:
            return response.json()

        print(
            f"Stats API error for {media_id}: "
            f"{response.status_code} {response.text}"
        )

    except RequestException as e:
        print(f"Stats request failed: {e}")

    return None


def get_media_engagement(secret, media_id):

    url = (
        f"https://api.wistia.com/modern/stats/"
        f"medias/{media_id}/engagement"
    )

    try:

        response = requests.get(
            url=url,
            headers=get_headers(secret),
            timeout=30
        )

        if response.status_code == 200:
            return response.json()

        print(
            f"Engagement API error for {media_id}: "
            f"{response.status_code} {response.text}"
        )

    except RequestException as e:
        print(f"Engagement request failed: {e}")

    return None


def get_events(secret, media_id):

    url = "https://api.wistia.com/modern/stats/events"

    page = 1
    per_page = 100

    while True:

        print(
            f"Processing events for {media_id}, page {page}"
        )

        params = {
            "media_id": media_id,
            "page": page,
            "per_page": per_page
        }

        response = requests.get(
            url=url,
            headers=get_headers(secret),
            params=params,
            timeout=30
        )

        if response.status_code != 200:

            raise RuntimeError(
                f"Events API error for {media_id}: "
                f"{response.status_code} {response.text}"
            )

        events = response.json()

        if not events:
            break

        write_to_s3(
            f"bronze/wistia/events/{media_id}/page_{page}.json",
            events
        )

        print(
            f"{media_id} page {page}: "
            f"{len(events)} events written"
        )

        if len(events) < per_page:
            break

        page += 1


def get_visitors(secret, checkpoint):

    if checkpoint.get("visitor_initial_load_complete", False):

        print(
            "Visitor initial load already complete. "
            "Skipping historical visitor load."
        )

        return

        url = "https://api.wistia.com/modern/stats/visitors"

        per_page = 100

        page = checkpoint.get("visitor_page", 1)

        while True:

            print(f"Processing visitor page {page}")

            params = {
                "page": page,
                "per_page": per_page
            }

            response = None

            for attempt in range(3):

                try:

                    response = requests.get(
                        url=url,
                        headers=get_headers(secret),
                        params=params,
                        timeout=30
                    )

                    break

                except RequestException as e:

                    print(
                        f"Visitor page {page} failed "
                        f"(attempt {attempt + 1}/3): {e}"
                    )

                    time.sleep(2)

            if response is None:

                raise RuntimeError(
                    f"Visitor page {page} failed after 3 attempts"
                )

            if response.status_code != 200:

                raise RuntimeError(
                    f"Visitor API error on page {page}: "
                    f"{response.status_code}"
                )

            visitors = response.json()

            if not visitors:

                print("Visitor pagination complete")

                checkpoint.pop(
                    "visitor_page",
                    None
                )

                checkpoint["visitor_initial_load_complete"] = True

                save_checkpoint(checkpoint)

                break

            write_to_s3(
                f"bronze/wistia/visitors/page_{page}.json",
                visitors
            )

            checkpoint["visitor_page"] = page + 1

            save_checkpoint(checkpoint)

            print(
                f"Page {page}: "
                f"{len(visitors)} visitors written"
            )

            if len(visitors) < per_page:

                checkpoint.pop(
                    "visitor_page",
                    None
                )

                checkpoint["visitor_initial_load_complete"] = True

                save_checkpoint(checkpoint)

                print("Visitor initial load complete")

                break

            page += 1


def write_to_s3(key, data):

    if data is None:
        return

    s3_client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(
        f"Written to s3://{BUCKET}/{key}"
    )


def load_checkpoint():

    try:

        response = s3_client.get_object(
            Bucket=BUCKET,
            Key=CHECKPOINT_KEY
        )

        checkpoint_data = (
            response["Body"]
            .read()
            .decode("utf-8")
        )

        return json.loads(
            checkpoint_data
        )

    except s3_client.exceptions.NoSuchKey:

        print(
            "Checkpoint not found. "
            "Initial load will run."
        )

        return {}


def save_checkpoint(checkpoint):

    s3_client.put_object(
        Bucket=BUCKET,
        Key=CHECKPOINT_KEY,
        Body=json.dumps(
            checkpoint,
            indent=4
        ),
        ContentType="application/json"
    )

    print("Checkpoint saved")


def main():

    secret = get_secret()

    checkpoint = load_checkpoint()

    for media_id in MEDIA_IDS:

        print(f"Processing media: {media_id}")

        # ========================================================
        # MEDIA METADATA
        # ========================================================

        metadata = get_media_metadata(
            secret,
            media_id
        )

        if metadata:

            current_updated = metadata.get(
                "updated"
            )

            last_updated = checkpoint.get(
                f"media_{media_id}_updated"
            )

            if (
                not last_updated
                or current_updated > last_updated
            ):

                write_to_s3(
                    f"bronze/wistia/media/{media_id}.json",
                    metadata
                )

                checkpoint[
                    f"media_{media_id}_updated"
                ] = current_updated

        # ========================================================
        # MEDIA STATS
        # ========================================================

        stats = get_media_stats(
            secret,
            media_id
        )

        write_to_s3(
            f"bronze/wistia/stats/{media_id}.json",
            stats
        )

        # ========================================================
        # MEDIA ENGAGEMENT
        # ========================================================

        engagement = get_media_engagement(
            secret,
            media_id
        )

        write_to_s3(
            f"bronze/wistia/engagement/{media_id}.json",
            engagement
        )

        # ========================================================
        # MEDIA EVENTS
        # ========================================================

        get_events(secret, media_id)

    # ============================================================
    # VISITORS
    # ============================================================

    get_visitors(
        secret,
        checkpoint
    )

    save_checkpoint(
        checkpoint
    )


if __name__ == "__main__":

    main()
