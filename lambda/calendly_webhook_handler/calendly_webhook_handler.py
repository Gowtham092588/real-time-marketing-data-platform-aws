import json
import os
import boto3
from datetime import datetime, timezone

s3 = boto3.client('s3')
bucket_name = os.environ['CALENDLY_DATA_BUCKET']


def lambda_handler(event, context):

    try:
        print(f"Received event: {event}")

        body = json.loads(event['body'])

        print(f"Calendly Webhook Body: {body}")

        event_type = body.get('event', 'unknown')

        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M-%SZ')

        object_key = (
            f"bronze/calendly/"
            f"{event_type}_{timestamp}.json"
        )

        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json.dumps(body),
            ContentType='application/json'
        )

        print("Calendly event written to:", object_key)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Calendly webhook received successfully"
            })
        }

    except Exception as e:

        print("Failed to process Calendly webhook:", str(e))

        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Internal server error"
            })
        }
