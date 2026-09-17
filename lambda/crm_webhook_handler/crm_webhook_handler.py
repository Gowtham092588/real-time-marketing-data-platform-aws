import json
import os
import boto3

s3 = boto3.client('s3')
bucket_name = os.environ['CRM_DATA_BUCKET']


def lambda_handler(event, context):

    body = json.loads(event["body"])
    crm_event = body["event"]
    lead_id = crm_event["lead_id"]
    lead_data = crm_event["data"]

    output_data = {
        "event_id": crm_event["id"],
        "lead_id": lead_id,
        "action": crm_event["action"],
        "display_name": lead_data.get("display_name"),
        "date_created": lead_data.get("date_created"),
        "status_label": lead_data.get("status_label")
    }

    s3_key = f"source/crm/crm_event_{lead_id}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(output_data)
    )

    return {
        'statusCode': 200,
        'body': json.dumps({
            "message": "CRM webhook processed successfully",
            "lead_id": lead_id
        })

    }
