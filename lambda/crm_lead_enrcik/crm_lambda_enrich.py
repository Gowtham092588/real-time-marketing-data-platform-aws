import json
import boto3
from urllib.request import urlopen, Request
from urllib.error import HTTPError


s3 = boto3.client('s3')
sns = boto3.client('sns')
secretsmanager = boto3.client("secretsmanager")


def get_slack_webhook_url():

    response = secretsmanager.get_secret_value(
        SecretId="slack/url"
    )

    secret = json.loads(response['SecretString'])

    return secret["slack_webhook_url"]


def lambda_handler(event, context):

    try:

        message_body = json.loads(event['Records'][0]['body'])

        bucket_name = message_body['Records'][0]['s3']['bucket']['name']
        object_key = message_body['Records'][0]['s3']['object']['key']

        response = s3.get_object(
            Bucket=bucket_name,
            Key=object_key
        )

        file_content = response["Body"].read().decode("utf-8")

        crm_data = json.loads(file_content)

        print(f"crm data : {crm_data}")

        lead_id = crm_data["lead_id"]
        display_name = crm_data["display_name"]
        date_created = crm_data["date_created"]
        status_label = crm_data["status_label"]

        lookup_bucket_name = "dea-lead-owner"
        file_name = f"{lead_id}.json"
        public_url = (
            f"https://{lookup_bucket_name}.s3.us-east-1.amazonaws.com/"
            f"{file_name}"
        )

        try:
            response = urlopen(public_url)

        except HTTPError as e:
            if e.code == 403:
                print(
                    f"Lead owner file not available yet for {lead_id}. "
                    f"Will retry through SQS."
                )
            else:
                print(f"HTTP error during lead owner lookup: {e}")

            raise
        lookup_content = response.read().decode('utf-8')
        lookup_data = json.loads(lookup_content)

        lead_email = lookup_data["lead_email"]
        lead_owner = lookup_data["lead_owner"]
        funnel = lookup_data["funnel"]

        if crm_data["lead_id"] != lookup_data["lead_id"]:
            raise ValueError("Lead ID mismatch")

        enriched_data = {
            "lead_id": lead_id,
            "display_name": display_name,
            "date_created": date_created,
            "status_label": status_label,
            "lead_email": lead_email,
            "lead_owner": lead_owner,
            "funnel": funnel
        }

        target_key = f"target/crm/enriched_lead_{lead_id}.json"

        s3.put_object(
            Bucket=bucket_name,
            Key=target_key,
            Body=json.dumps(enriched_data),
            ContentType="application/json"
        )

        print("Enriched file written to:", target_key)

        print("Processing completed successfully")

        print("Enriched file written to:", target_key)

        message = (
            f"New Lead Alert\n\n"
            f"Name: {enriched_data['display_name']}\n"
            f"Lead ID: {enriched_data['lead_id']}\n"
            f"Created Date: {enriched_data['date_created']}\n"
            f"Label: {enriched_data['status_label']}\n"
            f"Email: {enriched_data['lead_email']}\n"
            f"Lead Owner: {enriched_data['lead_owner']}\n"
            f"Funnel: {enriched_data['funnel']}\n"
        )

        sns_response = sns.publish(
            TopicArn="arn:aws:sns:us-east-2:229378727976:crm-lead-notifications",
            Subject="New CRM Lead Alert",
            Message=message
        )

        print("SNS MessageId:", sns_response["MessageId"])
        print("SNS notification sent successfully")

        slack_webhook_url = get_slack_webhook_url()

        slack_message = {
            "text": (
                f"*New CRM Lead Alert*\n\n"
                f"*Name:* {enriched_data['display_name']}\n"
                f"*Lead ID:* {enriched_data['lead_id']}\n"
                f"*Created Date:* {enriched_data['date_created']}\n"
                f"*Label:* {enriched_data['status_label']}\n"
                f"*Email:* {enriched_data['lead_email']}\n"
                f"*Lead Owner:* {enriched_data['lead_owner']}\n"
                f"*Funnel:* {enriched_data['funnel']}"
            )
        }

        request = Request(
            slack_webhook_url,
            data=json.dumps(slack_message).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )

        response = urlopen(request)

        print("Slack response:", response.status)
        print("Slack notification sent successfully")

    except Exception as e:
        print(
            f"Failed to process CRM enrichment. "
            f"Error: {str(e)}"
        )
        raise
