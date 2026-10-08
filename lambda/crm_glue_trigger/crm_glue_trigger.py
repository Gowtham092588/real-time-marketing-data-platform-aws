import boto3

glue = boto3.client("glue")

GLUE_JOB_NAME = "crm_glue_transformation"


def lambda_handler(event, context):

    print("Received event:", event)

    bucket = event["detail"]["bucket"]["name"]
    key = event["detail"]["object"]["key"]

    print(f"Bucket: {bucket}")
    print(f"Key: {key}")

    # Only react to enriched CRM target files
    if bucket != "crm-data-bkt":
        print("Wrong bucket. Ignoring.")
        return

    if not key.startswith("target/crm/"):
        print("Not a CRM target object. Ignoring.")
        return

    # Check whether the Glue job is already running
    response = glue.get_job_runs(
        JobName=GLUE_JOB_NAME,
        MaxResults=10
    )

    running_states = {
        "STARTING",
        "RUNNING",
        "STOPPING"
    }

    for job_run in response.get("JobRuns", []):

        if job_run.get("JobRunState") in running_states:

            print(
                f"{GLUE_JOB_NAME} is already running. "
                f"Skipping duplicate trigger."
            )

            return {
                "status": "SKIPPED",
                "reason": "Glue job already running"
            }

    # Start Glue job
    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME
    )

    job_run_id = response["JobRunId"]

    print(
        f"Started Glue job: "
        f"{GLUE_JOB_NAME}, "
        f"Run ID: {job_run_id}"
    )

    return {
        "status": "STARTED",
        "jobRunId": job_run_id
    }
