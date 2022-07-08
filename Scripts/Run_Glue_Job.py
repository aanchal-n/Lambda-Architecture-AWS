import boto3
from botocore.exceptions import ClientError

# Takes the glue job's name as an argument and triggers execution
def run_glue_job(job_name, arguments = {}):
    glue_client = boto3.client('glue',region_name ="ap-south-1")
    try:
        job_run_id = glue_client.start_job_run(JobName="S3-to-S3-transform ")
        return job_run_id

    except ClientError as e:
        raise Exception( "boto3 client error in run_glue_job: " + e.__str__())
    except Exception as e:
        raise Exception( "Unexpected error in run_glue_job: " + e.__str__())