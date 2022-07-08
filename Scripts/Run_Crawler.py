import boto3
from botocore.exceptions import ClientError

# Given the cralwer's name as an argument, issues a command to trigger the crawler 
def start_a_crawler(crawler_name):
    session = boto3.session.Session()
    glue_client = session.client('glue')
    try:
        response = glue_client.start_crawler(Name=crawler_name)
        return response
    except ClientError as e:
        raise Exception("boto3 client error in start_a_crawler: " + e.__str__())
    except Exception as e:
        raise Exception("Unexpected error in start_a_crawler: " + e.__str__())

