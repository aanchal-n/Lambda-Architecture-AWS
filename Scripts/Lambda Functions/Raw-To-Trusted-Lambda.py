import json
import urllib.parse
import boto3
import Run_Crawler
import Run_Glue_Job

print('Loading function')

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

def lambda_handler(event, context):
    
    
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        
        resp = s3.list_objects_v2(Bucket='raw-lambda-poc')
        keys=[]
        for obj in resp['Contents']:
            if obj['Key'] != key:
                existingObj = s3.get_object(Bucket=bucket,Key=obj['Key'])
                print(obj['Key'])
                if response['ETag'] == existingObj['ETag'] and response['ContentLength'] == existingObj['ContentLength']:
                    print(key,"is a Duplicate")
                    s3_resource.Object('raw-lambda-poc', key).delete()
        
        Run_Crawler.start_a_crawler('s3-to-s3-crawler')
        Run_Glue_Job.run_glue_job('S3-to-S3-transform ')
        
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
