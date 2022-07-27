import boto3
import json 
import os 
from os import listdir
from os.path import isfile, join
import re
from botocore.exceptions import ClientError
import logging

def create_iam_role(roleName):
    # params
    # roleName: name of the new role being created for lambda architecture components 
    # Usage 
    # Creates a new role and attachs policies related to S3, Glue, Kinesis and Lambda 

    client = boto3.client('iam')

    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::581950338244:user/Aanchal-jpmc-poc",
                    "Service": "glue.amazonaws.com",
                    "Service": "firehose.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]   
    }

    try:
        response = client.create_role(
            RoleName= roleName,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy)
        )
        print(roleName, "created Successfuly")

    except Exception as e:
        print(roleName, "create failed with error", e)

    resource = boto3.resource('iam')
    role = resource.Role(roleName)

    role.attach_policy(PolicyArn = 'arn:aws:iam::aws:policy/AmazonS3FullAccess')
    role.attach_policy(PolicyArn = 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole')
    role.attach_policy(PolicyArn = 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole')
    role.attach_policy(PolicyArn = 'arn:aws:iam::aws:policy/AmazonKinesisFullAccess')
    role.attach_policy(PolicyArn = 'arn:aws:iam::aws:policy/AmazonKinesisFirehoseFullAccess')
    role.attach_policy(PolicyArn = 'arn:aws:iam::aws:policy/AmazonKinesisAnalyticsFullAccess')

    return response

def create_bucket(bucketName):
    # params
    # bucketName: unique bucket name in the region 
    # Usage
    # Uses the boto3 client to create a bucket 

    client = boto3.client("s3", region_name = "ap-south-1")

    location = {'LocationConstraint': "ap-south-1"}
    
    try:
        response = client.create_bucket(Bucket=bucketName, CreateBucketConfiguration=location)
        print(bucketName,"created successfully")
    except Exception as e:
        print(bucketName,"creation failed with error",e)

    return response

def create_glue_database(dbName):
    # params
    # dbName: the name of the database to catalog all glue tables for connecting to glue studio, athena, quicksight, redshift, etc
    # usage 
    # uses the boto3 client to create a Database to store tables of crawled s3 buckets 

    client = boto3.client('glue')

    try:
        response = client.create_database(DatabaseInput={
            'Name': dbName,
            'Description': 'Glue Catalog for Crawled Tables from Lambda Arch Pipeline'
        })
        print(dbName,"glueDatabase created successfully")

    except Exception as e:
        print(dbName,"creation failed with error",e)

    return response

def create_crawler(crawlerName, iamRole, dbName, bucketName):
    # params 
    # crawlerName: unique name for cralwer 
    # iamRole: Role assumed to create crawler (has to have aws managed GlueServiceRole attached
    # dbName: the database where the tables generated are stored 
    # bucketName: source of data whose files are to be crawled to generate tables 
    # usage 
    # uses the parameters to create crawlers to generate tables to monitor schema changes 

    client = boto3.client('glue')

    response = client.create_crawler(
        Name= crawlerName,
        Role= iamRole,
        DatabaseName= dbName,
        Targets={
            'S3Targets': [
                {
                    'Path': 's3://{BUCKET_NAME}/'.format(BUCKET_NAME = bucketName),
                }
            ]
        },
        TablePrefix='lambda-'
    )

    print(response)
    return response 

def ingest_glue_script(bucketName):
    # params
    # bucketName: String consisting of bucket name to store glue scripts
    # usage
    # scans the current working directory for .py files starting with glue to indicate scripts for glue jobs 
    # compares with current contents of s3 bucket to avoid duplication of script before uploading to a bucket 

    dir = os.curdir
    onlyfiles = [f for f in listdir(dir) if isfile(join(dir, f))]
    index = 0
    accepted_formats = [".py"]

    while index < len(onlyfiles):
        name,extension = os.path.splitext(onlyfiles[index])
        index+=1
        if extension not in accepted_formats:
            onlyfiles.remove(onlyfiles[index-1])
            index = 0
    
    finalFiles = []
    for curFile in onlyfiles:
        res = re.match(r'glue.*\.py$',curFile)
        if res is not None:
            finalFiles.append(curFile)

    currentWorkingDir = os.getcwd()
    
    s3 = boto3.client('s3')
    response = s3.list_objects(Bucket=bucketName)
    response=response['Contents']
    
    filesInBucket = []
    for flag in range(0,len(response)):
        filesInBucket.append(response[flag]['Key'])

    for index in range(0,len(finalFiles)):
        if finalFiles[index] in filesInBucket:
            print(finalFiles[index],"already present in Bucket")
            continue
        else:
            filePath = currentWorkingDir+"/"+ finalFiles[index]
            fileName = finalFiles[index]
            response = s3.upload_file(filePath, bucketName, fileName)
            print("Uploaded", fileName)

    return 

def create_glue_job(jobName, iamRole, scriptPath, pythonVer):
    # params
    # jobName: String consisting of unique job name 
    # iamRole: predefined IAM role with glueService policy attached 
    # scriptPath: location in s3 bucket where script is present 
    # pythonVer: to specify the python version 
    # usage
    # uses the given parameters to create a new glue job 

    client = boto3.client('glue')

    response = client.create_job(
        Name= jobName,
        Role= iamRole,
        Command={
            'Name': jobName,
            'ScriptLocation': scriptPath,
            'PythonVersion': pythonVer
        },
        DefaultArguments={
        '--TempDir': 's3://glue-source-hoc/temp_dir',
        '--job-bookmark-option': 'job-bookmark-disable'
        },
        MaxRetries=1,
        GlueVersion='3.0',
        NumberOfWorkers=2,
        WorkerType='Standard'
    )

    return response

def create_kinesis_stream(stream_name, num_shards=1):
    # params
    # stream_name: Data stream name
    # num_shards: Number of stream shards
    # usage
    # Create a Kinesis data stream and returns true if stream was started 

    # Create the data stream
    kinesis_client = boto3.client('kinesis')

    try:
        response = kinesis_client.create_stream(StreamName=stream_name,
                                     ShardCount=num_shards)
        print(response)

    except ClientError as e:
        logging.error(e)
        return False
    return True

def get_kinesis_arn(stream_name):
    """Retrieve the ARN for a Kinesis data stream

    :param stream_name: Kinesis data stream name
    :return: ARN of stream. If error, return None.
    """

    # Retrieve stream info
    kinesis_client = boto3.client('kinesis')
    try:
        result = kinesis_client.describe_stream_summary(StreamName=stream_name)
    except ClientError as e:
        logging.error(e)
        return None
    return result['StreamDescriptionSummary']['StreamARN']

def get_iam_role_arn(iam_role_name):
    """Retrieve the ARN of the specified IAM role

    :param iam_role_name: IAM role name
    :return: If the IAM role exists, return ARN, else None
    """

    # Try to retrieve information about the role
    iam_client = boto3.client('iam')
    try:
        result = iam_client.get_role(RoleName=iam_role_name)
    except ClientError as e:
        logging.error(e)
        return None
    return result['Role']['Arn']

def create_firehose_to_s3(firehose_name, s3_bucket_arn, iam_role_name,
                          firehose_src_type='DirectPut',
                          firehose_src_stream=None):
    """Create a Kinesis Firehose delivery stream to S3

    The data source can be either a Kinesis Data Stream or puts sent directly
    to the Firehose stream.

    :param firehose_name: Delivery stream name
    :param s3_bucket_arn: ARN of S3 bucket
    :param iam_role_name: Name of Firehose-to-S3 IAM role. If the role doesn't
        exist, it is created.
    :param firehose_src_type: 'DirectPut' or 'KinesisStreamAsSource'
    :param firehose_src_stream: ARN of source Kinesis Data Stream. Required if
        firehose_src_type is 'KinesisStreamAsSource'
    :return: ARN of Firehose delivery stream. If error, returns None.
    """

    iam_role = get_iam_role_arn(iam_role_name)

    # Create the S3 configuration dictionary
    # Both BucketARN and RoleARN are required
    # Set the buffer interval=60 seconds (Default=300 seconds)
    s3_config = {
        'BucketARN': s3_bucket_arn,
        'RoleARN': iam_role,
        'BufferingHints': {
            'IntervalInSeconds': 60,
        },
    }

    # Create the delivery stream
    # By default, the DeliveryStreamType='DirectPut'
    firehose_client = boto3.client('firehose')
    try:
        if firehose_src_type == 'KinesisStreamAsSource':
            # Define the Kinesis Data Stream configuration
            stream_config = {
                'KinesisStreamARN': firehose_src_stream,
                'RoleARN': iam_role,
            }
            result = firehose_client.create_delivery_stream(
                DeliveryStreamName=firehose_name,
                DeliveryStreamType=firehose_src_type,
                KinesisStreamSourceConfiguration=stream_config,
                ExtendedS3DestinationConfiguration=s3_config)
        else:
            result = firehose_client.create_delivery_stream(
                DeliveryStreamName=firehose_name,
                DeliveryStreamType=firehose_src_type,
                ExtendedS3DestinationConfiguration=s3_config)
    except ClientError as e:
        logging.error(e)
        return None
    return result['DeliveryStreamARN']


if __name__ == "__main__":

    create_bucket("raw-lambdaarch")
    create_bucket("trusted-lambdaarch")
    create_bucket("refined-lambdaarch")
    create_bucket("kinesis-datastream-ingress")
    create_bucket("streamed-lambda")
    create_bucket("shift-to-refined")
    create_bucket("shift-to-trusted")
    create_bucket("glue-scripts-lambdaarch")

    roleName = "lambda-poc"
    create_iam_role(roleName)

    glueDB = "lambdaCatalog"
    create_glue_database(glueDB)
    
    create_crawler("Raw", roleName, glueDB, "raw-lambdaarch")
    create_crawler("Trusted", roleName, glueDB, "trusted-lambdaarch")

    ingest_glue_script("glue-scripts-lambdaarch")
    create_glue_job("Raw-to-Trusted", roleName, "s3://glue-scripts-lambdaarch/glue-raw-to-trusted.py","3")
    create_glue_job("Trusted-to-Refined", roleName, "s3://glue-scripts-lambdaarch/glue-trusted-to-refined.py","3")

    kinesis_name = 'kinesis_lambda_stream'
    create_kinesis_stream(kinesis_name)
    kinesis_arn = get_kinesis_arn(kinesis_name)

    firehose_name = 'firehose_kinesis_lambda_stream'
    bucket_arn = 'arn:aws:s3:::kinesis-datastream-ingress'
    firehose_src_type = 'KinesisStreamAsSource'

    firehose_arn = create_firehose_to_s3(firehose_name, bucket_arn, roleName, firehose_src_type, kinesis_arn)