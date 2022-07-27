import json
import logging
import time
import boto3
from botocore.exceptions import ClientError
import firehose_to_s3 as fh_s3

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


def wait_for_active_kinesis_stream(stream_name):
    """Wait for a new Kinesis data stream to become active

    :param stream_name: Data stream name
    :return: True if steam is active. False if error creating stream.
    """

    # Wait until the stream is active
    kinesis_client = boto3.client('kinesis')
    while True:
        try:
            # Get the stream's current status
            result = kinesis_client.describe_stream_summary(StreamName=stream_name)
        except ClientError as e:
            logging.error(e)
            return False
        status = result['StreamDescriptionSummary']['StreamStatus']
        if status == 'ACTIVE':
            return True
        if status == 'DELETING':
            logging.error(f'Kinesis stream {stream_name} is being deleted.')
            return False
        time.sleep(5)

def get_firehose_arn(firehose_name):
    """Retrieve the ARN for a Kinesis data stream

    :param stream_name: Kinesis data stream name
    :return: ARN of stream. If error, return None.
    """

    # Retrieve stream info
    firehose_client = boto3.client('firehose')
    try:
        result = firehose_client.describe_stream_summary(StreamName=firehose_name)
    except ClientError as e:
        logging.error(e)
        return None
    return result['StreamDescriptionSummary']['StreamARN']


def main():

    kinesis_name = 'kinesis_test_stream'
    number_of_shards = 1
    firehose_name = 'firehose_kinesis_test_stream'
    bucket_arn = 'arn:aws:s3:::kinesis-datastream-test-poc'
    iam_role_name = 'kinesis_to_firehose_to_s3'

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s: %(message)s')

    # Wait for the stream to become active
    logging.info(f'Waiting for new Kinesis stream {kinesis_name} to become active...')
    if not wait_for_active_kinesis_stream(kinesis_name):
        exit(1)
    logging.info(f'Kinesis stream {kinesis_name} is active')

    # Retrieve the Kinesis stream's ARN
    kinesis_arn = get_kinesis_arn(kinesis_name)

    # Create a Firehose delivery stream as a consumer of the Kinesis stream
    firehose_src_type = 'KinesisStreamAsSource'
    #firehose_arn = fh_s3.create_firehose_to_s3(firehose_name,
                                               #bucket_arn,
                                               #iam_role_name,
                                               #firehose_src_type,
                                               #kinesis_arn)
    firehose_arn = "arn:aws:firehose:ap-south-1:581950338244:deliverystream/firehose_kinesis_test_stream"

    if firehose_arn is None:
        exit(1)
    
    logging.info(f'Created Firehose delivery stream to S3: {firehose_arn}')

    # Wait for the Firehose to become active
    if not fh_s3.wait_for_active_firehose(firehose_name):
        exit(1)
    logging.info('Firehose stream is active')

    test_data_file = 'aanchal_flt_ind_20220707_180156.csv'
    kinesis_client = boto3.client('kinesis')
    with open(test_data_file, 'rb') as f:
        logging.info('Putting 1000 records into the Kinesis stream one at a time')
        for i in range(1000):
            # Read a record of test data
            line = next(f)

            # Extract the "sector" value to use as the partition key
            sector = "flt"

            # Put the record into the stream
            try:
                result = kinesis_client.put_record(StreamName=kinesis_name,
                                          Data=line,
                                          PartitionKey=sector)
            except ClientError as e:
                logging.error(e)
                exit(1)

        time.sleep(100)
        logging.info('Putting next 1000 records')

        for i in range(1000):
            # Read a record of test data
            line = next(f)

            # Extract the "sector" value to use as the partition key
            sector = "flt"

            # Put the record into the stream
            try:
                result = kinesis_client.put_record(StreamName=kinesis_name,
                                          Data=line,
                                          PartitionKey=sector)
            except ClientError as e:
                logging.error(e)
                exit(1)

    logging.info('Test data sent to Kinesis stream')


if __name__ == '__main__':
    main()
