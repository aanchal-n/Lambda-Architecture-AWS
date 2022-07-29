import json
import logging
import time
import boto3
from botocore.exceptions import ClientError

def get_kinesis_arn(stream_name):
    """Retrieve the ARN for a Kinesis data stream

    :param stream_name: Kinesis data stream name
    :return: ARN of stream. If error, return None.
    dummy comment
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
    """Retrieve the ARN of the specified Firehose

    :param firehose_name: Firehose stream name
    :return: If the Firehose stream exists, return ARN, else None
    """

    # Try to get the description of the Firehose
    firehose_client = boto3.client('firehose')
    try:
        result = firehose_client.describe_delivery_stream(DeliveryStreamName=firehose_name)
    except ClientError as e:
        logging.error(e)
        return None
    return result['DeliveryStreamDescription']['DeliveryStreamARN']

def wait_for_active_firehose(firehose_name):
    """Wait until the Firehose delivery stream is active

    :param firehose_name: Name of Firehose delivery stream
    :return: True if delivery stream is active. Otherwise, False.
    """

    # Wait until the stream is active
    firehose_client = boto3.client('firehose')
    while True:
        try:
            # Get the stream's current status
            result = firehose_client.describe_delivery_stream(DeliveryStreamName=firehose_name)
        except ClientError as e:
            logging.error(e)
            return False
        status = result['DeliveryStreamDescription']['DeliveryStreamStatus']
        if status == 'ACTIVE':
            return True
        if status == 'DELETING':
            logging.error(f'Firehose delivery stream {firehose_name} is being deleted.')
            return False
        time.sleep(2)


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
    
    firehose_arn = get_firehose_arn(firehose_name)

    if firehose_arn is None:
        exit(1)
    
    logging.info(f'Created Firehose delivery stream to S3: {firehose_arn}')

    # Wait for the Firehose to become active
    if not wait_for_active_firehose(firehose_name):
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

        batch = [{'Data': next(f)} for x in range(1000)]

        try:
            result = kinesis_client.put_records(StreamName=kinesis_name,
                                                Records=batch)
        except ClientError as e:
            logging.error(e)
            exit(1)


        num_failures = result['FailedRecordCount']
        '''
        # Test: Simulate a failed record
        num_failures = 1
        failed_rec_index = 3
        result['Records'][failed_rec_index]['ErrorCode'] = 404
        '''
        if num_failures:
            # Resend failed records
            logging.info(f'Resending {num_failures} failed records')
            rec_index = 0
            for record in result['Records']:
                if 'ErrorCode' in record:
                    # Resend the record
                    kinesis_client.put_record(StreamName=kinesis_name,
                                              Data=batch[rec_index]['Data'],
                                              PartitionKey=batch[rec_index]['PartitionKey'])

                    # Stop if all failed records have been resent
                    num_failures -= 1
                    if not num_failures:
                        break
                rec_index += 1

    logging.info('Test data sent to Kinesis stream')


if __name__ == '__main__':
    main()
