import logging
import boto3
from botocore.exceptions import ClientError
import sys
import os

def abs_path_check(path):
    """ Tests to see if the path inputted is absolute. If it is relative, gives the user one try to re-enter the path. 
    Terminates if second path inputted is also relative 

    :param path: path being tested 
    :return: path if the path is absolute, else informs the user and terminates current execution 
    """

    if os.path.isabs(path):
        return path
    else:
        new_path = input("Enter Absolute path")
        if os.path.isabs(new_path):
            print("Updated Path")
            return new_path
        else:
            print("Incorrect Path Specification. Program Terminating")
            sys.exit(1)

def file_at_path_check(path):
    """ Tests to see if a file exists at the current path. If the path is of a directory or doesn't exist,
     appropriately informs the user and terminates current execution . 

    :param path: path being verified 
    """

    if os.path.exists(path):
        if os.path.isfile(path):
            print("Path Verified and File exists")
            return
        elif os.path.isdir(path):
            print("Inputted path is a directory. Program Terminating")
            sys.exit(1)
    else:
        print("File does not exist at given path. Program terminating")
        sys.exit(1)

def file_extension_check(name):
    """ Tests to see if the file extension is among the expected input file formats. 
    If it has multiple extensions or is not in expected format, informs the user and terminates the program 

    :param name: name of the file being tested 
    """

    accepted_formats = [".csv",".tsv",".xlsx",".txt"]
    name,extension = os.path.splitext(name)
    name, extension_2 = os.path.splitext(name)

    if extension_2 != "":
        print("File with Multiple extensions. Program Terminating")
        sys.exit(1)
    elif extension in accepted_formats:
        print("File is in accepted format")
        return
    else:
        print(extension,"file is not accepted. Program Terminating")
        sys.exit(1)

def file_empty_check(path):
    """ Tests to see if the file is empty. 
    If the file is empty, prompts user on wether to proceed with upload. Terminates current execution if user does not want to proceed with upload

    :param path: path of the file being tested  
    """

    if os.path.getsize(path) == 0:
        option = input("File is empty. Proceed with upload? [Y/N]")
        if option.upper() == 'Y':
            return 
        elif option.upper() == 'N':
            print("File at Given path is empty. Program Terminating")
            sys.exit(1)
        else:
            print("Incorrect option entered and File is empty. Program Terminating")
            sys.exit(1)
    else:
        return 

def bucket_exist_check(bucket_name):
    """ Tests to see if the bucket user wants to uplaod to exists. 
    If it does not exist, lists existing buckets and terminates current execution 

    :param bucket_name: name of the bucket user wants to upload the dataset to 
    """

    existing_buckets = boto3.resource("s3").buckets.all()
    
    buckets = []

    for bucket in existing_buckets:
        buckets.append(bucket.name)

    if bucket_name in buckets:
        print("Bucket is present")
        return 
    else:        
        print("Bucket with inputted name was not found. Current Buckets are")
        for current_bucket in buckets:
            print(current_bucket)
        print("Program Terminating")
        sys.exit(1)

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        print("File Successfully uploaded to S3 Bucket")
        sys.exit(0)

    except ClientError as e:
        logging.error(e)
        print("File not uploaded to S3 bucket")
        print(e)
        sys.exit(1)

if __name__ == "__main__":

    file_path = input("Enter absolute file path")

    file_path = abs_path_check(file_path) #test to see if absolute path is inputted to avoid path issues due to relativity 
    
    file_at_path_check(file_path) #test to see if the file exists at the given path 

    file_name = os.path.basename(file_path)
    file_extension_check(file_name) #test to ensure that file has only one, valid extension 

    file_empty_check(file_path) #test to see if file is empty 

    bucket_name = input("Enter bucket name")

    bucket_exist_check(bucket_name) #test to see if bucket is present 

    upload_file(file_path,bucket_name,file_name) #upload file to bucket if all of the above criteria is met 