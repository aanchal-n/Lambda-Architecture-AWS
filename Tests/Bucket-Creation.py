from urllib import response
from setup import create_bucket
import boto3
import unittest
from re import search

class TestBucketCreation(unittest.TestCase):
    def test_create_UniqueBucket(self):
        bucketName =  "aanchals-test-bucket-1"
        
        response = create_bucket(bucketName)
        location = response['Location']
        s3ResponseBucket = location[7:].split('.')

        self.assertEqual(bucketName, s3ResponseBucket[0])

    def test_create_duplicateBucket(self):
        bucketName =  "aanchals-test-bucket-1"
        
        response = create_bucket(bucketName)

        response = str (response)
        ErrorCheck = False
        
        if search("BucketAlreadyOwned", response):
            ErrorCheck = True

        self.assertEqual(ErrorCheck,True)

if __name__ == '__main__':
    unittest.main()