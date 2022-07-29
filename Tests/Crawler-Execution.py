from Run_Crawler import start_a_crawler
import boto3
import unittest

class TestBucketCreation(unittest.TestCase):
    def test_create_UniqueBucket(self):

        crawler_name = "s3-to-s3-crawler"
        response = start_a_crawler(crawler_name)

        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

if __name__ == '__main__':
    unittest.main()