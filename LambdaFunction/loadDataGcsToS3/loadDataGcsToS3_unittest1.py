import unittest
from unittest import mock
import sys
sys.path.insert(1, '/path/to/LAMBDAFUNCTION/loadDataGcsToS3')
from loadDataGcsToS3.lambda_function import get_key, connect_gcs, get_log, send_alert_slack
import os
from unittest.mock import patch

gc_bucket = 'dont8_bucket'
google_access_key_id = ''
google_access_key_secret = ''

class loadDataGcsToS3FileTest(unittest.TestCase):
    
    def test_get_key_in_parameter_store(self):
        expected_param1 = google_access_key_id
        expected_param2 = google_access_key_secret
        test_parameter1, test_parameter2 = get_key()
        self.assertEqual(expected_param1, test_parameter1)
        self.assertEqual(expected_param2, test_parameter2)

    def test_get_client_and_data_from_gcs(self):
        gcs_client = connect_gcs(google_access_key_id, google_access_key_secret)
        data_name = gcs_client.list_objects_v2(Bucket=gc_bucket)['Contents'][0]['Key']
        print("gcs_data: ", data_name)
        gcs_client_result = '2015/01/data_000000000000.csv'
        self.assertEqual(data_name, gcs_client_result)

    @patch.dict(os.environ, {"AWS_LAMBDA_LOG_GROUP_NAME": "MyLog/IngestData", "AWS_REGION": "us-east-1"})
    def test_get_log_url(self):
        log_stream_url = get_log()
        expected_result = 'https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/MyLog$252FIngestData'
        self.assertEqual(expected_result, log_stream_url)

if __name__ == '__main__':
    unittest.main()