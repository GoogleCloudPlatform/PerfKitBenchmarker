"""Tests for the AWS S3 service."""

import unittest
import mock
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import s3
from tests import pkb_common_test_case


class S3Test(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    flag_values = {'timeout_minutes': 0, 'persistent_timeout_minutes': 0}
    p = mock.patch.object(s3, 'FLAGS')
    flags_mock = p.start()
    flags_mock.configure_mock(**flag_values)
    self.mock_command = mock.patch.object(vm_util, 'IssueCommand').start()
    self.mock_retryable_command = mock.patch.object(
        vm_util, 'IssueRetryableCommand'
    ).start()
    self.s3_service = s3.S3Service()
    self.s3_service.PrepareService(None)  # will use s3.DEFAULT_AWS_REGION

  def tearDown(self):
    super().tearDown()
    mock.patch.stopall()

  def test_make_bucket(self):
    self.mock_command.return_value = (None, None, None)
    self.s3_service.MakeBucket(bucket_name='test_bucket')
    self.mock_command.assert_called_once_with(
        [
            'aws',
            's3api',
            'create-bucket',
            '--bucket=test_bucket',
            '--region={}'.format(s3.DEFAULT_AWS_REGION),
        ],
        raise_on_failure=False,
    )
    self.mock_retryable_command.assert_called_once_with([
        'aws',
        's3api',
        'put-bucket-tagging',
        '--bucket',
        'test_bucket',
        '--tagging',
        'TagSet=[]',
        '--region={}'.format(s3.DEFAULT_AWS_REGION),
    ])

  def test_s3_bucket_create_regional_by_default(self):
    spec = s3.S3BucketSpec(
        mount_point='/mnt',
        bucket_name='mountpoint-test',
        region='us-east-2',
        zone='us-east-2a',
        is_s3_express=False,
    )
    bucket = s3.S3Bucket(spec)
    with mock.patch.object(bucket.service, 'MakeBucket') as mock_make_bucket:
      bucket._Create()
      mock_make_bucket.assert_called_once_with('mountpoint-test')
    self.assertEqual(bucket.service.region, 'us-east-2')
    self.assertIsNone(bucket.service.zone)

  @mock.patch.object(s3.util, 'GetZoneId', return_value='use2-az1')
  def test_s3_bucket_create_zonal_when_s3_express(self, mock_get_zone_id):
    spec = s3.S3BucketSpec(
        mount_point='/mnt',
        bucket_name='mountpoint-test--use2-az1--x-s3',
        region='us-east-2',
        zone='us-east-2a',
        is_s3_express=True,
    )
    bucket = s3.S3Bucket(spec)
    with mock.patch.object(bucket.service, 'MakeBucket') as mock_make_bucket:
      bucket._Create()
      mock_make_bucket.assert_called_once_with(
          'mountpoint-test--use2-az1--x-s3'
      )
    mock_get_zone_id.assert_called_once_with('us-east-2a')
    self.assertEqual(bucket.service.region, 'us-east-2')
    self.assertEqual(bucket.service.zone, 'us-east-2a')
    self.assertEqual(bucket.service.zone_id, 'use2-az1')


if __name__ == '__main__':
  unittest.main()
