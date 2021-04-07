"""Tests for the AWS S3 service."""


import unittest
import mock

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import s3
from tests import pkb_common_test_case


class S3Test(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(S3Test, self).setUp()
    flag_values = {
        'timeout_minutes': 0,
        'persistent_timeout_minutes': 0}
    p = mock.patch.object(s3, 'FLAGS')
    flags_mock = p.start()
    flags_mock.configure_mock(**flag_values)
    self.mock_command = mock.patch.object(vm_util, 'IssueCommand').start()
    self.mock_retryable_command = mock.patch.object(
        vm_util, 'IssueRetryableCommand').start()
    self.s3_service = s3.S3Service()
    self.s3_service.PrepareService(None)  # will use s3.DEFAULT_AWS_REGION

  def tearDown(self):
    super(S3Test, self).tearDown()
    mock.patch.stopall()

  def test_make_bucket(self):
    self.mock_command.return_value = (None, None, None)
    self.s3_service.MakeBucket(bucket_name='test_bucket')
    self.mock_command.assert_called_once_with([
        'aws', 's3', 'mb', 's3://test_bucket',
        '--region={}'.format(s3.DEFAULT_AWS_REGION)], raise_on_failure=False)
    self.mock_retryable_command.assert_called_once_with([
        'aws', 's3api', 'put-bucket-tagging', '--bucket', 'test_bucket',
        '--tagging', 'TagSet=[]', '--region={}'.format(s3.DEFAULT_AWS_REGION)])

if __name__ == '__main__':
  unittest.main()
