import datetime
import unittest

from absl import flags
import mock
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import bigquery_slot_resource
from tests import pkb_common_test_case


FLAGS = flags.FLAGS

_TEST_RUN_URI = 'fakeru'
_GCP_ZONE_US_CENTRAL_1_C = 'us-central1-c'

TEST_SPEC = mock.Mock()


def GetBigquerySlotsTestInstance(spec=TEST_SPEC):
  return bigquery_slot_resource.BigquerySlotResource(spec)


class BigquerySlotsTestCase(pkb_common_test_case.PkbCommonTestCase):
  SLOT_NUM = 500
  TEST_PROJECT = 'test_project'
  TEST_REGION = 'aws-fake-region'

  def setUp(self):
    super().setUp()
    FLAGS.cloud = 'GCP'
    FLAGS.run_uri = _TEST_RUN_URI
    FLAGS.zones = [_GCP_ZONE_US_CENTRAL_1_C]
    FLAGS.bq_slot_allocation_project = self.TEST_PROJECT
    FLAGS.bq_slot_allocation_num = self.SLOT_NUM
    FLAGS.bq_slot_allocation_region = self.TEST_REGION
    self.test_instance = GetBigquerySlotsTestInstance()

  @mock.patch.object(
      vm_util,
      'IssueCommand',
      return_value=(
          '{"name": "region/project/test_id/1", "state":"ACTIVE"}',
          'fake_stderr',
          0,
      ),
  )
  def testCreate(self, mock_issue):
    self.test_instance._Create()
    self.assertEqual(mock_issue.call_count, 1)
    self.assertTrue(self.test_instance.marked_active)
    command_string = ' '.join(mock_issue.call_args[0][0])
    self.assertIn(f'--project_id={self.TEST_PROJECT}', command_string)
    self.assertIn(f'--location={self.TEST_REGION}', command_string)
    self.assertIn(f'--slots={self.SLOT_NUM}', command_string)
    self.assertEqual(self.test_instance.compute_resource_identifier, '1')

  @mock.patch.object(
      vm_util,
      'IssueCommand',
      return_value=('{"name": "region/project/test_id/1"}', 'fake_stderr', 0),
  )
  def testDelete(self, mock_issue):
    self.test_instance.compute_resource_identifier = '1'
    self.test_instance.final_commitment_start_time = (
        datetime.datetime.now() - datetime.timedelta(minutes=1)
    )
    self.test_instance._Delete()
    self.assertEqual(mock_issue.call_count, 1)
    command_string = ' '.join(mock_issue.call_args[0][0])
    self.assertIn(f'--project_id={self.TEST_PROJECT}', command_string)
    self.assertIn(f'--location={self.TEST_REGION}', command_string)

  @mock.patch.object(
      vm_util,
      'IssueCommand',
      return_value=('[{"name": "region/project/test_id/1"}]', 'fake_stderr', 0),
  )
  def testExistsTrue(self, mock_issue):
    self.test_instance.compute_resource_identifier = '1'
    self.assertTrue(self.test_instance._Exists())
    self.assertEqual(mock_issue.call_count, 1)
    command_string = ' '.join(mock_issue.call_args[0][0])
    self.assertIn(f'--project_id={self.TEST_PROJECT}', command_string)
    self.assertIn(f'--location={self.TEST_REGION}', command_string)

  @mock.patch.object(
      vm_util,
      'IssueCommand',
      return_value=('[{"name": "region/project/test_id/1"}]', 'fake_stderr', 0),
  )
  def testExistsFalse(self, mock_issue):
    self.test_instance.compute_resource_identifier = '2'
    self.assertFalse(self.test_instance._Exists())
    self.assertEqual(mock_issue.call_count, 1)
    command_string = ' '.join(mock_issue.call_args[0][0])
    self.assertIn(f'--project_id={self.TEST_PROJECT}', command_string)
    self.assertIn(f'--location={self.TEST_REGION}', command_string)

  @mock.patch.object(
      vm_util,
      'IssueCommand',
      return_value=('No Commitments', 'fake_stderr', 0),
  )
  def testExistsNoCommitments(self, mock_issue):
    self.test_instance.compute_resource_identifier = '2'
    self.assertFalse(self.test_instance._Exists())
    self.assertEqual(mock_issue.call_count, 1)
    command_string = ' '.join(mock_issue.call_args[0][0])
    self.assertIn(f'--project_id={self.TEST_PROJECT}', command_string)
    self.assertIn(f'--location={self.TEST_REGION}', command_string)


if __name__ == '__main__':
  unittest.main()
