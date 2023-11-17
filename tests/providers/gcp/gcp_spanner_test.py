"""Tests for google3.third_party.py.perfkitbenchmarker.providers.gcp.gcp_spanner."""

import inspect
import unittest

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_spanner
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case

import requests

FLAGS = flags.FLAGS


def GetTestSpannerInstance(engine='spanner-googlesql'):
  spec_args = {'cloud': 'GCP', 'engine': engine}
  spanner_spec = gcp_spanner.SpannerSpec(
      'test_component', flag_values=FLAGS, **spec_args)
  spanner_spec.spanner_database_name = 'test_database'
  spanner_class = relational_db.GetRelationalDbClass(
      cloud='GCP', is_managed_db=True, engine=engine)
  return spanner_class(spanner_spec)


class SpannerTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    saved_flag_values = flagsaver.save_flag_values()
    FLAGS.run_uri = 'test_uri'
    self.addCleanup(flagsaver.restore_flag_values, saved_flag_values)

  def testFlagOverridesAutoScaler(self):
    FLAGS['cloud_spanner_autoscaler'].parse('True')

    test_instance = GetTestSpannerInstance()
    self.assertEqual(test_instance._autoscaler, True)
    self.assertEqual(test_instance._min_processing_units, 5000)
    self.assertEqual(test_instance._max_processing_units, 50000)
    self.assertEqual(test_instance._high_priority_cpu_target, 65)
    self.assertEqual(test_instance._storage_target, 95)

  def testFlagOverrides(self):
    FLAGS['cloud_spanner_config'].parse('regional-us-central1')
    FLAGS['cloud_spanner_nodes'].parse(5)
    FLAGS['cloud_spanner_project'].parse('test_project')

    test_instance = GetTestSpannerInstance()

    self.assertEqual(test_instance.nodes, 5)
    self.assertEqual(test_instance._config, 'regional-us-central1')
    self.assertEqual(test_instance.project, 'test_project')

  def testSetNodes(self):
    test_instance = GetTestSpannerInstance()
    # Don't actually issue a command.
    self.enter_context(
        mock.patch.object(test_instance, '_GetNodes', return_value=1)
    )
    self.enter_context(
        mock.patch.object(test_instance, '_WaitUntilInstanceReady')
    )
    cmd = self.enter_context(
        mock.patch.object(
            vm_util, 'IssueCommand', return_value=[None, None, 0]))

    test_instance._SetNodes(3)

    self.assertIn('--nodes 3', ' '.join(cmd.call_args[0][0]))

  def testSetNodesSkipsIfCountAlreadyCorrect(self):
    test_instance = GetTestSpannerInstance()
    self.enter_context(
        mock.patch.object(test_instance, '_GetNodes', return_value=1)
    )
    self.enter_context(
        mock.patch.object(test_instance, '_WaitUntilInstanceReady')
    )
    cmd = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=[None, None, 0])
    )

    test_instance._SetNodes(1)

    cmd.assert_not_called()

  def testFreezeUsesCorrectNodeCount(self):
    instance = GetTestSpannerInstance()
    mock_set_nodes = self.enter_context(
        mock.patch.object(instance, '_SetNodes', autospec=True))

    instance._Freeze()

    mock_set_nodes.assert_called_once_with(gcp_spanner._FROZEN_NODE_COUNT)

  def testRestoreUsesCorrectNodeCount(self):
    instance = GetTestSpannerInstance()
    instance.nodes = 5
    mock_set_nodes = self.enter_context(
        mock.patch.object(instance, '_SetNodes', autospec=True))

    instance._Restore()

    mock_set_nodes.assert_called_once_with(5)

  @flagsaver.flagsaver(run_uri='test_uri')
  def testUpdateLabels(self):
    # Arrange
    instance = GetTestSpannerInstance()
    mock_endpoint_response = '"https://spanner.googleapis.com"'
    mock_labels_response = inspect.cleandoc("""
    {
      "config": "test_config",
      "displayName": "test_display_name",
      "labels": {
        "benchmark": "test_benchmark",
        "timeout_minutes": "10"
      },
      "name": "test_name"
    }
    """)
    self.enter_context(
        mock.patch.object(
            util.GcloudCommand,
            'Issue',
            side_effect=[(mock_endpoint_response, '', 0),
                         (mock_labels_response, '', 0)]))
    self.enter_context(
        mock.patch.object(util, 'GetAccessToken', return_value='test_token'))
    mock_request = self.enter_context(
        mock.patch.object(
            requests, 'patch', return_value=mock.Mock(status_code=200)))

    # Act
    new_labels = {
        'benchmark': 'test_benchmark_2',
        'metadata': 'test_metadata',
    }
    instance._UpdateLabels(new_labels)

    # Assert
    mock_request.assert_called_once_with(
        'https://spanner.googleapis.com/v1/projects/test_project/instances/pkb-instance-test_uri',
        headers={'Authorization': 'Bearer test_token'},
        json={
            'instance': {
                'labels': {
                    'benchmark': 'test_benchmark_2',
                    'timeout_minutes': '10',
                    'metadata': 'test_metadata'
                }
            },
            'fieldMask': 'labels'
        })

  @parameterized.named_parameters([
      {
          'testcase_name': 'AllRead',
          'write_proportion': 0.0,
          'read_proportion': 1.0,
          'expected_qps': 30000,
      },
      {
          'testcase_name': 'AllWrite',
          'write_proportion': 1.0,
          'read_proportion': 0.0,
          'expected_qps': 6000,
      },
      {
          'testcase_name': 'ReadWrite',
          'write_proportion': 0.5,
          'read_proportion': 0.5,
          'expected_qps': 10000,
      },
  ])
  def testCalculateStartingThroughput(self, write_proportion, read_proportion,
                                      expected_qps):
    # Arrange
    test_spanner = GetTestSpannerInstance()
    test_spanner.nodes = 3

    # Act
    actual_qps = test_spanner.CalculateTheoreticalMaxThroughput(
        read_proportion, write_proportion)

    # Assert
    self.assertEqual(expected_qps, actual_qps)

  @parameterized.named_parameters([
      {
          'testcase_name': 'AllRead',
          'write_proportion': 0.0,
          'read_proportion': 1.0,
          'expected_qps': 45000,
      },
      {
          'testcase_name': 'AllWrite',
          'write_proportion': 1.0,
          'read_proportion': 0.0,
          'expected_qps': 9000,
      },
      {
          'testcase_name': 'ReadWrite',
          'write_proportion': 0.5,
          'read_proportion': 0.5,
          'expected_qps': 15000,
      },
  ])
  def testCalculateAdjustedStartingThroughput(
      self, write_proportion, read_proportion, expected_qps
  ):
    # Arrange
    test_spanner = GetTestSpannerInstance()
    test_spanner._config = 'regional-us-east4'
    test_spanner.nodes = 3

    # Act
    actual_qps = test_spanner.CalculateTheoreticalMaxThroughput(
        read_proportion, write_proportion
    )

    # Assert
    self.assertEqual(expected_qps, actual_qps)


class CreateTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    saved_flag_values = flagsaver.save_flag_values()
    FLAGS.run_uri = 'test_uri'
    self.addCleanup(flagsaver.restore_flag_values, saved_flag_values)

    self.test_instance = GetTestSpannerInstance()
    self.enter_context(mock.patch.object(self.test_instance, '_UpdateLabels'))

  def testCreateOnExistingInstance(self):
    self.assertFalse(self.test_instance.created)
    self.enter_context(
        mock.patch.object(self.test_instance, '_Exists', return_value=True)
    )
    self.enter_context(
        mock.patch.object(
            vm_util,
            'IssueCommand',
            side_effect=[('', 'error', -1), ('', 'success', 0)],
        )
    )

    self.test_instance.Create()

    self.assertTrue(self.test_instance.created)
    self.assertTrue(self.test_instance.user_managed)

  def testCreate(self):
    self.assertFalse(self.test_instance.created)
    self.enter_context(
        mock.patch.object(
            self.test_instance, '_Exists', side_effect=[False, True]
        )
    )
    self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=('', '', 0))
    )

    self.test_instance.Create()

    self.assertTrue(self.test_instance.created)
    self.assertFalse(self.test_instance.user_managed)


if __name__ == '__main__':
  unittest.main()
