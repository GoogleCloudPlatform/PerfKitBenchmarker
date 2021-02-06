"""Tests for google3.third_party.py.perfkitbenchmarker.providers.gcp.gcp_spanner."""

import unittest

from absl import flags
from absl.testing import flagsaver
import mock
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_spanner
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


def GetTestSpannerInstance():
  return gcp_spanner.GcpSpannerInstance(
      name='test_instance', database='test_database')


class SpannerTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    pass

  @flagsaver.flagsaver
  def testInitFromSpec(self):
    FLAGS.zone = ['us-east1-a']
    spec_args = {
        'service_type': gcp_spanner.DEFAULT_SPANNER_TYPE,
        'name': 'test_instance',
        'description': 'test_description',
        'database': 'test_database',
        'ddl': 'test_schema',
        'nodes': 2,
        'project': 'test_project',
    }
    test_spec = gcp_spanner.SpannerSpec('test_component', None, **spec_args)

    spanner = gcp_spanner.GcpSpannerInstance.FromSpec(test_spec)

    self.assertEqual(spanner.name, 'test_instance')
    self.assertEqual(spanner._description, 'test_description')
    self.assertEqual(spanner.database, 'test_database')
    self.assertEqual(spanner._ddl, 'test_schema')
    self.assertEqual(spanner._nodes, 2)
    self.assertEqual(spanner.project, 'test_project')
    self.assertEqual(spanner._config, 'regional-us-east1')

  def testSetNodes(self):
    test_instance = GetTestSpannerInstance()
    # Don't actually issue a command.
    cmd = self.enter_context(
        mock.patch.object(
            vm_util, 'IssueCommand', return_value=[None, None, 0]))

    test_instance._SetNodes(3)

    self.assertIn('--nodes 3', ' '.join(cmd.call_args[0][0]))

  def testFreezeUsesCorrectNodeCount(self):
    instance = GetTestSpannerInstance()
    mock_set_nodes = self.enter_context(
        mock.patch.object(instance, '_SetNodes', autospec=True))

    instance._Freeze()

    mock_set_nodes.assert_called_once_with(gcp_spanner._FROZEN_NODE_COUNT)

  def testRestoreUsesCorrectNodeCount(self):
    instance = GetTestSpannerInstance()
    instance._nodes = 5
    mock_set_nodes = self.enter_context(
        mock.patch.object(instance, '_SetNodes', autospec=True))

    instance._Restore()

    mock_set_nodes.assert_called_once_with(5)

if __name__ == '__main__':
  unittest.main()
