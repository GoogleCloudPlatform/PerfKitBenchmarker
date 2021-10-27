"""Tests for google3.third_party.py.perfkitbenchmarker.tests.providers.gcp.gce_placement_group."""

import json
import unittest
from absl import flags
import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import errors
from perfkitbenchmarker import placement_group
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import gce_placement_group
from tests import pkb_common_test_case

_PROJECT = 'myproject'
_ZONE = 'us-east1-b'
_REGION = 'us-east1'
_RUN_URI = 'be67a2dd-e312-496d-864a-1d5bc1857dec'
_PLACEMENT_GROUP_NAME = 'perfkit-{}'.format(_RUN_URI)
_STRATEGY = placement_group.PLACEMENT_GROUP_CLUSTER

# The GcePlacementGroup._Create()
_CREATE_RESPONSE = (json.dumps({
    'creationTimestamp': '2020-03-16T15:31:23.802-07:00',
    'description': 'PKB: test',
    'groupPlacementPolicy': {
        'collocation': 'COLLOCATED',
        'vmCount': 2
    },
    'id': '123',
    'kind': 'compute#resourcePolicy',
    'name': 'perfkit-test',
    'region': 'https://www.googleapis.com/test',
    'selfLink': 'https://www.googleapis.com/test',
    'status': 'READY'
}), '', 0)

_QUOTA_FAILURE_RESPONSE = (
    '',
    'ERROR: (gcloud.alpha.compute.resource-policies.create.group-placement) '
    'Could not fetch resource: - Quota \'RESOURCE_POLICIES\' exceeded.  Limit: '
    '10.0 in region europe-west4.', 1)
# The GcePlacementGroup._Exists() response done after a Create() call.
_EXISTS_RESPONSE = json.dumps({'status': 'ACTIVE'}), '', 0
# The GcePlacementGroup._Exists() response done after a Delete() call.
_DELETE_RESPONSE = json.dumps({}), '', 1

FLAGS = flags.FLAGS


def _BenchmarkSpec(num_vms=2, benchmark='cluster_boot'):
  # Creates a fake BenchmarkSpec() that will be the response for calls to
  # perfkitbenchmarker.context.GetThreadBenchmarkSpec().
  config_spec = benchmark_config_spec.BenchmarkConfigSpec(
      benchmark, flag_values=FLAGS)
  config_spec.vm_groups = {'x': mock.Mock(vm_count=num_vms, static_vms=[])}
  bm_module = mock.MagicMock(BENCHMARK_NAME=benchmark)
  bm_spec = benchmark_spec.BenchmarkSpec(bm_module, config_spec, 'uid')
  bm_spec.uuid = _RUN_URI


def _CreateGcePlacementGroupSpec(group_style=_STRATEGY):
  FLAGS.placement_group_style = group_style
  return gce_placement_group.GcePlacementGroupSpec(
      'GcePlacementGroupSpec',
      zone=_ZONE,
      project=_PROJECT,
      num_vms=2,
      flag_values=FLAGS)


def _CreateGcePlacementGroup(group_style=_STRATEGY):
  return gce_placement_group.GcePlacementGroup(
      _CreateGcePlacementGroupSpec(group_style))


class GcePlacementGroupTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GcePlacementGroupTest, self).setUp()
    self.mock_cmd = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand'))
    # Register a fake benchmark
    _BenchmarkSpec(2)

  def testPlacementGroupCreate(self):
    self.mock_cmd.return_value = _CREATE_RESPONSE
    _CreateGcePlacementGroup()._Create()
    self.mock_cmd.assert_called_with([
        'gcloud', 'compute', 'resource-policies', 'create',
        'group-placement', _PLACEMENT_GROUP_NAME, '--collocation', 'COLLOCATED',
        '--format', 'json',
        '--project', 'myproject', '--quiet', '--region', 'us-east1',
        '--vm-count', '2'
    ], raise_on_failure=False)

  def testPlacementGroupDelete(self):
    self.mock_cmd.side_effect = [_EXISTS_RESPONSE, _DELETE_RESPONSE]
    pg = _CreateGcePlacementGroup()
    pg.created = True
    pg.Delete()
    self.mock_cmd.assert_called_with([
        'gcloud', 'compute', 'resource-policies', 'describe',
        _PLACEMENT_GROUP_NAME, '--format', 'json', '--project', 'myproject',
        '--quiet', '--region', 'us-east1'
    ], raise_on_failure=False)

  def testPlacementGroupQuotaFailure(self):
    self.mock_cmd.return_value = _QUOTA_FAILURE_RESPONSE
    with self.assertRaises(errors.Benchmarks.QuotaFailure):
      _CreateGcePlacementGroup()._Create()


if __name__ == '__main__':
  unittest.main()
