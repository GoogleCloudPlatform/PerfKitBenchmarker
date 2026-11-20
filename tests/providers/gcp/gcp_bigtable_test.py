# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for perfkitbenchmarker.providers.gcp.gcp_bigtable."""

import inspect
import json
import os
import unittest
from unittest import mock

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import gcp_bigtable
from perfkitbenchmarker.providers.gcp import util
from tests import matchers
from tests import pkb_common_test_case
import requests

FLAGS = flags.FLAGS

NAME = 'test_name'
PROJECT = 'test_project'
ZONE = 'test_zone'

VALID_JSON_BASE = """
    {{
      "displayName": "{name}",
      "name": "projects/{project}/instances/{name}",
      "state": "READY"
    }}
"""

_TEST_LIST_CLUSTERS_OUTPUT = [
    {
        'defaultStorageType': 'SSD',
        'location': 'projects/pkb-test/locations/us-west1-a',
        'name': 'projects/pkb-test/instances/pkb-bigtable-730b1e6b/clusters/pkb-bigtable-730b1e6b-1',
        'serveNodes': 3,
        'state': 'READY',
    },
    {
        'defaultStorageType': 'SSD',
        'location': 'projects/pkb-test/locations/us-west1-c',
        'name': 'projects/pkb-test/instances/pkb-bigtable-730b1e6b/clusters/pkb-bigtable-730b1e6b-0',
        'serveNodes': 3,
        'state': 'READY',
    },
]


OUT_OF_QUOTA_STDERR = """
ERROR: (gcloud.beta.bigtable.instances.create) Operation successfully rolled
back : Insufficient node quota. You requested a node count of 1 nodes for your
cluster, but this request would exceed your project's node quota of 30 nodes
total across all clusters in this zone. Contact us to request a
quota increase: https://cloud.google.com/bigtable/quotas#quota-increase
"""

_TEST_BENCHMARK_SPEC = f"""
cloud_bigtable_ycsb:
  non_relational_db:
    service_type: bigtable
    enable_freeze_restore: True
    name: {NAME}
    zone: {ZONE}
    project: {PROJECT}
"""

_TEST_BENCHMARK_SPEC_MINIMAL = """
cloud_bigtable_ycsb:
  non_relational_db:
    service_type: bigtable
"""

_TEST_BENCHMARK_SPEC_ALL_ATTRS = f"""
cloud_bigtable_ycsb:
  non_relational_db:
    service_type: bigtable
    enable_freeze_restore: True
    name: {NAME}
    zone: {ZONE}
    project: {PROJECT}
    node_count: 10
    storage_type: hdd
    replication_cluster: True
    replication_cluster_zone: test_zone
    multicluster_routing: True
    autoscaling_min_nodes: 5
    autoscaling_max_nodes: 15
    autoscaling_cpu_target: 99
"""


def GetTestBigtableInstance(spec=_TEST_BENCHMARK_SPEC):
  test_benchmark_spec = pkb_common_test_case.CreateBenchmarkSpecFromYaml(
      yaml_string=spec, benchmark_name='cloud_bigtable_ycsb'
  )
  test_benchmark_spec.ConstructNonRelationalDb()
  instance = test_benchmark_spec.non_relational_db
  if instance is None:
    raise ValueError(
        'BenchmarkSpec.ConstructNonRelationalDb() failed to create a '
        'non_relational_db instance for testing, tests cannot proceed.'
    )
  return instance


class GcpBigtableTestCase(pkb_common_test_case.PkbCommonTestCase):
  bigtable: gcp_bigtable.GcpBigtableInstance

  def setUp(self):
    super().setUp()
    self.bigtable = GetTestBigtableInstance()

  def testNotFoundTable(self):
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=('', '', 1)
    ):
      self.assertFalse(self.bigtable._Exists())

  def testFoundTable(self):
    stdout = VALID_JSON_BASE.format(project=PROJECT, name=NAME)
    with mock.patch.object(
        util.GcloudCommand, 'Issue', return_value=(stdout, '', 0)
    ):
      self.assertTrue(self.bigtable._Exists())

  def testQuotaError(self):
    self.enter_context(
        mock.patch.object(
            util.GcloudCommand,
            'Issue',
            return_value=[None, OUT_OF_QUOTA_STDERR, None],
        )
    )
    with self.assertRaises(errors.Benchmarks.QuotaFailure):
      self.bigtable._Create()

  @flagsaver.flagsaver
  def testInitFromSpec(self):
    # Arrange and Act
    instance = GetTestBigtableInstance(spec=_TEST_BENCHMARK_SPEC_ALL_ATTRS)

    # Assert
    self.assertEqual(instance.name, NAME)
    self.assertEqual(instance.project, PROJECT)
    self.assertEqual(instance.zone, ZONE)
    self.assertEqual(instance.node_count, 10)
    self.assertEqual(instance.storage_type, 'hdd')
    self.assertEqual(instance.replication_cluster, True)
    self.assertEqual(instance.replication_cluster_zone, 'test_zone')
    self.assertEqual(instance.multicluster_routing, True)
    self.assertEqual(instance.autoscaling_min_nodes, 5)
    self.assertEqual(instance.autoscaling_max_nodes, 15)
    self.assertEqual(instance.autoscaling_cpu_target, 99)
    self.assertTrue(instance.user_managed)

  @flagsaver.flagsaver
  def testMulticlusterRoutingRequiresReplicationCluster(self):
    FLAGS['bigtable_multicluster_routing'].parse(True)
    with self.assertRaises(Exception):
      GetTestBigtableInstance(spec=_TEST_BENCHMARK_SPEC_MINIMAL)

  @flagsaver.flagsaver
  def testGetResourceMetadataUserManaged(self):
    # Arrange
    FLAGS['google_bigtable_instance_name'].parse('test_name')
    instance = GetTestBigtableInstance(spec=_TEST_BENCHMARK_SPEC_MINIMAL)
    mock_get_cluster_output = [
        {
            'zone': 'test_zone1',
            'defaultStorageType': 'test_storage_type1',
            'serveNodes': 'test_serve_nodes1',
        },
        {
            'zone': 'test_zone2',
            'defaultStorageType': 'test_storage_type2',
            'serveNodes': 'test_serve_nodes2',
        },
    ]
    self.enter_context(
        mock.patch.object(
            gcp_bigtable,
            'GetClustersDescription',
            return_value=mock_get_cluster_output,
        )
    )

    # Act
    actual_metadata = instance.GetResourceMetadata()

    # Assert
    expected_metadata = {
        'bigtable_zone': ['test_zone1', 'test_zone2'],
        'bigtable_storage_type': ['test_storage_type1', 'test_storage_type2'],
        'bigtable_node_count': ['test_serve_nodes1', 'test_serve_nodes2'],
    }
    self.assertEqual(actual_metadata, expected_metadata)

  def testGetResourceMetadata(self):
    FLAGS['google_bigtable_zone'].parse('parsed_zone')
    FLAGS['bigtable_replication_cluster'].parse(True)
    FLAGS['bigtable_replication_cluster_zone'].parse('parsed_rep_zone')
    FLAGS['bigtable_multicluster_routing'].parse(True)
    FLAGS['bigtable_row_affinity'].parse(True)

    instance = GetTestBigtableInstance(spec=_TEST_BENCHMARK_SPEC_MINIMAL)
    actual_metadata = instance.GetResourceMetadata()

    expected_metadata = {
        'bigtable_zone': 'parsed_zone',
        'bigtable_replication_zone': 'parsed_rep_zone',
        'bigtable_storage_type': 'ssd',
        'bigtable_node_count': 3,
        'bigtable_multicluster_routing': True,
        'bigtable_row_affinity': True,
    }
    self.assertEqual(actual_metadata, expected_metadata)

  def testBuildClusterConfigsDefault(self):
    # Arrange and Act
    actual_flag_values = self.bigtable._BuildClusterConfigs()

    # Assert
    expected_flag_values = ['id=test_name-0,zone=test_zone,nodes=3']
    self.assertEqual(actual_flag_values, expected_flag_values)

  @flagsaver.flagsaver
  def testBuildClusterConfigsWithFlags(self):
    # Arrange
    FLAGS['bigtable_node_count'].parse(10)
    FLAGS['bigtable_autoscaling_min_nodes'].parse(1)
    FLAGS['bigtable_autoscaling_max_nodes'].parse(5)
    FLAGS['bigtable_autoscaling_cpu_target'].parse(50)
    FLAGS['bigtable_replication_cluster_zone'].parse('test_replication_zone')
    FLAGS['bigtable_replication_cluster'].parse(True)
    bigtable = GetTestBigtableInstance()

    # Act
    actual_flag_values = bigtable._BuildClusterConfigs()

    # Assert
    expected_flag_values = [
        (
            'id=test_name-0,zone=test_zone,'
            'autoscaling-min-nodes=1,autoscaling-max-nodes=5,'
            'autoscaling-cpu-target=50'
        ),
        (
            'id=test_name-1,zone=test_replication_zone,'
            'autoscaling-min-nodes=1,autoscaling-max-nodes=5,'
            'autoscaling-cpu-target=50'
        ),
    ]
    self.assertEqual(actual_flag_values, expected_flag_values)

  @flagsaver.flagsaver(run_uri='test_uri')
  def testUpdateLabels(self):
    # Arrange
    instance = GetTestBigtableInstance()
    mock_json_response = inspect.cleandoc("""
    {
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
            return_value=(mock_json_response, '', 0),
        )
    )
    self.enter_context(
        mock.patch.object(util, 'GetAccessToken', return_value='test_token')
    )
    mock_request = self.enter_context(
        mock.patch.object(
            requests, 'patch', return_value=mock.Mock(status_code=200)
        )
    )

    # Act
    new_labels = {
        'benchmark': 'test_benchmark_2',
        'metadata': 'test_metadata',
    }
    instance._UpdateLabels(new_labels)

    # Assert
    mock_request.assert_called_once_with(
        'https://bigtableadmin.googleapis.com/v2/projects/test_project'
        '/instances/test_name',
        headers={'Authorization': 'Bearer test_token'},
        params={
            'updateMask': 'labels',
        },
        json={
            'labels': {
                'benchmark': 'test_benchmark_2',
                'timeout_minutes': '10',
                'metadata': 'test_metadata',
            }
        },
    )

  def testBigtableGcloudCommand(self):
    bigtable = GetTestBigtableInstance()

    cmd = gcp_bigtable._GetBigtableGcloudCommand(bigtable, 'instances', 'test')

    # Command does not have the zone flag but has the project flag.
    self.assertEqual(cmd.args, ['instances', 'test'])
    self.assertEqual(cmd.flags['project'], PROJECT)
    self.assertEmpty(cmd.flags['zone'])

  def testSetNodes(self):
    test_instance = GetTestBigtableInstance()
    test_instance.user_managed = False
    self.enter_context(
        mock.patch.object(
            test_instance,
            '_GetClusters',
            return_value=_TEST_LIST_CLUSTERS_OUTPUT,
        )
    )
    self.enter_context(
        mock.patch.object(test_instance, '_Exists', return_value=True)
    )
    cmd = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=[None, None, 0])
    )

    test_instance._UpdateNodes(6)

    self.assertSequenceEqual(
        [
            mock.call(
                matchers.HASALLOF('--num-nodes', '6'), stack_level=mock.ANY
            ),
            mock.call(
                matchers.HASALLOF('--num-nodes', '6'), stack_level=mock.ANY
            ),
        ],
        cmd.mock_calls,
    )

  def testSetNodesSkipsIfCountAlreadyCorrect(self):
    test_instance = GetTestBigtableInstance()
    self.enter_context(
        mock.patch.object(
            test_instance,
            '_GetClusters',
            return_value=_TEST_LIST_CLUSTERS_OUTPUT,
        )
    )
    self.enter_context(
        mock.patch.object(test_instance, '_Exists', return_value=True)
    )
    cmd = self.enter_context(
        mock.patch.object(vm_util, 'IssueCommand', return_value=[None, None, 0])
    )

    test_instance._UpdateNodes(3)

    cmd.assert_not_called()

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
          'expected_qps': 30000,
      },
      {
          'testcase_name': 'ReadWrite',
          'write_proportion': 0.5,
          'read_proportion': 0.5,
          'expected_qps': 30000,
      },
  ])
  def testCalculateStartingThroughput(
      self, write_proportion, read_proportion, expected_qps
  ):
    # Arrange
    test_bigtable = GetTestBigtableInstance()
    test_bigtable.nodes = 3

    # Act
    actual_qps = test_bigtable.CalculateTheoreticalMaxThroughput(
        read_proportion, write_proportion
    )

    # Assert
    self.assertEqual(expected_qps, actual_qps)

  @flagsaver.flagsaver
  def testGetClustersDescriptionDefaultEndpointDoesNotOverrideEnv(self):
    mock_issue = self.enter_context(
        mock.patch.object(
            vm_util,
            'IssueCommand',
            return_value=('[]', '', 0),
            autospec=True,
            spec_set=True,
        )
    )

    clusters = gcp_bigtable.GetClustersDescription(NAME, PROJECT)

    self.assertEmpty(clusters)
    _, issue_kwargs = mock_issue.call_args
    self.assertNotIn('env', issue_kwargs)

  @parameterized.named_parameters(
      dict(
          testcase_name='BareHostname',
          admin_endpoint='test-bigtableadmin.googleapis.com',
          expected_url='https://test-bigtableadmin.googleapis.com/',
      ),
      dict(
          testcase_name='HttpsWithoutTrailingSlash',
          admin_endpoint='https://test-bigtableadmin.googleapis.com',
          expected_url='https://test-bigtableadmin.googleapis.com/',
      ),
      dict(
          testcase_name='HttpsWithTrailingSlash',
          admin_endpoint='https://test-bigtableadmin.googleapis.com/',
          expected_url='https://test-bigtableadmin.googleapis.com/',
      ),
      dict(
          testcase_name='HttpLocalhost',
          admin_endpoint='http://localhost:8086',
          expected_url='http://localhost:8086/',
      ),
  )
  def testGetClustersDescriptionPropagatesNormalizedAdminEndpoint(
      self, admin_endpoint, expected_url
  ):
    clusters_output = [
        {
            'defaultStorageType': 'SSD',
            'location': f'projects/{PROJECT}/locations/us-west1-a',
            'name': (
                f'projects/{PROJECT}/instances/{NAME}/clusters/{NAME}-0'
            ),
            'serveNodes': 3,
            'state': 'READY',
        },
        {
            'defaultStorageType': 'SSD',
            'location': f'projects/{PROJECT}/locations/us-west1-b',
            'name': (
                f'projects/{PROJECT}/instances/other_instance/'
                'clusters/other_instance-0'
            ),
            'serveNodes': 5,
            'state': 'READY',
        },
    ]
    self.enter_context(
        mock.patch.dict(
            os.environ, {'EXISTING_TEST_ENV': 'existing_value'}, clear=False
        )
    )
    mock_issue = self.enter_context(
        mock.patch.object(
            vm_util,
            'IssueCommand',
            return_value=(json.dumps(clusters_output), '', 0),
            autospec=True,
            spec_set=True,
        )
    )

    with flagsaver.as_parsed(google_bigtable_admin_endpoint=admin_endpoint):
      clusters = gcp_bigtable.GetClustersDescription(NAME, PROJECT)

    self.assertNotIn(
        'CLOUDSDK_API_ENDPOINT_OVERRIDES_BIGTABLEADMIN', os.environ
    )
    self.assertEqual(
        clusters,
        [{
            'defaultStorageType': 'SSD',
            'location': f'projects/{PROJECT}/locations/us-west1-a',
            'name': f'{NAME}-0',
            'serveNodes': 3,
            'state': 'READY',
            'zone': 'us-west1-a',
        }],
    )
    cmd_args, issue_kwargs = mock_issue.call_args
    self.assertContainsSubsequence(
        cmd_args[0], ['bigtable', 'clusters', 'list']
    )
    self.assertNotIn('--zone', cmd_args[0])
    self.assertEqual(
        issue_kwargs['env']['CLOUDSDK_API_ENDPOINT_OVERRIDES_BIGTABLEADMIN'],
        expected_url,
    )
    self.assertEqual(
        issue_kwargs['env']['EXISTING_TEST_ENV'], 'existing_value'
    )

  @flagsaver.as_parsed(
      run_uri='test_uri',
      google_bigtable_zone=ZONE,
      project=PROJECT,
      google_bigtable_admin_endpoint='test-bigtableadmin.googleapis.com',
  )
  def testCreateUpdatesLabelsUsingCustomAdminEndpoint(self):
    expected_name = 'pkb-bigtable-test_uri'
    instance = GetTestBigtableInstance(spec=_TEST_BENCHMARK_SPEC_MINIMAL)
    ready_json = VALID_JSON_BASE.format(project=PROJECT, name=expected_name)
    mock_issue = self.enter_context(
        mock.patch.object(
            vm_util,
            'IssueCommand',
            side_effect=[
                ('', '', 0),
                ('{"labels": {}}', '', 0),
                (ready_json, '', 0),
            ],
            autospec=True,
            spec_set=True,
        )
    )
    self.enter_context(
        mock.patch.object(
            util,
            'GetDefaultTags',
            return_value={'benchmark': 'test_benchmark'},
            autospec=True,
            spec_set=True,
        )
    )
    self.enter_context(
        mock.patch.object(
            util,
            'GetAccessToken',
            return_value='test_token',
            autospec=True,
            spec_set=True,
        )
    )
    response = requests.Response()
    response.status_code = 200
    mock_request = self.enter_context(
        mock.patch.object(
            requests,
            'patch',
            return_value=response,
            autospec=True,
            spec_set=True,
        )
    )

    instance.Create()

    self.assertLen(mock_issue.call_args_list, 3)
    for _, call_kwargs in mock_issue.call_args_list:
      self.assertEqual(
          call_kwargs['env']['CLOUDSDK_API_ENDPOINT_OVERRIDES_BIGTABLEADMIN'],
          'https://test-bigtableadmin.googleapis.com/',
      )
    mock_request.assert_called_once_with(
        f'https://test-bigtableadmin.googleapis.com/v2/projects/{PROJECT}'
        f'/instances/{expected_name}',
        headers={'Authorization': 'Bearer test_token'},
        params={'updateMask': 'labels'},
        json={'labels': {'benchmark': 'test_benchmark'}},
    )


if __name__ == '__main__':
  unittest.main()
