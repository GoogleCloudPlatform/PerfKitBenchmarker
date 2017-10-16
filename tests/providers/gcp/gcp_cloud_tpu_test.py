# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.gcp.gcp_cloud_tpu."""

import contextlib
import json
import unittest
import mock

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import gcp_cloud_tpu
from perfkitbenchmarker.providers.gcp import util


NAME = 'testname'
PROJECT = 'testproject'
ZONE = 'testzone'


class GcpTpuTestCase(unittest.TestCase):

  def CreateCloudTpuSpecDict(self):
    return {
        'tpu_name': 'pkb-tpu-123',
        'tpu_cidr_range': '192.168.0.0/29',
        'tpu_accelerator_type': 'tpu-v2',
        'tpu_description': 'MyTFNode',
        'tpu_network': 'default',
        'tpu_tf_version': 'nightly',
        'tpu_zone': 'us-central1-a'
    }

  def CreateCloudTpuFromSpec(self, spec_dict):
    mock_tpu_spec = mock.Mock(
        spec=benchmark_config_spec._CloudTpuSpec)
    mock_tpu_spec.configure_mock(**spec_dict)
    tpu_class = gcp_cloud_tpu.GcpCloudTpu(mock_tpu_spec)
    return tpu_class

  def setUp(self):
    flag_values = {'run_uri': '123', 'project': None}

    p = mock.patch(gcp_cloud_tpu.__name__ + '.FLAGS')
    flags_mock = p.start()
    flags_mock.configure_mock(**flag_values)
    self.addCleanup(p.stop)
    mock_tpu_spec_attrs = self.CreateCloudTpuSpecDict()
    self.mock_tpu_spec = mock.Mock(
        spec=benchmark_config_spec._CloudTpuSpec)
    self.mock_tpu_spec.configure_mock(**mock_tpu_spec_attrs)

  @contextlib.contextmanager
  def _PatchCriticalObjects(self, stdout='', stderr='', return_code=0):
    """A context manager that patches a few critical objects with mocks."""
    retval = (stdout, stderr, return_code)
    with mock.patch(vm_util.__name__ + '.IssueCommand',
                    return_value=retval) as issue_command, \
            mock.patch('__builtin__.open'), \
            mock.patch(vm_util.__name__ + '.NamedTemporaryFile'), \
            mock.patch(util.__name__ + '.GetDefaultProject',
                       return_value='fakeproject'):
      yield issue_command

  def testCreate(self):
    with self._PatchCriticalObjects() as issue_command:
      tpu = gcp_cloud_tpu.GcpCloudTpu(self.mock_tpu_spec)
      tpu._Create()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])
      self.assertTrue(
          command_string.startswith(
              'gcloud alpha compute tpus create pkb-tpu-123'),
          command_string)
      self.assertIn('--project fakeproject', command_string)
      self.assertIn('--range 192.168.0.0/29', command_string)
      self.assertIn('--accelerator-type tpu-v2', command_string)
      self.assertIn('--description MyTFNode', command_string)
      self.assertIn('--network default', command_string)
      self.assertIn('--version nightly', command_string)
      self.assertIn('--zone us-central1-a', command_string)

  def testDelete(self):
    with self._PatchCriticalObjects() as issue_command:
      tpu = gcp_cloud_tpu.GcpCloudTpu(self.mock_tpu_spec)
      tpu._Delete()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])
      self.assertTrue(
          command_string.startswith(
              'gcloud alpha compute tpus delete pkb-tpu-123'))
      self.assertIn('--project fakeproject', command_string)
      self.assertIn('--zone us-central1-a', command_string)

  def testExists(self):
    with self._PatchCriticalObjects() as issue_command:
      tpu = gcp_cloud_tpu.GcpCloudTpu(self.mock_tpu_spec)
      tpu._Exists()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])
      self.assertTrue(
          command_string.startswith(
              'gcloud alpha compute tpus describe pkb-tpu-123'))
      self.assertIn('--project fakeproject', command_string)
      self.assertIn('--zone us-central1-a', command_string)

  def testGetCloudTpuIp(self):
    with self._PatchCriticalObjects(stdout=json.dumps({
        '@type': 'type.googleapis.com/google.cloud.tpu.v1alpha1.Node',
        'acceleratorType': 'zones/us-central1-a/acceleratorTypes/tpu-v2',
        'cidrBlock': '10.240.0.0/29',
        'createTime': '2017-10-02T20:48:54.421627Z',
        'greenVmInstanceId': '4423519461898655909',
        'greenVmSelflink': ('https://www.googleapis.com/compute/alpha/projects/'
                            'g9250ab742a5cc428-tp/zones/us-central1-a/instances'
                            '/n-74e6bf72-w-0'),
        'ipAddress': '196.168.0.2',
        'machineType': 'zones/us-central1-a/machineTypes/custom-64-425984',
        'name': ('projects/fakeproject/locations/us-central1-a/nodes/'
                 'pkb-tpu-123'),
        'network': 'global/networks/default',
        'port': '8470',
        'serviceAccount': '34313586767-compute@developer.gserviceaccount.com',
        'tensorflowVersion': 'nightly'
    })) as issue_command:
      tpu = gcp_cloud_tpu.GcpCloudTpu(self.mock_tpu_spec)
      tpu_ip = tpu.GetCloudTpuIp()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])
      self.assertTrue(
          command_string.startswith(
              'gcloud alpha compute tpus describe pkb-tpu-123'))
      self.assertIn('--project fakeproject', command_string)
      self.assertIn('--zone us-central1-a', command_string)
      self.assertEqual(tpu_ip, '196.168.0.2')

if __name__ == '__main__':
  unittest.main()
