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
"""Tests for perfkitbenchmarker.providers.gcp.gcp_tpu."""

import contextlib
import unittest
import mock

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import gcp_tpu
from perfkitbenchmarker.providers.gcp import util
from tests import mock_flags


NAME = 'testname'
PROJECT = 'testproject'
ZONE = 'testzone'


class GcpTpuTestCase(unittest.TestCase):

  def CreateTpuSpecDict(self):
    return {
        'tpu_name': 'pkb-tpu-123',
        'tpu_cidr_range': '192.168.0.0/29',
        'tpu_accelerator_type': 'tpu-v2',
        'tpu_description': 'MyTFNode',
        'tpu_network': 'default',
        'tpu_tf_version': 'nightly',
        'tpu_zone': 'us-central1-a',
        'tpu_preemptible': True
    }

  def CreateTpuFromSpec(self, spec_dict):
    mock_tpu_spec = mock.Mock(
        spec=benchmark_config_spec._TpuGroupSpec)
    mock_tpu_spec.configure_mock(**spec_dict)
    tpu_class = gcp_tpu.GcpTpu(mock_tpu_spec)
    return tpu_class

  def setUp(self):
    self.flags = mock_flags.PatchTestCaseFlags(self)
    self.flags.run_uri = '123'
    self.flags.project = ''
    self.flags.tpu_cores_per_donut = 8
    self.flags.gcloud_path = 'gcloud'

    mock_tpu_spec_attrs = self.CreateTpuSpecDict()
    self.mock_tpu_spec = mock.Mock(
        spec=benchmark_config_spec._TpuGroupSpec)
    self.mock_tpu_spec.configure_mock(**mock_tpu_spec_attrs)

  @contextlib.contextmanager
  def _PatchCriticalObjects(self, stdout='', stderr='', return_code=0):
    """A context manager that patches a few critical objects with mocks."""
    retval = (stdout, stderr, return_code)
    with mock.patch(
        vm_util.__name__ + '.IssueCommand',
        return_value=retval) as issue_command, mock.patch(
            '__builtin__.open'), mock.patch(
                vm_util.__name__ + '.NamedTemporaryFile'), mock.patch(
                    util.__name__ + '.GetDefaultProject',
                    return_value='fakeproject'):
      yield issue_command

  def testCreate(self):
    with self._PatchCriticalObjects() as issue_command:
      tpu = gcp_tpu.GcpTpu(self.mock_tpu_spec)
      tpu._Create()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])
      self.assertTrue(
          command_string.startswith(
              'gcloud compute tpus create pkb-tpu-123'),
          command_string)
      self.assertIn('--project fakeproject', command_string)
      self.assertIn('--range 192.168.0.0/29', command_string)
      self.assertIn('--accelerator-type tpu-v2', command_string)
      self.assertIn('--description MyTFNode', command_string)
      self.assertIn('--network default', command_string)
      self.assertIn('--version nightly', command_string)
      self.assertIn('--zone us-central1-a', command_string)
      self.assertIn('--preemptible', command_string)

  def testDelete(self):
    with self._PatchCriticalObjects() as issue_command:
      tpu = gcp_tpu.GcpTpu(self.mock_tpu_spec)
      tpu._Delete()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])
      self.assertTrue(
          command_string.startswith(
              'gcloud compute tpus delete pkb-tpu-123'))
      self.assertIn('--project fakeproject', command_string)
      self.assertIn('--zone us-central1-a', command_string)

  def testExists(self):
    with self._PatchCriticalObjects() as issue_command:
      tpu = gcp_tpu.GcpTpu(self.mock_tpu_spec)
      tpu._Exists()
      self.assertEqual(issue_command.call_count, 1)
      command_string = ' '.join(issue_command.call_args[0][0])
      self.assertTrue(
          command_string.startswith(
              'gcloud compute tpus describe pkb-tpu-123'))
      self.assertIn('--project fakeproject', command_string)
      self.assertIn('--zone us-central1-a', command_string)

  def testGetName(self):
    with self._PatchCriticalObjects():
      tpu = gcp_tpu.GcpTpu(self.mock_tpu_spec)
      name = tpu.GetName()
      self.assertEqual(name, 'pkb-tpu-123')

  def testGetNumShards(self):
    with self._PatchCriticalObjects(stdout='{"networkEndpoints": [{"ipAddress":'
                                    ' "10.199.12.2", "port": 8470}]}'):
      tpu = gcp_tpu.GcpTpu(self.mock_tpu_spec)
      num_shards = tpu.GetNumShards()
      self.assertEqual(num_shards, 8)

  def testGetMasterGrpcAddress(self):
    with self._PatchCriticalObjects(stdout="""{
  "networkEndpoints": [{
    "ipAddress": "10.199.12.2",
    "port": 8470
  }]
}
    """):
      tpu = gcp_tpu.GcpTpu(self.mock_tpu_spec)
      ip_address = tpu.GetMasterGrpcAddress()
      self.assertEqual(ip_address, 'grpc://10.199.12.2:8470')


if __name__ == '__main__':
  unittest.main()
