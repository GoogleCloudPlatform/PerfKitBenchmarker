# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.aws.aws_elasticache_redis."""
import unittest
from absl import flags
import mock
import os

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_global_accelerator
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

def _ReadTestDataFile(filename):
  path = os.path.join(
      os.path.dirname(__file__), '../../data',
      filename)
  with open(path) as fp:
    return fp.read()

class AwsGlobalAcceleratorTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsGlobalAcceleratorTestCase, self).setUp()
    FLAGS.project = 'project'
    FLAGS.zones = ['us-east-1a', 'us-west-1a']
    FLAGS.cloud_redis_region = 'us-east-1'
    FLAGS.run_uri = 'run12345'
    FLAGS.aws_global_accelerator = True
    mock_spec = mock.Mock()
    mock_vm_1 = mock.Mock()
    mock_vm_2 = mock.Mock()
    mock_vm_1.zone = FLAGS.zones[0]
    mock_vm_2.zone = FLAGS.zones[1]
    mock_spec.vms = [mock_vm_1, mock_vm_2]
    self.global_accelerator = aws_global_accelerator.AwsGlobalAccelerator()

  @mock.patch.object(vm_util, 'IssueCommand')
  def testCreate(self, mock_cmd):
    global_accelerator_create_output = _ReadTestDataFile('aws-create-global-accelerator-output.json')
    mock_cmd.side_effect = [(global_accelerator_create_output, '', 0)]
    self.global_accelerator._Create()
    self.assertEqual(
      self.global_accelerator.accelerator_arn, 
      'arn:aws:globalaccelerator::320929487545:accelerator/4e183541-0a46-4d7d-ad51-6a214fde8841')

  # def testDelete(self):
  #   self.mock_command.return_value = (None, '', None)
  #   expected_output = [
  #       'aws', 'elasticache', 'delete-replication-group', '--region',
  #       'us-east-1', '--replication-group-id', 'pkb-run12345'
  #   ]
  #   self.redis._Delete()
  #   self.mock_command.assert_called_once_with(
  #       expected_output, raise_on_failure=False)

  # def testExistTrue(self):
  #   self.mock_command.return_value = (None, '', None)
  #   expected_output = [
  #       'aws', 'elasticache', 'describe-replication-groups', '--region',
  #       'us-east-1', '--replication-group-id', 'pkb-run12345'
  #   ]
  #   self.redis._Exists()
  #   self.mock_command.assert_called_once_with(
  #       expected_output, raise_on_failure=False)


class AwsGlobalAcceleratorListenerTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsGlobalAcceleratorListenerTestCase, self).setUp()
    FLAGS.project = 'project'
    FLAGS.zones = ['us-east-1a', 'us-west-1a']
    FLAGS.cloud_redis_region = 'us-east-1'
    FLAGS.run_uri = 'run12345'
    FLAGS.aws_global_accelerator = True
    mock_spec = mock.Mock()
    mock_vm_1 = mock.Mock()
    mock_vm_2 = mock.Mock()
    mock_vm_1.zone = FLAGS.zones[0]
    mock_vm_2.zone = FLAGS.zones[1]
    mock_spec.vms = [mock_vm_1, mock_vm_2]
    self.global_accelerator = aws_global_accelerator.AwsGlobalAccelerator()
    self.global_accelerator_listener = aws_global_accelerator.AwsGlobalAcceleratorListener(
      self.global_accelerator,
      'TCP',
      '10',
      '60000')

  @mock.patch.object(vm_util, 'IssueCommand')
  def testCreate(self, mock_cmd):
    global_accelerator_listener_create_output = _ReadTestDataFile('aws-create-global-accelerator-listener-output.json')
    mock_cmd.side_effect = [(global_accelerator_listener_create_output, '', 0)]
    self.global_accelerator_listener._Create()
    self.assertEqual(
      self.global_accelerator_listener.listener_arn,
      'arn:aws:globalaccelerator::320929487545:accelerator/4e183541-0a46-4d7d-ad51-6a214fde8841/listener/9b984339')


class AwsEndpointGroupTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsEndpointGroupTestCase, self).setUp()
    FLAGS.project = 'project'
    FLAGS.run_uri = 'run12345'
    FLAGS.aws_global_accelerator = True
    self.region = 'us-west-2'
    self.global_accelerator = aws_global_accelerator.AwsGlobalAccelerator()
    self.global_accelerator_listener = aws_global_accelerator.AwsGlobalAcceleratorListener(
      self.global_accelerator,
      'TCP',
      '10',
      '60000')
    self.endpoint_group = aws_global_accelerator.AwsEndpointGroup(self.global_accelerator_listener, self.region)

  @mock.patch.object(vm_util, 'IssueCommand')
  def testCreate(self, mock_cmd):
    endpoint_group_create_output = _ReadTestDataFile('aws-create-endpoint-group-output.json')
    mock_cmd.side_effect = [(endpoint_group_create_output, '', 0)]
    self.endpoint_group._Create()
    self.assertEqual(
      self.endpoint_group.endpoint_group_arn,
      'arn:aws:globalaccelerator::320929487545:accelerator/4e183541-0a46-4d7d-ad51-6a214fde8841/listener/9b984339/endpoint-group/667ed7678002')
+
  @mock.patch.object(vm_util, 'IssueCommand')
  def testExists(self, mock_cmd):
    endpoint_group_exists_output = _ReadTestDataFile('aws-endpoint-group-exists-output.json')
    mock_cmd.side_effect = [(endpoint_group_exists_output, '', 0)]
    response = self.endpoint_group._Exists()
    self.assertEqual(reponse,True)

  @mock.patch.object(vm_util, 'IssueCommand')
  def testNotExists(self, mock_cmd):
    mock_cmd.side_effect = [('', '', 255)]
    response = self.endpoint_group._Exists()
    self.assertEqual(reponse,False)


if __name__ == '__main__':
  unittest.main()

