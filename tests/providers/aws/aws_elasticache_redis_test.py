# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_elasticache_redis
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class AwsElasticacheRedisTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(AwsElasticacheRedisTestCase, self).setUp()
    FLAGS.project = 'project'
    FLAGS.zones = ['us-east-1a']
    FLAGS.cloud_redis_region = 'us-east-1'
    FLAGS.run_uri = 'run12345'
    mock_spec = mock.Mock()
    mock_spec.config.cloud_redis.redis_version = 'redis_4_0'
    mock_vm = mock.Mock()
    mock_vm.zone = FLAGS.zones[0]
    mock_spec.vms = [mock_vm]
    self.redis = aws_elasticache_redis.ElastiCacheRedis(mock_spec)
    self.mock_command = mock.patch.object(vm_util, 'IssueCommand').start()

  def testCreate(self):
    self.mock_command.return_value = (None, '', None)
    expected_output = [
        'aws', 'elasticache', 'create-replication-group', '--engine', 'redis',
        '--engine-version', '4.0.10', '--replication-group-id', 'pkb-run12345',
        '--replication-group-description', 'pkb-run12345', '--region',
        'us-east-1', '--cache-node-type', 'cache.m4.large',
        '--cache-subnet-group-name', 'subnet-pkb-run12345',
        '--preferred-cache-cluster-a-zs', 'us-east-1a', '--tags'
    ]
    self.redis._Create()
    self.mock_command.assert_called_once_with(
        expected_output, raise_on_failure=False)

  def testDelete(self):
    self.mock_command.return_value = (None, '', None)
    expected_output = [
        'aws', 'elasticache', 'delete-replication-group', '--region',
        'us-east-1', '--replication-group-id', 'pkb-run12345'
    ]
    self.redis._Delete()
    self.mock_command.assert_called_once_with(
        expected_output, raise_on_failure=False)

  def testExistTrue(self):
    self.mock_command.return_value = (None, '', None)
    expected_output = [
        'aws', 'elasticache', 'describe-replication-groups', '--region',
        'us-east-1', '--replication-group-id', 'pkb-run12345'
    ]
    self.redis._Exists()
    self.mock_command.assert_called_once_with(
        expected_output, raise_on_failure=False)


if __name__ == '__main__':
  unittest.main()
