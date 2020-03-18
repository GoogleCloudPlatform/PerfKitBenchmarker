# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.aws.athena."""

import copy
import unittest

from absl.testing import flagsaver
from perfkitbenchmarker import flags
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.aws import athena
from tests import pkb_common_test_case

_TEST_RUN_URI = 'fakeru'
_AWS_ZONE_US_EAST_1A = 'us-east-1a'
_BASE_ATHENA_SPEC = {'type': 'athena', 'cluster_identifier': 'tpc_h_100'}

FLAGS = flags.FLAGS


class FakeRemoteVMCreateLambdaRole(object):

  def Install(self, package_name):
    if package_name not in ('aws_credentials', 'awscli'):
      raise RuntimeError


class AthenaTestCase(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(cloud='AWS')
  @flagsaver.flagsaver(run_uri=_TEST_RUN_URI)
  @flagsaver.flagsaver(zones=[_AWS_ZONE_US_EAST_1A])
  def testInstallAndAuthenticateRunner(self):
    kwargs = copy.copy(_BASE_ATHENA_SPEC)
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    athena_local = athena.Athena(spec)
    athena_local.InstallAndAuthenticateRunner(
        vm=FakeRemoteVMCreateLambdaRole(), benchmark_name='fake_benchmark_name')


if __name__ == '__main__':
  unittest.main()
