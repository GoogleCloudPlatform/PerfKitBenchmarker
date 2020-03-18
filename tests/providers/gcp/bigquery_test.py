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
"""Tests for perfkitbenchmarker.providers.gcp.bigquery."""
import copy
import unittest

from absl.testing import flagsaver
import mock
from perfkitbenchmarker import flags
from perfkitbenchmarker.configs import benchmark_config_spec
from perfkitbenchmarker.providers.gcp import bigquery
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case

_TEST_RUN_URI = 'fakeru'
_GCP_ZONE_US_CENTRAL_1_C = 'us-central1-c'

_BASE_BIGQUERY_SPEC = {
    'type': 'bigquery',
    'cluster_identifier': 'bigquerypkb.tpcds_100G'
}

FLAGS = flags.FLAGS


class FakeRemoteVM(object):

  def Install(self, package_name):
    if package_name != 'google_cloud_sdk':
      raise RuntimeError


class AzureDWTestCase(pkb_common_test_case.PkbCommonTestCase):

  @flagsaver.flagsaver(cloud='GCP')
  @flagsaver.flagsaver(run_uri=_TEST_RUN_URI)
  @flagsaver.flagsaver(zones=[_GCP_ZONE_US_CENTRAL_1_C])
  def testInstallAndAuthenticateRunner(self):
    kwargs = copy.copy(_BASE_BIGQUERY_SPEC)
    spec = benchmark_config_spec._EdwServiceSpec('NAME', **kwargs)
    bigquery_local = bigquery.Bigquery(spec)
    with mock.patch(util.__name__ +
                    '.AuthenticateServiceAccount') as mock_issue:
      bigquery_local.InstallAndAuthenticateRunner(
          vm=FakeRemoteVM(), benchmark_name='fake_benchmark_name')
      mock_issue.assert_called_once()


if __name__ == '__main__':
  unittest.main()
