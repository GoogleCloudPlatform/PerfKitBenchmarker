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

import unittest
import mock

from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.gcp import gcp_bigtable
from perfkitbenchmarker.providers.gcp import util
from tests import pkb_common_test_case

NAME = 'testcluster'
PROJECT = 'testproject'
ZONE = 'testzone'

VALID_JSON_BASE = """[
    {{
      "displayName": "not{name}",
      "name": "projects/{project}/instances/not{name}",
      "state": "READY"
    }},
    {{
      "displayName": "{name}",
      "name": "projects/{project}/instances/{name}",
      "state": "READY"
    }}
]"""


OUT_OF_QUOTA_STDERR = """
ERROR: (gcloud.beta.bigtable.instances.create) Operation successfully rolled
back : Insufficient node quota. You requested a node count of 1 nodes for your
cluster, but this request would exceed your project's node quota of 30 nodes
total across all clusters in this zone. Contact us to request a
quota increase: https://cloud.google.com/bigtable/quotas#quota-increase
"""


class GcpBigtableTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(GcpBigtableTestCase, self).setUp()
    self.bigtable = gcp_bigtable.GcpBigtableInstance(NAME, PROJECT,
                                                     ZONE)

  def testEmptyTableList(self):
    with mock.patch.object(util.GcloudCommand, 'Issue',
                           return_value=('{}', '', 0)):
      self.assertFalse(self.bigtable._Exists())

  def testGcloudError(self):
    with mock.patch.object(util.GcloudCommand, 'Issue',
                           return_value=('', '', 1)):
      self.assertFalse(self.bigtable._Exists())

  def testFoundTable(self):
    stdout = VALID_JSON_BASE.format(project=PROJECT, name=NAME)
    with mock.patch.object(util.GcloudCommand, 'Issue',
                           return_value=(stdout, '', 0)):
      self.assertTrue(self.bigtable._Exists())

  def testNotFoundTable(self):
    stdout = VALID_JSON_BASE.format(project=PROJECT, name=NAME + 'nope')
    with mock.patch.object(util.GcloudCommand, 'Issue',
                           return_value=(stdout, '', 0)):
      self.assertFalse(self.bigtable._Exists())

  def testQuotaError(self):
    self.enter_context(
        mock.patch.object(
            util.GcloudCommand,
            'Issue',
            return_value=[None, OUT_OF_QUOTA_STDERR, None]))
    with self.assertRaises(errors.Benchmarks.QuotaFailure):
      self.bigtable._Create()


if __name__ == '__main__':
  unittest.main()
