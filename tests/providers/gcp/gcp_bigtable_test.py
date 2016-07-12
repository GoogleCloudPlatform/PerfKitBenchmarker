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
"""Tests for perfkitbenchmarker.providers.gcp.gcp_bigtable"""

import mock
import unittest

from perfkitbenchmarker.providers.gcp import gcp_bigtable
from perfkitbenchmarker.providers.gcp import util


NAME = 'testcluster'
NUM_NODES = 3
PROJECT = 'testproject'
ZONE = 'testzone'

VALID_JSON_BASE = """[
    {{
      "displayName": "SSD Instance",
      "name": "projects/{0}/instances/not{1}",
      "state": "READY"
    }},
    {{
      "displayName": "HDD Instance",
      "name": "projects/{0}/instances/{1}",
      "state": "READY"
    }}
]"""


class GcpBigtableTestCase(unittest.TestCase):

  def setUp(self):
    super(GcpBigtableTestCase, self).setUp()
    self.bigtable = gcp_bigtable.GcpBigtableInstance(NAME, NUM_NODES, PROJECT,
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
    stdout = VALID_JSON_BASE.format(PROJECT, NAME)
    with mock.patch.object(util.GcloudCommand, 'Issue',
                           return_value=(stdout, '', 0)):
      self.assertTrue(self.bigtable._Exists())

  def testNotFoundTable(self):
    stdout = VALID_JSON_BASE.format(PROJECT, NAME + 'nope')
    with mock.patch.object(util.GcloudCommand, 'Issue',
                           return_value=(stdout, '', 0)):
      self.assertFalse(self.bigtable._Exists())


if __name__ == '__main__':
  unittest.main()
