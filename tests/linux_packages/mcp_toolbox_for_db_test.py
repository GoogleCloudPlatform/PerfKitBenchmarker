# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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

import unittest

from absl.testing import flagsaver
from perfkitbenchmarker.linux_packages import mcp_toolbox_for_db
from tests import pkb_common_test_case


class McpToolboxForDbTest(pkb_common_test_case.PkbCommonTestCase):

  def test_get_version_default(self):
    self.assertEqual(mcp_toolbox_for_db.GetVersion(), 'v1.6.0')

  @flagsaver.flagsaver(mcp_toolbox_version='2.0.0')
  def test_get_version_without_v_prefix(self):
    self.assertEqual(mcp_toolbox_for_db.GetVersion(), 'v2.0.0')

  @flagsaver.flagsaver(mcp_toolbox_version='v2.0.0')
  def test_get_version_with_v_prefix(self):
    self.assertEqual(mcp_toolbox_for_db.GetVersion(), 'v2.0.0')


if __name__ == '__main__':
  unittest.main()
