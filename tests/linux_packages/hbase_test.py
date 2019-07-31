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
"""Tests for perfkitbenchmarker.linux_packages.hbase."""

import unittest
import mock
from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import hbase
from tests import pkb_common_test_case
from six.moves import urllib

FLAGS = flags.FLAGS


class HbaseTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(HbaseTest, self).setUp()
    FLAGS['hbase_version'].parse('1.3.2.1')
    p = mock.patch.object(urllib.request, 'urlopen')
    self.mock_url_open = p.start()
    self.addCleanup(p.stop)

  def testGetUrlVersion(self):
    url = hbase._GetHBaseURL()
    self.assertRegexpMatches(url, 'hbase-1.3.2.1-bin.tar.gz$')


def MakeHbaseUrl(version):
  return ('<a href="hbase-{version}-bin.tar.gz">'
          'hbase-{version}-bin.tar.gz</a>').format(version=version)


if __name__ == '__main__':
  unittest.main()
