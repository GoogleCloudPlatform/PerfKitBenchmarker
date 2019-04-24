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
    FLAGS['hbase_use_stable'].parse(False)
    FLAGS['hbase_version'].parse('1.3.2.1')
    p = mock.patch.object(urllib.request, 'urlopen')
    self.mock_url_open = p.start()
    self.addCleanup(p.stop)

  def SetUrlOpenResponse(self, text, http_code=200):
    response = mock.Mock()
    self.mock_url_open.return_value = response
    response.read.return_value = text
    response.getcode.return_value = http_code

  def testGetUrlStable(self):
    FLAGS['hbase_use_stable'].parse(True)
    self.SetUrlOpenResponse(MakeHbaseUrl('1.4.6'))
    url = hbase._GetHBaseURL()
    self.assertRegexpMatches(url, 'hbase-1.4.6-bin.tar.gz$')

  def testGetUrlVersion(self):
    self.SetUrlOpenResponse(MakeHbaseUrl('1.3.2.1'))
    url = hbase._GetHBaseURL()
    self.assertRegexpMatches(url, 'hbase-1.3.2.1-bin.tar.gz$')

  def testGetHBaseURLBadResponse(self):
    # response is for version 0.1.2 but wanted the default 1.3.2.1
    self.SetUrlOpenResponse(MakeHbaseUrl('0.1.2'))
    with self.assertRaisesRegexp(ValueError, r'0\.1\.2.*1\.3\.2\.1'):
      hbase._GetHBaseURL()


def MakeHbaseUrl(version):
  return ('<a href="hbase-{version}-bin.tar.gz">'
          'hbase-{version}-bin.tar.gz</a>').format(version=version)


if __name__ == '__main__':
  unittest.main()
