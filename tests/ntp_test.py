# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for ntp.py."""

import mock
import unittest

from perfkitbenchmarker.linux_packages import ntp


def MockVMValReader(name, value):
  """Return an object for mocking a remote NTP installation.

  Args:
    name: string. The name of an NTP property.
    value: string. The value of that property.

  Returns:
    A mock object whose RemoteCommand method returns the given
    name=value pair in the format of NTP's readvar command.
  """

  vm = mock.MagicMock()
  vm.RemoteCommand = mock.MagicMock(return_value=('%s=%s' % (name, value), ''))
  return vm


class TestReadDecimalSystemVar(unittest.TestCase):
  def testReadZero(self):
    vm = MockVMValReader('valname', '0.000')
    self.assertEqual(ntp._ReadDecimalSystemVar(vm, 'valname'),
                     0.0)

  def testReadPositive(self):
    vm = MockVMValReader('valname', '0.500')
    self.assertEqual(ntp._ReadDecimalSystemVar(vm, 'valname'),
                     0.5)

  def testReadNegative(self):
    vm = MockVMValReader('valname', '-0.500')
    self.assertEqual(ntp._ReadDecimalSystemVar(vm, 'valname'),
                     -0.5)


class TestSecsSinceLastSync(unittest.TestCase):
  def testExample(self):
    # The return value of 'ntpq -p'
    NTPQ_P = """      remote           refid      st t when poll reach   delay   offset  jitter
==============================================================================
*metadata.google 120.68.64.68     2 u   30   64    1    0.513    0.300   0.248

"""  # noqa: line is too long.

    self.assertEqual(ntp._SecsSinceLastSyncFromNTPQP(NTPQ_P), 30.0)

  def testUnreachedPeer(self):
    NTPQ_P = """      remote           refid      st t when poll reach   delay   offset  jitter
==============================================================================
*ec2-54-83-7-186 18.26.4.105      2 u    8   64   37    0.802    6.506   3.643
 time-b.nist.gov .ACTS.           1 u  132   64   14   66.487   39.934   1.653
+hydrogen.consta 200.98.196.212   2 u    2   64   37    7.566    5.767   3.285
-h113.95.219.67. 128.138.141.172  2 u   64   64   17   53.159    2.801  12.343
+juniperberry.ca 193.79.237.14    2 u    -   64   37   74.317    3.049   2.730
"""  # noqa: line is too long.

    self.assertEqual(ntp._SecsSinceLastSyncFromNTPQP(NTPQ_P), 2.0)
