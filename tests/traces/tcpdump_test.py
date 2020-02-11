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
"""Tests for tcpdump utility."""

import unittest
from absl.testing import flagsaver
import mock

from perfkitbenchmarker import flags
from tests import pkb_common_test_case
from perfkitbenchmarker.traces import tcpdump

FLAGS = flags.FLAGS

_OUTPUT_FILE = '/tmp/x.pcap'
# all vm.RemoteCommands to launch tcpdump look like this
_CMD_FORMAT = ('sudo tcpdump -n -w {output_file} {{command}} '
               '> /dev/null 2>&1 & echo $!').format(output_file=_OUTPUT_FILE)


class TcpdumpTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(TcpdumpTestCase, self).setUp()
    mock.patch('os.path.isdir', return_value=True).start()
    self.addCleanup(mock.patch.stopall)

  def assertRunLine(self, command):
    expected_command = _CMD_FORMAT.format(command=command)
    collector = tcpdump._CreateCollector(FLAGS)
    actual_command = collector._CollectorRunCommand(None, _OUTPUT_FILE)
    self.assertEqual(expected_command, actual_command)

  def testDefaults(self):
    self.assertRunLine(r'-s 96 not port \(22\)')

  @flagsaver.flagsaver(tcpdump_snaplen=10)
  def testSnaplen(self):
    self.assertRunLine(r'-s 10 not port \(22\)')

  @flagsaver.flagsaver(tcpdump_snaplen=0)
  def testNoSnaplen(self):
    self.assertRunLine(r'not port \(22\)')

  @flagsaver.flagsaver(tcpdump_packet_count=12)
  def testCount(self):
    self.assertRunLine(r'-s 96 -c 12 not port \(22\)')

  @flagsaver.flagsaver(tcpdump_ignore_ports=[53, 80])
  def testIgnorePorts(self):
    self.assertRunLine(r'-s 96 not port \(53 or 80\)')

  @flagsaver.flagsaver(tcpdump_include_ports=[22, 443])
  def testIncludePorts(self):
    self.assertRunLine(r'-s 96 port \(22 or 443\)')

  @flagsaver.flagsaver(tcpdump_ignore_ports=[])
  def testIncludeAll(self):
    self.assertRunLine(r'-s 96')

  def testKillCommand(self):
    collector = tcpdump._CreateCollector(FLAGS)
    vm = mock.Mock()
    vm.RemoteCommand.return_value = ('pid1234', '')
    collector._StartOnVm(vm)
    vm.RemoteCommand.reset_mock()
    collector._StopOnVm(vm, 'roleA')
    vm.RemoteCommand.assert_called_with(
        'sudo kill -s INT pid1234; sleep 3', ignore_failure=True)


if __name__ == '__main__':
  unittest.main()
