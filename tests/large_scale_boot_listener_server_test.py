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

"""Test for perfkitbenchmarker/data/large_scale?boot/listener_server.py."""

import unittest
import mock

from perfkitbenchmarker.data.large_scale_boot import listener_server
from tests import pkb_common_test_case


class ListenerServerTest(pkb_common_test_case.PkbCommonTestCase):

  @mock.patch('subprocess.Popen')
  def testConfirmIPAccessibleSuccess(self, mock_subproc_popen):
    reply = 'booter.p3rf-scaling.internal [10.150.0.158] 22 (ssh) open'
    process_mock = mock.Mock()
    mock_subproc_popen.return_value = process_mock
    process_mock.communicate.return_value = None, reply.encode('utf-8')
    fake_client_host = '1.2.3.4'
    fake_port = 22
    success = listener_server.ConfirmIPAccessible(fake_client_host, fake_port)
    self.assertEqual(success.split(':')[0], 'Pass')
    self.assertEqual(success.split(':')[1], fake_client_host)

  @mock.patch('subprocess.Popen')
  def testConfirmIPAccessibleFail(self, mock_subproc_popen):
    reply = 'booter.p3rf-scaling.internal [1.2.3.4] 22 (ssh):Connection refused'
    process_mock = mock.Mock()
    mock_subproc_popen.return_value = process_mock
    process_mock.communicate.return_value = None, reply.encode('utf-8')
    fake_client_host = '1.2.3.4'
    fake_port = 22
    success = listener_server.ConfirmIPAccessible(
        fake_client_host, fake_port, timeout=1)
    self.assertEqual(success.split(':')[0], 'Fail')
    self.assertEqual(success.split(':')[1], fake_client_host)

if __name__ == '__main__':
  unittest.main()
