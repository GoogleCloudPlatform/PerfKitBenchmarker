# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.packages.dstat"""

import os
import unittest


from perfkitbenchmarker.linux_packages import dstat


class DstatTestCase(unittest.TestCase):

  def testParseDstatFile(self):
    path = os.path.join(os.path.dirname(__file__), '..', 'data',
                        'dstat-result.csv')
    with open(path) as f:
      labels, out = dstat.ParseCsvFile(iter(f))

    self.assertEqual(len(labels), len(out[0]))
    self.assertEqual(out.shape, (383, 62))
    self.assertEqual([
        'epoch__epoch', 'usr__total cpu usage', 'sys__total cpu usage',
        'idl__total cpu usage', 'wai__total cpu usage', 'hiq__total cpu usage',
        'siq__total cpu usage', '1m__load avg', '5m__load avg', '15m__load avg',
        'read__io/total', 'writ__io/total', 'read__io/sda', 'writ__io/sda',
        'read__dsk/total', 'writ__dsk/total', 'read__dsk/sda', 'writ__dsk/sda',
        'recv__net/total', 'send__net/total', 'in__paging', 'out__paging',
        'int__system', 'csw__system', '12__interrupts', '25__interrupts',
        '30__interrupts', 'run__procs', 'blk__procs', 'new__procs',
        'used__memory usage', 'buff__memory usage', 'cach__memory usage',
        'free__memory usage', 'used__swap', 'free__swap', 'files__filesystem',
        'inodes__filesystem', 'msg__sysv ipc', 'sem__sysv ipc', 'shm__sysv ipc',
        'lis__tcp sockets', 'act__tcp sockets', 'syn__tcp sockets',
        'tim__tcp sockets', 'clo__tcp sockets', 'lis__udp', 'act__udp',
        'raw__raw', 'tot__sockets', 'tcp__sockets', 'udp__sockets',
        'raw__sockets', 'frg__sockets', 'dgm__unix sockets',
        'str__unix sockets', 'lis__unix sockets', 'act__unix sockets',
        'majpf__virtual memory', 'minpf__virtual memory',
        'alloc__virtual memory', 'free__virtual memory'], labels)


if __name__ == '__main__':
  unittest.main()
