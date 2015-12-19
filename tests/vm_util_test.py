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

"""Tests for perfkitbenchmarker.vm_util."""

import functools
import multiprocessing
import multiprocessing.managers
import os
import psutil
import subprocess
import threading
import time
import unittest

import mock

from perfkitbenchmarker import errors
from perfkitbenchmarker import vm_util


class ShouldRunOnInternalIpAddressTestCase(unittest.TestCase):

  def setUp(self):
    p = mock.patch(vm_util.__name__ + '.FLAGS')
    self.flags = p.start()
    self.flags_patch = p
    self.sending_vm = mock.MagicMock()
    self.receiving_vm = mock.MagicMock()

  def tearDown(self):
    self.flags_patch.stop()

  def _RunTest(self, expectation, ip_addresses, is_reachable=True):
    self.flags.ip_addresses = ip_addresses
    self.sending_vm.IsReachable.return_value = is_reachable
    self.assertEqual(
        expectation,
        vm_util.ShouldRunOnInternalIpAddress(
            self.sending_vm, self.receiving_vm))

  def testExternal_Reachable(self):
    self._RunTest(False, vm_util.IpAddressSubset.EXTERNAL, True)

  def testExternal_Unreachable(self):
    self._RunTest(False, vm_util.IpAddressSubset.EXTERNAL, False)

  def testInternal_Reachable(self):
    self._RunTest(True, vm_util.IpAddressSubset.INTERNAL, True)

  def testInternal_Unreachable(self):
    self._RunTest(True, vm_util.IpAddressSubset.INTERNAL, False)

  def testBoth_Reachable(self):
    self._RunTest(True, vm_util.IpAddressSubset.BOTH, True)

  def testBoth_Unreachable(self):
    self._RunTest(True, vm_util.IpAddressSubset.BOTH, False)

  def testReachable_Reachable(self):
    self._RunTest(True, vm_util.IpAddressSubset.REACHABLE, True)

  def testReachable_Unreachable(self):
    self._RunTest(
        False, vm_util.IpAddressSubset.REACHABLE, False)


def HaveSleepSubprocess():
  """Checks if the current process has a sleep subprocess."""

  for child in psutil.Process(os.getpid()).children(recursive=True):
    if 'sleep' in child.cmdline():
      return True
  return False


class WaitUntilSleepTimer(threading.Thread):
  """Timer that waits for a sleep subprocess to appear.

  This is intended for specific tests that want to trigger timer
  expiry as soon as it detects that a subprocess is executing a
  "sleep" command.

  It assumes that the test driver is not parallelizing the tests using
  this method since that may lead to inconsistent results.
  TODO(klausw): If that's an issue, could add a unique fractional part
  to the sleep command args to distinguish them.
  """
  def __init__(self, interval, function):
    threading.Thread.__init__(self)
    self.end_time = time.time() + interval
    self.function = function
    self.finished = threading.Event()
    self.have_sleep = threading.Event()

    def WaitForSleep():
      while not self.finished.is_set():
        if HaveSleepSubprocess():
          self.have_sleep.set()
          break
        time.sleep(0)  # yield to other Python threads

    threading.Thread(target=WaitForSleep).run()

  def cancel(self):
    self.finished.set()

  def run(self):
    while time.time() < self.end_time and not self.have_sleep.is_set():
      time.sleep(0)  # yield to other Python threads
    if not self.finished.is_set():
      self.function()
    self.finished.set()


def _ReturnArgs(a, b=None):
  return b, a


def _RaiseValueError():
  raise ValueError('ValueError')


def _IncrementCounter(lock, counter):
  with lock:
    counter.value += 1


def _AppendLength(int_list):
  int_list.append(len(int_list))


class GetCallStringTestCase(unittest.TestCase):

  def testNoArgs(self):
    result = vm_util._GetCallString((_ReturnArgs, (), {}))
    self.assertEqual(result, '_ReturnArgs()')

  def testArgs(self):
    result = vm_util._GetCallString((_ReturnArgs, ('blue', 5), {}))
    self.assertEqual(result, '_ReturnArgs(blue, 5)')

  def testKwargs(self):
    result = vm_util._GetCallString((_ReturnArgs, (), {'x': 8}))
    self.assertEqual(result, '_ReturnArgs(x=8)')

  def testArgsAndKwargs(self):
    result = vm_util._GetCallString((_ReturnArgs, ('blue', 5), {'x': 8}))
    self.assertEqual(result, '_ReturnArgs(blue, 5, x=8)')

  def testSinglePartial(self):
    _ReturnArgs2 = functools.partial(_ReturnArgs, 1, x=2)
    result = vm_util._GetCallString((_ReturnArgs2, (), {}))
    self.assertEqual(result, '_ReturnArgs(1, x=2)')
    result = vm_util._GetCallString((_ReturnArgs2, ('blue', 5), {'x': 8}))
    self.assertEqual(result, '_ReturnArgs(1, blue, 5, x=8)')

  def testDoublePartial(self):
    _ReturnArgs2 = functools.partial(_ReturnArgs, 1, x=2)
    _ReturnArgs3 = functools.partial(_ReturnArgs2, 3, x=4)
    result = vm_util._GetCallString((_ReturnArgs3, (), {}))
    self.assertEqual(result, '_ReturnArgs(1, 3, x=4)')
    result = vm_util._GetCallString((_ReturnArgs3, ('blue', 5), {'x': 8}))
    self.assertEqual(result, '_ReturnArgs(1, 3, blue, 5, x=8)')


class RunParallelThreadsTestCase(unittest.TestCase):

  def testFewerThreadsThanConcurrencyLimit(self):
    calls = [(_ReturnArgs, ('a',), {'b': i}) for i in range(2)]
    result = vm_util.RunParallelThreads(calls, max_concurrency=4)
    self.assertEqual(result, [(0, 'a'), (1, 'a')])

  def testMoreThreadsThanConcurrencyLimit(self):
    calls = [(_ReturnArgs, ('a',), {'b': i}) for i in range(10)]
    result = vm_util.RunParallelThreads(calls, max_concurrency=4)
    self.assertEqual(result, [(i, 'a') for i in range(10)])

  def testException(self):
    int_list = []
    calls = [(_AppendLength, (int_list,), {}), (_RaiseValueError, (), {}),
             (_AppendLength, (int_list,), {})]
    with self.assertRaises(errors.VmUtil.ThreadException):
      vm_util.RunParallelThreads(calls, max_concurrency=1)
    self.assertEqual(int_list, [0, 1])


class RunThreadedTestCase(unittest.TestCase):

  def testNonListParams(self):
    with self.assertRaises(ValueError):
      vm_util.RunThreaded(_ReturnArgs, 'blue')

  def testNoParams(self):
    result = vm_util.RunThreaded(_ReturnArgs, [])
    self.assertEqual(result, [])

  def testInvalidTupleParams(self):
    with self.assertRaises(ValueError):
      vm_util.RunThreaded(_ReturnArgs, [('blue', 'red')])

  def testSimpleListParams(self):
    result = vm_util.RunThreaded(_ReturnArgs, ['blue', 'red'])
    self.assertEqual(result, [(None, 'blue'), (None, 'red')])

  def testListOfTupleParams(self):
    result = vm_util.RunThreaded(
        _ReturnArgs, [(('red',), {}), (('green',), {'b': 'blue'})])
    self.assertEqual(result, [(None, 'red'), ('blue', 'green')])


class RunParallelProcessesTestCase(unittest.TestCase):

  def testFewerThreadsThanConcurrencyLimit(self):
    calls = [(_ReturnArgs, ('a',), {'b': i}) for i in range(2)]
    result = vm_util.RunParallelProcesses(calls, max_concurrency=4)
    self.assertEqual(result, [(0, 'a'), (1, 'a')])

  def testMoreThreadsThanConcurrencyLimit(self):
    calls = [(_ReturnArgs, ('a',), {'b': i}) for i in range(10)]
    result = vm_util.RunParallelProcesses(calls, max_concurrency=4)
    self.assertEqual(result, [(i, 'a') for i in range(10)])

  def testException(self):
    manager = multiprocessing.managers.SyncManager()
    manager.start()
    lock = manager.Lock()
    counter = manager.Value('i', 0)
    calls = [(_IncrementCounter, (lock, counter), {}),
             (_RaiseValueError, (), {}),
             (_IncrementCounter, (lock, counter), {})]
    with self.assertRaises(errors.VmUtil.CalledProcessException):
      vm_util.RunParallelProcesses(calls, max_concurrency=1)
    self.assertEqual(counter.value, 2)


class IssueCommandTestCase(unittest.TestCase):

  def testTimeoutNotReached(self):
    _, _, retcode = vm_util.IssueCommand(['sleep', '0s'])
    self.assertEqual(retcode, 0)

  @mock.patch('threading.Timer', new=WaitUntilSleepTimer)
  def testTimeoutReached(self):
    _, _, retcode = vm_util.IssueCommand(['sleep', '2s'], timeout=1)
    self.assertEqual(retcode, -9)
    self.assertFalse(HaveSleepSubprocess())

  def testNoTimeout(self):
    _, _, retcode = vm_util.IssueCommand(['sleep', '0s'], timeout=None)
    self.assertEqual(retcode, 0)

  def testNoTimeout_ExceptionRaised(self):
    with mock.patch('subprocess.Popen', spec=subprocess.Popen) as mock_popen:
      mock_popen.return_value.communicate.side_effect = KeyboardInterrupt()
      with self.assertRaises(KeyboardInterrupt):
        vm_util.IssueCommand(['sleep', '2s'], timeout=None)
    self.assertFalse(HaveSleepSubprocess())


if __name__ == '__main__':
  unittest.main()
