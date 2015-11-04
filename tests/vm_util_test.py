# Copyright 2014 Google Inc. All rights reserved.
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
import signal
import subprocess
import threading
import traceback
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


def _RaiseIfEqual(a, b=None, test_hook=None):
  if test_hook:
    test_hook_done = False
    while not test_hook_done:
      try:
        test_hook()
        test_hook_done = True
      except KeyboardInterrupt:
        pass
  if a == b:
    raise ValueError('a={0}, b={1}'.format(a, b))
  return a


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


def _ChangeProcessGroupAndExecuteFunction(initialized_event, namespace, target,
                                          *args, **kwargs):
  try:
    os.setpgrp()
    initialized_event.set()
    namespace.return_value = target(*args, **kwargs)
  except BaseException as e:
    namespace.exception_type = type(e).__name__
    namespace.traceback = traceback.format_exc()


class RunningInDifferentProcessGroup(object):
  """Context manager that calls a function in a different process group.

  The block of code enclosed by a with will run in the current process in
  parallel with the passed-in function.

  Attributes:
    pgid: None or int. ID of the new process group. Initially None, but is set
        upon entering the context block.
    expected_exception_type: None or string. If set to a string name of an
        exception while inside the context block, then upon exiting the block,
        the type of any exception raised by the called function will be compared
        against that expected type.
    return_value: Return value of the function call. Initially None, but is set
        upon exiting the context block.
  """

  def __init__(self, target, *args, **kwargs):
    self._manager = multiprocessing.managers.SyncManager()
    self._manager.start()
    self._initialized_event = self._manager.Event()
    self._namespace = self._manager.Namespace()
    self._namespace.return_value = None
    self._namespace.exception_type = None
    self._namespace.traceback = None
    self._process = multiprocessing.Process(
        target=_ChangeProcessGroupAndExecuteFunction,
        args=(self._initialized_event, self._namespace, target) + args,
        kwargs=kwargs)
    self.pgid = None
    self.expected_exception_type = None
    self.return_value = None

  def __enter__(self):
    self._process.start()
    self._initialized_event.wait()
    self.pgid = os.getpgid(self._process.pid)
    return self

  def __exit__(self, unused_exc_type, unused_exc_value, unused_traceback):
    self._process.join(timeout=5.)
    try:
      os.killpg(self.pgid, signal.SIGKILL)
    except OSError:
      pass
    if self._namespace.exception_type != self.expected_exception_type:
      if self._namespace.exception_type:
        msg = '{0} was raised with the following traceback:{1}{2}'.format(
            self._namespace.exception_type, os.linesep,
            self._namespace.traceback)
        if self.expected_exception_type:
          msg = 'Expected {0}, but '.format(self.expected_exception_type) + msg
        else:
          msg = 'Did not expect an exception, but ' + msg
        raise AssertionError(msg)
      else:
        raise AssertionError(
            'Expected {0}, but no exception was raised.'.format(
                self.expected_exception_type))
    self.return_value = self._namespace.return_value


def _RunParallelProcessesTestHook(set_event, wait_event):
  """Test hook function to be used with RunParallelProcesses.

  Generate a partial from this function that pre-injects two events. When
  RunParallelProcesses reaches a test hook, it will call the corresponding
  partial, which will set an event and then wait for another event.

  Args:
    set_event: multiprocessing.Event. The event to be set.
    wait_event: multiprocessing.Event. The event to wait for.
  """
  set_event.set()
  wait_event.wait()


class _ParallelProcessCallTestHookController(object):
  """Helper class to allow RunParallelProcess tests to control timing of a call.

  Creates events for a child process to wait on or for the parent process to
  wait on when interacting with a child process.
  """

  def __init__(self):
    self._event_pairs = {}
    for test_hook_name in vm_util.RUN_PARALLEL_PROCESSES_TEST_HOOKS:
      event_pair = (multiprocessing.Event(), multiprocessing.Event())
      event_pair[1].set()
      self._event_pairs[test_hook_name] = event_pair

  def GetTestHooks(self):
    test_hooks = (functools.partial(_RunParallelProcessesTestHook,
                                    *self._event_pairs[hook_name])
                  for hook_name in vm_util.RUN_PARALLEL_PROCESSES_TEST_HOOKS)
    return vm_util.RunParallelProcessesTestHooks(*test_hooks)

  def RegisterWaitAtHook(self, test_hook_name):
    self._event_pairs[test_hook_name][1].clear()

  def WaitUntilHookCalled(self, test_hook_name):
    event_was_set = self._event_pairs[test_hook_name][0].wait(timeout=5.)
    if not event_was_set:
      raise AssertionError(
          'Expected {0} hook to be called, but it was not.'.format(
              test_hook_name))

  def ProceedBeyondHook(self, test_hook_name):
    self._event_pairs[test_hook_name][0].clear()
    self._event_pairs[test_hook_name][1].set()

  def WaitUntilHookCalledAndProceed(self, test_hook_name):
    self.WaitUntilHookCalled(test_hook_name)
    self.ProceedBeyondHook(test_hook_name)

  def HookWasCalled(self, test_hook_name):
    return self._event_pairs[test_hook_name][0].is_set()


class RunParallelProcessesTestCase(unittest.TestCase):

  def setUp(self):
    super(RunParallelProcessesTestCase, self).setUp()
    self.call_controllers = []
    self.test_hooks = []
    for call_id in range(6):
      call_controller = _ParallelProcessCallTestHookController()
      self.call_controllers.append(call_controller)
      self.test_hooks.append(call_controller.GetTestHooks())

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

  def testSuppressCallException(self):
    calls = [(_RaiseIfEqual, (i, 2), {}) for i in range(4)]
    result = vm_util.RunParallelProcesses(calls, max_concurrency=4,
                                          suppress_exceptions=True)
    self.assertEqual(result, [0, 1, None, 3])

  def testInterruptAndRaise(self):
    calls = [(_RaiseIfEqual, (call_id,), {}) for call_id in range(6)]
    self.call_controllers[3].RegisterWaitAtHook('child_before_queue')
    self.call_controllers[4].RegisterWaitAtHook('parent_before_active')
    rpp = functools.partial(vm_util.RunParallelProcesses, calls,
                            max_concurrency=2, test_hooks=self.test_hooks)
    with RunningInDifferentProcessGroup(rpp) as r:
      for call_id in range(3):
        self.call_controllers[call_id].WaitUntilHookCalledAndProceed(
            'child_after_queue')
      self.call_controllers[3].WaitUntilHookCalled('child_before_queue')
      self.call_controllers[4].WaitUntilHookCalled('parent_before_active')
      os.killpg(r.pgid, signal.SIGINT)
      r.expected_exception_type = 'KeyboardInterrupt'

  def _testInterruptAtTestHook(self, test_hook_name):
    calls = [(_RaiseIfEqual, (call_id,),
              {'test_hook': self.test_hooks[call_id].child_inside_call})
             for call_id in range(6)]
    for call_id in range(3):
      self.call_controllers[call_id].RegisterWaitAtHook('child_after_queue')
    self.call_controllers[3].RegisterWaitAtHook('child_before_queue')
    self.call_controllers[4].RegisterWaitAtHook(test_hook_name)
    self.call_controllers[4].RegisterWaitAtHook('child_before_call')
    rpp = functools.partial(vm_util.RunParallelProcesses, calls,
                            max_concurrency=2, suppress_exceptions=True,
                            test_hooks=self.test_hooks)
    with RunningInDifferentProcessGroup(rpp) as r:
      for call_id in range(3):
        self.call_controllers[call_id].WaitUntilHookCalledAndProceed(
            'child_after_queue')
      self.call_controllers[3].WaitUntilHookCalled('child_before_queue')
      self.call_controllers[4].WaitUntilHookCalled(test_hook_name)
      if test_hook_name == 'parent_after_start':
        self.call_controllers[4].WaitUntilHookCalled('child_before_call')
      os.killpg(r.pgid, signal.SIGINT)
    self.assertEqual(r.return_value, [0, 1, 2, 3, None, None])

  def testInterruptBeforeActive(self):
    self._testInterruptAtTestHook('parent_before_active')

  def testInterruptAfterActive(self):
    self._testInterruptAtTestHook('parent_after_active')

  def testInterruptAfterStart(self):
    self._testInterruptAtTestHook('parent_after_start')

  def testChildrenCanHandleInterruptGracefully(self):
    calls = [(_RaiseIfEqual, (call_id,),
              {'test_hook': self.test_hooks[call_id].child_inside_call})
             for call_id in range(4)]
    for call_id in range(4):
      self.call_controllers[call_id].RegisterWaitAtHook('child_inside_call')
    self.call_controllers[3].RegisterWaitAtHook('parent_after_start')
    rpp = functools.partial(vm_util.RunParallelProcesses, calls,
                            max_concurrency=4, suppress_exceptions=True,
                            test_hooks=self.test_hooks)
    with RunningInDifferentProcessGroup(rpp) as r:
      for call_id in range(4):
        self.call_controllers[call_id].WaitUntilHookCalled('child_inside_call')
      self.call_controllers[3].WaitUntilHookCalled('parent_after_start')
      os.killpg(r.pgid, signal.SIGINT)
      for call_id in range(4):
        self.call_controllers[call_id].ProceedBeyondHook('child_inside_call')
    self.assertEqual(r.return_value, [0, 1, 2, 3])

  def testSecondInterruptKillsChildren(self):
    calls = [(_RaiseIfEqual, (call_id,),
              {'test_hook': self.test_hooks[call_id].child_inside_call})
             for call_id in range(4)]
    for call_id in range(4):
      self.call_controllers[call_id].RegisterWaitAtHook('child_inside_call')
      self.call_controllers[call_id].RegisterWaitAtHook('parent_after_join')
    rpp = functools.partial(vm_util.RunParallelProcesses, calls,
                            max_concurrency=4, suppress_exceptions=True,
                            test_hooks=self.test_hooks)
    with RunningInDifferentProcessGroup(rpp) as r:
      for call_id in range(4):
        self.call_controllers[call_id].WaitUntilHookCalled('child_inside_call')
      # First call completes successfully.
      self.call_controllers[0].ProceedBeyondHook('child_inside_call')
      self.call_controllers[0].WaitUntilHookCalled('parent_after_join')
      # Send first interrupt. Allow second call to clean up in response.
      os.killpg(r.pgid, signal.SIGINT)
      self.call_controllers[1].ProceedBeyondHook('child_inside_call')
      self.call_controllers[1].WaitUntilHookCalled('parent_after_join')
      # Send second interrupt. Remaining calls are killed.
      os.killpg(r.pgid, signal.SIGINT)
    self.assertEqual(r.return_value, [0, 1, None, None])


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
