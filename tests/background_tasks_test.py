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

"""Tests for perfkitbenchmarker.background_tasks."""


import functools
import os
import signal
import threading
import unittest

from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import errors
from tests import pkb_common_test_case
from six.moves import range


def _ReturnArgs(a, b=None):
  return b, a


def _RaiseValueError():
  raise ValueError('ValueError')


def _IncrementCounter(counter):
  counter.value += 1


def _AppendLength(int_list):
  int_list.append(len(int_list))


def _WaitAndAppendInt(int_list, int_to_append, event=None, timeout=None):
  if event:
    event.wait(timeout)
  int_list.append(int_to_append)


class Counter():

  def __init__(self):
    self.value = 0


class GetCallStringTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testNoArgs(self):
    result = background_tasks._GetCallString((_ReturnArgs, (), {}))
    self.assertEqual(result, '_ReturnArgs()')

  def testArgs(self):
    result = background_tasks._GetCallString((_ReturnArgs, ('blue', 5), {}))
    self.assertEqual(result, '_ReturnArgs(blue, 5)')

  def testKwargs(self):
    result = background_tasks._GetCallString((_ReturnArgs, (), {'x': 8}))
    self.assertEqual(result, '_ReturnArgs(x=8)')

  def testArgsAndKwargs(self):
    result = background_tasks._GetCallString((_ReturnArgs, ('blue', 5),
                                              {'x': 8}))
    self.assertEqual(result, '_ReturnArgs(blue, 5, x=8)')

  def testSinglePartial(self):
    _ReturnArgs2 = functools.partial(_ReturnArgs, 1, x=2)
    result = background_tasks._GetCallString((_ReturnArgs2, (), {}))
    self.assertEqual(result, '_ReturnArgs(1, x=2)')
    result = background_tasks._GetCallString((_ReturnArgs2, ('blue', 5),
                                              {'x': 8}))
    self.assertEqual(result, '_ReturnArgs(1, blue, 5, x=8)')

  def testDoublePartial(self):
    _ReturnArgs2 = functools.partial(_ReturnArgs, 1, x=2)
    _ReturnArgs3 = functools.partial(_ReturnArgs2, 3, x=4)
    result = background_tasks._GetCallString((_ReturnArgs3, (), {}))
    self.assertEqual(result, '_ReturnArgs(1, 3, x=4)')
    result = background_tasks._GetCallString((_ReturnArgs3, ('blue', 5),
                                              {'x': 8}))
    self.assertEqual(result, '_ReturnArgs(1, 3, blue, 5, x=8)')


class RunParallelThreadsTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testFewerThreadsThanConcurrencyLimit(self):
    calls = [(_ReturnArgs, ('a',), {'b': i}) for i in range(2)]
    result = background_tasks.RunParallelThreads(calls, max_concurrency=4)
    self.assertEqual(result, [(0, 'a'), (1, 'a')])

  def testMoreThreadsThanConcurrencyLimit(self):
    calls = [(_ReturnArgs, ('a',), {'b': i}) for i in range(10)]
    result = background_tasks.RunParallelThreads(calls, max_concurrency=4)
    self.assertEqual(result, [(i, 'a') for i in range(10)])

  def testException(self):
    int_list = []
    calls = [(_AppendLength, (int_list,), {}), (_RaiseValueError, (), {}),
             (_AppendLength, (int_list,), {})]
    with self.assertRaises(errors.VmUtil.ThreadException):
      background_tasks.RunParallelThreads(calls, max_concurrency=1)
    self.assertEqual(int_list, [0, 1])

  def testInterrupt(self):
    # Uses RunParallelThreads to try to run four threads:
    #   0: Waits 5 seconds and adds 0 to int_list.
    #   1: Adds 1 to int_list.
    #   2: Sends a SIGINT to the current process.
    #   3: Waits 5 seconds and adds 3 to int_list.
    # Since the max_concurrency is set to 2, what should happen is that thread 0
    # waits, thread 1 succeeds, thread 2 sends the SIGINT, and then neither
    # thread 1 nor 3 is able to append to int_list.
    int_list = []
    event = threading.Event()
    calls = [(_WaitAndAppendInt, (int_list, 0, event, 5), {}),
             (_WaitAndAppendInt, (int_list, 1), {}),
             (os.kill, (os.getpid(), signal.SIGINT), {}),
             (_WaitAndAppendInt, (int_list, 3, event, 5), {})]
    with self.assertRaises(KeyboardInterrupt):
      background_tasks.RunParallelThreads(calls, max_concurrency=2)
    self.assertEqual(int_list, [1])


class RunThreadedTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testNonListParams(self):
    with self.assertRaises(ValueError):
      background_tasks.RunThreaded(_ReturnArgs, 'blue')

  def testNoParams(self):
    result = background_tasks.RunThreaded(_ReturnArgs, [])
    self.assertEqual(result, [])

  def testInvalidTupleParams(self):
    with self.assertRaises(ValueError):
      background_tasks.RunThreaded(_ReturnArgs, [('blue', 'red')])

  def testSimpleListParams(self):
    result = background_tasks.RunThreaded(_ReturnArgs, ['blue', 'red'])
    self.assertEqual(result, [(None, 'blue'), (None, 'red')])

  def testListOfTupleParams(self):
    result = background_tasks.RunThreaded(
        _ReturnArgs, [(('red',), {}), (('green',), {'b': 'blue'})])
    self.assertEqual(result, [(None, 'red'), ('blue', 'green')])


class RunParallelProcessesTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testFewerThreadsThanConcurrencyLimit(self):
    calls = [(_ReturnArgs, ('a',), {'b': i}) for i in range(2)]
    result = background_tasks.RunParallelProcesses(calls, max_concurrency=4)
    self.assertEqual(result, [(0, 'a'), (1, 'a')])

  def testMoreThreadsThanConcurrencyLimit(self):
    calls = [(_ReturnArgs, ('a',), {'b': i}) for i in range(10)]
    result = background_tasks.RunParallelProcesses(calls, max_concurrency=4)
    self.assertEqual(result, [(i, 'a') for i in range(10)])

  def testException(self):
    counter = Counter()
    calls = [(_IncrementCounter, (counter,), {}),
             (_RaiseValueError, (), {}),
             (_IncrementCounter, (counter,), {})]
    with self.assertRaises(errors.VmUtil.CalledProcessException):
      background_tasks.RunParallelProcesses(calls, max_concurrency=1)

    # RunParallelProcesses does not gurantee the tasks are run in order.
    self.assertLessEqual(counter.value, 2, 'Unexpected counter value')


if __name__ == '__main__':
  unittest.main()
