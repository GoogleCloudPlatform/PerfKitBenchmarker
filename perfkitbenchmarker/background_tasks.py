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
"""Background tasks that propagate PKB thread context.

TODO(skschneider): Many of the threading module flaws have been corrected in
Python 3. When PKB switches to Python 3, this module can be simplified.

PKB tries its best to clean up provisioned resources upon SIGINT. By default,
Python raises a KeyboardInterrupt upon a SIGINT, but none of the built-in
threading module classes are designed to handle a KeyboardInterrupt very well:

- threading.Lock has an atomic acquire method that cannot be interrupted and
  hangs forever if the same thread tries to acquire twice. Its release method
  can be called by any thread but raises thread.error if an unacquired Lock is
  released.

- More complicated classes (threading.RLock, threading.Event, threading.Thread,
  Queue.Queue) use internal Locks in such a way that a KeyboardInterrupt can
  cause a thread that has acquired a Lock to jump out of its current action
  without releasing the Lock. For example, in the below code, a
  KeyboardInterrupt can be raised immediately after the acquire call but before
  entering the try block:
    lock.acquire()
    try:
      ...
    except:
      lock.release()

Taken together, this means that there is a possibility to leave an internal Lock
acquired, and when later cleanup steps on the same or different thread attempt
to acquire the Lock, they will hang forever, unresponsive to even a second
KeyboardInterrupt. A KeyboardInterrupt during Thread.start() or Thread.join()
can even trigger an unbalanced acquire on a global lock used to keep track of
active threads, so that later attempts to start or join any Thread will hang
forever.

While it would take a significant and impractical redesign of PKB's code to
completely eliminate any risk of deadlock following a KeyboardInterrupt, the
code in this module is designed to allow interrupting parallel tasks while
keeping the risk of deadlock low.
"""

import abc
from collections import deque
from concurrent import futures
import ctypes
import functools
import logging
import os
import Queue
import signal
import threading
import time
import traceback

from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker import log_util


# For situations where an interruptable wait is necessary, a loop of waits with
# long timeouts is used instead. This is because some of Python's built-in wait
# methods are non-interruptable without a timeout.
_LONG_TIMEOUT = 1000.

# Constants used for polling waits. See _WaitForCondition.
_WAIT_MIN_RECHECK_DELAY = 0.001  # 1 ms
_WAIT_MAX_RECHECK_DELAY = 0.050  # 50 ms

# Values sent to child threads that have special meanings.
_THREAD_STOP_PROCESSING = 0
_THREAD_WAIT_FOR_KEYBOARD_INTERRUPT = 1


def _GetCallString(target_arg_tuple):
  """Returns the string representation of a function call."""
  target, args, kwargs = target_arg_tuple
  while isinstance(target, functools.partial):
    args = target.args + args
    inner_kwargs = target.keywords.copy()
    inner_kwargs.update(kwargs)
    kwargs = inner_kwargs
    target = target.func
  arg_strings = [str(a) for a in args]
  arg_strings.extend(['{0}={1}'.format(k, v) for k, v in kwargs.iteritems()])
  return '{0}({1})'.format(getattr(target, '__name__', target),
                           ', '.join(arg_strings))


def _WaitForCondition(condition_callback, timeout=None):
  """Waits until the specified callback returns a value that evaluates True.

  Similar to the threading.Condition.wait method that is the basis of most
  threading class wait routines. Polls the condition, starting with frequent
  checks but extending the delay between checks upon each failure.

  Args:
    condition_callback: Callable that returns a value that evaluates True to end
        the wait or evaluates False to continue the wait.
    timeout: Optional float. Number of seconds to wait before giving up. If
        provided, the condition is still checked at least once before giving up.
        If not provided, the wait does not time out.

  Returns:
    True if condition_callback returned a value that evaluated True. False if
    condition_callback did not return a value that evaluated True before the
    timeout.
  """
  deadline = None if timeout is None else time.time() + timeout
  delay = _WAIT_MIN_RECHECK_DELAY
  while True:
    if condition_callback():
      return True
    remaining_time = (_WAIT_MAX_RECHECK_DELAY if deadline is None
                      else deadline - time.time())
    if remaining_time <= 0:
      return False
    time.sleep(delay)
    delay = min(delay * 2, remaining_time, _WAIT_MAX_RECHECK_DELAY)


class _SingleReaderQueue(object):
  """Queue to which multiple threads write but from which only one thread reads.

  A lightweight substitute for the Queue.Queue class that does not use
  internal Locks.

  Gets are interruptable but depend on polling.
  """

  def __init__(self):
    self._deque = deque()

  def Get(self, timeout=None):
    if not _WaitForCondition(lambda: self._deque, timeout):
      raise Queue.Empty
    return self._deque.popleft()

  def Put(self, item):
    self._deque.append(item)


class _NonPollingSingleReaderQueue(object):
  """Queue to which multiple threads write but from which only one thread reads.

  Uses a threading.Lock to implement a non-interruptable Get that does not poll
  and is therefore easier on CPU usage. The reader waits for items by acquiring
  the Lock, and writers release the Lock to signal that items have been written.
  """

  def __init__(self):
    self._deque = deque()
    self._lock = threading.Lock()
    self._lock.acquire()

  def _WaitForItem(self):
    self._lock.acquire()

  def _SignalAvailableItem(self):
    try:
      self._lock.release()
    except threading.ThreadError:
      pass

  def Get(self):
    while True:
      self._WaitForItem()
      if self._deque:
        item = self._deque.popleft()
        if self._deque:
          self._SignalAvailableItem()
        return item

  def Put(self, item):
    self._deque.append(item)
    self._SignalAvailableItem()


class _BackgroundTaskThreadContext(object):
  """Thread-specific information that can be inherited by a background task.

  Attributes:
    benchmark_spec: BenchmarkSpec of the benchmark currently being executed.
    log_context: ThreadLogContext of the parent thread.
  """

  def __init__(self):
    self.benchmark_spec = context.GetThreadBenchmarkSpec()
    self.log_context = log_util.GetThreadLogContext()

  def CopyToCurrentThread(self):
    """Sets the thread context of the current thread."""
    log_util.SetThreadLogContext(log_util.ThreadLogContext(self.log_context))
    context.SetThreadBenchmarkSpec(self.benchmark_spec)


class _BackgroundTask(object):
  """Base class for a task executed in a child thread or process.

  Attributes:
    target: Function that is invoked in the child thread or process.
    args: Series of unnamed arguments to be passed to the target.
    kwargs: dict. Keyword arguments to be passed to the target.
    context: _BackgroundTaskThreadContext. Thread-specific state to be inherited
        from parent to child thread.
    return_value: Return value if the call was executed successfully, or None
        otherwise.
    traceback: The traceback string if the call raised an exception, or None
        otherwise.
  """

  def __init__(self, target, args, kwargs, thread_context):
    self.target = target
    self.args = args
    self.kwargs = kwargs
    self.context = thread_context
    self.return_value = None
    self.traceback = None

  def Run(self):
    """Sets the current thread context and executes the target."""
    self.context.CopyToCurrentThread()
    try:
      self.return_value = self.target(*self.args, **self.kwargs)
    except Exception:
      self.traceback = traceback.format_exc()


class _BackgroundTaskManager(object):
  """Base class for a context manager that manages state for background tasks.

  Attributes:
    tasks: list of _BackgroundTask instances. Contains one _BackgroundTask per
        started task, in the order that they were started.
  """

  __metaclass__ = abc.ABCMeta

  def __init__(self, max_concurrency):
    self._max_concurrency = max_concurrency
    self.tasks = []

  def __enter__(self):
    return self

  def __exit__(self, *unused_args, **unused_kwargs):
    pass

  @abc.abstractmethod
  def StartTask(self, target, args, kwargs, thread_context):
    """Creates and starts a _BackgroundTask.

    The created task is appended to self.tasks.

    Args:
      target: Function that is invoked in the child thread or process.
      args: Series of unnamed arguments to be passed to the target.
      kwargs: dict. Keyword arguments to be passed to the target.
      thread_context: _BackgroundTaskThreadContext. Thread-specific state to be
          inherited from parent to child thread.
    """
    raise NotImplemented()

  @abc.abstractmethod
  def AwaitAnyTask(self):
    """Waits for any of the started tasks to complete.

    Returns:
      int. Index of the task that completed in self.tasks.
    """
    raise NotImplemented()

  @abc.abstractmethod
  def HandleKeyboardInterrupt(self):
    """Called by the parent thread if a KeyboardInterrupt occurs.

    Ensures that any child thread also receives a KeyboardInterrupt, and then
    waits for each child thread to stop executing.
    """
    raise NotImplemented()


def _ExecuteBackgroundThreadTasks(worker_id, task_queue, response_queue):
  """Executes tasks received on a task queue.

  Executed in a child Thread by _BackgroundThreadTaskManager.

  Args:
    worker_id: int. Identifier for the child thread relative to other child
        threads.
    task_queue: _NonPollingSingleReaderQueue. Queue from which input is read.
        Each value in the queue can be one of three types of values. If it is a
        (task_id, _BackgroundTask) pair, the task is executed on this thread.
        If it is _THREAD_STOP_PROCESSING, the thread stops executing. If it is
        _THREAD_WAIT_FOR_KEYBOARD_INTERRUPT, the thread waits for a
        KeyboardInterrupt.
    response_queue: _SingleReaderQueue. Queue to which output is written. It
        receives worker_id when this thread's bootstrap code has completed and
        receives a (worker_id, task_id) pair for each task completed on this
        thread.
  """
  try:
    response_queue.Put(worker_id)
    while True:
      task_tuple = task_queue.Get()
      if task_tuple == _THREAD_STOP_PROCESSING:
        break
      elif task_tuple == _THREAD_WAIT_FOR_KEYBOARD_INTERRUPT:
        while True:
          time.sleep(_WAIT_MAX_RECHECK_DELAY)
      task_id, task = task_tuple
      task.Run()
      response_queue.Put((worker_id, task_id))
  except KeyboardInterrupt:
    # TODO(skschneider): Detect when the log would be unhelpful (e.g. if the
    # current thread was spinning in the _THREAD_WAIT_FOR_KEYBOARD_INTERRUPT
    # sub-loop). Only log in helpful cases, like when the task is interrupted.
    logging.debug('Child thread %s received a KeyboardInterrupt from its '
                  'parent.', worker_id, exc_info=True)


class _BackgroundThreadTaskManager(_BackgroundTaskManager):
  """Manages state for background tasks started in child threads."""

  def __init__(self, *args, **kwargs):
    super(_BackgroundThreadTaskManager, self).__init__(*args, **kwargs)
    self._response_queue = _SingleReaderQueue()
    self._task_queues = []
    self._threads = []
    self._available_worker_ids = range(self._max_concurrency)
    uninitialized_worker_ids = set(self._available_worker_ids)
    for worker_id in self._available_worker_ids:
      task_queue = _NonPollingSingleReaderQueue()
      self._task_queues.append(task_queue)
      thread = threading.Thread(
          target=_ExecuteBackgroundThreadTasks,
          args=(worker_id, task_queue, self._response_queue))
      thread.daemon = True
      self._threads.append(thread)
      thread.start()
    # Wait for each Thread to finish its bootstrap code. Starting all the
    # threads upfront like this and reusing them for later calls minimizes the
    # risk of a KeyboardInterrupt interfering with any of the Lock interactions.
    for _ in self._threads:
      worker_id = self._response_queue.Get()
      uninitialized_worker_ids.remove(worker_id)
    assert not uninitialized_worker_ids, uninitialized_worker_ids

  def __exit__(self, *unused_args, **unused_kwargs):
    # Shut down worker threads.
    for task_queue in self._task_queues:
      task_queue.Put(_THREAD_STOP_PROCESSING)
    for thread in self._threads:
      _WaitForCondition(lambda: not thread.is_alive())

  def StartTask(self, target, args, kwargs, thread_context):
    assert self._available_worker_ids, ('StartTask called when no threads were '
                                        'available')
    task = _BackgroundTask(target, args, kwargs, thread_context)
    task_id = len(self.tasks)
    self.tasks.append(task)
    worker_id = self._available_worker_ids.pop()
    self._task_queues[worker_id].Put((task_id, task))

  def AwaitAnyTask(self):
    worker_id, task_id = self._response_queue.Get()
    self._available_worker_ids.append(worker_id)
    return task_id

  def HandleKeyboardInterrupt(self):
    # Raise a KeyboardInterrupt in each child thread.
    for thread in self._threads:
      ctypes.pythonapi.PyThreadState_SetAsyncExc(
          ctypes.c_long(thread.ident), ctypes.py_object(KeyboardInterrupt))
    # Wake threads up from possible non-interruptable wait states so they can
    # actually see the KeyboardInterrupt.
    for task_queue, thread in zip(self._task_queues, self._threads):
      task_queue.Put(_THREAD_WAIT_FOR_KEYBOARD_INTERRUPT)
    for thread in self._threads:
      _WaitForCondition(lambda: not thread.is_alive())


def _ExecuteProcessTask(task):
  """Function invoked in another process by _BackgroundProcessTaskManager.

  Executes a specified task function and returns the result or exception
  traceback.

  TODO(skschneider): Rework this helper function when moving to Python 3.5 or
  when the backport of concurrent.futures.ProcessPoolExecutor is able to
  preserve original traceback.

  Args:
    task: _BackgroundTask to execute.

  Returns:
    (result, traceback) tuple. The first element is the return value from the
    task function, or None if the function raised an exception. The second
    element is the exception traceback string, or None if the function
    succeeded.
  """
  def handle_sigint(signum, frame):
    # Ignore any new SIGINTs since we are already tearing down.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    # Execute the default SIGINT handler which throws a KeyboardInterrupt
    # in the main thread of the process.
    signal.default_int_handler(signum, frame)
  signal.signal(signal.SIGINT, handle_sigint)
  task.Run()
  return task.return_value, task.traceback


class _BackgroundProcessTaskManager(_BackgroundTaskManager):
  """Manages states for background tasks started in child processes.

  TODO(skschneider): This class uses futures.ProcessPoolExecutor. We have been
  using this executor since before issues regarding KeyboardInterrupt were
  fully explored. The only consumer of this class is RunParallelProcesses, and
  currently the uses for RunParallelProcesses are limited. In the future, this
  class should also be redesigned for protection against KeyboardInterrupt.
  """

  def __init__(self, *args, **kwargs):
    super(_BackgroundProcessTaskManager, self).__init__(*args, **kwargs)
    self._active_futures = {}
    self._executor = futures.ProcessPoolExecutor(self._max_concurrency)

  def __enter__(self):
    self._executor.__enter__()
    return self

  def __exit__(self, *args, **kwargs):
    # Note: This invokes a non-interruptable wait.
    return self._executor.__exit__(*args, **kwargs)

  def StartTask(self, target, args, kwargs, thread_context):
    task = _BackgroundTask(target, args, kwargs, thread_context)
    task_id = len(self.tasks)
    self.tasks.append(task)
    future = self._executor.submit(_ExecuteProcessTask, task)
    self._active_futures[future] = task_id

  def AwaitAnyTask(self):
    completed_tasks = None
    while not completed_tasks:
      completed_tasks, _ = futures.wait(
          self._active_futures, timeout=_LONG_TIMEOUT,
          return_when=futures.FIRST_COMPLETED)
    future = completed_tasks.pop()
    task_id = self._active_futures.pop(future)
    task = self.tasks[task_id]
    task.return_value, task.traceback = future.result()
    return task_id

  def HandleKeyboardInterrupt(self):
    # If this thread received an interrupt signal, then processes started with
    # a ProcessPoolExecutor will also have received an interrupt without any
    # extra work needed from this class. Only need to wait for child processes.
    # Note: This invokes a non-interruptable wait.
    self._executor.shutdown(wait=True)


def _RunParallelTasks(target_arg_tuples, max_concurrency, get_task_manager,
                      parallel_exception_class):
  """Executes function calls concurrently in separate threads or processes.

  Args:
    target_arg_tuples: list of (target, args, kwargs) tuples. Each tuple
        contains the function to call and the arguments to pass it.
    max_concurrency: int or None. The maximum number of concurrent new
        threads or processes.
    get_task_manager: Callable that accepts an int max_concurrency arg and
        returns a _TaskManager.
    parallel_exception_class: Type of exception to raise upon an exception in
        one of the called functions.

  Returns:
    list of function return values in the order corresponding to the order of
    target_arg_tuples.

  Raises:
    parallel_exception_class: When an exception occurred in any of the called
        functions.
  """
  thread_context = _BackgroundTaskThreadContext()
  max_concurrency = min(max_concurrency, len(target_arg_tuples))
  error_strings = []
  started_task_count = 0
  active_task_count = 0
  with get_task_manager(max_concurrency) as task_manager:
    try:
      while started_task_count < len(target_arg_tuples) or active_task_count:
        if (started_task_count < len(target_arg_tuples) and
            active_task_count < max_concurrency):
          # Start a new task.
          target, args, kwargs = target_arg_tuples[started_task_count]
          task_manager.StartTask(target, args, kwargs, thread_context)
          started_task_count += 1
          active_task_count += 1
          continue

        # Wait for a task to complete.
        task_id = task_manager.AwaitAnyTask()
        active_task_count -= 1
        # If the task failed, it may still be a long time until all remaining
        # tasks complete. Log the failure immediately before continuing to wait
        # for other tasks.
        stacktrace = task_manager.tasks[task_id].traceback
        if stacktrace:
          msg = ('Exception occurred while calling {0}:{1}{2}'.format(
              _GetCallString(target_arg_tuples[task_id]), os.linesep,
              stacktrace))
          logging.error(msg)
          error_strings.append(msg)

    except KeyboardInterrupt:
      logging.error(
          'Received KeyboardInterrupt while executing parallel tasks. Waiting '
          'for %s tasks to clean up.', active_task_count)
      task_manager.HandleKeyboardInterrupt()
      raise

  if error_strings:
    # TODO(skschneider): Combine errors.VmUtil.ThreadException and
    # errors.VmUtil.CalledProcessException so this can be a single exception
    # type.
    raise parallel_exception_class(
        'The following exceptions occurred during parallel execution:'
        '{0}{1}'.format(os.linesep, os.linesep.join(error_strings)))
  results = [task.return_value for task in task_manager.tasks]
  assert len(target_arg_tuples) == len(results), (target_arg_tuples, results)
  return results


def RunParallelThreads(target_arg_tuples, max_concurrency):
  """Executes function calls concurrently in separate threads.

  Args:
    target_arg_tuples: list of (target, args, kwargs) tuples. Each tuple
        contains the function to call and the arguments to pass it.
    max_concurrency: int or None. The maximum number of concurrent new
        threads.

  Returns:
    list of function return values in the order corresponding to the order of
    target_arg_tuples.

  Raises:
    errors.VmUtil.ThreadException: When an exception occurred in any of the
        called functions.
  """
  return _RunParallelTasks(
      target_arg_tuples, max_concurrency, _BackgroundThreadTaskManager,
      errors.VmUtil.ThreadException)


def RunThreaded(target, thread_params, max_concurrent_threads=200):
  """Runs the target method in parallel threads.

  The method starts up threads with one arg from thread_params as the first arg.

  Args:
    target: The method to invoke in the thread.
    thread_params: A thread is launched for each value in the list. The items
        in the list can either be a singleton or a (args, kwargs) tuple/list.
        Usually this is a list of VMs.
    max_concurrent_threads: The maximum number of concurrent threads to allow.

  Returns:
    List of the same length as thread_params. Contains the return value from
    each threaded function call in the corresponding order as thread_params.

  Raises:
    ValueError: when thread_params is not valid.
    errors.VmUtil.ThreadException: When an exception occurred in any of the
        called functions.

  Example 1: # no args other than list.
    args = [self.CreateVm()
            for x in range(0, 10)]
    RunThreaded(MyThreadedTargetMethod, args)

  Example 2: # using args only to pass to the thread:
    args = [((self.CreateVm(), i, 'somestring'), {})
            for i in range(0, 10)]
    RunThreaded(MyThreadedTargetMethod, args)

  Example 3: # using args & kwargs to pass to the thread:
    args = [((self.CreateVm(),), {'num': i, 'name': 'somestring'})
            for i in range(0, 10)]
    RunThreaded(MyThreadedTargetMethod, args)
  """
  if not isinstance(thread_params, list):
    raise ValueError('Param "thread_params" must be a list')

  if not thread_params:
    # Nothing to do.
    return []

  if not isinstance(thread_params[0], tuple):
    target_arg_tuples = [(target, (arg,), {}) for arg in thread_params]
  elif (not isinstance(thread_params[0][0], tuple) or
        not isinstance(thread_params[0][1], dict)):
    raise ValueError('If Param is a tuple, the tuple must be (tuple, dict)')
  else:
    target_arg_tuples = [(target, args, kwargs)
                         for args, kwargs in thread_params]

  return RunParallelThreads(target_arg_tuples,
                            max_concurrency=max_concurrent_threads)


def RunParallelProcesses(target_arg_tuples, max_concurrency):
  """Executes function calls concurrently in separate processes.

  Args:
    target_arg_tuples: list of (target, args, kwargs) tuples. Each tuple
        contains the function to call and the arguments to pass it.
    max_concurrency: int or None. The maximum number of concurrent new
        processes. If None, it will default to the number of processors on the
        machine.

  Returns:
    list of function return values in the order corresponding to the order of
    target_arg_tuples.

  Raises:
    errors.VmUtil.CalledProcessException: When an exception occurred in any
        of the called functions.
  """
  def handle_sigint(signum, frame):
    # Ignore any SIGINTS in the parent process, but let users know
    # that the child processes are getting cleaned up.
    logging.error('Got SIGINT while executing parallel tasks. '
                  'Waiting for tasks to clean up.')
  old_handler = None
  try:
    old_handler = signal.signal(signal.SIGINT, handle_sigint)
    ret_val = _RunParallelTasks(
        target_arg_tuples, max_concurrency, _BackgroundProcessTaskManager,
        errors.VmUtil.CalledProcessException)
  finally:
    if old_handler:
      signal.signal(signal.SIGINT, old_handler)
  return ret_val
