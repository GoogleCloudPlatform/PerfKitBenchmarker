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

"""Set of utility functions for working with virtual machines."""

from collections import namedtuple
from concurrent import futures
import contextlib
import functools
import logging
import os
import Queue
import random
import re
import string
import subprocess
import tempfile
import threading
import time
import traceback

import jinja2

from perfkitbenchmarker import context
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import log_util
from perfkitbenchmarker import regex_util

FLAGS = flags.FLAGS

PRIVATE_KEYFILE = 'perfkitbenchmarker_keyfile'
PUBLIC_KEYFILE = 'perfkitbenchmarker_keyfile.pub'
CERT_FILE = 'perfkitbenchmarker.pem'
TEMP_DIR = os.path.join(tempfile.gettempdir(), 'perfkitbenchmarker')

# The temporary directory on VMs. We cannot reuse GetTempDir()
# because run_uri will not be available at time of module load and we need
# to use this directory as a base for other module level constants.
VM_TMP_DIR = '/tmp/pkb'

# Default timeout for issuing a command.
DEFAULT_TIMEOUT = 300

# Defaults for retrying commands.
POLL_INTERVAL = 30
TIMEOUT = 1200
FUZZ = .5
MAX_RETRIES = -1

WINDOWS = 'nt'
PASSWORD_LENGTH = 15

OUTPUT_STDOUT = 0
OUTPUT_STDERR = 1
OUTPUT_EXIT_CODE = 2

flags.DEFINE_integer('default_timeout', TIMEOUT, 'The default timeout for '
                     'retryable commands in seconds.')
flags.DEFINE_integer('burn_cpu_seconds', 0,
                     'Amount of time in seconds to burn cpu on vm before '
                     'starting benchmark')
flags.DEFINE_integer('burn_cpu_threads', 1, 'Number of threads to use to '
                     'burn cpu before starting benchmark.')
flags.DEFINE_integer('background_cpu_threads', None,
                     'Number of threads of background cpu usage while '
                     'running a benchmark')
flags.DEFINE_integer('background_network_mbits_per_sec', None,
                     'Number of megabits per second of background '
                     'network traffic to generate during the run phase '
                     'of the benchmark')


class IpAddressSubset(object):
  """Enum of options for --ip_addresses."""
  REACHABLE = 'REACHABLE'
  BOTH = 'BOTH'
  INTERNAL = 'INTERNAL'
  EXTERNAL = 'EXTERNAL'

  ALL = (REACHABLE, BOTH, INTERNAL, EXTERNAL)

flags.DEFINE_enum('ip_addresses', IpAddressSubset.REACHABLE,
                  IpAddressSubset.ALL,
                  'For networking tests: use both internal and external '
                  'IP addresses (BOTH), external and internal only if '
                  'the receiving VM is reachable by internal IP (REACHABLE), '
                  'external IP only (EXTERNAL) or internal IP only (INTERNAL)')

flags.DEFINE_enum('background_network_ip_type', IpAddressSubset.EXTERNAL,
                  (IpAddressSubset.INTERNAL, IpAddressSubset.EXTERNAL),
                  'IP address type to use when generating background network '
                  'traffic')


def GetTempDir():
  """Returns the tmp dir of the current run."""
  return os.path.join(TEMP_DIR, 'run_{0}'.format(FLAGS.run_uri))


def PrependTempDir(file_name):
  """Returns the file name prepended with the tmp dir of the current run."""
  return os.path.join(GetTempDir(), file_name)


def GenTempDir():
  """Creates the tmp dir for the current run if it does not already exist."""
  if not os.path.exists(GetTempDir()):
    os.makedirs(GetTempDir())


def SSHKeyGen():
  """Create PerfKitBenchmarker SSH keys in the tmp dir of the current run."""
  if not os.path.isdir(GetTempDir()):
    GenTempDir()

  if not os.path.isfile(GetPrivateKeyPath()):
    create_cmd = ['ssh-keygen',
                  '-t',
                  'rsa',
                  '-N',
                  '',
                  '-q',
                  '-f',
                  PrependTempDir(PRIVATE_KEYFILE)]
    shell_value = RunningOnWindows()
    create_process = subprocess.Popen(create_cmd,
                                      shell=shell_value,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
    create_process.communicate()

  if not os.path.isfile(GetCertPath()):
    create_cmd = ['openssl',
                  'req',
                  '-x509',
                  '-new',
                  '-out',
                  PrependTempDir(CERT_FILE),
                  '-key',
                  PrependTempDir(PRIVATE_KEYFILE)]
    shell_value = RunningOnWindows()
    create_process = subprocess.Popen(create_cmd,
                                      shell=shell_value,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE,
                                      stdin=subprocess.PIPE)
    create_process.communicate(input='\n' * 7)


def GetPrivateKeyPath():
  return PrependTempDir(PRIVATE_KEYFILE)


def GetPublicKeyPath():
  return PrependTempDir(PUBLIC_KEYFILE)


def GetCertPath():
  return PrependTempDir(CERT_FILE)


def GetSshOptions(ssh_key_filename):
  """Return common set of SSH and SCP options."""
  options = [
      '-2',
      '-o', 'UserKnownHostsFile=/dev/null',
      '-o', 'StrictHostKeyChecking=no',
      '-o', 'IdentitiesOnly=yes',
      '-o', 'PreferredAuthentications=publickey',
      '-o', 'PasswordAuthentication=no',
      '-o', 'ConnectTimeout=5',
      '-o', 'GSSAPIAuthentication=no',
      '-o', 'ServerAliveInterval=30',
      '-o', 'ServerAliveCountMax=10',
      '-i', ssh_key_filename
  ]
  options.extend(FLAGS.ssh_options)
  if FLAGS.log_level == 'debug':
    options.append('-v')

  return options


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


# Result of a call executed by RunParallelThreads.
#
# Attributes:
#   call_id: int. Index corresponding to the call in the target_arg_tuples
#       argument of RunParallelThreads.
#   return_value: Return value if the call was executed successfully, or None if
#       an exception was raised.
#   traceback: None if the call was executed successfully, or the traceback
#       string if the call raised an exception.
ThreadCallResult = namedtuple('ThreadCallResult', [
    'call_id', 'return_value', 'traceback'])


def _ExecuteThreadCall(target_arg_tuple, call_id, queue, parent_log_context,
                       parent_benchmark_spec):
  """Function invoked in another thread by RunParallelThreads.

  Executes a specified function call and captures the traceback upon exception.

  Args:
    target_arg_tuple: (target, args, kwargs) tuple containing the function to
        call and the arguments to pass it.
    call_id: int. Index corresponding to the call in the thread_params argument
        of RunParallelThreads.
    queue: Queue. Receives a ThreadCallResult.
    parent_log_context: ThreadLogContext of the parent thread.
    parent_benchmark_spec: BenchmarkSpec of the parent thread.
  """
  target, args, kwargs = target_arg_tuple
  try:
    log_context = log_util.ThreadLogContext(parent_log_context)
    log_util.SetThreadLogContext(log_context)
    context.SetThreadBenchmarkSpec(parent_benchmark_spec)
    queue.put(ThreadCallResult(call_id, target(*args, **kwargs), None))
  except:
    queue.put(ThreadCallResult(call_id, None, traceback.format_exc()))


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
  queue = Queue.Queue()
  log_context = log_util.GetThreadLogContext()
  benchmark_spec = context.GetThreadBenchmarkSpec()
  max_concurrency = min(max_concurrency, len(target_arg_tuples))
  results = [None] * len(target_arg_tuples)
  error_strings = []
  for call_id in xrange(max_concurrency):
    target_arg_tuple = target_arg_tuples[call_id]
    thread = threading.Thread(
        target=_ExecuteThreadCall,
        args=(target_arg_tuple, call_id, queue, log_context, benchmark_spec))
    thread.daemon = True
    thread.start()
  active_thread_count = max_concurrency
  next_call_id = max_concurrency
  while active_thread_count:
    try:
      # Using a timeout makes this wait interruptable.
      call_id, result, stacktrace = queue.get(block=True, timeout=1000)
    except Queue.Empty:
      continue
    results[call_id] = result
    if stacktrace:
      msg = ('Exception occurred while calling {0}:{1}{2}'.format(
          _GetCallString(target_arg_tuples[call_id]), os.linesep, stacktrace))
      logging.error(msg)
      error_strings.append(msg)
    if next_call_id == len(target_arg_tuples):
      active_thread_count -= 1
    else:
      target_arg_tuple = target_arg_tuples[next_call_id]
      thread = threading.Thread(
          target=_ExecuteThreadCall,
          args=(target_arg_tuple, next_call_id, queue, log_context,
                benchmark_spec))
      thread.daemon = True
      thread.start()
      next_call_id += 1
  if error_strings:
    raise errors.VmUtil.ThreadException(
        'The following exceptions occurred during threaded execution:'
        '{0}{1}'.format(os.linesep, os.linesep.join(error_strings)))
  return results


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


def _ExecuteProcCall(target_arg_tuple):
  """Function invoked in another process by RunParallelProcesses.

  Executes a specified function call and captures the traceback upon exception.
  TODO(skschneider): Remove this helper function when moving to Python 3.5 or
  when the backport of concurrent.futures.ProcessPoolExecutor is able to
  preserve original traceback.

  Args:
    target_arg_tuple: (target, args, kwargs) tuple containing the function to
        call and the arguments to pass it.

  Returns:
    (result, traceback) tuple. The first element is the return value from the
    called function, or None if the function raised an exception. The second
    element is the exception traceback string, or None if the function
    succeeded.
  """
  target, args, kwargs = target_arg_tuple
  try:
    return target(*args, **kwargs), None
  except:
    return None, traceback.format_exc()


def RunParallelProcesses(target_arg_tuples, max_concurrency=None):
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
  call_futures = []
  results = []
  error_strings = []
  with futures.ProcessPoolExecutor(max_workers=max_concurrency) as executor:
    for target_arg_tuple in target_arg_tuples:
      call_futures.append(executor.submit(_ExecuteProcCall, target_arg_tuple))
    for index, future in enumerate(call_futures):
      try:
        result, stacktrace = future.result()
      except:
        result = None
        stacktrace = traceback.format_exc()
      results.append(result)
      if stacktrace:
        msg = ('Exception occurred while calling {0}:{1}{2}'.format(
            _GetCallString(target_arg_tuples[index]), os.linesep, stacktrace))
        logging.error(msg)
        error_strings.append(msg)
  if error_strings:
    msg = ('The following exceptions occurred during parallel execution:'
           '{0}{1}'.format(os.linesep, os.linesep.join(error_strings)))
    raise errors.VmUtil.CalledProcessException(msg)
  return results


def Retry(poll_interval=POLL_INTERVAL, max_retries=MAX_RETRIES,
          timeout=None, fuzz=FUZZ, log_errors=True,
          retryable_exceptions=None):
  """A function decorator that will retry when exceptions are thrown.

  Args:
    poll_interval: The time between tries in seconds. This is the maximum poll
        interval when fuzz is specified.
    max_retries: The maximum number of retries before giving up. If -1, this
        means continue until the timeout is reached. The function will stop
        retrying when either max_retries is met or timeout is reached.
    timeout: The timeout for all tries in seconds. If -1, this means continue
        until max_retries is met. The function will stop retrying when either
        max_retries is met or timeout is reached.
    fuzz: The ammount of randomness in the sleep time. This is used to
        keep threads from all retrying at the same time. At 0, this
        means sleep exactly poll_interval seconds. At 1, this means
        sleep anywhere from 0 to poll_interval seconds.
    log_errors: A boolean describing whether errors should be logged.
    retryable_exceptions: A tuple of exceptions that should be retried. By
        default, this is None, which indicates that all exceptions should
        be retried.

  Returns:
    A function that wraps functions in retry logic. It can be
        used as a decorator.
  """
  if retryable_exceptions is None:
    retryable_exceptions = Exception

  def Wrap(f):
    """Wraps the supplied function with retry logic."""
    def WrappedFunction(*args, **kwargs):
      """Holds the retry logic."""
      local_timeout = FLAGS.default_timeout if timeout is None else timeout

      if local_timeout >= 0:
        deadline = time.time() + local_timeout
      else:
        deadline = float('inf')

      tries = 0
      while True:
        try:
          tries += 1
          return f(*args, **kwargs)
        except retryable_exceptions as e:
          fuzz_multiplier = 1 - fuzz + random.random() * fuzz
          sleep_time = poll_interval * fuzz_multiplier
          if ((time.time() + sleep_time) >= deadline or
              (max_retries >= 0 and tries > max_retries)):
            raise e
          else:
            if log_errors:
              logging.error('Got exception running %s: %s', f.__name__, e)
            time.sleep(sleep_time)
    return WrappedFunction
  return Wrap


def IssueCommand(cmd, force_info_log=False, suppress_warning=False,
                 env=None, timeout=DEFAULT_TIMEOUT, input=None):
  """Tries running the provided command once.

  Args:
    cmd: A list of strings such as is given to the subprocess.Popen()
        constructor.
    force_info_log: A boolean indicating whether the command result should
        always be logged at the info level. Command results will always be
        logged at the debug level if they aren't logged at another level.
    suppress_warning: A boolean indicating whether the results should
        not be logged at the info level in the event of a non-zero
        return code. When force_info_log is True, the output is logged
        regardless of suppress_warning's value.
    env: A dict of key/value strings, such as is given to the subprocess.Popen()
        constructor, that contains environment variables to be injected.
    timeout: Timeout for the command in seconds. If the command has not finished
        before the timeout is reached, it will be killed. Set timeout to None to
        let the command run indefinitely. If the subprocess is killed, the
        return code will indicate an error, and stdout and stderr will
        contain what had already been written to them before the process was
        killed.

  Returns:
    A tuple of stdout, stderr, and retcode from running the provided command.
  """
  logging.debug('Environment variables: %s' % env)

  full_cmd = ' '.join(cmd)
  logging.info('Running: %s', full_cmd)

  shell_value = RunningOnWindows()
  process = subprocess.Popen(cmd, env=env, shell=shell_value,
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

  def _KillProcess():
    logging.error('IssueCommand timed out after %d seconds. '
                  'Killing command "%s".', timeout, full_cmd)
    process.kill()

  timer = threading.Timer(timeout, _KillProcess)
  timer.start()

  try:
    stdout, stderr = process.communicate(input)
  finally:
    timer.cancel()

  stdout = stdout.decode('ascii', 'ignore')
  stderr = stderr.decode('ascii', 'ignore')

  debug_text = ('Ran %s. Got return code (%s).\nSTDOUT: %s\nSTDERR: %s' %
                (full_cmd, process.returncode, stdout, stderr))
  if force_info_log or (process.returncode and not suppress_warning):
    logging.info(debug_text)
  else:
    logging.debug(debug_text)

  return stdout, stderr, process.returncode


def IssueBackgroundCommand(cmd, stdout_path, stderr_path, env=None):
  """Run the provided command once in the background.

  Args:
    cmd: Command to be run, as expected by subprocess.Popen.
    stdout_path: Redirect stdout here. Overwritten.
    stderr_path: Redirect stderr here. Overwritten.
    env: A dict of key/value strings, such as is given to the subprocess.Popen()
        constructor, that contains environment variables to be injected.
  """
  logging.debug('Environment variables: %s' % env)

  full_cmd = ' '.join(cmd)
  logging.info('Spawning: %s', full_cmd)
  outfile = open(stdout_path, 'w')
  errfile = open(stderr_path, 'w')
  shell_value = RunningOnWindows()
  subprocess.Popen(cmd, env=env, shell=shell_value,
                   stdout=outfile, stderr=errfile, close_fds=True)


@Retry()
def IssueRetryableCommand(cmd, env=None):
  """Tries running the provided command until it succeeds or times out.

  Args:
    cmd: A list of strings such as is given to the subprocess.Popen()
        constructor.
    env: An alternate environment to pass to the Popen command.

  Returns:
    A tuple of stdout and stderr from running the provided command.
  """
  stdout, stderr, retcode = IssueCommand(cmd, env=env)
  if retcode:
    raise errors.VmUtil.CalledProcessException(
        'Command returned a non-zero exit code.\n')
  return stdout, stderr


def ParseTimeCommandResult(command_result):
  """Parse command result and get time elapsed.

  Args:
     command_result: The result after executing a remote time command.

  Returns:
     Time taken for the command.
  """
  time_data = re.findall(r'real\s+(\d+)m(\d+.\d+)', command_result)
  time_in_seconds = 60 * float(time_data[0][0]) + float(time_data[0][1])
  return time_in_seconds



def ShouldRunOnExternalIpAddress():
  """Returns whether a test should be run on an instance's external IP."""
  return FLAGS.ip_addresses in (IpAddressSubset.EXTERNAL,
                                IpAddressSubset.BOTH,
                                IpAddressSubset.REACHABLE)


def ShouldRunOnInternalIpAddress(sending_vm, receiving_vm):
  """Returns whether a test should be run on an instance's internal IP.

  Based on the command line flag --ip_addresses. Internal IP addresses are used
  when:

  * --ip_addresses=BOTH or --ip-addresses=INTERNAL
  * --ip_addresses=REACHABLE and 'sending_vm' can ping 'receiving_vm' on its
    internal IP.

  Args:
    sending_vm: VirtualMachine. The client.
    receiving_vm: VirtualMachine. The server.

  Returns:
    Whether a test should be run on an instance's internal IP.
  """
  return (FLAGS.ip_addresses in (IpAddressSubset.BOTH,
                                 IpAddressSubset.INTERNAL) or
          (FLAGS.ip_addresses == IpAddressSubset.REACHABLE and
           sending_vm.IsReachable(receiving_vm)))


def GetLastRunUri():
  """Returns the last run_uri used (or None if it can't be determined)."""
  if RunningOnWindows():
    cmd = ['powershell', '-Command',
           'gci %s | sort LastWriteTime | select -last 1' % TEMP_DIR]
  else:
    cmd = ['bash', '-c', 'ls -1t %s | head -1' % TEMP_DIR]
  stdout, _, _ = IssueCommand(cmd)
  try:
    return regex_util.ExtractGroup('run_([^\s]*)', stdout)
  except regex_util.NoMatchError:
    return None


@contextlib.contextmanager
def NamedTemporaryFile(prefix='tmp', suffix='', dir=None, delete=True):
  """Behaves like tempfile.NamedTemporaryFile.

  The existing tempfile.NamedTemporaryFile has the annoying property on
  Windows that it cannot be opened a second time while it is already open.
  This makes it impossible to use it with a "with" statement in a cross platform
  compatible way. This serves a similar role, but allows the file to be closed
  within a "with" statement without causing the file to be unlinked until the
  context exits.
  """
  f = tempfile.NamedTemporaryFile(prefix=prefix, suffix=suffix,
                                  dir=dir, delete=False)
  try:
    yield f
  finally:
    if not f.closed:
      f.close()
    if delete:
      os.unlink(f.name)


def GenerateSSHConfig(benchmark_spec):
  """Generates an SSH config file to simplify connecting to "vms".

  Writes a file to GetTempDir()/ssh_config with SSH configuration for each VM in
  'vms'.  Users can then SSH with any of the following:

      ssh -F <ssh_config_path> <vm_name>
      ssh -F <ssh_config_path> vm<vm_index>
      ssh -F <ssh_config_path> <group_name>-<index>

  Args:
    benchmark_spec: Benchmark specification.
  """
  target_file = os.path.join(GetTempDir(), 'ssh_config')
  template_path = data.ResourcePath('ssh_config.j2')
  environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
  with open(template_path) as fp:
    template = environment.from_string(fp.read())
  with open(target_file, 'w') as ofp:
    ofp.write(template.render({'vms': benchmark_spec.vms,
                               'vm_groups': benchmark_spec.vm_groups}))

  ssh_options = ['  ssh -F {0} {1}'.format(target_file, pattern)
                 for pattern in ('<vm_name>', 'vm<index>',
                                 '<group_name>-<index>')]
  logging.info('ssh to VMs in this benchmark by name with:\n%s',
               '\n'.join(ssh_options))


def RunningOnWindows():
  """Returns True if PKB is running on Windows."""
  return os.name == WINDOWS


def ExecutableOnPath(executable_name):
  """Return True if the given executable can be found on the path."""
  cmd = ['where'] if RunningOnWindows() else ['which']
  cmd.append(executable_name)

  shell_value = RunningOnWindows()
  process = subprocess.Popen(cmd,
                             shell=shell_value,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
  process.communicate()

  if process.returncode:
    return False
  return True


def GenerateRandomWindowsPassword(password_length=PASSWORD_LENGTH):
  """Generates a password that meets Windows complexity requirements."""
  # The special characters have to be recognized by the Azure CLI as
  # special characters. This greatly limits the set of characters
  # that we can safely use. See
  # https://github.com/Azure/azure-xplat-cli/blob/master/lib/commands/arm/vm/vmOsProfile._js#L145
  special_chars = '*!@#$%+='
  password = [
      random.choice(string.ascii_letters + string.digits + special_chars)
      for _ in range(password_length - 4)]
  # Ensure that the password contains at least one of each 4 required
  # character types.
  password.append(random.choice(string.ascii_lowercase))
  password.append(random.choice(string.ascii_uppercase))
  password.append(random.choice(string.digits))
  password.append(random.choice(special_chars))
  return ''.join(password)
