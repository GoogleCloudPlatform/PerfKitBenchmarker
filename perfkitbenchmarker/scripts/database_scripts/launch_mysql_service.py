#!/usr/bin/env python

# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""launch_mysql_service is a wrapper class for mysql_service_benchmark.
It will call mysql_service_benchmark with varying thread counts depending on
thread_count_list flag values.

If a run_uri flag is provided, the program assumes that the PKB instance
has been provisioned and prepared, and will only execute the run phase of PKB.
If no run_uri is given, the program will execute the provision and prepare
phase followed by consecutive run phases and then the cleanup and teardown
phase. Below are examples of possible ways to call this class.

Possible call:
./launch_mysql_service --thread_count_list=[1,2,5] --sysbench_run_seconds=20
./launch_mysql_service --run_uri=12e4s6t8 --thread_count_list=[1,2,4,8,10]

All requirements for mysql_service_benchmark still apply. See
perkfitbenchmarker.linuxbenchmarks.mysql_service_benchmark for more details.

Any additional flags not specifically outlined in the flags below can be added
as a list of strings under the 'additional_flags' flag. These flags will be
passed through as is to the underlying benchmarking code. For example,
a possible call with additional flags could be:
./launch_mysql_service --run_uri=2u2u2u3i
  --additional_flags=['--storage_size=100', '--cloud=GCP'].
"""

import datetime
import logging
import re
import shlex
import subprocess
import sys
import time
import gflags

# GLOBAL STRINGS
SYSBENCH_RUN_SECONDS = 'sysbench_run_seconds'
SYSBENCH_THREAD_COUNT = 'sysbench_thread_count'
SYSBENCH_REPORT_INTERVAL = 'sysbench_report_interval'
THREAD_COUNT_LIST = 'thread_count_list'
RUN_URI = 'run_uri'
DEFAULT_RUN_TIME = 60
STDOUT = 'STDOUT'
STDERR = 'STDERR'
DATETIME_FORMAT = '{:%m_%d_%Y_%H_%M_}'
URI_REGEX = r'run_uri=([a-z0-9]{8})'
ADDITIONAL_FLAGS = 'additional_flags'
SLEEP_TIME_BETWEEN_RUNS = 20  # seconds
TAIL_LINE_NUM = '20'

PKB_TIMEOUT = 43200  # max wait time for a run in seconds

# FLAG STRINGS
PKB = './pkb.py --benchmarks=mysql_service'
STAGE_FLAG = ' --run_stage='
URI_FLAG = ' --run_uri='
THREAD_FLAG = ' --sysbench_thread_count='
RUN_TIME = ' --sysbench_run_seconds='
PROVISION = 'provision'
PREPARE = 'prepare'
RUN = 'run'
CLEANUP = 'cleanup'
TEARDOWN = 'teardown'

FLAGS = gflags.FLAGS
gflags.DEFINE_integer(SYSBENCH_RUN_SECONDS, 480,
                      'The duration, in seconds, of each run phase with varying'
                      'thread count.')
gflags.DEFINE_list(THREAD_COUNT_LIST, [1, 2, 4, 8],
                   'The number of test threads on the client side.')
gflags.DEFINE_integer(SYSBENCH_REPORT_INTERVAL, 2,
                      'The interval, in seconds, we ask sysbench to report '
                      'results.')
gflags.DEFINE_string(RUN_URI, None,
                     'Run identifier, if provided, only run phase '
                     'will be completed.')
gflags.DEFINE_list(ADDITIONAL_FLAGS, None,
                   'List of additional PKB mysql_service valid flags (strings).'
                   'For example: ["--storage_size=100"].')

# TODO: Implement flag for STDOUT/STDERR file paths.


class UnexpectedFileOutputError(Exception):
  pass


class OperationTimeoutError(Exception):
  pass


def driver(argv):
  """Driver initiates sysbench run with different thread counts.

  Args:
    argv: system arguments (command line flags).
  """
  try:  # Parse command line flags
    argv = FLAGS(argv)
  except gflags.FlagsError as e:
    logging.error('%s\nUsage: %s ARGS\n%s', e, sys.argv[0], FLAGS)
    sys.exit(1)
  run_uri = FLAGS.run_uri
  if not run_uri:
    logging.info('No run_uri given. Will run full mysql_service_benchmark '
                 'test.')
    run_uri = _provision_prepare_pkb()
    logging.info('Provision and prepare completed. Run uri assigned: %s',
                 run_uri)
  _run(run_uri)
  if not FLAGS.run_uri:
    _cleanup_teardown_pkb(run_uri)


def _provision_prepare_pkb():
  """Run provision and prepare stage of PKB benchmark.

  Returns:
    run_uri: (string)
  """
  pkb_cmd = PKB + STAGE_FLAG + PROVISION + ',' + PREPARE
  if FLAGS.additional_flags:
    pkb_cmd = _append_additional_flags(pkb_cmd)
  # PKB run with prepare,provision, wait
  logging.info('Provision and prepare sysbench with the following command:\n%s',
               pkb_cmd)
  [stdout_filename, stderr_filename] = _generate_filenames(PROVISION, None)
  _execute_pkb_cmd(pkb_cmd, stdout_filename, stderr_filename)
  return _get_run_uri(stderr_filename)


def _run(run_uri):
  """Run stage of PKB benchmark.

  Args:
    run_uri: (string).
  """

  run_iterations = len(FLAGS.thread_count_list)
  logging.info(
      'Beginning run phase. Will execute runs with %d different thread counts.',
      run_iterations)
  for t in FLAGS.thread_count_list:
    pkb_cmd = (PKB + STAGE_FLAG + RUN + URI_FLAG + run_uri + THREAD_FLAG +
               str(t) + RUN_TIME + str(FLAGS.sysbench_run_seconds) + ' &')
    if FLAGS.additional_flags:
      pkb_cmd = _append_additional_flags(pkb_cmd)
    stdout_filename, stderr_filename = _generate_filenames(RUN, t)
    logging.info('Executing PKB run with thread count: %i', t)
    _execute_pkb_cmd(pkb_cmd, stdout_filename, stderr_filename)
    logging.info('Finished executing PKB with thread count: %i', t)
    time.sleep(SLEEP_TIME_BETWEEN_RUNS)


def _cleanup_teardown_pkb(run_uri):
  """Run cleanup stage of PKB benchmark.

  Args:
    run_uri: (string)
  """
  logging.info('Run phase complete. Starting cleanup/teardown.')
  pkb_cmd = (PKB + STAGE_FLAG + CLEANUP + ',' + TEARDOWN + URI_FLAG + run_uri)
  [stdout_filename, stderr_filename] = _generate_filenames(CLEANUP, None)
  _execute_pkb_cmd(pkb_cmd, stdout_filename, stderr_filename)
  logging.info('Finished executing PKB cleanup and teardown.')


def _execute_pkb_cmd(pkb_cmd, stdout_filename, stderr_filename):
  """Given pkb run command, execute.

  Args:
    pkb_cmd: (str)
    stdout_filename: (str) filename string.
    stderr_filename: (str) filename_str
  """
  stdout_file = open(stdout_filename, 'w+')
  stderr_file = open(stderr_filename, 'w+')
  pkb_cmd_list = shlex.split(pkb_cmd)
  logging.info('pkb command list: %s', str(pkb_cmd_list))
  start_time = time.time()
  p = subprocess.Popen(pkb_cmd_list, stdout=stdout_file, stderr=stderr_file)
  logging.info('Waiting for PKB call to finish.')
  # TODO: implement timeout. Currently this call will wait unbounded.
  # Will probably have to implement with threading.
  p.wait()
  elapsed_time = time.time() - start_time
  logging.info('PKB call finished in %d seconds.', elapsed_time)


def _get_run_uri(filename):
  """Grab the last lines of file and return the first match with URI_REGEX.

  Args:
    filename: (string)

  Returns:
    run_uri: (string) Run identifier from file.

  Raises:
    Exception: No match with regular expression. Unexpected output to filename.
  """
  grab_file_tail_cmd = ['tail', '-n', TAIL_LINE_NUM, filename]
  p = subprocess.Popen(
      grab_file_tail_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  lines = p.stdout.readlines()
  r = re.compile(URI_REGEX)
  for line in lines:
    matches = r.search(line)
    if matches:
      return matches.group(matches.lastindex)
  raise UnexpectedFileOutputError('No regex match with %s.' % filename)


def _append_additional_flags(pkb_cmd):
  """Appends additional flags to the end of pkb_cmd.

  Args:
    pkb_cmd: (string) Current pkb command.

  Returns:
    pkb_cmd: (string) PKB command with additional flags.
  """
  for flag in FLAGS.additional_flags:
    pkb_cmd += ' ' + flag
  return pkb_cmd


def _generate_filenames(run_stage, thread_number):
  """Generate filenames for STDOUT and STDERR based on phase and time.

  Args:
    run_stage: Current stage of sysbench.
    thread_number: (int) Number of sysbench threads for run iteration.

  Returns:
    [stdout_filename, stderr_filename]: list of filename strings.
  """
  date_string = DATETIME_FORMAT.format(datetime.datetime.now())
  if run_stage == RUN:
    stdout_filename = date_string + str(
        thread_number) + '_THREAD_RUN' + '_PKB_STDOUT.txt'
    stderr_filename = date_string + str(
        thread_number) + '_THREAD_RUN' + '_PKB_STDERR.txt'
  else:
    stdout_filename = date_string + str(run_stage) + '_PKB_STDOUT.txt'
    stderr_filename = date_string + str(run_stage) + '_PKB_STDERR.txt'
  logging.info('STDOUT will be copied to: %s', stdout_filename)
  logging.info('STDERR will be copied to: %s', stderr_filename)
  return [stdout_filename, stderr_filename]


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  driver(sys.argv)
