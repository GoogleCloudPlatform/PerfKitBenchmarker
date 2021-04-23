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

# -*- coding: utf-8 -*-

"""Waits for a command started by execute_command.py to complete.

Blocks until a command wrapped by "execute_command.py" completes, then mimics
the wrapped command, copying the wrapped command's stdout/stderr to this
process' stdout/stderr, and exiting with the wrapped command's status.

If passed the path to the status file, but not the stdout and stderr files,
this script will block until command completion, then print
'Command finished.' before returning with status 0.

*Runs on the guest VM. Supports Python 3.x.*
"""

import errno
import fcntl
import optparse
import os
import shutil
import signal
import sys
import threading
import time

WAIT_TIMEOUT_IN_SEC = 120.0
WAIT_SLEEP_IN_SEC = 5.0
RETRYABLE_SSH_RETCODE = 255


def signal_handler(signum, frame):
  # Pre python3.5 the interruption of a system call would automatically raise
  # an InterruptedError exception, but since PEP 475 was implemented for fcntl
  # interruptions are automatically retried; this implementation depends on
  # interrupting the attempt to acquire a lock on the status file, so we can
  # ensure this in all python3 versions by raising it explicitly in the signal
  # handler.
  raise InterruptedError()


def main():
  p = optparse.OptionParser()
  p.add_option('-o', '--stdout', dest='stdout',
               help="""Read stdout from FILE.""", metavar='FILE')
  p.add_option('-e', '--stderr', dest='stderr',
               help="""Read stderr from FILE.""", metavar='FILE')
  p.add_option('-s', '--status', dest='status', metavar='FILE',
               help='Get process exit status from FILE. '
               'Will block until a shared lock is acquired on FILE.')
  p.add_option('-d', '--delete', dest='delete', action='store_true',
               help='Delete stdout, stderr, and status files when finished.')
  p.add_option(
      '-x',
      '--exclusive',
      dest='exclusive',
      help='Will block until FILE exists to ensure that status is ready to be '
      'read. Required.',
      metavar='FILE')
  options, args = p.parse_args()
  if args:
    sys.stderr.write('Unexpected arguments: {0}\n'.format(args))
    return 1

  missing = []
  for option in ('status', 'exclusive'):
    if getattr(options, option) is None:
      missing.append(option)

  if missing:
    p.print_usage()
    msg = 'Missing required flag(s): {0}\n'.format(
        ', '.join('--' + i for i in missing))
    sys.stderr.write(msg)
    return 1

  start = time.time()
  return_code_str = None
  while time.time() < WAIT_TIMEOUT_IN_SEC + start:
    try:
      with open(options.exclusive, 'r'):
        with open(options.status, 'r'):
          break
    except IOError as e:
      print('WARNING: file doesn\'t exist, retrying: %s' % e, file=sys.stderr)
      time.sleep(WAIT_SLEEP_IN_SEC)

  # Set a signal handler to raise an InterruptedError on SIGALRM (this is no
  # longer done automatically after PEP 475).
  signal.signal(signal.SIGALRM, signal_handler)
  # Send a SIGALRM signal after WAIT_TIMEOUT_IN_SEC seconds
  signal.alarm(int(WAIT_TIMEOUT_IN_SEC))
  with open(options.status, 'r') as status:
    try:
      # If we can acquire the lock on status, the command we're waiting on is
      # done; if we can't acquire it for the next WAIT_TIMEOUT_IN_SEC seconds
      # this attempt will be interrupted and we'll catch an InterruptedError.
      fcntl.lockf(status, fcntl.LOCK_SH)
    except InterruptedError:
      print('Wait timed out. This will be retried with a subsequent wait.')
      return 0
    # OSError and IOError have similar interfaces, and later versions of fcntl
    # will raise OSError where earlier versions raised IOError--we catch both
    # here for compatibility.
    except (OSError, IOError) as e:
      if e.errno == errno.ECONNREFUSED:
        print('Connection refused during wait. '
              'This will be retried with a subsequent wait.')
        return 0
      elif e.errno in (errno.EAGAIN, errno.EACCES):
        print('Status currently being modified and cannot be read right now. '
              'This will be retried with a subsequent wait.')
        return 0
      raise e
    signal.alarm(0)
    return_code_str = status.read()

  if not (options.stdout and options.stderr):
    print('Command finished.')
    return 0

  # Some commands write out non UTF-8 control characters. Replace them with '?'
  # to make Python 3.6+ happy.
  with open(options.stdout, 'r', errors='backslashreplace') as stdout:
    with open(options.stderr, 'r', errors='backslashreplace') as stderr:
      if return_code_str:
        return_code = int(return_code_str)
      else:
        print('WARNING: wrapper script interrupted.', file=sys.stderr)
        return_code = 1

      # RemoteCommand retries 255 as temporary SSH failure. In this case,
      # long running command actually returned 255 and should not be retried.
      if return_code == RETRYABLE_SSH_RETCODE:
        print('WARNING: command returned 255.', file=sys.stderr)
        return_code = 1

      stderr_copier = threading.Thread(target=shutil.copyfileobj,
                                       args=[stderr, sys.stderr],
                                       name='stderr-copier')
      stderr_copier.daemon = True
      stderr_copier.start()
      try:
        shutil.copyfileobj(stdout, sys.stdout)
      finally:
        stderr_copier.join()

  if options.delete:
    for f in [options.stdout, options.stderr, options.status]:
      os.unlink(f)

  return return_code

if __name__ == '__main__':
  sys.exit(main())
