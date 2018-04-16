#!/usr/bin/env python2
#
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

*Runs on the guest VM. Supports Python 2.6, 2.7, and 3.x.*
"""

from __future__ import print_function

import fcntl
import optparse
import os
import shutil
import sys
import threading
import time

WAIT_TIMEOUT_IN_SEC = 120.0
WAIT_SLEEP_IN_SEC = 5.0


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
  options, args = p.parse_args()
  if args:
    sys.stderr.write('Unexpected arguments: {0}\n'.format(args))
    return 1

  missing = []
  for option in ('status',):
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
  while (time.time() < WAIT_TIMEOUT_IN_SEC + start):
    try:
      with open(options.status, 'r') as status:
        fcntl.lockf(status, fcntl.LOCK_SH)
        return_code_str = status.read()
        break
    except IOError:
      print('WARNING: file doesn\'t exist, retrying', file=sys.stderr)
      time.sleep(WAIT_SLEEP_IN_SEC)

  if not (options.stdout and options.stderr):
    print('Command finished.')
    return 0

  with open(options.stdout, 'r') as stdout:
    with open(options.stderr, 'r') as stderr:
      if return_code_str:
        return_code = int(return_code_str)
      else:
        print('WARNING: wrapper script interrupted.', file=sys.stderr)
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
