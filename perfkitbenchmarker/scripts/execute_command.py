#!/usr/bin/env python
#
# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs a command, saving stdout, stderr, and the return code in files.

Simplifies executing long-running commands on a remote host.
The status file (as specified by --status) is exclusively locked until the
child process running the user-specified command exits.
This command will fail if the status file cannot be successfully locked.

To await completion, "wait_for_command.py" acquires a shared lock on the
status file, which blocks until the process completes.

*Runs on the guest VM. Supports Python 2.6, 2.7, and 3.x.*
"""

import fcntl
import logging
import optparse
import sys
import subprocess


def main():
  parser = optparse.OptionParser()
  parser.add_option('-o', '--stdout', dest='stdout', metavar='FILE',
                    help="""Write stdout to FILE. Required.""")
  parser.add_option('-e', '--stderr', dest='stderr', metavar='FILE',
                    help="""Write stderr to FILE. Required.""")
  parser.add_option('-p', '--pid', dest='pid', help="""Write PID to FILE.""",
                    metavar='FILE')
  parser.add_option('-s', '--status', dest='status', help="""Write process exit
                    status to FILE. An exclusive lock will be placed on FILE
                    until this process exits. Required.""", metavar='FILE')
  parser.add_option('-c', '--command', dest='command', help="""Shell command to
                    execute. Required.""")
  options, args = parser.parse_args()
  if args:
    sys.stderr.write('Unexpected arguments: {0}\n'.format(args))
    return 1

  missing = []
  for option in ('stdout', 'stderr', 'status', 'command'):
    if getattr(options, option) is None:
      missing.append(option)

  if missing:
    parser.print_usage()
    msg = 'Missing required flag(s): {0}\n'.format(
        ', '.join('--' + i for i in missing))
    sys.stderr.write(msg)
    return 1

  with open(options.status, 'w+') as status:
    with open(options.stdout, 'w') as stdout:
      with open(options.stderr, 'w') as stderr:
        logging.info('Acquiring lock on %s', options.status)
        # Non-blocking exclusive lock acquisition; will raise an IOError if
        # acquisition fails, which is desirable here.
        fcntl.lockf(status, fcntl.LOCK_EX | fcntl.LOCK_NB)

        # Initialize the status to 99; will be filled with the exit status
        # on subprocess completion.
        status.write('')
        status.flush()
        status.seek(0)

        p = subprocess.Popen(options.command, stdout=stdout, stderr=stderr,
                             shell=True)
        logging.info('Started pid %d: %s', p.pid, options.command)

        if options.pid:
          with open(options.pid, 'w') as pid:
            pid.write(str(p.pid))

        logging.info('Waiting on PID %s', p.pid)
        return_code = p.wait()
        logging.info('Return code: %s', return_code)
        status.truncate()
        status.write(str(return_code))

        # File lock will be released when the status file is closed.
        return return_code

if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  sys.exit(main())
