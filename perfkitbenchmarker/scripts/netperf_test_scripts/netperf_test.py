#!/usr/bin/env python

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

import json
import logging
import subprocess
import sys
import time
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_integer('num_streams', 1, 'Number of netperf processes to run')

flags.DEFINE_string('netperf_cmd', None,
                    'netperf command to run')

flags.DEFINE_integer('port_start', None,
                     'Starting port for netperf command and data ports')


def Main(argv=sys.argv):
  # Parse command-line flags
  try:
    argv = FLAGS(argv)
  except flags.Error as e:
    logging.error('%s\nUsage: %s ARGS\n%s', e, sys.argv[0], FLAGS)
    sys.exit(1)

  netperf_cmd = FLAGS.netperf_cmd
  num_streams = FLAGS.num_streams
  port_start = FLAGS.port_start

  assert netperf_cmd
  assert num_streams >= 1
  assert port_start

  stdouts = [None] * num_streams
  stderrs = [None] * num_streams
  return_codes = [None] * num_streams
  processes = [None] * num_streams

  # Start all of the netperf processes
  begin_starting_processes = time.time()
  for i in range(num_streams):
    command_port = port_start + i * 2
    data_port = port_start + i * 2 + 1
    cmd = netperf_cmd.format(command_port=command_port, data_port=data_port)
    processes[i] = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, shell=True)
  end_starting_processes = time.time()
  # Wait for all of the netperf processes to finish and save their return codes
  for i, process in enumerate(processes):
    stdouts[i], stderrs[i] = process.communicate()
    return_codes[i] = process.returncode
  # Dump the stdouts, stderrs, and return_codes to stdout in json form
  print(json.dumps((stdouts, stderrs, return_codes,
                    begin_starting_processes, end_starting_processes)))

if __name__ == '__main__':
  sys.exit(Main())
