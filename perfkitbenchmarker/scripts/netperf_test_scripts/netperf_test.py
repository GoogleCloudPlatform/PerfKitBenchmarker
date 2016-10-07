#!/usr/bin/env python

import io
import json
import subprocess
import sys
import gflags as flags

FLAGS = flags.FLAGS

flags.DEFINE_integer('num_streams', 1, 'Number of netperf processes to run')

flags.DEFINE_string('netperf_cmd', None,
                    'netperf command to run')

def Main(argv=sys.argv):
  # Parse command-line flags
  try:
    argv = FLAGS(argv)
  except flags.FlagsError as e:
    logging.error('%s\nUsage: %s ARGS\n%s', e, sys.argv[0], FLAGS)
    sys.exit(1)

  netperf_cmd = FLAGS.netperf_cmd
  num_streams = FLAGS.num_streams

  assert(netperf_cmd)
  assert(num_streams >= 1)

  stdouts = [None for _ in range(num_streams)]
  stderrs = [None for _ in range(num_streams)]
  return_codes = [None for _ in range(num_streams)]
  processes = [None for _ in range(num_streams)]

  # Start all of the netperf processes
  for i in range(num_streams):
    processes[i] = subprocess.Popen(netperf_cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, shell=True)
  # Wait for all of the netperf processes to finish and save their return codes
  for i, process in enumerate(processes):
    stdouts[i], stderrs[i] = process.communicate()
    return_codes[i] = process.returncode
  # Dump the stdouts, stderrs, and return_codes to stdout in json form
  print(json.dumps((stdouts, stderrs, return_codes)))

if __name__ == '__main__':
  sys.exit(Main())
