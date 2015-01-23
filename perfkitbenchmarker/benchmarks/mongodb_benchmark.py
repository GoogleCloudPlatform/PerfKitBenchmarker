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

"""Run YCSB against MongoDB.

YCSB is a load generator for many 'cloud' databases. MongoDB is a NoSQL
database.

MongoDB homepage: http://www.mongodb.org/
YCSB homepage: https://github.com/brianfrankcooper/YCSB/wiki
"""

import logging
from perfkitbenchmarker import regex_util
from perfkitbenchmarker.packages import ycsb

YCSB_CMD = ('cd %s; ./bin/ycsb %s mongodb -s -P workloads/workloada '
            '-threads 10 -p mongodb.url=mongodb://%s:27017 '
            '-p mongodb.writeConcern=normal')

BENCHMARK_INFO = {'name': 'mongodb',
                  'description': 'Run YCSB against MongoDB.',
                  'scratch_disk': False,
                  'num_machines': 2}

RESULT_REGEX = r'\[(\w+)\], (\w+)\(([\w/]+)\), ([-+]?[0-9]*\.?[0-9]+)'
OPERATIONS_REGEX = r'\[(\w+)\], Operations, ([-+]?[0-9]*\.?[0-9]+)'


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Install MongoDB on one VM and YCSB on another.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  assert len(vms) == BENCHMARK_INFO['num_machines']
  vm = vms[0]

  # Install mongodb on the 1st machine.
  vm.Install('mongodb_server')
  vm.RemoteCommand('sudo sed -i \'/bind_ip/ s/^/#/\' %s' %
                   vm.GetPathToConfig('mongodb_server'))
  vm.RemoteCommand('sudo service %s restart' %
                   vm.GetServiceName('mongodb_server'))

  # Setup YCSB load generator on the 2nd machine.
  vm = vms[1]
  vm.Install('ycsb')
  vm.RemoteCommand(YCSB_CMD % (ycsb.YCSB_DIR, 'load', vms[0].internal_ip))


def ParseResults(output):
  """Parse YCSB output.

  Sample Output:
  [OVERALL], RunTime(ms), 723.0
  [OVERALL], Throughput(ops/sec), 1383.1258644536654
  [UPDATE], Operations, 496
  [UPDATE], AverageLatency(us), 5596.689516129032
  [UPDATE], MinLatency(us), 2028
  [UPDATE], MaxLatency(us), 46240
  [UPDATE], 95thPercentileLatency(ms), 10
  [UPDATE], 99thPercentileLatency(ms), 43
  [UPDATE], Return=0, 496

  Args:
    output: String output of YCSB tool from commandline.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  samples = []
  result_match = regex_util.ExtractAllMatches(RESULT_REGEX, output)
  for groups in result_match:
    samples.append(
        [groups[1], float(groups[3]), groups[2], {'stage': groups[0]}])
  operations_match = regex_util.ExtractAllMatches(OPERATIONS_REGEX, output)
  for groups in operations_match:
    samples.append(['Operations', float(groups[1]), '', {'stage': groups[0]}])
  return samples


def Run(benchmark_spec):
  """Run run YCSB with workloada against MongoDB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  vms = benchmark_spec.vms
  vm = vms[1]
  # TODO(user): We are running workloada with default parameters.
  #    This does not seem like a rigorous benchmark.
  logging.info('MongoDb Results:')
  ycsb_cmd = YCSB_CMD % (ycsb.YCSB_DIR, 'run', vms[0].internal_ip)
  stdout, _ = vm.RemoteCommand(ycsb_cmd, should_log=True)
  return ParseResults(stdout)


def Cleanup(benchmark_spec):
  """Remove MongoDb and YCSB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
