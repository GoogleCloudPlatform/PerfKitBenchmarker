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

"""Runs plain vanilla bonnie++."""

import copy
import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'bonnie++',
                  'description': 'Runs Bonnie++.',
                  'scratch_disk': True,
                  'num_machines': 1}

LATENCY_REGEX = r'([0-9]*\.?[0-9]+)(\w+)'


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Install Bonnie++ on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('Bonnie++ prepare on %s', vm)
  vm.Install('bonnieplusplus')


def IsValueValid(value):
  """Validate the value.

  An invalid value is either an empty string or a string of multiple '+'.

  Args:
    value: The value in raw result.

  Returns:
    A boolean indicate if the value is valid or not.
  """
  if value == '' or '+' in value:
    return False
  return True


def ParseLatencyResult(result):
  """Parse latency result into value and unit.

  Args:
    result: Latency result in string format, contains both value and unit.
            eg. 200ms

  Returns:
    A tuple of value (float) and unit (string).
  """
  match = regex_util.ExtractAllMatches(LATENCY_REGEX, result)[0]
  return float(match[0]), match[1]


def ParseCSVResults(results):
  """Parse csv format bonnie++ results.

  Sample Results:
    1.96,1.96,perfkit-7b22f510-0,1,1421800799,7423M,,,,72853,15,47358,5,,,
    156821,7,537.7,10,100,,,,,49223,58,+++++,+++,54405,53,2898,97,+++++,+++,
    59089,60,,512ms,670ms,,44660us,200ms,3747us,1759us,1643us,33518us,192us,
    839us

  Args:
    results: bonnie++ results.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  results = results.split(',')
  samples = []
  metadata = {'version': results[0],
              'test_size': results[5]}
  if IsValueValid(results[3]):
    metadata['chunk_size'] = results[3]
  if IsValueValid(results[7]):
    samples.extend(['Sequential Output:Per Char:Throughput',
                    float(results[7]), 'K/sec', metadata])
  if IsValueValid(results[8]):
    samples.extend(['Sequential Output:Per Char:Cpu Percentage',
                    float(results[8]), '%', metadata])
  if IsValueValid(results[36]):
    value, unit = ParseLatencyResult(results[36])
    samples.extend(['Sequential Output:Per Char:Latency',
                    value, unit, metadata])
  if IsValueValid(results[9]):
    samples.extend(['Sequential Output:Block:Throughput',
                    float(results[9]), 'K/sec', metadata])
  if IsValueValid(results[10]):
    samples.extend(['Sequential Output:Block:Cpu Percentage',
                    float(results[10]), '%', metadata])
  if IsValueValid(results[37]):
    value, unit = ParseLatencyResult(results[37])
    samples.extend(['Sequential Output:Block:Latency',
                    value, unit, metadata])
  if IsValueValid(results[11]):
    samples.extend(['Sequential Output:Rewrite:Throughput',
                    float(results[11]), 'K/sec', metadata])
  if IsValueValid(results[12]):
    samples.extend(['Sequential Output:Rewrite:Cpu Percentage',
                    float(results[12]), '%', metadata])
  if IsValueValid(results[38]):
    value, unit = ParseLatencyResult(results[38])
    samples.extend(['Sequential Output:Rewrite:Latency',
                    value, unit, metadata])
  if IsValueValid(results[13]):
    samples.extend(['Sequential Input:Per Char:Throughput',
                    float(results[13]), 'K/sec', metadata])
  if IsValueValid(results[14]):
    samples.extend(['Sequential Input:Per Char:Cpu Percentage',
                    float(results[14]), '%', metadata])
  if IsValueValid(results[39]):
    value, unit = ParseLatencyResult(results[39])
    samples.extend(['Sequential Iutput:Per Char:Latency',
                    value, unit, metadata])
  if IsValueValid(results[15]):
    samples.extend(['Sequential Input:Block:Throughput',
                    float(results[15]), 'K/sec', metadata])
  if IsValueValid(results[16]):
    samples.extend(['Sequential Input:Block:Cpu Percentage',
                    float(results[16]), '%', metadata])
  if IsValueValid(results[40]):
    value, unit = ParseLatencyResult(results[40])
    samples.extend(['Sequential Iutput:Block:Latency',
                    value, unit, metadata])
  if IsValueValid(results[17]):
    samples.extend(['Random Seeks:Throughput',
                    float(results[17]), 'K/sec', metadata])
  if IsValueValid(results[18]):
    samples.extend(['Random Seeks:Cpu Percentage',
                    float(results[18]), '%', metadata])
  if IsValueValid(results[41]):
    value, unit = ParseLatencyResult(results[41])
    samples.extend(['Random Seeks:Latency',
                    value, unit, metadata])
  metadata = copy.deepcopy(metadata)
  if IsValueValid(results[19]):
    metadata['num_files'] = results[19]
  if IsValueValid(results[20]):
    metadata['max_size'] = results[20]
  if IsValueValid(results[21]):
    metadata['min_size'] = results[21]
  if IsValueValid(results[22]):
    metadata['num_dirs'] = results[22]
  if IsValueValid(results[23]):
    metadata['chunk_size'] = results[23]
  if IsValueValid(results[24]):
    samples.extend(['Sequential Create:Create:Throughput',
                    float(results[24]), 'K/sec', metadata])
  if IsValueValid(results[25]):
    samples.extend(['Sequential Create:Create:Cpu Percentage',
                    float(results[25]), '%', metadata])
  if IsValueValid(results[42]):
    value, unit = ParseLatencyResult(results[42])
    samples.extend(['Sequential Create:Create:Latency',
                    value, unit, metadata])
  if IsValueValid(results[26]):
    samples.extend(['Sequential Create:Read:Throughput',
                    float(results[26]), 'K/sec', metadata])
  if IsValueValid(results[27]):
    samples.extend(['Sequential Create:Read:Cpu Percentage',
                    float(results[27]), '%', metadata])
  if IsValueValid(results[43]):
    value, unit = ParseLatencyResult(results[43])
    samples.extend(['Sequential Create:Read:Latency',
                    value, unit, metadata])
  if IsValueValid(results[28]):
    samples.extend(['Sequential Create:Delete:Throughput',
                    float(results[28]), 'K/sec', metadata])
  if IsValueValid(results[29]):
    samples.extend(['Sequential Create:Delete:Cpu Percentage',
                    float(results[29]), '%', metadata])
  if IsValueValid(results[44]):
    value, unit = ParseLatencyResult(results[44])
    samples.extend(['Sequential Create:Delete:Latency',
                    value, unit, metadata])
  if IsValueValid(results[30]):
    samples.extend(['Random Create:Create:Throughput',
                    float(results[30]), 'K/sec', metadata])
  if IsValueValid(results[31]):
    samples.extend(['Random Create:Create:Cpu Percentage',
                    float(results[31]), '%', metadata])
  if IsValueValid(results[45]):
    value, unit = ParseLatencyResult(results[45])
    samples.extend(['Random Create:Create:Latency',
                    value, unit, metadata])
  if IsValueValid(results[32]):
    samples.extend(['Random Create:Read:Throughput',
                    float(results[32]), 'K/sec', metadata])
  if IsValueValid(results[33]):
    samples.extend(['Random Create:Read:Cpu Percentage',
                    float(results[33]), '%', metadata])
  if IsValueValid(results[46]):
    value, unit = ParseLatencyResult(results[46])
    samples.extend(['Random Create:Read:Latency',
                    value, unit, metadata])
  if IsValueValid(results[34]):
    samples.extend(['Random Create:Delete:Throughput',
                    float(results[34]), 'K/sec', metadata])
  if IsValueValid(results[35]):
    samples.extend(['Random Create:Delete:Cpu Percentage',
                    float(results[35]), '%', metadata])
  if IsValueValid(results[47]):
    value, unit = ParseLatencyResult(results[47])
    samples.extend(['Random Create:Delete:Latency',
                    value, unit, metadata])
  return samples


def Run(benchmark_spec):
  """Run Bonnie++ on the target vm.

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
  vm = vms[0]
  logging.info('Bonnie++ running on %s', vm)
  bonnie_command = ('/usr/sbin/bonnie++ -q -d %s -s %d -n 100 -f' %
                    (vm.GetScratchDir(),
                     2 * vm.total_memory_kb / 1024))
  logging.info('Bonnie++ Results:')
  out, _ = vm.RemoteCommand(bonnie_command, should_log=True)
  return ParseCSVResults(out)


def Cleanup(benchmark_spec):
  """Cleanup Bonnie++ on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
