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

"""Runs UnixBench.

Documentation & code: http://code.google.com/p/byte-unixbench/

Unix bench is a holistic performance benchmark, measuing CPU performance,
some memory bandwidth, and disk.
"""

import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import unixbench

FLAGS = flags.FLAGS


BENCHMARK_NAME = 'unixbench'
BENCHMARK_CONFIG = """
unixbench:
  description: Runs UnixBench.
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
"""

flags.DEFINE_boolean('unixbench_all_cores', default=False,
                     help='Setting this flag changes the default behavior of '
                     'Unix bench. It will now scale to the number of CPUs on '
                     'the machine vs the limit of 16 CPUs today.')

UNIXBENCH_PATCH_FILE = 'unixbench-16core-limitation.patch'
SYSTEM_SCORE_REGEX = r'\nSystem Benchmarks Index Score\s+([-+]?[0-9]*\.?[0-9]+)'
RESULT_REGEX = (
    r'\n([A-Z][\w\-\(\) ]+)\s+([-+]?[0-9]*\.?[0-9]+) (\w+)\s+\('
    r'([-+]?[0-9]*\.?[0-9]+) (\w+), (\d+) samples\)')
SCORE_REGEX = (
    r'\n([A-Z][\w\-\(\) ]+)\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)'
    r'\s+([-+]?[0-9]*\.?[0-9]+)')
PARALLEL_COPIES_REGEX = r'running (\d+) parallel cop[yies]+ of tests'
RESULT_START_STRING = 'Benchmark Run:'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites():
  """Verifies that the required resources for UnixBench are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """

  if FLAGS.unixbench_all_cores:
    data.ResourcePath(UNIXBENCH_PATCH_FILE)


def Prepare(benchmark_spec):
  """Install Unixbench on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('Unixbench prepare on %s', vm)
  vm.Install('unixbench')

  if FLAGS.unixbench_all_cores:
    vm.PushDataFile(UNIXBENCH_PATCH_FILE)
    vm.RemoteCommand('cp %s %s' %
                     (UNIXBENCH_PATCH_FILE, unixbench.UNIXBENCH_DIR))
    vm.RemoteCommand('cd %s && patch ./Run %s' %
                     (unixbench.UNIXBENCH_DIR, UNIXBENCH_PATCH_FILE))


def ParseResults(results):
  """Result parser for UnixBench.

  Sample Results:
  1 CPUs in system; running 1 parallel copy of tests
  8 CPUs in system; running 8 parallel copies of tests
  Double-Precision Whetstone                    4022.0 MWIPS (9.9 s, 7 samples)
  Execl Throughput                              4735.8 lps   (29.8 s, 2 samples)
  File Copy 1024 bufsize 2000 maxblocks      1294367.0 KBps  (30.0 s, 2 samples)
  File Copy 256 bufsize 500 maxblocks         396912.9 KBps  (30.0 s, 2 samples)
  File Copy 4096 bufsize 8000 maxblocks      2513158.7 KBps  (30.0 s, 2 samples)
  Pipe Throughput                            2221775.6 lps   (10.0 s, 7 samples)
  Pipe-based Context Switching                369000.7 lps   (10.0 s, 7 samples)
  Process Creation                             12587.7 lps   (30.0 s, 2 samples)
  Shell Scripts (1 concurrent)                  8234.3 lpm   (60.0 s, 2 samples)
  Shell Scripts (8 concurrent)                  1064.5 lpm   (60.0 s, 2 samples)
  System Call Overhead                       4439274.5 lps   (10.0 s, 7 samples)

  System Benchmarks Index Values               BASELINE       RESULT    INDEX
  Dhrystone 2 using register variables         116700.0   34872897.7   2988.3
  Double-Precision Whetstone                       55.0       4022.0    731.3
  Execl Throughput                                 43.0       4735.8   1101.4
  File Copy 1024 bufsize 2000 maxblocks          3960.0    1294367.0   3268.6
  File Copy 256 bufsize 500 maxblocks            1655.0     396912.9   2398.3
  File Copy 4096 bufsize 8000 maxblocks          5800.0    2513158.7   4333.0
  Pipe Throughput                               12440.0    2221775.6   1786.0
  Pipe-based Context Switching                   4000.0     369000.7    922.5
  Process Creation                                126.0      12587.7    999.0
  Shell Scripts (1 concurrent)                     42.4       8234.3   1942.1
  Shell Scripts (8 concurrent)                      6.0       1064.5   1774.2
  System Call Overhead                          15000.0    4439274.5   2959.5
  ========
  System Benchmarks Index Score                                        1825.8


  Args:
    results: UnixBench result.

  Returns:
    A list of sample.Sample objects.
  """
  samples = []
  start_index = results.find(RESULT_START_STRING)
  while start_index != -1:
    next_start_index = results.find(RESULT_START_STRING, start_index + 1)
    result = results[start_index: next_start_index]
    parallel_copies = regex_util.ExtractAllMatches(
        PARALLEL_COPIES_REGEX, result)
    parallel_copy_metadata = {'num_parallel_copies': int(parallel_copies[0])}

    match = regex_util.ExtractAllMatches(RESULT_REGEX, result)
    for groups in match:
      metadata = {'samples': int(groups[5]), 'time': groups[3] + groups[4]}
      metadata.update(parallel_copy_metadata)
      samples.append(sample.Sample(
          groups[0].strip(), float(groups[1]), groups[2], metadata))
    match = regex_util.ExtractAllMatches(SCORE_REGEX, result)
    for groups in match:
      metadata = {'baseline': float(groups[1]), 'index': float(groups[3])}
      metadata.update(parallel_copy_metadata)
      samples.append(sample.Sample('%s:score' % groups[0].strip(),
                                   value=float(groups[2]),
                                   unit='',
                                   metadata=metadata))
    match = regex_util.ExtractAllMatches(SYSTEM_SCORE_REGEX, result)
    samples.append(sample.Sample('System Benchmarks Index Score',
                                 float(match[0]), unit='',
                                 metadata=parallel_copy_metadata))
    start_index = next_start_index

  return samples


def Run(benchmark_spec):
  """Run UnixBench on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('UnixBench running on %s', vm)
  unixbench_command = 'cd {0} && UB_TMPDIR={1} ./Run'.format(
      unixbench.UNIXBENCH_DIR, vm.GetScratchDir())
  logging.info('Unixbench Results:')
  stdout, _ = vm.RemoteCommand(unixbench_command, should_log=True)
  return ParseResults(stdout)


def Cleanup(benchmark_spec):
  """Cleanup UnixBench on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
