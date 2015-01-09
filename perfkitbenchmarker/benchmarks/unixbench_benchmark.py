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

"""Runs UnixBench.

Documentation & code: http://code.google.com/p/byte-unixbench/

Unix bench is a holistic performance benchmark, measuing CPU performance,
some memory bandwidth, and disk.
"""

import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'UnixBench++_benchmark',
                  'description': 'Runs UnixBench.',
                  'scratch_disk': True,
                  'num_machines': 1}


UNIXBENCH_NAME = 'UnixBench5.1.3.tgz'
UNIXBENCH_LOC = 'http://byte-unixbench.googlecode.com/files/%s' % UNIXBENCH_NAME

SYSTEM_SCORE_REGEX = '\nSystem Benchmarks Index Score\s+([-+]?[0-9]*\.?[0-9]+)'
RESULT_REGEX = (
    '\n([A-Z][\w\-\(\) ]+)\s+([-+]?[0-9]*\.?[0-9]+) (\w+)\s+\('
    '([-+]?[0-9]*\.?[0-9]+) (\w+), (\d+) samples\)')
SCORE_REGEX = (
    '\n([A-Z][\w\-\(\) ]+)\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)\s+'
    '([-+]?[0-9]*\.?[0-9]+)')


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Install Unixbench++ on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('Unixbench prepare on %s', vm)
  vm.InstallPackage('build-essential libx11-dev libgl1-mesa-dev libxext-dev')
  wget_cmd = '/usr/bin/wget %s' % UNIXBENCH_LOC
  vm.RemoteCommand(wget_cmd)
  vm.RemoteCommand('tar xvfz %s -C %s' % (UNIXBENCH_NAME, vm.GetScratchDir()))


def ParseResult(result):
  """Result parser for UnixBench.

  Sample Results:
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
    result: UnixBench result.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  samples = []
  match = regex_util.ExtractAllMatch(RESULT_REGEX, result)
  for groups in match:
    samples.append([groups[0].strip(), float(groups[1]), groups[2],
                    {'samples': int(groups[5]), 'time': groups[3] + groups[4]}])
  match = regex_util.ExtractAllMatches(SCORE_REGEX, result)
  for groups in match:
    samples.append(['%s:score' % groups[0].strip(), float(groups[2]), '',
                    {'baseline': float(groups[1]), 'index': float(groups[3])}])
  match = regex_util.ExtractAllMatches(SYSTEM_SCORE_REGEX, result)
  samples.append(['System Benchmarks Index Score', float(match[0]), '', {}])
  return samples


def Run(benchmark_spec):
  """Run UnixBench on the target vm.

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
  logging.info('UnixBench running on %s', vm)
  unixbench_command = 'cd %s/UnixBench;./Run' % vm.GetScratchDir()
  logging.info('Unixbench Results:')
  stdout, _ = vm.RemoteCommand(unixbench_command, should_log=True)
  return ParseResult(stdout)


def Cleanup(benchmark_spec):
  """Cleanup UnixBench on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('UnixBench Cleanup on %s', vm)
  vm.RemoteCommand('rm -f ~/%s' % UNIXBENCH_NAME)
  vm.RemoteCommand('rm -rf %s/UnixBench' % vm.GetScratchDir())
