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
    '\n([\w\-\(\) ]+)\s+([-+]?[0-9]*\.?[0-9]+) (\w+)\s+\(([-+]?[0-9]*\.?[0-9]+) '
    '(\w+), (\d+) samples\)')
SCORE_REGEX = (
    '\n([\w\-\(\) ]+)\s+([-+]?[0-9]*\.?[0-9]+)\s+([-+]?[0-9]*\.?[0-9]+)\s+'
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


def UnixBenchParser(result):
  """Result parser for UnixBench.

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
                    {'samples': groups[5], 'time': groups[3] + groups[4]}])
  match = regex_util.ExtractAllMatch(SCORE_REGEX, result)
  for groups in match:
    samples.append(['%s:score' % groups[0].strip(), float(groups[2]), '',
                    {'baseline': float(groups[1]), 'index': float(groups[3])}])
  match = regex_util.ExtractAllMatch(SYSTEM_SCORE_REGEX, result)
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
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('UnixBench running on %s', vm)
  unixbench_command = 'cd %s/UnixBench;./Run' % vm.GetScratchDir()
  logging.info('Unixbench Results:')
  stdout, _ = vm.RemoteCommand(unixbench_command, should_log=True)
  return UnixBenchParser(stdout)
  """
  return UnixBenchParser(open('/usr/local/google/home/yuyanting/Documents/PkbUnixParser/PerfKitBenchmarker/perfkitbenchmarker/benchmarks/result').read())


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
