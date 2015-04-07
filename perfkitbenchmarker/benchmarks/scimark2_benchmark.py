# Copyright 2015 Zi Shen Lim. All rights reserved.
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

"""Runs SciMark2.

Original documentation & code: http://math.nist.gov/scimark2/

SciMark2 is a Java (and C) benchmark for scientific and numerical computing.
It measures several computational kernels and reports a composite score in
approximate Mflops (Millions of floating point operations per second).

For convenience, we use the code base at https://github.com/zlim/scimark2,
which contains both the Java and C versions of SciMark2 in a single
repository.
"""

import logging
import re

from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

SCIMARK2_URL = 'https://github.com/zlim/scimark2/archive/master.zip'
SCIMARK2_ZIP = '{0}/scimark2-master.zip'.format(vm_util.VM_TMP_DIR)
SCIMARK2_PATH = '{0}/scimark2-master'.format(vm_util.VM_TMP_DIR)

BENCHMARK_INFO = {'name': 'scimark2',
                  'description': 'Runs SciMark2',
                  'scratch_disk': False,
                  'num_machines': 1}

TEST_PARSE_RESULTS = False


def GetInfo():
  return BENCHMARK_INFO


def CheckPrerequisites():
  pass


def Prepare(benchmark_spec):
  """Install SciMark2 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('Preparing SciMark2 on %s', vm)
  vm.Install('build_tools')
  vm.Install('wget')
  vm.InstallPackages('unzip')
  cmds = [
      'wget {0} -O {1}'.format(SCIMARK2_URL, SCIMARK2_ZIP),
      '(cd {0} && rm -rf {1} && unzip {2})'.format(
          vm_util.VM_TMP_DIR, SCIMARK2_PATH, SCIMARK2_ZIP),
      '(cd {0} && make build_c)'.format(SCIMARK2_PATH),
      '(cd {0} && make build_java)'.format(SCIMARK2_PATH)
  ]
  for cmd in cmds:
    vm.RemoteCommand(cmd, should_log=True)


def Run(benchmark_spec):
  """Run SciMark2 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('Running SciMark2 on %s', vm)
  samples = []
  if TEST_PARSE_RESULTS:
    samples.extend(TestParseResults())
    return samples
  cmds = [
      '(cd {0} && make c)'.format(SCIMARK2_PATH),
      '(cd {0} && make java)'.format(SCIMARK2_PATH)
  ]
  for cmd in cmds:
    stdout, _ = vm.RemoteCommand(cmd, should_log=True)
    samples.extend(ParseResults(stdout))
  return samples


def Cleanup(benchmark_spec):
  """Cleanup SciMark2 on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass


def ParseResults(results):
  """Result parser for SciMark2.

  Sample Results (C version):
    **                                                              **
    ** SciMark2 Numeric Benchmark, see http://math.nist.gov/scimark **
    ** for details. (Results can be submitted to pozo@nist.gov)     **
    **                                                              **
    Using       2.00 seconds min time per kenel.
    Composite Score:         1596.04
    FFT             Mflops:  1568.64    (N=1024)
    SOR             Mflops:  1039.98    (100 x 100)
    MonteCarlo:     Mflops:   497.64
    Sparse matmult  Mflops:  1974.39    (N=1000, nz=5000)
    LU              Mflops:  2899.56    (M=100, N=100)

  Sample Results (Java version):

    SciMark 2.0a

    Composite Score: 1731.4467627163242
    FFT (1024): 996.9938397943672
    SOR (100x100):   1333.5328291027124
    Monte Carlo : 724.5221517116782
    Sparse matmult (N=1000, nz=5000): 1488.18620413327
    LU (100x100): 4113.998788839592

    java.vendor: Oracle Corporation
    java.version: 1.7.0_75
    os.arch: amd64
    os.name: Linux
    os.version: 3.16.0-25-generic

  Args:
    results: SciMark2 result.

  Returns:
    A list of sample.Sample objects.
  """
  RESULT_START_C = '** SciMark2 Numeric Benchmark'
  RESULT_START_JAVA = 'SciMark 2.0a'

  SCORE_REGEX = r'\n(Composite Score):\s+(\d+\.\d+)'
  RESULT_REGEX_C = r'\n(.+?)\s+Mflops:\s+(\d+\.\d+)(\s+\(.+?\))?'
  RESULT_REGEX_JAVA = r'\n([^:]+):\s+(\d+\.\d+)'
  PLATFORM_REGEX = r'\n(\w+\.\w+):\s+(.*)'

  def FindBenchStart(results, start_index=0):
    bench_version = 'Unknown'
    index = results.find(RESULT_START_C, start_index)
    if index != -1:
      bench_version = 'C'
    else:
      index = results.find(RESULT_START_JAVA, start_index)
      if index != -1:
        bench_version = 'Java'
    return index, bench_version

  def ExtractPlatform(result, bench_version, clear=True):
    metadata = dict()
    if bench_version == 'C':
      pass
    elif bench_version == 'Java':
      matches = re.finditer(PLATFORM_REGEX, result)
      for m in matches:
        metadata.update({m.group(1): m.group(2)})
        if clear:
          result = result[:m.start()] + ' ' * (m.end() - m.start()) + \
              result[m.end():]
    return metadata, result

  def ExtractScore(result, bench_version, clear=True):
    m = re.search(SCORE_REGEX, result)
    label = m.group(1)
    score = float(m.group(2))
    if clear:
      result = result[:m.start()] + ' ' * (m.end() - m.start()) + \
          result[m.end():]
    return score, label, result

  def ExtractResults(result, bench_version):
    datapoints = []
    if bench_version == 'C':
      match = regex_util.ExtractAllMatches(RESULT_REGEX_C, result)
      for groups in match:
        metric = '{0} {1}'.format(groups[0].strip(), groups[2].strip())
        metric = metric.strip().strip(':')
        value = float(groups[1])
        datapoints.append((metric, value))
    elif bench_version == 'Java':
      match = regex_util.ExtractAllMatches(RESULT_REGEX_JAVA, result)
      for groups in match:
        datapoints.append((groups[0].strip(), float(groups[1])))
    return datapoints

  samples = []
  start_index, bench_version = FindBenchStart(results)
  while start_index != -1:
    next_start, next_bench = FindBenchStart(results, start_index + 1)
    result = results[start_index:next_start]

    metadata = {'bench_version': bench_version}

    platform_metadata, result = ExtractPlatform(result, bench_version)
    metadata.update(platform_metadata)

    score, label, result = ExtractScore(result, bench_version)
    samples.append(sample.Sample(label, score, 'Mflops', metadata))

    datapoints = ExtractResults(result, bench_version)
    for metric, value in datapoints:
      samples.append(sample.Sample(metric, value, 'Mflops', metadata))

    start_index, bench_version = next_start, next_bench

  return samples


def TestParseResults():
  TEST_C = """**                                                              **
** SciMark2 Numeric Benchmark, see http://math.nist.gov/scimark **
** for details. (Results can be submitted to pozo@nist.gov)     **
**                                                              **
Using       2.00 seconds min time per kenel.
Composite Score:         1596.04
FFT             Mflops:  1568.64    (N=1024)
SOR             Mflops:  1039.98    (100 x 100)
MonteCarlo:     Mflops:   497.64
Sparse matmult  Mflops:  1974.39    (N=1000, nz=5000)
LU              Mflops:  2899.56    (M=100, N=100)
"""
  TEST_JAVA = """SciMark 2.0a

Composite Score: 1716.3662351463677
FFT (1024): 1000.1380057152871
SOR (100x100):   1353.1987180103354
Monte Carlo : 727.7138820888014
Sparse matmult (N=1000, nz=5000): 1495.40225150659
LU (100x100): 4005.3783184108247

java.vendor: Oracle Corporation
java.version: 1.7.0_75
os.arch: amd64
os.name: Linux
os.version: 3.16.0-25-generic
"""
  samples = []
  samples.extend(ParseResults(TEST_C))
  samples.extend(ParseResults(TEST_JAVA))
  return samples
