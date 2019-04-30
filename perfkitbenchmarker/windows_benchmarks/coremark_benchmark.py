# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Runs Coremark."""

from perfkitbenchmarker import flags

from perfkitbenchmarker.linux_benchmarks import coremark_benchmark

FLAGS = flags.FLAGS

BENCHMARK_NAME = coremark_benchmark.BENCHMARK_NAME
BENCHMARK_CONFIG = coremark_benchmark.BENCHMARK_CONFIG
GetConfig = coremark_benchmark.GetConfig


def Prepare(benchmark_spec):
  """Installs coremark on the target VM under Cygwin."""
  vm = benchmark_spec.vms[0]
  vm.InstallCygwin(packages=['wget', 'gcc-core', 'tar', 'make'])
  coremark_benchmark.PrepareCoremark(vm.RemoteCommandCygwin)


def Run(benchmark_spec):
  """Runs coremark on the VM under Cygwin.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects with the benchmark results.
  """
  vm = benchmark_spec.vms[0]
  return coremark_benchmark.RunCoremark(vm.RemoteCommandCygwin,
                                        vm.NumCpusForBenchmark())


def Cleanup(benchmark_spec):
  """Cleans up coremark on the target VM."""
  vm = benchmark_spec.vms[0]
  coremark_benchmark.CleanupCoremark(vm.RemoteCommandCygwin)
