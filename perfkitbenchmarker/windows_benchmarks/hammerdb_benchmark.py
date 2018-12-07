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

"""Run HammerDB in a single VM."""

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags

from perfkitbenchmarker.windows_packages import hammerdb

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'hammerdb'
BENCHMARK_CONFIG = """
hammerdb:
  description: Run hammerdb on a single machine
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: 1
      disk_spec: *default_500_gb
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  vm = benchmark_spec.vms[0]
  vm.Install('hammerdb')


def Run(benchmark_spec):
  """Measure the sql performance in one VM.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects with the benchmark results.
  """

  vm = benchmark_spec.vms[0]
  return hammerdb.RunHammerDB(vm)


def Cleanup(unused_benchmark_spec):
  pass
