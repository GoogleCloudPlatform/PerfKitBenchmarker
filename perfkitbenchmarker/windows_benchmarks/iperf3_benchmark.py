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

"""Run iperf3 between two VMs."""

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

from perfkitbenchmarker.windows_packages import iperf3

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'iperf3'
BENCHMARK_CONFIG = """
iperf3:
  description: Run iperf3 between two VMs.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: 2
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  for vm in benchmark_spec.vms:
    vm.Install('iperf3')
    vm.AllowPort(iperf3.IPERF3_UDP_PORT)


def Run(benchmark_spec):
  """Measure UDP bandwidth between two VMs.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects with the benchmark results.
  """

  vms = benchmark_spec.vms
  results = []

  def _RunTest(sending_vm, receiving_vm):
    if vm_util.ShouldRunOnExternalIpAddress():
      results.extend(iperf3.RunIperf3UDPStream(sending_vm,
                                               receiving_vm,
                                               use_internal_ip=False))

    if vm_util.ShouldRunOnInternalIpAddress(sending_vm, receiving_vm):
      results.extend(iperf3.RunIperf3UDPStream(sending_vm,
                                               receiving_vm,
                                               use_internal_ip=True))

  _RunTest(vms[0], vms[1])
  _RunTest(vms[1], vms[0])

  return results


def Cleanup(unused_benchmark_spec):
  pass
