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

"""Run nutttcp between two VMs."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import vm_util

from perfkitbenchmarker.windows_packages import nuttcp
from six.moves import range

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'nuttcp'
BENCHMARK_CONFIG = """
nuttcp:
  description: Run nuttcp between two VMs.
  vm_groups:
    vm_1:
      vm_spec: *default_single_core
    vm_2:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  for vm in benchmark_spec.vms:
    vm.Install('nuttcp')
    vm.AllowPort(nuttcp.CONTROL_PORT)
    vm.AllowPort(nuttcp.UDP_PORT)


def RunNuttcp(vms, exec_path):
  """Run nuttcps tests.

  Args:
    vms: list of vms to run the tests.
    exec_path: path to the nuttcp executable.

  Returns:
    list of samples
  """

  results = []

  def _RunNuttcpTest(sending_vm, receiving_vm, iteration):
    if vm_util.ShouldRunOnExternalIpAddress():
      results.extend(
          nuttcp.RunNuttcp(sending_vm, receiving_vm, exec_path,
                           receiving_vm.ip_address, 'external', iteration))
    if vm_util.ShouldRunOnInternalIpAddress(sending_vm, receiving_vm):
      results.extend(
          nuttcp.RunNuttcp(sending_vm, receiving_vm, exec_path,
                           receiving_vm.internal_ip, 'internal', iteration))

  # run in both directions just for completeness
  for iteration in range(FLAGS.nuttcp_udp_iterations):
    _RunNuttcpTest(vms[0], vms[1], iteration)
    if FLAGS.nuttcp_udp_run_both_directions:
      _RunNuttcpTest(vms[1], vms[0], iteration)

  return results


def Run(benchmark_spec):
  vms = benchmark_spec.vms
  exec_path = nuttcp.GetExecPath()
  return RunNuttcp(vms, exec_path)


def Cleanup(unused_benchmark_spec):
  pass
