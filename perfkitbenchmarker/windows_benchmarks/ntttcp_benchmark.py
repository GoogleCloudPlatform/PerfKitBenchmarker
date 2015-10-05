# Copyright 2015 Google Inc. All rights reserved.
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

"""Run NTttcp between two VMs."""

from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

from perfkitbenchmarker.windows_packages import ntttcp

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'ntttcp',
                  'description': 'Run ntttcp between two VMs.',
                  'scratch_disk': False,
                  'num_machines': 2}


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  vms = benchmark_spec.vms
  fw = benchmark_spec.firewall

  for vm in vms:
    vm.Install('ntttcp')
    fw.AllowPort(vm, ntttcp.CONTROL_PORT)
    for port in xrange(ntttcp.BASE_DATA_PORT,
                       ntttcp.BASE_DATA_PORT + FLAGS.ntttcp_threads):
      fw.AllowPort(vm, port)


def Run(benchmark_spec):
  """Measure the boot time for all VMs.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects with the benchmark results.
  """

  vms = benchmark_spec.vms
  results = []

  # Send traffic in both directions
  for originator in [0, 1]:
    sending_vm = vms[originator]
    receiving_vm = vms[originator ^ 1]
    # Send using external IP addresses
    if vm_util.ShouldRunOnExternalIpAddress():
      results.extend(ntttcp.RunNtttcp(sending_vm,
                                      receiving_vm,
                                      receiving_vm.ip_address,
                                      'external'))

    # Send using internal IP addresses
    if vm_util.ShouldRunOnInternalIpAddress(sending_vm,
                                            receiving_vm):
      results.extend(ntttcp.RunNtttcp(sending_vm,
                                      receiving_vm,
                                      receiving_vm.internal_ip,
                                      'internal'))
  return results


def Cleanup(unused_benchmark_spec):
  pass
