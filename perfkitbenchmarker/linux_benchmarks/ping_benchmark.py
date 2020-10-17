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

"""Runs ping.

This benchmark runs ping using the internal, and optionally external, ips of
vms in the same zone.
"""

import logging
import re
from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS


BENCHMARK_NAME = 'ping'
BENCHMARK_CONFIG = """
ping:
  description: Benchmarks ping latency over internal IP addresses
  vm_groups:
    vm_1:
      vm_spec: *default_single_core
    vm_2:
      vm_spec: *default_single_core
"""

METRICS = ('Min Latency', 'Average Latency', 'Max Latency', 'Latency Std Dev')


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):  # pylint: disable=unused-argument
  """Install ping on the target vm.
  Checks that there are exactly two vms specified.
  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  if len(benchmark_spec.vms) != 2:
    raise ValueError(
        'Ping benchmark requires exactly two machines, found {0}'
        .format(len(benchmark_spec.vms)))
  if vm_util.ShouldRunOnExternalIpAddress():
    vms = benchmark_spec.vms
    for vm in vms:
      vm.AllowIcmp()


def Run(benchmark_spec):
  """Run ping on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  results = []
  for sending_vm, receiving_vm in vms, reversed(vms):
    if vm_util.ShouldRunOnExternalIpAddress():
      ip_type = vm_util.IpAddressMetadata.EXTERNAL
      results = results + _RunPing(sending_vm,
                                   receiving_vm,
                                   receiving_vm.ip_address,
                                   ip_type)
    if vm_util.ShouldRunOnInternalIpAddress(sending_vm, receiving_vm):
      ip_type = vm_util.IpAddressMetadata.INTERNAL
      results = results + _RunPing(sending_vm,
                                   receiving_vm,
                                   receiving_vm.internal_ip,
                                   ip_type)
  return results


def _RunPing(sending_vm, receiving_vm, receiving_ip, ip_type):
  """Run ping using 'sending_vm' to connect to 'receiving_ip'.

  Args:
    sending_vm: The VM issuing the ping request.
    receiving_vm: The VM receiving the ping.  Needed for metadata.
    receiving_ip: The IP address to be pinged.
    ip_type: The type of 'receiving_ip',
        (either 'vm_util.IpAddressSubset.INTERNAL
         or vm_util.IpAddressSubset.EXTERNAL')

  Returns:
    A list of samples, with one sample for each metric.
  """
  if (ip_type == vm_util.IpAddressMetadata.INTERNAL and
      not sending_vm.IsReachable(receiving_vm)):
    logging.warn('%s is not reachable from %s', receiving_vm, sending_vm)
    return []

  logging.info('Ping results (ip_type = %s):', ip_type)
  ping_cmd = 'ping -c 100 %s' % receiving_ip
  stdout, _ = sending_vm.RemoteCommand(ping_cmd, should_log=True)
  stats = re.findall('([0-9]*\\.[0-9]*)', stdout.splitlines()[-1])
  assert len(stats) == len(METRICS), stats
  results = []

  metadata = {'ip_type': ip_type,
              'receiving_zone': receiving_vm.zone,
              'sending_zone': sending_vm.zone}
  for i, metric in enumerate(METRICS):
    results.append(sample.Sample(metric, float(stats[i]), 'ms', metadata))
  return results


def Cleanup(benchmark_spec):  # pylint: disable=unused-argument
  """Cleanup ping on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
