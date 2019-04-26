# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

import itertools
import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import publisher
from perfkitbenchmarker import vm_util

from perfkitbenchmarker.windows_packages import ntttcp


# When adding new configs to ntttcp_config_list, increase this value
_NUM_PARAMS_IN_CONFIG = 3

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'ntttcp'
BENCHMARK_CONFIG = """
ntttcp:
  description: Run ntttcp between two VMs.
  vm_groups:
    default:
      vm_spec: *default_single_core
      vm_count: 2
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Install ntttcp and open any ports we need."""
  vms = benchmark_spec.vms

  for vm in vms:
    vm.Install('ntttcp')
    vm.AllowPort(ntttcp.CONTROL_PORT)
    # get the number of ports needed based on the flags
    num_ports = max([c.threads for c in ntttcp.ParseConfigList()])
    vm.AllowPort(ntttcp.BASE_DATA_PORT, ntttcp.BASE_DATA_PORT + num_ports)


def _RunTest(benchmark_spec, sender, receiver, dest_ip, ip_type, conf,
             cooldown_s):
  """Run a single NTTTCP test, and publish the results."""
  try:
    results = ntttcp.RunNtttcp(sender, receiver, dest_ip, ip_type, conf.udp,
                               conf.threads, conf.time_s, conf.packet_size,
                               cooldown_s)
    publisher.PublishRunStageSamples(benchmark_spec, results)
    return True
  except IOError:
    logging.info('Failed to publish %s IP results for config %s', ip_type,
                 str(conf))
    return False


def Run(benchmark_spec):
  """Measure TCP stream throughput between two VMs.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects with the benchmark results.
  """

  vm_sets = [(benchmark_spec.vms[0], benchmark_spec.vms[1]),
             (benchmark_spec.vms[1], benchmark_spec.vms[0])]

  parsed_configs = ntttcp.ParseConfigList()

  # Keep accounting of failed configs.
  failed_confs = []

  # Send traffic in both directions
  for ((sender, receiver), conf) in itertools.product(vm_sets, parsed_configs):
    # Send using external IP addresses
    if vm_util.ShouldRunOnExternalIpAddress(conf.ip_type):
      if not _RunTest(benchmark_spec, sender, receiver, receiver.ip_address,
                      'external', conf, True):
        failed_confs.append(('external', conf))

    # Send using internal IP addresses
    if vm_util.ShouldRunOnInternalIpAddress(sender, receiver, conf.ip_type):
      if not _RunTest(benchmark_spec, sender, receiver, receiver.internal_ip,
                      'internal', conf,
                      len(parsed_configs) > 1):
        failed_confs.append(('internal', conf))

  if failed_confs:
    logging.info('Failed to run test and/or gather results for %s',
                 str(failed_confs))

  return []


def Cleanup(unused_benchmark_spec):
  pass
