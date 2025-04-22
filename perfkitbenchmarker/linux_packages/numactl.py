# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing numactl installation and cleanup functions."""

from typing import Dict, Pattern, Union
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import regex_util

NUMA_CPUS_REGEX = r'node (\d+) cpus: ([\d ]*)'
NUMA_MEMORY_REGEX = r'node (\d+) size: (\d+) MB'


def _ParseNuma(vm, regex: Union[str, Pattern[str]]) -> Dict[int, str]:
  """Parses the numactl --hardware output with the supplied regex."""
  out, _ = vm.RemoteCommand('numactl --hardware')
  matches = regex_util.ExtractAllMatches(regex, out)
  return {int(m[0]): m[1] for m in matches}


def GetNuma(vm) -> Dict[int, int]:
  """Get NUMA CPU topology of the VM that only includes the available nodes.

  This method provides the number of available vCPUs on each NUMA node.
  While counting, it filters out the vCPUs that are not available to the current
  process. For example: if there are 16 vCPUs in a NUMA node, but only 8 vCPUs
  are available to the current process via taskset, etc., then only
  8 vCPUs for that NUMA node will be reported.

  Args:
    vm: VirtualMachine.

  Returns:
    A dictionary, key is the numa node, value is the number of available vCPUs
    on the node.
  """
  all_numa_map = {}
  allowed_cpu_set = vm.GetCpusAllowedSet()
  for node, cpus_in_numa_str in _ParseNuma(vm, NUMA_CPUS_REGEX).items():
    cpus_in_numa_csv = cpus_in_numa_str.strip().replace(' ', ',')
    cpu_set_in_numa = linux_virtual_machine.ParseRangeList(cpus_in_numa_csv)
    allowed_cpus_in_numa = allowed_cpu_set & cpu_set_in_numa
    all_numa_map[node] = len(allowed_cpus_in_numa)
  stdout, _ = vm.RemoteCommand('cat /proc/self/status | grep Mems_allowed_list')
  available_numa_nodes = linux_virtual_machine.ParseRangeList(
      stdout.split(':\t')[-1]
  )

  numa_map = {}
  for node, num_cpus in all_numa_map.items():
    if node in available_numa_nodes:
      numa_map[node] = num_cpus

  return numa_map


def GetNumaMemory(vm) -> Dict[int, int]:
  """Get NUMA memory topology of the VM.

  Args:
    vm: VirtualMachine.

  Returns:
    A dictionary, key is the numa node, value is the size in megabytes of
    the memory on that node
  """
  return {
      node: int(value)
      for node, value in _ParseNuma(vm, NUMA_MEMORY_REGEX).items()
  }


def Install(vm) -> None:
  """Installs the numactl package on the VM."""
  vm.InstallPackages('numactl')
