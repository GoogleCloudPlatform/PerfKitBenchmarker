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

import collections
import re
from typing import Dict, Pattern, Set, Union
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import regex_util

NUMA_CPUS_REGEX = r'node (\d+) cpus: ([\d ]*)'
NUMA_MEMORY_REGEX = r'node (\d+) size: (\d+) MB'


def _ParseNuma(vm, regex: Union[str, Pattern[str]]) -> Dict[int, str]:
  """Parses the numactl --hardware output with the supplied regex."""
  out, _ = vm.RemoteCommand('numactl --hardware')
  matches = regex_util.ExtractAllMatches(regex, out)
  return {int(m[0]): m[1] for m in matches}


def GetNumaCpus(vm) -> Dict[int, Set[int]]:
  """Get the set of available vCPUs on each available NUMA node of the VM.

  Only vCPUs available to the current process (see vm.GetCpusAllowedSet()) and
  NUMA nodes listed in Mems_allowed_list are included.

  Args:
    vm: VirtualMachine.

  Returns:
    A dictionary, key is the numa node, value is the set of available vCPUs on
    the node.
  """
  allowed_cpu_set = vm.GetCpusAllowedSet()
  all_numa_cpus = {}
  for node, cpus_in_numa_str in _ParseNuma(vm, NUMA_CPUS_REGEX).items():
    cpus_in_numa_csv = cpus_in_numa_str.strip().replace(' ', ',')
    cpu_set_in_numa = linux_virtual_machine.ParseRangeList(cpus_in_numa_csv)
    all_numa_cpus[node] = allowed_cpu_set & cpu_set_in_numa
  stdout, _ = vm.RemoteCommand('cat /proc/self/status | grep Mems_allowed_list')
  available_numa_nodes = linux_virtual_machine.ParseRangeList(
      stdout.split(':\t')[-1]
  )
  return {
      node: cpus
      for node, cpus in all_numa_cpus.items()
      if node in available_numa_nodes
  }


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
  return {node: len(cpus) for node, cpus in GetNumaCpus(vm).items()}


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


def GetNumaDistances(vm) -> Dict[int, Dict[int, int]]:
  """Get the NUMA distance matrix reported by `numactl --hardware`.

  The matrix is the firmware's (SLIT) view of relative memory access cost. The
  local distance is conventionally 10, so a value of 32 means "roughly 3.2x the
  local access cost". It is the only portable way to tell which nodes sit on a
  remote socket, since node numbering and node counts vary per cloud shape.

  Example `numactl --hardware` tail that this parses:

    node distances:
    node   0   1
      0:  10  32
      1:  32  10

  Args:
    vm: VirtualMachine.

  Returns:
    A nested dictionary d where d[a][b] is the distance from node a to node b.
    Empty if the VM does not report a distance matrix.
  """
  out, _ = vm.RemoteCommand('numactl --hardware')
  _, separator, distances = out.partition('node distances:')
  if not separator:
    return {}

  matrix = {}
  column_nodes = []
  for line in distances.splitlines():
    line = line.strip()
    if not line:
      continue
    # The section is introduced by a `node   0   1 ...` column header.
    if line.startswith('node'):
      column_nodes = [int(node) for node in line.split()[1:]]
      continue
    match = re.match(r'^(\d+):\s*([\d\s]+)$', line)
    if not match:
      # Anything else means the distance section has ended.
      break
    matrix[int(match.group(1))] = dict(
        zip(column_nodes, (int(v) for v in match.group(2).split()))
    )
  return matrix


def _FormatNodeRanges(nodes: list[int]) -> str:
  """Formats a sorted list of node indices into compact ranges, e.g. '0,2-4'."""
  if not nodes:
    return ''
  ranges = []
  start = end = nodes[0]
  for node in nodes[1:]:
    if node == end + 1:
      end = node
    else:
      ranges.append(f'{start}-{end}' if start != end else str(start))
      start = end = node
  ranges.append(f'{start}-{end}' if start != end else str(start))
  return ','.join(ranges)


def FormatNumaDistances(node_distances: Dict[int, int]) -> str:
  """Formats one node's distance row by grouping target nodes per distance.

  For example, {0: 10, 1: 12, 2: 32, 3: 32} becomes '10:[0], 12:[1], 32:[2-3]'.

  Args:
    node_distances: Mapping from target NUMA node to SLIT distance.

  Returns:
    A compact string grouping destination node ranges by distance value.
  """
  by_distance = collections.defaultdict(list)
  for target_node, distance in sorted(node_distances.items()):
    by_distance[distance].append(target_node)
  return ', '.join(
      f'{distance}:[{_FormatNodeRanges(nodes)}]'
      for distance, nodes in sorted(by_distance.items())
  )


def Install(vm) -> None:
  """Installs the numactl package on the VM."""
  vm.InstallPackages('numactl')
