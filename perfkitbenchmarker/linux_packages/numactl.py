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
from perfkitbenchmarker import regex_util

NUMA_CPUS_REGEX = r'node (\d+) cpus: ([\d ]*)'
NUMA_MEMORY_REGEX = r'node (\d+) size: (\d+) MB'


def _ParseNuma(vm, regex: Union[str, Pattern[str]]) -> Dict[int, str]:
  """Parses the numactl --hardware output with the supplied regex."""
  out, _ = vm.RemoteCommand('numactl --hardware')
  matches = regex_util.ExtractAllMatches(regex, out)
  return {int(m[0]): m[1] for m in matches}


def _ParseNodeList(node_str: str) -> list[int]:
  """Parses the NUMA node list string and returns the list of node indices.

  Args:
    node_str: The node list string, e.g. 1-2,4-5

  Returns:
    The list of node indices.
    For the example with input '1-2,4-5', it returns [1,2,4,5].
  """
  if ',' in node_str:
    node_list = []
    for sub_node_str in node_str.split(','):
      node_list += _ParseNodeList(sub_node_str)
    return node_list

  if '-' in node_str:
    lhs, rhs = node_str.split('-')
    try:
      lhs = int(lhs)
      rhs = int(rhs)
    except ValueError as exc:
      raise ValueError(f'Invalid node range: [{node_str}]') from exc
    if lhs > rhs:
      raise ValueError(f'Invalid range found while parsing: [{lhs}-{rhs}]')

    return list(range(lhs, rhs + 1))

  try:
    int(node_str)
  except ValueError as exc:
    raise ValueError(f'Invalid NUMA node specified: [{node_str}]') from exc

  return [int(node_str)]


def GetNuma(vm) -> Dict[int, int]:
  """Get NUMA CPU topology of the VM that only includes the available nodes.

  Args:
    vm: VirtualMachine.

  Returns:
    A dictionary, key is the numa node, value is the
    number of vCPUs on the node.
  """
  all_numa_map = {
      node: len(value.split())
      for node, value in _ParseNuma(vm, NUMA_CPUS_REGEX).items()
  }
  stdout, _ = vm.RemoteCommand('cat /proc/self/status | grep Mems_allowed_list')
  available_numa_nodes = _ParseNodeList(stdout.split(':\t')[-1])

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
