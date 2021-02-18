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

from typing import Dict, Pattern
from perfkitbenchmarker import regex_util

NUMA_CPUS_REGEX = r'node (\d+) cpus: ([\d ]*)'
NUMA_MEMORY_REGEX = r'node (\d+) size: (\d+) MB'


def _ParseNuma(vm, regex: Pattern[str]) -> Dict[int, str]:
  """Parses the numactl --hardware output with the supplied regex."""
  out, _ = vm.RemoteCommand('numactl --hardware')
  matches = regex_util.ExtractAllMatches(regex, out)
  return {int(m[0]): m[1] for m in matches}


def GetNuma(vm) -> Dict[int, int]:
  """Get NUMA CPU topology of the VM.

  Args:
    vm: VirtualMachine.

  Returns:
    A dictionary, key is the numa node, value is the
    number of vCPUs on the node.
  """
  return {
      node: len(value.split())
      for node, value in _ParseNuma(vm, NUMA_CPUS_REGEX).items()
  }


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
