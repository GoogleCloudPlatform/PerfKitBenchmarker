# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""MLPerf Inference custom systems."""

from __future__ import annotations

from code.common.constants import AcceleratorType
from code.common.constants import ByteSuffix
from code.common.constants import Memory
from code.common.systems.accelerator import AcceleratorConfiguration
from code.common.systems.accelerator import GPU
from code.common.systems.base import MATCH_ANY
from code.common.systems.known_hardware import match_float_approximate
from code.common.systems.systems import SystemConfiguration


def system_configuration(
    gpu_type: GPU, num_gpus: int, system_id: str
) -> SystemConfiguration:
  return SystemConfiguration(
      host_cpu_conf=MATCH_ANY,
      host_mem_conf=MATCH_ANY,
      accelerator_conf=AcceleratorConfiguration(layout={gpu_type: num_gpus}),
      numa_conf=None,
      system_id=system_id,
  )


def gpu(
    name: str,
    memory_quantity: int,
    max_power_limit: float,
    pci_id: str,
    compute_cm: int,
) -> GPU:
  return GPU(
      name=name,
      accelerator_type=AcceleratorType.Discrete,
      vram=match_float_approximate(
          Memory(quantity=memory_quantity, byte_suffix=ByteSuffix.GiB)
      ),
      max_power_limit=max_power_limit,
      pci_id=pci_id,
      compute_sm=compute_cm,
  )

T4 = gpu("Tesla T4", 15, 70.0, "0x1EB810DE", 75)
L4 = gpu("Tesla L4", 22, 75.0, "0x27B810DE", 89)
A10G = gpu("NVIDIA A10G", 22, 300.0, "0x223710DE", 86)
A10 = gpu("NVIDIA A10-4Q", 24, None, "0x223610DE", 86)

custom_systems = {
    "T4x1": system_configuration(T4, 1, "T4x1"),
    "T4x4": system_configuration(T4, 4, "T4x4"),
    "L4x1": system_configuration(L4, 1, "L4x1"),
    "L4x2": system_configuration(L4, 2, "L4x2"),
    "L4x4": system_configuration(L4, 4, "L4x4"),
    "L4x8": system_configuration(L4, 8, "L4x8"),
    "A10Gx1": system_configuration(A10G, 1, "A10Gx1"),
    "A10Gx4": system_configuration(A10G, 4, "A10Gx4"),
    "A10Gx8": system_configuration(A10G, 8, "A10Gx8"),
    "A10x1": system_configuration(A10, 1, "A10x1"),
    "A10x2": system_configuration(A10, 2, "A10x2"),
}
