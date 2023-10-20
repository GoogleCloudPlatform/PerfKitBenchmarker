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
      numa_conf=MATCH_ANY,
      system_id=system_id,
  )


def gpu(
    name: str,
    memory_quantity: int,
    max_power_limit: float,
    pci_id: str,
    compute_cm: int
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

CloudT4 = gpu("Tesla T4", 15, 70.0, "0x1EB810DE", 75)
CloudL4 = gpu("NVIDIA L4", 22, 72.0, "0x27B810DE", 89)
A10G = gpu("NVIDIA A10G", 22, 300.0, "0x223710DE", 86)
A10 = gpu("NVIDIA A10-4Q", 24, None, "0x223610DE", 86)
H100 = gpu("NVIDIA H100 80GB HBM3", 79.6, 700.0, "0x233010DE", 90)

custom_systems = {
    "CloudT4x1": system_configuration(CloudT4, 1, "CloudT4x1"),
    "CloudT4x4": system_configuration(CloudT4, 4, "CloudT4x4"),
    "CloudL4x1": system_configuration(CloudL4, 1, "CloudL4x1"),
    "CloudL4x2": system_configuration(CloudL4, 2, "CloudL4x2"),
    "CloudL4x4": system_configuration(CloudL4, 4, "CloudL4x4"),
    "CloudL4x8": system_configuration(CloudL4, 8, "CloudL4x8"),
    "A10Gx1": system_configuration(A10G, 1, "A10Gx1"),
    "A10Gx4": system_configuration(A10G, 4, "A10Gx4"),
    "A10Gx8": system_configuration(A10G, 8, "A10Gx8"),
    "A10x1": system_configuration(A10, 1, "A10x1"),
    "A10x2": system_configuration(A10, 2, "A10x2"),
    "H100x1": system_configuration(H100, 1, "H100x1"),
    "H100x8": system_configuration(H100, 8, "H100x8"),
}
