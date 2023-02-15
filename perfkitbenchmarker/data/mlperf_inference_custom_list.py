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


custom_systems = dict()

custom_systems["T4x1"] = SystemConfiguration(
    host_cpu_conf=MATCH_ANY,
    host_mem_conf=MATCH_ANY,
    accelerator_conf=AcceleratorConfiguration(
        layout={
            GPU(
                name="Tesla T4",
                accelerator_type=AcceleratorType.Discrete,
                vram=match_float_approximate(
                    Memory(quantity=15, byte_suffix=ByteSuffix.GiB)
                ),
                max_power_limit=70.0,
                pci_id="0x1EB810DE",
                compute_sm=75,
            ): 1
        }
    ),
    numa_conf=None,
    system_id="T4x1",
)

custom_systems["T4x4"] = SystemConfiguration(
    host_cpu_conf=MATCH_ANY,
    host_mem_conf=MATCH_ANY,
    accelerator_conf=AcceleratorConfiguration(
        layout={
            GPU(
                name="Tesla T4",
                accelerator_type=AcceleratorType.Discrete,
                vram=match_float_approximate(
                    Memory(quantity=15, byte_suffix=ByteSuffix.GiB)
                ),
                max_power_limit=70.0,
                pci_id="0x1EB810DE",
                compute_sm=75,
            ): 4
        }
    ),
    numa_conf=None,
    system_id="T4x4",
)
