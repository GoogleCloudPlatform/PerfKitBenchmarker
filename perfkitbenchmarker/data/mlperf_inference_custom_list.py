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
from code.common.constants import CPUArchitecture
from code.common.constants import Memory
from code.common.systems.accelerator import AcceleratorConfiguration
from code.common.systems.accelerator import GPU
from code.common.systems.cpu import CPU
from code.common.systems.cpu import CPUConfiguration
from code.common.systems.known_hardware import KnownGPU
from code.common.systems.memory import MemoryConfiguration
from code.common.systems.systems import SystemConfiguration


custom_systems = dict()

custom_systems["GcpT4x1"] = SystemConfiguration(
    host_cpu_conf=CPUConfiguration(
        layout={
            CPU(
                name="Intel(R) Xeon(R) CPU @ 2.00GHz",
                architecture=CPUArchitecture.x86_64,
                core_count=8,
                threads_per_core=2,
            ): 1
        }
    ),
    host_mem_conf=MemoryConfiguration(
        host_memory_capacity=Memory(
            quantity=107.138476, byte_suffix=ByteSuffix.GB
        ),
        comparison_tolerance=0.05,
    ),
    accelerator_conf=AcceleratorConfiguration(
        layout={
            GPU(
                name="Tesla T4",
                accelerator_type=AcceleratorType.Discrete,
                vram=Memory(quantity=14.7548828125, byte_suffix=ByteSuffix.GiB),
                max_power_limit=70.0,
                pci_id="0x1EB810DE",
                compute_sm=75,
            ): 1
        }
    ),
    numa_conf=None,
    system_id="GcpT4x1",
)

custom_systems["GcpT4x4"] = SystemConfiguration(
    host_cpu_conf=CPUConfiguration(
        layout={
            CPU(
                name="Intel(R) Xeon(R) CPU @ 2.00GHz",
                architecture=CPUArchitecture.x86_64,
                core_count=32,
                threads_per_core=2,
            ): 1
        }
    ),
    host_mem_conf=MemoryConfiguration(
        host_memory_capacity=Memory(
            quantity=428.553904, byte_suffix=ByteSuffix.GB
        ),
        comparison_tolerance=0.05,
    ),
    accelerator_conf=AcceleratorConfiguration(
        layout={
            GPU(
                name="Tesla T4",
                accelerator_type=AcceleratorType.Discrete,
                vram=Memory(quantity=14.7548828125, byte_suffix=ByteSuffix.GiB),
                max_power_limit=70.0,
                pci_id="0x1EB810DE",
                compute_sm=75,
            ): 4
        }
    ),
    numa_conf=None,
    system_id="GcpT4x4",
)

custom_systems["AwsT4x1"] = SystemConfiguration(
    host_cpu_conf=CPUConfiguration(
        layout={
            CPU(
                name="Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz",
                architecture=CPUArchitecture.x86_64,
                core_count=8,
                threads_per_core=2,
            ): 1
        }
    ),
    host_mem_conf=MemoryConfiguration(
        host_memory_capacity=Memory(
            quantity=65.034928, byte_suffix=ByteSuffix.GB
        ),
        comparison_tolerance=0.05,
    ),
    accelerator_conf=AcceleratorConfiguration(
        layout={
            GPU(
                name="Tesla T4",
                accelerator_type=AcceleratorType.Discrete,
                vram=Memory(quantity=14.7548828125, byte_suffix=ByteSuffix.GiB),
                max_power_limit=70.0,
                pci_id="0x1EB810DE",
                compute_sm=75,
            ): 1
        }
    ),
    numa_conf=None,
    system_id="AwsT4x1",
)

custom_systems["AwsT4x4"] = SystemConfiguration(
    host_cpu_conf=CPUConfiguration(
        layout={
            CPU(
                name="Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz",
                architecture=CPUArchitecture.x86_64,
                core_count=24,
                threads_per_core=2,
            ): 1
        }
    ),
    host_mem_conf=MemoryConfiguration(
        host_memory_capacity=Memory(
            quantity=195.104784, byte_suffix=ByteSuffix.GB
        ),
        comparison_tolerance=0.05,
    ),
    accelerator_conf=AcceleratorConfiguration(
        layout={
            GPU(
                name="Tesla T4",
                accelerator_type=AcceleratorType.Discrete,
                vram=Memory(quantity=14.7548828125, byte_suffix=ByteSuffix.GiB),
                max_power_limit=70.0,
                pci_id="0x1EB810DE",
                compute_sm=75,
            ): 4
        }
    ),
    numa_conf=None,
    system_id="AwsT4x4",
)

custom_systems["AzureT4x1"] = SystemConfiguration(
    host_cpu_conf=CPUConfiguration(
        layout={
            CPU(
                name="AMD EPYC 7V12 64-Core Processor",
                architecture=CPUArchitecture.x86_64,
                core_count=16,
                threads_per_core=1,
            ): 1
        }
    ),
    host_mem_conf=MemoryConfiguration(
        host_memory_capacity=Memory(
            quantity=113.33704399999999, byte_suffix=ByteSuffix.GB
        ),
        comparison_tolerance=0.05,
    ),
    accelerator_conf=AcceleratorConfiguration(layout={KnownGPU.T4.value: 1}),
    numa_conf=None,
    system_id="AzureT4x1",
)


custom_systems["AzureT4x4"] = SystemConfiguration(
    host_cpu_conf=CPUConfiguration(
        layout={
            CPU(
                name="AMD EPYC 7V12 64-Core Processor",
                architecture=CPUArchitecture.x86_64,
                core_count=32,
                threads_per_core=1,
            ): 2
        }
    ),
    host_mem_conf=MemoryConfiguration(
        host_memory_capacity=Memory(
            quantity=454.04437199999995, byte_suffix=ByteSuffix.GB
        ),
        comparison_tolerance=0.05,
    ),
    accelerator_conf=AcceleratorConfiguration(
        layout={
            GPU(
                name="Tesla T4",
                accelerator_type=AcceleratorType.Discrete,
                vram=Memory(quantity=15.7490234375, byte_suffix=ByteSuffix.GiB),
                max_power_limit=70.0,
                pci_id="0x1EB810DE",
                compute_sm=75,
            ): 4
        }
    ),
    numa_conf=None,
    system_id="AzureT4x4",
)
