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
"""MLPerf Inference custom system DLRM configuration."""
from . import AccuracyTarget
from . import ConfigRegistry
from . import HarnessType
from . import KnownSystem
from . import PowerSetting
from . import ServerGPUBaseConfig


class T4x1(ServerGPUBaseConfig):
  enable_interleaved_top_mlp = True
  deque_timeout_usec = 1
  embedding_weights_on_gpu_part = 0.5
  gpu_batch_size = 65500
  gpu_num_bundles = 2
  num_staging_batches = 2
  num_staging_threads = 4
  server_target_qps = 24000
  use_jemalloc = True


class T4x1HighAccuracy(T4x1):
  pass


class T4x1Triton(T4x1):
  buffer_manager_thread_count = 8
  use_triton = True


class T4x1HighAccuracyTriton(T4x1Triton):
  pass


class T4x4(T4x1):
  gpu_num_bundles = 1
  num_staging_batches = 4
  num_staging_threads = 4
  server_target_qps = 125000
  use_jemalloc = True


class T4x4HighAccuracy(T4x4):
  pass


class T4x4Triton(T4x4):
  server_target_qps = 27500
  use_triton = True


class T4x4HighAccuracyTriton(T4x4Triton):
  pass


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class GcpT4x1(T4x1):
  system = KnownSystem.GcpT4x1


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class GcpT4x1HighAccuracy(T4x1HighAccuracy):
  system = KnownSystem.GcpT4x1


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class GcpT4x1Triton(T4x1Triton):
  system = KnownSystem.GcpT4x1


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class GcpT4x1HighAccuracyTriton(T4x1HighAccuracyTriton):
  system = KnownSystem.GcpT4x1


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class GcpT4x4(T4x4):
  system = KnownSystem.GcpT4x4


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class GcpT4x4HighAccuracy(T4x4HighAccuracy):
  system = KnownSystem.GcpT4x4


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class GcpT4x4Triton(T4x4Triton):
  system = KnownSystem.GcpT4x4


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class GcpT4x4HighAccuracyTriton(T4x4HighAccuracyTriton):
  system = KnownSystem.GcpT4x4


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AwsT4x1(T4x1):
  system = KnownSystem.AwsT4x1


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AwsT4x1HighAccuracy(T4x1HighAccuracy):
  system = KnownSystem.AwsT4x1


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AwsT4x1Triton(T4x1Triton):
  system = KnownSystem.AwsT4x1


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AwsT4x1HighAccuracyTriton(T4x1HighAccuracyTriton):
  system = KnownSystem.AwsT4x1


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AwsT4x4(T4x4):
  system = KnownSystem.AwsT4x4


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AwsT4x4HighAccuracy(T4x4HighAccuracy):
  system = KnownSystem.AwsT4x4


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AwsT4x4Triton(T4x4Triton):
  system = KnownSystem.AwsT4x4


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AwsT4x4HighAccuracyTriton(T4x4HighAccuracyTriton):
  system = KnownSystem.AwsT4x4


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AzureT4x1(T4x1):
  system = KnownSystem.AzureT4x1


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AzureT4x1HighAccuracy(T4x1HighAccuracy):
  system = KnownSystem.AzureT4x1


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AzureT4x1Triton(T4x1Triton):
  system = KnownSystem.AzureT4x1


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AzureT4x1HighAccuracyTriton(T4x1HighAccuracyTriton):
  system = KnownSystem.AzureT4x1


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AzureT4x4(T4x4):
  system = KnownSystem.AzureT4x4


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AzureT4x4HighAccuracy(T4x4HighAccuracy):
  system = KnownSystem.AzureT4x4


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AzureT4x4Triton(T4x4Triton):
  system = KnownSystem.AzureT4x4


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AzureT4x4HighAccuracyTriton(T4x4HighAccuracyTriton):
  system = KnownSystem.AzureT4x4
