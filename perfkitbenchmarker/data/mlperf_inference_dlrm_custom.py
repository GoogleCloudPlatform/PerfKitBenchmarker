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


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class GcpT4x1(ServerGPUBaseConfig):
  system = KnownSystem.GcpT4x1
  enable_interleaved_top_mlp = True
  deque_timeout_usec = 1
  embedding_weights_on_gpu_part = 0.5
  gpu_batch_size = 65500
  gpu_num_bundles = 2
  num_staging_batches = 2
  num_staging_threads = 4
  server_target_qps = 24000
  use_jemalloc = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class GcpT4x1HighAccuracy(GcpT4x1):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class GcpT4x1Triton(GcpT4x1):
  use_triton = True
  buffer_manager_thread_count = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class GcpT4x1HighAccuracyTriton(GcpT4x1HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class GcpT4x4(ServerGPUBaseConfig):
  system = KnownSystem.GcpT4x4
  enable_interleaved_top_mlp = True
  deque_timeout_usec = 1
  embedding_weights_on_gpu_part = 0.5
  gpu_batch_size = 65500
  gpu_num_bundles = 2
  num_staging_batches = 2
  num_staging_threads = 4
  server_target_qps = 24000
  use_jemalloc = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class GcpT4x4HighAccuracy(GcpT4x4):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class GcpT4x4Triton(GcpT4x4):
  use_triton = True
  buffer_manager_thread_count = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class GcpT4x4HighAccuracyTriton(GcpT4x4HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AwsT4x1(ServerGPUBaseConfig):
  system = KnownSystem.AwsT4x1
  enable_interleaved_top_mlp = True
  deque_timeout_usec = 1
  embedding_weights_on_gpu_part = 0.5
  gpu_batch_size = 65500
  gpu_num_bundles = 2
  num_staging_batches = 2
  num_staging_threads = 4
  server_target_qps = 24000
  use_jemalloc = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AwsT4x1HighAccuracy(AwsT4x1):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AwsT4x1Triton(AwsT4x1):
  use_triton = True
  buffer_manager_thread_count = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AwsT4x1HighAccuracyTriton(AwsT4x1HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AwsT4x4(ServerGPUBaseConfig):
  system = KnownSystem.AwsT4x4
  enable_interleaved_top_mlp = True
  deque_timeout_usec = 1
  embedding_weights_on_gpu_part = 0.5
  gpu_batch_size = 65500
  gpu_num_bundles = 2
  num_staging_batches = 2
  num_staging_threads = 4
  server_target_qps = 24000
  use_jemalloc = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AwsT4x4HighAccuracy(AwsT4x4):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AwsT4x4Triton(AwsT4x4):
  use_triton = True
  buffer_manager_thread_count = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AwsT4x4HighAccuracyTriton(AwsT4x4HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AzureT4x1(ServerGPUBaseConfig):
  system = KnownSystem.AzureT4x1
  enable_interleaved_top_mlp = True
  deque_timeout_usec = 1
  embedding_weights_on_gpu_part = 0.5
  gpu_batch_size = 65500
  gpu_num_bundles = 2
  num_staging_batches = 2
  num_staging_threads = 4
  server_target_qps = 24000
  use_jemalloc = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AzureT4x1HighAccuracy(AzureT4x1):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AzureT4x1Triton(AzureT4x1):
  use_triton = True
  buffer_manager_thread_count = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AzureT4x1HighAccuracyTriton(AzureT4x1HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AzureT4x4(ServerGPUBaseConfig):
  system = KnownSystem.AzureT4x4
  enable_interleaved_top_mlp = True
  deque_timeout_usec = 1
  embedding_weights_on_gpu_part = 0.5
  gpu_batch_size = 65500
  gpu_num_bundles = 2
  num_staging_batches = 2
  num_staging_threads = 4
  server_target_qps = 24000
  use_jemalloc = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AzureT4x4HighAccuracy(AzureT4x4):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AzureT4x4Triton(AzureT4x4):
  use_triton = True
  buffer_manager_thread_count = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AzureT4x4HighAccuracyTriton(AzureT4x4HighAccuracy):
  use_triton = True
