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
from . import OfflineGPUBaseConfig
from . import PowerSetting


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudT4x1(OfflineGPUBaseConfig):
  """Default T4 config for DLRM inference."""

  system = KnownSystem.CloudT4x1
  enable_interleaved_top_mlp = True
  output_padding_granularity = 32
  use_small_tile_gemm_plugin = False
  complete_threads = 2
  deque_timeout_usec = 1
  embedding_weights_on_gpu_part = 0.5
  gpu_batch_size = 262100
  offline_expected_qps = 34000
  max_pairs_per_staging_thread = 262100
  num_staging_batches = 4
  num_staging_threads = 4
  use_jemalloc = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudT4x1HighAccuracy(CloudT4x1):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudT4x1Triton(CloudT4x1):
  batch_triton_requests = True
  buffer_manager_thread_count = 8
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudT4x1HighAccuracyTriton(CloudT4x1Triton):
  pass


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudL4x1(OfflineGPUBaseConfig):
  system = KnownSystem.CloudL4x1
  embedding_weights_on_gpu_part = 0.8
  complete_threads = 1
  deque_timeout_usec = 1
  gpu_batch_size = 14000
  offline_expected_qps = 93000
  max_pairs_per_staging_thread = 262100
  num_staging_batches = 8
  num_staging_threads = 8
  use_jemalloc = True
  use_small_tile_gemm_plugin = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudL4x1HighAccuracy(CloudL4x1):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudL4x1Triton(CloudL4x1):
  buffer_manager_thread_count = 8
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudL4x1HighAccuracyTriton(CloudL4x1Triton):
  pass
