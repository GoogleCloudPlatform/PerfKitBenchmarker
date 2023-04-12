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
class T4x1(ServerGPUBaseConfig):
  system = KnownSystem.T4x1
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
class T4x1HighAccuracy(T4x1):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class T4x1Triton(T4x1):
  buffer_manager_thread_count = 8
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class T4x1HighAccuracyTriton(T4x1Triton):
  pass


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class T4x4(T4x1):
  system = KnownSystem.T4x4
  gpu_num_bundles = 1
  num_staging_batches = 4
  num_staging_threads = 4
  use_jemalloc = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class T4x4HighAccuracy(T4x4):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class T4x4Triton(T4x4):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class T4x4HighAccuracyTriton(T4x4Triton):
  pass


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x1(ServerGPUBaseConfig):
  system = KnownSystem.L4x1
  embedding_weights_on_gpu_part = 0.8
  num_staging_batches = 2
  num_staging_threads = 2
  gpu_num_bundles = 2
  gpu_batch_size = 14000
  server_target_qps = 89000
  use_jemalloc = False
  use_small_tile_gemm_plugin = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x1HighAccuracy(L4x1):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x1Triton(L4x1):
  buffer_manager_thread_count = 8
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x1HighAccuracyTriton(L4x1Triton):
  pass


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x2(L4x1):
  system = KnownSystem.L4x2


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x2HighAccuracy(L4x2):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x2Triton(L4x2):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x2HighAccuracyTriton(L4x2Triton):
  pass


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x4(L4x1):
  system = KnownSystem.L4x4


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x4HighAccuracy(L4x4):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x4Triton(L4x4):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x4HighAccuracyTriton(L4x4Triton):
  pass


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x8(L4x1):
  system = KnownSystem.L4x8


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x8HighAccuracy(L4x8):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x8Triton(L4x8):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x8HighAccuracyTriton(L4x8Triton):
  pass


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10Gx1(ServerGPUBaseConfig):
  system = KnownSystem.A10Gx1
  deque_timeout_usec = 1
  embedding_weights_on_gpu_part = 0.8
  gpu_batch_size = 65500
  gpu_num_bundles = 2
  num_staging_batches = 2
  num_staging_threads = 4
  server_target_qps = 68000
  use_jemalloc = True
  use_small_tile_gemm_plugin = True
  gemm_plugin_fairshare_cache_size = 18


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10Gx1HighAccuracy(A10Gx1):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10Gx1Triton(A10Gx1):
  buffer_manager_thread_count = 0
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10Gx1HighAccuracyTriton(A10Gx1Triton):
  pass


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10Gx4(A10Gx1):
  system = KnownSystem.A10Gx4
  gpu_batch_size = 60000
  num_staging_batches = 4
  num_staging_threads = 8
  use_jemalloc = False


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10Gx4HighAccuracy(A10Gx4):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10Gx4Triton(A10Gx4):
  buffer_manager_thread_count = 0
  use_triton = True
  batch_triton_requests = True
  gather_kernel_buffer_threshold = 64


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10Gx4HighAccuracyTriton(A10Gx4Triton):
  pass


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10Gx8(A10Gx1):
  system = KnownSystem.A10Gx8
  gpu_batch_size = 60000
  num_staging_batches = 8
  num_staging_threads = 8
  use_jemalloc = False


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10Gx8HighAccuracy(A10Gx8):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10Gx8Triton(A10Gx8):
  buffer_manager_thread_count = 0
  use_triton = True
  batch_triton_requests = True
  gather_kernel_buffer_threshold = 64


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10Gx8HighAccuracyTriton(A10Gx8Triton):
  pass


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10x1(ServerGPUBaseConfig):
  system = KnownSystem.A10x1
  deque_timeout_usec = 1
  embedding_weights_on_gpu_part = 0.8
  gpu_batch_size = 65500
  gpu_num_bundles = 2
  num_staging_batches = 2
  num_staging_threads = 4
  server_target_qps = 68000
  use_jemalloc = True
  use_small_tile_gemm_plugin = True
  gemm_plugin_fairshare_cache_size = 18


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10x1HighAccuracy(A10x1):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10x1Triton(A10x1):
  buffer_manager_thread_count = 0
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10x1HighAccuracyTriton(A10x1Triton):
  pass


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10x2(A10x1):
  system = KnownSystem.A10x2
  gpu_batch_size = 60000
  num_staging_batches = 2
  num_staging_threads = 8
  use_jemalloc = False


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10x2HighAccuracy(A10x2):
  pass


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10x2Triton(A10x2):
  buffer_manager_thread_count = 0
  use_triton = True
  batch_triton_requests = True
  gather_kernel_buffer_threshold = 64


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10x2HighAccuracyTriton(A10x2Triton):
  pass
