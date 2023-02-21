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
"""MLPerf Inference custom system BERT configuration."""
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
  enable_interleaved = True
  active_sms = 100
  gpu_batch_size = 16
  graphs_max_seqlen = 240
  server_num_issue_query_threads = 0
  server_target_qps = 360
  soft_drop = 0.993
  gemm_plugin_fairshare_cache_size = None
  use_small_tile_gemm_plugin = None


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class T4x1HighAccuracy(T4x1):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  graph_specs = (
      "(128, 4, 256, 4), (192, 128, 512, 4), (256, 192, 1536, 8), (384, 256,"
      " 2048, 16)"
  )


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class T4x1Triton(T4x1):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class T4x1HighAccuracyTriton(T4x1HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class T4x4(T4x1):
  system = KnownSystem.T4x4
  gpu_batch_size = 14
  graphs_max_seqlen = 260
  server_num_issue_query_threads = 8
  soft_drop = 0.992


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class T4x4HighAccuracy(T4x4):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  server_num_issue_query_threads = 4
  soft_drop = 0.992


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class T4x4Triton(T4x4):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class T4x4HighAccuracyTriton(T4x4HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x1(ServerGPUBaseConfig):
  system = KnownSystem.L4x1
  enable_interleaved = True
  active_sms = 100
  gpu_batch_size = 16
  graphs_max_seqlen = 240
  server_num_issue_query_threads = 0
  server_target_qps = 360
  soft_drop = 0.993
  gemm_plugin_fairshare_cache_size = None
  use_small_tile_gemm_plugin = None


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x1HighAccuracy(L4x1):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x1Triton(L4x1):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x1HighAccuracyTriton(L4x1HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x2(L4x1):
  system = KnownSystem.L4x2
  gpu_batch_size = 14
  graphs_max_seqlen = 260
  server_num_issue_query_threads = 4
  soft_drop = 0.992


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x2HighAccuracy(L4x2):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  server_num_issue_query_threads = 2
  soft_drop = 0.992


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x2Triton(L4x2):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x2HighAccuracyTriton(L4x2HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x4(L4x1):
  system = KnownSystem.L4x4
  gpu_batch_size = 14
  graphs_max_seqlen = 260
  server_num_issue_query_threads = 8
  soft_drop = 0.992


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x4HighAccuracy(L4x4):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  server_num_issue_query_threads = 4
  soft_drop = 0.992


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x4Triton(L4x4):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x4HighAccuracyTriton(L4x4HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x8(L4x1):
  system = KnownSystem.L4x8
  gpu_batch_size = 14
  graphs_max_seqlen = 260
  server_num_issue_query_threads = 16
  soft_drop = 0.992


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x8HighAccuracy(L4x8):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  server_num_issue_query_threads = 8
  soft_drop = 0.992


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class L4x8Triton(L4x8):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class L4x8HighAccuracyTriton(L4x8HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10x1(ServerGPUBaseConfig):
  system = KnownSystem.A10x1
  active_sms = 100
  gpu_batch_size = 16
  graphs_max_seqlen = 240
  server_num_issue_query_threads = 0
  server_target_qps = 900
  soft_drop = 0.993


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10x1HighAccuracy(A10x1):
  precision = "fp16"
  gpu_batch_size = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10x1Triton(A10x1):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10x1HighAccuracyTriton(A10x1HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10x2(A10x1):
  system = KnownSystem.A10x2


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10x2HighAccuracy(A10x2):
  precision = "fp16"
  gpu_batch_size = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10x2Triton(A10x2):
  use_triton = True
  max_queue_delay_usec = 9000


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10x2HighAccuracyTriton(A10x2HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10x4(A10x1):
  system = KnownSystem.A10x4


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10x4HighAccuracy(A10x4):
  precision = "fp16"
  gpu_batch_size = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10x4Triton(A10x4):
  use_triton = True
  max_queue_delay_usec = 9000


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10x4HighAccuracyTriton(A10x4HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10x8(A10x1):
  system = KnownSystem.A10x8


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10x8HighAccuracy(A10x8):
  precision = "fp16"
  gpu_batch_size = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10x8Triton(A10x8):
  use_triton = True
  max_queue_delay_usec = 9000


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10x8HighAccuracyTriton(A10x8HighAccuracy):
  use_triton = True
