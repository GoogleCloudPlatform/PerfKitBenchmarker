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
class CloudT4x1(ServerGPUBaseConfig):
  system = KnownSystem.CloudT4x1
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
class CloudT4x1HighAccuracy(CloudT4x1):
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
class CloudT4x1Triton(CloudT4x1):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudT4x1HighAccuracyTriton(CloudT4x1HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudT4x4(CloudT4x1):
  system = KnownSystem.CloudT4x4
  gpu_batch_size = 14
  graphs_max_seqlen = 260
  server_num_issue_query_threads = 8
  soft_drop = 0.992


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudT4x4HighAccuracy(CloudT4x4):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  server_num_issue_query_threads = 4
  soft_drop = 0.992


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudT4x4Triton(CloudT4x4):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudT4x4HighAccuracyTriton(CloudT4x4HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudL4x1(ServerGPUBaseConfig):
  system = KnownSystem.CloudL4x1
  gpu_batch_size = 16
  graphs_max_seqlen = 200
  server_num_issue_query_threads = 1
  server_target_qps = 900
  soft_drop = 1.0
  use_small_tile_gemm_plugin = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudL4x1HighAccuracy(CloudL4x1):
  precision = "fp16"


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudL4x1Triton(CloudL4x1):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudL4x1HighAccuracyTriton(CloudL4x1HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudL4x2(CloudL4x1):
  system = KnownSystem.CloudL4x2
  server_num_issue_query_threads = 4


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudL4x2HighAccuracy(CloudL4x2):
  precision = "fp16"


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudL4x2Triton(CloudL4x2):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudL4x2HighAccuracyTriton(CloudL4x2HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudL4x4(CloudL4x1):
  system = KnownSystem.CloudL4x4
  server_num_issue_query_threads = 8


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudL4x4HighAccuracy(CloudL4x4):
  precision = "fp16"


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudL4x4Triton(CloudL4x4):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudL4x4HighAccuracyTriton(CloudL4x4HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudL4x8(CloudL4x1):
  system = KnownSystem.CloudL4x8
  server_num_issue_query_threads = 16


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudL4x8HighAccuracy(CloudL4x8):
  precision = "fp16"


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudL4x8Triton(CloudL4x8):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudL4x8HighAccuracyTriton(CloudL4x8HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10Gx1(ServerGPUBaseConfig):
  system = KnownSystem.A10Gx1
  active_sms = 100
  gpu_batch_size = 16
  graphs_max_seqlen = 240
  server_num_issue_query_threads = 0
  server_target_qps = 900
  soft_drop = 0.993


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10Gx1HighAccuracy(A10Gx1):
  precision = "fp16"
  gpu_batch_size = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10Gx1Triton(A10Gx1):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10Gx1HighAccuracyTriton(A10Gx1HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10Gx4(A10Gx1):
  system = KnownSystem.A10Gx4


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10Gx4HighAccuracy(A10Gx4):
  precision = "fp16"
  gpu_batch_size = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10Gx4Triton(A10Gx4):
  use_triton = True
  max_queue_delay_usec = 9000


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10Gx4HighAccuracyTriton(A10Gx4HighAccuracy):
  use_triton = True


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10Gx8(A10Gx1):
  system = KnownSystem.A10Gx8


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10Gx8HighAccuracy(A10Gx8):
  precision = "fp16"
  gpu_batch_size = 8


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class A10Gx8Triton(A10Gx8):
  use_triton = True
  max_queue_delay_usec = 9000


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class A10Gx8HighAccuracyTriton(A10Gx8HighAccuracy):
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
class H100x1(ServerGPUBaseConfig):
  system = KnownSystem.H100x1
  use_small_tile_gemm_plugin = False
  enable_interleaved = False
  use_graphs = False
  gpu_batch_size = 128
  gpu_copy_streams = 2
  gpu_inference_streams = 2
  server_target_qps = 7500
  server_num_issue_query_threads = 1
  workspace_size = 7516192768


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class H100x8(H100x1):
  system = KnownSystem.H100x8
  gpu_inference_streams = 2
  gpu_copy_streams = 4
  server_target_qps = 7450 * 8


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class H100x1HighAccuracy(H100x1):
  precision = "fp16"
  use_fp8 = True
  use_graphs = False
  gpu_batch_size = 512
  server_target_qps = 6350


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class H100x8HighAccuracy(H100x8):
  precision = "fp16"
  use_fp8 = True
  use_graphs = False
  gpu_batch_size = 512
  gpu_inference_streams = 1
  server_target_qps = 6200 * 8
