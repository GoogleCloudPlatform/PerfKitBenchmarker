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
class GcpT4x1(ServerGPUBaseConfig):
  system = KnownSystem.GcpT4x1

  enable_interleaved = True
  active_sms = 100
  gpu_batch_size = 16
  graphs_max_seqlen = 240
  server_num_issue_query_threads = 0
  server_target_qps = 269.0
  soft_drop = 0.993
  gemm_plugin_fairshare_cache_size = None
  use_small_tile_gemm_plugin = None


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class GcpT4x1HighAccuracy(GcpT4x1):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  server_target_qps = 269.0
  graph_specs = (
      "(128, 4, 256, 4), (192, 128, 512, 4), (256, 192, 1536, 8), (384, 256,"
      " 2048, 16)"
  )


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class GcpT4x1Triton(GcpT4x1):
  use_triton = True
  server_target_qps = 269.0


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class GcpT4x1HighAccuracyTriton(GcpT4x1HighAccuracy):
  use_triton = True
  server_target_qps = 269.0


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class GcpT4x4(ServerGPUBaseConfig):
  system = KnownSystem.GcpT4x4

  enable_interleaved = True
  active_sms = 100
  gpu_batch_size = 16
  graphs_max_seqlen = 240
  server_num_issue_query_threads = 0
  server_target_qps = 269.0
  soft_drop = 0.993
  gemm_plugin_fairshare_cache_size = None
  use_small_tile_gemm_plugin = None


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class GcpT4x4HighAccuracy(GcpT4x4):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  server_target_qps = 269.0
  graph_specs = (
      "(128, 4, 256, 4), (192, 128, 512, 4), (256, 192, 1536, 8), (384, 256,"
      " 2048, 16)"
  )


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class GcpT4x4Triton(GcpT4x4):
  use_triton = True
  server_target_qps = 269.0


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class GcpT4x4HighAccuracyTriton(GcpT4x4HighAccuracy):
  use_triton = True
  server_target_qps = 269.0


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AwsT4x1(ServerGPUBaseConfig):
  system = KnownSystem.AwsT4x1

  enable_interleaved = True
  active_sms = 100
  gpu_batch_size = 16
  graphs_max_seqlen = 240
  server_num_issue_query_threads = 0
  server_target_qps = 269.0
  soft_drop = 0.993
  gemm_plugin_fairshare_cache_size = None
  use_small_tile_gemm_plugin = None


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AwsT4x1HighAccuracy(AwsT4x1):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  server_target_qps = 269.0
  graph_specs = (
      "(128, 4, 256, 4), (192, 128, 512, 4), (256, 192, 1536, 8), (384, 256,"
      " 2048, 16)"
  )


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AwsT4x1Triton(AwsT4x1):
  use_triton = True
  server_target_qps = 269.0


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AwsT4x1HighAccuracyTriton(AwsT4x1HighAccuracy):
  use_triton = True
  server_target_qps = 269.0


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AwsT4x4(ServerGPUBaseConfig):
  system = KnownSystem.AwsT4x4

  enable_interleaved = True
  active_sms = 100
  gpu_batch_size = 16
  graphs_max_seqlen = 240
  server_num_issue_query_threads = 0
  server_target_qps = 269.0
  soft_drop = 0.993
  gemm_plugin_fairshare_cache_size = None
  use_small_tile_gemm_plugin = None


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AwsT4x4HighAccuracy(AwsT4x4):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  server_target_qps = 269.0
  graph_specs = (
      "(128, 4, 256, 4), (192, 128, 512, 4), (256, 192, 1536, 8), (384, 256,"
      " 2048, 16)"
  )


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AwsT4x4Triton(AwsT4x4):
  use_triton = True
  server_target_qps = 269.0


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AwsT4x4HighAccuracyTriton(AwsT4x4HighAccuracy):
  use_triton = True
  server_target_qps = 269.0


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AzureT4x1(ServerGPUBaseConfig):
  system = KnownSystem.AzureT4x1

  enable_interleaved = True
  active_sms = 100
  gpu_batch_size = 16
  graphs_max_seqlen = 240
  server_num_issue_query_threads = 0
  server_target_qps = 269.0
  soft_drop = 0.993
  gemm_plugin_fairshare_cache_size = None
  use_small_tile_gemm_plugin = None


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AzureT4x1HighAccuracy(AzureT4x1):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  server_target_qps = 269.0
  graph_specs = (
      "(128, 4, 256, 4), (192, 128, 512, 4), (256, 192, 1536, 8), (384, 256,"
      " 2048, 16)"
  )


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AzureT4x1Triton(AzureT4x1):
  use_triton = True
  server_target_qps = 269.0


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AzureT4x1HighAccuracyTriton(AzureT4x1HighAccuracy):
  use_triton = True
  server_target_qps = 269.0


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AzureT4x4(ServerGPUBaseConfig):
  system = KnownSystem.AzureT4x4

  enable_interleaved = True
  active_sms = 100
  gpu_batch_size = 16
  graphs_max_seqlen = 240
  server_num_issue_query_threads = 0
  server_target_qps = 269.0
  soft_drop = 0.993
  gemm_plugin_fairshare_cache_size = None
  use_small_tile_gemm_plugin = None


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AzureT4x4HighAccuracy(AzureT4x4):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  server_target_qps = 269.0
  graph_specs = (
      "(128, 4, 256, 4), (192, 128, 512, 4), (256, 192, 1536, 8), (384, 256,"
      " 2048, 16)"
  )


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99, PowerSetting.MaxP
)
class AzureT4x4Triton(AzureT4x4):
  use_triton = True
  server_target_qps = 269.0


@ConfigRegistry.register(
    HarnessType.Triton, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class AzureT4x4HighAccuracyTriton(AzureT4x4HighAccuracy):
  use_triton = True
  server_target_qps = 269.0
