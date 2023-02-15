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


class T4x1(ServerGPUBaseConfig):
  enable_interleaved = True
  active_sms = 100
  gpu_batch_size = 16
  graphs_max_seqlen = 240
  server_num_issue_query_threads = 0
  server_target_qps = 360
  soft_drop = 0.993
  gemm_plugin_fairshare_cache_size = None
  use_small_tile_gemm_plugin = None


class T4x1HighAccuracy(T4x1):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  server_target_qps = 160
  graph_specs = (
      "(128, 4, 256, 4), (192, 128, 512, 4), (256, 192, 1536, 8), (384, 256,"
      " 2048, 16)"
  )


class T4x1Triton(T4x1):
  server_target_qps = 324
  use_triton = True


class T4x1HighAccuracyTriton(T4x1HighAccuracy):
  server_target_qps = 144
  use_triton = True


class T4x4(T4x1):
  gpu_batch_size = 14
  graphs_max_seqlen = 260
  server_num_issue_query_threads = 8
  server_target_qps = 1100
  soft_drop = 0.992


class T4x4HighAccuracy(T4x4):
  gpu_inference_streams = 1
  precision = "fp16"
  gpu_batch_size = 8
  server_num_issue_query_threads = 4
  server_target_qps = 665
  soft_drop = 0.992


class T4x4Triton(T4x4):
  use_triton = True


class T4x4HighAccuracyTriton(T4x4HighAccuracy):
  use_triton = True


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
