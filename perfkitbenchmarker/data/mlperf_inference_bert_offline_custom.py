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
from . import OfflineGPUBaseConfig
from . import PowerSetting


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99, PowerSetting.MaxP
)
class CloudT4x1(OfflineGPUBaseConfig):
  system = KnownSystem.CloudT4x1
  enable_interleaved = True
  use_small_tile_gemm_plugin = False
  gpu_batch_size = 256
  offline_expected_qps = 430


@ConfigRegistry.register(
    HarnessType.Custom, AccuracyTarget.k_99_9, PowerSetting.MaxP
)
class CloudT4x1HighAccuracy(CloudT4x1):
  precision = "fp16"
  gpu_inference_streams = 1


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
class CloudL4x1(OfflineGPUBaseConfig):
  system = KnownSystem.CloudL4x1
  use_small_tile_gemm_plugin = True
  gpu_batch_size = 16
  offline_expected_qps = 1000
  workspace_size = 7516192768


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
