# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""EKS swap configuration resource (AWS provider) — stub.

Full implementation deferred.  Configures kubelet LimitedSwap
via nodeadm bootstrap configuration on EKS nodes.

See configs/swap_config_spec.py for the cloud-agnostic spec and factory.
"""

from perfkitbenchmarker.resources.container_service import swap_config


class EksSwapConfig(swap_config.BaseSwapConfig):
  """EKS swap configuration for a nodepool.

  Configures kubelet LimitedSwap via nodeadm bootstrap configuration.
  Full implementation deferred.

  Attributes:
    swappiness: vm.swappiness sysctl value (inherited from BaseSwapConfig).
    min_free_kbytes: vm.min_free_kbytes sysctl (inherited).
    watermark_scale_factor: vm.watermark_scale_factor (inherited).
    memory_swap_behavior: kubelet memorySwapBehavior value ('LimitedSwap').
    fail_swap_on: kubelet failSwapOn setting (False to allow swap on EKS).
  """

  CLOUD = 'AWS'
  RESOURCE_TYPE = 'BaseSwapConfig'
  REQUIRED_ATTRS = ['CLOUD']

  def __init__(
      self,
      swappiness: int = 100,
      min_free_kbytes: int = 67584,
      watermark_scale_factor: int = 500,
      memory_swap_behavior: str = 'LimitedSwap',
      fail_swap_on: bool = False,
  ) -> None:
    super().__init__(
        swappiness=swappiness,
        min_free_kbytes=min_free_kbytes,
        watermark_scale_factor=watermark_scale_factor,
    )
    self.memory_swap_behavior = memory_swap_behavior
    self.fail_swap_on = fail_swap_on

  @classmethod
  def from_spec(cls, swap_spec) -> 'EksSwapConfig':
    """Create an EksSwapConfig from a SwapConfigSpec."""
    return cls(
        swappiness=swap_spec.swappiness,
        min_free_kbytes=swap_spec.min_free_kbytes,
        watermark_scale_factor=swap_spec.watermark_scale_factor,
    )

  def _Create(self) -> None:
    """Stub: EKS kubelet LimitedSwap config (deferred)."""
    raise NotImplementedError(
        'EksSwapConfig._Create() is not yet implemented. '
        'EKS kubelet LimitedSwap config via nodeadm is deferred.'
    )

  def GetNodeadmConfig(self) -> str:
    """Return nodeadm bootstrap YAML for kubelet swap settings."""
    return (
        'apiVersion: node.eks.aws/v1alpha1\n'
        'kind: NodeConfig\n'
        'spec:\n'
        '  kubelet:\n'
        '    config:\n'
        f'      memorySwapBehavior: {self.memory_swap_behavior}\n'
        f'      failSwapOn: {str(self.fail_swap_on).lower()}\n'
    )
