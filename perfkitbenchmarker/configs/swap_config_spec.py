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
"""SwapConfigSpec and decoder for BENCHMARK_CONFIG swap_config declarations.

Declares cloud-agnostic swap options for a GKE/EKS nodepool.
Cloud-specific implementation classes (GkeSwapConfig, EksSwapConfig) live in
their respective provider directories:
  providers/gcp/gcp_swap_config.py  (CLOUD = 'GCP')
  providers/aws/aws_swap_config.py  (CLOUD = 'AWS', deferred to PR #6780)

Use GetSwapConfigClass(cloud) to obtain the correct implementation class.
"""

from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec


class SwapConfigSpec(spec.BaseSpec):
  """Cloud-agnostic swap options for a nodepool.

  Declared in BENCHMARK_CONFIG under nodepools.<name>.swap_config.
  Consumed by the cloud provider's _AddNodeParamsToCmd() to apply
  cloud-specific swap configuration during nodepool creation.

  Common attributes apply to all clouds. GCP-specific attributes (lssd,
  boot_disk_iops, boot_disk_throughput) are ignored by non-GCP providers.
  EKS/AKS support is deferred to PR #6780.

  Attributes:
    enabled: Whether to enable swap on the nodepool (default True).
    swappiness: vm.swappiness sysctl value (0-200, default 100).
    min_free_kbytes: vm.min_free_kbytes sysctl (default 200).
    watermark_scale_factor: vm.watermark_scale_factor sysctl (default 500).
    lssd: True if the nodepool uses local NVMe SSDs for the swap device (GCP).
    lssd_count: Number of local NVMe SSDs — GKE dedicatedLocalSsdProfile (GCP).
    boot_disk_iops: Provisioned IOPS for hyperdisk-balanced (GCP, 0 = not set).
    boot_disk_throughput: Provisioned throughput MiB/s for hyperdisk-balanced (GCP).
  """

  def __init__(self, *args, **kwargs):
    self.enabled: bool = True
    self.swappiness: int = 100
    self.min_free_kbytes: int = 200
    self.watermark_scale_factor: int = 500
    self.lssd: bool = False
    self.lssd_count: int = 0
    self.boot_disk_iops: int = 0
    self.boot_disk_throughput: int = 0
    super().__init__(*args, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'enabled': (
            option_decoders.BooleanDecoder,
            {'default': True},
        ),
        'swappiness': (
            option_decoders.IntDecoder,
            {'default': 100, 'min': 0, 'max': 200},
        ),
        'min_free_kbytes': (
            option_decoders.IntDecoder,
            {'default': 200, 'min': 0},
        ),
        'watermark_scale_factor': (
            option_decoders.IntDecoder,
            {'default': 500, 'min': 0},
        ),
        'lssd': (
            option_decoders.BooleanDecoder,
            {'default': False},
        ),
        'lssd_count': (
            option_decoders.IntDecoder,
            {'default': 0, 'min': 0},
        ),
        'boot_disk_iops': (
            option_decoders.IntDecoder,
            {'default': 0, 'min': 0},
        ),
        'boot_disk_throughput': (
            option_decoders.IntDecoder,
            {'default': 0, 'min': 0},
        ),
    })
    return result


class _SwapConfigDecoder(option_decoders.TypeVerifier):
  """Decodes the swap_config option of a NodepoolSpec."""

  def Decode(self, value, component_full_name, flag_values):
    """Decodes the swap_config dictionary into a SwapConfigSpec.

    Args:
      value: dict. Keys match SwapConfigSpec._GetOptionDecoderConstructions.
      component_full_name: str. Fully qualified name of the parent component.
      flag_values: flags.FlagValues. Runtime flags propagated to BaseSpec.

    Returns:
      SwapConfigSpec instance.

    Raises:
      errors.Config.InvalidValue upon invalid input value.
    """
    super().Decode(value, component_full_name, flag_values)
    return SwapConfigSpec(
        self._GetOptionFullName(component_full_name),
        flag_values=flag_values,
        **value,
    )


def GetSwapConfigClass(cloud: str):
  """Return the swap config implementation class for the given cloud.

  Mirrors the GetContainerClusterClass pattern so callers do not import
  provider-specific modules directly.

  Args:
    cloud: Cloud provider string, e.g. 'GCP' or 'AWS'.

  Returns:
    A BaseSwapConfig subclass with CLOUD == cloud.

  Raises:
    NotImplementedError: if no implementation exists for the given cloud.
  """
  # Import here to avoid circular imports; providers import configs.
  if cloud == 'GCP':
    from perfkitbenchmarker.providers.gcp import gcp_swap_config  # pylint: disable=g-import-not-at-top
    return gcp_swap_config.GkeSwapConfig
  if cloud == 'AWS':
    from perfkitbenchmarker.providers.aws import aws_swap_config  # pylint: disable=g-import-not-at-top
    return aws_swap_config.EksSwapConfig
  raise NotImplementedError(
      f'No SwapConfig implementation for cloud={cloud!r}. '
      'Supported: GCP. AWS deferred to PR #6780.'
  )
