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
"""GKE swap configuration resource (GCP provider).

GkeSwapConfig encapsulates the linuxConfig YAML written for
--system-config-from-file during GKE nodepool creation.  It is selected
automatically when the cluster cloud is GCP via GetSwapConfigClass('GCP').

See configs/swap_config_spec.py for the cloud-agnostic spec and factory.
"""

import logging
import os
import tempfile

from perfkitbenchmarker.resources.container_service import swap_config

# GCP Hyperdisk Balanced constraint: provisioned_iops <= 256 × throughput_MiB_s.
_HYPERDISK_MAX_IOPS_PER_MBPS = 256


class GkeSwapConfig(swap_config.BaseSwapConfig):
  """GKE swap configuration for a nodepool.

  Encapsulates the linuxConfig (swapConfig + sysctl) YAML for
  --system-config-from-file and optional Hyperdisk IOPS/throughput overrides.

  Consumed by GkeCluster._AddNodeParamsToCmd() when nodepool_config.swap_config
  is set.

  Attributes:
    swappiness: vm.swappiness sysctl value (0-200, default 100).
    min_free_kbytes: vm.min_free_kbytes sysctl (default 67584).
    watermark_scale_factor: vm.watermark_scale_factor sysctl (default 500).
    lssd: True if the nodepool uses local NVMe SSDs for swap device.
    lssd_count: Number of local NVMe SSDs (dedicatedLocalSsdProfile.diskCount).
    boot_disk_iops: Provisioned IOPS for hyperdisk-balanced (0 = not set).
    boot_disk_throughput: Provisioned throughput MiB/s for hyperdisk-balanced.
  """

  CLOUD = 'GCP'
  RESOURCE_TYPE = 'GkeSwapConfig'
  REQUIRED_ATTRS = ['CLOUD']

  def __init__(
      self,
      swappiness: int = 100,
      min_free_kbytes: int = 67584,
      watermark_scale_factor: int = 500,
      lssd: bool = False,
      lssd_count: int = 0,
      boot_disk_iops: int = 0,
      boot_disk_throughput: int = 0,
  ) -> None:
    super().__init__(
        swappiness=swappiness,
        min_free_kbytes=min_free_kbytes,
        watermark_scale_factor=watermark_scale_factor,
    )
    self.lssd = lssd
    self.lssd_count = lssd_count
    self.boot_disk_iops = boot_disk_iops
    self.boot_disk_throughput = boot_disk_throughput
    self._yaml_path: str | None = None

  @classmethod
  def from_spec(cls, swap_spec) -> 'GkeSwapConfig':
    """Create a GkeSwapConfig from a SwapConfigSpec decoded from BENCHMARK_CONFIG."""
    return cls(
        swappiness=swap_spec.swappiness,
        min_free_kbytes=swap_spec.min_free_kbytes,
        watermark_scale_factor=swap_spec.watermark_scale_factor,
        lssd=swap_spec.lssd,
        lssd_count=swap_spec.lssd_count,
        boot_disk_iops=swap_spec.boot_disk_iops,
        boot_disk_throughput=swap_spec.boot_disk_throughput,
    )

  def _Delete(self) -> None:
    """Cleans up any written YAML tempfile."""
    self.CleanupYaml()

  def WriteLinuxConfigYaml(self) -> str:
    """Write the GKE linuxConfig YAML to a tempfile; return the path.

    Called by GkeCluster._AddNodeParamsToCmd() to supply
    --system-config-from-file. The caller is responsible for deleting the
    tempfile via CleanupYaml() after the gcloud command completes.

    Per Ajay review r3472513706:
      linuxConfig.swapConfig.enabled=true automatically sets
      kubeletConfig.memorySwapBehavior=LimitedSwap — no need to set
      kubeletConfig explicitly.
    For LSSD machines, dedicatedLocalSsdProfile.diskCount instructs GKE to
    use local NVMe as the swap device.

    Returns:
      Absolute path to the written tempfile.
    """
    if self.lssd and self.lssd_count > 0:
      swap_block = (
          '  swapConfig:\n'
          '    enabled: true\n'
          '    dedicatedLocalSsdProfile:\n'
          f'      diskCount: {self.lssd_count}\n'
      )
    else:
      swap_block = '  swapConfig:\n    enabled: true\n'

    yaml_content = (
        'linuxConfig:\n'
        + swap_block
        + '  sysctl:\n'
        + f'    vm.swappiness: "{self.swappiness}"\n'
        + f'    vm.min_free_kbytes: "{self.min_free_kbytes}"\n'
        + f'    vm.watermark_scale_factor: "{self.watermark_scale_factor}"\n'
    )

    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
    try:
      tmp.write(yaml_content)
      tmp.flush()
      self._yaml_path = tmp.name
    finally:
      tmp.close()

    logging.info(
        '[gcp_swap_config] Wrote linuxConfig YAML (lssd=%s, lssd_count=%d) to %s:\n%s',
        self.lssd,
        self.lssd_count,
        self._yaml_path,
        yaml_content,
    )
    return self._yaml_path

  def ValidHyperdiskThroughput(self) -> int:
    """Return clamped throughput satisfying GCP Hyperdisk Balanced constraints.

    GCP Hyperdisk Balanced requires: provisioned_iops <= 256 × throughput_MiB_s.
    Raises ValueError if throughput is set but too low — callers should either
    clamp or set correct values in BENCHMARK_CONFIG.

    Returns:
      Validated throughput value (unchanged if constraints are satisfied).

    Raises:
      ValueError: if boot_disk_throughput is positive but violates the
        256×throughput >= IOPS constraint.
    """
    if not self.boot_disk_iops or not self.boot_disk_throughput:
      return self.boot_disk_throughput
    min_tput = -(-int(self.boot_disk_iops) // _HYPERDISK_MAX_IOPS_PER_MBPS)
    if self.boot_disk_throughput < min_tput:
      raise ValueError(
          f'boot_disk_throughput={self.boot_disk_throughput} MiB/s is too low'
          f' for boot_disk_iops={self.boot_disk_iops}: GCP requires'
          f' iops <= {_HYPERDISK_MAX_IOPS_PER_MBPS} x throughput_MiB_s'
          f' (minimum throughput = {min_tput} MiB/s).'
          f' Fix boot_disk_throughput in BENCHMARK_CONFIG.'
      )
    return self.boot_disk_throughput

  def CleanupYaml(self) -> None:
    """Delete the linuxConfig tempfile if it was written."""
    if self._yaml_path and os.path.exists(self._yaml_path):
      os.unlink(self._yaml_path)
      logging.info(
          '[gcp_swap_config] Cleaned up YAML tempfile: %s', self._yaml_path
      )
      self._yaml_path = None
