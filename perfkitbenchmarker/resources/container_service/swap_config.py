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
"""Swap configuration as PKB BaseResource: BaseSwapConfig, GkeSwapConfig, EksSwapConfig.

These resources encapsulate cloud-specific swap configuration for GKE and EKS
nodepools. They are referenced via NodepoolSpec.swap_config (declared in the
benchmark BENCHMARK_CONFIG YAML) and consumed by the cloud provider's
_AddNodeParamsToCmd() during cluster/nodepool creation.

Class hierarchy:
  BaseSwapConfig(BaseResource)   — common sysctl attrs + abstract from_spec()
    GkeSwapConfig(BaseSwapConfig) — linuxConfig YAML for --system-config-from-file
    EksSwapConfig(BaseSwapConfig) — nodeadm kubelet config (deferred to PR #6780)

Usage in BENCHMARK_CONFIG:
  container_cluster:
    nodepools:
      benchmark:
        vm_spec:
          GCP:
            machine_type: n4-highmem-32
            boot_disk_type: hyperdisk-balanced
            boot_disk_size: 500
        swap_config:
          enabled: true
          swappiness: 100
          min_free_kbytes: 67584
          watermark_scale_factor: 500
          boot_disk_iops: 160000
          boot_disk_throughput: 2400

GkeCluster._AddNodeParamsToCmd() creates a GkeSwapConfig from the
SwapConfigSpec and calls WriteLinuxConfigYaml() to obtain the path for
--system-config-from-file. No separate resource.Create() call is needed
for the swap config itself — it is applied as part of nodepool creation.
"""

import logging
import os
import tempfile

from perfkitbenchmarker import resource

# GCP Hyperdisk Balanced constraint: provisioned_iops <= 256 × throughput_MiB_s.
_HYPERDISK_MAX_IOPS_PER_MBPS = 256


class BaseSwapConfig(resource.BaseResource):
  """Abstract base class for cloud-specific nodepool swap configuration.

  Subclasses (GkeSwapConfig, EksSwapConfig) implement the cloud-specific
  method for applying swap configuration during nodepool creation.

  Common sysctl attributes (vm.swappiness, vm.min_free_kbytes,
  vm.watermark_scale_factor) are shared across all cloud providers.

  _Create() and _Delete() are no-ops: the swap config is applied as a
  parameter to nodepool creation, not as a standalone cloud resource.
  """

  RESOURCE_TYPE = 'BaseSwapConfig'
  REQUIRED_ATTRS = []

  def __init__(
      self,
      swappiness: int = 100,
      min_free_kbytes: int = 67584,
      watermark_scale_factor: int = 500,
  ) -> None:
    super().__init__()
    self.swappiness = swappiness
    self.min_free_kbytes = min_free_kbytes
    self.watermark_scale_factor = watermark_scale_factor

  @classmethod
  def from_spec(cls, swap_spec) -> 'BaseSwapConfig':
    """Create a BaseSwapConfig subclass from a SwapConfigSpec.

    Subclasses must override this to instantiate with cloud-specific attrs.
    """
    raise NotImplementedError(
        f'{cls.__name__}.from_spec() must be implemented by subclasses.'
    )

  def _Create(self) -> None:
    """No-op: swap config is applied during nodepool creation."""

  def _Delete(self) -> None:
    """No-op: cleaned up when the nodepool is deleted."""


class GkeSwapConfig(BaseSwapConfig):
  """GKE swap configuration for a nodepool.

  Encapsulates the linuxConfig (swapConfig + sysctl) YAML for
  --system-config-from-file and optional Hyperdisk IOPS/throughput overrides.

  Consumed by GkeCluster._AddNodeParamsToCmd() when nodepool_config.swap_config
  is set.

  Attributes:
    swappiness: vm.swappiness sysctl value (0-200, default 100).
    min_free_kbytes: vm.min_free_kbytes sysctl (default 67584, GKE minimum >= 67584).
    watermark_scale_factor: vm.watermark_scale_factor sysctl (default 500).
    lssd: True if the nodepool uses local NVMe SSDs for swap device.
    lssd_count: Number of local NVMe SSDs (dedicatedLocalSsdProfile.diskCount).
    boot_disk_iops: Provisioned IOPS for hyperdisk-balanced (0 = not set).
    boot_disk_throughput: Provisioned throughput MiB/s for hyperdisk-balanced.
  """

  RESOURCE_TYPE = 'GkeSwapConfig'
  REQUIRED_ATTRS = []

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
    self._CleanupYaml()

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

    tmp = tempfile.NamedTemporaryFile(
        mode='w', suffix='.yaml', delete=False
    )
    try:
      tmp.write(yaml_content)
      tmp.flush()
      self._yaml_path = tmp.name
    finally:
      tmp.close()

    logging.info(
        '[swap_config] Wrote linuxConfig YAML (lssd=%s, lssd_count=%d)'
        ' to %s:\n%s',
        self.lssd,
        self.lssd_count,
        self._yaml_path,
        yaml_content,
    )
    return self._yaml_path

  def ValidHyperdiskThroughput(self) -> int:
    """Return clamped throughput satisfying GCP Hyperdisk Balanced constraints.

    GCP Hyperdisk Balanced requires: provisioned_iops <= 256 × throughput_MiB_s.
    Clamps throughput UP so a mismatched pair cannot abort nodepool creation.
    """
    if not self.boot_disk_iops or not self.boot_disk_throughput:
      return self.boot_disk_throughput
    min_tput = -(-int(self.boot_disk_iops) // _HYPERDISK_MAX_IOPS_PER_MBPS)
    if self.boot_disk_throughput < min_tput:
      logging.warning(
          '[swap_config] boot disk throughput %d MiB/s too low for %d IOPS;'
          ' clamping to minimum %d MiB/s',
          self.boot_disk_throughput,
          self.boot_disk_iops,
          min_tput,
      )
      return min_tput
    return self.boot_disk_throughput

  def CleanupYaml(self) -> None:
    """Delete the linuxConfig tempfile if it was written."""
    if self._yaml_path and os.path.exists(self._yaml_path):
      try:
        os.unlink(self._yaml_path)
        logging.info(
            '[swap_config] Cleaned up YAML tempfile: %s', self._yaml_path
        )
      except OSError:
        pass
      self._yaml_path = None

  def _CleanupYaml(self) -> None:
    self.CleanupYaml()


class EksSwapConfig(BaseSwapConfig):
  """EKS swap configuration for a nodepool (stub).

  Configures kubelet LimitedSwap via nodeadm bootstrap configuration.
  Full implementation deferred to PR #6780.

  Attributes:
    swappiness: vm.swappiness sysctl value (inherited from BaseSwapConfig).
    min_free_kbytes: vm.min_free_kbytes sysctl (inherited from BaseSwapConfig).
    watermark_scale_factor: vm.watermark_scale_factor (inherited from BaseSwapConfig).
    memory_swap_behavior: kubelet memorySwapBehavior value ('LimitedSwap').
    fail_swap_on: kubelet failSwapOn setting (False to allow swap on EKS).
  """

  RESOURCE_TYPE = 'EksSwapConfig'
  REQUIRED_ATTRS = []

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
    """Stub: EKS kubelet LimitedSwap config via nodeadm (deferred to PR #6780)."""
    logging.warning(
        '[swap_config] EksSwapConfig._Create() is a stub. '
        'EKS kubelet LimitedSwap config via nodeadm not yet implemented '
        '(deferred to PR #6780). Swap will not be enabled on EKS nodes.'
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
        '  containerd:\n'
        '    config:\n'
        f'      vm.swappiness: {self.swappiness}\n'
        f'      vm.min_free_kbytes: {self.min_free_kbytes}\n'
        f'      vm.watermark_scale_factor: {self.watermark_scale_factor}\n'
    )
