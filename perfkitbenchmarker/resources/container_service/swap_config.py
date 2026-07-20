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
"""BaseSwapConfig: abstract base class for cloud-specific nodepool swap config.

Cloud provider subclasses live in their provider directories:
  providers/gcp/gcp_swap_config.py  — GkeSwapConfig (CLOUD = 'GCP')
  providers/aws/aws_swap_config.py  — EksSwapConfig (CLOUD = 'AWS')

Use GetSwapConfigClass(cloud) to select the correct implementation.
"""

from perfkitbenchmarker import resource


class BaseSwapConfig(resource.BaseResource):
  """Abstract base class for cloud-specific nodepool swap configuration.

  Subclasses (GkeSwapConfig, EksSwapConfig) implement the cloud-specific
  method for applying swap configuration during nodepool creation.

  Common sysctl attributes (vm.swappiness, vm.min_free_kbytes,
  vm.watermark_scale_factor) are shared across all cloud providers.

  _Create() and _Delete() are no-ops by default: the swap config is applied
  as a parameter to nodepool creation, not as a standalone cloud resource.
  Subclasses override as needed (e.g. to clean up temp YAML files).
  """

  RESOURCE_TYPE = 'BaseSwapConfig'
  REQUIRED_ATTRS = ['CLOUD']

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

  def CleanupYaml(self) -> None:
    pass

  @classmethod
  def from_spec(cls, swap_spec) -> 'BaseSwapConfig':
    """Create a BaseSwapConfig subclass from a SwapConfigSpec.

    Subclasses must override this to instantiate with cloud-specific attrs.

    Args:
      swap_spec: The SwapConfigSpec to decode.
    """
    raise NotImplementedError(
        f'{cls.__name__}.from_spec() must be implemented by subclasses.'
    )

  def _Create(self) -> None:
    """No-op: swap config is applied during nodepool creation."""

  def _Delete(self) -> None:
    """No-op: cleaned up when the nodepool is deleted."""


def GetSwapConfigClass(cloud: str):
  """Get the BaseSwapConfig subclass corresponding to 'cloud'."""
  return resource.GetResourceClass(BaseSwapConfig, CLOUD=cloud)
