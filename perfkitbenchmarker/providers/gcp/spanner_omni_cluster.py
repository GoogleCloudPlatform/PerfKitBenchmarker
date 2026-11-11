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
"""GCP (GKE) implementation of the Spanner Omni cluster resource.

Adds the GCP-specific pieces to `BaseSpannerOmniCluster`: the license key from
Secret Manager and the hyperdisk storage class.
"""

import os
from typing import Any

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.gcp import flags as gcp_flags
from perfkitbenchmarker.providers.gcp import util as gcp_util
from perfkitbenchmarker.resources import spanner_omni_cluster
from perfkitbenchmarker.resources import spanner_omni_cluster_spec

_LOCAL_LICENSE_FILENAME: str = 'spanner_omni_license.bin'

# GCP spec options and the (always registered) flags that override them.
_GCP_FLAG_OVERRIDES: dict[str, flags.FlagHolder[Any]] = {
    'license_secret_name': gcp_flags.SPANNER_OMNI_LICENSE_SECRET_NAME,
    'license_secret_project': gcp_flags.SPANNER_OMNI_LICENSE_SECRET_PROJECT,
}


class GcpSpannerOmniClusterSpec(
    spanner_omni_cluster_spec.SpannerOmniClusterSpec
):
  """GCP-specific options of a Spanner Omni cluster.

  Attributes:
    license_secret_name: Secret Manager secret holding the license key.
    license_secret_project: GCP project containing the license secret.
  """

  CLOUD = provider_info.GCP

  def __init__(
      self,
      component_full_name: str,
      flag_values: flags.FlagValues | None = None,
      **kwargs: Any,
  ):
    self.license_secret_name: str | None
    self.license_secret_project: str | None
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls) -> dict[str, Any]:
    result = super()._GetOptionDecoderConstructions()
    none_ok_string = (
        option_decoders.StringDecoder,
        {'default': None, 'none_ok': True},
    )
    result.update({option: none_ok_string for option in _GCP_FLAG_OVERRIDES})
    return result

  @classmethod
  def _ApplyFlags(
      cls, config_values: dict[str, Any], flag_values: flags.FlagValues
  ) -> None:
    super()._ApplyFlags(config_values, flag_values)
    for option, flag in _GCP_FLAG_OVERRIDES.items():
      if flag_values[flag.name].present:
        config_values[option] = flag_values[flag.name].value


class GcpSpannerOmniCluster(spanner_omni_cluster.BaseSpannerOmniCluster):
  """Spanner Omni on GKE."""

  CLOUD = provider_info.GCP
  PLATFORM = 'gke'
  STORAGE_CLASS = 'hyperdisk-balanced-rwo'

  @property
  def gcp_spec(self) -> GcpSpannerOmniClusterSpec:
    if not isinstance(self.spec, GcpSpannerOmniClusterSpec):
      raise TypeError(
          'Expected a GcpSpannerOmniClusterSpec, got'
          f' {type(self.spec).__name__}.'
      )
    return self.spec

  @classmethod
  def CheckSpecPrerequisites(
      cls, omni_spec: spanner_omni_cluster_spec.SpannerOmniClusterSpec
  ) -> None:
    """Verifies the license secret is set.

    Args:
      omni_spec: The decoded Spanner Omni cluster spec.

    Raises:
      errors.Config.InvalidValue: If the license secret is not configured.
    """
    if not isinstance(omni_spec, GcpSpannerOmniClusterSpec):
      raise errors.Config.InvalidValue(
          f'Expected a GcpSpannerOmniClusterSpec, got {type(omni_spec)}.'
      )
    if (
        not omni_spec.license_secret_name
        or not omni_spec.license_secret_project
    ):
      raise errors.Config.InvalidValue(
          'spanner_omni_license_secret_name and'
          ' spanner_omni_license_secret_project must be set.'
      )

  def _FetchLicenseKeyFile(self) -> str:
    """Fetches the license key from Secret Manager into a local temp file."""
    if (
        not self.gcp_spec.license_secret_name
        or not self.gcp_spec.license_secret_project
    ):
      raise errors.Config.InvalidValue(
          'spanner_omni_license_secret_name and'
          ' spanner_omni_license_secret_project must be set.'
      )
    secret_path = os.path.join(vm_util.GetTempDir(), _LOCAL_LICENSE_FILENAME)
    secret_cmd = gcp_util.GcloudCommand(
        None, 'secrets', 'versions', 'access', 'latest'
    )
    secret_cmd.flags['secret'] = self.gcp_spec.license_secret_name
    secret_cmd.flags['project'] = self.gcp_spec.license_secret_project
    secret_cmd.flags['out-file'] = secret_path
    _, stderr, retcode = secret_cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      raise errors.Resource.CreationError(
          'Failed to fetch Spanner Omni license secret'
          f' {self.gcp_spec.license_secret_project}/'
          f'{self.gcp_spec.license_secret_name}: {stderr}'
      )
    return secret_path
