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
Secret Manager (or a local file) and the hyperdisk storage class.
"""

from collections.abc import Sequence
import os
import re
import string

from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker.providers.gcp import util as gcp_util
from perfkitbenchmarker.resources import spanner_omni_cluster
from perfkitbenchmarker.resources import spanner_omni_cluster_spec

# Secret Manager secret version resource name, e.g.
# projects/my-project/secrets/my-secret/versions/latest.
_SECRET_MANAGER_VERSION_RE = re.compile(
    r'^projects/[^/]+/secrets/[^/]+/versions/[^/]+$'
)


def _IsSecretManagerVersion(license_uri: str) -> bool:
  """Returns True if `license_uri` is a Secret Manager secret version name."""
  return bool(_SECRET_MANAGER_VERSION_RE.match(license_uri))


class GcpSpannerOmniClusterSpec(
    spanner_omni_cluster_spec.SpannerOmniClusterSpec
):
  """Spanner Omni cluster spec for GCP (GKE)."""

  CLOUD = provider_info.GCP


class GcpSpannerOmniCluster(spanner_omni_cluster.BaseSpannerOmniCluster):
  """Spanner Omni on GKE."""

  CLOUD = provider_info.GCP
  HELM_PLATFORM = 'gke'
  STORAGE_CLASS = 'hyperdisk-balanced-rwo'

  def _GetRegionFromZone(self, zone: str) -> str:
    return gcp_util.GetRegionFromZone(zone)

  def _BuildZoneShortNames(self, zones: Sequence[str]) -> dict[str, str]:
    """Maps each GCP zone to a unique 1-letter Helm `shortName`.

    Uses the zone's suffix after its region (e.g. `a` for `us-central1-a`) when
    it is a single unused lowercase letter; otherwise picks the next free
    letter.

    Args:
      zones: Sequence of GCP zone names.

    Returns:
      Mapping from zone name to its unique 1-letter short name.
    """
    zone_short_names: dict[str, str] = {}
    used_short_names: set[str] = set()
    for zone in zones:
      region = self._GetRegionFromZone(zone)
      suffix = zone.removeprefix(region).lstrip('-')
      if (
          len(suffix) == 1
          and suffix in string.ascii_lowercase
          and suffix not in used_short_names
      ):
        short_name = suffix
      else:
        short_name = next(
            c for c in string.ascii_lowercase if c not in used_short_names
        )
      used_short_names.add(short_name)
      zone_short_names[zone] = short_name
    return zone_short_names

  @classmethod
  def CheckSpecPrerequisites(
      cls, omni_spec: spanner_omni_cluster_spec.SpannerOmniClusterSpec
  ) -> None:
    """Verifies `license_uri` is a Secret Manager version or a local file.

    Args:
      omni_spec: The decoded Spanner Omni cluster spec.

    Raises:
      errors.Config.InvalidValue: If the license key location is missing or
        not supported on GCP.
    """
    super().CheckSpecPrerequisites(omni_spec)
    license_uri = omni_spec.license_uri or ''  # Non-empty: checked by super().
    if not _IsSecretManagerVersion(license_uri) and not os.path.isfile(
        license_uri
    ):
      raise errors.Config.InvalidValue(
          f'spanner_omni_license_uri {license_uri!r} must be a Secret Manager'
          ' secret version (projects/<project>/secrets/<secret>/versions/'
          '<version>) or an existing local file.'
      )

  def _FetchLicenseKeyFile(self) -> str:
    """Fetches the license key from Secret Manager (or a local file).

    Returns:
      Path of an owner-only local temp file holding the license key; the caller
      deletes it.

    Raises:
      errors.Resource.CreationError: If the Secret Manager access fails.
    """
    license_uri = self.spec.license_uri or ''
    if not _IsSecretManagerVersion(license_uri):
      return super()._FetchLicenseKeyFile()
    secret_path = self._NewLicenseTempFile()
    secret_cmd = gcp_util.GcloudCommand(
        None, 'secrets', 'versions', 'access', license_uri
    )
    secret_cmd.flags['out-file'] = secret_path
    _, stderr, retcode = secret_cmd.Issue(raise_on_failure=False)
    if retcode != 0:
      if os.path.exists(secret_path):
        os.remove(secret_path)
      raise errors.Resource.CreationError(
          f'Failed to fetch Spanner Omni license secret {license_uri}: {stderr}'
      )
    return secret_path
