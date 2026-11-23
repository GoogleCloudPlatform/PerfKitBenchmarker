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
Secret Manager (or a local file), the hyperdisk storage class, and GCS external
backup storage.
`CheckSpecPrerequisites` runs a runner-side visibility/object-listing pre-flight
check on the GCS bucket; the Spanner pods read the bucket using a
pre-configured GKE Workload Identity Federation principal (when enabled on the
cluster) or the GKE node service account.
"""

from collections.abc import Sequence
import os
import re
import string
from typing import Any

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.providers.gcp import util as gcp_util
from perfkitbenchmarker.resources import spanner_omni_cluster
from perfkitbenchmarker.resources import spanner_omni_cluster_spec

_SPANNER_OMNI_GCS_BACKUP_BUCKET = flags.DEFINE_string(
    'spanner_omni_gcs_backup_bucket',
    None,
    'GCS bucket name for Spanner Omni external backup storage.',
)

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
  """GCP-specific options of a Spanner Omni cluster.

  Attributes:
    gcs_backup_bucket: GCS bucket used as external backup storage.
  """

  CLOUD = provider_info.GCP

  def __init__(
      self,
      component_full_name: str,
      flag_values: flags.FlagValues | None = None,
      **kwargs: Any,
  ):
    self.gcs_backup_bucket: str | None
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)
    if self.gcs_backup_bucket:
      # Accept `gs://bucket/` as well as a bare bucket name, but reject object
      # path prefixes (`gs://bucket/prefix`).
      self.gcs_backup_bucket = (
          self.gcs_backup_bucket.strip().removeprefix('gs://').rstrip('/')
          or None
      )
      if self.gcs_backup_bucket and '/' in self.gcs_backup_bucket:
        raise errors.Config.InvalidValue(
            'spanner_omni_gcs_backup_bucket must be a bucket name without an'
            f' object path prefix, got {self.gcs_backup_bucket!r}.'
        )
    if self.backup_descriptor_id and not self.gcs_backup_bucket:
      raise errors.Config.InvalidValue(
          'spanner_omni_gcs_backup_bucket must be set when restoring a backup.'
      )

  @classmethod
  def _GetOptionDecoderConstructions(cls) -> dict[str, Any]:
    result = super()._GetOptionDecoderConstructions()
    result['gcs_backup_bucket'] = (
        option_decoders.StringDecoder,
        {'default': None, 'none_ok': True},
    )
    return result

  @classmethod
  def _ApplyFlags(
      cls, config_values: dict[str, Any], flag_values: flags.FlagValues
  ) -> None:
    super()._ApplyFlags(config_values, flag_values)
    if flag_values[_SPANNER_OMNI_GCS_BACKUP_BUCKET.name].present:
      config_values['gcs_backup_bucket'] = flag_values[
          _SPANNER_OMNI_GCS_BACKUP_BUCKET.name
      ].value


def _VerifyGcsBackupBucketAccess(bucket_name: str) -> None:
  """Verifies the runner can list objects in the GCS backup bucket.

  Backup descriptor IDs are base64-encoded protos rather than GCS object paths
  (Spanner Omni nests backup objects under `backup/...`), so this check only
  verifies bucket existence and list access. The Spanner pods read backup
  objects using the GKE node service account or Workload Identity Federation
  principal, which must also be granted access to `gs://<bucket_name>`.

  Args:
    bucket_name: GCS bucket name containing the Spanner Omni backup.

  Raises:
    errors.Setup.InvalidConfigurationError: If `gcloud storage ls` fails
      (e.g., bucket does not exist or runner lacks list permissions).
  """
  bucket_uri = f'gs://{bucket_name}'
  storage_cmd = gcp_util.GcloudCommand(None, 'storage', 'ls', bucket_uri)
  # GcloudCommand defaults --format=json, which `gcloud storage ls` rejects.
  storage_cmd.flags['format'] = 'gsutil'
  _, stderr, retcode = storage_cmd.Issue(raise_on_failure=False)
  if retcode != 0:
    raise errors.Setup.InvalidConfigurationError(
        f'Cannot list Spanner Omni GCS backup bucket {bucket_uri}. Verify the'
        ' bucket exists and that the runner service account (and GKE node'
        ' service account / Workload Identity principal for namespace'
        f' `{spanner_omni_cluster.NAMESPACE}`) have roles/storage.objectUser'
        f' and roles/storage.legacyBucketReader (stderr: {stderr}).'
    )


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

  @property
  def gcp_spec(self) -> GcpSpannerOmniClusterSpec:
    if not isinstance(self.spec, GcpSpannerOmniClusterSpec):
      raise errors.Config.InvalidValue(
          f'Expected a GcpSpannerOmniClusterSpec, got {type(self.spec)}.'
      )
    return self.spec

  @classmethod
  def CheckSpecPrerequisites(
      cls, omni_spec: spanner_omni_cluster_spec.SpannerOmniClusterSpec
  ) -> None:
    """Verifies the license key location and the backup bucket.

    Args:
      omni_spec: The decoded Spanner Omni cluster spec.

    Raises:
      errors.Config.InvalidValue: If the license key location is missing or
        not supported on GCP.
      errors.Setup.InvalidConfigurationError: If the backup bucket check fails.
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
    if not isinstance(omni_spec, GcpSpannerOmniClusterSpec):
      raise errors.Config.InvalidValue(
          f'Expected a GcpSpannerOmniClusterSpec, got {type(omni_spec)}.'
      )
    if omni_spec.gcs_backup_bucket:
      _VerifyGcsBackupBucketAccess(omni_spec.gcs_backup_bucket)

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

  def GetResourceMetadata(self) -> dict[str, Any]:
    metadata = super().GetResourceMetadata()
    if self.gcp_spec.gcs_backup_bucket:
      metadata['omni_gcs_backup_bucket'] = self.gcp_spec.gcs_backup_bucket
    return metadata

  def _ExternalStorageCliArgs(self) -> list[str]:
    if not self.gcp_spec.gcs_backup_bucket:
      return []
    return [f'--gcs-bucket-name={self.gcp_spec.gcs_backup_bucket}']
