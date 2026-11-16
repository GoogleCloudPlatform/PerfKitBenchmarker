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

Adds the GCP-specific storage class and `license_uri` validation (a Secret
Manager secret version or a local file) to `BaseSpannerOmniCluster`.
"""

import os
import re

from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
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
  STORAGE_CLASS = 'hyperdisk-balanced-rwo'

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
