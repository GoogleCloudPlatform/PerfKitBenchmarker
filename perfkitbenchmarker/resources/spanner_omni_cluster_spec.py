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
"""Spec and flags for a Spanner Omni cluster on Kubernetes.

Decoded from the `spanner_omni_cluster:` block of a BENCHMARK_CONFIG, e.g.:

  spanner_omni_cluster:
    cloud: GCP
    database_name: benchbase
    replicas_per_zone: 3

Most options can be overridden by the matching `--spanner_omni_*` flag (and
`cloud` by `--cloud`).
"""

from typing import Any

from absl import flags
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

FLAGS = flags.FLAGS

ZONES = flags.DEFINE_list(
    'spanner_omni_zones',
    None,
    'Comma-separated subset of the container_cluster zones to deploy Spanner'
    ' Omni onto. Overrides the container_cluster zones. Defaults to the'
    ' container_cluster zones.',
)
CHART_URI = flags.DEFINE_string(
    'spanner_omni_chart_uri',
    'oci://us-docker.pkg.dev/spanner-omni/charts/spanner-omni',
    'Helm chart URI or local directory path for Spanner Omni.',
)
CHART_VERSION = flags.DEFINE_string(
    'spanner_omni_chart_version',
    None,
    'Optional Helm chart version for Spanner Omni. Defaults to None (latest).',
)
OMNI_VERSION = flags.DEFINE_string(
    'spanner_omni_version',
    None,
    'Optional Spanner Omni release version tag for the CLI and Helm image.tag.'
    ' Defaults to None (dynamically resolves the latest release from'
    ' gs://spanner-omni).',
)
DEFAULT_REPLICAS_PER_ZONE: int = 3
REPLICAS_PER_ZONE = flags.DEFINE_integer(
    'spanner_omni_replicas_per_zone',
    None,
    'Number of Spanner Omni servers per zone (must match the'
    ' container_cluster node count per zone). Defaults to the'
    ' container_cluster node count per zone.',
    lower_bound=1,
)
# Helm chart default, capped at the number of servers per zone.
DEFAULT_ROOT_SERVERS_PER_ZONE: int = 3
ROOT_SERVERS_PER_ZONE = flags.DEFINE_integer(
    'spanner_omni_root_servers_per_zone',
    None,
    'Number of Spanner Omni root servers per zone (Helm'
    ' deployment.rootServersPerZone). Defaults to min(3, servers per zone);'
    ' must not exceed the number of servers per zone.',
    lower_bound=1,
)
CPU_REQUEST = flags.DEFINE_string(
    'spanner_omni_cpu_request',
    '7',
    'CPU request and limit per Spanner Omni pod.',
)
MEMORY_REQUEST = flags.DEFINE_string(
    'spanner_omni_memory_request',
    '23Gi',
    'Memory request and limit per Spanner Omni pod.',
)
DATA_DISK_SIZE = flags.DEFINE_string(
    'spanner_omni_data_disk_size',
    '1200Gi',
    'Size of the data PVC per Spanner Omni pod.',
)
LOGS_DISK_SIZE = flags.DEFINE_string(
    'spanner_omni_logs_disk_size',
    '200Gi',
    'Size of the logs PVC per Spanner Omni pod.',
)
BACKUP_DESCRIPTOR_ID = flags.DEFINE_string(
    'spanner_omni_backup_descriptor_id',
    None,
    'Backup descriptor ID in external storage to import and restore from.',
)
RUN_ANALYZE = flags.DEFINE_bool(
    'spanner_omni_run_analyze',
    True,
    'Whether to run ANALYZE on the Spanner Omni database after loading or'
    ' restoring it.',
)
LICENSE_URI = flags.DEFINE_string(
    'spanner_omni_license_uri',
    None,
    'Location of the Spanner Omni license key: a provider secret ID (on GCP,'
    ' a Secret Manager version such as'
    ' projects/<project>/secrets/<secret>/versions/latest) or a local file'
    ' path.',
)

# Maps spec option names to the flags that override them.
_FLAG_OVERRIDES: dict[str, flags.FlagHolder[Any]] = {
    'zones': ZONES,
    'chart_uri': CHART_URI,
    'chart_version': CHART_VERSION,
    'version': OMNI_VERSION,
    'replicas_per_zone': REPLICAS_PER_ZONE,
    'root_servers_per_zone': ROOT_SERVERS_PER_ZONE,
    'cpu_request': CPU_REQUEST,
    'memory_request': MEMORY_REQUEST,
    'data_disk_size': DATA_DISK_SIZE,
    'logs_disk_size': LOGS_DISK_SIZE,
    'backup_descriptor_id': BACKUP_DESCRIPTOR_ID,
    'run_analyze': RUN_ANALYZE,
    'license_uri': LICENSE_URI,
}

_DEFAULT_NONE_STRING = (
    option_decoders.StringDecoder,
    {'default': None, 'none_ok': True},
)


class SpannerOmniClusterSpec(spec.BaseSpec):
  """Configurable options of a Spanner Omni cluster.

  Provider-specific options live in subclasses registered with a `CLOUD`
  attribute (e.g. `GcpSpannerOmniClusterSpec`); see
  `GetSpannerOmniClusterSpecClass`.

  Attributes:
    cloud: Cloud provider hosting the Kubernetes cluster (selects the
      provider-specific `BaseSpannerOmniCluster` subclass).
    client_vm_group: VM group whose first VM runs the Spanner CLI (for TLS
      certificates) and PGAdapter.
    database_name: PostgreSQL-dialect database to create or restore.
    zones: Subset of the container_cluster's configured zones to deploy onto.
      Defaults to all container_cluster zones when unset.
    replicas_per_zone: Spanner Omni servers per zone (must match the
      container_cluster node count per zone). When unset, the resource uses the
      container_cluster node count per zone.
    root_servers_per_zone: Root servers per zone (<= replicas_per_zone). When
      unset, the resource uses min(3, servers per zone).
    cpu_request: CPU request and limit per Spanner Omni pod.
    memory_request: Memory request and limit per Spanner Omni pod.
    data_disk_size: Size of the data PVC per pod.
    logs_disk_size: Size of the logs PVC per pod.
    chart_uri: Helm chart URI or local path.
    chart_version: Optional Helm chart version.
    version: Optional Spanner Omni release; latest public release if unset.
    backup_descriptor_id: Backup descriptor to import from the provider's
      external backup storage and restore.
    run_analyze: Whether to run ANALYZE after loading or restoring.
    license_uri: Location of the license key (provider secret ID or local file
      path).
  """

  SPEC_TYPE = 'SpannerOmniClusterSpec'
  CLOUD = None

  def __init__(
      self,
      component_full_name: str,
      flag_values: flags.FlagValues | None = None,
      **kwargs: Any,
  ):
    self.cloud: str
    self.client_vm_group: str
    self.database_name: str
    self.zones: list[str] | None
    self.replicas_per_zone: int | None
    self.root_servers_per_zone: int | None
    self.cpu_request: str
    self.memory_request: str
    self.data_disk_size: str
    self.logs_disk_size: str
    self.chart_uri: str
    self.chart_version: str | None
    self.version: str | None
    self.backup_descriptor_id: str | None
    self.run_analyze: bool
    self.license_uri: str | None
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls) -> dict[str, Any]:
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'cloud': (option_decoders.StringDecoder, {'default': None}),
        'client_vm_group': (
            option_decoders.StringDecoder,
            {'default': 'clients'},
        ),
        'database_name': (option_decoders.StringDecoder, {'default': 'omni'}),
        'zones': (
            option_decoders.ListDecoder,
            {
                'item_decoder': option_decoders.StringDecoder(),
                'default': None,
                'none_ok': True,
            },
        ),
        'replicas_per_zone': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True, 'min': 1},
        ),
        'root_servers_per_zone': (
            option_decoders.IntDecoder,
            {'default': None, 'none_ok': True, 'min': 1},
        ),
        'cpu_request': (
            option_decoders.StringDecoder,
            {'default': CPU_REQUEST.default},
        ),
        'memory_request': (
            option_decoders.StringDecoder,
            {'default': MEMORY_REQUEST.default},
        ),
        'data_disk_size': (
            option_decoders.StringDecoder,
            {'default': DATA_DISK_SIZE.default},
        ),
        'logs_disk_size': (
            option_decoders.StringDecoder,
            {'default': LOGS_DISK_SIZE.default},
        ),
        'chart_uri': (
            option_decoders.StringDecoder,
            {'default': CHART_URI.default},
        ),
        'chart_version': _DEFAULT_NONE_STRING,
        'version': _DEFAULT_NONE_STRING,
        'backup_descriptor_id': _DEFAULT_NONE_STRING,
        'run_analyze': (
            option_decoders.BooleanDecoder,
            {'default': RUN_ANALYZE.default},
        ),
        'license_uri': _DEFAULT_NONE_STRING,
    })
    return result

  @classmethod
  def _ApplyFlags(
      cls, config_values: dict[str, Any], flag_values: flags.FlagValues
  ) -> None:
    """Applies `--spanner_omni_*` flag overrides and the `--cloud` default."""
    super()._ApplyFlags(config_values, flag_values)
    if flag_values['cloud'].present or 'cloud' not in config_values:
      config_values['cloud'] = flag_values.cloud
    for option, flag in _FLAG_OVERRIDES.items():
      if flag_values[flag.name].present:
        config_values[option] = flag_values[flag.name].value


def GetSpannerOmniClusterSpecClass(
    cloud: str | None,
) -> type[SpannerOmniClusterSpec]:
  """Returns the spec class registered for `cloud` (or the base spec)."""
  return spec.GetSpecClass(  # pyrefly: ignore[bad-return]
      SpannerOmniClusterSpec, CLOUD=cloud
  )
