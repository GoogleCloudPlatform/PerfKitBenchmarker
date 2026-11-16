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
"""Spanner Omni cluster on Kubernetes, deployed with the public Helm chart.

Requires: A container_cluster (one node per Spanner Omni server) and a Linux
client VM in the same VPC, both initialized by PKB.

`BaseSpannerOmniCluster` holds the cloud-agnostic logic for a multi-zone
Spanner Omni cluster on Kubernetes. Provider subclasses (e.g.
`providers/gcp/gcp_spanner_omni_cluster.py`) supply the license key and storage
class.
"""

from typing import Any

from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.resources import spanner_omni_cluster_spec
from perfkitbenchmarker.resources.container_service import container_cluster


class BaseSpannerOmniCluster(resource.BaseResource):
  """A multi-zone Spanner Omni cluster deployed on Kubernetes via Helm.

  Provider subclasses set `CLOUD` and `STORAGE_CLASS`; they may override
  `CheckSpecPrerequisites`.

  Attributes:
    spec: The `SpannerOmniClusterSpec`.
    container_cluster: The Kubernetes cluster hosting Spanner Omni.
    client_vm: The client VM running the Spanner CLI (for certificates);
      attached by `SetVms` from the `client_vm_group` VM group.
    database_name: The PostgreSQL-dialect database to create.
  """

  RESOURCE_TYPE = 'BaseSpannerOmniCluster'
  REQUIRED_ATTRS = ['CLOUD']

  # Kubernetes storage class for the data and logs PVCs.
  STORAGE_CLASS: str

  def __init__(
      self,
      omni_spec: spanner_omni_cluster_spec.SpannerOmniClusterSpec,
      cluster: container_cluster.BaseContainerCluster,
  ):
    super().__init__()
    self.spec = omni_spec
    self.container_cluster = cluster
    self._client_vm: virtual_machine.BaseVirtualMachine | None = None
    self.database_name = omni_spec.database_name

  def SetVms(
      self, vm_groups: dict[str, list[virtual_machine.BaseVirtualMachine]]
  ) -> None:
    """Attaches the client VM from the spec's `client_vm_group` VM group.

    Args:
      vm_groups: The benchmark's VM groups, keyed by group name.

    Raises:
      errors.Config.InvalidValue: If the client VM group is missing or empty.
    """
    group_name = self.spec.client_vm_group
    client_vms = vm_groups.get(group_name)
    if not client_vms:
      raise errors.Config.InvalidValue(
          f'spanner_omni_cluster requires the {group_name!r} vm_group.'
      )
    self._client_vm = client_vms[0]

  @property
  def client_vm(self) -> virtual_machine.BaseVirtualMachine:
    """The client VM attached by `SetVms`.

    Raises:
      errors.Resource.CreationError: If `SetVms` has not attached a client VM.
    """
    if self._client_vm is None:
      raise errors.Resource.CreationError(
          'Spanner Omni cluster has no client VM; call SetVms(vm_groups)'
          ' before using it.'
      )
    return self._client_vm

  @classmethod
  def CheckSpecPrerequisites(
      cls, omni_spec: spanner_omni_cluster_spec.SpannerOmniClusterSpec
  ) -> None:
    """Verifies spec prerequisites before provisioning.

    Provider subclasses extend this (calling `super()`) to validate
    provider-specific options such as the `license_uri` format.

    Args:
      omni_spec: The decoded Spanner Omni cluster spec.

    Raises:
      errors.Config.InvalidValue: If no license key location is configured.
    """
    if not omni_spec.license_uri:
      raise errors.Config.InvalidValue('spanner_omni_license_uri must be set.')

  def CheckPrerequisites(self) -> None:
    """Called by `BenchmarkSpec.CheckPrerequisites` before provisioning."""
    super().CheckPrerequisites()
    type(self).CheckSpecPrerequisites(self.spec)
    self._ValidateTopology()

  def _ClusterZones(self) -> list[str]:
    """Returns the Kubernetes cluster's configured node zones."""
    # Only GKE clusters are supported; they expose `zones: list[str]` (empty
    # for regional clusters, where `zone` holds the region).
    return list(getattr(self.container_cluster, 'zones', None) or [])

  def _NodesPerZone(self) -> int | None:
    """Returns the Kubernetes cluster's node count per zone, if known."""
    return self.container_cluster.num_nodes or None

  def _ValidateTopology(self) -> None:
    """Validates zones, replicas_per_zone, and root_servers_per_zone."""
    cluster_zones = self._ClusterZones()
    if self.spec.zones:
      if not cluster_zones:
        raise errors.Config.InvalidValue(
            'spanner_omni_zones cannot be validated against a regional'
            ' container_cluster with no explicit node zones. Configure zonal'
            ' container_cluster zones instead.'
        )
      unknown_zones = sorted(set(self.spec.zones) - set(cluster_zones))
      if unknown_zones:
        raise errors.Config.InvalidValue(
            f'spanner_omni_zones {unknown_zones} must be a subset of'
            f' container_cluster zones {cluster_zones}.'
        )
    if not self.zones:
      raise errors.Config.InvalidValue(
          'Spanner Omni cluster has no zones configured. Specify zonal'
          ' container_cluster zones (not a regional cluster) or set'
          ' --spanner_omni_zones.'
      )
    nodes_per_zone = self._NodesPerZone()
    if (
        nodes_per_zone is not None
        and self.spec.replicas_per_zone is not None
        and self.spec.replicas_per_zone != nodes_per_zone
    ):
      raise errors.Config.InvalidValue(
          f'spanner_omni_replicas_per_zone ({self.spec.replicas_per_zone})'
          ' must match container_cluster node count per zone'
          f' ({nodes_per_zone}).'
      )
    if self.root_servers_per_zone > self.replicas_per_zone:
      raise errors.Config.InvalidValue(
          f'spanner_omni_root_servers_per_zone ({self.root_servers_per_zone})'
          ' must not exceed servers per zone'
          f' ({self.replicas_per_zone}).'
      )

  @property
  def zones(self) -> list[str]:
    """Zones of the Spanner Omni cluster (`spec.zones` overrides cluster)."""
    if self.spec.zones:
      return list(self.spec.zones)
    return self._ClusterZones()

  @property
  def replicas_per_zone(self) -> int:
    """Spanner Omni servers per zone (one per Kubernetes node)."""
    if self.spec.replicas_per_zone is not None:
      return self.spec.replicas_per_zone
    return (
        self._NodesPerZone()
        or spanner_omni_cluster_spec.DEFAULT_REPLICAS_PER_ZONE
    )

  @property
  def root_servers_per_zone(self) -> int:
    """Root servers per zone; defaults to min(3, servers per zone)."""
    if self.spec.root_servers_per_zone is not None:
      return self.spec.root_servers_per_zone
    return min(
        spanner_omni_cluster_spec.DEFAULT_ROOT_SERVERS_PER_ZONE,
        self.replicas_per_zone,
    )

  @property
  def cluster_machine_type(self) -> str | None:
    """Machine type of the Kubernetes nodes, from the container_cluster spec."""
    return self.container_cluster.default_nodepool.machine_type

  @property
  def client_zone(self) -> str | None:
    """The client VM's zone, falling back to the first cluster zone."""
    if self._client_vm is not None and self._client_vm.zone:
      return self._client_vm.zone
    zones = self.zones
    return zones[0] if zones else None

  def _Create(self) -> None:
    raise NotImplementedError()

  def _Delete(self) -> None:
    raise NotImplementedError()

  def GetResourceMetadata(self) -> dict[str, Any]:
    """Returns cluster and Spanner Omni configuration metadata."""
    metadata: dict[str, Any] = {}
    client_machine_type = (
        self._client_vm.machine_type if self._client_vm is not None else None
    )
    metadata.update({
        'db_engine': sql_engine_utils.SPANNER_POSTGRES,
        'omni_zones': ','.join(self.zones),
        'omni_chart_uri': self.spec.chart_uri,
        'omni_replicas_per_zone': self.replicas_per_zone,
        'omni_root_servers_per_zone': self.root_servers_per_zone,
        'omni_cpu_request': self.spec.cpu_request,
        'omni_memory_request': self.spec.memory_request,
        'omni_storage_class': self.STORAGE_CLASS,
        'omni_data_disk_size': self.spec.data_disk_size,
        'omni_logs_disk_size': self.spec.logs_disk_size,
        'omni_run_analyze': self.spec.run_analyze,
    })
    optional_metadata = {
        'omni_cluster_machine_type': self.cluster_machine_type,
        'omni_client_machine_type': client_machine_type,
        'omni_client_zone': (
            self.client_zone if self._client_vm is not None else None
        ),
        'omni_version': self.spec.version,
        'omni_chart_version': self.spec.chart_version,
    }
    metadata.update(
        {k: v for k, v in optional_metadata.items() if v is not None}
    )
    return metadata


def GetSpannerOmniClusterClass(
    cloud: str,
) -> type[BaseSpannerOmniCluster]:
  """Returns the `BaseSpannerOmniCluster` subclass registered for `cloud`."""
  return resource.GetResourceClass(  # pyrefly: ignore[bad-return]
      BaseSpannerOmniCluster, CLOUD=cloud
  )
