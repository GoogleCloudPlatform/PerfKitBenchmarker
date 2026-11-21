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

`BaseSpannerOmniCluster` holds the cloud-agnostic logic: it deploys a
multi-zone Spanner Omni cluster via Helm in secure TLS mode with password
authentication and manages PostgreSQL-dialect databases through the Spanner
Omni CLI running inside a Spanner pod. Provider subclasses (e.g.
`providers/gcp/gcp_spanner_omni_cluster.py`) supply the license key and storage
class.
"""

import abc
import base64
import collections
from collections.abc import Iterator, Sequence
import contextlib
import json
import logging
import os
import re
import shutil
import tempfile
from typing import Any
import urllib.request

from perfkitbenchmarker import db_util
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources import spanner_omni_cluster_spec
from perfkitbenchmarker.resources.container_service import container_cluster
from perfkitbenchmarker.resources.container_service import helm
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands
import yaml

RELEASE_NAME: str = 'spanner-omni'
NAMESPACE: str = 'spanner-ns'
_DATABASE_DIALECT: str = 'POSTGRESQL'
_ADMIN_USERNAME: str = 'admin'
_SPANNER_GRPC_PORT: int = 15000
_SPANNER_POD_WAIT_TIMEOUT_SEC: int = 1200
_SPANNER_ANALYZE_TIMEOUT_SEC: int = 60 * 60 * 6
_SPANNER_CLI_DEFAULT_TIMEOUT_SEC: int = 120
_KUBECTL_DEFAULT_TIMEOUT_SEC: int = 60
# --async CLI commands (DDL, backup import/restore) return right after the
# operation is submitted.
_SPANNER_ASYNC_SUBMIT_TIMEOUT_SEC: int = 300
_CLI_LOGIN_MAX_ATTEMPTS: int = 5
_LIVENESS_PROBE_FAILURE_THRESHOLD: int = 60
_LIVENESS_PROBE_TIMEOUT_SEC: int = 15
_SPANNER_CLI_BIN: str = '/google/spanner/bin/spanner'
_BOOTSTRAP_JOB_NAME: str = 'spanner-bootstrap-job'
_TLS_SECRET_NAME: str = 'tls-certs'
_ADMIN_PASSWORD_SECRET_NAME: str = 'spanner-admin-password'
_POD_CA_CERT_PATH: str = '/spanner/tls/ca-api.crt'
_POD_ADMIN_PASSWORD_FILE: str = '/tmp/admin_pw.txt'
_LOCAL_TLS_SECRETS_FILENAME: str = 'omni_tls_secrets.json'
_LOCAL_LICENSE_FILENAME: str = 'spanner_omni_license.bin'
_LOCAL_ADMIN_PASSWORD_FILENAME: str = 'spanner_omni_admin_pw.txt'
_LOCAL_ADMIN_SECRET_FILENAME: str = 'spanner_omni_admin_secret.json'
_OMNI_GCS_RELEASES_API_URL: str = (
    'https://storage.googleapis.com/storage/v1/b/spanner-omni/o?delimiter=/'
)
_OMNI_VERSION_REGEX: re.Pattern[str] = re.compile(
    r'^(\d{4})\.r(\d+)(?:\.(\d+))?(?:-([a-z]+)(?:\.(\d+))?)?$'
)
_CLI_TARBALL_URL_TEMPLATE: str = (
    'https://storage.googleapis.com/spanner-omni/{version}/'
    'spanner-omni-cli-{version}-linux-x86_64.tar.gz'
)
_CLIENT_CLI_DIR: str = '/tmp/spanner_omni_cli'
_CLIENT_TLS_DIR: str = '/tmp/omni_tls'
_CLIENT_TLS_SECRETS_FILE: str = f'{_CLIENT_TLS_DIR}/tls_secrets.json'


def ResolveLatestOmniReleaseVersion(version: str | None = None) -> str:
  """Returns `version` if set, else the latest public Spanner Omni release.

  `gs://spanner-omni` only publishes versioned prefixes (there is no `latest`
  tag/pointer), so we list prefixes and pick the highest release, preferring
  stable/LTS releases over `-beta`/`-alpha` tags.

  Args:
    version: Optional explicit Spanner Omni release version tag.

  Returns:
    The resolved Spanner Omni release version tag.
  """
  if version:
    return version
  try:
    with urllib.request.urlopen(_OMNI_GCS_RELEASES_API_URL, timeout=30) as resp:
      payload = json.load(resp)
  except Exception as exc:
    raise errors.Resource.CreationError(
        'Failed to query Spanner Omni releases from'
        f' {_OMNI_GCS_RELEASES_API_URL}: {exc}'
    ) from exc

  candidates: list[tuple[tuple[bool, int, int, int, bool, str, int], str]] = []
  for prefix in payload.get('prefixes', []):
    tag = prefix.rstrip('/')
    match = _OMNI_VERSION_REGEX.match(tag)
    if not match:
      continue
    prerelease_tag = match.group(4) or ''
    is_stable = not prerelease_tag or prerelease_tag == 'lts'
    candidates.append((
        (
            is_stable,
            int(match.group(1)),
            int(match.group(2)),
            int(match.group(3) or 0),
            prerelease_tag == 'lts',
            prerelease_tag,
            int(match.group(5) or 0),
        ),
        tag,
    ))

  if not candidates:
    raise errors.Resource.CreationError(
        'No valid Spanner Omni release versions found in'
        f' {_OMNI_GCS_RELEASES_API_URL}.'
    )
  _, latest_version = max(candidates, key=lambda item: item[0])
  logging.info(
      'Resolved latest Spanner Omni release version: %s', latest_version
  )
  return latest_version


# Generic kubectl transport retries are handled by
# `kubectl.RunRetryableKubectlCommand` (`RETRYABLE_KUBECTL_ERRORS`), and pod
# disconnection errors by `kubectl.IsPodDisconnectionError`. The helpers below
# classify Spanner Omni CLI gRPC status codes and auth failures.
def _StderrHasCode(stderr: str, code: str) -> bool:
  """Returns True if `stderr` contains the gRPC/Spanner status `code`."""
  normalized_code = re.sub(r'[\s_-]+', '', code.lower())
  return normalized_code in re.sub(r'[\s_-]+', '', stderr.lower())


def _IsAlreadyExistsError(stderr: str) -> bool:
  """Returns True if `stderr` explicitly indicates an ALREADY_EXISTS error."""
  return _StderrHasCode(stderr, 'ALREADY_EXISTS')


def _IsAuthError(stderr: str) -> bool:
  """Returns True if `stderr` indicates an authentication/login error."""
  lowered = stderr.lower()
  return 'not authenticated' in lowered or 'unauthenticated' in lowered


def _IsRetryableCliError(stderr: str) -> bool:
  """Returns True if a non-zero Spanner CLI exit is transient/retryable."""
  if _IsAuthError(stderr) or kubectl.IsPodDisconnectionError(stderr):
    return True
  return any(
      _StderrHasCode(stderr, code)
      for code in (
          'UNAVAILABLE',
          'DEADLINE_EXCEEDED',
          'ABORTED',
          'RESOURCE_EXHAUSTED',
      )
  )


def _StripCliSpinnerOutput(stdout: str) -> str:
  r"""Strips `cobrautil.ShowLoader` ANSI spinner frames (`\r\x1b[K...`)."""
  if '\x1b[K' in stdout:
    stdout = stdout.rsplit('\x1b[K', 1)[-1]
  return stdout.lstrip('\r')


def _ParseCliYaml(stdout: str) -> tuple[str, Any]:
  """Strips CLI spinner frames and parses the YAML output.

  Args:
    stdout: Raw Spanner Omni CLI stdout.

  Returns:
    `(cleaned_stdout, data)` where `data` is the parsed YAML (or None if the
    output is not valid YAML).
  """
  cleaned_stdout = _StripCliSpinnerOutput(stdout)
  try:
    return cleaned_stdout, yaml.safe_load(cleaned_stdout)
  except yaml.YAMLError:
    return cleaned_stdout, None


def _IsAnalyzeStatement(sql_query: str) -> bool:
  return sql_query.strip().rstrip(';').strip().upper() == 'ANALYZE'


@contextlib.contextmanager
def _PrivateTempFile(filename: str, content: str | bytes) -> Iterator[str]:
  """Writes `content` to a unique owner-only temp file and removes it."""
  data = content.encode('utf-8') if isinstance(content, str) else content
  fd, path = tempfile.mkstemp(prefix=f'{filename}.', dir=vm_util.GetTempDir())
  try:
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, 'wb') as f:
      f.write(data)
    yield path
  finally:
    if os.path.exists(path):
      os.remove(path)


class BaseSpannerOmniCluster(resource.BaseResource):
  """A multi-zone Spanner Omni cluster deployed on Kubernetes via Helm.

  Lifecycle:
    * `_CreateDependencies`: validates the topology, resolves the release,
      generates TLS certificates (with the Spanner CLI on the client VM) and
      creates the TLS and admin password Kubernetes Secrets.
    * `_Create`: fetches the license key and installs the Helm chart.
    * `_IsReady`: all Spanner pods are Ready and the bootstrap job completed.
    * `_PostCreate`: logs into the CLI and creates an empty database.
    * `_Delete`: uninstalls the release and deletes the PVCs so the
      underlying persistent disks do not leak.

  Provider subclasses set `CLOUD`, `HELM_PLATFORM` and `STORAGE_CLASS` and
  implement `_FetchLicenseKeyFile`, `_GetRegionFromZone`, and
  `_BuildZoneShortNames`; they may override `CheckSpecPrerequisites`.

  Attributes:
    spec: The `SpannerOmniClusterSpec`.
    container_cluster: The Kubernetes cluster hosting Spanner Omni.
    client_vm: The client VM running the Spanner CLI (for certificates);
      attached by `SetVms` from the `client_vm_group` VM group.
    database_name: The PostgreSQL-dialect database to create.
    omni_version: The resolved Spanner Omni release version tag.
    pod_endpoints: Sorted `(pod_name, pod_ip)` tuples of the Spanner pods; the
      first entry is a root server in the client VM's zone when possible.
  """

  RESOURCE_TYPE = 'BaseSpannerOmniCluster'
  REQUIRED_ATTRS = ['CLOUD']
  READY_TIMEOUT = _SPANNER_POD_WAIT_TIMEOUT_SEC
  POLL_INTERVAL = 10

  # Spanner Omni Helm chart `global.platform` enum value (e.g. 'gke', 'eks'),
  # distinct from PKB's `CLOUD` attribute.
  HELM_PLATFORM: str
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
    self.omni_version: str | None = omni_spec.version
    self.pod_endpoints: list[tuple[str, str]] = []
    self._admin_password: str | None = None
    self._bound_pv_names: list[str] = []

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

  def __getstate__(self) -> dict[str, Any]:
    # Never pickle the admin password; it is re-read from the Kubernetes
    # Secret when needed in a later stage.
    state = self.__dict__.copy()
    state['_admin_password'] = None
    return state

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

  @property
  def first_pod_name(self) -> str:
    """Returns the primary (client-zone root) Spanner pod name."""
    if not self.pod_endpoints:
      raise errors.Resource.CreationError(
          'Spanner Omni pod endpoints are not initialized; call'
          ' WaitForPodsReady() before running pod commands.'
      )
    return self.pod_endpoints[0][0]

  # ---------------------------------------------------------------------------
  # Provider hooks.
  # ---------------------------------------------------------------------------

  def _NewLicenseTempFile(self) -> str:
    """Creates an empty owner-only local temp file for the license key."""
    fd, path = tempfile.mkstemp(
        prefix=f'{_LOCAL_LICENSE_FILENAME}.', dir=vm_util.GetTempDir()
    )
    os.fchmod(fd, 0o600)
    os.close(fd)
    return path

  def _FetchLicenseKeyFile(self) -> str:
    """Copies the license key into an owner-only local temp file.

    The default supports a local `license_uri` file; provider subclasses
    override this to also read their secret store, calling `super()` for local
    files.

    Returns:
      Path of the temp file holding the license key; the caller deletes it.

    Raises:
      errors.Config.InvalidValue: If `license_uri` is not an existing file.
    """
    license_uri = self.spec.license_uri or ''
    if not os.path.isfile(license_uri):
      raise errors.Config.InvalidValue(
          f'spanner_omni_license_uri {license_uri!r} is not a supported license'
          f' location for {type(self).__name__}.'
      )
    path = self._NewLicenseTempFile()
    shutil.copyfile(license_uri, path)
    return path

  def _CreateDependencies(self) -> None:
    """Creates the namespace, TLS and admin password Secrets."""
    _ = self.client_vm  # Raises if `SetVms` has not attached one.
    self._ValidateTopology()
    self.omni_version = ResolveLatestOmniReleaseVersion(self.spec.version)
    self._EnsureNamespaceExists()
    self._ConfigureTlsCertificatesAndSecrets()
    self._CreateAdminPasswordSecret()

  def _Create(self) -> None:
    """Installs the Spanner Omni Helm chart."""
    license_path = self._FetchLicenseKeyFile()
    try:
      chart = self._BuildHelmChart(license_path)
      logging.info(
          'Deploying Spanner Omni Helm chart %s (image.tag=%s)',
          self.spec.chart_uri,
          self.omni_version,
      )
      try:
        helm.InstallCharts([chart])
      except Exception:
        # `created` is only set after `_Create` succeeds, so `Delete` would
        # skip a partially installed release; clean it up here instead.
        logging.exception('Helm install failed; removing partial release.')
        try:
          self._Delete()
        except Exception:  # pylint: disable=broad-except
          logging.exception(
              'Failed to clean up partial Spanner Omni Helm release.'
          )
        raise
    finally:
      if os.path.exists(license_path):
        os.remove(license_path)

  @abc.abstractmethod
  def _GetRegionFromZone(self, zone: str) -> str:
    """Returns the cloud provider region name for `zone`."""

  @abc.abstractmethod
  def _BuildZoneShortNames(self, zones: Sequence[str]) -> dict[str, str]:
    """Maps each zone to a unique 1-letter Helm `shortName`."""

  def _BuildLocationsJson(self, zones: Sequence[str]) -> str:
    """Builds the Helm `locations` JSON string from a list of zones.

    Args:
      zones: Sequence of zone names (e.g., `['us-central1-a', ...]`).

    Returns:
      Compact JSON string representing the `locations` Helm value.
    """
    zone_short_names = self._BuildZoneShortNames(zones)
    regions: dict[str, list[dict[str, str]]] = collections.defaultdict(list)
    for zone in zones:
      region_name = self._GetRegionFromZone(zone)
      regions[region_name].append(
          {'name': zone, 'shortName': zone_short_names[zone]}
      )
    locations = [
        {'name': region_name, 'zones': zone_entries}
        for region_name, zone_entries in regions.items()
    ]
    return json.dumps(locations, separators=(',', ':'))

  def _BuildHelmChart(self, license_path: str) -> helm.HelmChart:
    set_values = {
        'global.platform': self.HELM_PLATFORM,
        'global.insecureMode': 'false',
        'image.tag': str(self.omni_version),
        'deployment.replicasPerZone': str(self.replicas_per_zone),
        'deployment.rootServersPerZone': str(self.root_servers_per_zone),
        'deployment.enablePasswordAuthentication': 'true',
        'deployment.enableClientCertificateAuthentication': 'false',
        'deployment.adminPasswordSecret': _ADMIN_PASSWORD_SECRET_NAME,
        'resources.cpu': self.spec.cpu_request,
        'resources.memory': self.spec.memory_request,
        'createStorageClasses': 'true',
        'dataStorageClass': self.STORAGE_CLASS,
        'logsStorageClass': self.STORAGE_CLASS,
        'storage.data.size': self.spec.data_disk_size,
        'storage.logs.size': self.spec.logs_disk_size,
        'probes.liveness.failureThreshold': str(
            _LIVENESS_PROBE_FAILURE_THRESHOLD
        ),
        'probes.liveness.timeoutSeconds': str(_LIVENESS_PROBE_TIMEOUT_SEC),
    }
    return helm.HelmChart(
        release_name=RELEASE_NAME,
        chart_ref=self.spec.chart_uri,
        namespace=NAMESPACE,
        version=self.spec.chart_version,
        set_values=set_values,
        set_json_values={'locations': self._BuildLocationsJson(self.zones)},
        set_files={'licenseKey': license_path},
        wait=False,
        atomic=False,
    )

  def _IsReady(self) -> bool:
    """Returns True once all pods are Ready and the bootstrap job completed."""
    try:
      self.pod_endpoints = self._GetReadyPodEndpoints()
    except errors.Resource.RetryableCreationError as e:
      logging.info('Spanner Omni pods not ready yet: %s', e)
      return False
    return self._IsBootstrapJobComplete()

  def _PostCreate(self) -> None:
    """Logs in and creates the database."""
    self.LoginSpannerCli()
    self.CreateDatabase(self.database_name)

  def _DeleteResource(self) -> None:
    """Deletes Spanner Omni without failing the rest of the teardown.

    `BaseResource._DeleteResource` runs `_Delete` and then polls `_IsDeleting`
    until the bound persistent volumes are gone. If they outlive that wait (or a
    teardown command fails unexpectedly), log the possible disk leak instead of
    raising, so `BenchmarkSpec.Delete` still deletes the VMs and the container
    cluster.
    """
    try:
      super()._DeleteResource()
    except Exception:  # pylint: disable=broad-except
      logging.exception(
          'Failed to fully delete Spanner Omni; persistent volumes %s may'
          ' leak.',
          self._bound_pv_names,
      )

  def _Delete(self) -> None:
    """Uninstalls Spanner Omni and deletes its PVCs (best effort).

    Records the persistent volumes bound to the namespace's claims so
    `_IsDeleting` can wait for them to be deleted and the underlying disks do
    not leak (leaked disks exhaust quota).
    """
    helm.RunHelmCommand(
        ['uninstall', RELEASE_NAME, '--namespace', NAMESPACE],
        raise_on_failure=False,
        raise_on_timeout=False,
    )
    stdout, _, _ = kubectl.RunKubectlCommand(
        [
            'get',
            'pvc',
            '-n',
            NAMESPACE,
            '-o',
            'jsonpath={.items[*].spec.volumeName}',
        ],
        raise_on_failure=False,
        raise_on_timeout=False,
    )
    for pv_name in (stdout or '').split():
      if pv_name not in self._bound_pv_names:
        self._bound_pv_names.append(pv_name)
    for command in (
        ['delete', 'pods', '--all', '-n', NAMESPACE],
        ['delete', 'pvc', '--all', '-n', NAMESPACE],
    ):
      kubectl.RunKubectlCommand(
          command, raise_on_failure=False, raise_on_timeout=False
      )
    if self._bound_pv_names:
      kubernetes_commands.WaitForResource(
          f'pv/{self._bound_pv_names[0]}',
          condition_name='delete',
          condition_type='',
          extra_args=[f'pv/{name}' for name in self._bound_pv_names[1:]],
          timeout=300,
          raise_on_failure=False,
          raise_on_timeout=False,
      )
    kubectl.RunKubectlCommand(
        [
            'delete',
            'namespace',
            NAMESPACE,
            '--ignore-not-found=true',
            '--wait=false',
        ],
        raise_on_failure=False,
        raise_on_timeout=False,
    )

  def _IsDeleting(self) -> bool:
    """Returns True while any persistent volumes from the release still exist."""
    if not self._bound_pv_names:
      return False
    remaining_stdout, remaining_stderr, remaining_retcode = (
        kubectl.RunKubectlCommand(
            [
                'get',
                'pv',
                *self._bound_pv_names,
                '--ignore-not-found=true',
                '-o',
                'name',
            ],
            raise_on_failure=False,
            raise_on_timeout=False,
        )
    )
    if remaining_retcode != 0 or (remaining_stdout or '').strip():
      logging.info(
          'Spanner Omni PVs still deleting: stdout=%r, stderr=%r',
          remaining_stdout,
          remaining_stderr,
      )
      return True
    self._bound_pv_names = []
    return False

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
        'omni_version': self.omni_version,
        'omni_chart_version': self.spec.chart_version,
    }
    metadata.update(
        {k: v for k, v in optional_metadata.items() if v is not None}
    )
    return metadata

  # ---------------------------------------------------------------------------
  # Kubernetes helpers.
  # ---------------------------------------------------------------------------

  def _EnsureNamespaceExists(self) -> None:
    """Creates the Spanner Omni Kubernetes namespace if it does not exist."""
    _, stderr, retcode = kubectl.RunKubectlCommand(
        ['create', 'namespace', NAMESPACE],
        timeout=_KUBECTL_DEFAULT_TIMEOUT_SEC,
        raise_on_failure=False,
    )
    if retcode != 0 and not _IsAlreadyExistsError(stderr):
      raise errors.Resource.CreationError(
          f'Failed to create namespace {NAMESPACE!r}: {stderr}'
      )

  def _ConfigureTlsCertificatesAndSecrets(self) -> None:
    """Generates TLS certs via the Spanner CLI on the client VM.

    The certificates are applied as the `tls-certs` Kubernetes Secret used by
    the chart and stay on the client VM for the PGAdapter truststore.
    """
    client_vm = self.client_vm
    cli_url = _CLI_TARBALL_URL_TEMPLATE.format(version=self.omni_version)
    cli_bin = f'{_CLIENT_CLI_DIR}/google/spanner/bin/spanner'
    pod_hosts = (
        f'localhost,127.0.0.1,*.pod.{NAMESPACE}.svc.cluster.local,'
        f'*.pod.{NAMESPACE}.svc.cluster,*.pod.{NAMESPACE}.svc,'
        f'*.pod.{NAMESPACE},*.pod'
    )
    svc_hosts = (
        f'spanner,spanner.{NAMESPACE},spanner.{NAMESPACE}.svc,'
        f'spanner.{NAMESPACE}.svc.cluster,'
        f'spanner.{NAMESPACE}.svc.cluster.local'
    )
    cert_script = vm_util.ReadAndRenderJinja2Template(
        'spanner_omni/create_certs.sh.j2',
        cli_dir=_CLIENT_CLI_DIR,
        tls_dir=_CLIENT_TLS_DIR,
        cli_url=cli_url,
        cli_bin=cli_bin,
        pod_hosts=pod_hosts,
        svc_hosts=svc_hosts,
        tls_secret_name=_TLS_SECRET_NAME,
        namespace=NAMESPACE,
        tls_secrets_file=_CLIENT_TLS_SECRETS_FILE,
    )
    try:
      client_vm.RemoteCommand(cert_script)
      with _PrivateTempFile(
          _LOCAL_TLS_SECRETS_FILENAME, b''
      ) as local_secrets_path:
        client_vm.PullFile(local_secrets_path, _CLIENT_TLS_SECRETS_FILE)
        os.chmod(local_secrets_path, 0o600)
        kubectl.RunRetryableKubectlCommand(
            [
                'apply',
                '--server-side',
                '--force-conflicts',
                '-n',
                NAMESPACE,
                '-f',
                local_secrets_path,
            ],
            timeout=_KUBECTL_DEFAULT_TIMEOUT_SEC,
        )
    finally:
      client_vm.RemoteCommand(
          f'rm -f {_CLIENT_TLS_SECRETS_FILE} {_CLIENT_TLS_DIR}/*.key',
          ignore_failure=True,
      )

  def _CreateAdminPasswordSecret(self) -> None:
    """Generates a random admin password and stores it in a K8s Secret."""
    self._admin_password = db_util.GenerateRandomDbPassword()
    secret = {
        'apiVersion': 'v1',
        'kind': 'Secret',
        'metadata': {
            'name': _ADMIN_PASSWORD_SECRET_NAME,
            'namespace': NAMESPACE,
        },
        'type': 'Opaque',
        'data': {
            'password': (
                base64.b64encode(self._admin_password.encode('utf-8')).decode(
                    'ascii'
                )
            )
        },
    }
    with _PrivateTempFile(
        _LOCAL_ADMIN_SECRET_FILENAME, json.dumps(secret)
    ) as path:
      kubectl.RunRetryableKubectlCommand(
          [
              'apply',
              '--server-side',
              '--force-conflicts',
              '-n',
              NAMESPACE,
              '-f',
              path,
          ],
          timeout=_KUBECTL_DEFAULT_TIMEOUT_SEC,
      )

  def _GetAdminPassword(self) -> str:
    """Returns the admin password, reading the K8s Secret if not cached."""
    if self._admin_password is None:
      stdout, _, _ = kubectl.RunRetryableKubectlCommand(
          [
              'get',
              'secret',
              _ADMIN_PASSWORD_SECRET_NAME,
              '-n',
              NAMESPACE,
              '-o',
              'jsonpath={.data.password}',
          ],
          timeout=_KUBECTL_DEFAULT_TIMEOUT_SEC,
          suppress_logging=True,
      )
      self._admin_password = base64.b64decode(stdout.strip()).decode('utf-8')
    return self._admin_password

  def _GetReadyPodEndpoints(self) -> list[tuple[str, str]]:
    """Returns sorted `(name, ip)` of the Spanner pods if all are Ready.

    Prioritizes root server pods (`-rt-` / `server-type=root`) in the client
    VM's zone so `pod_endpoints[0]` is a root server co-located with the client.

    Raises:
      errors.Resource.RetryableCreationError: If pods are missing or not Ready.
    """
    try:
      stdout, stderr, retcode = kubectl.RunRetryableKubectlCommand(
          [
              'get',
              'pods',
              '-n',
              NAMESPACE,
              '-l',
              'app.kubernetes.io/component=spanner',
              '-o',
              'json',
          ],
          timeout=_KUBECTL_DEFAULT_TIMEOUT_SEC,
          raise_on_failure=False,
      )
    except (errors.VmUtil.IssueCommandTimeoutError, vm_util.RetryError) as e:
      raise errors.Resource.RetryableCreationError(
          f'Timed out listing Spanner Omni pods: {e}'
      ) from e
    if retcode != 0:
      raise errors.Resource.RetryableCreationError(
          f'Failed to list Spanner Omni pods: {stderr}'
      )
    payload = json.loads(stdout or '{}')
    items = [
        item
        for item in payload.get('items', [])
        if not item.get('metadata', {}).get('deletionTimestamp')
    ]
    expected_pods = len(self.zones) * self.replicas_per_zone
    if len(items) < expected_pods:
      raise errors.Resource.RetryableCreationError(
          f'Expected {expected_pods} active Spanner pods, found {len(items)}.'
      )

    preferred_zone = self.client_zone
    preferred_short = ''
    if preferred_zone:
      short_names = self._BuildZoneShortNames(self.zones)
      preferred_short = short_names.get(
          preferred_zone,
          self._BuildZoneShortNames([preferred_zone])[preferred_zone],
      )

    sortable_endpoints: list[tuple[tuple[bool, bool, str], str, str]] = []
    for item in items:
      metadata = item.get('metadata', {})
      pod_name = metadata.get('name', '')
      labels = metadata.get('labels', {}) or {}
      status = item.get('status') or {}
      pod_ip = status.get('podIP', '')
      conditions = status.get('conditions') or []
      is_ready = any(
          cond.get('type') == 'Ready' and cond.get('status') == 'True'
          for cond in conditions
      )
      if not pod_name or not pod_ip or not is_ready:
        raise errors.Resource.RetryableCreationError(
            f'Spanner pod {pod_name!r} not ready yet (ip={pod_ip!r},'
            f' ready={is_ready}).'
        )
      in_preferred_zone = not preferred_zone or (
          labels.get('zone') == preferred_zone
          or pod_name.startswith(f'spanner-{preferred_short}-')
      )
      is_root = labels.get('server-type') == 'root' or '-rt-' in pod_name
      sort_key = (not in_preferred_zone, not is_root, pod_name)
      sortable_endpoints.append((sort_key, pod_name, pod_ip))

    sortable_endpoints.sort(key=lambda entry: entry[0])
    return [(pod_name, pod_ip) for _, pod_name, pod_ip in sortable_endpoints]

  def _IsBootstrapJobComplete(self) -> bool:
    """Returns True if the chart's bootstrap job has completed."""
    try:
      stdout, stderr, retcode = kubectl.RunRetryableKubectlCommand(
          ['get', 'job', _BOOTSTRAP_JOB_NAME, '-n', NAMESPACE, '-o', 'json'],
          timeout=_KUBECTL_DEFAULT_TIMEOUT_SEC,
          raise_on_failure=False,
      )
    except (errors.VmUtil.IssueCommandTimeoutError, vm_util.RetryError) as e:
      logging.info('Spanner Omni bootstrap job check timed out: %s', e)
      return False
    if retcode != 0:
      logging.info('Spanner Omni bootstrap job not found yet: %s', stderr)
      return False
    conditions = (
        json.loads(stdout or '{}').get('status', {}).get('conditions', [])
    )
    for cond in conditions:
      if cond.get('status') != 'True':
        continue
      if cond.get('type') == 'Complete':
        return True
      if cond.get('type') == 'Failed':
        raise errors.Resource.CreationError(
            f'Spanner Omni bootstrap job failed: {cond}'
        )
    return False

  @vm_util.Retry(
      poll_interval=10,
      timeout=_SPANNER_POD_WAIT_TIMEOUT_SEC,
      retryable_exceptions=(errors.Resource.RetryableCreationError,),
  )
  def WaitForPodsReady(self) -> list[tuple[str, str]]:
    """Waits for all Spanner pods to be Ready and refreshes `pod_endpoints`.

    Pods may be rescheduled during long restores, so callers refresh the
    endpoints before (re)configuring client routing.

    Returns:
      Sorted list of `(pod_name, pod_ip)` tuples.
    """
    self.pod_endpoints = self._GetReadyPodEndpoints()
    return self.pod_endpoints

  # ---------------------------------------------------------------------------
  # Spanner Omni CLI helpers (run inside a Spanner pod via `kubectl exec`).
  # ---------------------------------------------------------------------------

  def _BuildPodCliCommand(self, cli_args: Sequence[str]) -> list[str]:
    """Builds the `kubectl exec` argument list for running the Spanner CLI."""
    return [
        'exec',
        '-n',
        NAMESPACE,
        self.first_pod_name,
        '--',
        'env',
        'HOME=/tmp',
        _SPANNER_CLI_BIN,
        *cli_args,
        f'--ca-certificate-file={_POD_CA_CERT_PATH}',
        f'--deployment-endpoint=dns:///127.0.0.1:{_SPANNER_GRPC_PORT}',
    ]

  def _RunCli(
      self, cli_args: Sequence[str], **kwargs: Any
  ) -> tuple[str, str, int]:
    return kubectl.RunRetryableKubectlCommand(
        self._BuildPodCliCommand(cli_args), **kwargs
    )

  def _RunAuthenticatedCli(
      self,
      cli_args: Sequence[str],
      timeout: int = _SPANNER_CLI_DEFAULT_TIMEOUT_SEC,
  ) -> tuple[str, str, int]:
    """Runs a CLI command, re-logging in on pod restarts and auth errors.

    Args:
      cli_args: Spanner Omni CLI arguments.
      timeout: Per-command timeout in seconds.

    Returns:
      `(stdout, stderr, retcode)` of the command (retried once after an
      authentication error or pod disconnection).

    Raises:
      errors.Resource.RetryableCreationError: If the command timed out.
    """

    def _Run() -> tuple[str, str, int]:
      try:
        return self._RunCli(cli_args, timeout=timeout, raise_on_failure=False)
      except (errors.VmUtil.IssueCommandTimeoutError, vm_util.RetryError) as e:
        raise errors.Resource.RetryableCreationError(
            f'Spanner Omni CLI command {list(cli_args[:2])} timed out: {e}'
        ) from e

    stdout, stderr, retcode = _Run()
    if retcode != 0 and kubectl.IsPodDisconnectionError(stderr):
      try:
        self.WaitForPodsReady()
      except vm_util.RetryError as e:
        raise errors.Resource.RetryableCreationError(
            f'Timed out waiting for Spanner Omni pods to recover: {e}'
        ) from e
      self.LoginSpannerCli(raise_on_failure=False)
      stdout, stderr, retcode = _Run()
    elif retcode != 0 and _IsAuthError(stderr):
      self.LoginSpannerCli(raise_on_failure=False)
      stdout, stderr, retcode = _Run()
    return stdout, stderr, retcode

  @vm_util.Retry(
      poll_interval=5,
      fuzz=0,
      max_retries=_CLI_LOGIN_MAX_ATTEMPTS - 1,
      timeout=-1,
      retryable_exceptions=(
          errors.VmUtil.IssueCommandError,
          errors.VmUtil.IssueCommandTimeoutError,
      ),
  )
  def _RunCliLogin(self) -> None:
    """Pipes the password into the pod and runs `spanner auth login`.

    The password is streamed over stdin from an owner-only local temp file, so
    it never appears on a command line, and the pod-side copy is removed right
    after logging in.
    """
    login_script = (
        f"trap 'rm -f {_POD_ADMIN_PASSWORD_FILE}' EXIT && "
        f'cat > {_POD_ADMIN_PASSWORD_FILE} && '
        f'chmod 600 {_POD_ADMIN_PASSWORD_FILE} && '
        f'HOME=/tmp {_SPANNER_CLI_BIN} auth login {_ADMIN_USERNAME} '
        f'--password-file={_POD_ADMIN_PASSWORD_FILE} '
        f'--ca-certificate-file={_POD_CA_CERT_PATH} '
        f'--deployment-endpoint=dns:///127.0.0.1:{_SPANNER_GRPC_PORT}'
    )
    with _PrivateTempFile(
        _LOCAL_ADMIN_PASSWORD_FILENAME, self._GetAdminPassword()
    ) as password_path:
      kubectl.RunKubectlCommand(
          [
              'exec',
              '-i',
              '-n',
              NAMESPACE,
              self.first_pod_name,
              '--',
              'sh',
              '-c',
              login_script,
          ],
          input_file=password_path,
          timeout=_SPANNER_CLI_DEFAULT_TIMEOUT_SEC,
      )

  def LoginSpannerCli(self, raise_on_failure: bool = True) -> None:
    """Logs into the Spanner Omni CLI inside the first Spanner pod."""
    try:
      self._RunCliLogin()
    except (
        errors.VmUtil.IssueCommandError,
        errors.VmUtil.IssueCommandTimeoutError,
        vm_util.RetryError,
    ):
      if raise_on_failure:
        raise
      pod_name = (
          self.pod_endpoints[0][0] if self.pod_endpoints else '<uninitialized>'
      )
      logging.warning(
          'Failed refreshing Spanner Omni CLI login on %s; continuing.',
          pod_name,
      )

  def CreateDatabase(self, database_name: str) -> None:
    """Creates a PostgreSQL-dialect database; an existing one is a no-op.

    Args:
      database_name: Name of the database to create.

    Raises:
      errors.Resource.CreationError: If creation fails for any reason other
        than the database already existing.
    """

    @vm_util.Retry(
        poll_interval=10,
        fuzz=0,
        timeout=600,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def _Create() -> None:
      _, stderr, retcode = self._RunAuthenticatedCli([
          'databases',
          'create',
          database_name,
          f'--database-dialect={_DATABASE_DIALECT}',
      ])
      if retcode == 0 or _IsAlreadyExistsError(stderr):
        return
      if _IsRetryableCliError(stderr):
        raise errors.Resource.RetryableCreationError(
            'Transient error creating Spanner Omni database'
            f' {database_name!r}: {stderr}'
        )
      raise errors.Resource.CreationError(
          f'Failed to create Spanner Omni database {database_name!r}: {stderr}'
      )

    try:
      _Create()
    except vm_util.RetryError as e:
      raise errors.Resource.CreationError(
          f'Timed out creating Spanner Omni database {database_name!r}: {e}'
      ) from e

  def RunDDLQuery(
      self,
      sql_query: str,
      *,
      timeout: int = _SPANNER_ANALYZE_TIMEOUT_SEC,
      async_proc: bool = False,
      database_name: str | None = None,
  ) -> tuple[str, str]:
    """Runs an idempotent DDL statement via the CLI and waits for the operation.

    `ANALYZE` is skipped when the spec's `run_analyze` is false. Matches the
    `GcpSpanner.RunDDLQuery` hook used by BenchBase; transient CLI/transport
    errors during `--async` submission are retried before polling.

    Args:
      sql_query: The idempotent DDL statement (e.g. `ANALYZE;`).
      timeout: Maximum time in seconds to poll the DDL operation.
      async_proc: Unused; the statement is always submitted with `--async` and
        polled until done.
      database_name: Target database; defaults to `database_name`.

    Returns:
      `(stdout, stderr)` of the DDL submit command.

    Raises:
      errors.Benchmarks.RunError: If the DDL cannot be submitted, the operation
        ID cannot be parsed, or the operation fails/times out.
    """
    del async_proc
    database_name = database_name or self.database_name
    if _IsAnalyzeStatement(sql_query) and not self.spec.run_analyze:
      logging.info('Skipping ANALYZE (--spanner_omni_run_analyze=false).')
      return '', ''
    logging.info(
        'Running DDL %r on Spanner Omni database %s', sql_query, database_name
    )

    @vm_util.Retry(
        poll_interval=10,
        fuzz=0,
        max_retries=2,
        timeout=_SPANNER_ASYNC_SUBMIT_TIMEOUT_SEC,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def _SubmitDdl() -> tuple[str, str]:
      stdout, stderr, retcode = self._RunAuthenticatedCli(
          [
              'databases',
              'ddl',
              'update',
              database_name,
              f'--ddl={sql_query}',
              '--async',
          ],
          timeout=_SPANNER_ASYNC_SUBMIT_TIMEOUT_SEC,
      )
      if retcode != 0:
        if _IsRetryableCliError(stderr):
          raise errors.Resource.RetryableCreationError(
              f'Transient error submitting DDL {sql_query!r}: {stderr}'
          )
        raise errors.Benchmarks.RunError(
            f'Failed to submit DDL {sql_query!r}: {stderr}'
        )
      return stdout, stderr

    try:
      stdout, stderr = _SubmitDdl()
    except vm_util.RetryError as e:
      raise errors.Benchmarks.RunError(
          f'Failed to submit DDL {sql_query!r}: {e}'
      ) from e
    operation_id = ''
    output = f'{_StripCliSpinnerOutput(stdout)}\n{stderr}'
    for line in output.splitlines():
      if 'Operation name:' in line:
        operation_name = line.split('Operation name:', 1)[1].strip()
        operation_id = operation_name.rsplit('/', 1)[-1]
        break
    if not operation_id:
      raise errors.Benchmarks.RunError(
          f'Failed to parse DDL operation ID from output: {stdout}\n{stderr}'
      )
    self.WaitForDatabaseOperation(operation_id, database_name, timeout=timeout)
    return stdout, stderr

  def WaitForDatabaseOperation(
      self,
      operation_id: str,
      database_name: str,
      timeout: int = _SPANNER_ANALYZE_TIMEOUT_SEC,
  ) -> None:
    """Polls a Spanner Omni database operation until `done: true`."""

    @vm_util.Retry(
        poll_interval=30,
        fuzz=0,
        timeout=timeout,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def _PollOperation() -> None:
      stdout, stderr, retcode = self._RunAuthenticatedCli([
          'operations',
          'describe',
          operation_id,
          f'--database={database_name}',
      ])
      if retcode != 0:
        if _IsRetryableCliError(stderr):
          raise errors.Resource.RetryableCreationError(
              'Transient error describing Spanner Omni operation'
              f' {operation_id}: {stderr}'
          )
        raise errors.Benchmarks.RunError(
            f'Failed to describe Spanner Omni operation {operation_id}:'
            f' {stderr}'
        )
      cleaned_stdout, op_data = _ParseCliYaml(stdout)
      if not isinstance(op_data, dict):
        raise errors.Benchmarks.RunError(
            'Unexpected non-YAML output describing Spanner Omni operation'
            f' {operation_id}:\n{cleaned_stdout}'
        )
      if op_data.get('error'):
        raise errors.Benchmarks.RunError(
            f'Spanner Omni operation {operation_id} failed:\n{cleaned_stdout}'
        )
      if op_data.get('done'):
        logging.info('Spanner Omni operation %s completed.', operation_id)
        return
      raise errors.Resource.RetryableCreationError(
          f'Spanner Omni operation {operation_id} still in'
          f' progress:\n{cleaned_stdout}'
      )

    try:
      _PollOperation()
    except vm_util.RetryError as e:
      raise errors.Benchmarks.RunError(
          f'Timed out waiting for Spanner Omni operation {operation_id}.'
      ) from e


def GetSpannerOmniClusterClass(
    cloud: str,
) -> type[BaseSpannerOmniCluster]:
  """Returns the `BaseSpannerOmniCluster` subclass registered for `cloud`."""
  return resource.GetResourceClass(  # pyrefly: ignore[bad-return]
      BaseSpannerOmniCluster, CLOUD=cloud
  )
