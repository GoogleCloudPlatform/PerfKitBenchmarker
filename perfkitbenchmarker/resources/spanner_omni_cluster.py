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
`providers/gcp/spanner_omni_cluster.py`) supply the license key and storage
class.

The admin password is generated randomly per run and only stored in the
Kubernetes Secret referenced by the chart (`deployment.adminPasswordSecret`).
It is never placed on a command line or logged.
"""

import base64
import collections
from collections.abc import Iterator, Sequence
import contextlib
import json
import logging
import os
import re
import secrets
import shlex
import string
from typing import Any
import urllib.request

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources import spanner_omni_cluster_spec
from perfkitbenchmarker.resources.container_service import container_cluster
from perfkitbenchmarker.resources.container_service import helm
from perfkitbenchmarker.resources.container_service import kubectl
import yaml

FLAGS = flags.FLAGS

RELEASE_NAME: str = 'spanner-omni'
NAMESPACE: str = 'spanner-ns'
_DATABASE_DIALECT: str = 'POSTGRESQL'
_ADMIN_USERNAME: str = 'admin'
_ADMIN_PASSWORD_LENGTH: int = 24
# Shell- and JDBC-property-safe special characters for generated passwords.
_ADMIN_PASSWORD_SPECIAL_CHARS: str = '-_'
_SPANNER_GRPC_PORT: int = 15000
_SPANNER_POD_WAIT_TIMEOUT_SEC: int = 1200
_SPANNER_ANALYZE_TIMEOUT_SEC: int = 60 * 60 * 6
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


def _ClientVmGroupName(config: dict[str, Any]) -> str:
  omni_config = config.get('spanner_omni_cluster') or {}
  return omni_config.get('client_vm_group') or 'clients'


def _SplitZones(zones: str | None) -> list[str]:
  return [z.strip() for z in (zones or '').split(',') if z.strip()]


def ApplyConfigOverrides(config: dict[str, Any]) -> dict[str, Any]:
  """Reconciles the Spanner Omni and container_cluster settings in a config.

  Zones and servers per zone have a single source of truth:

  * Zones: `--spanner_omni_zones`, else `spanner_omni_cluster.zones` from the
    config, is written to the container_cluster `vm_spec.<cloud>.zone` and the
    client VM group zone (first zone). Otherwise the zones are taken from the
    container_cluster's configured zone list.
  * Servers per zone: `--spanner_omni_replicas_per_zone`, else
    `spanner_omni_cluster.replicas_per_zone`, sets the container_cluster
    `vm_count`; otherwise `vm_count` sets `replicas_per_zone`.

  The resulting zones, cluster zone and cluster machine type are recorded in the
  `spanner_omni_cluster` block so `ApplyPrerequisiteOverrides` can undo generic
  `--zone`/`--machine_type` flags meant for the client VM.

  Args:
    config: Loaded benchmark configuration dictionary with a `container_cluster`
      and the client VM group (`client_vm_group`, default `clients`).

  Returns:
    The updated benchmark configuration dictionary.

  Raises:
    errors.Config.InvalidValue: If the zones flag is empty.
  """
  omni_config = config.setdefault('spanner_omni_cluster', {})
  cluster_config = config['container_cluster']
  cloud = cluster_config.get('cloud') or FLAGS.cloud

  if FLAGS['spanner_omni_replicas_per_zone'].present:
    omni_config['replicas_per_zone'] = (
        spanner_omni_cluster_spec.REPLICAS_PER_ZONE.value
    )
  if omni_config.get('replicas_per_zone') is not None:
    cluster_config['vm_count'] = omni_config['replicas_per_zone']
  elif cluster_config.get('vm_count') is not None:
    omni_config['replicas_per_zone'] = cluster_config['vm_count']

  zones: list[str] = []
  if FLAGS['spanner_omni_zones'].present:
    zones = list(spanner_omni_cluster_spec.ZONES.value or [])
    if not zones:
      raise errors.Config.InvalidValue(
          'spanner_omni_zones must contain at least one zone.'
      )
  elif omni_config.get('zones'):
    zones = list(omni_config['zones'])
  cluster_vm_spec = (cluster_config.get('vm_spec') or {}).get(cloud)
  client_group = (config.get('vm_groups') or {}).get(
      _ClientVmGroupName(config)
  ) or {}
  client_vm_spec = (client_group.get('vm_spec') or {}).get(cloud)
  if zones:
    if cluster_vm_spec is not None:
      cluster_vm_spec['zone'] = ','.join(zones)
    if client_vm_spec is not None:
      client_vm_spec['zone'] = zones[0]
  elif cluster_vm_spec is not None:
    zones = _SplitZones(cluster_vm_spec.get('zone'))
  if zones:
    omni_config['zones'] = zones
  if cluster_vm_spec is not None:
    omni_config['cluster_zone'] = cluster_vm_spec.get('zone')
    omni_config['cluster_machine_type'] = cluster_vm_spec.get('machine_type')
  return config


def ApplyPrerequisiteOverrides(benchmark_config: Any) -> None:
  """Keeps the cluster spec intact despite generic `--zone`/`--machine_type`.

  Generic `--zone`/`--machine_type` flags (as passed by suites to size the
  client VM) would otherwise overwrite the multi-zone cluster spec; this
  restores the zones, servers per zone and machine type resolved by
  `ApplyConfigOverrides` (recorded in the `spanner_omni_cluster` spec) and keeps
  the client VM co-located with one of those zones. Provider prerequisites
  (e.g. backup bucket access) are checked by `BenchmarkSpec.CheckPrerequisites`.

  Args:
    benchmark_config: The decoded benchmark config spec.
  """
  omni_spec = getattr(benchmark_config, 'spanner_omni_cluster', None)
  if not isinstance(
      omni_spec, spanner_omni_cluster_spec.SpannerOmniClusterSpec
  ):
    omni_spec = None
  zones = list(omni_spec.zones or []) if omni_spec else []
  cluster_spec = getattr(benchmark_config, 'container_cluster', None)
  if cluster_spec:
    if omni_spec:
      cluster_spec.vm_count = omni_spec.replicas_per_zone
    vm_spec = getattr(cluster_spec, 'vm_spec', None)
    if vm_spec:
      if zones:
        vm_spec.zone = ','.join(zones)
      config_machine_type = (
          omni_spec.cluster_machine_type if omni_spec else None
      )
      if FLAGS['machine_type'].present and config_machine_type:
        vm_spec.machine_type = config_machine_type
  vm_groups = getattr(benchmark_config, 'vm_groups', None) or {}
  client_group = vm_groups.get(
      omni_spec.client_vm_group if omni_spec else 'clients'
  )
  if zones and client_group and getattr(client_group, 'vm_spec', None):
    if FLAGS['spanner_omni_zones'].present or (
        FLAGS['zone'].present and (not FLAGS.zone or FLAGS.zone[0] not in zones)
    ):
      client_group.vm_spec.zone = zones[0]
      if FLAGS.zone:
        FLAGS.zone = [zones[0]]


def BuildZoneShortNames(zones: Sequence[str]) -> dict[str, str]:
  """Maps each zone to a unique 1-letter Helm `shortName`.

  Uses the zone's suffix (e.g. `a` for `us-central1-a`) when it is a single
  unused lowercase letter; otherwise picks the next free letter.

  Args:
    zones: Sequence of zone names.

  Returns:
    Mapping from zone name to its unique 1-letter short name.
  """
  zone_short_names: dict[str, str] = {}
  used_short_names: set[str] = set()
  for zone in zones:
    suffix = zone.rsplit('-', 1)[1] if '-' in zone else ''
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


def BuildLocationsJson(zones: Sequence[str]) -> str:
  """Builds the Helm `locations` JSON string from a list of zones.

  Args:
    zones: Sequence of zone names (e.g., `['us-central1-a', ...]`).

  Returns:
    Compact JSON string representing the `locations` Helm value.
  """
  zone_short_names = BuildZoneShortNames(zones)
  regions: dict[str, list[dict[str, str]]] = collections.defaultdict(list)
  for zone in zones:
    region_name = zone.rsplit('-', 1)[0] if '-' in zone else zone
    regions[region_name].append(
        {'name': zone, 'shortName': zone_short_names[zone]}
    )
  locations = [
      {'name': region_name, 'zones': zone_entries}
      for region_name, zone_entries in regions.items()
  ]
  return json.dumps(locations, separators=(',', ':'))


def ResolveLatestOmniReleaseVersion(version: str | None = None) -> str:
  """Returns `version` if set, else the latest public Spanner Omni release."""
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


def _GenerateAdminPassword() -> str:
  """Returns a random password meeting the Spanner Omni password policy.

  The policy requires 8-32 characters with at least one uppercase letter,
  lowercase letter, digit and special character. Special characters are
  limited to ones that need no quoting in shells or PGAdapter properties.
  """
  alphabet = (
      string.ascii_letters + string.digits + _ADMIN_PASSWORD_SPECIAL_CHARS
  )
  while True:
    password = ''.join(
        secrets.choice(alphabet) for _ in range(_ADMIN_PASSWORD_LENGTH)
    )
    if (
        any(c.isupper() for c in password)
        and any(c.islower() for c in password)
        and any(c.isdigit() for c in password)
        and any(c in _ADMIN_PASSWORD_SPECIAL_CHARS for c in password)
    ):
      return password


def IsAlreadyExistsError(stderr: str) -> bool:
  """Returns True if `stderr` explicitly indicates an ALREADY_EXISTS error."""
  lowered = stderr.lower()
  normalized = lowered.replace(' ', '').replace('_', '')
  return (
      'alreadyexists' in normalized
      or 'already being used by an active backup' in lowered
  )


def IsNotFoundError(stderr: str) -> bool:
  """Returns True if `stderr` indicates a NOT_FOUND error."""
  normalized = stderr.lower().replace(' ', '').replace('_', '')
  return 'notfound' in normalized


def _IsAuthError(stderr: str) -> bool:
  """Returns True if `stderr` indicates an authentication/login error."""
  lowered = stderr.lower()
  return 'not authenticated' in lowered or 'unauthenticated' in lowered


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
def PrivateTempFile(filename: str, content: str | bytes) -> Iterator[str]:
  """Writes `content` to an owner-only temp file and removes it afterwards."""
  path = os.path.join(vm_util.GetTempDir(), filename)
  data = content.encode('utf-8') if isinstance(content, str) else content
  fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
  try:
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

  Provider subclasses set `CLOUD`, `PLATFORM` and `STORAGE_CLASS` and
  implement `_FetchLicenseKeyFile`; they may override
  `CheckSpecPrerequisites`.

  Attributes:
    spec: The `SpannerOmniClusterSpec`.
    container_cluster: The Kubernetes cluster hosting Spanner Omni.
    client_vm: The client VM running the Spanner CLI (for certificates);
      attached by `SetVms` from the `client_vm_group` VM group.
    database_name: The PostgreSQL-dialect database to create.
    create_database: Whether `_PostCreate` creates an empty database.
    omni_version: The resolved Spanner Omni release version tag.
    pod_endpoints: Sorted `(pod_name, pod_ip)` tuples of the Spanner pods; the
      first entry is a root server in the client VM's zone when possible.
  """

  RESOURCE_TYPE = 'BaseSpannerOmniCluster'
  REQUIRED_ATTRS = ['CLOUD']
  READY_TIMEOUT = _SPANNER_POD_WAIT_TIMEOUT_SEC
  POLL_INTERVAL = 10

  # Helm `global.platform` value (e.g. 'gke').
  PLATFORM: str
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
    self.client_vm: virtual_machine.BaseVirtualMachine | None = None
    self.database_name = omni_spec.database_name
    self.create_database = omni_spec.create_database
    self.omni_version: str | None = omni_spec.version
    self.pod_endpoints: list[tuple[str, str]] = []
    self._admin_password: str | None = None

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
    self.client_vm = client_vms[0]

  def GetClientVm(self) -> virtual_machine.BaseVirtualMachine:
    """Returns the client VM attached by `SetVms`.

    Raises:
      errors.Resource.CreationError: If `SetVms` has not attached a client VM.
    """
    if self.client_vm is None:
      raise errors.Resource.CreationError(
          'Spanner Omni cluster has no client VM; call SetVms(vm_groups)'
          ' before using it.'
      )
    return self.client_vm

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
    """Verifies provider prerequisites before provisioning (default: none)."""
    del omni_spec

  def CheckPrerequisites(self) -> None:
    """Called by `BenchmarkSpec.CheckPrerequisites` before provisioning."""
    super().CheckPrerequisites()
    type(self).CheckSpecPrerequisites(self.spec)

  @property
  def zones(self) -> list[str]:
    """Zones of the Kubernetes cluster, falling back to the spec zones."""
    # GKE multi-zone clusters expose `zones` and leave `zone` unset.
    cluster_zones = getattr(self.container_cluster, 'zones', None)
    if (
        isinstance(cluster_zones, list)
        and cluster_zones
        and all(isinstance(z, str) for z in cluster_zones)
    ):
      return list(cluster_zones)
    for owner in (
        getattr(self.container_cluster, 'default_nodepool', None),
        self.container_cluster,
    ):
      zone = getattr(owner, 'zone', None)
      if isinstance(zone, str) and zone:
        return _SplitZones(zone)
    return list(self.spec.zones or [])

  @property
  def replicas_per_zone(self) -> int:
    """Spanner Omni servers per zone (one per Kubernetes node)."""
    num_nodes = getattr(self.container_cluster, 'num_nodes', None)
    if isinstance(num_nodes, int) and num_nodes > 0:
      return num_nodes
    return self.spec.replicas_per_zone

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
    nodepool = getattr(self.container_cluster, 'default_nodepool', None)
    machine_type = getattr(nodepool, 'machine_type', None)
    return machine_type if isinstance(machine_type, str) else None

  @property
  def client_zone(self) -> str | None:
    """The client VM's zone, falling back to the first cluster zone."""
    client_zone = getattr(self.client_vm, 'zone', None)
    if isinstance(client_zone, str) and client_zone:
      return client_zone
    zones = self.zones
    return zones[0] if zones else None

  @property
  def first_pod_name(self) -> str:
    if not self.pod_endpoints:
      self.WaitForPodsReady()
    return self.pod_endpoints[0][0]

  # ---------------------------------------------------------------------------
  # Provider hooks.
  # ---------------------------------------------------------------------------

  def _FetchLicenseKeyFile(self) -> str:
    """Writes the license key to a local temp file and returns its path."""
    raise NotImplementedError()

  def _ValidateTopology(self) -> None:
    if not self.zones:
      raise errors.Config.InvalidValue(
          'Could not determine the Spanner Omni zones; set the'
          ' container_cluster zone or --spanner_omni_zones.'
      )
    if self.root_servers_per_zone > self.replicas_per_zone:
      raise errors.Config.InvalidValue(
          f'spanner_omni_root_servers_per_zone ({self.root_servers_per_zone})'
          f' must not exceed servers per zone ({self.replicas_per_zone}).'
      )

  def _CreateDependencies(self) -> None:
    """Creates the namespace, TLS and admin password Secrets."""
    self.GetClientVm()
    self._ValidateTopology()
    self.omni_version = ResolveLatestOmniReleaseVersion(self.spec.version)
    self._EnsureNamespaceExists()
    self._ConfigureTlsCertificatesAndSecrets()
    self._CreateAdminPasswordSecret()

  def _Create(self) -> None:
    """Installs the Spanner Omni Helm chart."""
    license_path = self._FetchLicenseKeyFile()
    try:
      helm_cmd = self._BuildHelmInstallCommand(license_path)
      logging.info(
          'Deploying Spanner Omni Helm chart %s (image.tag=%s)',
          self.spec.chart_uri,
          self.omni_version,
      )
      try:
        helm.RunHelmCommand(helm_cmd)
      except Exception:
        # `created` is only set after `_Create` succeeds, so `Delete` would
        # skip a partially installed release; clean it up here instead.
        logging.exception('Helm install failed; removing partial release.')
        self._Delete()
        raise
    finally:
      if os.path.exists(license_path):
        os.remove(license_path)

  def _BuildHelmInstallCommand(self, license_path: str) -> list[str]:
    set_values = [
        f'global.platform={self.PLATFORM}',
        'global.insecureMode=false',
        f'image.tag={self.omni_version}',
        f'deployment.replicasPerZone={self.replicas_per_zone}',
        f'deployment.rootServersPerZone={self.root_servers_per_zone}',
        'deployment.enablePasswordAuthentication=true',
        'deployment.enableClientCertificateAuthentication=false',
        f'deployment.adminPasswordSecret={_ADMIN_PASSWORD_SECRET_NAME}',
        f'resources.cpu={self.spec.cpu_request}',
        f'resources.memory={self.spec.memory_request}',
        'createStorageClasses=true',
        f'dataStorageClass={self.STORAGE_CLASS}',
        f'logsStorageClass={self.STORAGE_CLASS}',
        f'storage.data.size={self.spec.data_disk_size}',
        f'storage.logs.size={self.spec.logs_disk_size}',
        f'probes.liveness.failureThreshold={_LIVENESS_PROBE_FAILURE_THRESHOLD}',
        f'probes.liveness.timeoutSeconds={_LIVENESS_PROBE_TIMEOUT_SEC}',
    ]
    cmd = [
        'upgrade',
        '--install',
        RELEASE_NAME,
        self.spec.chart_uri,
        '--namespace',
        NAMESPACE,
        '--create-namespace',
    ]
    for value in set_values:
      cmd.extend(['--set', value])
    cmd.extend([
        '--set-json',
        f'locations={BuildLocationsJson(self.zones)}',
        '--set-file',
        f'licenseKey={license_path}',
    ])
    if self.spec.chart_version:
      cmd.extend(['--version', self.spec.chart_version])
    return cmd

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
    if self.create_database:
      self.CreateDatabase(self.database_name)

  def _Delete(self) -> None:
    """Uninstalls Spanner Omni and deletes its PVCs and PVs.

    Waits for the persistent volumes bound to the namespace's claims to be
    deleted so the underlying disks do not leak (leaked disks exhaust quota).
    """
    helm.RunHelmCommand(
        ['uninstall', RELEASE_NAME, '--namespace', NAMESPACE],
        raise_on_failure=False,
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
    volume_names = (stdout or '').split()
    commands = [
        ['delete', 'pods', '--all', '-n', NAMESPACE],
        ['delete', 'pvc', '--all', '-n', NAMESPACE],
    ]
    if volume_names:
      commands.append([
          'wait',
          '--for=delete',
          *[f'pv/{name}' for name in volume_names],
          '--timeout=300s',
      ])
    commands.append([
        'delete',
        'namespace',
        NAMESPACE,
        '--ignore-not-found=true',
        '--wait=false',
    ])
    for command in commands:
      kubectl.RunKubectlCommand(
          command, raise_on_failure=False, raise_on_timeout=False
      )

  def GetResourceMetadata(self) -> dict[str, Any]:
    """Returns cluster and Spanner Omni configuration metadata."""
    metadata: dict[str, Any] = {}
    client_machine_type = getattr(self.client_vm, 'machine_type', None)
    metadata.update({
        'db_engine': sql_engine_utils.SPANNER_POSTGRES,
        'spanner_omni_zones': ','.join(self.zones),
        'spanner_omni_chart_uri': self.spec.chart_uri,
        'spanner_omni_replicas_per_zone': self.replicas_per_zone,
        'spanner_omni_root_servers_per_zone': self.root_servers_per_zone,
        'spanner_omni_cpu_request': self.spec.cpu_request,
        'spanner_omni_memory_request': self.spec.memory_request,
        'spanner_omni_storage_class': self.STORAGE_CLASS,
        'spanner_omni_data_disk_size': self.spec.data_disk_size,
        'spanner_omni_logs_disk_size': self.spec.logs_disk_size,
        'spanner_omni_run_analyze': self.spec.run_analyze,
    })
    optional_metadata = {
        'spanner_omni_cluster_machine_type': self.cluster_machine_type,
        'spanner_omni_client_machine_type': (
            client_machine_type
            if isinstance(client_machine_type, str)
            else None
        ),
        'spanner_omni_client_zone': (
            self.client_zone if self.client_vm is not None else None
        ),
        'spanner_omni_version': self.omni_version,
        'spanner_omni_chart_version': self.spec.chart_version,
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
        raise_on_failure=False,
    )
    if retcode != 0 and not IsAlreadyExistsError(stderr):
      raise errors.Resource.CreationError(
          f'Failed to create namespace {NAMESPACE!r}: {stderr}'
      )

  def _ConfigureTlsCertificatesAndSecrets(self) -> None:
    """Generates TLS certs via the Spanner CLI on the client VM.

    The certificates are applied as the `tls-certs` Kubernetes Secret used by
    the chart and stay on the client VM for the PGAdapter truststore.
    """
    client_vm = self.GetClientVm()
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
    ca_flags = (
        f'--ca-certificate-directory={_CLIENT_TLS_DIR} '
        f'--ca-private-key-directory={_CLIENT_TLS_DIR}'
    )
    cert_script = (
        f'rm -rf {_CLIENT_CLI_DIR} {_CLIENT_TLS_DIR} && '
        f'mkdir -p {_CLIENT_CLI_DIR} {_CLIENT_TLS_DIR} && '
        f'chmod 700 {_CLIENT_TLS_DIR} && '
        f'curl -fsSL "{cli_url}" -o {_CLIENT_CLI_DIR}/cli.tar.gz && '
        f'tar -xzf {_CLIENT_CLI_DIR}/cli.tar.gz -C {_CLIENT_CLI_DIR} && '
        f'chmod +x {cli_bin} && '
        f'{cli_bin} certificates create-ca {ca_flags} && '
        f'cp {_CLIENT_TLS_DIR}/ca.crt {_CLIENT_TLS_DIR}/ca-api.crt && '
        f'{cli_bin} certificates create-server {ca_flags}'
        f' --output-directory={_CLIENT_TLS_DIR}'
        f' --filename-prefix=server --hostnames="{pod_hosts}" && '
        f'{cli_bin} certificates create-server {ca_flags}'
        f' --output-directory={_CLIENT_TLS_DIR}'
        f' --filename-prefix=api --hostnames="{svc_hosts},{pod_hosts}" && '
        "python3 - << 'EOF'\n"
        'import base64, json, pathlib\n'
        f'tls_dir = pathlib.Path("{_CLIENT_TLS_DIR}")\n'
        'tls_data = {}\n'
        'for name in ("ca.crt", "ca-api.crt", "server.crt", "server.key",'
        ' "api.crt", "api.key"):\n'
        '  tls_data[name] = base64.b64encode((tls_dir /'
        ' name).read_bytes()).decode("ascii")\n'
        'secret = {\n'
        '  "apiVersion": "v1",\n'
        '  "kind": "Secret",\n'
        f'  "metadata": {{"name": "{_TLS_SECRET_NAME}", "namespace":'
        f' "{NAMESPACE}"}},\n'
        '  "type": "Opaque",\n'
        '  "data": tls_data,\n'
        '}\n'
        f'pathlib.Path("{_CLIENT_TLS_SECRETS_FILE}").write_text('
        'json.dumps(secret))\n'
        'EOF'
    )
    client_vm.RemoteCommand(cert_script)
    local_secrets_path = os.path.join(
        vm_util.GetTempDir(), _LOCAL_TLS_SECRETS_FILENAME
    )
    try:
      client_vm.PullFile(local_secrets_path, _CLIENT_TLS_SECRETS_FILE)
      os.chmod(local_secrets_path, 0o600)
      kubectl.RunRetryableKubectlCommand(
          ['apply', '-n', NAMESPACE, '-f', local_secrets_path]
      )
    finally:
      if os.path.exists(local_secrets_path):
        os.remove(local_secrets_path)
      client_vm.RemoteCommand(
          f'rm -f {_CLIENT_TLS_SECRETS_FILE}', ignore_failure=True
      )

  def _CreateAdminPasswordSecret(self) -> None:
    """Generates a random admin password and stores it in a K8s Secret."""
    self._admin_password = _GenerateAdminPassword()
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
    with PrivateTempFile(
        _LOCAL_ADMIN_SECRET_FILENAME, json.dumps(secret)
    ) as path:
      kubectl.RunRetryableKubectlCommand(['apply', '-n', NAMESPACE, '-f', path])

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
    stdout, _, _ = kubectl.RunRetryableKubectlCommand([
        'get',
        'pods',
        '-n',
        NAMESPACE,
        '-l',
        'app.kubernetes.io/component=spanner',
        '-o',
        'json',
    ])
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
      preferred_short = BuildZoneShortNames(self.zones).get(
          preferred_zone,
          preferred_zone.rsplit('-', 1)[1] if '-' in preferred_zone else 'a',
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
    stdout, stderr, retcode = kubectl.RunRetryableKubectlCommand(
        ['get', 'job', _BOOTSTRAP_JOB_NAME, '-n', NAMESPACE, '-o', 'json'],
        raise_on_failure=False,
    )
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
      self, cli_args: Sequence[str], timeout: int | None = None
  ) -> tuple[str, str, int]:
    """Runs a CLI command, re-logging in on timeouts and auth errors.

    Args:
      cli_args: Spanner Omni CLI arguments.
      timeout: Optional timeout in seconds for the command.

    Returns:
      `(stdout, stderr, retcode)` of the command (retried once after an
      authentication error).

    Raises:
      errors.Resource.RetryableCreationError: If the command timed out.
    """

    def _Run() -> tuple[str, str, int]:
      try:
        return self._RunCli(cli_args, timeout=timeout, raise_on_failure=False)
      except (errors.VmUtil.IssueCommandTimeoutError, vm_util.RetryError) as e:
        self.LoginSpannerCli(raise_on_failure=False)
        raise errors.Resource.RetryableCreationError(
            f'Spanner Omni CLI command {list(cli_args[:2])} timed out: {e}'
        ) from e

    stdout, stderr, retcode = _Run()
    if retcode != 0 and _IsAuthError(stderr):
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
        f'cat > {_POD_ADMIN_PASSWORD_FILE} && '
        f'chmod 600 {_POD_ADMIN_PASSWORD_FILE} && '
        f'HOME=/tmp {_SPANNER_CLI_BIN} auth login {_ADMIN_USERNAME} '
        f'--password-file={_POD_ADMIN_PASSWORD_FILE} '
        f'--ca-certificate-file={_POD_CA_CERT_PATH} '
        f'--deployment-endpoint=dns:///127.0.0.1:{_SPANNER_GRPC_PORT}; '
        f'rc=$?; rm -f {_POD_ADMIN_PASSWORD_FILE}; exit $rc'
    )
    kubectl_args = [FLAGS.kubectl]
    kubeconfig = getattr(FLAGS, 'kubeconfig', None)
    if kubeconfig:
      kubectl_args.extend(['--kubeconfig', kubeconfig])
    kubectl_args.extend([
        'exec',
        '-i',
        '-n',
        NAMESPACE,
        self.first_pod_name,
        '--',
        'sh',
        '-c',
        login_script,
    ])
    kubectl_cmd = shlex.join(kubectl_args)
    with PrivateTempFile(
        _LOCAL_ADMIN_PASSWORD_FILENAME, self._GetAdminPassword()
    ) as password_path:
      vm_util.IssueCommand(
          ['bash', '-c', f'{kubectl_cmd} < {shlex.quote(password_path)}']
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
      logging.warning(
          'Failed refreshing Spanner Omni CLI login on %s; continuing.',
          self.first_pod_name,
      )

  def CreateDatabase(self, database_name: str) -> None:
    """Creates a PostgreSQL-dialect database; an existing one is a no-op.

    Args:
      database_name: Name of the database to create.

    Raises:
      errors.Resource.CreationError: If creation fails for any reason other
        than the database already existing.
      errors.Resource.RetryableCreationError: If the CLI command timed out.
    """
    _, stderr, retcode = self._RunAuthenticatedCli([
        'databases',
        'create',
        database_name,
        f'--database-dialect={_DATABASE_DIALECT}',
    ])
    if retcode != 0 and not IsAlreadyExistsError(stderr):
      raise errors.Resource.CreationError(
          f'Failed to create Spanner Omni database {database_name!r}: {stderr}'
      )

  def DeleteDatabase(self, database_name: str) -> None:
    """Deletes a database; a missing database is a no-op.

    Args:
      database_name: Name of the database to delete.

    Raises:
      errors.VmUtil.IssueCommandError: If deletion fails for any reason other
        than the database not existing.
      errors.Resource.RetryableCreationError: If the CLI command timed out.
    """
    _, stderr, retcode = self._RunAuthenticatedCli(
        ['databases', 'delete', database_name, '--quiet']
    )
    if retcode != 0 and not IsNotFoundError(stderr):
      raise errors.VmUtil.IssueCommandError(
          f'Failed to delete Spanner Omni database {database_name!r}: {stderr}'
      )

  def DatabaseExists(self, database_name: str) -> bool:
    """Returns True if `databases describe` confirms `database_name` exists."""
    try:
      _, _, retcode = self._RunAuthenticatedCli(
          ['databases', 'describe', database_name]
      )
    except errors.Resource.RetryableCreationError:
      return False
    return retcode == 0

  def RunDDLQuery(
      self,
      sql_query: str,
      timeout: int = _SPANNER_ANALYZE_TIMEOUT_SEC,
      async_proc: bool = False,
      database_name: str | None = None,
  ) -> None:
    """Runs a DDL statement via the CLI and waits for the operation.

    `ANALYZE` is skipped when the spec's `run_analyze` is false. Matches the
    `relational_db.RunDDLQuery` hook used by BenchBase.

    Args:
      sql_query: The DDL statement (e.g. `ANALYZE;`).
      timeout: Unused; the statement is submitted with `--async` (300 s submit
        timeout) and the operation is polled for up to 6 h.
      async_proc: Unused; the statement is always submitted with `--async` and
        polled until done.
      database_name: Target database; defaults to `database_name`.

    Raises:
      errors.Resource.CreationError: If the DDL cannot be submitted or the
        operation ID cannot be parsed.
    """
    del timeout, async_proc
    database_name = database_name or self.database_name
    if _IsAnalyzeStatement(sql_query) and not self.spec.run_analyze:
      logging.info('Skipping ANALYZE (--spanner_omni_run_analyze=false).')
      return
    logging.info(
        'Running DDL %r on Spanner Omni database %s', sql_query, database_name
    )
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
      raise errors.Resource.CreationError(
          f'Failed to submit DDL {sql_query!r}: {stderr}'
      )
    operation_id = ''
    output = f'{_StripCliSpinnerOutput(stdout)}\n{stderr}'
    for line in output.splitlines():
      if 'Operation name:' in line:
        operation_name = line.split('Operation name:', 1)[1].strip()
        operation_id = operation_name.rsplit('/', 1)[-1]
        break
    if not operation_id:
      raise errors.Resource.CreationError(
          f'Failed to parse DDL operation ID from output: {stdout}\n{stderr}'
      )
    self.WaitForDatabaseOperation(operation_id, database_name)

  @vm_util.Retry(
      poll_interval=30,
      fuzz=0,
      timeout=_SPANNER_ANALYZE_TIMEOUT_SEC,
      retryable_exceptions=(errors.Resource.RetryableCreationError,),
  )
  def WaitForDatabaseOperation(
      self, operation_id: str, database_name: str
  ) -> None:
    """Polls a Spanner Omni database operation until `done: true`."""
    stdout, stderr, retcode = self._RunAuthenticatedCli([
        'operations',
        'describe',
        operation_id,
        f'--database={database_name}',
    ])
    if retcode != 0:
      raise errors.Resource.RetryableCreationError(
          f'Failed to describe Spanner Omni operation {operation_id}: {stderr}'
      )
    cleaned_stdout, op_data = _ParseCliYaml(stdout)
    if isinstance(op_data, dict):
      failed = bool(op_data.get('error'))
      done = bool(op_data.get('done'))
    else:
      lines = [line.strip() for line in cleaned_stdout.splitlines()]
      failed = any(line.startswith('error:') for line in lines)
      done = 'done: true' in lines
    if failed:
      raise errors.Resource.CreationError(
          f'Spanner Omni operation {operation_id} failed:\n{cleaned_stdout}'
      )
    if done:
      logging.info('Spanner Omni operation %s completed.', operation_id)
      return
    raise errors.Resource.RetryableCreationError(
        f'Spanner Omni operation {operation_id} still in'
        f' progress:\n{cleaned_stdout}'
    )


def GetSpannerOmniClusterClass(
    cloud: str,
) -> type[BaseSpannerOmniCluster]:
  """Returns the `BaseSpannerOmniCluster` subclass registered for `cloud`."""
  return resource.GetResourceClass(  # pyrefly: ignore[bad-return]
      BaseSpannerOmniCluster, CLOUD=cloud
  )
