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

"""Run Sysbench against containerized AlloyDB Omni on Kubernetes.

Deploys a standalone AlloyDB Omni container (without an Operator) as a
Kubernetes StatefulSet and executes Sysbench OLTP workloads from a client pod in
the same cluster.
"""

from collections.abc import Sequence
import functools
import json
import logging
import os
import re
import time
from typing import Any

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_benchmarks import sysbench_benchmark  # pylint: disable=unused-import
from perfkitbenchmarker.linux_packages import postgresql
from perfkitbenchmarker.linux_packages import sysbench
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS

_CLOUD_STORAGE_PROVISIONERS = {
    'GCP': 'pd.csi.storage.gke.io',
    'AWS': 'ebs.csi.aws.com',
    'Azure': 'disk.csi.azure.com',
}

_DEFAULT_CLOUD_DISK_TYPES = {
    'GCP': 'hyperdisk-balanced',
    'AWS': 'gp3',
    'Azure': 'Premium_LRS',
}

_GCP_PD_SSD_PREFIXES = (
    'e2-',
    'n1-',
    'n2-',
    'n2d-',
    'c2-',
    'c2d-',
    't2a-',
    't2d-',
)

_BINARY_MEMORY_UNITS_GIB = {
    'Ki': 1.0 / (1024.0 * 1024.0),
    'Mi': 1.0 / 1024.0,
    'Gi': 1.0,
    'Ti': 1024.0,
    'Pi': 1024.0 * 1024.0,
    'Ei': 1024.0 * 1024.0 * 1024.0,
}

_DECIMAL_MEMORY_UNITS_GIB = {
    'k': 1000.0 / (1024.0**3),
    'K': 1000.0 / (1024.0**3),
    'M': (1000.0**2) / (1024.0**3),
    'G': (1000.0**3) / (1024.0**3),
    'T': (1000.0**4) / (1024.0**3),
    'P': (1000.0**5) / (1024.0**3),
    'E': (1000.0**6) / (1024.0**3),
}

_CLIENT_POD_NAME = 'postgres-client'
_SERVER_POD_NAME = 'postgres-standalone-0'
_SERVER_STS_NAME = 'postgres-standalone'
_NAMESPACE_RE = re.compile(r'^[a-z0-9]([-a-z0-9]*[a-z0-9])?$')
_LOAD_TIMEOUT_SECONDS = 7200
_STABILIZE_CMD_TIMEOUT_SECONDS = 600

_OMNI_IMAGE = flags.DEFINE_string(
    'kubernetes_alloydb_omni_image',
    'google/alloydbomni:16',
    'Container image for AlloyDB Omni.',
)
_TEMPLATE_PATH = flags.DEFINE_string(
    'kubernetes_alloydb_omni_sysbench_template_path',
    'container/postgres_sysbench/alloydb_omni_all.yaml.j2',
    'Path to the AlloyDB Omni Kubernetes manifest template file.',
)
_NAMESPACE = flags.DEFINE_string(
    'kubernetes_alloydb_omni_namespace',
    'default',
    'Kubernetes namespace for AlloyDB Omni and Sysbench resources.',
)
_SERVER_MACHINE_TYPE = flags.DEFINE_string(
    'kubernetes_alloydb_omni_server_machine_type',
    '',
    'Machine type override for AlloyDB Omni server nodes.',
)
_CLIENT_MACHINE_TYPE = flags.DEFINE_string(
    'kubernetes_alloydb_omni_client_machine_type',
    '',
    'Machine type override for Sysbench client nodes.',
)
_USE_INIT_CONTAINER = flags.DEFINE_bool(
    'kubernetes_alloydb_omni_use_init_container',
    True,
    'Whether to run an init container to set data directory permissions.',
)
_SERVER_CPU_REQUEST = flags.DEFINE_string(
    'kubernetes_alloydb_omni_server_cpu_request',
    '',
    'CPU request for the AlloyDB Omni pod (auto-sized from node if empty).',
)
_SERVER_CPU_LIMIT = flags.DEFINE_string(
    'kubernetes_alloydb_omni_server_cpu_limit',
    '',
    'CPU limit for the AlloyDB Omni pod (auto-sized from node if empty).',
)
_SERVER_MEMORY_REQUEST = flags.DEFINE_string(
    'kubernetes_alloydb_omni_server_memory_request',
    '',
    'Memory request for the AlloyDB Omni pod (auto-sized from node if empty).',
)
_SERVER_MEMORY_LIMIT = flags.DEFINE_string(
    'kubernetes_alloydb_omni_server_memory_limit',
    '',
    'Memory limit for the AlloyDB Omni pod (auto-sized from node if empty).',
)
_SHARED_BUFFERS = flags.DEFINE_string(
    'kubernetes_alloydb_omni_shared_buffers',
    '',
    'PostgreSQL shared_buffers (auto-sized to ~25% node RAM if empty; pass'
    ' "0" to use AlloyDB Omni dynamic shared_buffers sizing).',
)
_EFFECTIVE_CACHE_SIZE = flags.DEFINE_string(
    'kubernetes_alloydb_omni_effective_cache_size',
    '',
    'PostgreSQL effective_cache_size (auto-sized to ~50% node RAM if empty).',
)
_SHM_SIZE = flags.DEFINE_string(
    'kubernetes_alloydb_omni_shm_size',
    '',
    'Size limit for /dev/shm memory-backed emptyDir (defaults to server memory'
    ' limit).',
)
_ENABLE_COLUMNAR_ENGINE = flags.DEFINE_bool(
    'kubernetes_alloydb_omni_enable_columnar_engine',
    False,
    'Whether to enable AlloyDB Omni google_columnar_engine.',
)
_COLUMNAR_MEMORY_SIZE_MB = flags.DEFINE_integer(
    'kubernetes_alloydb_omni_columnar_memory_size_mb',
    0,
    'Memory in MB allocated to google_columnar_engine when > 0.',
)
_POSTGRES_FLAGS = flags.DEFINE_list(
    'kubernetes_alloydb_omni_postgres_flags',
    [],
    'Additional key=value PostgreSQL/AlloyDB Omni GUC flags passed via -c.',
)
_CLIENT_IMAGE = flags.DEFINE_string(
    'kubernetes_alloydb_omni_client_image',
    'ubuntu:22.04',
    'Container image for the Sysbench client pod.',
)
_CLIENT_CPU_REQUEST = flags.DEFINE_string(
    'kubernetes_alloydb_omni_client_cpu_request',
    '4',
    'CPU request for the Sysbench client pod.',
)
_CLIENT_MEMORY_REQUEST = flags.DEFINE_string(
    'kubernetes_alloydb_omni_client_memory_request',
    '10Gi',
    'Memory request for the Sysbench client pod.',
)
_CLIENT_CPU_LIMIT = flags.DEFINE_string(
    'kubernetes_alloydb_omni_client_cpu_limit',
    '8',
    'CPU limit for the Sysbench client pod ("None" to omit).',
)
_CLIENT_MEMORY_LIMIT = flags.DEFINE_string(
    'kubernetes_alloydb_omni_client_memory_limit',
    '20Gi',
    'Memory limit for the Sysbench client pod ("None" to omit).',
)

BENCHMARK_NAME = 'kubernetes_alloydb_omni_sysbench'
BENCHMARK_CONFIG = """
kubernetes_alloydb_omni_sysbench:
  description: >
    Run Sysbench against containerized AlloyDB Omni on Kubernetes.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: e2-medium
        zone: us-central1-a
      AWS:
        machine_type: m6i.large
        zone: us-east-1a
      Azure:
        machine_type: Standard_D2s_v5
        zone: eastus-1
    nodepools:
      postgres:
        vm_spec:
          GCP:
            machine_type: c4-standard-16
            zone: us-central1-a
            boot_disk_size: 500
          AWS:
            machine_type: m7i.4xlarge
            zone: us-east-1a
            boot_disk_size: 500
          Azure:
            machine_type: Standard_D16s_v5
            zone: eastus-1
            boot_disk_size: 500
            boot_disk_type: Premium_LRS
        vm_count: 1
      clients:
        vm_spec:
          GCP:
            machine_type: c4-standard-16
            zone: us-central1-a
            boot_disk_size: 100
          AWS:
            machine_type: m7i.4xlarge
            zone: us-east-1a
            boot_disk_size: 100
          Azure:
            machine_type: Standard_D16s_v5
            zone: eastus-1
            boot_disk_size: 100
            boot_disk_type: Premium_LRS
        vm_count: 1
  flags:
    sysbench_tables: 10
    sysbench_table_size: 4000000
    sysbench_run_threads: 512
    sysbench_run_seconds: 300
    sysbench_report_interval: 10
    sysbench_testname: oltp_read_write
"""


class _DatabaseBusyError(errors.VmUtil.IssueCommandError):
  """Raised when database connections or autovacuum workers are still active."""


def ParseK8sCpu(value: str) -> float:
  """Parses a Kubernetes CPU quantity string into cores."""
  raw = value.strip()
  if not raw:
    raise ValueError('Empty CPU quantity')
  if raw.endswith('m'):
    return float(raw[:-1]) / 1000.0
  if raw.endswith('n'):
    return float(raw[:-1]) / 1_000_000_000.0
  return float(raw)


def ParseK8sMemoryGiB(value: str) -> float:
  """Parses a Kubernetes memory quantity string into GiB."""
  raw = value.strip()
  if not raw:
    raise ValueError('Empty memory quantity')
  for suffix, factor in _BINARY_MEMORY_UNITS_GIB.items():
    if raw.endswith(suffix):
      return float(raw[: -len(suffix)]) * factor
  for suffix, factor in _DECIMAL_MEMORY_UNITS_GIB.items():
    if raw.endswith(suffix):
      return float(raw[: -len(suffix)]) * factor
  return float(raw) / (1024.0**3)


def _GetNamespace() -> str:
  """Returns the target Kubernetes namespace."""
  return _NAMESPACE.value or 'default'


def _IsPinToNodepool(bm_spec: benchmark_spec.BenchmarkSpec) -> bool:
  """Returns whether pods should bind to PKB-created nodepools."""
  cluster = bm_spec.container_cluster
  return not getattr(cluster, 'user_managed', False)


def _GetPostgresPassword() -> str:
  """Returns a deterministic benchmark password derived from run_uri."""
  return postgresql.GetPsqlUserPassword(FLAGS.run_uri or 'omni')


def _GetStorageClassName(namespace: str) -> str:
  """Returns the StorageClass name scoped to the target namespace."""
  return (
      'postgres-storage-class'
      if namespace == 'default'
      else f'postgres-storage-class-{namespace}'
  )


def _ResolveEffectiveServerMachineType(
    bm_spec: benchmark_spec.BenchmarkSpec,
    detected_machine_type: str | None = None,
) -> str:
  """Resolves server machine type from flag, live static node, or spec."""
  if _SERVER_MACHINE_TYPE.value:
    return _SERVER_MACHINE_TYPE.value
  if (
      getattr(bm_spec.container_cluster, 'user_managed', False)
      and detected_machine_type
  ):
    return detected_machine_type
  configured = _GetServerMachineType(bm_spec)
  if configured == 'auto' and detected_machine_type:
    return detected_machine_type
  return configured


def _ResolveDiskType(
    cloud: str,
    bm_spec: benchmark_spec.BenchmarkSpec,
    detected_machine_type: str | None = None,
) -> str:
  """Selects the PersistentVolume StorageClass disk type for the cloud."""
  if FLAGS.data_disk_type:
    return FLAGS.data_disk_type
  if cloud == 'GCP':
    machine_type = _ResolveEffectiveServerMachineType(
        bm_spec, detected_machine_type
    )
    if machine_type.startswith(_GCP_PD_SSD_PREFIXES):
      return 'pd-ssd'
  return _DEFAULT_CLOUD_DISK_TYPES.get(cloud, 'hyperdisk-balanced')


def _GetServerMachineType(bm_spec: benchmark_spec.BenchmarkSpec) -> str:
  """Returns the configured server machine type string."""
  if _SERVER_MACHINE_TYPE.value:
    return _SERVER_MACHINE_TYPE.value
  cluster = bm_spec.container_cluster
  if cluster and 'postgres' in getattr(cluster, 'nodepools', {}):
    machine_type = cluster.nodepools['postgres'].machine_type
    if machine_type:
      return machine_type
  nodepools = getattr(
      getattr(bm_spec, 'config', None), 'container_cluster', None
  )
  if nodepools and getattr(nodepools, 'nodepools', None):
    pg_pool = nodepools.nodepools.get('postgres')
    cloud = getattr(cluster, 'CLOUD', 'GCP')
    if pg_pool and getattr(pg_pool, 'vm_spec', None):
      cloud_spec = pg_pool.vm_spec
      if isinstance(cloud_spec, dict) and cloud in cloud_spec:
        return cloud_spec[cloud].get('machine_type', 'auto')
  return 'auto'


def _InspectClusterNodes(
    pool_name: str,
    user_managed: bool,
) -> tuple[tuple[float, float, float, float] | None, str | None]:
  """Queries node resources and detected instance type from Kubernetes.

  Args:
    pool_name: PKB nodepool label value (`postgres` or `clients`).
    user_managed: Whether the cluster is user-managed (no `pkb_nodepool` label).

  Returns:
    Tuple of (node_resources, detected_machine_type), where node_resources is
    (alloc_cpu, alloc_mem_gib, cap_cpu, cap_mem_gib) or None if unavailable.
  """
  cmd = ['get', 'nodes']
  if not user_managed:
    cmd.extend(['-l', f'pkb_nodepool={pool_name}'])
  cmd.extend(['-o', 'json'])
  stdout, _, retcode = kubectl.RunKubectlCommand(cmd, raise_on_failure=False)
  if retcode != 0 or not stdout:
    return None, None
  try:
    payload = json.loads(stdout)
  except ValueError:
    logging.warning(
        'Failed to parse kubectl get nodes JSON for pool %s.', pool_name
    )
    return None, None
  items = payload.get('items') or []
  if not items:
    return None, None

  alloc_cpus: list[float] = []
  alloc_mems: list[float] = []
  cap_cpus: list[float] = []
  cap_mems: list[float] = []
  detected_machine_type: str | None = None
  for node in items:
    if detected_machine_type is None:
      labels = node.get('metadata', {}).get('labels', {})
      detected_machine_type = (
          labels.get('node.kubernetes.io/instance-type')
          or labels.get('beta.kubernetes.io/instance-type')
      )
    status = node.get('status', {})
    alloc = status.get('allocatable', {})
    cap = status.get('capacity', alloc)
    try:
      if 'cpu' in alloc and 'memory' in alloc:
        alloc_cpus.append(ParseK8sCpu(alloc['cpu']))
        alloc_mems.append(ParseK8sMemoryGiB(alloc['memory']))
        cap_cpus.append(ParseK8sCpu(cap.get('cpu', alloc['cpu'])))
        cap_mems.append(ParseK8sMemoryGiB(cap.get('memory', alloc['memory'])))
    except ValueError:
      continue

  if not alloc_cpus:
    return None, detected_machine_type
  return (
      min(alloc_cpus),
      min(alloc_mems),
      min(cap_cpus),
      min(cap_mems),
  ), detected_machine_type


def _QueryNodeResources(
    pool_name: str,
    user_managed: bool,
) -> tuple[float, float, float, float] | None:
  """Queries Kubernetes node allocatable and capacity CPU and memory (GiB)."""
  resources, _ = _InspectClusterNodes(pool_name, user_managed)
  return resources


def ResolveServerResources(
    node_resources: tuple[float, float, float, float] | None,
) -> dict[str, str]:
  """Computes server pod CPU, memory, shared_buffers, and shm_size values."""
  if node_resources is None:
    defaults = {
        'cpu_request': '6',
        'cpu_limit': '14',
        'memory_request': '24Gi',
        'memory_limit': '45Gi',
        'shared_buffers': '15GB',
        'effective_cache_size': '30GB',
    }
  else:
    alloc_cpu, alloc_mem_gib, _, cap_mem_gib = node_resources
    cpu_req = max(1, int(alloc_cpu * 0.75))
    cpu_lim = max(cpu_req, int(alloc_cpu * 0.90))
    mem_req = max(2, int(alloc_mem_gib * 0.75))
    mem_lim = max(mem_req, int(alloc_mem_gib * 0.85))
    shared_buf_gb = max(1, round(cap_mem_gib * 0.25))
    eff_cache_gb = max(2, round(cap_mem_gib * 0.50))
    defaults = {
        'cpu_request': str(cpu_req),
        'cpu_limit': str(cpu_lim),
        'memory_request': f'{mem_req}Gi',
        'memory_limit': f'{mem_lim}Gi',
        'shared_buffers': f'{shared_buf_gb}GB',
        'effective_cache_size': f'{eff_cache_gb}GB',
    }

  cpu_request = _SERVER_CPU_REQUEST.value or defaults['cpu_request']
  cpu_limit = _SERVER_CPU_LIMIT.value or defaults['cpu_limit']
  memory_request = _SERVER_MEMORY_REQUEST.value or defaults['memory_request']
  memory_limit = _SERVER_MEMORY_LIMIT.value or defaults['memory_limit']
  shared_buffers = _SHARED_BUFFERS.value or defaults['shared_buffers']
  effective_cache_size = (
      _EFFECTIVE_CACHE_SIZE.value or defaults['effective_cache_size']
  )
  shm_size = _SHM_SIZE.value or (
      memory_limit if memory_limit != 'None' else ''
  )
  return {
      'cpu_request': cpu_request,
      'cpu_limit': cpu_limit,
      'memory_request': memory_request,
      'memory_limit': memory_limit,
      'shared_buffers': shared_buffers,
      'effective_cache_size': effective_cache_size,
      'shm_size': shm_size,
  }


def ResolveClientResources(
    node_resources: tuple[float, float, float, float] | None,
) -> dict[str, str]:
  """Computes client pod CPU and memory requests/limits with node clamping."""
  cpu_request = _CLIENT_CPU_REQUEST.value
  cpu_limit = _CLIENT_CPU_LIMIT.value
  memory_request = _CLIENT_MEMORY_REQUEST.value
  memory_limit = _CLIENT_MEMORY_LIMIT.value

  if node_resources is None:
    return {
        'client_cpu_request': cpu_request,
        'client_cpu_limit': cpu_limit,
        'client_memory_request': memory_request,
        'client_memory_limit': memory_limit,
    }

  alloc_cpu, alloc_mem_gib, _, _ = node_resources
  max_cpu_req = max(1, int(alloc_cpu * 0.75))
  max_cpu_lim = max(max_cpu_req, int(alloc_cpu * 0.90))
  max_mem_req = max(1, int(alloc_mem_gib * 0.75))
  max_mem_lim = max(max_mem_req, int(alloc_mem_gib * 0.85))

  if (
      not FLAGS[_CLIENT_CPU_REQUEST.name].present
      and ParseK8sCpu(cpu_request) > max_cpu_req
  ):
    cpu_request = str(max_cpu_req)
  if (
      not FLAGS[_CLIENT_CPU_LIMIT.name].present
      and cpu_limit != 'None'
      and ParseK8sCpu(cpu_limit) > max_cpu_lim
  ):
    cpu_limit = str(max_cpu_lim)
  if (
      not FLAGS[_CLIENT_MEMORY_REQUEST.name].present
      and ParseK8sMemoryGiB(memory_request) > max_mem_req
  ):
    memory_request = f'{max_mem_req}Gi'
  if (
      not FLAGS[_CLIENT_MEMORY_LIMIT.name].present
      and memory_limit != 'None'
      and ParseK8sMemoryGiB(memory_limit) > max_mem_lim
  ):
    memory_limit = f'{max_mem_lim}Gi'

  return {
      'client_cpu_request': cpu_request,
      'client_cpu_limit': cpu_limit,
      'client_memory_request': memory_request,
      'client_memory_limit': memory_limit,
  }


def CheckPrerequisites(benchmark_config: Any) -> None:
  """Verifies flags, template availability, and cluster configuration."""
  template_path = _TEMPLATE_PATH.value
  if not os.path.exists(template_path):
    try:
      data.ResourcePath(template_path)
    except (errors.Setup.InvalidSetupError, data.ResourceNotFound) as exc:
      raise errors.Setup.InvalidConfigurationError(
          f'Could not find manifest template file: {template_path}'
      ) from exc

  if not _NAMESPACE_RE.match(_GetNamespace()):
    raise errors.Setup.InvalidConfigurationError(
        f'Invalid Kubernetes namespace: {_GetNamespace()}'
    )

  for pg_flag in _POSTGRES_FLAGS.value:
    if '=' not in pg_flag or not pg_flag.split('=', 1)[0].strip():
      raise errors.Setup.InvalidConfigurationError(
          'Each entry in --kubernetes_alloydb_omni_postgres_flags must be'
          f' formatted as key=value, got: {pg_flag!r}'
      )

  if FLAGS.gke_node_system_config:
    cloud = benchmark_config.container_cluster.cloud
    if cloud != 'GCP':
      raise errors.Setup.InvalidConfigurationError(
          f'gke_node_system_config is only supported on GCP, got cloud {cloud}.'
      )


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Loads and returns the merged benchmark config spec."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  if _SERVER_MACHINE_TYPE.value:
    vm_spec = config['container_cluster']['nodepools']['postgres']['vm_spec']
    for cloud_spec in vm_spec.values():
      cloud_spec['machine_type'] = _SERVER_MACHINE_TYPE.value

  if _CLIENT_MACHINE_TYPE.value:
    vm_spec = config['container_cluster']['nodepools']['clients']['vm_spec']
    for cloud_spec in vm_spec.values():
      cloud_spec['machine_type'] = _CLIENT_MACHINE_TYPE.value

  return config


def _ExecInPod(
    pod_name: str,
    shell_cmd: str,
    namespace: str,
    timeout: int | None = vm_util.DEFAULT_TIMEOUT,
    raise_on_failure: bool = True,
) -> tuple[str, str, int]:
  """Executes a bash command inside a Kubernetes pod."""
  kubectl_cmd = [
      'exec',
      '-n',
      namespace,
      pod_name,
      '--',
      'bash',
      '-c',
      shell_cmd,
  ]
  return kubectl.RunKubectlCommand(
      kubectl_cmd,
      timeout=timeout,
      raise_on_failure=raise_on_failure,
  )


def _LogServerFailureDebugInfo(namespace: str) -> None:
  """Logs Kubernetes pod, StatefulSet, and database container diagnostics."""
  stdout, _, _ = kubectl.RunKubectlCommand(
      ['get', 'statefulset', '-n', namespace, '-o',
       'jsonpath={.items[0].metadata.name}'],
      raise_on_failure=False,
  )
  ss_name = stdout.strip() if stdout.strip() else _SERVER_STS_NAME

  pods_out, _, _ = kubectl.RunKubectlCommand(
      ['describe', 'pods', '-n', namespace],
      raise_on_failure=False,
  )
  logging.error('Pods description in namespace %s:\n%s', namespace, pods_out)

  ss_out, _, _ = kubectl.RunKubectlCommand(
      ['describe', 'statefulset', ss_name, '-n', namespace],
      raise_on_failure=False,
  )
  logging.error('StatefulSet %s description:\n%s', ss_name, ss_out)

  logs_out, _, _ = kubectl.RunKubectlCommand(
      ['logs', f'{ss_name}-0', '-n', namespace, '--tail=100'],
      raise_on_failure=False,
  )
  logging.error('AlloyDB Omni container logs:\n%s', logs_out)


@vm_util.Retry(
    max_retries=12,
    poll_interval=5,
    retryable_exceptions=(
        errors.VmUtil.IssueCommandError,
        kubectl.RetryableKubectlError,
    ),
)
def _WaitForDatabaseTcpReady(namespace: str) -> None:
  """Polls TCP readiness and SQL execution on the AlloyDB Omni pod."""
  kubectl.RunKubectlCommand([
      'exec',
      '-n',
      namespace,
      _SERVER_POD_NAME,
      '--',
      'pg_isready',
      '-h',
      '127.0.0.1',
      '-p',
      '5432',
      '-U',
      'benchmark',
      '-d',
      'benchmark',
  ])
  kubectl.RunKubectlCommand([
      'exec',
      '-n',
      namespace,
      _SERVER_POD_NAME,
      '--',
      'psql',
      '-h',
      '127.0.0.1',
      '-U',
      'benchmark',
      '-d',
      'benchmark',
      '-c',
      'SELECT 1',
  ])


def _GetServerPodIp(namespace: str) -> str:
  """Returns the Pod IP of the AlloyDB Omni server pod."""
  stdout, _, _ = kubectl.RunRetryableKubectlCommand([
      'get',
      'pod',
      _SERVER_POD_NAME,
      '-n',
      namespace,
      '-o',
      'jsonpath={.status.podIP}',
  ])
  pod_ip = stdout.strip()
  if not pod_ip:
    raise errors.Benchmarks.RunError(
        f'Pod {_SERVER_POD_NAME} in namespace {namespace} has no podIP.'
    )
  return pod_ip


def _EnsureNamespace(namespace: str) -> None:
  """Ensures the target namespace exists and permits privileged pods."""
  if namespace != 'default':
    _, _, rc = kubectl.RunKubectlCommand(
        ['get', 'namespace', namespace],
        raise_on_failure=False,
    )
    if rc != 0:
      kubectl.RunRetryableKubectlCommand(['create', 'namespace', namespace])
  kubectl.RunRetryableKubectlCommand([
      'label',
      'namespace',
      namespace,
      'pod-security.kubernetes.io/enforce=privileged',
      '--overwrite',
  ])


def _PrepareAlloyDBOmniCluster(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Deploys AlloyDB Omni as a StatefulSet on Kubernetes."""
  cluster = bm_spec.container_cluster
  cloud = cluster.CLOUD
  namespace = _GetNamespace()
  user_managed = getattr(cluster, 'user_managed', False)

  server_node_resources, detected_machine_type = _InspectClusterNodes(
      'postgres', user_managed
  )
  effective_machine_type = _ResolveEffectiveServerMachineType(
      bm_spec, detected_machine_type
  )
  disk_type = _ResolveDiskType(cloud, bm_spec, detected_machine_type)

  if cloud == 'Kubernetes':
    storage_class_provisioner = ''
    create_storage_class = False
    storage_class = ''
  else:
    storage_class_provisioner = _CLOUD_STORAGE_PROVISIONERS.get(cloud)
    if not storage_class_provisioner:
      raise errors.Setup.InvalidConfigurationError(
          f'Unsupported cloud for storage provisioner: {cloud}'
      )
    create_storage_class = True
    storage_class = _GetStorageClassName(namespace)
    if user_managed:
      # Recreate any prior static-cluster StorageClass cleanly because
      # StorageClass parameters are immutable across runs with different disks.
      kubectl.RunRetryableKubectlCommand([
          'delete',
          'storageclass',
          storage_class,
          '--ignore-not-found',
      ])

  server_resources = ResolveServerResources(server_node_resources)

  max_threads = max(
      [FLAGS.sysbench_load_threads, *(FLAGS.sysbench_run_threads or [64])]
  )
  max_connections = max(1000, max_threads + 100)

  template_params: dict[str, Any] = {
      'namespace': namespace,
      'omni_image': _OMNI_IMAGE.value,
      'postgres_version': '16',
      'storage_class': storage_class,
      'create_storage_class': create_storage_class,
      'storage_class_provisioner': storage_class_provisioner,
      'postgres_user': 'benchmark',
      'postgres_password': _GetPostgresPassword(),
      'postgres_database': 'benchmark',
      'disk_size': f'{FLAGS.data_disk_size or 100}Gi',
      'disk_type': disk_type,
      'pin_to_nodepool': _IsPinToNodepool(bm_spec),
      'use_init_container': _USE_INIT_CONTAINER.value,
      'host_network': False,
      'cpu_request': server_resources['cpu_request'],
      'cpu_limit': server_resources['cpu_limit'],
      'memory_request': server_resources['memory_request'],
      'memory_limit': server_resources['memory_limit'],
      'shm_size': server_resources['shm_size'],
      'max_connections': max_connections,
      'shared_buffers': server_resources['shared_buffers'],
      'effective_cache_size': server_resources['effective_cache_size'],
      'work_mem': '64MB',
      'max_worker_processes': 20,
      'max_parallel_workers_per_gather': 8,
      'max_parallel_workers': 12,
      'wal_buffers': '64MB',
      'max_wal_size': '16GB',
      'checkpoint_timeout': '5min',
      'checkpoint_completion_target': '0.9',
      'effective_io_concurrency': 100,
      'random_page_cost': '1.1',
      'autovacuum_max_workers': 3,
      'enable_columnar_engine': _ENABLE_COLUMNAR_ENGINE.value,
      'columnar_memory_size_mb': _COLUMNAR_MEMORY_SIZE_MB.value,
      'extra_postgres_flags': list(_POSTGRES_FLAGS.value),
  }

  kubernetes_commands.ApplyManifest(_TEMPLATE_PATH.value, **template_params)

  try:
    logging.info(
        'Waiting for AlloyDB Omni pod %s in namespace %s to become Ready...',
        _SERVER_POD_NAME,
        namespace,
    )

    @vm_util.Retry(
        max_retries=3,
        retryable_exceptions=(
            errors.VmUtil.IssueCommandTimeoutError,
            errors.VmUtil.IssueCommandError,
        ),
    )
    def _WaitForPodReady() -> None:
      cluster.WaitForResource(  # pyrefly: ignore[missing-attribute]
          f'pod/{_SERVER_POD_NAME}',
          'Ready',
          namespace=namespace,
          timeout=1800,
      )

    _WaitForPodReady()
    _WaitForDatabaseTcpReady(namespace)
    logging.info('AlloyDB Omni pod is ready and accepting TCP connections.')
  except Exception:
    logging.error('AlloyDB Omni pod failed to become ready in %s.', namespace)
    _LogServerFailureDebugInfo(namespace)
    raise

  service_ip = _GetServerPodIp(namespace)
  bm_spec.postgres_service_ip = service_ip  # pyrefly: ignore[missing-attribute]
  bm_spec.resolved_disk_type = disk_type  # pyrefly: ignore[missing-attribute]
  bm_spec.resolved_shm_size = server_resources['shm_size']  # pyrefly: ignore[missing-attribute]
  bm_spec.resolved_machine_type = effective_machine_type  # pyrefly: ignore[missing-attribute]
  logging.info('AlloyDB Omni server pod IP: %s', service_ip)


@vm_util.Retry(
    max_retries=5,
    poll_interval=10,
    retryable_exceptions=(
        errors.VmUtil.IssueCommandError,
        kubectl.RetryableKubectlError,
    ),
)
def _RunClientInstallStep(namespace: str, command: str) -> None:
  """Runs an installation command inside the Sysbench client pod."""
  _ExecInPod(_CLIENT_POD_NAME, command, namespace)


def _PrepareSysbenchClient(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Deploys the Sysbench client pod and installs sysbench and psql."""
  cluster = bm_spec.container_cluster
  namespace = _GetNamespace()
  user_managed = getattr(cluster, 'user_managed', False)

  kubectl.RunRetryableKubectlCommand([
      'delete',
      'secret',
      'sysbench-passwords',
      '-n',
      namespace,
      '--ignore-not-found',
  ])
  kubectl.RunRetryableKubectlCommand([
      'create',
      'secret',
      'generic',
      'sysbench-passwords',
      f'--from-literal=benchmark-password={_GetPostgresPassword()}',
      '-n',
      namespace,
  ])

  client_node_resources = _QueryNodeResources('clients', user_managed)
  client_resources = ResolveClientResources(client_node_resources)

  template_params: dict[str, Any] = {
      'namespace': namespace,
      'client_image': _CLIENT_IMAGE.value,
      'pin_to_nodepool': _IsPinToNodepool(bm_spec),
      **client_resources,
  }
  kubernetes_commands.ApplyManifest(
      'container/postgres_sysbench/client_pod.yaml.j2',
      **template_params,
  )

  cluster.WaitForResource(  # pyrefly: ignore[missing-attribute]
      f'pod/{_CLIENT_POD_NAME}',
      'Ready',
      namespace=namespace,
      timeout=600,
  )

  _RunClientInstallStep(namespace, 'apt-get update')
  _RunClientInstallStep(
      namespace,
      'DEBIAN_FRONTEND=noninteractive apt-get install -y'
      ' --no-install-recommends sysbench postgresql-client ca-certificates',
  )


def _LoadDatabase(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Loads initial Sysbench tables into AlloyDB Omni."""
  namespace = _GetNamespace()
  postgres_ip = bm_spec.postgres_service_ip  # pyrefly: ignore[missing-attribute]
  load_cmd = (
      'sysbench /usr/share/sysbench/oltp_read_write.lua '
      '--db-driver=pgsql '
      f'--tables={FLAGS.sysbench_tables} '
      f'--table_size={FLAGS.sysbench_table_size} '
      f'--threads={FLAGS.sysbench_load_threads} '
      '--pgsql-user=benchmark '
      '--pgsql-password="$PGPASSWORD" '
      '--pgsql-db=benchmark '
      f'--pgsql-host={postgres_ip} '
      '--pgsql-port=5432 '
      'prepare'
  )
  _ExecInPod(
      _CLIENT_POD_NAME,
      load_cmd,
      namespace,
      timeout=_LOAD_TIMEOUT_SECONDS,
  )
  logging.info('Sysbench tables loaded successfully.')


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Prepares AlloyDB Omni and Sysbench client pods on Kubernetes."""
  cluster = bm_spec.container_cluster
  if getattr(cluster, 'user_managed', False):
    bm_spec.always_call_cleanup = True
    if (
        FLAGS.kubeconfig
        and not os.path.exists(FLAGS.kubeconfig)
        and hasattr(cluster, '_GetKubeconfig')
    ):
      cluster._GetKubeconfig()  # pylint: disable=protected-access
  _EnsureNamespace(_GetNamespace())

  prepare_fns = [
      functools.partial(_PrepareAlloyDBOmniCluster, bm_spec),
      functools.partial(_PrepareSysbenchClient, bm_spec),
  ]
  background_tasks.RunThreaded(lambda fn: fn(), prepare_fns)

  if not FLAGS.sysbench_skip_load_stage:
    _LoadDatabase(bm_spec)


def _QuerySingleSetting(
    postgres_ip: str,
    namespace: str,
    sql_expr: str,
) -> str:
  """Runs a single-value psql query from the client pod."""
  cmd = (
      f'psql -h {postgres_ip} -U benchmark -d benchmark -t -A -c "{sql_expr}"'
  )
  stdout, _, retcode = _ExecInPod(
      _CLIENT_POD_NAME, cmd, namespace, raise_on_failure=False
  )
  if retcode != 0:
    return 'unknown'
  return stdout.strip() or 'unknown'


def _GetAlloyDBOmniLiveMetadata(
    postgres_ip: str,
    namespace: str,
) -> dict[str, str]:
  """Queries live AlloyDB Omni configuration and version metadata."""
  omni_version = 'unknown'
  ver_out, _, ver_rc = kubectl.RunKubectlCommand(
      ['exec', '-n', namespace, _SERVER_POD_NAME, '--', 'cat', '/VERSION.txt'],
      raise_on_failure=False,
  )
  if ver_rc == 0 and ver_out.strip():
    omni_version = ver_out.strip().removeprefix('AlloyDB Omni version:').strip()

  return {
      'alloydb_omni_version': omni_version,
      'postgres_server_version': _QuerySingleSetting(
          postgres_ip, namespace, "SELECT current_setting('server_version');"
      ),
      'postgres_shared_buffers': _QuerySingleSetting(
          postgres_ip, namespace, "SELECT current_setting('shared_buffers');"
      ),
      'postgres_effective_cache_size': _QuerySingleSetting(
          postgres_ip,
          namespace,
          "SELECT current_setting('effective_cache_size');",
      ),
      'google_columnar_engine_enabled': _QuerySingleSetting(
          postgres_ip,
          namespace,
          "SELECT COALESCE(current_setting('google_columnar_engine.enabled',"
          " true), 'unknown');",
      ),
  }


def _WaitForDatabaseIdle(
    postgres_ip: str,
    namespace: str,
    timeout_sec: int = 60,
) -> None:
  """Polls pg_stat_activity until background activity settles."""
  query = (
      "SELECT count(*) FROM pg_stat_activity WHERE (state = 'active' OR"
      " backend_type = 'autovacuum worker') AND pid <> pg_backend_pid();"
  )
  cmd = f'psql -h {postgres_ip} -U benchmark -d benchmark -t -A -c "{query}"'

  @vm_util.Retry(
      timeout=timeout_sec,
      poll_interval=5,
      retryable_exceptions=(_DatabaseBusyError,),
  )
  def _CheckIdle() -> None:
    stdout, _, _ = _ExecInPod(_CLIENT_POD_NAME, cmd, namespace)
    active_count = int(stdout.strip()) if stdout.strip().isdigit() else 0
    if active_count > 0:
      raise _DatabaseBusyError(
          f'Database still has {active_count} active backends'
      )

  try:
    _CheckIdle()
    logging.info('AlloyDB Omni database settled (0 active workers).')
  except (vm_util.RetryError, _DatabaseBusyError):
    logging.info(
        'Database settle timeout (%ds) reached; proceeding to benchmark run.',
        timeout_sec,
    )


def _StabilizeDatabaseBeforeRun(
    postgres_ip: str,
    namespace: str,
) -> None:
  """Runs ANALYZE and CHECKPOINTs before timed Sysbench execution."""
  logging.info(
      'Running ANALYZE on %d Sysbench tables...', FLAGS.sysbench_tables
  )
  for table_idx in range(1, FLAGS.sysbench_tables + 1):
    analyze_cmd = (
        f'psql -h {postgres_ip} -U benchmark -d benchmark -c '
        f'"ANALYZE sbtest{table_idx};"'
    )
    _ExecInPod(
        _CLIENT_POD_NAME,
        analyze_cmd,
        namespace,
        timeout=_STABILIZE_CMD_TIMEOUT_SECONDS,
    )

  checkpoint_cmd = (
      f'psql -h {postgres_ip} -U benchmark -d benchmark -c "CHECKPOINT;"'
  )
  for idx in range(3):
    logging.info('Issuing pre-run CHECKPOINT %d/3', idx + 1)
    _ExecInPod(
        _CLIENT_POD_NAME,
        checkpoint_cmd,
        namespace,
        timeout=_STABILIZE_CMD_TIMEOUT_SECONDS,
    )
    time.sleep(5)

  _WaitForDatabaseIdle(postgres_ip, namespace)


def _GetWorkloadList() -> Sequence[str]:
  """Returns the list of Sysbench workloads configured by flags."""
  return [w.strip() for w in FLAGS.sysbench_testname.split(',') if w.strip()]


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs Sysbench against AlloyDB Omni and collects performance samples."""
  namespace = _GetNamespace()
  postgres_ip = getattr(
      bm_spec, 'postgres_service_ip', None
  ) or _GetServerPodIp(namespace)
  samples: list[sample.Sample] = []

  live_metadata = _GetAlloyDBOmniLiveMetadata(postgres_ip, namespace)
  disk_type = (
      getattr(bm_spec, 'resolved_disk_type', None)
      or FLAGS.data_disk_type
      or 'auto'
  )
  shm_size = getattr(bm_spec, 'resolved_shm_size', None) or (
      _SHM_SIZE.value or 'auto'
  )
  machine_type = (
      getattr(bm_spec, 'resolved_machine_type', None)
      or _GetServerMachineType(bm_spec)
  )
  run_threads = list(FLAGS.sysbench_run_threads or [64])

  for workload in _GetWorkloadList():
    for thread_count in run_threads:
      _StabilizeDatabaseBeforeRun(postgres_ip, namespace)

      lua_script = f'/usr/share/sysbench/{workload}.lua'
      run_cmd = (
          f'sysbench {lua_script} '
          '--db-driver=pgsql '
          f'--tables={FLAGS.sysbench_tables} '
          f'--table_size={FLAGS.sysbench_table_size} '
          f'--threads={thread_count} '
          f'--report-interval={FLAGS.sysbench_report_interval} '
          f'--time={FLAGS.sysbench_run_seconds} '
          '--pgsql-user=benchmark '
          '--pgsql-password="$PGPASSWORD" '
          '--pgsql-db=benchmark '
          f'--pgsql-host={postgres_ip} '
          '--pgsql-port=5432 '
          'run'
      )
      cmd_timeout = (
          FLAGS.sysbench_run_seconds
          + sysbench.SYSBENCH_THREAD_INIT_TIMEOUT.value
          + 120
      )
      stdout, _, _ = _ExecInPod(
          _CLIENT_POD_NAME,
          run_cmd,
          namespace,
          timeout=cmd_timeout,
      )

      sysbench_params = sysbench.SysbenchInputParameters(
          db_driver='pgsql',
          tables=FLAGS.sysbench_tables,
          table_size=FLAGS.sysbench_table_size,
          threads=thread_count,
          report_interval=FLAGS.sysbench_report_interval,
          db_user='benchmark',
          db_password='',
          db_name='benchmark',
          host_ip=postgres_ip,
          port=5432,
          built_in_test=True,
          test=f'{sysbench.LUA_SCRIPT_PATH}{workload}.lua',
      )
      metadata = sysbench.GetMetadata(sysbench_params)
      metadata.update(live_metadata)
      metadata.update({
          'omni_image': _OMNI_IMAGE.value,
          'template_path': _TEMPLATE_PATH.value,
          'namespace': namespace,
          'machine_type': machine_type,
          'disk_type': disk_type,
          'shm_size': shm_size,
          'workload_type': workload,
      })

      if FLAGS.sysbench_report_interval > 0:
        try:
          samples.extend(sysbench.ParseSysbenchTimeSeries(stdout, metadata))
        except (ValueError, IndexError) as exc:
          logging.warning('Failed to parse Sysbench time series: %s', exc)
      samples.extend(sysbench.ParseSysbenchLatency([stdout], metadata))
      samples.extend(sysbench.ParseSysbenchTransactions(stdout, metadata))

  if not samples:
    raise errors.Benchmarks.RunError(
        'No performance samples collected from Sysbench output.'
    )
  logging.info('Collected %d AlloyDB Omni Sysbench samples.', len(samples))
  return samples


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Cleans up Kubernetes resources when running on a static cluster."""
  cluster = getattr(bm_spec, 'container_cluster', None)
  if not cluster or not getattr(cluster, 'user_managed', False):
    return

  namespace = _GetNamespace()
  resources_to_delete = [
      f'pod/{_CLIENT_POD_NAME}',
      f'statefulset/{_SERVER_STS_NAME}',
      'service/postgres-standalone',
      f'pvc/postgres-data-{_SERVER_POD_NAME}',
      'configmap/postgres-config',
      'configmap/postgres-init-scripts',
      'secret/postgres-secret',
      'secret/sysbench-passwords',
  ]
  for resource in resources_to_delete:
    kubectl.RunKubectlCommand(
        ['delete', resource, '-n', namespace, '--ignore-not-found'],
        raise_on_failure=False,
    )
  if getattr(cluster, 'CLOUD', '') in _CLOUD_STORAGE_PROVISIONERS:
    kubectl.RunKubectlCommand(
        [
            'delete',
            f'storageclass/{_GetStorageClassName(namespace)}',
            '--ignore-not-found',
        ],
        raise_on_failure=False,
    )
