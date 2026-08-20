"""Implements a ClickHouse cluster on GKE.

Requires: A container_cluster also initialized by PKB.
"""

import copy
import hashlib
import json
import logging
import os
from typing import Any
import uuid

from absl import flags
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS

_BYTES_PER_GB = 1024 * 1024 * 1024


class ClickhouseClientInterface(edw_service.EdwClientInterface):
  """Python Client Interface class for ClickHouse."""

  def __init__(
      self,
      address: str = '127.0.0.1',
      port: int = 9000,
      http_port: int = 8123,
      user: str = 'external',
      password: str = '',
  ):
    super().__init__()
    self.address = address
    self.port = port
    self.http_port = http_port
    self.user = user
    self.password = password

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query."""
    assert self.client_vm
    self.client_vm.RemoteCommand('curl https://clickhouse.com/ | sh')
    self.client_vm.RemoteCommand('sudo ./clickhouse install')
    # Give access to non-root users.
    self.client_vm.RemoteCommand('chmod a+rx ~')
    self.client_vm.RemoteCommand('sudo mkdir -p /var/lib/clickhouse/user_files')
    self.client_vm.RemoteCommand(
        'sudo chmod 755 /var/lib/clickhouse /var/lib/clickhouse/user_files'
    )

    # Install dependencies for python driver
    self.client_vm.Install('pip')
    self.client_vm.RemoteCommand(
        'sudo apt-get -qq update && DEBIAN_FRONTEND=noninteractive sudo apt-get'
        ' -qq install python3.12-venv'
    )
    self.client_vm.RemoteCommand('python3 -m venv .venv')
    self.client_vm.RemoteCommand(
        'source .venv/bin/activate && pip install clickhouse-connect absl-py'
    )

    # Push driver script and common library to client vm
    self.client_vm.PushDataFile(
        os.path.join(
            _CLICKHOUSE_PYTHON_CLIENT_DIR, _CLICKHOUSE_PYTHON_CLIENT_FILE
        )
    )
    self.client_vm.PushDataFile(
        os.path.join(
            edw_service.EDW_PYTHON_DRIVER_LIB_DIR,
            edw_service.EDW_PYTHON_DRIVER_LIB_FILE,
        )
    )

  def _RunClientCommand(
      self,
      base: str,
      additional_args: list[str] | None = None,
      port: int | None = None,
  ) -> tuple[str, str]:
    """Runs a command adding common arguments."""
    if port is None:
      port = self.port
    cmd = [
        base,
        f'--host={self.address}',
        f'--port={port}',
        f'--user={self.user}',
        f'--password={self.password}',
    ]
    if additional_args:
      cmd.extend(additional_args)
    assert self.client_vm
    return self.client_vm.RemoteCommand(' '.join(cmd))

  def _RunPythonClientCommand(
      self, command: str, additional_args: list[str]
  ) -> str:
    """Runs a command on the clickhouse python client."""
    stdout = self._RunClientCommand(
        f'.venv/bin/python {_CLICKHOUSE_PYTHON_CLIENT_FILE} {command}',
        port=self.http_port,
        additional_args=additional_args,
    )[0]
    return stdout

  def ExecuteViaClickhouseClient(self, statement: str) -> str:
    """Executes a SQL statement via clickhouse-client and returns stdout."""
    stdout = self._RunClientCommand(
        'clickhouse-client',
        port=self.port,
        additional_args=[f'--query="{statement}"'],
    )[0]
    return stdout

  def ExecuteQuery(
      self, query_name: str, print_results: bool = False
  ) -> tuple[float, dict[str, Any]]:
    """Executes a query file and returns performance details."""
    args = [f'--query_file={query_name}']
    if print_results:
      args.append('--print_results')
    stdout = self._RunPythonClientCommand('single', args)
    json_output = json.loads(stdout)
    details = copy.copy(self.GetMetadata())
    details.update(json_output['details'])
    return json_output['query_wall_time_in_secs'], details

  def ExecuteThroughput(
      self,
      concurrency_streams: list[list[str]],
      labels: dict[str, str] | None = None,
  ) -> str:
    """Executes queries simultaneously on client and return performance details."""
    del labels  # Currently not supported by clickhouse python api
    args = [f"--query_streams='{json.dumps(concurrency_streams)}'"]
    return self._RunPythonClientCommand('throughput', args)

  def GetMetadata(self) -> dict[str, str]:
    return {
        'client': 'python',
    }

  def GetTableStats(self, table_name: str) -> tuple[float, int]:
    """Gets the size in gigabytes and row count of the table."""
    query = (
        'SELECT total_bytes, total_rows'
        f" FROM system.tables WHERE table = '{table_name}'"
    )
    stdout = self.ExecuteViaClickhouseClient(query)
    if not stdout or not stdout.strip():
      raise ValueError(f'Table stats for {table_name} were not returned.')
    parts = stdout.strip().split()
    return int(parts[0]) / _BYTES_PER_GB, int(parts[1])


_NAMESPACE = 'clickhouse'
_CLICKHOUSE_PYTHON_CLIENT_FILE = 'ch_python_driver.py'
_CLICKHOUSE_PYTHON_CLIENT_DIR = 'edw/clickhouse/clients/python'
_KEEPER_CHART = 'container/clickhouse/keeper-cluster.yaml.j2'
_CLICKHOUSE_CHART = 'container/clickhouse/clickhouse-cluster.yaml.j2'
_LOADBALANCER_CHART = 'container/loadbalancer.yaml.j2'


class Clickhouse(edw_service.EdwService):
  """Object representing a ClickHouse cluster on GKE."""

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'clickhouse'
  QUERY_SET = 'clickhouse'

  def __init__(self, edw_service_spec):
    """Initialize the ClickHouse object."""
    super().__init__(edw_service_spec)
    self.name: str = f'pkb-{FLAGS.run_uri}'
    self.project: str = FLAGS.project
    # Following advice from:
    # https://clickhouse.com/docs/guides/sizing-and-hardware-recommendations
    # default cpu & replicas off memory & shards.
    self.memory: int = edw_service.CLICKHOUSE_MEMORY.value
    self.cpu: float = (edw_service.CLICKHOUSE_CPU.value or self.memory / 4.0)
    self.num_shards: int = edw_service.CLICKHOUSE_NUM_SHARDS.value
    self.num_replicas: int = (
        edw_service.CLICKHOUSE_NUM_REPLICAS.value or 3 * self.num_shards
    )
    self.address = '127.0.0.1'
    self.user = 'external'
    self.password = str(uuid.uuid4())[-8:]
    self.port: int = 9000
    self.http_port: int = 8123
    self.client_interface: ClickhouseClientInterface = (
        ClickhouseClientInterface(
            address=self.address,
            port=self.port,
            http_port=self.http_port,
            user=self.user,
            password=self.password,
        )
    )

  def IsUserManaged(self, edw_service_spec) -> bool:
    """Indicates if the edw service instance is user managed."""
    return False

  def _Create(self) -> None:
    """Creates the ClickHouse cluster on GKE following operator workflow."""
    self._InstallClickhouse()
    self._WaitForDeployment()
    self._CreateLoadBalancer()

  def _InstallClickhouse(self) -> None:
    """Installs ClickHouse and operator dependencies."""
    kubectl.RunKubectlCommand(['create', 'namespace', _NAMESPACE, '-o', 'yaml'])
    service_account = util.GetDefaultComputeServiceAccount(self.project)
    cmd = [
        'annotate',
        'serviceaccount',
        'default',
        '--namespace',
        _NAMESPACE,
        f'iam.gke.io/gcp-service-account={service_account}',
        '--overwrite',
    ]
    kubectl.RunKubectlCommand(cmd)

    # 1. Install cert-manager
    cmd = [
        'helm',
        'upgrade',
        '--install',
        'cert-manager',
        'oci://quay.io/jetstack/charts/cert-manager',
        '--create-namespace',
        '--namespace',
        'cert-manager',
        '--set',
        'crds.enabled=true',
        '--version',
        'v1.19.2',
        '--kubeconfig',
        FLAGS.kubeconfig,
    ]
    vm_util.IssueCommand(cmd)
    kubernetes_commands.WaitForResource(
        'deploy/cert-manager-webhook',
        condition_name='Available',
        namespace='cert-manager',
        timeout=300,
    )

    # 2. Install clickhouse-operator into clickhouse namespace
    vm_util.IssueCommand([
        'helm',
        'repo',
        'add',
        'altinity',
        'https://helm.altinity.com',
    ])
    vm_util.IssueCommand(['helm', 'repo', 'update'])
    cmd = [
        'helm',
        'upgrade',
        '--install',
        'clickhouse-operator',
        'altinity/altinity-clickhouse-operator',
        '--create-namespace',
        '--namespace',
        _NAMESPACE,
        '--set',
        'configs.files.config\\.yaml.reconcile.coordination.keeper.onKeeperResourceUpdate=reconcile',
        '--kubeconfig',
        FLAGS.kubeconfig,
    ]
    vm_util.IssueCommand(cmd)
    kubernetes_commands.WaitForResource(
        'deploy/clickhouse-operator-altinity-clickhouse-operator',
        condition_name='Available',
        namespace=_NAMESPACE,
        timeout=300,
    )

    # 3. Deploy ClickHouse Keeper
    kubernetes_commands.ApplyManifest(_KEEPER_CHART)

    # 4. Deploy the actual ClickHouse cluster
    password_sha256_hex = hashlib.sha256(
        self.password.encode('utf-8')
    ).hexdigest()
    kubernetes_commands.ApplyManifest(
        _CLICKHOUSE_CHART,
        cluster_name=self.name,
        num_shards=self.num_shards,
        num_replicas=self.num_replicas,
        memory=self.memory,
        user=self.user,
        password_sha256_hex=password_sha256_hex,
    )

  def _GetClickhouseReplicaPrefix(self) -> str:
    """Returns the resource prefix for ClickHouse server replicas created by the operator."""
    return f'chi-{self.name}-{self.name}'

  @vm_util.Retry(retryable_exceptions=(errors.Resource.RetryableCreationError,))
  def _WaitForDeployment(self) -> None:
    """Waits for ClickHouse pods to be ready."""
    try:
      kubernetes_commands.WaitForRollout(
          f'statefulset.apps/{self._GetClickhouseReplicaPrefix()}-0-0',
          namespace=_NAMESPACE,
          timeout=60 * 4 + 20 * self.node_count,
      )
      kubernetes_commands.WaitForResource(
          'pods/chk-keeper-cluster-keeper-cluster-0-0-0',
          namespace=_NAMESPACE,
          condition_name='Ready',
          timeout=60 * 4 + 20 * self.node_count,
      )
    except errors.VmUtil.IssueCommandError as e:
      if 'not found' in str(e):
        raise errors.Resource.RetryableCreationError(
            f'ClickHouse resource not found yet: {e}'
        ) from e
      raise

  def _IsReady(self) -> bool:
    """Checks if ClickHouse is ready by querying the HTTP endpoint."""
    # Check via curl / HTTP because the client_vm isn't set yet.
    url = (
        f'http://{self.address}:{self.http_port}/?'
        f'user={self.user}&password={self.password}&query=SELECT+version()'
    )
    cmd = ['curl', url]
    stdout, _, retcode = vm_util.IssueCommand(cmd, raise_on_failure=False)
    if retcode:
      return False
    return '.' in str(stdout)

  def _CreateLoadBalancer(self) -> None:
    """Creates a Kubernetes LoadBalancer service for ClickHouse."""
    manifest_dicts = kubernetes_commands.ConvertManifestToYamlDicts(
        _LOADBALANCER_CHART,
        name=self.name + '-lb',
        namespace=_NAMESPACE,
        port=self.port,
    )
    manifest_dicts[0]['spec']['selector'] = {
        'clickhouse.altinity.com/app': 'chop',
        'clickhouse.altinity.com/chi': self.name,
        'clickhouse.altinity.com/ready': 'yes',
    }
    manifest_dicts[0]['spec']['ports'].append({
        'name': 'http-port',
        'protocol': 'TCP',
        'port': self.http_port,
        'targetPort': self.http_port,
    })
    kubernetes_commands.ApplyYaml(manifest_dicts)
    self.address = self._GetLoadBalancerIP()
    self.client_interface.address = self.address
    logging.info(
        'Clickhouse running & accessible at %s:%d', self.address, self.port
    )

  @vm_util.Retry(retryable_exceptions=(errors.Resource.RetryableCreationError,))
  def _GetLoadBalancerIP(self) -> str:
    """Returns the IP address of a LoadBalancer service when ready."""
    get_cmd = [
        'get',
        'service',
        f'{self.name}-lb',
        '-n',
        _NAMESPACE,
        '-o',
        'jsonpath={.status.loadBalancer.ingress[0].ip}',
    ]
    out, err, _ = kubectl.RunKubectlCommand(get_cmd, raise_on_failure=False)
    if ('pending' in out or 'pending' in err) or '.' not in out:
      raise errors.Resource.RetryableCreationError(
          'Load Balancer IP for service %s is not ready.' % self.name
      )
    return out

  def _Delete(self) -> None:
    """Deletes the cluster and associated operator components."""
    kubectl.RunKubectlCommand([
        'delete',
        'clickhouseinstallation',
        self.name,
        '-n',
        _NAMESPACE,
        '--ignore-not-found',
    ])
    kubectl.RunKubectlCommand([
        'delete',
        'clickhousekeeperinstallation',
        'keeper-cluster',
        '-n',
        _NAMESPACE,
        '--ignore-not-found',
    ])
    vm_util.IssueCommand(
        [
            'helm',
            'uninstall',
            'clickhouse-operator',
            '--namespace',
            _NAMESPACE,
            '--kubeconfig',
            FLAGS.kubeconfig,
        ],
        raise_on_failure=False,
    )
    vm_util.IssueCommand(
        [
            'helm',
            'uninstall',
            'cert-manager',
            '--namespace',
            'cert-manager',
            '--kubeconfig',
            FLAGS.kubeconfig,
        ],
        raise_on_failure=False,
    )

  def GetMetadata(self) -> dict[str, Any]:
    """Returns the metadata for the ClickHouse service."""
    metadata = super().GetMetadata()
    metadata.update(self.client_interface.GetMetadata())
    metadata.update({
        'clickhouse_memory': self.memory,
        'clickhouse_cpu': self.cpu,
        'clickhouse_num_shards': self.num_shards,
        'clickhouse_num_replicas': self.num_replicas,
    })
    return metadata

  def ExtractDataset(
      self, dest_bucket, dataset=None, tables=None, dest_format='CSV'
  ):
    """Extract all tables in a dataset to object storage."""
    pass

  def RemoveDataset(self, dataset=None):
    """Removes a dataset."""
    pass

  def CreateDataset(self, dataset=None, description=None):
    """Creates a new dataset."""
    pass

  def LoadDataset(self, source_bucket, tables, dataset=None):
    """Load all tables in a dataset to a database from object storage."""
    pass

  def OpenDataset(self, dataset: str):
    """Switch from the currently active dataset to the one specified."""
    pass

  def CopyTable(self, copy_table_name: str, to_dataset: str) -> None:
    """Copy a table from the active dataset to the specified dataset."""
    pass
