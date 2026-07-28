"""Implements a ClickHouse cluster on GKE.

Requires: A container_cluster also initialized by PKB.
"""

import logging
from typing import Any

from absl import flags
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS


class ClickhouseClientInterface(edw_service.EdwClientInterface):
  """Python Client Interface class for ClickHouse."""

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query."""
    del package_name

  def _RunClientCommand(self, command: str, additional_args: list[str]) -> str:
    del command
    del additional_args
    return ''

  def ExecuteQuery(
      self, query_name: str, print_results: bool = False
  ) -> tuple[float, dict[str, Any]]:
    """Executes a query and returns performance details."""
    del query_name
    del print_results
    return 0, {}

  def ExecuteThroughput(
      self,
      concurrency_streams: list[list[str]],
      labels: dict[str, str] | None = None,
  ) -> str:
    """Executes queries simultaneously on client and return performance details."""
    del concurrency_streams
    del labels
    return ''

  def GetMetadata(self) -> dict[str, str]:
    return {
        'client': 'python',
    }


_NAMESPACE = 'clickhouse'
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
    self.address: str = ''
    self.port: int = 9000
    self.address: str = ''
    self.project: str = FLAGS.project
    # Following advice from:
    # https://clickhouse.com/docs/guides/sizing-and-hardware-recommendations
    # default cpu & replicas off memory & shards.
    self.memory: int = edw_service.CLICKHOUSE_MEMORY.value
    self.cpu: float = self.memory / 4.0
    self.num_shards: int = edw_service.CLICKHOUSE_NUM_SHARDS.value
    self.num_replicas: int = (
        edw_service.CLICKHOUSE_NUM_REPLICAS.value or 3 * self.num_shards
    )
    self.client_interface: ClickhouseClientInterface = (
        ClickhouseClientInterface()
    )

  def IsUserManaged(self, edw_service_spec) -> bool:
    """Indicates if the edw service instance is user managed."""
    return False

  def _Create(self) -> None:
    """Creates the ClickHouse cluster on GKE following operator workflow."""
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
    kubernetes_commands.ApplyManifest(
        _CLICKHOUSE_CHART,
        cluster_name=self.name,
        num_shards=self.num_shards,
        num_replicas=self.num_replicas,
        memory=self.memory,
    )

  def _GetClickhouseReplicaPrefix(self) -> str:
    """Returns the resource prefix for ClickHouse server replicas created by the operator."""
    return f'chi-{self.name}-{self.name}'

  def _IsReady(self) -> bool:
    """Checks if ClickHouse pods are ready."""
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
      return True
    except errors.VmUtil.IssueCommandError as e:
      if 'not found' not in str(e):
        logging.exception(
            'ClickHouse is not ready and gave unexpected error: %s', e
        )
      return False

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
    }
    kubernetes_commands.ApplyYaml(manifest_dicts)
    self.address = self._GetLoadBalancerIP()
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

  def _PostCreate(self) -> None:
    """Gets the ClickHouse service port and deploys a load balancer."""
    super()._PostCreate()
    self._CreateLoadBalancer()

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
