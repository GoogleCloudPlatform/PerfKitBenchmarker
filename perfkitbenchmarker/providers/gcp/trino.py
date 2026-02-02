"""Implements a Trino cluster on GKE.

Requies: A container_cluster also initialized by PKB.
"""

import copy
import enum
import json
import logging
import os
from typing import Any
import urllib.parse

from absl import flags
from perfkitbenchmarker import container_service
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.container_service import kubernetes_commands
from perfkitbenchmarker.providers.gcp import util
from perfkitbenchmarker.resources.container_service import kubectl


FLAGS = flags.FLAGS


class HttpScheme(enum.Enum):
  HTTP = 'http'
  HTTPS = 'https'


_TRINO_PYTHON_CLIENT_FILE = 'trino_python_driver.py'
_TRINO_PYTHON_CLIENT_DIR = 'edw/trino/clients/python'
_TRINO_CHART = 'container/trino.yaml.j2'


def _MemoryToString(memory: float) -> str:
  """Returns the memory number as a string."""
  return f'{round(memory)}G'


class TrinoClientInterface(edw_service.EdwClientInterface):
  """Python Client Interface class for Trino."""

  def __init__(
      self,
      catalog: str,
      schema: str,
  ):
    super().__init__()
    self.catalog: str = catalog
    self.schema: str = schema
    self.hostname: str | None = None
    self.port: int | None = None
    self.http_scheme: HttpScheme = HttpScheme.HTTP

  def Prepare(self, package_name: str) -> None:
    """Prepares the client vm to execute query."""
    # Install dependencies for driver
    self.client_vm.Install('pip')
    self.client_vm.RemoteCommand(
        'sudo apt-get -qq update && DEBIAN_FRONTEND=noninteractive sudo apt-get'
        ' -qq install python3.12-venv'
    )
    self.client_vm.RemoteCommand('python3 -m venv .venv')
    self.client_vm.RemoteCommand(
        'source .venv/bin/activate && pip install trino absl-py'
    )

    # Push driver script to client vm
    self.client_vm.PushDataFile(
        os.path.join(_TRINO_PYTHON_CLIENT_DIR, _TRINO_PYTHON_CLIENT_FILE)
    )
    self.client_vm.PushDataFile(
        os.path.join(
            edw_service.EDW_PYTHON_DRIVER_LIB_DIR,
            edw_service.EDW_PYTHON_DRIVER_LIB_FILE,
        )
    )

  def _RunClientCommand(self, command: str, additional_args: list[str]) -> str:
    """Runs a command on the python client."""
    cmd_parts = [
        f'.venv/bin/python {_TRINO_PYTHON_CLIENT_FILE}',
        command,
        f'--hostname {self.hostname}',
        f'--port {self.port}',
        f'--catalog {self.catalog}',
        f'--schema {self.schema}',
        f'--http_scheme {self.http_scheme.value}',
    ]
    cmd_parts.extend(additional_args)
    cmd = ' '.join(cmd_parts)
    stdout, _ = self.client_vm.RobustRemoteCommand(cmd)
    return stdout

  def ExecuteQuery(
      self, query_name: str, print_results: bool = False
  ) -> tuple[float, dict[str, Any]]:
    """Executes a query and returns performance details."""
    args = [f'--query_file {query_name}']
    if print_results:
      args.append('--print_results')
    stdout = self._RunClientCommand('single', args)
    results = json.loads(stdout)
    details = copy.copy(self.GetMetadata())
    details.update(results['details'])
    return results['query_wall_time_in_secs'], details

  def ExecuteThroughput(
      self,
      concurrency_streams: list[list[str]],
      labels: dict[str, str] | None = None,
  ) -> str:
    """Executes queries simultaneously on client and return performance details."""
    del labels  # Currently not supported by trino python api
    args = [f"--query_streams='{json.dumps(concurrency_streams)}'"]
    return self._RunClientCommand('throughput', args)

  def GetMetadata(self) -> dict[str, str]:
    return {
        'client': 'python',
        'trino_catalog': self.catalog,
        'trino_schema': self.schema,
    }


# TODO(howellz): Move this from GCP -> resources/ as it is not GCP specific.
class Trino(edw_service.EdwService):
  """Object representing a Trino cluster."""

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'trino'
  QUERY_SET = 'trino'

  def __init__(self, edw_service_spec):
    """Initialize the Trino object."""
    super().__init__(edw_service_spec)
    self.name = f'pkb-{FLAGS.run_uri}'
    self.address: str = ''
    self.project: str = FLAGS.project
    self.memory: int = edw_service.TRINO_MEMORY.value

    self.client_interface: TrinoClientInterface = TrinoClientInterface(
        catalog=edw_service.TRINO_CATALOG.value,
        schema=edw_service.TRINO_SCHEMA.value,
    )

  def IsUserManaged(self, edw_service_spec):
    """Indicates if the edw service instance is user managed.

    Args:
      edw_service_spec: spec of the edw service.

    Returns:
      A boolean, set to True if the edw service instance is user managed, False
       otherwise.
    """
    # TODO(howellz): Set up static implementations and/or set the
    # cluster_identifier value to None with config_overrides.
    return False

  def _WriteConfigYaml(self) -> str:
    """Writes the config yaml for the Trino cluster."""
    contents = vm_util.ReadAndRenderJinja2Template(
        _TRINO_CHART,
        **{
            'num_workers': self.node_count,
            'memory': self.memory,
            'hive_uri': self.endpoint,
            'project': self.project,
        },
    )
    yaml_dicts = vm_util.ReadYamlAsDicts(contents)
    yaml_dict: dict[str, Any] = yaml_dicts[0]
    # From Trino sizing guidelines:
    # https://vjain143.github.io/Trino_Memory_Sizing_Guidelines.html
    jvm_heap_size_num: float = self.memory * 0.75
    query_size_per_node_num: float = 0.3 * jvm_heap_size_num
    jvm_heap_size_str: str = _MemoryToString(jvm_heap_size_num)
    yaml_dict['server']['config'] = {
        'query': {
            'maxMemory': (
                f'{_MemoryToString(query_size_per_node_num * self.node_count)}B'
            ),
        }
    }
    yaml_dict['coordinator']['jvm'] = {
        'maxHeapSize': jvm_heap_size_str,
        'additionalJVMConfigs': f'-Xms{jvm_heap_size_str}',
    }
    yaml_dict['worker'] = {
        'config': {
            'query': {
                'maxMemoryPerNode': (
                    f'{_MemoryToString(query_size_per_node_num)}B'
                ),
            }
        },
        'jvm': {
            'maxHeapSize': jvm_heap_size_str,
        },
        'resources': {
            'requests': {
                'memory': f'{_MemoryToString(self.memory)}i',
                'ephemeral-storage': f'{_MemoryToString(self.memory)}i',
            },
        },
    }
    return vm_util.WriteYaml([yaml_dict], should_log_file=True)

  def _Create(self):
    """Creates the Trino cluster on GKE.

    Side effects:
      Sets the client interface hostname and port to the deployed ingress.
    """
    service_account = util.GetDefaultComputeServiceAccount(self.project)
    cmd = [
        'annotate',
        'serviceaccount',
        'default',
        '--namespace',
        'default',
        f'iam.gke.io/gcp-service-account={service_account}',
    ]
    kubectl.RunKubectlCommand(cmd)
    cmd = [
        'helm',
        'repo',
        'add',
        'trino',
        'https://trinodb.github.io/charts',
    ]
    vm_util.IssueCommand(cmd)
    filename = self._WriteConfigYaml()
    cmd = [
        'helm',
        'install',
        '-f',
        filename,
        self.name,
        'trino/trino',
        '--kubeconfig',
        FLAGS.kubeconfig,
    ]
    vm_util.IssueCommand(cmd)
    self._DeployIngress()

  def _DeployIngress(self):
    """Deploy the ingress for the Trino service & saves the address.

    Side effects:
      Sets the client interface hostname and port to the deployed ingress.
    """
    port, _, _ = container_service.RunKubectlCommand(
        [
            'get',
            'service',
            f'{self.name}-trino',
            '--output',
            'jsonpath={.spec.ports[0].port}',
        ],
    )
    port = port.strip()
    assert self.container_cluster is not None
    address = self.container_cluster.DeployIngress(
        name='trino-ingress',
        namespace='default',
        port=int(port),
        node_selectors={
            'app.kubernetes.io/name': 'trino',
            'app.kubernetes.io/component': 'coordinator',
        },
    )
    logging.info('Trino port exposed at: %s', address)
    # Parse full address (eg http://12.0.0.0:8080) into pieces.
    self.address = address
    parsed_address = urllib.parse.urlparse(address)
    assert parsed_address.scheme, f'Invalid address had no scheme: {address}'
    self.client_interface.http_scheme = HttpScheme(parsed_address.scheme)
    self.client_interface.hostname = parsed_address.hostname
    self.client_interface.port = parsed_address.port

  def _Exists(self):
    """Checks if Trino exists (or at least if its pods do)."""
    try:
      kubernetes_commands.WaitForRollout(
          f'deployment.apps/{self.name}-trino-worker',
          timeout=60 * 4 + 20 * self.node_count,
      )
      return True
    except errors.VmUtil.IssueCommandError as e:
      if 'not found' in str(e):
        return False
      raise

  def _Delete(self):
    """Deleting the cluster."""
    cmd = [
        'helm',
        'uninstall',
        self.name,
        '--kubeconfig',
        FLAGS.kubeconfig,
    ]
    vm_util.IssueCommand(cmd)

  def GetMetadata(self):
    """Returns the metadata for the Trino service."""
    metadata = super().GetMetadata()
    metadata.update(self.client_interface.GetMetadata())
    return metadata

  def ExtractDataset(
      self, dest_bucket, dataset=None, tables=None, dest_format='CSV'
  ):
    """Extract all tables in a dataset to object storage.

    Args:
      dest_bucket: Name of the bucket to extract the data to. Should already
        exist.
      dataset: Optional name of the dataset. If none, will be determined by the
        service.
      tables: Optional list of table names to extract. If none, all tables in
        the dataset will be extracted.
      dest_format: Format to extract data in.
    """
    # TODO(howellz): Implement all dataset operations.
    pass

  def RemoveDataset(self, dataset=None):
    """Removes a dataset.

    Args:
      dataset: Optional name of the dataset. If none, will be determined by the
        service.
    """
    pass

  def CreateDataset(self, dataset=None, description=None):
    """Creates a new dataset.

    Args:
      dataset: Optional name of the dataset. If none, will be determined by the
        service.
      description: Optional description of the dataset.
    """
    pass

  def LoadDataset(self, source_bucket, tables, dataset=None):
    """Load all tables in a dataset to a database from object storage.

    Args:
      source_bucket: Name of the bucket to load the data from. Should already
        exist. Each table must have its own subfolder in the bucket named after
        the table, containing one or more csv files that make up the table data.
      tables: list of table names to load.
      dataset: Optional name of the dataset. If none, will be determined by the
        service.
    """
    pass

  def OpenDataset(self, dataset: str):
    """Switch from the currently active dataset to the one specified.

    Switches the dataset that will be accessed by queries sent through the
    client interface that this EDW service provides.

    Args:
      dataset: Name of the dataset to make the active dataset.
    """
    pass

  def CopyTable(self, copy_table_name: str, to_dataset: str) -> None:
    """Copy a table from the active dataset to the specified dataset.

    Copies a table between datasets, from the active (current) dataset to
    another named dataset in the same project.

    Args:
      copy_table_name: Name of the table to copy from the loaded dataset to the
        copy dataset.
      to_dataset: Name of the dataset to copy the table into.
    """
    pass
