"""Implements a Trino cluster on GKE.

Requies: A container_cluster also initialized by PKB.
"""

import logging
from typing import Any

from absl import flags
from perfkitbenchmarker import container_service
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.container_service import kubectl
from perfkitbenchmarker.providers.gcp import util


FLAGS = flags.FLAGS
_TRINO_CHART = 'container/trino.yaml.j2'


class Trino(edw_service.EdwService):
  """Object representing a Trino cluster."""

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'trino'

  def __init__(self, edw_service_spec):
    """Initialize the Trino object."""
    super().__init__(edw_service_spec)
    self.name = f'pkb-{FLAGS.run_uri}'
    self.address: str = ''
    self.project: str = FLAGS.project

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
            'hive_uri': self.endpoint,
            'project': self.project,
        },
    )
    yaml_dicts = vm_util.ReadYamlAsDicts(contents)
    yaml_dict: dict[str, Any] = yaml_dicts[0]
    # TODO(howellz): Add support for memory.
    return vm_util.WriteYaml([yaml_dict], should_log_file=True)

  def _Create(self):
    """Resuming the cluster."""
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
    self.address = self._DeployIngress()

  def _DeployIngress(self) -> str:
    """Deploy the ingress for the Trino service & returns the address."""
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
    return address

  def _Exists(self):
    """Checks if Trino exists (or at least if its pods do)."""
    assert self.container_cluster
    self.container_cluster.WaitForRollout(
        f'deployment.apps/{self.name}-trino-worker',
        timeout=60 * 5,
    )
    return True

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
      tables: List of table names to load.
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
