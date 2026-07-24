"""Implements a ClickHouse cluster on GKE.

Requires: A container_cluster also initialized by PKB.
"""

from typing import Any

from absl import flags
from perfkitbenchmarker import edw_service
from perfkitbenchmarker import provider_info

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


class Clickhouse(edw_service.EdwService):
  """Object representing a ClickHouse cluster on GKE."""

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'clickhouse'
  QUERY_SET = 'clickhouse'

  def __init__(self, edw_service_spec):
    """Initialize the ClickHouse object."""
    super().__init__(edw_service_spec)
    self.name: str = f'pkb-{FLAGS.run_uri}'
    self.client_interface: ClickhouseClientInterface = (
        ClickhouseClientInterface()
    )

  def IsUserManaged(self, edw_service_spec):
    """Indicates if the edw service instance is user managed."""
    return False

  def _Create(self):
    """Creates the ClickHouse cluster on GKE."""

  def _Delete(self):
    """Deletes the cluster."""

  def GetMetadata(self):
    """Returns the metadata for the ClickHouse service."""
    metadata = super().GetMetadata()
    metadata.update(self.client_interface.GetMetadata())
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
