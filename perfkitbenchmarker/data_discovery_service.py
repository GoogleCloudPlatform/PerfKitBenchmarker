"""Benchmarking support for Data Discovery Services for data lakes.

In this module, the base class for Data Discovery Services is defined, which
outlines the common interface that must be implemented for each concrete Data
Discovery Service PKB supports (e.g. AWS Glue Crawler).
"""

import abc
from typing import Any, Dict

from absl import flags
from perfkitbenchmarker import resource

_DATA_DISCOVERY_OBJECT_STORE_PATH = flags.DEFINE_string(
    'data_discovery_object_store_path', None,
    'Object store path which will be analyzed by the Data Discovery Service. '
    'Must be a fully qualified object store URL (e.g. s3://bucket/dir for S3, '
    'or gs://bucket/dir for GCS).'
)
_DATA_DISCOVERY_REGION = flags.DEFINE_string(
    'data_discovery_region', None,
    'Region on which the data discovery service will be deployed.'
)

# Available service types
GLUE = 'glue'


def GetDataDiscoveryServiceClass(cloud, service_type):
  """Gets the underlying Data Discovery Service class."""
  return resource.GetResourceClass(
      BaseDataDiscoveryService, CLOUD=cloud, SERVICE_TYPE=service_type)


class BaseDataDiscoveryService(resource.BaseResource):
  """Common interface of a data discovery service resource.

  Attributes:
    region: The region the service was set up to use.
    data_discovery_path: The object store path containing the files to be
      discovered.
  """

  REQUIRED_ATTRS = ['CLOUD', 'SERVICE_TYPE']
  RESOURCE_TYPE = 'BaseDataDiscoveryService'
  CLOUD = 'abstract'
  SERVICE_TYPE = 'abstract'

  def __init__(self):
    super().__init__()
    self.region = _DATA_DISCOVERY_REGION.value
    self.data_discovery_path = _DATA_DISCOVERY_OBJECT_STORE_PATH.value

  @classmethod
  def FromSpec(cls, data_discovery_service_spec):
    return cls()

  @abc.abstractmethod
  def DiscoverData(self) -> float:
    """Runs data discovery. Returns the time elapsed in secs."""
    raise NotImplementedError('Must implement in subclasses.')

  def GetMetadata(self) -> Dict[str, Any]:
    """Return a dictionary of the metadata for this service."""
    return {'cloud': self.CLOUD, 'data_discovery_region': self.region}
