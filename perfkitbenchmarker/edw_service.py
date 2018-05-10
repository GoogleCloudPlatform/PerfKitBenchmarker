# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Resource encapsulating provisioned Data Warehouse in the cloud Services.

Classes to wrap specific backend services are in the corresponding provider
directory as a subclass of BaseEdwService.
"""

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource


flags.DEFINE_integer('edw_service_cluster_concurrency', 5,
                     'Number of queries to run concurrently on the cluster.')
flags.DEFINE_string('edw_service_cluster_snapshot', None,
                    'If set, the snapshot to restore as cluster.')
flags.DEFINE_string('edw_service_cluster_identifier', None,
                    'If set, the preprovisioned edw cluster.')
flags.DEFINE_string('edw_service_endpoint', None,
                    'If set, the preprovisioned edw cluster endpoint.')
flags.DEFINE_string('edw_service_cluster_db', None,
                    'If set, the db on cluster to use during the benchmark ('
                    'only applicable when using snapshots).')
flags.DEFINE_string('edw_service_cluster_user', None,
                    'If set, the user authorized on cluster (only applicable '
                    'when using snapshots).')
flags.DEFINE_string('edw_service_cluster_password', None,
                    'If set, the password authorized on cluster (only '
                    'applicable when using snapshots).')
flags.DEFINE_enum('edw_query_execution_mode', 'sequential', ['sequential',
                                                             'concurrent'],
                  'The mode for executing the queries on the edw cluster.')

FLAGS = flags.FLAGS


TYPE_2_PROVIDER = dict([('redshift', 'aws'), ('bigquery', 'gcp')])
TYPE_2_MODULE = dict([('redshift',
                       'perfkitbenchmarker.providers.aws.redshift'),
                      ('bigquery',
                       'perfkitbenchmarker.providers.gcp.bigquery')])
DEFAULT_NUMBER_OF_NODES = 2


class EdwService(resource.BaseResource):
  """Object representing a EDW Service."""

  def __init__(self, edw_service_spec):
    """Initialize the edw service object.

    Args:
      edw_service_spec: spec of the edw service.
    """
    # Hand over the actual creation to the resource module, which assumes the
    # resource is pkb managed by default
    # edw_service attribute
    if edw_service_spec.cluster_identifier:
      super(EdwService, self).__init__(user_managed=True)
      self.cluster_identifier = edw_service_spec.cluster_identifier
    else:
      super(EdwService, self).__init__(user_managed=False)
      self.cluster_identifier = 'pkb-' + FLAGS.run_uri

    # Provision related attributes
    if edw_service_spec.snapshot:
      self.snapshot = edw_service_spec.snapshot
    else:
      self.snapshot = None

    # Cluster related attributes
    self.concurrency = edw_service_spec.concurrency
    self.node_type = edw_service_spec.node_type
    self.node_count = edw_service_spec.node_count

    # Interaction related attributes
    if edw_service_spec.endpoint:
      self.endpoint = edw_service_spec.endpoint
    else:
      self.endpoint = ''
    self.db = edw_service_spec.db
    self.user = edw_service_spec.user
    self.password = edw_service_spec.password
    # resource config attribute
    self.spec = edw_service_spec
    # resource workflow management
    self.supports_wait_on_delete = True

  def GetMetadata(self):
    """Return a dictionary of the metadata for this edw service."""
    basic_data = {'edw_service_type': self.spec.type,
                  'edw_cluster_identifier': self.cluster_identifier,
                  'edw_cluster_node_type': self.node_type,
                  'edw_cluster_node_count': self.node_count}
    return basic_data

  def RunCommandHelper(self):
    """Returns EDW instance specific launch command components.

    Returns:
      A string with additional command components needed when invoking script
      runner.
    """
    raise NotImplementedError
