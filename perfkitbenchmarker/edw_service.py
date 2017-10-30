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

"""Resource encapsulating provisioned Data Warehouse in the cloud Services

Classes to wrap specific backend services are in the corresponding provider
directory as a subclass of BaseEdwService.
"""

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource


flags.DEFINE_string('edw_service_cluster_snapshot', None,
                    'If set, the snapshot to restore as cluster.')
flags.DEFINE_string('edw_service_cluster_user', None,
                    'If set, the user account authorized on the cluster.')
flags.DEFINE_string('edw_service_cluster_password', None,
                    'If set, the password of account authorized on cluster.')


FLAGS = flags.FLAGS

TYPE_2_PROVIDER = dict([('redshift', 'aws')])
TYPE_2_MODULE = dict([('redshift',
                       'perfkitbenchmarker.providers.aws.redshift')])
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
    super(EdwService, self).__init__()
    self.spec = edw_service_spec

  def GetMetadata(self):
    """Return a dictionary of the metadata for this edw service."""
    basic_data = {'type': self.type,
                  'cluster_identifier': self.cluster_identifier,
                  'node_type': self.node_type,
                  'node_count': self.node_count
                  }
    return basic_data
