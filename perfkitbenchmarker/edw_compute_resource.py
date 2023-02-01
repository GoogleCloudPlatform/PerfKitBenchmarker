# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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

"""Resource representing a compute resource associated with a specific edw service.

This is an abstract resource representing computation resources provisioned for
edw systems. Examples of these resources include Bigquery slot commitments,
Snowflake warehouses, and Redshift clusters. Note that these resources are
computation-only -- no storage is provisioned (except, in some limited cases,
for storage that comes attached to compute units -- such as with Redshift DC2
nodes), and no datasets are configured. In order to run queries, these
additional resources must be pre-provisioned or provisioned in the benchmark.
"""

from typing import List, Optional, Type

from perfkitbenchmarker import resource
from perfkitbenchmarker import sample


class EdwComputeResource(resource.BaseResource):
  """Abstract resource representing a computational resource for an EDW service.

  Examples include Bigquery slot commitments, Redshift clusters, and Snowflake
  warehouses.

  Attributes:
    compute_resource_identifier: An ID or other resource identifier for the
      provisioned compute resource.
  """

  REQUIRED_ATTRS = ['CLOUD', 'SERVICE_TYPE']
  RESOURCE_TYPE = 'EdwComputeResource'
  CLOUD = 'abstract'
  SERVICE_TYPE = 'abstract'

  def __init__(self, edw_service_spec):
    super(EdwComputeResource, self).__init__()
    self.compute_resource_identifier = None

  def GetLifecycleMetrics(self) -> List[sample.Sample]:
    metrics = []
    provision_response_latency = self.create_end_time - self.create_start_time
    provision_complete_latency = (
        self.resource_ready_time - self.create_start_time
    )
    metrics.extend([
        sample.Sample(
            'edw_resource_provision_latency', provision_response_latency, 's'
        ),
        sample.Sample(
            'edw_resource_provision_ready_latency',
            provision_complete_latency,
            's',
        ),
    ])
    return metrics


def GetEdwComputeResourceClass(
    cloud: str, edw_compute_service_type: str
) -> Optional[Type[EdwComputeResource]]:
  return resource.GetResourceClass(
      EdwComputeResource, CLOUD=cloud, SERVICE_TYPE=edw_compute_service_type
  )
