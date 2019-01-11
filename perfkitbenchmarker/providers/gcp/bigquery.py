# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for GCP's Bigquery EDW service."""

from perfkitbenchmarker import edw_service
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker.providers.gcp import util as gcp_util


FLAGS = flags.FLAGS


class Bigquery(edw_service.EdwService):
  """Object representing a Bigquery cluster.

  Attributes:
    job_id_prefix: A string prefix for the job id for bigquery job.
  """

  CLOUD = providers.GCP
  SERVICE_TYPE = 'bigquery'

  def __init__(self, edw_service_spec):
    super(Bigquery, self).__init__(edw_service_spec)
    self.job_id_prefix = FLAGS.run_uri

  def _Create(self):
    """Create a BigQuery cluster.

    Bigquery clusters creation is out of scope of the benchmarking.
    """
    raise NotImplementedError

  def _Exists(self):
    """Method to validate the existence of a Bigquery cluster.

    Returns:
      Boolean value indicating the existence of a cluster.
    """
    return True

  def _Delete(self):
    """Delete a BigQuery cluster.

    Bigquery cluster deletion is out of scope of benchmarking.
    """
    raise NotImplementedError

  def GetMetadata(self):
    """Return a dictionary of the metadata for the BigQuery cluster."""
    basic_data = super(Bigquery, self).GetMetadata()
    basic_data['bq_job_id_prefix'] = self.job_id_prefix
    return basic_data

  def RunCommandHelper(self):
    """Bigquery specific run script command components."""
    bq = self.cluster_identifier.split('.')
    return ('--bq_project_id={} --bq_dataset_id={}'
            ' --bq_job_id_prefix={}').format(bq[0], bq[1], self.job_id_prefix)

  def InstallAndAuthenticateRunner(self, vm):
    """Method to perform installation and authentication of bigquery runner.

    Native Bigquery client that ships with the google_cloud_sdk
    https://cloud.google.com/bigquery/docs/bq-command-line-too used as client.

    Args:
      vm: Client vm on which the script will be run.
    """
    vm.Install('google_cloud_sdk')
    gcp_util.AuthenticateServiceAccount(vm)
