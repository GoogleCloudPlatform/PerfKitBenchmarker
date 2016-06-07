# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing class for GCP's spark service.

Spark clusters can be created and deleted.
"""

import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import spark_service
from perfkitbenchmarker.providers.gcp import util


FLAGS = flags.FLAGS


class GcpDataproc(spark_service.BaseSparkService):
  """Object representing a GCP Dataproc cluster.

  Attributes:
    name: Cluster name.
    num_nodes: Number of nodes in the cluster.
    project: Enclosing project for the cluster.
    zone: zone of the cluster.
  """

  CLOUD = providers.GCP
  SERVICE_NAME = 'dataproc'

  def _Create(self):
    """Creates the cluster."""
    cmd = util.GcloudCommand(self, 'dataproc', 'clusters', 'create',
                             self.name)
    if self.project is not None:
      cmd.flags['project'] = self.project
    cmd.flags['num-workers'] = self.num_workers
    if self.machine_type:
      cmd.flags['worker-machine-type'] = self.machine_type
      cmd.flags['master-machine-type'] = self.machine_type
    cmd.Issue()

  def _Delete(self):
    """Deletes the cluster."""
    cmd = util.GcloudCommand(self, 'dataproc', 'clusters', 'delete',
                             self.name)
    cmd.Issue()

  def _Exists(self):
    """Check to see whether the cluster exists."""
    cmd = util.GcloudCommand(self, 'dataproc', 'clusters', 'describe',
                             self.name)
    stdout, stderr, retcode = cmd.Issue()
    return retcode == 0

  def SubmitJob(self, jarfile, classname):
    cmd = util.GcloudCommand(self, 'dataproc', 'jobs', 'submit', 'spark')
    cmd.flags['cluster'] = self.name
    cmd.flags['jar'] = jarfile
    cmd.flags['class'] = classname
    stdout, stderr, retcode = cmd.Issue()
    if retcode != 0:
      logging.error('Submit job returned code %s STDOUT: %s STDERR: %s',
                    retcode, stdout, stderr)
    return stdout, stderr, retcode

  def SetClusterProperty(self):
    pass
