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
    cluster_id: ID of the cluster.
    num_workers: Number of nodes in the cluster.
    project: Enclosing project for the cluster.
  """

  CLOUD = providers.GCP
  SERVICE_NAME = 'dataproc'

  def _Create(self):
    """Creates the cluster."""

    if self.cluster_id is None:
      self.cluster_id = 'pkb-' + FLAGS.run_uri
    cmd = util.GcloudCommand(self, 'dataproc', 'clusters', 'create',
                             self.cluster_id)
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
                             self.cluster_id)
    cmd.Issue()

  def _Exists(self):
    """Check to see whether the cluster exists."""
    cmd = util.GcloudCommand(self, 'dataproc', 'clusters', 'describe',
                             self.cluster_id)
    stdout, stderr, retcode = cmd.Issue()
    return retcode == 0

  def SubmitJob(self, jarfile, classname, job_poll_interval=None,
                job_arguments=None, job_stdout_file=None):
    cmd = util.GcloudCommand(self, 'dataproc', 'jobs', 'submit', 'spark')
    cmd.flags['cluster'] = self.cluster_id
    cmd.flags['jar'] = jarfile
    cmd.flags['class'] = classname
    # Dataproc gives as stdout an object describing job execution.
    # Its stderr contains a mix of the stderr of the job, and the
    # stdout of the job.  We set the driver log level to FATAL
    # to supress those messages, and we can then separate, hopefully
    # the job standard out from the log messages.
    cmd.flags['driver-log-levels'] = 'root=FATAL'
    if job_arguments:
      cmd.additional_flags = job_arguments
    stdout, stderr, retcode = cmd.Issue()
    logging.debug('STDOUT: {0}\nSTDERR: {1}\n'.format(stdout, stderr))
    if retcode != 0:
      return False
    if job_stdout_file:
      with open(job_stdout_file, 'w') as f:
        # dataproc prints progress lines that end with a cr and so
        # overwrite themselves.  We need to move past that.
        last_cr_index = stderr.rfind('\r')
        lines = stderr[last_cr_index + 1:].split('\n')
        # the visible line says the job is done, and the
        # then the split leaves an empty line.
        good_lines = lines[:-2]
        for l in good_lines:
          f.write(l)
          f.write('\n')
    return True

  def SetClusterProperty(self):
    pass
