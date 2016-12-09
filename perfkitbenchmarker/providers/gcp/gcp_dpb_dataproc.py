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
"""Module containing class for GCP's dataproc service.

Clusters can be created, have jobs submitted to them and deleted.
"""

import datetime
import json
import logging

from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker.providers.gcp import util


FLAGS = flags.FLAGS

GCP_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

SPARK_SAMPLE_LOCATION = ('file:///usr/lib/spark/examples/jars/'
                         'spark-examples.jar')


class GcpDpbDataproc(dpb_service.BaseDpbService):
  """Object representing a GCP Dataproc cluster.

  Attributes:
    cluster_id: ID of the cluster.
    project: ID of the project.
  """

  CLOUD = providers.GCP
  SERVICE_TYPE = 'dataproc'


  def __init__(self, dpb_service_spec):
    super(GcpDpbDataproc, self).__init__(dpb_service_spec)
    self.project =  None

  @staticmethod
  def _GetStats(stdout):
    results = json.loads(stdout)
    stats = {}
    done_time = datetime.datetime.strptime(
      results['status']['stateStartTime'], GCP_TIME_FORMAT)
    pending_time = None
    start_time = None
    for state in results['statusHistory']:
      if state['state'] == 'PENDING':
        pending_time = datetime.datetime.strptime(state['stateStartTime'],
                                                  GCP_TIME_FORMAT)
      elif state['state'] == 'RUNNING':
        start_time = datetime.datetime.strptime(state['stateStartTime'],
                                                GCP_TIME_FORMAT)

    if done_time and start_time:
      stats[dpb_service.RUNTIME] = (done_time - start_time).total_seconds()
    if start_time and pending_time:
      stats[dpb_service.WAITING] = (
        (start_time - pending_time).total_seconds())
    return stats

  def _Create(self):
    """Creates the cluster."""

    if self.cluster_id is None:
      self.cluster_id = 'pkb-' + FLAGS.run_uri
    cmd = util.GcloudCommand(self, 'dataproc', 'clusters', 'create',
                             self.cluster_id)
    if self.project is not None:
      cmd.flags['project'] = self.project

    # The number of worker machines in the cluster
    cmd.flags['num-workers'] = self.spec.worker_count

    ## can configure initialization actions here
    if self.spec.initialization_actions:
      logging.info('Provider specific mechanism to include the requested applications')

    # TODO: cleanup the setup and then the comment
    # There is only one group the worker group now
    # I think the spec is the one copying the worker configuration to the
    # master configuration

    for group_type, group_spec in [
        ('worker', self.spec.worker_group)]:
      flag_name = group_type + '-machine-type'
      cmd.flags[flag_name] = group_spec.vm_spec.machine_type

      if group_spec.vm_spec.num_local_ssds:
        ssd_flag = 'num-{0}-local-ssds'.format(group_type)
        cmd.flags[ssd_flag] = group_spec.vm_spec.num_local_ssds

      if group_spec.vm_spec.boot_disk_size:
        disk_flag = group_type + '-boot-disk-size'
        cmd.flags[disk_flag] = group_spec.vm_spec.boot_disk_size

    cmd.Issue()

  def _Delete(self):
    """Deletes the cluster."""
    cmd = util.GcloudCommand(self, 'dataproc', 'clusters', 'delete',
                             self.cluster_id)
    # TODO test this claim
    # If we don't put this here, zone is automatically added, which
    # breaks the dataproc clusters delete
    cmd.flags['zone'] = []
    cmd.Issue()

  def _Exists(self):
    """Check to see whether the cluster exists."""
    cmd = util.GcloudCommand(self, 'dataproc', 'clusters', 'describe',
                             self.cluster_id)
    # TODO test this claim
    # If we don't put this here, zone is automatically added to
    # the command, which breaks dataproc clusters describe
    cmd.flags['zone'] = []
    _, _, retcode = cmd.Issue()
    return retcode == 0


  def SubmitJob(self, jarfile, classname, job_poll_interval=None,
                job_arguments=None, job_stdout_file=None,
                job_type=None):
    cmd = util.GcloudCommand(self, 'dataproc', 'jobs', 'submit', job_type)
    cmd.flags['cluster'] = self.cluster_id
    # TODO test this claim
    # If we don't put this here, zone is auotmatically added to the command
    # which breaks dataproc jobs submit
    cmd.flags['zone'] = []

    if classname:
      cmd.flags['jars'] = jarfile
      cmd.flags['class'] = classname
    else:
      cmd.flags['jar'] = jarfile

    # Dataproc gives as stdout an object describing job execution.
    # Its stderr contains a mix of the stderr of the job, and the
    # stdout of the job.  We set the driver log level to FATAL
    # to suppress those messages, and we can then separate, hopefully
    # the job standard out from the log messages.
    cmd.flags['driver-log-levels'] = 'root=FATAL'

    if job_arguments:
      cmd.additional_flags = ['--'] + job_arguments

    stdout, stderr, retcode = cmd.Issue(timeout=None)
    if retcode != 0:
      return {dpb_service.SUCCESS: False}

    stats = self._GetStats(stdout)
    return stats

  def SetClusterProperty(self):
    pass

"""
TODO:
1. Add the stats for the cluster creation time
2. Rename initialization actions to applications
3. Maybe add a validate function to verify that we the
bare requirements of job submit working
4. Improve upon the _GetStats method. Figure out the stats
that should be pulled out (STATUS, various times)
"""