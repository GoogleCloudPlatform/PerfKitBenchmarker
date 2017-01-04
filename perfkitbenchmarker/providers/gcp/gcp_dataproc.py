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

import datetime
import json
import re

from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import spark_service
from perfkitbenchmarker.providers.gcp import util


FLAGS = flags.FLAGS

GCP_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


class GcpDataproc(spark_service.BaseSparkService):
  """Object representing a GCP Dataproc cluster.

  Attributes:
    cluster_id: ID of the cluster.
    project: ID of the project.
  """

  CLOUD = providers.GCP
  SERVICE_NAME = 'dataproc'

  def __init__(self, spark_service_spec):
    super(GcpDataproc, self).__init__(spark_service_spec)
    self.project = self.spec.master_group.vm_spec.project

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
      stats[spark_service.RUNTIME] = (done_time - start_time).total_seconds()
    if start_time and pending_time:
      stats[spark_service.WAITING] = (
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
    cmd.flags['num-workers'] = self.spec.worker_group.vm_count

    for group_type, group_spec in [
        ('worker', self.spec.worker_group),
        ('master', self.spec.master_group)]:
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
    # If we don't put this here, zone is automatically added, which
    # breaks the dataproc clusters delete
    cmd.flags['zone'] = []
    cmd.Issue()

  def _Exists(self):
    """Check to see whether the cluster exists."""
    cmd = util.GcloudCommand(self, 'dataproc', 'clusters', 'describe',
                             self.cluster_id)
    # If we don't put this here, zone is automatically added to
    # the command, which breaks dataproc clusters describe
    cmd.flags['zone'] = []
    _, _, retcode = cmd.Issue()
    return retcode == 0


  def SubmitJob(self, jarfile, classname, job_poll_interval=None,
                job_arguments=None, job_stdout_file=None,
                job_type=spark_service.SPARK_JOB_TYPE):
    cmd = util.GcloudCommand(self, 'dataproc', 'jobs', 'submit', job_type)
    cmd.flags['cluster'] = self.cluster_id
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
      return {spark_service.SUCCESS: False}

    stats = self._GetStats(stdout)
    stats[spark_service.SUCCESS] = True

    if job_stdout_file:
      with open(job_stdout_file, 'w') as f:
        lines = stderr.splitlines(True)
        if (not re.match(r'Job \[.*\] submitted.', lines[0]) or
            not re.match(r'Waiting for job output...', lines[1])):
          raise Exception('Dataproc output in unexpected format.')
        i = 2
        if job_type == spark_service.SPARK_JOB_TYPE:
          if not re.match(r'\r', lines[i]):
            raise Exception('Dataproc output in unexpected format.')
          i += 1
          # Eat these status lines.  They end in \r, so they overwrite
          # themselves at the console or when you cat a file.  But they
          # are part of this string.
          while re.match(r'\[Stage \d+:', lines[i]):
            i += 1
          if not re.match(r' *\r$', lines[i]):
            raise Exception('Dataproc output in unexpected format.')

        while i < len(lines) and not re.match(r'Job \[.*\]', lines[i]):
          f.write(lines[i])
          i += 1
        if i != len(lines) - 1:
          raise Exception('Dataproc output in unexpected format.')
    return stats

  def SetClusterProperty(self):
    pass

  def GetMetadata(self):
    basic_data = super(GcpDataproc, self).GetMetadata()
    if self.spec.worker_group.vm_spec.num_local_ssds:
      basic_data.update({'ssd_count':
                        str(self.spec.worker_group.vm_spec.num_local_ssds)})
    return basic_data
