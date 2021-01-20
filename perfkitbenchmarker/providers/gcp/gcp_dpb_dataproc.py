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

Clusters can be created, have jobs submitted to them and deleted. See details
at https://cloud.google.com/dataproc/
"""

import datetime
import json
import logging
from absl import flags
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import aws_credentials
from perfkitbenchmarker.providers import gcp
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
flags.DEFINE_string('dpb_dataproc_image_version', None,
                    'The image version to use for the cluster.')
flags.DEFINE_integer('dpb_dataproc_distcp_num_maps', None,
                     'Number of maps to copy data.')

SPARK_SAMPLE_LOCATION = ('file:///usr/lib/spark/examples/jars/'
                         'spark-examples.jar')

disk_to_hdfs_map = {
    'pd-standard': 'HDD',
    'pd-ssd': 'SSD'
}


class GcpDpbDataproc(dpb_service.BaseDpbService):
  """Object representing a GCP Dataproc cluster.

  Attributes:
    project: ID of the project.
  """

  CLOUD = gcp.CLOUD
  SERVICE_TYPE = 'dataproc'
  PERSISTENT_FS_PREFIX = 'gs://'

  def __init__(self, dpb_service_spec):
    super(GcpDpbDataproc, self).__init__(dpb_service_spec)
    self.dpb_service_type = GcpDpbDataproc.SERVICE_TYPE
    self.project = FLAGS.project
    if FLAGS.dpb_dataproc_image_version:
      self.dpb_version = FLAGS.dpb_dataproc_image_version
    if not self.dpb_service_zone:
      raise errors.Setup.InvalidSetupError(
          'dpb_service_zone must be provided, for provisioning.')
    self.region = self.dpb_service_zone.rsplit('-', 1)[0]
    self.storage_service = gcs.GoogleCloudStorageService()
    self.storage_service.PrepareService(location=self.region)

  @staticmethod
  def _ParseTime(state_time: str) -> datetime:
    """Parses time from json output.

    Args:
      state_time: string. the state start time.

    Returns:
      Parsed datetime.
    """
    try:
      return datetime.datetime.strptime(state_time, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
      return datetime.datetime.strptime(state_time, '%Y-%m-%dT%H:%M:%SZ')

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    del benchmark_config  # Unused

  def DataprocGcloudCommand(self, *args):
    all_args = ('dataproc',) + args
    cmd = util.GcloudCommand(self, *all_args)
    cmd.flags['region'] = self.region
    return cmd

  def _Create(self):
    """Creates the cluster."""
    cmd = self.DataprocGcloudCommand('clusters', 'create', self.cluster_id)
    if self.project is not None:
      cmd.flags['project'] = self.project

    if self.spec.worker_count:
      # The number of worker machines in the cluster
      cmd.flags['num-workers'] = self.spec.worker_count
    else:
      cmd.flags['single-node'] = True

    # Initialize applications on the dataproc cluster
    if self.spec.applications:
      logging.info('Include the requested applications')
      cmd.flags['optional-components'] = ','.join(self.spec.applications)

    # Enable component gateway for debuggability. Does not impact performance.
    cmd.flags['enable-component-gateway'] = True

    # TODO(pclay): stop ignoring spec.master_group?
    for role in ['worker', 'master']:
      # Set machine type
      if self.spec.worker_group.vm_spec.machine_type:
        self._AddToCmd(cmd, '{0}-machine-type'.format(role),
                       self.spec.worker_group.vm_spec.machine_type)
      # Set boot_disk_size
      if self.spec.worker_group.disk_spec.disk_size:
        size_in_gb = '{}GB'.format(
            str(self.spec.worker_group.disk_spec.disk_size))
        self._AddToCmd(cmd, '{0}-boot-disk-size'.format(role), size_in_gb)
      # Set boot_disk_type
      if self.spec.worker_group.disk_spec.disk_type:
        self._AddToCmd(cmd, '{0}-boot-disk-type'.format(role),
                       self.spec.worker_group.disk_spec.disk_type)
        self.dpb_hdfs_type = disk_to_hdfs_map[
            self.spec.worker_group.disk_spec.disk_type]

      # Set ssd count
      if self.spec.worker_group.vm_spec.num_local_ssds:
        self._AddToCmd(cmd, 'num-{0}-local-ssds'.format(role),
                       self.spec.worker_group.vm_spec.num_local_ssds)
    # Set zone
    cmd.flags['zone'] = self.dpb_service_zone
    if self.dpb_version:
      cmd.flags['image-version'] = self.dpb_version

    if FLAGS.gcp_dataproc_image:
      cmd.flags['image'] = FLAGS.gcp_dataproc_image

    cmd.flags['metadata'] = util.MakeFormattedDefaultTags()
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
    timeout = 900  # 15 min
    # TODO(saksena): Retrieve the cluster create time and hold in a var
    _, stderr, retcode = cmd.Issue(timeout=timeout, raise_on_failure=False)
    if retcode:
      util.CheckGcloudResponseKnownFailures(stderr, retcode)
      raise errors.Resource.CreationError(stderr)

  def _Delete(self):
    """Deletes the cluster."""
    cmd = self.DataprocGcloudCommand('clusters', 'delete', self.cluster_id)
    cmd.Issue(raise_on_failure=False)

  def _Exists(self):
    """Check to see whether the cluster exists."""
    cmd = self.DataprocGcloudCommand('clusters', 'describe', self.cluster_id)
    _, _, retcode = cmd.Issue(raise_on_failure=False)
    return retcode == 0

  def SubmitJob(self,
                jarfile=None,
                classname=None,
                pyspark_file=None,
                query_file=None,
                job_poll_interval=None,
                job_stdout_file=None,
                job_arguments=None,
                job_files=None,
                job_jars=None,
                job_type=None,
                properties=None):
    """See base class."""
    assert job_type
    args = ['jobs', 'submit', job_type]

    if job_type == self.PYSPARK_JOB_TYPE:
      args.append(pyspark_file)

    cmd = self.DataprocGcloudCommand(*args)

    cmd.flags['cluster'] = self.cluster_id
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()

    job_jars = job_jars or []
    if classname:
      if jarfile:
        # Dataproc does not support both a main class and a main jar so just
        # make the main jar an additional jar instead.
        job_jars.append(jarfile)
      cmd.flags['class'] = classname
    elif jarfile:
      cmd.flags['jar'] = jarfile

    if query_file:
      cmd.flags['file'] = query_file

    if job_files:
      cmd.flags['files'] = ','.join(job_files)
    if job_jars:
      cmd.flags['jars'] = ','.join(job_jars)

    # Dataproc gives as stdout an object describing job execution.
    # Its stderr contains a mix of the stderr of the job, and the
    # stdout of the job.  We set the driver log level to FATAL
    # to suppress those messages, and we can then separate, hopefully
    # the job standard out from the log messages.
    cmd.flags['driver-log-levels'] = 'root={}'.format(FLAGS.dpb_log_level)

    all_properties = self.GetJobProperties()
    all_properties.update(properties or {})
    if all_properties:
      # For commas: https://cloud.google.com/sdk/gcloud/reference/topic/escaping
      cmd.flags['properties'] = '^@^' + '@'.join(
          '{}={}'.format(k, v) for k, v in all_properties.items())

    if job_arguments:
      cmd.additional_flags = ['--'] + job_arguments

    stdout, stderr, retcode = cmd.Issue(timeout=None, raise_on_failure=False)
    if retcode != 0:
      raise dpb_service.JobSubmissionError(stderr)

    results = json.loads(stdout)
    # Otherwise retcode would not have been 0
    assert results['status']['state'] == 'DONE'
    done_time = GcpDpbDataproc._ParseTime(results['status']['stateStartTime'])
    pending_time = None
    start_time = None
    for state in results['statusHistory']:
      if state['state'] == 'PENDING':
        pending_time = GcpDpbDataproc._ParseTime(state['stateStartTime'])
      elif state['state'] == 'RUNNING':
        start_time = GcpDpbDataproc._ParseTime(state['stateStartTime'])

    assert pending_time and start_time and done_time

    return dpb_service.JobResult(
        run_time=(done_time - start_time).total_seconds(),
        pending_time=(start_time - pending_time).total_seconds())

  def _GetCompletedJob(self, job_id):
    raise NotImplementedError('Dataproc SubmitJob uses a blocking command.')

  def _AddToCmd(self, cmd, cmd_property, cmd_value):
    flag_name = cmd_property
    cmd.flags[flag_name] = cmd_value

  def distributed_copy(self, source_location, destination_location):
    """Method to copy data using a distributed job on the cluster."""
    cmd = self.DataprocGcloudCommand('jobs', 'submit', 'hadoop')
    cmd.flags['cluster'] = self.cluster_id
    cmd.flags['class'] = 'org.apache.hadoop.tools.DistCp'

    job_arguments = (['-m={}'.format(FLAGS.dpb_dataproc_distcp_num_maps)]
                     if FLAGS.dpb_dataproc_distcp_num_maps is not None else [])

    job_arguments.extend([source_location, destination_location])

    cmd.additional_flags = ['--'] + job_arguments
    _, _, retcode = cmd.Issue(timeout=None, raise_on_failure=False)
    return {dpb_service.SUCCESS: retcode == 0}

  def MigrateCrossCloud(self,
                        source_location,
                        destination_location,
                        dest_cloud='AWS'):
    """Method to copy data cross cloud using a distributed job on the cluster.

    Currently the only supported destination cloud is AWS.
    TODO(user): Add support for other destination clouds.

    Args:
      source_location: The source GCS path to migrate.
      destination_location: The destination path.
      dest_cloud: The cloud to copy data to.

    Returns:
      A dictionary with key 'success' and boolean value set to the status of
      data migration command.
    """
    if dest_cloud == 'AWS':
      dest_prefix = 's3a://'
    else:
      raise ValueError('Unsupported destination cloud.')

    cmd = self.DataprocGcloudCommand('jobs', 'submit', 'hadoop')
    if self.project is not None:
      cmd.flags['project'] = self.project
    cmd.flags['cluster'] = self.cluster_id
    cmd.flags['class'] = 'org.apache.hadoop.tools.DistCp'
    s3_access_key, s3_secret_key = aws_credentials.GetCredentials()
    cmd.flags['properties'] = 'fs.s3a.access.key=%s,fs.s3a.secret.key=%s' % (
        s3_access_key, s3_secret_key)
    cmd.additional_flags = ['--'] + [
        'gs://' + source_location, dest_prefix + destination_location
    ]
    _, _, retcode = cmd.Issue(timeout=None, raise_on_failure=False)
    return {dpb_service.SUCCESS: retcode == 0}
