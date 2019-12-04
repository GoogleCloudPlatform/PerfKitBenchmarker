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

from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import aws_credentials
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
flags.DEFINE_string('dpb_dataproc_image_version', None,
                    'The image version to use for the cluster.')
flags.DEFINE_integer('dpb_dataproc_distcp_num_maps', None,
                     'Number of maps to copy data.')

SPARK_SAMPLE_LOCATION = ('file:///usr/lib/spark/examples/jars/'
                         'spark-examples.jar')

TESTDFSIO_JAR_LOCATION = ('file:///usr/lib/hadoop-mapreduce/'
                          'hadoop-mapreduce-client-jobclient.jar')

TESTDFSIO_PROGRAM = 'TestDFSIO'

disk_to_hdfs_map = {
    'pd-standard': 'HDD',
    'pd-ssd': 'SSD'
}


class GcpDpbDataproc(dpb_service.BaseDpbService):
  """Object representing a GCP Dataproc cluster.

  Attributes:
    project: ID of the project.
  """

  CLOUD = providers.GCP
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

  @staticmethod
  def _ParseTime(state_time):
    """Parses time from json output.

    Args:
      state_time: string. the state start time.

    Returns:
      datetime.
    """
    try:
      return datetime.datetime.strptime(state_time, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
      return datetime.datetime.strptime(state_time, '%Y-%m-%dT%H:%M:%SZ')

  @staticmethod
  def _GetStats(stdout):
    results = json.loads(stdout)
    stats = {}
    done_time = GcpDpbDataproc._ParseTime(results['status']['stateStartTime'])
    pending_time = None
    start_time = None
    for state in results['statusHistory']:
      if state['state'] == 'PENDING':
        pending_time = GcpDpbDataproc._ParseTime(state['stateStartTime'])
      elif state['state'] == 'RUNNING':
        start_time = GcpDpbDataproc._ParseTime(state['stateStartTime'])

    if done_time and start_time:
      stats[dpb_service.RUNTIME] = (done_time - start_time).total_seconds()
    if start_time and pending_time:
      stats[dpb_service.WAITING] = (
          (start_time - pending_time).total_seconds())
    return stats

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    del benchmark_config  # Unused

  def _Create(self):
    """Creates the cluster."""
    cmd = util.GcloudCommand(self, 'dataproc', 'clusters', 'create',
                             self.cluster_id)
    if self.project is not None:
      cmd.flags['project'] = self.project

    # The number of worker machines in the cluster
    cmd.flags['num-workers'] = self.spec.worker_count

    # Initialize applications on the dataproc cluster
    if self.spec.applications:
      logging.info('Include the requested applications')

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
    if self.dpb_version != 'latest':
      cmd.flags['image-version'] = self.dpb_version

    if FLAGS.gcp_dataproc_image:
      cmd.flags['image'] = FLAGS.gcp_dataproc_image

    cmd.flags['metadata'] = util.MakeFormattedDefaultTags()
    # TODO(saksena): Retrieve the cluster create time and hold in a var
    cmd.Issue()

  def _PostCreate(self):
    """Get the cluster's data and tag it."""
    cmd = util.GcloudCommand(
        self, 'dataproc', 'clusters', 'describe', self.cluster_id)
    stdout, _, _ = cmd.Issue()
    config = json.loads(stdout)['config']
    master = config['masterConfig']
    worker = config['workerConfig']
    for disk in master['instanceNames'] + worker['instanceNames']:
      cmd = util.GcloudCommand(
          self, 'compute', 'disks', 'add-labels', disk)
      cmd.flags['labels'] = util.MakeFormattedDefaultTags()
      cmd.flags['zone'] = self.dpb_service_zone
      cmd.Issue()

  def append_region(self, cmd):
    region = self.dpb_service_zone.rsplit('-', 1)[0]
    cmd.flags['region'] = region

  def _Delete(self):
    """Deletes the cluster."""
    cmd = util.GcloudCommand(self, 'dataproc', 'clusters', 'delete',
                             self.cluster_id)
    cmd.Issue(raise_on_failure=False)

  def _Exists(self):
    """Check to see whether the cluster exists."""
    cmd = util.GcloudCommand(self, 'dataproc', 'clusters', 'describe',
                             self.cluster_id)
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
                job_type=None):
    """See base class."""
    args = ['jobs', 'submit', job_type]

    if job_type == self.PYSPARK_JOB_TYPE:
      args.append(pyspark_file)

    cmd = util.GcloudCommand(self, 'dataproc', *args)

    cmd.flags['cluster'] = self.cluster_id
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()

    if classname:
      cmd.flags['jars'] = jarfile
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

    if job_arguments:
      cmd.additional_flags = ['--'] + job_arguments

    stdout, _, retcode = cmd.Issue(timeout=None, raise_on_failure=False)
    if retcode != 0:
      return {dpb_service.SUCCESS: False}

    stats = self._GetStats(stdout)
    stats[dpb_service.SUCCESS] = True
    return stats

  def SetClusterProperty(self):
    pass

  def _AddToCmd(self, cmd, cmd_property, cmd_value):
    flag_name = cmd_property
    cmd.flags[flag_name] = cmd_value

  def CreateBucket(self, source_bucket):
    mb_command = ['gsutil', 'mb']
    region = self.dpb_service_zone.rsplit('-', 1)[0]
    mb_command.extend(['-c', 'regional', '-l', region])

    if self.project is not None:
      mb_command.extend(['-p', self.project])

    mb_command.append('{}{}'.format(self.PERSISTENT_FS_PREFIX, source_bucket))
    vm_util.IssueCommand(mb_command)

  def generate_data(self, source_dir, udpate_default_fs, num_files, size_file):
    """Method to generate data using a distributed job on the cluster."""
    cmd = util.GcloudCommand(self, 'dataproc', 'jobs', 'submit', 'hadoop')
    cmd.flags['cluster'] = self.cluster_id
    cmd.flags['jar'] = TESTDFSIO_JAR_LOCATION

    job_arguments = [TESTDFSIO_PROGRAM]
    if udpate_default_fs:
      job_arguments.append('-Dfs.default.name={}'.format(source_dir))
    job_arguments.append('-Dtest.build.data={}'.format(source_dir))
    job_arguments.extend(['-write', '-nrFiles', str(num_files), '-fileSize',
                          str(size_file)])
    cmd.additional_flags = ['--'] + job_arguments
    _, _, retcode = cmd.Issue(timeout=None, raise_on_failure=False)
    return {dpb_service.SUCCESS: retcode == 0}

  def read_data(self, source_dir, udpate_default_fs, num_files, size_file):
    """Method to read data using a distributed job on the cluster."""
    cmd = util.GcloudCommand(self, 'dataproc', 'jobs', 'submit', 'hadoop')
    cmd.flags['cluster'] = self.cluster_id
    cmd.flags['jar'] = TESTDFSIO_JAR_LOCATION

    job_arguments = [TESTDFSIO_PROGRAM]
    if udpate_default_fs:
      job_arguments.append('-Dfs.default.name={}'.format(source_dir))
    job_arguments.append('-Dtest.build.data={}'.format(source_dir))
    job_arguments.extend(['-read', '-nrFiles', str(num_files), '-fileSize',
                          str(size_file)])
    cmd.additional_flags = ['--'] + job_arguments
    _, _, retcode = cmd.Issue(timeout=None, raise_on_failure=False)
    return {dpb_service.SUCCESS: retcode == 0}

  def distributed_copy(self, source_location, destination_location):
    """Method to copy data using a distributed job on the cluster."""
    cmd = util.GcloudCommand(self, 'dataproc', 'jobs', 'submit', 'hadoop')
    cmd.flags['cluster'] = self.cluster_id
    cmd.flags['class'] = 'org.apache.hadoop.tools.DistCp'

    job_arguments = (['-m={}'.format(FLAGS.dpb_dataproc_distcp_num_maps)]
                     if FLAGS.dpb_dataproc_distcp_num_maps is not None else [])

    job_arguments.extend([source_location, destination_location])

    cmd.additional_flags = ['--'] + job_arguments
    _, _, retcode = cmd.Issue(timeout=None, raise_on_failure=False)
    return {dpb_service.SUCCESS: retcode == 0}

  def cleanup_data(self, base_dir, udpate_default_fs):
    """Method to cleanup data using a distributed job on the cluster."""
    cmd = util.GcloudCommand(self, 'dataproc', 'jobs', 'submit', 'hadoop')
    cmd.flags['cluster'] = self.cluster_id
    cmd.flags['jar'] = TESTDFSIO_JAR_LOCATION

    job_arguments = [TESTDFSIO_PROGRAM]
    if udpate_default_fs:
      job_arguments.append('-Dfs.default.name={}'.format(base_dir))
    job_arguments.append('-Dtest.build.data={}'.format(base_dir))
    job_arguments.append('-clean')
    cmd.additional_flags = ['--'] + job_arguments
    _, _, retcode = cmd.Issue(timeout=None, raise_on_failure=False)
    if retcode != 0:
      return {dpb_service.SUCCESS: False}
    if udpate_default_fs:
      vm_util.IssueCommand(['gsutil', '-m', 'rm', '-r', base_dir])
    return {dpb_service.SUCCESS: True}

  def MigrateCrossCloud(self, source_location, destination_location):
    """Method to copy data cross cloud using a distributed job on the cluster.

    Currently the only supported destination cloud is AWS.
    TODO(user): Add support for other destination clouds.

    Args:
      source_location: The source GCS path to migrate.
      destination_location: The destination S3 location.

    Returns:
      A dictionary with key 'success' and boolean value set to the status of
      data migration command.
    """
    cmd = util.GcloudCommand(self, 'dataproc', 'jobs', 'submit', 'hadoop')
    if self.project is not None:
      cmd.flags['project'] = self.project
    cmd.flags['cluster'] = self.cluster_id
    cmd.flags['class'] = 'org.apache.hadoop.tools.DistCp'
    s3_access_key, s3_secret_key = aws_credentials.GetCredentials()
    cmd.flags['properties'] = 'fs.s3a.access.key=%s,fs.s3a.secret.key=%s' % (
        s3_access_key, s3_secret_key)
    cmd.additional_flags = ['--'] + [
        'gs://' + source_location, 's3a://' + destination_location
    ]
    _, _, retcode = cmd.Issue(timeout=None, raise_on_failure=False)
    return {dpb_service.SUCCESS: retcode == 0}
