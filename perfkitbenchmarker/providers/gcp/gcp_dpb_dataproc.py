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
from typing import Any, Dict, Optional

from absl import flags
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import providers
from perfkitbenchmarker.linux_packages import aws_credentials
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
flags.DEFINE_string('dpb_dataproc_image_version', None,
                    'The image version to use for the cluster.')

disk_to_hdfs_map = {
    'pd-standard': 'HDD',
    'pd-balanced': 'SSD (Balanced)',
    'pd-ssd': 'SSD',
}


class GcpDpbDataproc(dpb_service.BaseDpbService):
  """Object representing a GCP Dataproc cluster.

  Attributes:
    project: ID of the project.
  """

  CLOUD = providers.GCP
  SERVICE_TYPE = 'dataproc'

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
    self.persistent_fs_prefix = 'gs://'
    self._cluster_create_time = None
    if self.user_managed and not FLAGS.dpb_service_bucket:
      self.bucket = self._GetCluster()['config']['tempBucket']

  def GetClusterCreateTime(self) -> Optional[float]:
    """Returns the cluster creation time.

    On this implementation, the time returned is based on the timestamps
    reported by the Dataproc API (which is stored in the _cluster_create_time
    attribute).

    Returns:
      A float representing the creation time in seconds or None.
    """
    return self._cluster_create_time

  @staticmethod
  def _ParseTime(state_time: str) -> datetime.datetime:
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
        # This will actually be used for storage
        self.dpb_hdfs_type = 'Local SSD'
    # Set zone
    cmd.flags['zone'] = self.dpb_service_zone
    if self.dpb_version:
      cmd.flags['image-version'] = self.dpb_version

    if FLAGS.gcp_dataproc_image:
      cmd.flags['image'] = FLAGS.gcp_dataproc_image

    if FLAGS.dpb_cluster_properties:
      cmd.flags['properties'] = ','.join(FLAGS.dpb_cluster_properties)

    # Ideally DpbServiceSpec would have a network spec, which we would create to
    # Resolve the name, but because EMR provisions its own VPC and we are
    # generally happy using pre-existing networks for Dataproc. Just use the
    # underlying flag instead.
    if FLAGS.gce_network_name:
      cmd.flags['network'] = FLAGS.gce_network_name

    metadata = util.GetDefaultTags()
    metadata.update(flag_util.ParseKeyValuePairs(FLAGS.gcp_instance_metadata))
    cmd.flags['metadata'] = util.FormatTags(metadata)
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
    timeout = 900  # 15 min
    stdout, stderr, retcode = cmd.Issue(timeout=timeout, raise_on_failure=False)
    self._cluster_create_time = self._ParseClusterCreateTime(stdout)
    if retcode:
      util.CheckGcloudResponseKnownFailures(stderr, retcode)
      raise errors.Resource.CreationError(stderr)

  @classmethod
  def _ParseClusterCreateTime(cls, stdout: str) -> Optional[float]:
    """Parses the cluster create time from a raw API response."""
    try:
      creation_data = json.loads(stdout)
    except json.JSONDecodeError:
      creation_data = {}
    can_parse = creation_data.get('status', {}).get('state') == 'RUNNING'
    status_history = creation_data.get('statusHistory', [])
    can_parse = can_parse and len(
        status_history) == 1 and status_history[0]['state'] == 'CREATING'
    if not can_parse:
      logging.warning('Unable to parse cluster creation duration.')
      return None
    creation_start = cls._ParseTime(status_history[0]['stateStartTime'])
    creation_end = cls._ParseTime(creation_data['status']['stateStartTime'])
    return (creation_end - creation_start).total_seconds()

  def _Delete(self):
    """Deletes the cluster."""
    cmd = self.DataprocGcloudCommand('clusters', 'delete', self.cluster_id)
    cmd.Issue(raise_on_failure=False)

  def _GetCluster(self) -> Optional[Dict[str, Any]]:
    """Get the cluster resource in a dict."""
    cmd = self.DataprocGcloudCommand('clusters', 'describe', self.cluster_id)
    stdout, _, retcode = cmd.Issue(raise_on_failure=False)
    if not retcode:
      return json.loads(stdout)

  def _Exists(self):
    """Check to see whether the cluster exists."""
    return self._GetCluster() is not None

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

  def _AddToCmd(self, cmd, cmd_property, cmd_value):
    flag_name = cmd_property
    cmd.flags[flag_name] = cmd_value

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
    s3_access_key, s3_secret_key = aws_credentials.GetCredentials()
    return self.DistributedCopy(
        'gs://' + source_location,
        dest_prefix + destination_location,
        properties={
            'fs.s3a.access.key': s3_access_key,
            'fs.s3a.secret.key': s3_secret_key,
        })


class GcpDpbDpgke(GcpDpbDataproc):
  """Dataproc on GKE cluster."""

  CLOUD = providers.GCP
  SERVICE_TYPE = 'dataproc_gke'

  def __init__(self, dpb_service_spec):
    super(GcpDpbDpgke, self).__init__(dpb_service_spec)
    required_spec_attrs = [
        'gke_cluster_name', 'gke_cluster_nodepools',
        'gke_cluster_location'
    ]
    missing_attrs = [
        attr for attr in required_spec_attrs
        if not getattr(self.spec, attr, None)
    ]
    if missing_attrs:
      raise errors.Setup.InvalidSetupError(
          f'{missing_attrs} must be provided for provisioning DPGKE.')

  def _Create(self):
    """Creates the dpgke virtual cluster."""
    cmd = self.DataprocGcloudCommand('clusters', 'gke', 'create',
                                     self.cluster_id)
    cmd.use_alpha_gcloud = True
    cmd.flags['setup-workload-identity'] = True
    cmd.flags['gke-cluster'] = self.spec.gke_cluster_name
    cmd.flags['namespace'] = self.cluster_id
    # replace ':' field delimiter with '=' since create cluster command
    # only accept '=' as field delimiter but pkb doesn't allow overriding
    # spec parameters containing '='
    cmd.flags['pools'] = self.spec.gke_cluster_nodepools.replace(':', '=')
    cmd.flags['gke-cluster-location'] = self.spec.gke_cluster_location
    if FLAGS.dpb_service_bucket:
      cmd.flags['staging-bucket'] = FLAGS.dpb_service_bucket
    if self.project is not None:
      cmd.flags['project'] = self.project
    cmd.flags['image-version'] = self.spec.version
    if FLAGS.dpb_cluster_properties:
      cmd.flags['properties'] = ','.join(FLAGS.dpb_cluster_properties)
    timeout = 900  # 15 min
    logging.info('Issuing command to create dpgke cluster. Flags %s, Args %s',
                 cmd.flags, cmd.args)
    stdout, stderr, retcode = cmd.Issue(timeout=timeout, raise_on_failure=False)
    self._cluster_create_time = self._ParseClusterCreateTime(stdout)
    if retcode:
      util.CheckGcloudResponseKnownFailures(stderr, retcode)
      raise errors.Resource.CreationError(stderr)
