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
import os
import re
from typing import Any, Dict, Optional, Tuple

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import provider_info
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import aws_credentials
from perfkitbenchmarker.providers.gcp import gcp_dpb_dataproc_serverless_prices
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util

FLAGS = flags.FLAGS
flags.DEFINE_string(
    'dpb_dataproc_image_version',
    None,
    'The image version to use for the cluster.',
)

disk_to_hdfs_map = {
    'pd-standard': 'HDD',
    'pd-balanced': 'SSD (Balanced)',
    'pd-ssd': 'SSD',
}
serverless_disk_to_hdfs_map = {
    'standard': 'HDD',
    'premium': 'Local SSD',
}

DATAPROC_FLINK_INIT_SCRIPT = os.path.join('beam', 'flink-init.sh')
DATAPROC_FLINK_PRESUBMIT_SCRIPT = os.path.join('beam', 'flink-presubmit.sh')
DATAPROC_FLINK_TRIGGER_SCRIPT = os.path.join('beam', 'flink-trigger.sh')


class MetricNotReadyError(Exception):
  """Used to signal metric is not ready."""

  pass


class GcpDpbBaseDataproc(dpb_service.BaseDpbService):
  """Base class for all Dataproc-based services (cluster or serverless)."""

  def __init__(self, dpb_service_spec):
    super().__init__(dpb_service_spec)
    self.project = FLAGS.project
    if not self.dpb_service_zone:
      raise errors.Setup.InvalidSetupError(
          'dpb_service_zone must be provided, for provisioning.'
      )
    self.region = self.dpb_service_zone.rsplit('-', 1)[0]
    self.storage_service = gcs.GoogleCloudStorageService()
    self.storage_service.PrepareService(location=self.region)
    self.persistent_fs_prefix = 'gs://'
    self._cluster_create_time: Optional[float] = None
    self._cluster_ready_time: Optional[float] = None
    self._cluster_delete_time: Optional[float] = None

  def GetDpbVersion(self) -> Optional[str]:
    return FLAGS.dpb_dataproc_image_version or super().GetDpbVersion()

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
    all_args = ('dataproc',) + tuple(args)
    cmd = util.GcloudCommand(self, *all_args)
    cmd.flags['region'] = self.region
    return cmd

  def MigrateCrossCloud(
      self, source_location, destination_location, dest_cloud='AWS'
  ):
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
        },
    )


class GcpDpbDataproc(GcpDpbBaseDataproc):
  """Object representing a managed GCP Dataproc cluster.

  Attributes:
    project: ID of the project.
  """

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'dataproc'
  SUPPORTS_NO_DYNALLOC = True

  def __init__(self, dpb_service_spec):
    super().__init__(dpb_service_spec)
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
    if self._cluster_create_time is None or self._cluster_ready_time is None:
      return None
    return self._cluster_ready_time - self._cluster_create_time

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
        self._AddToCmd(
            cmd,
            '{0}-machine-type'.format(role),
            self.spec.worker_group.vm_spec.machine_type,
        )
      # Set boot_disk_size
      if self.spec.worker_group.disk_spec.disk_size:
        size_in_gb = '{}GB'.format(
            str(self.spec.worker_group.disk_spec.disk_size)
        )
        self._AddToCmd(cmd, '{0}-boot-disk-size'.format(role), size_in_gb)
      # Set boot_disk_type
      if self.spec.worker_group.disk_spec.disk_type:
        self._AddToCmd(
            cmd,
            '{0}-boot-disk-type'.format(role),
            self.spec.worker_group.disk_spec.disk_type,
        )
      # Set ssd count
      if self.spec.worker_group.vm_spec.num_local_ssds:
        self._AddToCmd(
            cmd,
            'num-{0}-local-ssds'.format(role),
            self.spec.worker_group.vm_spec.num_local_ssds,
        )
      # Set SSD interface
      if self.spec.worker_group.vm_spec.ssd_interface:
        self._AddToCmd(
            cmd,
            '{0}-local-ssd-interface'.format(role),
            self.spec.worker_group.vm_spec.ssd_interface,
        )
    # Set zone
    cmd.flags['zone'] = self.dpb_service_zone
    if self.GetDpbVersion():
      cmd.flags['image-version'] = self.GetDpbVersion()

    if FLAGS.gcp_dataproc_image:
      cmd.flags['image'] = FLAGS.gcp_dataproc_image

    # http://cloud/dataproc/docs/guides/profiling#enable_profiling
    if FLAGS.gcloud_scopes:
      cmd.flags['scopes'] = ','.join(re.split(r'[,; ]', FLAGS.gcloud_scopes))

    if self.GetClusterProperties():
      cmd.flags['properties'] = ','.join(self.GetClusterProperties())

    if FLAGS.dpb_initialization_actions:
      cmd.flags['initialization-actions'] = FLAGS.dpb_initialization_actions

    # Ideally DpbServiceSpec would have a network spec, which we would create to
    # Resolve the name, but because EMR provisions its own VPC and we are
    # generally happy using pre-existing networks for Dataproc. Just use the
    # underlying flag instead.
    if FLAGS.gce_network_name:
      cmd.flags['network'] = FLAGS.gce_network_name[0]

    metadata = util.GetDefaultTags()
    metadata.update(flag_util.ParseKeyValuePairs(FLAGS.gcp_instance_metadata))
    cmd.flags['metadata'] = util.FormatTags(metadata)
    cmd.flags['labels'] = util.MakeFormattedDefaultTags()
    timeout = 900  # 15 min
    stdout, stderr, retcode = cmd.Issue(timeout=timeout, raise_on_failure=False)
    self._cluster_create_time, self._cluster_ready_time = (
        self._ParseClusterCreateTime(stdout)
    )
    if retcode:
      util.CheckGcloudResponseKnownFailures(stderr, retcode)
      raise errors.Resource.CreationError(stderr)

  @classmethod
  def _ParseClusterCreateTime(
      cls, stdout: str
  ) -> Tuple[Optional[float], Optional[float]]:
    """Parses the cluster create & ready time from a raw API response."""
    try:
      creation_data = json.loads(stdout)
    except json.JSONDecodeError:
      creation_data = {}
    can_parse = creation_data.get('status', {}).get('state') == 'RUNNING'
    status_history = creation_data.get('statusHistory', [])
    can_parse = (
        can_parse
        and len(status_history) == 1
        and status_history[0]['state'] == 'CREATING'
    )
    if not can_parse:
      logging.warning('Unable to parse cluster creation duration.')
      return None, None
    creation_start = (
        cls._ParseTime(status_history[0]['stateStartTime'])
        .replace(tzinfo=datetime.timezone.utc)
        .timestamp()
    )
    creation_end = (
        cls._ParseTime(creation_data['status']['stateStartTime'])
        .replace(tzinfo=datetime.timezone.utc)
        .timestamp()
    )
    return creation_start, creation_end

  def _Delete(self):
    """Deletes the cluster."""
    cmd = self.DataprocGcloudCommand('clusters', 'delete', self.cluster_id)
    stdout, _, _ = cmd.Issue(raise_on_failure=False)
    try:
      response = json.loads(stdout)
    except ValueError:
      return
    status = response.get('metadata', {}).get('status', {})
    if status.get('state') == 'DONE':
      delete_done_start_time_str = status.get('stateStartTime')
      self._cluster_delete_time = (
          self._ParseTime(delete_done_start_time_str)
          .replace(tzinfo=datetime.timezone.utc)
          .timestamp()
      )

  def _GetCluster(self) -> Optional[Dict[str, Any]]:
    """Gets the cluster resource in a dict."""
    cmd = self.DataprocGcloudCommand('clusters', 'describe', self.cluster_id)
    stdout, _, retcode = cmd.Issue(raise_on_failure=False)
    if not retcode:
      return json.loads(stdout)

  def _Exists(self):
    """Checks to see whether the cluster exists."""
    return self._GetCluster() is not None

  def SubmitJob(
      self,
      jarfile=None,
      classname=None,
      pyspark_file=None,
      query_file=None,
      job_poll_interval=None,
      job_stdout_file=None,
      job_arguments=None,
      job_files=None,
      job_jars=None,
      job_py_files=None,
      job_type=None,
      properties=None,
  ):
    """See base class."""
    assert job_type
    args = ['jobs', 'submit', job_type]

    if job_type == dpb_constants.PYSPARK_JOB_TYPE:
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
    if job_py_files:
      cmd.flags['py-files'] = ','.join(job_py_files)

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
          '{}={}'.format(k, v) for k, v in all_properties.items()
      )

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
        pending_time=(start_time - pending_time).total_seconds(),
    )

  def _AddToCmd(self, cmd, cmd_property, cmd_value):
    flag_name = cmd_property
    cmd.flags[flag_name] = cmd_value

  def GetHdfsType(self) -> Optional[str]:
    """Gets human friendly disk type for metric metadata."""
    hdfs_type = None

    if self.spec.worker_group.disk_spec.disk_type:
      hdfs_type = disk_to_hdfs_map[self.spec.worker_group.disk_spec.disk_type]

    # Change to SSD if Local SSDs are specified.
    if self.spec.worker_group.vm_spec.num_local_ssds:
      hdfs_type = 'Local SSD'

    return hdfs_type


class GcpDpbDpgke(GcpDpbDataproc):
  """Dataproc on GKE cluster.

  Extends from GcpDpbDataproc and not GcpDpbBaseDataproc as this represents a
  cluster with managed infrastructure.
  """

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'dataproc_gke'

  def __init__(self, dpb_service_spec):
    super(GcpDpbDpgke, self).__init__(dpb_service_spec)
    required_spec_attrs = [
        'gke_cluster_name',
        'gke_cluster_nodepools',
        'gke_cluster_location',
    ]
    missing_attrs = [
        attr
        for attr in required_spec_attrs
        if not getattr(self.spec, attr, None)
    ]
    if missing_attrs:
      raise errors.Setup.InvalidSetupError(
          f'{missing_attrs} must be provided for provisioning DPGKE.'
      )

  def _Create(self):
    """Creates the dpgke virtual cluster."""
    cmd = self.DataprocGcloudCommand(
        'clusters', 'gke', 'create', self.cluster_id
    )
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
    if self.GetClusterProperties():
      cmd.flags['properties'] = ','.join(self.GetClusterProperties())
    timeout = 900  # 15 min
    logging.info(
        'Issuing command to create dpgke cluster. Flags %s, Args %s',
        cmd.flags,
        cmd.args,
    )
    stdout, stderr, retcode = cmd.Issue(timeout=timeout, raise_on_failure=False)
    self._cluster_create_time, self._cluster_delete_time = (
        self._ParseClusterCreateTime(stdout)
    )
    if retcode:
      util.CheckGcloudResponseKnownFailures(stderr, retcode)
      raise errors.Resource.CreationError(stderr)

  def GetHdfsType(self) -> Optional[str]:
    """Gets human friendly disk type for metric metadata."""
    return None


class GcpDpbDataprocServerless(
    dpb_service.DpbServiceServerlessMixin, GcpDpbBaseDataproc
):
  """Resource that allows spawning serverless Dataproc Jobs."""

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'dataproc_serverless'

  def __init__(self, dpb_service_spec):
    self._dpb_s8s_disk_type = (
        dpb_service_spec.worker_group.disk_spec.disk_type or 'standard'
    )
    # This is to make it work with the default dpb_sparksql_benchmark GCP
    # disk_type.
    if self._dpb_s8s_disk_type == 'pd-standard':
      self._dpb_s8s_disk_type = 'standard'
    super().__init__(dpb_service_spec)
    self._job_counter = 0
    self.batch_name = f'{self.cluster_id}-{self._job_counter}'
    self._FillMetadata()

  def SubmitJob(
      self,
      jarfile=None,
      classname=None,
      pyspark_file=None,
      query_file=None,
      job_poll_interval=None,
      job_stdout_file=None,
      job_arguments=None,
      job_files=None,
      job_jars=None,
      job_py_files=None,
      job_type=None,
      properties=None,
  ):
    """See base class."""
    assert job_type
    args = ['batches', 'submit', job_type]
    additional_args = []

    if job_type == dpb_constants.PYSPARK_JOB_TYPE:
      args.append(pyspark_file)

    cmd = self.DataprocGcloudCommand(*args)

    self.batch_name = f'{self.cluster_id}-{self._job_counter}'
    self._job_counter += 1
    cmd.flags['batch'] = self.batch_name
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
      additional_args += query_file

    if job_files:
      cmd.flags['files'] = ','.join(job_files)
    if job_jars:
      cmd.flags['jars'] = ','.join(job_jars)
    if job_py_files:
      cmd.flags['py-files'] = ','.join(job_py_files)

    if FLAGS.gce_network_name:
      cmd.flags['network'] = FLAGS.gce_network_name[0]

    if self.GetDpbVersion():
      cmd.flags['version'] = self.GetDpbVersion()
    if FLAGS.gcp_dataproc_image:
      cmd.flags['container-image'] = FLAGS.gcp_dataproc_image

    all_properties = self.GetJobProperties()
    all_properties.update(properties or {})
    if all_properties:
      # For commas: https://cloud.google.com/sdk/gcloud/reference/topic/escaping
      cmd.flags['properties'] = '^@^' + '@'.join(
          '{}={}'.format(k, v) for k, v in all_properties.items()
      )

    if job_arguments:
      additional_args += ['--'] + job_arguments
    cmd.additional_flags = additional_args

    _, stderr, retcode = cmd.Issue(timeout=None, raise_on_failure=False)
    if retcode != 0:
      raise dpb_service.JobSubmissionError(stderr)

    fetch_batch_cmd = self.DataprocGcloudCommand(
        'batches', 'describe', self.batch_name
    )
    stdout, stderr, retcode = fetch_batch_cmd.Issue(
        timeout=None, raise_on_failure=False
    )
    if retcode != 0:
      raise dpb_service.JobSubmissionError(stderr)

    results = json.loads(stdout)
    # Otherwise retcode would not have been 0
    assert results['state'] == 'SUCCEEDED'
    done_time = self._ParseTime(results['stateTime'])
    pending_time = None
    start_time = None
    for state in results['stateHistory']:
      if state['state'] == 'PENDING':
        pending_time = self._ParseTime(state['stateStartTime'])
      elif state['state'] == 'RUNNING':
        start_time = self._ParseTime(state['stateStartTime'])

    assert pending_time and start_time and done_time

    return dpb_service.JobResult(
        run_time=(done_time - start_time).total_seconds(),
        pending_time=(start_time - pending_time).total_seconds(),
    )

  def GetJobProperties(self) -> Dict[str, str]:
    result = {}
    if self.spec.dataproc_serverless_core_count:
      result['spark.executor.cores'] = self.spec.dataproc_serverless_core_count
      result['spark.driver.cores'] = self.spec.dataproc_serverless_core_count
    if self.spec.dataproc_serverless_initial_executors:
      result['spark.executor.instances'] = (
          self.spec.dataproc_serverless_initial_executors
      )
    if self.spec.dataproc_serverless_min_executors:
      result['spark.dynamicAllocation.minExecutors'] = (
          self.spec.dataproc_serverless_min_executors
      )
    if self.spec.dataproc_serverless_max_executors:
      result['spark.dynamicAllocation.maxExecutors'] = (
          self.spec.dataproc_serverless_max_executors
      )
    if self.spec.worker_group.disk_spec.disk_size:
      result['spark.dataproc.driver.disk.size'] = (
          f'{self.spec.worker_group.disk_spec.disk_size}g'
      )
      result['spark.dataproc.executor.disk.size'] = (
          f'{self.spec.worker_group.disk_spec.disk_size}g'
      )
    if self.spec.worker_group.disk_spec.disk_type:
      result['spark.dataproc.driver.disk.tier'] = self._dpb_s8s_disk_type
      result['spark.dataproc.executor.disk.tier'] = self._dpb_s8s_disk_type
    if self.spec.dataproc_serverless_memory:
      result['spark.driver.memory'] = f'{self.spec.dataproc_serverless_memory}m'
      result['spark.executor.memory'] = (
          f'{self.spec.dataproc_serverless_memory}m'
      )
    if self.spec.dataproc_serverless_memory_overhead:
      result['spark.driver.memoryOverhead'] = (
          f'{self.spec.dataproc_serverless_memory_overhead}m'
      )
      result['spark.executor.memoryOverhead'] = (
          f'{self.spec.dataproc_serverless_memory_overhead}m'
      )
    result.update(super().GetJobProperties())
    return result

  def _FillMetadata(self) -> None:
    if self.spec.dataproc_serverless_core_count:
      cluster_shape = (
          f'dataproc-serverless-{self.spec.dataproc_serverless_core_count}'
      )
    else:
      cluster_shape = 'dataproc-serverless-default'

    initial_executors = self.spec.dataproc_serverless_initial_executors
    min_executors = self.spec.dataproc_serverless_min_executors
    max_executors = self.spec.dataproc_serverless_max_executors

    cluster_size = None
    if initial_executors == min_executors == max_executors:
      cluster_size = initial_executors

    self.metadata = {
        'dpb_service': self.metadata['dpb_service'],
        'dpb_version': self.metadata['dpb_version'],
        'dpb_service_version': self.metadata['dpb_service_version'],
        'dpb_batch_id': self.metadata['dpb_cluster_id'],
        'dpb_cluster_shape': cluster_shape,
        'dpb_cluster_size': cluster_size,
        'dpb_cluster_min_executors': min_executors,
        'dpb_cluster_max_executors': max_executors,
        'dpb_cluster_initial_executors': initial_executors,
        'dpb_cores_per_node': self.spec.dataproc_serverless_core_count,
        'dpb_memory_per_node': (
            self.spec.dataproc_serverless_memory or 'default'
        ),
        'dpb_memory_overhead_per_node': (
            self.spec.dataproc_serverless_memory_overhead or 'default'
        ),
        'dpb_hdfs_type': self.metadata['dpb_hdfs_type'],
        'dpb_disk_size': self.metadata['dpb_disk_size'],
        'dpb_service_zone': self.metadata['dpb_service_zone'],
        'dpb_job_properties': self.metadata['dpb_job_properties'],
    }

  def CalculateLastJobCost(self):
    fetch_batch_cmd = self.DataprocGcloudCommand(
        'batches', 'describe', self.batch_name
    )

    @vm_util.Retry(
        timeout=180,
        poll_interval=15,
        fuzz=0,
        retryable_exceptions=(MetricNotReadyError,),
    )
    def FetchBatchResults():
      stdout, _, _ = fetch_batch_cmd.Issue(timeout=None, raise_on_failure=False)
      results = json.loads(stdout)
      # If the approximate usage data is not available, sleep and retry
      if (
          'runtimeInfo' not in results
          or 'approximateUsage' not in results['runtimeInfo']
      ):
        raise MetricNotReadyError('Usage metric is not ready')
      return results

    # Pricing may vary based on regions. Only some regions available.
    usd_per_milli_dcu_sec = (
        gcp_dpb_dataproc_serverless_prices.DATAPROC_SERVERLESS_PRICES.get(
            self.region, {}
        ).get('usd_per_milli_dcu_sec')
    )
    usd_per_shuffle_storage_gb_sec = (
        gcp_dpb_dataproc_serverless_prices.DATAPROC_SERVERLESS_PRICES.get(
            self.region, {}
        ).get('usd_per_shuffle_storage_gb_sec')
    )
    if self._dpb_s8s_disk_type == 'premium':
      # prices based on
      # https://cloud.google.com/dataproc-serverless/pricing#data_compute_unit_dcu_pricing
      # TODO(odiego): Get prices for all regions like it's done for standard.
      usd_per_milli_dcu_sec = 0.089 / 1000 / 3600
      usd_per_shuffle_storage_gb_sec = 0.1 / 730 / 3600
    if usd_per_milli_dcu_sec is None or usd_per_shuffle_storage_gb_sec is None:
      return None
    results = FetchBatchResults()
    milli_dcu_seconds = int(
        results['runtimeInfo']['approximateUsage']['milliDcuSeconds']
    )
    shuffle_storage_gb_seconds = int(
        results['runtimeInfo']['approximateUsage']['shuffleStorageGbSeconds']
    )
    cost = (
        usd_per_milli_dcu_sec * milli_dcu_seconds
        + usd_per_shuffle_storage_gb_sec * shuffle_storage_gb_seconds
    )
    return cost

  def GetHdfsType(self) -> Optional[str]:
    """Gets human friendly disk type for metric metadata."""
    try:
      return serverless_disk_to_hdfs_map[self._dpb_s8s_disk_type]
    except KeyError:
      raise errors.Setup.InvalidSetupError(
          f'Invalid disk_type={self._dpb_s8s_disk_type!r} in spec.'
      ) from None


class GcpDpbDataprocFlink(GcpDpbDataproc):
  """Dataproc with Flink component.

  Extends from GcpDpbDataproc and not GcpDpbBaseDataproc as this represents a
  cluster with managed infrastructure.
  """

  CLOUD = provider_info.GCP
  SERVICE_TYPE = 'dataproc_flink'

  def _Create(self):
    # Make flink component installed when using Dataproc for flink jobs
    if self.spec.applications:
      self.spec.applications.append('flink')
    else:
      self.spec.applications = ['flink']
    super()._Create()
    self.ExecuteOnMaster(data.ResourcePath(DATAPROC_FLINK_INIT_SCRIPT), [])

  def SubmitJob(
      self,
      jarfile=None,
      classname=None,
      pyspark_file=None,
      query_file=None,
      job_poll_interval=None,
      job_stdout_file=None,
      job_arguments=None,
      job_files=None,
      job_jars=None,
      job_py_files=None,
      job_type=None,
      properties=None,
  ):
    """See base class."""
    assert job_type in [
        dpb_constants.FLINK_JOB_TYPE,
        dpb_constants.BEAM_JOB_TYPE,
    ], 'Unsupported job type {}'.format(job_type)
    logging.info('Running presubmit script...')
    start_time = datetime.datetime.now()
    self.ExecuteOnMaster(
        data.ResourcePath(DATAPROC_FLINK_PRESUBMIT_SCRIPT), [jarfile]
    )
    presubmit_done_time = datetime.datetime.now()
    logging.info('Submitting beam jobs with flink component enabled.')
    job_script_args = []
    job_script_args.append('-c {}'.format(classname))
    job_script_args.append('--')
    if job_type == dpb_constants.BEAM_JOB_TYPE:
      job_script_args.append('--runner=FlinkRunner')
      job_script_args.extend(job_arguments)
    else:
      job_script_args.extend([arg.replace('=', ' ') for arg in job_arguments])
    self.ExecuteOnMaster(
        data.ResourcePath(DATAPROC_FLINK_TRIGGER_SCRIPT), job_script_args
    )
    done_time = datetime.datetime.now()
    logging.info('Flink job done.')
    return dpb_service.JobResult(
        run_time=(done_time - presubmit_done_time).total_seconds(),
        pending_time=(presubmit_done_time - start_time).total_seconds(),
    )

  def ExecuteOnMaster(self, script_path, script_args):
    master_name = self.cluster_id + '-m'
    script_name = os.path.basename(script_path)
    if FLAGS.gcp_internal_ip:
      scp_cmd = ['gcloud', 'beta', 'compute', 'scp', '--internal-ip']
    else:
      scp_cmd = ['gcloud', 'compute', 'scp']
    if self.project is not None:
      scp_cmd += ['--project', self.project]
    scp_cmd += [
        '--zone',
        self.dpb_service_zone,
        '--quiet',
        script_path,
        'pkb@' + master_name + ':/tmp/' + script_name,
    ]
    vm_util.IssueCommand(scp_cmd)
    ssh_cmd = ['gcloud', 'compute', 'ssh']
    if self.project is not None:
      ssh_cmd += ['--project', self.project]
    if FLAGS.gcp_internal_ip:
      ssh_cmd += ['--internal-ip']
    ssh_cmd += [
        '--zone=' + self.dpb_service_zone,
        '--quiet',
        'pkb@' + master_name,
        '--',
        'chmod +x /tmp/'
        + script_name
        + '; sudo /tmp/'
        + script_name
        + ' '
        + ' '.join(script_args),
    ]
    vm_util.IssueCommand(ssh_cmd, timeout=None)
