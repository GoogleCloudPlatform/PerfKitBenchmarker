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
"""Benchmarking support for Data Processing Backend Services.

In order to benchmark Data Processing Backend services such as Google
Cloud Platform's Dataproc and Dataflow or Amazon's EMR, we create a
BaseDpbService class.  Classes to wrap specific backend services are in
the corresponding provider directory as a subclass of BaseDpbService.
"""

import abc
import dataclasses
import datetime
import logging
import os
import shutil
import tempfile
from typing import Dict, List, Optional, Type

from absl import flags
import jinja2
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import container_service
from perfkitbenchmarker import context
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import units
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import hadoop
from perfkitbenchmarker.linux_packages import spark
from perfkitbenchmarker.providers.aws import s3
from perfkitbenchmarker.providers.aws import util as aws_util
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util as gcp_util
import yaml

# Job types that are supported on the dpb service backends
_PYSPARK_JOB_TYPE = 'pyspark'
_SPARKSQL_JOB_TYPE = 'spark-sql'
_SPARK_JOB_TYPE = 'spark'
_HADOOP_JOB_TYPE = 'hadoop'
_DATAFLOW_JOB_TYPE = 'dataflow'
_BEAM_JOB_TYPE = 'beam'
_FLINK_JOB_TYPE = 'flink'

flags.DEFINE_string(
    'static_dpb_service_instance', None,
    'If set, the name of the pre created dpb implementation,'
    'assumed to be ready.')
flags.DEFINE_string('dpb_log_level', 'INFO', 'Manipulate service log level')
flags.DEFINE_string('dpb_job_jarfile', None,
                    'Executable Jarfile containing workload implementation')
flags.DEFINE_string('dpb_job_classname', None, 'Classname of the job '
                    'implementation in the jar file')
flags.DEFINE_string(
    'dpb_service_bucket', None, 'A bucket to use with the DPB '
    'service. If none is provided one will be created by the '
    'benchmark and cleaned up afterwards unless you are using '
    'a static instance.')
flags.DEFINE_string('dpb_service_zone', None, 'The zone for provisioning the '
                    'dpb_service instance.')
flags.DEFINE_list(
    'dpb_job_properties', [], 'A list of strings of the form '
    '"key=value" to be passed into DPB jobs.')
flags.DEFINE_list(
    'dpb_cluster_properties', [], 'A list of strings of the form '
    '"type:key=value" to be passed into DPB clusters. See '
    'https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/cluster-properties.'
)
flags.DEFINE_float(
    'dpb_job_poll_interval_secs', 5,
    'Poll interval to check submitted job status in seconds. Only applies for '
    'DPB service implementations that do not support synchronous job '
    'submissions (i.e. not Dataproc).',
    lower_bound=0, upper_bound=120)
flags.DEFINE_string(
    'dpb_initialization_actions', None,
    'A comma separated list of Google Cloud Storage URIs of executables to run '
    'on each node in the DPB cluster. See '
    'https://cloud.google.com/sdk/gcloud/reference/dataproc/clusters/create#--initialization-actions.'
)
flags.DEFINE_bool(
    'dpb_export_job_stats', False,
    'Exports job stats such as CPU usage and cost. Enabled by default, but not '
    'necessarily implemented on all services.')
flags.DEFINE_enum(
    'dpb_job_type',
    None,
    [
        _PYSPARK_JOB_TYPE,
        _SPARKSQL_JOB_TYPE,
        _SPARK_JOB_TYPE,
        _HADOOP_JOB_TYPE,
        _BEAM_JOB_TYPE,
        _DATAFLOW_JOB_TYPE,
        _FLINK_JOB_TYPE,
    ],
    'The type of the job to be run on the backends.',
)

FLAGS = flags.FLAGS

# List of supported data processing backend services
DATAPROC = 'dataproc'
DATAPROC_FLINK = 'dataproc_flink'
DATAPROC_GKE = 'dataproc_gke'
DATAPROC_SERVERLESS = 'dataproc_serverless'
DATAFLOW = 'dataflow'
DATAFLOW_TEMPLATE = 'dataflow_template'
EMR = 'emr'
EMR_SERVERLESS = 'emr_serverless'
GLUE = 'glue'
UNMANAGED_DPB_SVC_YARN_CLUSTER = 'unmanaged_dpb_svc_yarn_cluster'
UNMANAGED_SPARK_CLUSTER = 'unmanaged_spark_cluster'
KUBERNETES_SPARK_CLUSTER = 'kubernetes_spark_cluster'
KUBERNETES_FLINK_CLUSTER = 'kubernetes_flink_cluster'
UNMANAGED_SERVICES = [
    UNMANAGED_DPB_SVC_YARN_CLUSTER,
    UNMANAGED_SPARK_CLUSTER,
]

# Default number of workers to be used in the dpb service implementation
DEFAULT_WORKER_COUNT = 2

# List of supported applications that can be enabled on the dpb service
FLINK = 'flink'
HIVE = 'hive'

# Metrics and Status related metadata
# TODO(pclay): Remove these after migrating all callers to SubmitJob
SUCCESS = 'success'
RUNTIME = 'running_time'
WAITING = 'pending_time'


class JobNotCompletedError(Exception):
  """Used to signal a job is still running."""
  pass


class JobSubmissionError(errors.Benchmarks.RunError):
  """Thrown by all implementations if SubmitJob fails."""
  pass


@dataclasses.dataclass
class JobResult:
  """Data class for the timing of a successful DPB job."""
  # Service reported execution time
  run_time: float
  # Service reported pending time (0 if service does not report).
  pending_time: float = 0

  @property
  def wall_time(self) -> float:
    """The total time the service reported it took to execute."""
    return self.run_time + self.pending_time


class BaseDpbService(resource.BaseResource):
  """Object representing a Data Processing Backend Service."""

  REQUIRED_ATTRS = ['CLOUD', 'SERVICE_TYPE']
  RESOURCE_TYPE = 'BaseDpbService'
  CLOUD = 'abstract'
  SERVICE_TYPE = 'abstract'
  HDFS_FS = 'hdfs'
  GCS_FS = 'gs'
  S3_FS = 's3'

  # Job types that are supported on the dpb service backends
  PYSPARK_JOB_TYPE = _PYSPARK_JOB_TYPE
  SPARKSQL_JOB_TYPE = _SPARKSQL_JOB_TYPE
  SPARK_JOB_TYPE = _SPARK_JOB_TYPE
  HADOOP_JOB_TYPE = _HADOOP_JOB_TYPE
  DATAFLOW_JOB_TYPE = _DATAFLOW_JOB_TYPE
  BEAM_JOB_TYPE = _BEAM_JOB_TYPE
  FLINK_JOB_TYPE = _FLINK_JOB_TYPE

  def _JobJars(self) -> Dict[str, Dict[str, str]]:
    """Known mappings of jars in the cluster used by GetExecutionJar."""
    return {
        self.SPARK_JOB_TYPE: {
            # Default for Dataproc and EMR
            'examples': 'file:///usr/lib/spark/examples/jars/spark-examples.jar'
        }
    }

  def __init__(self, dpb_service_spec):
    """Initialize the Dpb service object.

    Args:
      dpb_service_spec: spec of the dpb service.
    """
    is_user_managed = dpb_service_spec.static_dpb_service_instance is not None
    # Hand over the actual creation to the resource module which treats the
    # user_managed resources in a special manner and skips creation attempt
    super(BaseDpbService, self).__init__(user_managed=is_user_managed)
    self.spec = dpb_service_spec
    self.dpb_hdfs_type = None
    if dpb_service_spec.static_dpb_service_instance:
      self.cluster_id = dpb_service_spec.static_dpb_service_instance
    else:
      self.cluster_id = 'pkb-' + FLAGS.run_uri
    if FLAGS.dpb_service_bucket:
      self.bucket = FLAGS.dpb_service_bucket
      self.manage_bucket = False
    else:
      self.bucket = 'pkb-' + FLAGS.run_uri
      self.manage_bucket = True
    self.dpb_service_zone = FLAGS.dpb_service_zone
    self.dpb_version = dpb_service_spec.version
    self.dpb_service_type = 'unknown'
    self.storage_service = None

  @property
  def base_dir(self):
    return self.persistent_fs_prefix + self.bucket  # pytype: disable=attribute-error  # bind-properties

  @abc.abstractmethod
  def SubmitJob(self,
                jarfile: Optional[str] = None,
                classname: Optional[str] = None,
                pyspark_file: Optional[str] = None,
                query_file: Optional[str] = None,
                job_poll_interval: Optional[float] = None,
                job_stdout_file: Optional[str] = None,
                job_arguments: Optional[List[str]] = None,
                job_files: Optional[List[str]] = None,
                job_jars: Optional[List[str]] = None,
                job_py_files: Optional[List[str]] = None,
                job_type: Optional[str] = None,
                properties: Optional[Dict[str, str]] = None) -> JobResult:
    """Submit a data processing job to the backend.

    Args:
      jarfile: Jar file to execute.
      classname: Name of the main class.
      pyspark_file: Comma separated list of Python files to be provided to the
        job. Must be one of the following file formats ".py, .zip, or .egg".
      query_file: HCFS URI of file containing Spark SQL script to execute as the
        job.
      job_poll_interval: number of seconds saying how often to poll for job
        completion. Not used by providers for which submit job is a synchronous
        operation.
      job_stdout_file: String giving the location of the file in which to put
        the standard out of the job.
      job_arguments: List of string arguments to pass to driver application.
        These are not the arguments passed to the wrapper that submits the job.
      job_files: Files passed to a Spark Application to be distributed to
        executors.
      job_jars: Jars to pass to the application.
      job_py_files: Python source files to pass to the application.
      job_type: Spark or Hadoop job
      properties: Dict of properties to pass with the job.

    Returns:
      A JobResult with the timing of the successful job.

    Raises:
      JobSubmissionError if job fails.
    """
    pass

  def _WaitForJob(self, job_id, timeout, poll_interval):

    if poll_interval is None:
      poll_interval = FLAGS.dpb_job_poll_interval_secs

    @vm_util.Retry(
        timeout=timeout,
        poll_interval=poll_interval,
        fuzz=0,
        retryable_exceptions=(JobNotCompletedError,))
    def Poll():
      result = self._GetCompletedJob(job_id)
      if result is None:
        raise JobNotCompletedError('Job {} not complete.'.format(job_id))
      return result

    return Poll()

  def _GetCompletedJob(self, job_id: str) -> Optional[JobResult]:
    """Get the job result if it has finished.

    Args:
      job_id: The step id to query.

    Returns:
      A dictionary describing the job if the step the step is complete,
          None otherwise.

    Raises:
      JobSubmissionError if job fails.
    """
    raise NotImplementedError('You need to implement _GetCompletedJob if you '
                              'use _WaitForJob')

  def GetSparkSubmitCommand(
      self,
      jarfile: Optional[str] = None,
      classname: Optional[str] = None,
      pyspark_file: Optional[str] = None,
      query_file: Optional[str] = None,
      job_arguments: Optional[List[str]] = None,
      job_files: Optional[List[str]] = None,
      job_jars: Optional[List[str]] = None,
      job_type: Optional[str] = None,
      job_py_files: Optional[List[str]] = None,
      properties: Optional[Dict[str, str]] = None,
      spark_submit_cmd: str = spark.SPARK_SUBMIT) -> List[str]:
    """Builds the command to run spark-submit on cluster."""
    # TODO(pclay): support BaseDpbService.SPARKSQL_JOB_TYPE
    if job_type not in [
        BaseDpbService.PYSPARK_JOB_TYPE,
        BaseDpbService.SPARK_JOB_TYPE,
    ]:
      raise NotImplementedError
    cmd = [spark_submit_cmd]
    # Order is important
    if classname:
      cmd += ['--class', classname]
    all_properties = self.GetJobProperties()
    all_properties.update(properties or {})
    for k, v in all_properties.items():
      cmd += ['--conf', '{}={}'.format(k, v)]
    if job_files:
      cmd = ['--files', ','.join(job_files)]
    if job_py_files:
      cmd += ['--py-files', ','.join(job_py_files)]
    # Main jar/script goes last before args.
    if job_type == BaseDpbService.SPARK_JOB_TYPE:
      assert jarfile
      cmd.append(jarfile)
    elif job_type == BaseDpbService.PYSPARK_JOB_TYPE:
      assert pyspark_file
      cmd.append(pyspark_file)
    if job_arguments:
      cmd += job_arguments
    return cmd

  def DistributedCopy(self,
                      source: str,
                      destination: str,
                      properties: Optional[Dict[str, str]] = None) -> JobResult:
    """Method to copy data using a distributed job on the cluster.

    Args:
      source: HCFS directory to copy data from.
      destination: name of new HCFS directory to copy data into.
      properties: properties to add to the job. Not supported on EMR.

    Returns:
      A JobResult with the timing of the successful job.

    Raises:
      JobSubmissionError if job fails.
    """
    return self.SubmitJob(
        classname='org.apache.hadoop.tools.DistCp',
        job_arguments=[source, destination],
        job_type=BaseDpbService.HADOOP_JOB_TYPE,
        properties=properties)

  def GetMetadata(self):
    """Return a dictionary of the metadata for this cluster."""
    pretty_version = self.dpb_version or 'default'
    basic_data = {
        'dpb_service':
            self.dpb_service_type,
        'dpb_version':
            pretty_version,
        'dpb_service_version':
            '{}_{}'.format(self.dpb_service_type, pretty_version),
        'dpb_cluster_id':
            self.cluster_id,
        'dpb_cluster_shape':
            self.spec.worker_group.vm_spec.machine_type,
        'dpb_cluster_size':
            self.spec.worker_count,
        'dpb_hdfs_type':
            self.dpb_hdfs_type,
        'dpb_disk_size':
            self.spec.worker_group.disk_spec.disk_size,
        'dpb_service_zone':
            self.dpb_service_zone,
        'dpb_job_properties':
            ','.join('{}={}'.format(k, v)
                     for k, v in self.GetJobProperties().items()),
        'dpb_cluster_properties':
            ','.join(FLAGS.dpb_cluster_properties),
    }
    return basic_data

  def _CreateDependencies(self):
    """Creates a bucket to use with the cluster."""
    if self.manage_bucket:
      self.storage_service.MakeBucket(self.bucket)

  def _Create(self):
    """Creates the underlying resource."""
    raise NotImplementedError()

  def _DeleteDependencies(self):
    """Deletes the bucket used with the cluster."""
    if self.manage_bucket:
      self.storage_service.DeleteBucket(self.bucket)

  def _Delete(self):
    """Deletes the underlying resource.

    Implementations of this method should be idempotent since it may
    be called multiple times, even if the resource has already been
    deleted.
    """
    raise NotImplementedError()

  def _ProcessWallTime(self, start_time, end_time):
    """Compute the wall time from the given start and end processing time.

    Args:
      start_time: Datetime value when the processing was started.
      end_time: Datetime value when the processing completed.

    Returns:
      Wall time in seconds.

    Raises:
        ValueError: Exception raised when invalid input is provided.
    """
    if start_time > end_time:
      raise ValueError('start_time cannot be later than the end_time')
    return (end_time - start_time).total_seconds()

  def GetJobProperties(self) -> Dict[str, str]:
    """Parse the dpb_job_properties_flag."""
    return dict(pair.split('=') for pair in FLAGS.dpb_job_properties)

  def GetExecutionJar(self, job_category: str, job_type: str) -> str:
    """Retrieve execution jar corresponding to the job_category and job_type.

    Args:
      job_category: String category of the job for eg. hadoop, spark, hive, etc.
      job_type: String name of the type of workload to executed on the cluster,
        for eg. word_count, terasort, etc.

    Returns:
      The path to the execusion jar on the cluster

    Raises:
        NotImplementedError: An unsupported combination of job_category
        and job_type was provided for execution on the cluster.
    """
    jar = self._JobJars().get(job_category, {}).get(job_type)
    if jar:
      return jar
    raise NotImplementedError(
        f'No jar found for category {job_category} and type {job_type}.')

  def GetClusterCreateTime(self) -> Optional[float]:
    """Returns the cluster creation time.

    This default implementation computes it by substracting the
    resource_ready_time and create_start_time attributes.

    Returns:
      A float representing the creation time in seconds or None.
    """
    if self.resource_ready_time is None or self.create_start_time is None:
      return None
    return self.resource_ready_time - self.create_start_time

  def GetServiceWrapperScriptsToUpload(self) -> List[str]:
    """Gets service wrapper scripts to upload alongside benchmark scripts."""
    return []

  def CalculateCost(self) -> Optional[float]:
    """Returns the cost of last job submitted.

    Returns:
      A float representing the cost in dollars or None if not implemented.
    """
    return None


class UnmanagedDpbService(BaseDpbService):
  """Object representing an un-managed dpb service."""

  def __init__(self, dpb_service_spec):
    super(UnmanagedDpbService, self).__init__(dpb_service_spec)
    #  Dictionary to hold the cluster vms.
    self.vms = {}
    self.cloud = dpb_service_spec.worker_group.cloud
    if not self.dpb_service_zone:
      raise errors.Setup.InvalidSetupError(
          'dpb_service_zone must be provided, for provisioning.')
    if self.cloud == 'GCP':
      self.region = gcp_util.GetRegionFromZone(FLAGS.dpb_service_zone)
      self.storage_service = gcs.GoogleCloudStorageService()
      self.persistent_fs_prefix = 'gs://'
    elif self.cloud == 'AWS':
      self.region = aws_util.GetRegionFromZone(FLAGS.dpb_service_zone)
      self.storage_service = s3.S3Service()
      self.persistent_fs_prefix = 's3://'
    else:
      self.region = None
      self.storage_service = None
      self.persistent_fs_prefix = None
      self.manage_bucket = False
      logging.warning(
          'Cloud provider %s does not support object storage. '
          'Some benchmarks will not work.',
          self.cloud)

    if self.storage_service:
      self.storage_service.PrepareService(location=self.region)

    # set in _Create of derived classes
    self.leader = None

  def GetClusterCreateTime(self) -> Optional[float]:
    """Returns the cluster creation time.

    UnmanagedDpbService Create phase doesn't consider actual VM creation, just
    further provisioning. Thus, we need to add the VMs create time to the
    default implementation.

    Returns:
      A float representing the creation time in seconds or None.
    """

    my_create_time = super().GetClusterCreateTime()
    if my_create_time is None:
      return None
    vms = []
    for vm_group in self.vms.values():
      for vm in vm_group:
        vms.append(vm)
    first_vm_create_start_time = min(
        (vm.create_start_time
         for vm in vms
         if vm.create_start_time is not None),
        default=None,
    )
    last_vm_ready_start_time = max(
        (vm.resource_ready_time
         for vm in vms
         if vm.resource_ready_time is not None),
        default=None,
    )
    if first_vm_create_start_time is None or last_vm_ready_start_time is None:
      return None
    return (my_create_time + last_vm_ready_start_time -
            first_vm_create_start_time)


class UnmanagedDpbServiceYarnCluster(UnmanagedDpbService):
  """Object representing an un-managed dpb service yarn cluster."""

  CLOUD = 'Unmanaged'
  SERVICE_TYPE = UNMANAGED_DPB_SVC_YARN_CLUSTER

  def __init__(self, dpb_service_spec):
    super(UnmanagedDpbServiceYarnCluster, self).__init__(dpb_service_spec)
    #  Dictionary to hold the cluster vms.
    self.dpb_service_type = UNMANAGED_DPB_SVC_YARN_CLUSTER
    # Set DPB version as Hadoop version for metadata
    self.dpb_version = hadoop.HadoopVersion()
    self.cloud = dpb_service_spec.worker_group.cloud

  def _Create(self):
    """Create an un-managed yarn cluster."""
    logging.info('Should have created vms by now.')
    logging.info(str(self.vms))

    def InstallHadoop(vm):
      vm.Install('hadoop')
      if self.cloud == 'GCP':
        hadoop.InstallGcsConnector(vm)

    if 'worker_group' not in self.vms:
      raise errors.Resource.CreationError(
          'UnmanagedDpbServiceYarnCluster requires VMs in a worker_group.')
    background_tasks.RunThreaded(
        InstallHadoop, self.vms['worker_group'] + self.vms['master_group']
    )
    self.leader = self.vms['master_group'][0]
    hadoop.ConfigureAndStart(
        self.leader, self.vms['worker_group'], configure_s3=self.cloud == 'AWS')

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
                job_py_files=None,
                job_type=None,
                properties=None):
    """Submit a data processing job to the backend."""
    if job_type != self.HADOOP_JOB_TYPE:
      raise NotImplementedError
    cmd_list = [hadoop.HADOOP_CMD]
    # Order is important
    if jarfile:
      cmd_list += ['jar', jarfile]
    # Specifying classname only works if jarfile is omitted or if it has no
    # main class.
    if classname:
      cmd_list += [classname]
    all_properties = self.GetJobProperties()
    all_properties.update(properties or {})
    cmd_list += ['-D{}={}'.format(k, v) for k, v in all_properties.items()]
    if job_arguments:
      cmd_list += job_arguments
    cmd_string = ' '.join(cmd_list)

    start_time = datetime.datetime.now()
    try:
      stdout, _ = self.leader.RobustRemoteCommand(cmd_string)
    except errors.VirtualMachine.RemoteCommandError as e:
      raise JobSubmissionError() from e
    end_time = datetime.datetime.now()

    if job_stdout_file:
      with open(job_stdout_file, 'w') as f:
        f.write(stdout)
    return JobResult(run_time=(end_time - start_time).total_seconds())

  def _Delete(self):
    pass

  def _GetCompletedJob(self, job_id: str) -> Optional[JobResult]:
    """Submitting Job via SSH is blocking so this is not meaningful."""
    raise NotImplementedError('Submitting Job via SSH is a blocking command.')


class UnmanagedDpbSparkCluster(UnmanagedDpbService):
  """Object representing an un-managed dpb service spark cluster."""

  CLOUD = 'Unmanaged'
  SERVICE_TYPE = UNMANAGED_SPARK_CLUSTER

  def _JobJars(self) -> Dict[str, Dict[str, str]]:
    """Known mappings of jars in the cluster used by GetExecutionJar."""
    return {self.SPARK_JOB_TYPE: {'examples': spark.SparkExamplesJarPath()}}

  def __init__(self, dpb_service_spec):
    super(UnmanagedDpbSparkCluster, self).__init__(dpb_service_spec)
    #  Dictionary to hold the cluster vms.
    self.vms = {}
    self.dpb_service_type = UNMANAGED_SPARK_CLUSTER
    # Set DPB version as Spark version for metadata
    self.dpb_version = f'spark_{spark.SparkVersion()}'
    self.cloud = dpb_service_spec.worker_group.cloud

  def _Create(self):
    """Create an un-managed yarn cluster."""
    logging.info('Should have created vms by now.')
    logging.info(str(self.vms))

    def InstallSpark(vm):
      vm.Install('spark')
      if self.cloud == 'GCP':
        hadoop.InstallGcsConnector(vm)

    if 'worker_group' not in self.vms:
      raise errors.Resource.CreationError(
          'UnmanagedDpbSparkCluster requires VMs in a worker_group.')

    background_tasks.RunThreaded(
        InstallSpark, self.vms['worker_group'] + self.vms['master_group']
    )
    self.leader = self.vms['master_group'][0]
    spark.ConfigureAndStart(
        self.leader, self.vms['worker_group'], configure_s3=self.cloud == 'AWS')

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
                job_py_files=None,
                job_type=None,
                properties=None):
    """Submit a data processing job to the backend."""
    cmd = self.GetSparkSubmitCommand(
        jarfile=jarfile,
        classname=classname,
        pyspark_file=pyspark_file,
        job_arguments=job_arguments,
        job_files=job_files,
        job_jars=job_jars,
        job_py_files=job_py_files,
        job_type=job_type,
        properties=properties)
    start_time = datetime.datetime.now()
    try:
      stdout, _ = self.leader.RobustRemoteCommand(' '.join(cmd))
    except errors.VirtualMachine.RemoteCommandError as e:
      raise JobSubmissionError() from e
    end_time = datetime.datetime.now()

    if job_stdout_file:
      with open(job_stdout_file, 'w') as f:
        f.write(stdout)
    return JobResult(run_time=(end_time - start_time).total_seconds())

  def _Delete(self):
    pass

  def _GetCompletedJob(self, job_id: str) -> Optional[JobResult]:
    """Submitting Job via SSH is blocking so this is not meaningful."""
    raise NotImplementedError('Submitting Job via SSH is a blocking command.')


class KubernetesSparkCluster(BaseDpbService):
  """Object representing a Kubernetes dpb service spark cluster."""

  CLOUD = container_service.KUBERNETES
  SERVICE_TYPE = KUBERNETES_SPARK_CLUSTER

  # Constants to sychronize between YAML and Spark configuration
  # TODO(pclay): Consider setting in YAML
  SPARK_DRIVER_SERVICE = 'spark-driver'
  SPARK_DRIVER_PORT = 4042
  SPARK_K8S_SERVICE_ACCOUNT = 'spark'

  def _JobJars(self) -> Dict[str, Dict[str, str]]:
    """Known mappings of jars in the cluster used by GetExecutionJar."""
    return {self.SPARK_JOB_TYPE: {'examples': spark.SparkExamplesJarPath()}}

  # TODO(odiego): Implement GetClusterCreateTime adding K8s cluster create time

  def __init__(self, dpb_service_spec):
    super().__init__(dpb_service_spec)
    self.dpb_service_type = self.SERVICE_TYPE
    # Set DPB version as Spark version for metadata
    self.dpb_version = 'spark_' + FLAGS.spark_version

    benchmark_spec = context.GetThreadBenchmarkSpec()
    self.k8s_cluster = benchmark_spec.container_cluster
    assert self.k8s_cluster
    assert self.k8s_cluster.CLUSTER_TYPE == container_service.KUBERNETES
    self.cloud = self.k8s_cluster.CLOUD
    self.container_registry = benchmark_spec.container_registry
    assert self.container_registry

    self.spark_drivers = []

    # TODO(pclay): Support overriding image?
    # Corresponds with data/docker/spark directory
    self.image = 'spark'

    if self.cloud == 'GCP':
      self.region = gcp_util.GetRegionFromZone(self.k8s_cluster.zone)
      self.storage_service = gcs.GoogleCloudStorageService()
      self.persistent_fs_prefix = 'gs://'
    elif self.cloud == 'AWS':
      self.region = self.k8s_cluster.region
      self.storage_service = s3.S3Service()
      self.persistent_fs_prefix = 's3://'
    else:
      raise errors.Config.InvalidValue(
          f'Unsupported Cloud provider {self.cloud}')

    self.storage_service.PrepareService(location=self.region)

    # TODO(pclay): support
    assert not FLAGS.dpb_cluster_properties

    if self.k8s_cluster.num_nodes < 2:
      raise errors.Config.InvalidValue(
          f'Cluster type {KUBERNETES_SPARK_CLUSTER} requires at least 2 nodes.'
          f'Found {self.k8s_cluster.num_nodes}.')

  def _Create(self):
    """Create docker image for cluster."""
    logging.info('Should have created k8s cluster by now.')
    # TODO(pclay): Should resources publicly declare they have been created?
    assert self.k8s_cluster.resource_ready_time
    assert self.container_registry.resource_ready_time
    logging.info(self.k8s_cluster)
    logging.info(self.container_registry)

    logging.info('Building Spark image.')
    self.image = self.container_registry.GetOrBuild(self.image)

    # https://spark.apache.org/docs/latest/running-on-kubernetes.html#rbac
    # TODO(pclay): Consider moving into manifest
    self.k8s_cluster.CreateServiceAccount(
        self.SPARK_K8S_SERVICE_ACCOUNT, clusterrole='edit')

  def _GetDriverName(self):
    return f'spark-driver-{len(self.spark_drivers)}'

  # Kubernetes unlike other Spark Shedulers reserves 40% of memory for PySpark
  # instead of the normal 10%. We don't need much memory for PySpark, because
  # our PySpark is 100% SQL and thus on the JVM (in dpb_sparksql_benchmark).
  # Force Spark to reserve the normal 10% overhead in all cases.
  # https://spark.apache.org/docs/latest/running-on-kubernetes.html#spark-properties
  MEMORY_OVERHEAD_FACTOR = 0.1

  def GetJobProperties(self) -> Dict[str, str]:
    node_cpu = self.k8s_cluster.node_num_cpu
    # TODO(pclay): Validate that we don't have too little memory?
    node_memory_mb = self.k8s_cluster.node_memory_allocatable.m_as(
        units.mebibyte)
    # Reserve 512 MB for system daemons
    node_memory_mb -= 512
    # Remove overhead
    node_memory_mb /= (1 + self.MEMORY_OVERHEAD_FACTOR)
    node_memory_mb = int(node_memory_mb)

    # Common PKB Spark cluster properties
    properties = spark.GetConfiguration(
        driver_memory_mb=node_memory_mb,
        worker_memory_mb=node_memory_mb,
        # Schedule one thread per vCPU
        worker_cores=node_cpu,
        # Reserve one node for driver
        num_workers=self.k8s_cluster.num_nodes - 1,
        configure_s3=self.cloud == 'AWS')
    # k8s specific properties
    properties.update({
        'spark.driver.host':
            self.SPARK_DRIVER_SERVICE,
        'spark.driver.port':
            str(self.SPARK_DRIVER_PORT),
        'spark.kubernetes.driver.pod.name':
            self._GetDriverName(),
        # Tell Spark to under-report cores by 1 to fit next to k8s services
        'spark.kubernetes.executor.request.cores':
            str(node_cpu - 1),
        'spark.kubernetes.container.image':
            self.image,
        # No HDFS available
        'spark.hadoop.fs.defaultFS':
            self.base_dir,
        'spark.kubernetes.memoryOverheadFactor':
            str(self.MEMORY_OVERHEAD_FACTOR),
    })
    # User specified properties
    properties.update(super().GetJobProperties())
    return properties

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
                job_py_files=None,
                job_type=None,
                properties=None):
    """Submit a data processing job to the backend."""
    # Specs can't be copied or created by hand. So we override the command of
    # the spec for each job.
    command = self.GetSparkSubmitCommand(
        jarfile=jarfile,
        classname=classname,
        pyspark_file=pyspark_file,
        job_arguments=job_arguments,
        job_files=job_files,
        job_jars=job_jars,
        job_py_files=job_py_files,
        job_type=job_type,
        properties=properties)
    driver_name = self._GetDriverName()
    # Request memory for driver. This should guarantee that driver does not get
    # scheduled on same VM as exectutor and OOM.
    driver_memory_mb = int(
        self.GetJobProperties()[spark.SPARK_DRIVER_MEMORY].strip('m'))
    start_time = datetime.datetime.now()
    self.k8s_cluster.ApplyManifest(
        'container/spark/spark-driver.yaml.j2',
        name=driver_name,
        command=command,
        driver_memory_mb=driver_memory_mb,
        driver_port=self.SPARK_DRIVER_PORT,
        driver_service=self.SPARK_DRIVER_SERVICE,
        image=self.image,
        service_account=self.SPARK_K8S_SERVICE_ACCOUNT)
    container = container_service.KubernetesPod(driver_name)
    # increments driver_name for next job
    self.spark_drivers.append(container)
    try:
      container.WaitForExit()
    except container_service.ContainerException as e:
      raise JobSubmissionError() from e
    end_time = datetime.datetime.now()

    if job_stdout_file:
      with open(job_stdout_file, 'w') as f:
        f.write(container.GetLogs())

    # TODO(pclay): use k8s output for timing?
    return JobResult(run_time=(end_time - start_time).total_seconds())

  def _Delete(self):
    pass

  def _GetCompletedJob(self, job_id: str) -> Optional[JobResult]:
    """container.WaitForExit is blocking so this is not meaningful."""
    raise NotImplementedError('container.WaitForExit is a blocking command.')


class KubernetesFlinkCluster(BaseDpbService):
  """Object representing a Kubernetes dpb service flink cluster."""

  CLOUD = container_service.KUBERNETES
  SERVICE_TYPE = KUBERNETES_FLINK_CLUSTER

  FLINK_JOB_MANAGER_SERVICE = 'flink-jobmanager'
  DEFAULT_FLINK_IMAGE = 'flink'

  def __init__(self, dpb_service_spec):
    super().__init__(dpb_service_spec)
    self.dpb_service_type = self.SERVICE_TYPE
    self.dpb_version = dpb_service_spec.version or self.DEFAULT_FLINK_IMAGE

    benchmark_spec = context.GetThreadBenchmarkSpec()
    self.k8s_cluster = benchmark_spec.container_cluster
    assert self.k8s_cluster
    assert self.k8s_cluster.CLUSTER_TYPE == container_service.KUBERNETES
    self.cloud = self.k8s_cluster.CLOUD
    self.container_registry = benchmark_spec.container_registry
    assert self.container_registry

    self.flink_jobmanagers = []
    self.image = self.DEFAULT_FLINK_IMAGE

    if self.cloud == 'GCP':
      self.region = gcp_util.GetRegionFromZone(self.k8s_cluster.zone)
      self.storage_service = gcs.GoogleCloudStorageService()
      self.persistent_fs_prefix = 'gs://'
    else:
      raise errors.Config.InvalidValue(
          f'Unsupported Cloud provider {self.cloud}')

    self.storage_service.PrepareService(location=self.region)

    if self.k8s_cluster.num_nodes < 2:
      raise errors.Config.InvalidValue(
          f'Cluster type {KUBERNETES_FLINK_CLUSTER} requires at least 2 nodes.'
          f'Found {self.k8s_cluster.num_nodes}.')

  def _CreateConfigMapDir(self):
    """Returns a TemporaryDirectory containing files in the ConfigMap."""
    temp_directory = tempfile.TemporaryDirectory()
    # Create flink-conf.yaml to configure flink.
    flink_conf_filename = os.path.join(temp_directory.name, 'flink-conf.yaml')
    with open(flink_conf_filename, 'w') as flink_conf_file:
      yaml.dump(
          self.GetJobProperties(), flink_conf_file, default_flow_style=False
      )
      flink_conf_file.close()
    # Create log4j-console.properties for logging configuration.
    logging_property_file = data.ResourcePath(
        'container/flink/log4j-console.properties'
    )
    logging_property_filename = os.path.join(
        temp_directory.name, 'log4j-console.properties'
    )
    shutil.copyfile(logging_property_file, logging_property_filename)
    return temp_directory

  def _GenerateConfig(self, config_file, **kwargs):
    """Returns a temporary config file."""
    filename = data.ResourcePath(config_file)
    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    with open(filename) as template_file, vm_util.NamedTemporaryFile(
        mode='w', suffix='.yaml', dir=vm_util.GetTempDir(), delete=False
    ) as rendered_template:
      config = environment.from_string(template_file.read()).render(kwargs)
      rendered_template.write(config)
      rendered_template.close()
      logging.info('Finish generating config file %s', rendered_template.name)
      return rendered_template.name

  def _Create(self):
    logging.info('Should have created k8s cluster by now.')
    # Create docker image for containers
    assert self.k8s_cluster.resource_ready_time
    assert self.container_registry.resource_ready_time
    logging.info(self.k8s_cluster)
    logging.info(self.container_registry)

    logging.info('Building Flink image.')
    if FLAGS.container_remote_build_config is None:
      FLAGS.container_remote_build_config = self._GenerateConfig(
          'docker/flink/cloudbuild.yaml.j2',
          dpb_job_jarfile=FLAGS.dpb_job_jarfile,
          base_image=self.dpb_version,
          full_tag=self.container_registry.GetFullRegistryTag(self.image),
      )
    self.image = self.container_registry.GetOrBuild(self.image)

    logging.info('Configuring Kubernetes Flink Cluster.')
    with self._CreateConfigMapDir() as config_dir:
      self.k8s_cluster.CreateConfigMap('default-config', config_dir)

  def _GetJobManagerName(self):
    return f'flink-jobmanager-{len(self.flink_jobmanagers)}'

  def GetJobProperties(self) -> Dict[str, str]:
    node_cpu = self.k8s_cluster.node_num_cpu
    node_memory_mb = self.k8s_cluster.node_memory_allocatable.m_as(
        units.mebibyte)
    # Reserve 512 MB for system daemons
    node_memory_mb -= 512
    node_memory_mb = int(node_memory_mb)

    # Common flink properties
    properties = {
        'jobmanager.rpc.address': self.FLINK_JOB_MANAGER_SERVICE,
        'blob.server.port': 6124,
        'jobmanager.rpc.port': 6123,
        'taskmanager.rpc.port': 6122,
        'queryable-state.proxy.ports': 6125,
        'jobmanager.memory.process.size': f'{node_memory_mb}m',
        'taskmanager.memory.process.size': f'{node_memory_mb}m',
        'taskmanager.numberOfTaskSlots': node_cpu,
        'parallelism.default': node_cpu * (self.k8s_cluster.num_nodes - 1),
        'execution.attached': True,
    }
    # User specified properties
    properties.update(super().GetJobProperties())
    return properties

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
    """Submit a data processing job to the backend."""
    job_manager_name = self._GetJobManagerName()
    job_properties = self.GetJobProperties()
    start_time = datetime.datetime.now()
    self.k8s_cluster.ApplyManifest(
        'container/flink/flink-job-and-deployment.yaml.j2',
        job_manager_name=job_manager_name,
        job_manager_service=self.FLINK_JOB_MANAGER_SERVICE,
        classname=classname,
        job_arguments=','.join(job_arguments),
        image=self.image,
        task_manager_replicas=self.k8s_cluster.num_nodes - 1,
        task_manager_rpc_port=job_properties.get('taskmanager.rpc.port'),
        job_manager_rpc_port=job_properties.get('jobmanager.rpc.port'),
        blob_server_port=job_properties.get('blob.server.port'),
        queryable_state_proxy_ports=job_properties.get(
            'queryable-state.proxy.ports'
        ),
    )
    stdout, _, _ = container_service.RunKubectlCommand(
        ['get', 'pod', f'--selector=job-name={job_manager_name}', '-o', 'yaml']
    )
    pods = yaml.safe_load(stdout)['items']
    if len(pods) <= 0:
      raise JobSubmissionError('No pod was created for the job.')
    container = container_service.KubernetesPod(pods[0]['metadata']['name'])
    self.flink_jobmanagers.append(container)
    try:
      container.WaitForExit()
    except container_service.ContainerException as e:
      raise JobSubmissionError() from e
    end_time = datetime.datetime.now()

    if job_stdout_file:
      with open(job_stdout_file, 'w') as f:
        f.write(container.GetLogs())

    return JobResult(run_time=(end_time - start_time).total_seconds())

  @staticmethod
  def CheckPrerequisites(benchmark_config):
    del benchmark_config  # Unused
    # Make sure dpb_job_jarfile is provided when using flink
    assert FLAGS.dpb_job_jarfile
    # dpb_cluster_properties is to be supported
    assert not FLAGS.dpb_cluster_properties

  def _GetCompletedJob(self, job_id: str) -> Optional[JobResult]:
    """container.WaitForExit is blocking so this is not meaningful."""
    raise NotImplementedError('container.WaitForExit is a blocking command.')

  def _Delete(self):
    pass


def GetDpbServiceClass(cloud: str,
                       dpb_service_type: str) -> Optional[Type[BaseDpbService]]:
  """Gets the Data Processing Backend class corresponding to 'service_type'.

  Args:
    cloud: String name of cloud of the service
    dpb_service_type: String service type as specified in configuration

  Returns:
    Implementation class corresponding to the argument dpb_service_type

  Raises:
    Exception: An invalid data processing backend service type was provided
  """
  if dpb_service_type in UNMANAGED_SERVICES:
    cloud = 'Unmanaged'
  elif dpb_service_type in [KUBERNETES_SPARK_CLUSTER, KUBERNETES_FLINK_CLUSTER]:
    cloud = container_service.KUBERNETES
  return resource.GetResourceClass(
      BaseDpbService, CLOUD=cloud, SERVICE_TYPE=dpb_service_type)
