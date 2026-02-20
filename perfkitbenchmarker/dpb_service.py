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
from collections.abc import Callable, MutableMapping
import dataclasses
import datetime
import logging
import os
import shutil
import tempfile
from typing import Any, Dict, List, Type, TypeAlias
from urllib import parse

from absl import flags
import jinja2
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import context
from perfkitbenchmarker import data
from perfkitbenchmarker import dpb_constants
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker import units
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import hadoop
from perfkitbenchmarker.linux_packages import spark
from perfkitbenchmarker.providers.aws import flags as aws_flags
from perfkitbenchmarker.providers.aws import s3
from perfkitbenchmarker.providers.aws import util as aws_util
from perfkitbenchmarker.providers.gcp import gcs
from perfkitbenchmarker.providers.gcp import util as gcp_util
from perfkitbenchmarker.resources.container_service import container as container_lib
from perfkitbenchmarker.resources.container_service import errors as container_errors
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes
from perfkitbenchmarker.resources.container_service import kubernetes_commands
import yaml

flags.DEFINE_string(
    'static_dpb_service_instance',
    None,
    'If set, the name of the pre created dpb implementation,'
    'assumed to be ready.',
)
flags.DEFINE_string('dpb_log_level', 'INFO', 'Manipulate service log level')
flags.DEFINE_string(
    'dpb_job_jarfile',
    None,
    'Executable Jarfile containing workload implementation',
)
flags.DEFINE_string(
    'dpb_job_classname',
    None,
    'Classname of the job implementation in the jar file',
)
flags.DEFINE_string(
    'dpb_storage_uri',
    None,
    'An object storage location where the DPB service will upload files'
    ' required for running the benchmark, such as scripts and queries. If set,'
    " then files will be staged in a folder named after PKB's run_uri at the"
    ' given location. If none is provided, a bucket will be created by the'
    ' benchmark and cleaned up afterwards.',
)
flags.DEFINE_string(
    'dpb_service_zone',
    None,
    'The zone for provisioning the dpb_service instance.',
)
flags.DEFINE_list(
    'dpb_job_properties',
    [],
    'A list of strings of the form "key=value" to be passed into DPB jobs.',
)
flags.DEFINE_list(
    'dpb_cluster_properties',
    [],
    'A list of strings of the form '
    '"type:key=value" to be passed into DPB clusters. See '
    'https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/cluster-properties.',
)
flags.DEFINE_float(
    'dpb_job_poll_interval_secs',
    5,
    'Poll interval to check submitted job status in seconds. Only applies for '
    'DPB service implementations that do not support synchronous job '
    'submissions (i.e. not Dataproc).',
    lower_bound=0,
    upper_bound=120,
)
flags.DEFINE_string(
    'dpb_initialization_actions',
    None,
    'A comma separated list of Google Cloud Storage URIs of executables to run '
    'on each node in the DPB cluster. See '
    'https://cloud.google.com/sdk/gcloud/reference/dataproc/clusters/create#--initialization-actions.',
)
flags.DEFINE_bool(
    'dpb_export_job_stats',
    False,
    'Exports job stats such as CPU usage and cost. Disabled by default and not '
    'necessarily implemented on all services.',
)
flags.DEFINE_enum(
    'dpb_job_type',
    None,
    [
        dpb_constants.PYSPARK_JOB_TYPE,
        dpb_constants.SPARKSQL_JOB_TYPE,
        dpb_constants.SPARK_JOB_TYPE,
        dpb_constants.HADOOP_JOB_TYPE,
        dpb_constants.BEAM_JOB_TYPE,
        dpb_constants.DATAFLOW_JOB_TYPE,
        dpb_constants.FLINK_JOB_TYPE,
    ],
    'The type of the job to be run on the backends.',
)
_HARDWARE_HOURLY_COST = flags.DEFINE_float(
    'dpb_hardware_hourly_cost',
    None,
    'Hardware hourly USD cost of running the DPB cluster. Set it along with '
    '--dpb_service_premium_hourly_cost to publish cost estimate metrics.',
)
_SERVICE_PREMIUM_HOURLY_COST = flags.DEFINE_float(
    'dpb_service_premium_hourly_cost',
    None,
    'Hardware hourly USD cost of running the DPB cluster. Set it along with '
    '--dpb_hardware_hourly_cost to publish cost estimate metrics.',
)
_DYNAMIC_ALLOCATION = flags.DEFINE_bool(
    'dpb_dynamic_allocation',
    True,
    'True by default. Set it to False to disable dynamic allocation and assign '
    'all cluster executors to an incoming job. Setting this off is only '
    'supported by Dataproc and EMR (non-serverless versions).',
)
DPB_EXTRA_JARS = flags.DEFINE_list(
    'dpb_extra_jars',
    [],
    'Pass additional object storage paths to jars to be loaded when submitting'
    ' a job. Only supported on Dataproc as of now.',
)
SPARK_EVENT_LOG_DIR = flags.DEFINE_string(
    'dpb_spark_event_logs',
    None,
    'Path where Spark event logs will be exported to if set. Can be either a'
    " full Hadoop URI or a relative path to cluster's base_dir (which can be"
    ' set with --dpb_storage_uri).',
)

FLAGS = flags.FLAGS


class JobNotCompletedError(Exception):
  """Used to signal a job is still running."""

  pass


class JobSubmissionError(errors.Benchmarks.RunError):
  """Thrown by all implementations if SubmitJob fails."""

  pass


FetchOutputFn: TypeAlias = Callable[[], tuple[str | None, str | None]]


@dataclasses.dataclass
class JobResult:
  """Data class for the timing of a successful DPB job.

  Attributes:
    run_time: Service reported execution time.
    pending_time: Service reported pending time (0 if service does not report).
    stdout: Job's stdout. Call FetchOutput before to ensure it's populated.
    stderr: Job's stderr. Call FetchOutput before to ensure it's populated.
    fetch_output_fn: Callback expected to return a 2-tuple of str or None whose
      values correspond to stdout and stderr respectively. This is called by
      FetchOutput which updates stdout and stderr if their respective value in
      this callback's return tuple is not None. Defaults to a no-op.
  """

  run_time: float
  pending_time: float = 0
  stdout: str = ''
  stderr: str = ''
  fetch_output_fn: FetchOutputFn = lambda: (None, None)

  def FetchOutput(self):
    """Populates stdout and stderr according to fetch_output_fn callback."""
    stdout, stderr = self.fetch_output_fn()
    if stdout is not None:
      self.stdout = stdout
    if stderr is not None:
      self.stderr = stderr

  @property
  def wall_time(self) -> float:
    """The total time the service reported it took to execute."""
    return self.run_time + self.pending_time


@dataclasses.dataclass
class JobCosts:
  """Contains cost stats for a job execution on a serverless DPB service.

  Attributes:
    total_cost: Total costs incurred by the job.
    compute_cost: Compute costs incurred by the job. RAM may be billed as a
      different item on some DPB service implementations.
    memory_cost: RAM costs incurred by the job if available.
    storage_cost: Shuffle storage costs incurred by the job.
    compute_units_used: Compute units consumed by the job.
    memory_units_used: RAM units consumed by this job.
    storage_units_used: Shuffle storage units consumed by the job.
    compute_unit_cost: Cost of 1 compute unit.
    memory_unit_cost: Cost of 1 memory unit.
    storage_unit_cost: Cost of 1 shuffle storage unit.
    compute_unit_name: Name of the compute units used by the service.
    memory_unit_name: Name of the memory units used by the service.
    storage_unit_name: Name of the shuffle storage units used by the service.
  """

  total_cost: float | None = None

  compute_cost: float | None = None
  memory_cost: float | None = None
  storage_cost: float | None = None

  compute_units_used: float | None = None
  memory_units_used: float | None = None
  storage_units_used: float | None = None

  compute_unit_cost: float | None = None
  memory_unit_cost: float | None = None
  storage_unit_cost: float | None = None

  compute_unit_name: str | None = None
  memory_unit_name: str | None = None
  storage_unit_name: str | None = None

  def GetSamples(
      self,
      prefix: str = '',
      renames: dict[str, str] | None = None,
      metadata: MutableMapping[str, str] | None = None,
  ) -> list[sample.Sample]:
    """Gets PKB samples for these costs.

    Args:
      prefix: Add a prefix before the samples name if passed.
      renames: Optional dict arg where each entry represents a metric rename,
        being the key the original name and the value the new name. prefix is
        added after the rename.
      metadata: String mapping with the metadata for each benchmark.

    Returns:
      A list of samples to be published.
    """

    if renames is None:
      renames = {}
    if metadata is None:
      metadata = {}

    def GetName(original_name):
      return f'{prefix}{renames.get(original_name) or original_name}'

    metrics = [
        ('total_cost', self.total_cost, '$'),
        ('compute_cost', self.compute_cost, '$'),
        ('memory_cost', self.memory_cost, '$'),
        ('storage_cost', self.storage_cost, '$'),
        ('compute_units_used', self.compute_units_used, self.compute_unit_name),
        ('memory_units_used', self.memory_units_used, self.memory_unit_name),
        ('storage_units_used', self.storage_units_used, self.storage_unit_name),
        (
            'compute_unit_cost',
            self.compute_unit_cost,
            f'$/({self.compute_unit_name})',
        ),
        (
            'memory_unit_cost',
            self.memory_unit_cost,
            f'$/({self.memory_unit_name})',
        ),
        (
            'storage_unit_cost',
            self.storage_unit_cost,
            f'$/({self.storage_unit_name})',
        ),
    ]

    results = []
    for metric_name, value, unit in metrics:
      if value is not None and unit is not None:
        results.append(
            sample.Sample(GetName(metric_name), value, unit, metadata)
        )
    return results


class BaseDpbService(resource.BaseResource):
  """Object representing a Data Processing Backend Service."""

  REQUIRED_ATTRS = ['CLOUD', 'SERVICE_TYPE']
  RESOURCE_TYPE = 'BaseDpbService'
  CLOUD = 'abstract'
  SERVICE_TYPE = 'abstract'
  HDFS_FS = dpb_constants.HDFS_FS
  GCS_FS = dpb_constants.GCS_FS
  S3_FS = dpb_constants.S3_FS

  SUPPORTS_NO_DYNALLOC = False

  def _JobJars(self) -> Dict[str, Dict[str, str]]:
    """Known mappings of jars in the cluster used by GetExecutionJar."""
    return {
        dpb_constants.SPARK_JOB_TYPE: {
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
    super().__init__(user_managed=is_user_managed)
    self.spec = dpb_service_spec
    if dpb_service_spec.static_dpb_service_instance:
      self.cluster_id = dpb_service_spec.static_dpb_service_instance
    else:
      self.cluster_id = 'pkb-' + FLAGS.run_uri
    if FLAGS.dpb_storage_uri:
      self.manage_bucket = False
    else:
      self._bucket = 'pkb-' + FLAGS.run_uri
      self.manage_bucket = True
    self.dpb_service_zone = FLAGS.dpb_service_zone
    self.dpb_service_type = self.SERVICE_TYPE
    self.storage_service = None
    if not self.SUPPORTS_NO_DYNALLOC and not _DYNAMIC_ALLOCATION.value:
      raise errors.Setup.InvalidFlagConfigurationError(
          'Dynamic allocation off is not supported for the current DPB '
          f'Service: {type(self).__name__}.'
      )
    self.cluster_duration = None
    self._InitializeMetadata()

  def GetDpbVersion(self) -> str | None:
    return self.spec.version

  def GetHdfsType(self) -> str | None:
    """Gets human friendly disk type for metric metadata."""
    return None

  @property
  @abc.abstractmethod
  def persistent_fs_prefix(self) -> str | None:
    """The prefix for the persistent filesystem."""
    raise NotImplementedError()

  @property
  def base_dir(self):
    if FLAGS.dpb_storage_uri:
      return os.path.join(FLAGS.dpb_storage_uri, 'pkb-' + FLAGS.run_uri)
    return self.persistent_fs_prefix + self._bucket

  @abc.abstractmethod
  def SubmitJob(
      self,
      jarfile: str | None = None,
      classname: str | None = None,
      pyspark_file: str | None = None,
      query_file: str | None = None,
      job_poll_interval: float | None = None,
      job_stdout_file: str | None = None,
      job_arguments: List[str] | None = None,
      job_files: List[str] | None = None,
      job_jars: List[str] | None = None,
      job_py_files: List[str] | None = None,
      job_type: str | None = None,
      properties: Dict[str, str] | None = None,
  ) -> JobResult:
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
        retryable_exceptions=(JobNotCompletedError,),
    )
    def Poll():
      result = self._GetCompletedJob(job_id)
      if result is None:
        raise JobNotCompletedError('Job {} not complete.'.format(job_id))
      return result

    return Poll()

  def _GetCompletedJob(self, job_id: str) -> JobResult | None:
    """Get the job result if it has finished.

    Args:
      job_id: The step id to query.

    Returns:
      A dictionary describing the job if the step the step is complete,
          None otherwise.

    Raises:
      JobSubmissionError if job fails.
    """
    raise NotImplementedError(
        'You need to implement _GetCompletedJob if you use _WaitForJob'
    )

  def GetSparkSubmitCommand(
      self,
      jarfile: str | None = None,
      classname: str | None = None,
      pyspark_file: str | None = None,
      query_file: str | None = None,
      job_arguments: List[str] | None = None,
      job_files: List[str] | None = None,
      job_jars: List[str] | None = None,
      job_type: str | None = None,
      job_py_files: List[str] | None = None,
      properties: Dict[str, str] | None = None,
      spark_submit_cmd: str = spark.SPARK_SUBMIT,
  ) -> List[str]:
    """Builds the command to run spark-submit on cluster."""
    # TODO(pclay): support dpb_constants.SPARKSQL_JOB_TYPE
    if job_type not in [
        dpb_constants.PYSPARK_JOB_TYPE,
        dpb_constants.SPARK_JOB_TYPE,
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
    if job_type == dpb_constants.SPARK_JOB_TYPE:
      assert jarfile
      cmd.append(jarfile)
    elif job_type == dpb_constants.PYSPARK_JOB_TYPE:
      assert pyspark_file
      cmd.append(pyspark_file)
    if job_arguments:
      cmd += job_arguments
    return cmd

  def DistributedCopy(
      self,
      source: str,
      destination: str,
      properties: Dict[str, str] | None = None,
  ) -> JobResult:
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
        job_type=dpb_constants.HADOOP_JOB_TYPE,
        properties=properties,
    )

  def _InitializeMetadata(self) -> None:
    pretty_version = self.GetDpbVersion()
    self.metadata = {
        'dpb_service': self.dpb_service_type,
        'dpb_version': pretty_version,
        'dpb_service_version': '{}_{}'.format(
            self.dpb_service_type, pretty_version
        ),
        'dpb_cluster_id': self.cluster_id,
        'dpb_cluster_shape': self.spec.worker_group.vm_spec.machine_type,
        'dpb_cluster_size': self.spec.worker_count,
        'dpb_hdfs_type': self.GetHdfsType(),
        'dpb_disk_size': self.spec.worker_group.disk_spec.disk_size,
        'dpb_service_zone': self.dpb_service_zone,
        'dpb_job_properties': ','.join(
            '{}={}'.format(k, v) for k, v in self.GetJobProperties().items()
        ),
        'dpb_cluster_properties': ','.join(self.GetClusterProperties()),
        'dpb_dynamic_allocation': _DYNAMIC_ALLOCATION.value,
        'dpb_extra_jars': ','.join(DPB_EXTRA_JARS.value),
    }
    if FLAGS.dpb_storage_uri:  # Else this is a tmp location not worth exporting
      self.metadata['dpb_base_dir'] = self.base_dir
    if SPARK_EVENT_LOG_DIR.value:
      self.metadata['dpb_spark_event_log_dir'] = parse.urljoin(
          self.base_dir, SPARK_EVENT_LOG_DIR.value
      )

  def _CreateDependencies(self):
    """Creates a bucket to use with the cluster."""
    if self.manage_bucket:
      if self.storage_service is None:
        raise ValueError('storage_service is None. Initialize before use.')
      self.storage_service.MakeBucket(self._bucket)

  def _Create(self):
    """Creates the underlying resource."""
    raise NotImplementedError()

  def _DeleteDependencies(self):
    """Deletes the bucket used with the cluster."""
    if self.manage_bucket:
      if self.storage_service is None:
        raise ValueError('storage_service is None. Initialize before use.')
      self.storage_service.DeleteBucket(self._bucket)

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

  def GetClusterProperties(self) -> list[str]:
    """Gets cluster props in the format of the dpb_cluster_properties flag.

    Note that this might return an empty list if both --dpb_dynamic_allocation
    and --dpb_cluster_properties are unset.

    Returns:
      A list of cluster properties, with each element being in the same format
      the --dpb_cluster_properties flag uses.
    """
    properties = []
    if not _DYNAMIC_ALLOCATION.value:
      properties.extend([
          'spark:spark.executor.instances=9999',
          'spark:spark.dynamicAllocation.enabled=false',
      ])
    properties.extend(FLAGS.dpb_cluster_properties)
    return properties

  def GetJobProperties(self) -> Dict[str, str]:
    """Parse the dpb_job_properties_flag."""
    job_props = {}
    if SPARK_EVENT_LOG_DIR.value:
      job_props['spark.eventLog.enabled'] = 'true'
      job_props['spark.eventLog.dir'] = parse.urljoin(
          self.base_dir, SPARK_EVENT_LOG_DIR.value
      )
    job_props.update(dict(pair.split('=') for pair in FLAGS.dpb_job_properties))
    return job_props

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
        f'No jar found for category {job_category} and type {job_type}.'
    )

  def GetClusterCreateTime(self) -> float | None:
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

  def CalculateLastJobCosts(self) -> JobCosts:
    """Returns the cost of last job submitted.

    Returns:
      A dpb_service.JobCosts object.
    """
    return JobCosts()

  def GetClusterDuration(self) -> float | None:
    """Gets how much time the cluster has been running in seconds.

    This default implementation just returns None. Override in subclasses if
    needed.

    Returns:
      A float representing the number of seconds the cluster has been running or
      None if it cannot be obtained.
    """
    return self.cluster_duration

  def GetClusterCost(self) -> float | None:
    """Gets the cost of running the cluster if applicable.

    Default implementation returns the sum of cluster hardware cost and service
    premium cost.

    Guaranteed to be called after the cluster has been shut down if applicable.

    Returns:
      A float representing the cost in dollars or None if not implemented.
    """
    hardware_cost = self.GetClusterHardwareCost()
    premium_cost = self.GetClusterPremiumCost()
    if hardware_cost is None or premium_cost is None:
      return None
    return hardware_cost + premium_cost

  def GetClusterHardwareCost(self) -> float | None:
    """Computes the hardware cost with --dpb_hardware_hourly_cost value.

    Default implementation multiplies --dpb_hardware_hourly_cost with the value
    returned by GetClusterDuration().

    Returns:
      A float representing the cost in dollars or None if not implemented.
    """
    # pylint: disable-next=assignment-from-none
    cluster_duration = self.GetClusterDuration()
    if cluster_duration is None or _HARDWARE_HOURLY_COST.value is None:
      return None
    return cluster_duration / 3600 * _HARDWARE_HOURLY_COST.value

  def GetClusterPremiumCost(self) -> float | None:
    """Computes the premium cost with --dpb_service_premium_hourly_cost value.

    Default implementation multiplies --dpb_service_premium_hourly_cost with the
    value returned by GetClusterDuration().

    Returns:
      A float representing the cost in dollars or None if not implemented.
    """
    # pylint: disable-next=assignment-from-none
    cluster_duration = self.GetClusterDuration()
    if cluster_duration is None or _SERVICE_PREMIUM_HOURLY_COST.value is None:
      return None
    return cluster_duration / 3600 * _SERVICE_PREMIUM_HOURLY_COST.value

  def GetSamples(self) -> list[sample.Sample]:
    """Gets samples with service statistics."""
    samples = []
    metrics: dict[str, tuple[float | None, str]] = {
        # Cluster creation time as reported by the DPB service
        # (non-Serverless DPB services only).
        'dpb_cluster_create_time': (self.GetClusterCreateTime(), 'seconds'),
        # Cluster duration as computed by the underlying benchmark.
        # (non-Serverless DPB services only).
        'dpb_cluster_duration': (self.GetClusterDuration(), 'seconds'),
        # Cluster hardware cost computed from cluster duration and
        # hourly costs passed in flags (non-Serverless DPB services only).
        'dpb_cluster_hardware_cost': (self.GetClusterHardwareCost(), '$'),
        # Cluster DPB service premium cost computed from cluster duration and
        # hourly costs passed in flags (non-Serverless DPB services only).
        'dpb_cluster_premium_cost': (self.GetClusterPremiumCost(), '$'),
        # Cluster hardware cost computed from cluster duration and
        # hourly costs passed in flags (non-Serverless DPB services only).
        'dpb_cluster_total_cost': (self.GetClusterCost(), '$'),
        # Cluster hardware cost per hour as specified in PKB flags.
        'dpb_cluster_hardware_hourly_cost': (
            _HARDWARE_HOURLY_COST.value,
            '$/hour',
        ),
        # DPB Service premium cost per hour as specified in PKB flags.
        'dpb_cluster_premium_hourly_cost': (
            _SERVICE_PREMIUM_HOURLY_COST.value,
            '$/hour',
        ),
    }
    for metric, value_unit_tuple in metrics.items():
      value, unit = value_unit_tuple
      if value is not None:
        samples.append(
            sample.Sample(metric, value, unit, self.GetResourceMetadata())
        )
    return samples


class DpbServiceServerlessMixin:
  """Mixin with default methods dpb services without managed infrastructure."""

  metadata: dict[str, Any]

  def _Create(self) -> None:
    pass

  def _Delete(self) -> None:
    pass

  def GetClusterCreateTime(self) -> float | None:
    return None

  def GetClusterDuration(self) -> float | None:
    return None

  def GetClusterCost(self) -> float | None:
    return None

  def GetClusterHardwareCost(self) -> float | None:
    return None

  def GetClusterPremiumCost(self) -> float | None:
    return None

  def _GetRunStorageLocationMetadata(self) -> dict[str, Any]:
    """Gets storage location-related metadata for further debugging the run."""
    metadata = {}
    if self.metadata.get('dpb_base_dir'):
      metadata['dpb_base_dir'] = self.metadata['dpb_base_dir']
    if self.metadata.get('dpb_spark_event_log_dir'):
      metadata['dpb_spark_event_log_dir'] = self.metadata[
          'dpb_spark_event_log_dir'
      ]
    return metadata


class UnmanagedDpbService(BaseDpbService):
  """Object representing an un-managed dpb service."""

  def __init__(self, dpb_service_spec):
    super().__init__(dpb_service_spec)
    #  Dictionary to hold the cluster vms.
    self.vms = {}
    self.cloud = dpb_service_spec.worker_group.cloud
    if not self.dpb_service_zone:
      raise errors.Setup.InvalidSetupError(
          'dpb_service_zone must be provided, for provisioning.'
      )
    if self.cloud == 'GCP':
      self.region = gcp_util.GetRegionFromZone(FLAGS.dpb_service_zone)
      self.storage_service = gcs.GoogleCloudStorageService()
    elif self.cloud == 'AWS':
      self.region = aws_util.GetRegionFromZone(FLAGS.dpb_service_zone)
      self.storage_service = s3.S3Service()
    else:
      self.region = None
      self.storage_service = None
      self.manage_bucket = False
      logging.warning(
          'Cloud provider %s does not support object storage. '
          'Some benchmarks will not work.',
          self.cloud,
      )

    if self.storage_service:
      self.storage_service.PrepareService(location=self.region)

    # set in _Create of derived classes
    self.leader = None

  @property
  def persistent_fs_prefix(self) -> str | None:
    if self.cloud == 'GCP':
      return 'gs://'
    if self.cloud == 'AWS':
      return 's3://'
    return None

  def CheckPrerequisites(self):
    if self.cloud == 'AWS' and not aws_flags.AWS_EC2_INSTANCE_PROFILE.value:
      raise ValueError(
          'EC2 based Spark and Hadoop services require '
          '--aws_ec2_instance_profile.'
      )

  def GetClusterCreateTime(self) -> float | None:
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
        (
            vm.create_start_time
            for vm in vms
            if vm.create_start_time is not None
        ),
        default=None,
    )
    last_vm_ready_start_time = max(
        (
            vm.resource_ready_time
            for vm in vms
            if vm.resource_ready_time is not None
        ),
        default=None,
    )
    if first_vm_create_start_time is None or last_vm_ready_start_time is None:
      return None
    return (
        my_create_time + last_vm_ready_start_time - first_vm_create_start_time
    )


class UnmanagedDpbServiceYarnCluster(UnmanagedDpbService):
  """Object representing an un-managed dpb service yarn cluster."""

  CLOUD = 'Unmanaged'
  SERVICE_TYPE = dpb_constants.UNMANAGED_DPB_SVC_YARN_CLUSTER

  def __init__(self, dpb_service_spec):
    super().__init__(dpb_service_spec)
    self.cloud = dpb_service_spec.worker_group.cloud

  def GetDpbVersion(self) -> str | None:
    return str(hadoop.HadoopVersion())

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
          'UnmanagedDpbServiceYarnCluster requires VMs in a worker_group.'
      )
    background_tasks.RunThreaded(
        InstallHadoop, self.vms['worker_group'] + self.vms['master_group']
    )
    self.leader = self.vms['master_group'][0]
    hadoop.ConfigureAndStart(
        self.leader, self.vms['worker_group'], configure_s3=self.cloud == 'AWS'
    )

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
    if job_type != dpb_constants.HADOOP_JOB_TYPE:
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

    if self.leader is None:
      raise JobSubmissionError(
          'Cannot submit job as leader VM is not initialized.'
      )
    start_time = datetime.datetime.now()
    try:
      stdout, stderr = self.leader.RobustRemoteCommand(cmd_string)
    except errors.VirtualMachine.RemoteCommandError as e:
      raise JobSubmissionError() from e
    end_time = datetime.datetime.now()

    if job_stdout_file:
      with open(job_stdout_file, 'w') as f:
        f.write(stdout)
    return JobResult(
        run_time=(end_time - start_time).total_seconds(),
        stdout=stdout,
        stderr=stderr,
    )

  def _Delete(self):
    pass

  def _GetCompletedJob(self, job_id: str) -> JobResult | None:
    """Submitting Job via SSH is blocking so this is not meaningful."""
    raise NotImplementedError('Submitting Job via SSH is a blocking command.')


class UnmanagedDpbSparkCluster(UnmanagedDpbService):
  """Object representing an un-managed dpb service spark cluster."""

  CLOUD = 'Unmanaged'
  SERVICE_TYPE = dpb_constants.UNMANAGED_SPARK_CLUSTER

  def _JobJars(self) -> Dict[str, Dict[str, str]]:
    """Known mappings of jars in the cluster used by GetExecutionJar."""
    return {
        dpb_constants.SPARK_JOB_TYPE: {'examples': spark.SparkExamplesJarPath()}
    }

  def __init__(self, dpb_service_spec):
    super().__init__(dpb_service_spec)
    #  Dictionary to hold the cluster vms.
    self.vms = {}
    self.cloud = dpb_service_spec.worker_group.cloud

  def GetDpbVersion(self) -> str | None:
    return f'spark_{spark.SparkVersion()}'

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
          'UnmanagedDpbSparkCluster requires VMs in a worker_group.'
      )

    background_tasks.RunThreaded(
        InstallSpark, self.vms['worker_group'] + self.vms['master_group']
    )
    self.leader = self.vms['master_group'][0]
    spark.ConfigureAndStart(
        self.leader, self.vms['worker_group'], configure_s3=self.cloud == 'AWS'
    )

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
    cmd = self.GetSparkSubmitCommand(
        jarfile=jarfile,
        classname=classname,
        pyspark_file=pyspark_file,
        job_arguments=job_arguments,
        job_files=job_files,
        job_jars=job_jars,
        job_py_files=job_py_files,
        job_type=job_type,
        properties=properties,
    )
    if self.leader is None:
      raise JobSubmissionError(
          'Cannot submit job as leader VM is not initialized.'
      )
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

  def _GetCompletedJob(self, job_id: str) -> JobResult | None:
    """Submitting Job via SSH is blocking so this is not meaningful."""
    raise NotImplementedError('Submitting Job via SSH is a blocking command.')


class KubernetesSparkCluster(BaseDpbService):
  """Object representing a Kubernetes dpb service spark cluster."""

  CLOUD = container_lib.KUBERNETES
  SERVICE_TYPE = dpb_constants.KUBERNETES_SPARK_CLUSTER

  # Constants to sychronize between YAML and Spark configuration
  # TODO(pclay): Consider setting in YAML
  SPARK_DRIVER_SERVICE = 'spark-driver'
  SPARK_DRIVER_PORT = 4042
  SPARK_K8S_SERVICE_ACCOUNT = 'spark'

  def _JobJars(self) -> Dict[str, Dict[str, str]]:
    """Known mappings of jars in the cluster used by GetExecutionJar."""
    return {
        dpb_constants.SPARK_JOB_TYPE: {'examples': spark.SparkExamplesJarPath()}
    }

  # TODO(odiego): Implement GetClusterCreateTime adding K8s cluster create time

  def __init__(self, dpb_service_spec):
    super().__init__(dpb_service_spec)

    benchmark_spec = context.GetThreadBenchmarkSpec()
    self.k8s_cluster = benchmark_spec.container_cluster
    assert self.k8s_cluster
    assert self.k8s_cluster.CLUSTER_TYPE == container_lib.KUBERNETES
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
    elif self.cloud == 'AWS':
      self.region = self.k8s_cluster.region
      self.storage_service = s3.S3Service()
    else:
      raise errors.Config.InvalidValue(
          f'Unsupported Cloud provider {self.cloud}'
      )

    self.storage_service.PrepareService(location=self.region)

    # TODO(pclay): support
    assert not FLAGS.dpb_cluster_properties

    if self.k8s_cluster.num_nodes < 2:
      raise errors.Config.InvalidValue(
          f'Cluster type {dpb_constants.KUBERNETES_SPARK_CLUSTER} requires at'
          f' least 2 nodes.Found {self.k8s_cluster.num_nodes}.'
      )

  @property
  def persistent_fs_prefix(self) -> str | None:
    if self.cloud == 'GCP':
      return 'gs://'
    if self.cloud == 'AWS':
      return 's3://'
    return None

  def GetDpbVersion(self) -> str | None:
    return 'spark_' + FLAGS.spark_version

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
        self.SPARK_K8S_SERVICE_ACCOUNT, clusterrole='edit'
    )

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
        units.mebibyte
    )
    # Reserve 512 MB for system daemons
    node_memory_mb -= 512
    # Remove overhead
    node_memory_mb /= 1 + self.MEMORY_OVERHEAD_FACTOR
    node_memory_mb = int(node_memory_mb)

    # Common PKB Spark cluster properties
    properties = spark.GetConfiguration(
        driver_memory_mb=node_memory_mb,
        worker_memory_mb=node_memory_mb,
        # Schedule one thread per vCPU
        worker_cores=node_cpu,
        # Reserve one node for driver
        num_workers=self.k8s_cluster.num_nodes - 1,
        configure_s3=self.cloud == 'AWS',
    )
    # k8s specific properties
    properties.update({
        'spark.driver.host': self.SPARK_DRIVER_SERVICE,
        'spark.driver.port': str(self.SPARK_DRIVER_PORT),
        'spark.kubernetes.driver.pod.name': self._GetDriverName(),
        # Tell Spark to under-report cores by 1 to fit next to k8s services
        'spark.kubernetes.executor.request.cores': str(node_cpu - 1),
        'spark.kubernetes.container.image': self.image,
        # No HDFS available
        'spark.hadoop.fs.defaultFS': self.base_dir,
        'spark.kubernetes.memoryOverheadFactor': str(
            self.MEMORY_OVERHEAD_FACTOR
        ),
    })
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
        properties=properties,
    )
    driver_name = self._GetDriverName()
    # Request memory for driver. This should guarantee that driver does not get
    # scheduled on same VM as exectutor and OOM.
    driver_memory_mb = int(
        self.GetJobProperties()[spark.SPARK_DRIVER_MEMORY].strip('m')
    )
    start_time = datetime.datetime.now()
    kubernetes_commands.ApplyManifest(
        'container/spark/spark-driver.yaml.j2',
        name=driver_name,
        command=command,
        driver_memory_mb=driver_memory_mb,
        driver_port=self.SPARK_DRIVER_PORT,
        driver_service=self.SPARK_DRIVER_SERVICE,
        image=self.image,
        service_account=self.SPARK_K8S_SERVICE_ACCOUNT,
    )
    pod = kubernetes.KubernetesPod(driver_name)
    # increments driver_name for next job
    self.spark_drivers.append(pod)
    try:
      pod.WaitForExit()
    except container_errors.ContainerError as e:
      raise JobSubmissionError() from e
    end_time = datetime.datetime.now()

    if job_stdout_file:
      with open(job_stdout_file, 'w') as f:
        f.write(pod.GetLogs())

    # TODO(pclay): use k8s output for timing?
    return JobResult(run_time=(end_time - start_time).total_seconds())

  def _Delete(self):
    pass

  def _GetCompletedJob(self, job_id: str) -> JobResult | None:
    """pod.WaitForExit is blocking so this is not meaningful."""
    raise NotImplementedError('pod.WaitForExit is a blocking command.')


class KubernetesFlinkCluster(BaseDpbService):
  """Object representing a Kubernetes dpb service flink cluster."""

  CLOUD = container_lib.KUBERNETES
  SERVICE_TYPE = dpb_constants.KUBERNETES_FLINK_CLUSTER

  FLINK_JOB_MANAGER_SERVICE = 'flink-jobmanager'
  DEFAULT_FLINK_IMAGE = 'flink'

  def __init__(self, dpb_service_spec):
    super().__init__(dpb_service_spec)

    benchmark_spec = context.GetThreadBenchmarkSpec()
    self.k8s_cluster = benchmark_spec.container_cluster
    assert self.k8s_cluster
    assert self.k8s_cluster.CLUSTER_TYPE == container_lib.KUBERNETES
    self.cloud = self.k8s_cluster.CLOUD
    self.container_registry = benchmark_spec.container_registry
    assert self.container_registry

    self.flink_jobmanagers = []
    self.image = self.DEFAULT_FLINK_IMAGE

    if self.cloud == 'GCP':
      self.region = gcp_util.GetRegionFromZone(self.k8s_cluster.zone)
      self.storage_service = gcs.GoogleCloudStorageService()
    else:
      raise errors.Config.InvalidValue(
          f'Unsupported Cloud provider {self.cloud}'
      )

    self.storage_service.PrepareService(location=self.region)

    if self.k8s_cluster.num_nodes < 2:
      raise errors.Config.InvalidValue(
          f'Cluster type {dpb_constants.KUBERNETES_FLINK_CLUSTER} requires at'
          f' least 2 nodes.Found {self.k8s_cluster.num_nodes}.'
      )

  @property
  def persistent_fs_prefix(self) -> str | None:
    if self.cloud == 'GCP':
      return 'gs://'
    return None

  def GetDpbVersion(self) -> str | None:
    return self.spec.version or self.DEFAULT_FLINK_IMAGE

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
          base_image=self.GetDpbVersion(),
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
        units.mebibyte
    )
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
    kubernetes_commands.ApplyManifest(
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
    stdout, _, _ = kubectl.RunKubectlCommand(
        ['get', 'pod', f'--selector=job-name={job_manager_name}', '-o', 'yaml']
    )
    pods = yaml.safe_load(stdout)['items']
    if len(pods) <= 0:
      raise JobSubmissionError('No pod was created for the job.')
    pod = kubernetes.KubernetesPod(pods[0]['metadata']['name'])
    self.flink_jobmanagers.append(pod)
    try:
      pod.WaitForExit()
    except container_errors.ContainerError as e:
      raise JobSubmissionError() from e
    end_time = datetime.datetime.now()

    if job_stdout_file:
      with open(job_stdout_file, 'w') as f:
        f.write(pod.GetLogs())

    return JobResult(run_time=(end_time - start_time).total_seconds())

  def CheckPrerequisites(self):
    # Make sure dpb_job_jarfile is provided when using flink
    assert FLAGS.dpb_job_jarfile
    # dpb_cluster_properties is to be supported
    assert not FLAGS.dpb_cluster_properties

  def _GetCompletedJob(self, job_id: str) -> JobResult | None:
    """pod.WaitForExit is blocking so this is not meaningful."""
    raise NotImplementedError('pod.WaitForExit is a blocking command.')

  def _Delete(self):
    pass


def GetDpbServiceClass(
    cloud: str, dpb_service_type: str
) -> Type[BaseDpbService] | None:
  """Gets the Data Processing Backend class corresponding to 'service_type'.

  Args:
    cloud: String name of cloud of the service
    dpb_service_type: String service type as specified in configuration

  Returns:
    Implementation class corresponding to the argument dpb_service_type

  Raises:
    Exception: An invalid data processing backend service type was provided
  """
  if dpb_service_type in dpb_constants.UNMANAGED_SERVICES:
    cloud = 'Unmanaged'
  elif dpb_service_type in [
      dpb_constants.KUBERNETES_SPARK_CLUSTER,
      dpb_constants.KUBERNETES_FLINK_CLUSTER,
  ]:
    cloud = container_lib.KUBERNETES
  return resource.GetResourceClass(
      BaseDpbService, CLOUD=cloud, SERVICE_TYPE=dpb_service_type
  )
