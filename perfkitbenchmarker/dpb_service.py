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
import datetime
import logging
import posixpath
from typing import Dict, List, Optional

from absl import flags
from dataclasses import dataclass
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import hadoop

flags.DEFINE_string(
    'static_dpb_service_instance', None,
    'If set, the name of the pre created dpb implementation,'
    'assumed to be ready.')
flags.DEFINE_string('dpb_log_level', 'INFO', 'Manipulate service log level')
flags.DEFINE_string('dpb_job_jarfile', None,
                    'Executable Jarfile containing workload implementation')
flags.DEFINE_string('dpb_job_classname', None, 'Classname of the job '
                    'implementation in the jar file')
flags.DEFINE_string('dpb_service_zone', None, 'The zone for provisioning the '
                    'dpb_service instance.')
flags.DEFINE_list('dpb_job_properties', [], 'A list of strings of the form '
                  '"key=vale" to be passed into DBP jobs.')


FLAGS = flags.FLAGS

# List of supported data processing backend services
DATAPROC = 'dataproc'
DATAFLOW = 'dataflow'
EMR = 'emr'
UNMANAGED_DPB_SVC_YARN_CLUSTER = 'unmanaged_dpb_svc_yarn_cluster'

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


@dataclass
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


def GetDpbServiceClass(dpb_service_type):
  """Gets the Data Processing Backend class corresponding to 'service_type'.

  Args:
    dpb_service_type: String service type as specified in configuration

  Returns:
    Implementation class corresponding to the argument dpb_service_type

  Raises:
    Exception: An invalid data processing backend service type was provided
  """
  return resource.GetResourceClass(
      BaseDpbService, SERVICE_TYPE=dpb_service_type)


class BaseDpbService(resource.BaseResource):
  """Object representing a Data Processing Backend Service."""

  REQUIRED_ATTRS = ['SERVICE_TYPE']
  RESOURCE_TYPE = 'BaseDpbService'
  SERVICE_TYPE = 'abstract'
  HDFS_FS = 'hdfs'
  GCS_FS = 'gs'
  S3_FS = 's3'

  # Job types that are supported on the dpb service backends
  PYSPARK_JOB_TYPE = 'pyspark'
  SPARKSQL_JOB_TYPE = 'spark-sql'
  SPARK_JOB_TYPE = 'spark'
  HADOOP_JOB_TYPE = 'hadoop'
  DATAFLOW_JOB_TYPE = 'dataflow'
  BEAM_JOB_TYPE = 'beam'

  JOB_JARS = {
      SPARK_JOB_TYPE: {
          'pi': 'file:///usr/lib/spark/examples/jars/spark-examples.jar'
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
    self.dpb_service_zone = FLAGS.dpb_service_zone
    self.dpb_version = dpb_service_spec.version
    self.dpb_service_type = 'unknown'
    self.storage_service = None

  @abc.abstractmethod
  def SubmitJob(self,
                jarfile: str = None,
                classname: str = None,
                pyspark_file: str = None,
                query_file: str = None,
                job_poll_interval: float = None,
                job_stdout_file: str = None,
                job_arguments: List[str] = None,
                job_files: List[str] = None,
                job_jars: List[str] = None,
                job_type: str = None,
                properties: Dict[str, str] = None) -> JobResult:
    """Submit a data processing job to the backend.

    Args:
      jarfile: Jar file to execute.
      classname: Name of the main class.
      pyspark_file: Comma separated list of Python files to be provided to the
        job. Must be one of the following file formats ".py, .zip, or .egg".
      query_file: HCFS URI of file containing Spark SQL script to execute as the
        job.
      job_poll_interval: integer saying how often to poll for job completion.
        Not used by providers for which submit job is a synchronous operation.
      job_stdout_file: String giving the location of the file in which to put
        the standard out of the job.
      job_arguments: List of string arguments to pass to driver application.
        These are not the arguments passed to the wrapper that submits the job.
      job_files: Files passed to a Spark Application to be distributed to
        executors.
      job_jars: Jars to pass to the application
      job_type: Spark or Hadoop job
      properties: Dict of properties to pass with the job.

    Returns:
      A JobResult with the timing of the successful job.

    Raises:
      JobSubmissionError if job fails.
    """
    pass

  def _WaitForJob(self, job_id, timeout, poll_interval):

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

  @abc.abstractmethod
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
    pass

  def GetMetadata(self):
    """Return a dictionary of the metadata for this cluster."""
    pretty_version = self.dpb_version or 'default'
    basic_data = {
        'dpb_service': self.dpb_service_type,
        'dpb_version': pretty_version,
        'dpb_service_version':
            '{}_{}'.format(self.dpb_service_type, pretty_version),
        'dpb_cluster_id': self.cluster_id,
        'dpb_cluster_shape': self.spec.worker_group.vm_spec.machine_type,
        'dpb_cluster_size': self.spec.worker_count,
        'dpb_hdfs_type': self.dpb_hdfs_type,
        'dpb_service_zone': self.dpb_service_zone,
        'dpb_job_properties': ','.join(
            '{}={}'.format(k, v) for k, v in self.GetJobProperties().items()),
    }
    return basic_data

  def _Create(self):
    """Creates the underlying resource."""
    raise NotImplementedError()

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

  def GetJobProperties(self):
    """Parse the dpb_job_properties_flag."""
    return dict(pair.split('=') for pair in FLAGS.dpb_job_properties)

  def GetExecutionJar(self, job_category, job_type):
    """Retrieve execution jar corresponding to the job_category and job_type.

    Args:
      job_category: String category of the job for eg. hadoop, spark, hive, etc.
      job_type: String name of the type of workload to executed on the cluster,
        for eg. word_count, terasort, etc.

    Returns:
      The path to the execusion jar on the cluster

    Raises:
        NotImplementedError: Exception: An unsupported combination of
        job_category
        and job_type was provided for execution on the cluster.
    """
    if job_category not in self.JOB_JARS or job_type not in self.JOB_JARS[
        job_category]:
      raise NotImplementedError()

    return self.JOB_JARS[job_category][job_type]

  def SubmitSparkJob(self, spark_application_jar, spark_application_classname,
                     spark_application_args):
    """Submit a SparkJob to the service instance, returning performance stats.

    Args:
      spark_application_jar: String path to the spark application executable
       that containing workload implementation.
      spark_application_classname: Classname of the spark job's implementation
       in the spark_application_jar file.
      spark_application_args: Arguments to pass to spark application. These are
       not the arguments passed to the wrapper that submits the job.

    Returns:
      JobResult of the Spark Job

    Raises:
      JobSubmissionError if the job fails.
    """
    return self.SubmitJob(
        jarfile=spark_application_jar,
        job_type='spark',
        classname=spark_application_classname,
        job_arguments=spark_application_args
    )

  def CreateBucket(self, source_bucket):
    """Creates an object-store bucket used during persistent data processing.

    Args:
      source_bucket: String, name of the bucket to create.
    """
    self.storage_service.MakeBucket(source_bucket)

  def DeleteBucket(self, source_bucket):
    """Deletes an object-store bucket used during persistent data processing.

    Args:
      source_bucket: String, name of the bucket to delete.
    """
    self.storage_service.DeleteBucket(source_bucket)


class UnmanagedDpbService(BaseDpbService):
  """Object representing an un-managed dpb service."""

  @abc.abstractmethod
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
    """Submit a data processing job to the backend."""
    pass


class UnmanagedDpbServiceYarnCluster(UnmanagedDpbService):
  """Object representing an un-managed dpb service yarn cluster."""

  SERVICE_TYPE = UNMANAGED_DPB_SVC_YARN_CLUSTER
  JOB_JARS = {
      'hadoop': {
          'terasort':
              '/opt/pkb/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar'
      },
  }

  def __init__(self, dpb_service_spec):
    super(UnmanagedDpbServiceYarnCluster, self).__init__(dpb_service_spec)
    #  Dictionary to hold the cluster vms.
    self.vms = {}
    self.dpb_service_type = UNMANAGED_DPB_SVC_YARN_CLUSTER

  def _Create(self):
    """Create an un-managed yarn cluster."""
    logging.info('Should have created vms by now.')
    logging.info(str(self.vms))

    # need to fix this to install spark
    def InstallHadoop(vm):
      vm.Install('hadoop')

    vm_util.RunThreaded(InstallHadoop, self.vms['worker_group'] +
                        self.vms['master_group'])
    self.leader = self.vms['master_group'][0]
    hadoop.ConfigureAndStart(self.leader,
                             self.vms['worker_group'])

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
    """Submit a data processing job to the backend."""
    if job_type != self.HADOOP_JOB_TYPE:
      raise NotImplementedError
    cmd_list = [posixpath.join(hadoop.HADOOP_BIN, 'hadoop')]
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
    stdout, stderr, retcode = self.leader.RemoteCommandWithReturnCode(
        cmd_string)
    if retcode:
      raise JobSubmissionError(stderr)
    end_time = datetime.datetime.now()

    if job_stdout_file:
      with open(job_stdout_file, 'w') as f:
        f.write(stdout)
    return JobResult(run_time=(end_time - start_time).total_seconds())

  def _Delete(self):
    pass

  def GetExecutionJar(self, job_category, job_type):
    """Retrieve execution jar corresponding to the job_category and job_type."""
    if (job_category not in self.JOB_JARS or
        job_type not in self.JOB_JARS[job_category]):
      raise NotImplementedError()
    return self.JOB_JARS[job_category][job_type]
