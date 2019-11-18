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

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource

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


FLAGS = flags.FLAGS

# List of supported data processing backend services
DATAPROC = 'dataproc'
DATAFLOW = 'dataflow'
EMR = 'emr'

# Default number of workers to be used in the dpb service implementation
DEFAULT_WORKER_COUNT = 2

# List of supported applications that can be enabled on the dpb service
FLINK = 'flink'
HIVE = 'hive'

# Metrics and Status related metadata
SUCCESS = 'success'
RUNTIME = 'running_time'
WAITING = 'pending_time'

# Terasort phases
TERAGEN = 'teragen'
TERASORT = 'terasort'
TERAVALIDATE = 'teravalidate'


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
      HADOOP_JOB_TYPE: {
          'terasort':
              'file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar'
      },
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
    self.dpb_version = 'latest'
    self.dpb_service_type = 'unknown'

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
                job_type=None):
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

    Returns:
      dictionary, where success is true if the job succeeded,
      false otherwise.  The dictionary may also contain an entry for
      running_time and pending_time if the platform reports those
      metrics.
    """
    pass

  def GetMetadata(self):
    """Return a dictionary of the metadata for this cluster."""
    basic_data = {
        'dpb_service': self.dpb_service_type,
        'dpb_version': self.dpb_version,
        'dpb_service_version': '{}_{}'.format(self.dpb_service_type,
                                              self.dpb_version),
        'dpb_cluster_id': self.cluster_id,
        'dpb_cluster_shape': self.spec.worker_group.vm_spec.machine_type,
        'dpb_cluster_size': self.spec.worker_count,
        'dpb_hdfs_type': self.dpb_hdfs_type,
        'dpb_service_zone': self.dpb_service_zone
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

  def GenerateDataForTerasort(self, base_dir, generate_jar,
                              generate_job_category):
    """TeraGen generates data used as input data for subsequent TeraSort run.

    Args:
      base_dir: String for the base directory URI (inclusive of the file system)
        for terasort benchmark data.
      generate_jar: String path to the executable for generating the data. Can
        point to a hadoop/yarn executable.
      generate_job_category: String category of the generate job for eg. hadoop,
        spark, hive, etc.

    Returns:
      Wall time for the Generate job.
      The statistics from running the Generate job.
    """

    generate_args = [
        TERAGEN,
        str(FLAGS.dpb_terasort_num_records), base_dir + TERAGEN
    ]
    start_time = datetime.datetime.now()
    stats = self.SubmitJob(
        jarfile=generate_jar,
        job_poll_interval=5,
        job_arguments=generate_args,
        job_type=generate_job_category)
    end_time = datetime.datetime.now()
    return self._ProcessWallTime(start_time, end_time), stats

  def SortDataForTerasort(self, base_dir, sort_jar, sort_job_category):
    """TeraSort samples the input data and sorts the data into a total order.

    TeraSort is implemented as a MapReduce sort job with a custom partitioner
    that uses a sorted list of n-1 sampled keys that define the key range for
    each reduce.

    Args:
      base_dir: String for the base directory URI (inclusive of the file system)
        for terasort benchmark data.
      sort_jar: String path to the executable for sorting the data. Can point to
        a hadoop/yarn executable.
      sort_job_category: String category of the generate job for eg. hadoop,
        spark, hive, etc.

    Returns:
      Wall time for the Sort job.
      The statistics from running the Sort job.
    """
    sort_args = [TERASORT, base_dir + TERAGEN, base_dir + TERASORT]
    start_time = datetime.datetime.now()
    stats = self.SubmitJob(
        jarfile=sort_jar,
        job_poll_interval=5,
        job_arguments=sort_args,
        job_type=sort_job_category)
    end_time = datetime.datetime.now()
    return self._ProcessWallTime(start_time, end_time), stats

  def ValidateDataForTerasort(self, base_dir, validate_jar,
                              validate_job_category):
    """TeraValidate ensures that the output data of TeraSort is globally sorted.

    Args:
      base_dir: String for the base directory URI (inclusive of the file system)
        for terasort benchmark data.
      validate_jar: String path to the executable for validating the sorted
        data. Can point to a hadoop/yarn executable.
      validate_job_category: String category of the validate job for eg. hadoop,
        spark, hive, etc.

    Returns:
      Wall time for the Validate job.
      The statistics from running the Validate job.
    """
    validate_args = [TERAVALIDATE, base_dir + TERASORT, base_dir + TERAVALIDATE]
    start_time = datetime.datetime.now()
    stats = self.SubmitJob(
        jarfile=validate_jar,
        job_poll_interval=5,
        job_arguments=validate_args,
        job_type=validate_job_category)
    end_time = datetime.datetime.now()
    return self._ProcessWallTime(start_time, end_time), stats

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
      Wall time for executing the Spark application.
      The statistics from running the Validate job.
    """
    start_time = datetime.datetime.now()
    stats = self.SubmitJob(
        jarfile=spark_application_jar,
        job_type='spark',
        classname=spark_application_classname,
        job_arguments=spark_application_args
    )
    end_time = datetime.datetime.now()
    return self._ProcessWallTime(start_time, end_time), stats
