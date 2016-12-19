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

"""Benchmarking support for Data Processing Backend Services

In order to benchmark Data Processing Backend services such as Google
Cloud Platform's Dataproc and Dataflow or Amazon's EMR, we create a
BaseDpbService class.  Classes to wrap specific backend services are in
the corresponding provider directory as a subclass of BaseDpbService.

"""

import abc

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource


flags.DEFINE_string('static_dpb_service_instance', None,
                    'If set, the name of the pre created dpb implementation,'
                    'assumed to be ready.')

_DPB_SERVICE_REGISTRY = {}
FLAGS = flags.FLAGS

"""
Supported data processing backend services
"""
DATAPROC = 'dataproc'
DATAFLOW = 'dataflow'
EMR = 'emr'

DEFAULT_WORKER_COUNT = 2

"""
Supported applications that can be enabled on the dpb service
"""
FLINK = 'flink'
HIVE = 'hive'

"""
Metrics and Status related metadata
"""
SUCCESS = 'success'
RUNTIME = 'running_time'
WAITING = 'pending_time'

"""
Job types that are supported on the dpb service backends
"""
SPARK_JOB_TYPE = 'spark'
HADOOP_JOB_TYPE = 'hadoop'
DATAFLOW_JOB_TYPE = 'dataflow' # TODO: decide how to pass this along or maybe we should be using the beam sdk as a job type to


def GetDpbServiceClass(dpb_service_type):
  """Get the Data Processing Backend class corresponding to 'service_type'."""
  if dpb_service_type in _DPB_SERVICE_REGISTRY:
    return _DPB_SERVICE_REGISTRY.get(dpb_service_type)
  else:
    raise Exception('No Data Processing Backend service found for {0}'.format(dpb_service_type))


class AutoRegisterDpbServiceMeta(abc.ABCMeta):
  """Metaclass which allows DpbServices to register."""

  def __init__(cls, name, bases, dct):
    if hasattr(cls, 'SERVICE_TYPE') and cls.SERVICE_TYPE is not None:
      _DPB_SERVICE_REGISTRY[cls.SERVICE_TYPE] = cls
    else:
      raise Exception('BaseDpbService subclasses must have a SERVICE_TYPE attribute.')


class BaseDpbService(resource.BaseResource):
  """Object representing a Data Processing Backend Service."""

  __metaclass__ = AutoRegisterDpbServiceMeta

  SERVICE_TYPE = 'abstract'
  HDFS_OUTPUT_FS = 'hdfs'
  GCS_OUTPUT_FS = 'gs'
  S3_OUTPUT_FS = 's3'


  def __init__(self, dpb_service_spec):
    """Initialize the Dpb service object.

    Args:
      dpb_service_spec: spec of the dpb service.
    """
    is_user_managed = dpb_service_spec.static_dpb_service_instance is not None
    """ Hand over the actual creation to the resource module"""
    super(BaseDpbService, self).__init__(user_managed=is_user_managed)
    self.spec = dpb_service_spec
    self.cluster_id = dpb_service_spec.static_dpb_service_instance

  @abc.abstractmethod
  def SubmitJob(self, job_jar, class_name, job_poll_interval=None,
                job_stdout_file=None, job_arguments=None,
                job_type=None):
    """Submit a data processing job to the backend.

    Args:
      job_jar: Jar file to execute.
      class_name: Name of the main class.
      job_poll_interval: integer saying how often to poll for job
        completion.  Not used by providers for which submit job is a
        synchronous operation.
      job_stdout_file: String giving the location of the file in
        which to put the standard out of the job.
      job_arguments: Arguments to pass to class_name.  These are
        not the arguments passed to the wrapper that submits the
        job.
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
    basic_data = {'dpb_service': self.SERVICE_TYPE,
                  'dpb_cluster_id': self.cluster_id}
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


"""
TODO:
2. pkb managed support
4. implement a get metadata method in the concrete derived implementations

"""