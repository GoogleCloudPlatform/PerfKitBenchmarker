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

"""Benchmarking support for Apache Spark services.

In order to benchmark Apache Spark services such as Google Cloud
Platform's Dataproc or Amazon's EMR, we create a BaseSparkService
class.  Classes to wrap each provider's Apache Spark Service are
in the provider directory as a subclass of BaseSparkService.

Also in this module is a PkbSparkService, which builds a Spark
cluster by creating VMs and installing the necessary software.

For more on Apache Spark: http://spark.apache.org/
"""

import abc

from perfkitbenchmarker import flags
from perfkitbenchmarker import resource


flags.DEFINE_string('spark_static_cluster_id', None,
                    'If set, the name of the Spark cluster, assumed to be '
                    'ready.')


# Cloud to use for pkb-created Spark service.
PKB_MANAGED = 'pkb_managed'
PROVIDER_MANAGED = 'managed'

SUCCESS = 'success'
RUNTIME = 'running_time'
WAITING = 'pending_time'

# This is used for error messages.

_SPARK_SERVICE_REGISTRY = {}


def GetSparkServiceClass(cloud, service_type):
  """Get the Spark class corresponding to 'cloud'."""
  if service_type == PKB_MANAGED:
    return _SPARK_SERVICE_REGISTRY.get(service_type)
  elif cloud in _SPARK_SERVICE_REGISTRY:
    return _SPARK_SERVICE_REGISTRY.get(cloud)
  else:
    raise Exception('No Spark service found for {0}'.format(cloud))


class AutoRegisterSparkServiceMeta(abc.ABCMeta):
  """Metaclass which allows SparkServices to register."""

  def __init__(cls, name, bases, dct):
    if hasattr(cls, 'CLOUD'):
      if cls.CLOUD is None:
        raise Exception('BaseSparkService subclasses must have a CLOUD'
                        'attribute.')
      else:
        _SPARK_SERVICE_REGISTRY[cls.CLOUD] = cls
    super(AutoRegisterSparkServiceMeta, cls).__init__(name, bases, dct)



class BaseSparkService(resource.BaseResource):
  """Object representing a Spark Service."""

  __metaclass__ = AutoRegisterSparkServiceMeta

  def __init__(self, spark_service_spec):
    """Initialize the Apache Spark Service object.

    Args:
      spark_service_spec: spec of the spark service.
    """
    is_user_managed = spark_service_spec.static_cluster_id is not None
    super(BaseSparkService, self).__init__(user_managed=is_user_managed)
    self.spec = spark_service_spec
    self.cluster_id = spark_service_spec.static_cluster_id
    self.num_workers = spark_service_spec.num_workers
    self.machine_type = spark_service_spec.machine_type
    self.project = spark_service_spec.project

  @abc.abstractmethod
  def SubmitJob(self, job_jar, class_name, job_poll_interval=None,
                job_stdout_file=None, job_arguments=None):
    """Submit a job to the spark service.

    Submits a job and waits for it to complete.

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

    Returns:
      dictionary, where success is true if the job succeeded,
      false otherwise.  The dictionary may also contain an entry for
      running_time and pending_time if the platform reports those
      metrics.
    """
    pass

  def GetMetadata(self):
    """Return a dictionary of the metadata for this cluster."""
    return {'spark_service': self.SERVICE_NAME,
            'num_workers': str(self.num_workers),
            'machine_type': str(self.machine_type),
            'spark_cluster_id': self.cluster_id}



class PkbSparkService(BaseSparkService):
  """A Spark service created from vms.

  This class will create a Spark service by creating VMs and installing
  the necessary software.  (Similar to how the hbase benchmark currently
  runs.  It should work across all or almost all providers.
  """

  CLOUD = PKB_MANAGED
  SERVICE_NAME = 'pkb-managed'

  def __init__(self, spark_service_spec):
    super(PkbSparkService, self).__init__(spark_service_spec)
    self.vms = []

  def _Create(self):
    """Create an Apache Spark cluster."""
    raise NotImplementedError()

  def _Delete(self):
    """Delete the vms."""
    for vm in self.vms:
      vm.Delete()

  # TODO(hildrum) actually implement this.
  def SubmitJob(self, jar_file, class_name):
    """Submit the jar file."""
    raise NotImplementedError()
