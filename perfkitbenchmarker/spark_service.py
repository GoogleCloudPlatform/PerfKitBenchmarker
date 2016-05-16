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

FLAGS = flags.FLAGS

# Cloud to use for pkb-created Spark service.
PKB_MANAGED = 'pkb_managed'
PROVIDER_MANAGED = 'managed'

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

  def __init__(self, name, static_cluster, spark_service_spec):
    super(BaseSparkService, self).__init__(user_managed=static_cluster)
    self.name = name
    self.num_workers = spark_service_spec.num_workers
    self.machine_type = spark_service_spec.machine_type
    self.project = spark_service_spec.project

  @abc.abstractmethod
  def SubmitJob(self, job_jar, class_name):
    """Submit a job to the spark service.

    What this returns is not currently defined.  Platforms have their
    own output from job submission, but users will typically want to
    access the output from the job itself.
    """
    # TODO(hildrum) determine what the return value from this will be
    # and provide a way to access the job's output
    pass

  def GetMetadata(self):
    """Return a dictionary of the metadata for this cluster."""
    return {'spark_service': self.SERVICE_NAME,
            'num_workers': self.num_workers,
            'machine_type': self.machine_type,
            'spark_cluster_name': self.name}



class PkbSparkService(BaseSparkService):
  """A Spark service created from vms.

  This class will create a Spark service by creating VMs and installing
  the necessary software.  (Similar to how the hbase benchmark currently
  runs.  It should work across all or almost all providers.
  """

  CLOUD = PKB_MANAGED
  SERVICE_NAME = 'pkb-managed'

  def __init__(self, name, static_cluster, spark_service_spec):
    super(PkbSparkService, self).__init__(name, static_cluster,
                                          spark_service_spec)
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
