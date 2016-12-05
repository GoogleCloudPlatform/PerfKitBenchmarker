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
import copy
import datetime
import posixpath

from perfkitbenchmarker import flags
from perfkitbenchmarker.linux_packages import hadoop
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util

flags.DEFINE_string('spark_static_cluster_id', None,
                    'If set, the name of the Spark cluster, assumed to be '
                    'ready.')


# Cloud to use for pkb-created Spark service.
PKB_MANAGED = 'pkb_managed'
PROVIDER_MANAGED = 'managed'

SUCCESS = 'success'
RUNTIME = 'running_time'
WAITING = 'pending_time'

SPARK_JOB_TYPE = 'spark'
HADOOP_JOB_TYPE = 'hadoop'

SPARK_VM_GROUPS = ('master_group', 'worker_group')

# This is used for error messages.

_SPARK_SERVICE_REGISTRY = {}
FLAGS = flags.FLAGS


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

  SPARK_SAMPLE_LOCATION = ('file:///usr/lib/spark/examples/jars/'
                           'spark-examples.jar')

  HADOOP_SAMPLE_LOCATION = ('file:///usr/lib/hadoop-mapreduce/'
                            'hadoop-mapreduce-examples.jar')

  def __init__(self, spark_service_spec):
    """Initialize the Apache Spark Service object.

    Args:
      spark_service_spec: spec of the spark service.
    """
    is_user_managed = spark_service_spec.static_cluster_id is not None
    super(BaseSparkService, self).__init__(user_managed=is_user_managed)
    self.spec = spark_service_spec
    # If only the worker group is specified, assume the master group is
    # configured the same way.
    if spark_service_spec.master_group is None:
      self.spec.master_group = copy.copy(self.spec.worker_group)
      self.spec.master_group.vm_count = 1
    self.cluster_id = spark_service_spec.static_cluster_id
    assert (spark_service_spec.master_group.vm_spec.zone ==
            spark_service_spec.worker_group.vm_spec.zone)
    self.zone = spark_service_spec.master_group.vm_spec.zone

  @abc.abstractmethod
  def SubmitJob(self, job_jar, class_name, job_poll_interval=None,
                job_stdout_file=None, job_arguments=None,
                job_type=SPARK_JOB_TYPE):
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
    basic_data = {'spark_service': self.SERVICE_NAME,
                  'spark_svc_cloud': self.CLOUD,
                  'spark_cluster_id': self.cluster_id}
    # TODO grab this information for user_managed clusters.
    if not self.user_managed:
      basic_data.update({'num_workers': str(self.spec.worker_group.vm_count),
                         'worker_machine_type':
                         str(self.spec.worker_group.vm_spec.machine_type)})
    return basic_data

  @classmethod
  def GetExampleJar(cls, job_type):
    if job_type == SPARK_JOB_TYPE:
      return cls.SPARK_SAMPLE_LOCATION
    elif job_type == HADOOP_JOB_TYPE:
      return cls.HADOOP_SAMPLE_LOCATION
    else:
      raise NotImplemented()



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
    assert self.cluster_id is None
    self.vms = {}

  def _Create(self):
    """Create an Apache Spark cluster."""

    # need to fix this to install spark
    def InstallHadoop(vm):
      vm.Install('hadoop')

    vm_util.RunThreaded(InstallHadoop, self.vms['worker_group'] +
                        self.vms['master_group'])
    self.leader = self.vms['master_group'][0]
    hadoop.ConfigureAndStart(self.leader,
                             self.vms['worker_group'])


  def _Delete(self):
    pass

  def SubmitJob(self, jar_file, class_name, job_poll_interval=None,
                job_stdout_file=None, job_arguments=None,
                job_type=SPARK_JOB_TYPE):
    """Submit the jar file."""
    if job_type == SPARK_JOB_TYPE:
      raise NotImplemented()

    cmd_list = [posixpath.join(hadoop.HADOOP_BIN, 'hadoop'),
                'jar', jar_file]
    if class_name:
      cmd_list.append(class_name)
    if job_arguments:
      cmd_list += job_arguments
    cmd_string = ' '.join(cmd_list)
    start_time = datetime.datetime.now()
    stdout, _ = self.leader.RemoteCommand(cmd_string)
    end_time = datetime.datetime.now()
    if job_stdout_file:
      with open(job_stdout_file, 'w') as f:
        f.write(stdout)
    return {SUCCESS: True,
            RUNTIME: (end_time - start_time).total_seconds()}

  @classmethod
  def GetExampleJar(cls, job_type):
    if job_type == HADOOP_JOB_TYPE:
      return posixpath.join(
          hadoop.HADOOP_DIR, 'share', 'hadoop', 'mapreduce',
          'hadoop-mapreduce-examples-{0}.jar'.format(hadoop.HADOOP_VERSION))
    else:
      raise NotImplemented()
