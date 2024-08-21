# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing Apache Spark installation and configuration.

For documentation of Spark Stalanone clusters, see:
https://spark.apache.org/docs/latest/spark-standalone.html
"""
import functools
import logging
import os
import posixpath
import re
import time
from typing import Dict

from absl import flags
import bs4
from packaging import version
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker.linux_packages import hadoop
import requests


FLAGS = flags.FLAGS

_SPARK_VERSION_FLAG = flags.DEFINE_string(
    'spark_version', None, 'Version of spark. Defaults to latest.'
)

DATA_FILES = [
    'spark/spark-defaults.conf.j2',
    'spark/spark-env.sh.j2',
    'spark/workers.j2',
]

SPARK_DOWNLOADS = 'https://downloads.apache.org/spark'
SPARK_VERSION_DIR_PATTERN = re.compile(r'spark-([0-9.]+)/')

SPARK_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'spark')
SPARK_BIN = posixpath.join(SPARK_DIR, 'bin')
SPARK_SBIN = posixpath.join(SPARK_DIR, 'sbin')
SPARK_CONF_DIR = posixpath.join(SPARK_DIR, 'conf')
SPARK_PRIVATE_KEY = posixpath.join(SPARK_CONF_DIR, 'spark_keyfile')

SPARK_SUBMIT = posixpath.join(SPARK_BIN, 'spark-submit')


@functools.lru_cache()
def SparkVersion() -> version.Version:
  """Get passed Spark version or latest available."""
  if _SPARK_VERSION_FLAG.value:
    return version.Version(_SPARK_VERSION_FLAG.value)
  # Find latest version
  # N.B. requests handles a redirect from spark to spark/.
  response = requests.get(SPARK_DOWNLOADS)
  if not response.ok:
    raise errors.Setup.MissingExecutableError(
        'Could not load ' + SPARK_DOWNLOADS
    )
  soup = bs4.BeautifulSoup(response.content, 'html.parser')
  found_versions = []
  for link in soup.find_all('a', href=SPARK_VERSION_DIR_PATTERN):
    match = re.match(SPARK_VERSION_DIR_PATTERN, link.get('href'))
    if match:
      found_versions.append(version.Version(match.group(1)))
  if not found_versions:
    raise errors.Setup.MissingExecutableError(
        'Could not find valid spark versions at ' + SPARK_DOWNLOADS
    )
  return max(found_versions)


def _ScalaVersion() -> version.Version:
  if SparkVersion().major >= 3:
    # https://spark.apache.org/docs/3.0.0/#downloading
    return version.Version('2.12')
  else:
    # https://spark.apache.org/docs/2.4.0/#downloading
    return version.Version('2.11')


def SparkExamplesJarPath() -> str:
  return posixpath.join(
      SPARK_DIR,
      'examples/jars/',
      f'spark-examples_{_ScalaVersion()}-{SparkVersion()}.jar',
  )


def Install(vm):
  """Install spark on a vm."""
  vm.Install('openjdk')
  vm.Install('python')
  vm.Install('curl')
  # Needed for HDFS not as a dependency.
  # Also used on Spark's classpath to support s3a client.
  vm.Install('hadoop')
  spark_version = SparkVersion()
  spark_url = (
      f'{SPARK_DOWNLOADS}/spark-{spark_version}/'
      f'spark-{spark_version}-bin-without-hadoop.tgz'
  )
  vm.RemoteCommand(
      (
          'mkdir {0} && curl -L {1} | tar -C {0} --strip-components=1 -xzf -'
      ).format(SPARK_DIR, spark_url)
  )


# Scheduling constants.
# Give 90% of VM memory to Spark for scheduling.
# This is roughly consistent with Dataproc 2.0+
SPARK_MEMORY_FRACTION = 0.9
SPARK_DRIVER_MEMORY = 'spark.driver.memory'
SPARK_WORKER_MEMORY = 'spark.executor.memory'
SPARK_WORKER_VCPUS = 'spark.executor.cores'


def GetConfiguration(
    driver_memory_mb: int,
    worker_memory_mb: int,
    worker_cores: int,
    num_workers: int,
    configure_s3: bool = False,
) -> Dict[str, str]:
  """Calculate Spark configuration. Shared between VMs and k8s."""
  conf = {
      SPARK_DRIVER_MEMORY: f'{driver_memory_mb}m',
      SPARK_WORKER_MEMORY: f'{worker_memory_mb}m',
      SPARK_WORKER_VCPUS: str(worker_cores),
      'spark.executor.instances': str(num_workers),
      # Tell spark not to run job if it can't schedule all workers. This would
      # silently degrade performance.
      'spark.scheduler.minRegisteredResourcesRatio': '1',
  }
  if configure_s3:
    # Configure S3A Hadoop's S3 filesystem
    conf.update({
        # Use s3:// scheme to be consistent with EMR
        'spark.hadoop.fs.s3.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'fs.s3a.aws.credentials.provider':
            'org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider'
    })
  return conf


def _RenderConfig(
    vm,
    leader,
    workers,
    memory_fraction=SPARK_MEMORY_FRACTION,
    configure_s3=False,
):
  """Load Spark Condfiguration on VM."""
  # Use first worker to get worker configuration
  worker = workers[0]
  worker_cores = worker.NumCpusForBenchmark()
  worker_memory_mb = int((worker.total_memory_kb / 1024) * memory_fraction)
  driver_memory_mb = int((leader.total_memory_kb / 1024) * memory_fraction)

  spark_conf = GetConfiguration(
      driver_memory_mb=driver_memory_mb,
      worker_memory_mb=worker_memory_mb,
      worker_cores=worker_cores,
      num_workers=len(workers),
      configure_s3=configure_s3,
  )

  if vm.scratch_disks:
    # TODO(pclay): support multiple scratch disks. A current suboptimal
    # workaround is RAID0 local_ssds with --num_striped_disks.
    scratch_dir = posixpath.join(vm.GetScratchDir(), 'spark')
  else:
    scratch_dir = posixpath.join('/tmp/pkb/local_scratch', 'spark')

  optional_tools = None
  if configure_s3:
    optional_tools = 'hadoop-aws'

  context = {
      'spark_conf': spark_conf,
      'leader_ip': leader.internal_ip,
      'worker_ips': [vm.internal_ip for vm in workers],
      'scratch_dir': scratch_dir,
      'worker_vcpus': worker_cores,
      'spark_private_key': SPARK_PRIVATE_KEY,
      'worker_memory': spark_conf[SPARK_WORKER_MEMORY],
      'hadoop_cmd': hadoop.HADOOP_CMD,
      'python_cmd': 'python3',
      'optional_tools': optional_tools,
  }

  for file_name in DATA_FILES:
    file_path = data.ResourcePath(file_name)
    if file_name == 'spark/workers.j2':
      # Spark calls its worker list slaves.
      file_name = 'spark/slaves.j2'
    remote_path = posixpath.join(SPARK_CONF_DIR, os.path.basename(file_name))
    if file_name.endswith('.j2'):
      vm.RenderTemplate(file_path, os.path.splitext(remote_path)[0], context)
    else:
      vm.RemoteCopy(file_path, remote_path)


def _GetOnlineWorkerCount(leader):
  """Curl Spark Master Web UI for worker status."""
  cmd = "curl http://localhost:8080 | grep 'Alive Workers' | grep -o '[0-9]\\+'"
  stdout = leader.RemoteCommand(cmd)[0]
  return int(stdout)


def ConfigureAndStart(leader, workers, configure_s3=False):
  """Run Spark Standalone and HDFS on a cluster.

  Args:
    leader: VM. leader VM - will be the HDFS NameNode, Spark Master.
    workers: List of VMs. Each VM will run an HDFS DataNode, Spark Worker.
    configure_s3: Whether to configure Spark to access S3.
  """
  # Start HDFS
  hadoop.ConfigureAndStart(leader, workers, start_yarn=False)

  vms = [leader] + workers
  # If there are no workers set up in pseudo-distributed mode, where the leader
  # node runs the worker daemons.
  workers = workers or [leader]
  fn = functools.partial(
      _RenderConfig, leader=leader, workers=workers, configure_s3=configure_s3
  )
  background_tasks.RunThreaded(fn, vms)

  leader.RemoteCommand(
      "rm -f {0} && ssh-keygen -q -t rsa -N '' -f {0}".format(SPARK_PRIVATE_KEY)
  )

  public_key = leader.RemoteCommand('cat {}.pub'.format(SPARK_PRIVATE_KEY))[0]

  def AddKey(vm):
    vm.RemoteCommand('echo "{}" >> ~/.ssh/authorized_keys'.format(public_key))

  background_tasks.RunThreaded(AddKey, vms)

  # HDFS setup and formatting, Spark startup
  leader.RemoteCommand('bash {}/start-all.sh'.format(SPARK_SBIN))

  logging.info('Sleeping 10s for Spark nodes to join.')
  time.sleep(10)

  logging.info('Checking Spark status.')
  worker_online_count = _GetOnlineWorkerCount(leader)
  if worker_online_count != len(workers):
    raise ValueError(
        'Not all nodes running Spark: {} < {}'.format(
            worker_online_count, len(workers)
        )
    )
  else:
    logging.info('Spark running on all %d workers', len(workers))
