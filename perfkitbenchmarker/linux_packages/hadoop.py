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
"""Module containing Hadoop installation and cleanup functions.

For documentation of commands to run at startup and shutdown, see:
http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html#Hadoop_Startup
"""
import functools
import logging
import os
import posixpath
import re
import time
from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import aws_credentials

FLAGS = flags.FLAGS

flags.DEFINE_string('hadoop_version', '3.3.1', 'Version of Hadoop.')
flags.DEFINE_string('hadoop_bin_url', None,
                    'Specify to override url from HADOOP_URL_BASE.')

DATA_FILES = [
    'hadoop/core-site.xml.j2', 'hadoop/yarn-site.xml.j2',
    'hadoop/hdfs-site.xml', 'hadoop/mapred-site.xml.j2',
    'hadoop/hadoop-env.sh.j2', 'hadoop/workers.j2'
]
START_HADOOP_SCRIPT = 'hadoop/start-hadoop.sh.j2'

HADOOP_URL_BASE = 'https://downloads.apache.org/hadoop/common'

HADOOP_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'hadoop')
HADOOP_BIN = posixpath.join(HADOOP_DIR, 'bin')
HADOOP_SBIN = posixpath.join(HADOOP_DIR, 'sbin')
HADOOP_CONF_DIR = posixpath.join(HADOOP_DIR, 'etc', 'hadoop')
HADOOP_PRIVATE_KEY = posixpath.join(HADOOP_CONF_DIR, 'hadoop_keyfile')
HADOOP_LIB_DIR = posixpath.join(HADOOP_DIR, 'share', 'hadoop', 'common', 'lib')
HADOOP_TOOLS_DIR = posixpath.join(HADOOP_DIR, 'share', 'hadoop', 'tools', 'lib')

HADOOP_CMD = posixpath.join(HADOOP_BIN, 'hadoop')
HDFS_CMD = posixpath.join(HADOOP_BIN, 'hdfs')
YARN_CMD = posixpath.join(HADOOP_BIN, 'yarn')


def _GetHadoopURL():
  """Gets the Hadoop download url based on flags.

  The default is to look for the version `--hadoop_version` to download.

  Returns:
    The Hadoop download url.
  """

  return '{0}/hadoop-{1}/hadoop-{1}.tar.gz'.format(HADOOP_URL_BASE,
                                                   FLAGS.hadoop_version)


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  for resource in DATA_FILES + [START_HADOOP_SCRIPT]:
    data.ResourcePath(resource)


def _Install(vm):
  vm.Install('openjdk')
  vm.Install('curl')
  hadoop_url = FLAGS.hadoop_bin_url or _GetHadoopURL()

  vm.RemoteCommand(
      ('mkdir {0} && curl -L {1} | '
       'tar -C {0} --strip-components=1 -xzf -').format(HADOOP_DIR, hadoop_url))


def YumInstall(vm):
  """Installs Hadoop on the VM."""
  vm.InstallPackages('snappy')
  _Install(vm)


def AptInstall(vm):
  """Installs Hadoop on the VM."""
  libsnappy = 'libsnappy1'
  if not vm.HasPackage(libsnappy):
    # libsnappy's name on ubuntu16.04 is libsnappy1v5. Let's try that instead.
    libsnappy = 'libsnappy1v5'
  vm.InstallPackages(libsnappy)
  _Install(vm)


def InstallGcsConnector(vm, install_dir=HADOOP_LIB_DIR):
  """Install the GCS connector for Hadoop, which allows I/O to GCS."""
  connector_url = ('https://storage.googleapis.com/hadoop-lib/gcs/'
                   'gcs-connector-hadoop{}-latest.jar'.format(
                       FLAGS.hadoop_version[0]))
  vm.RemoteCommand('cd {0} && curl -O {1}'.format(install_dir, connector_url))


# Scheduling constants.
# Give 90% of VM memory to YARN for scheduling.
# This is roguhly consistent with Dataproc 2.0+
YARN_MEMORY_FRACTION = 0.9
# Give 80% of the memory YARN schedules to the JVM Heap space.
# This is probably conservative on more memory mahcines, but is a traditonal
# rule of thumb.
HEAP_MEMORY_RATIO = 0.8

# Schedule slightly more tasks than vCPUs. This was found to be optimal for
# sorting 240 GB using standard GCE virtual machines with sufficient disk.
# Using a grid seach.
# TODO(pclay): Confirm results generalize to larger data sizes.
MAP_SLOTS_PER_CORE = 1.5
REDUCE_SLOTS_PER_CORE = 4 / 3


def _RenderConfig(vm,
                  master,
                  workers,
                  memory_fraction=YARN_MEMORY_FRACTION,
                  configure_s3=False):
  """Load Hadoop Condfiguration on VM."""
  # Use first worker to get worker configuration
  worker = workers[0]
  num_workers = len(workers)
  worker_cores = worker.NumCpusForBenchmark()
  yarn_memory_mb = int((vm.total_memory_kb / 1024) * memory_fraction)
  # Reserve 1 GB per worker for AppMaster containers.
  usable_memory_mb = yarn_memory_mb - 1024

  # YARN generally schedules based on memory (and ignores cores). We invert this
  # by calculating memory in terms of cores. This means that changing
  # machine memory will not change scheduling simply change the memory given to
  # each task.
  maps_per_node = int(worker_cores * MAP_SLOTS_PER_CORE)
  map_memory_mb = usable_memory_mb // maps_per_node
  map_heap_mb = int(map_memory_mb * HEAP_MEMORY_RATIO)

  reduces_per_node = int(worker_cores * REDUCE_SLOTS_PER_CORE)
  reduce_memory_mb = usable_memory_mb // reduces_per_node
  reduce_heap_mb = int(reduce_memory_mb * HEAP_MEMORY_RATIO)

  # This property is only used for generating data like teragen.
  # Divide 2 to avoid tiny files on large clusters.
  num_map_tasks = maps_per_node * num_workers
  # This determines the number of reduce tasks in Terasort and is critical to
  # scale with the cluster.
  num_reduce_tasks = reduces_per_node * num_workers

  if vm.scratch_disks:
    # TODO(pclay): support multiple scratch disks. A current suboptimal
    # workaround is RAID0 local_ssds with --num_striped_disks.
    scratch_dir = posixpath.join(vm.GetScratchDir(), 'hadoop')
  else:
    scratch_dir = posixpath.join('/tmp/pkb/local_scratch', 'hadoop')

  aws_access_key = None
  aws_secret_key = None
  optional_tools = None
  if configure_s3:
    aws_access_key, aws_secret_key = aws_credentials.GetCredentials()
    optional_tools = 'hadoop-aws'

  context = {
      'master_ip': master.internal_ip,
      'worker_ips': [vm.internal_ip for vm in workers],
      'scratch_dir': scratch_dir,
      'worker_vcpus': worker_cores,
      'hadoop_private_key': HADOOP_PRIVATE_KEY,
      'user': vm.user_name,
      'yarn_memory_mb': yarn_memory_mb,
      'map_memory_mb': map_memory_mb,
      'map_heap_mb': map_heap_mb,
      'num_map_tasks': num_map_tasks,
      'reduce_memory_mb': reduce_memory_mb,
      'reduce_heap_mb': reduce_heap_mb,
      'num_reduce_tasks': num_reduce_tasks,
      'aws_access_key': aws_access_key,
      'aws_secret_key': aws_secret_key,
      'optional_tools': optional_tools
  }

  for file_name in DATA_FILES:
    file_path = data.ResourcePath(file_name)
    if (file_name == 'hadoop/workers.j2' and
        FLAGS.hadoop_version.split('.')[0] < '3'):
      file_name = 'hadoop/slaves.j2'
    remote_path = posixpath.join(HADOOP_CONF_DIR, os.path.basename(file_name))
    if file_name.endswith('.j2'):
      vm.RenderTemplate(file_path, os.path.splitext(remote_path)[0], context)
    else:
      vm.RemoteCopy(file_path, remote_path)


def _GetHDFSOnlineNodeCount(master):
  cmd = HDFS_CMD + ' dfsadmin -report'
  stdout = master.RemoteCommand(cmd)[0]
  avail_str = regex_util.ExtractGroup(r'Live datanodes\s+\((\d+)\):', stdout)
  return int(avail_str)


def _GetYARNOnlineNodeCount(master):
  cmd = YARN_CMD + ' node -list -all'
  stdout = master.RemoteCommand(cmd)[0]
  return len(re.findall(r'RUNNING', stdout))


def ConfigureAndStart(master, workers, start_yarn=True, configure_s3=False):
  """Configure hadoop on a cluster.

  Args:
    master: VM. Master VM - will be the HDFS NameNode, YARN ResourceManager.
    workers: List of VMs. Each VM will run an HDFS DataNode, YARN node.
    start_yarn: bool. Start YARN and JobHistory server? Set to False if HDFS is
        the only service required. Default: True.
    configure_s3: Whether to configure Hadoop to access S3.
  """
  vms = [master] + workers
  # If there are no workers set up in pseudo-distributed mode, where the master
  # node runs the worker daemons.
  workers = workers or [master]
  fn = functools.partial(
      _RenderConfig, master=master, workers=workers, configure_s3=configure_s3)
  vm_util.RunThreaded(fn, vms)

  master.RemoteCommand("rm -f {0} && ssh-keygen -q -t rsa -N '' -f {0}".format(
      HADOOP_PRIVATE_KEY))

  public_key = master.RemoteCommand('cat {0}.pub'.format(HADOOP_PRIVATE_KEY))[0]

  def AddKey(vm):
    vm.RemoteCommand('echo "{0}" >> ~/.ssh/authorized_keys'.format(public_key))

  vm_util.RunThreaded(AddKey, vms)

  context = {
      'hadoop_dir': HADOOP_DIR,
      'vm_ips': [vm.internal_ip for vm in vms],
      'start_yarn': start_yarn
  }

  # HDFS setup and formatting, YARN startup
  script_path = posixpath.join(HADOOP_DIR, 'start-hadoop.sh')
  master.RenderTemplate(
      data.ResourcePath(START_HADOOP_SCRIPT), script_path, context=context)
  master.RemoteCommand('bash {0}'.format(script_path), should_log=True)

  logging.info('Sleeping 10s for Hadoop nodes to join.')
  time.sleep(10)

  logging.info('Checking HDFS status.')
  hdfs_online_count = _GetHDFSOnlineNodeCount(master)
  if hdfs_online_count != len(workers):
    raise ValueError('Not all nodes running HDFS: {0} < {1}'.format(
        hdfs_online_count, len(workers)))
  else:
    logging.info('HDFS running on all %d workers', len(workers))

  if start_yarn:
    logging.info('Checking YARN status.')
    yarn_online_count = _GetYARNOnlineNodeCount(master)
    if yarn_online_count != len(workers):
      raise ValueError('Not all nodes running YARN: {0} < {1}'.format(
          yarn_online_count, len(workers)))
    else:
      logging.info('YARN running on all %d workers', len(workers))
