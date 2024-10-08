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
from typing import Any

from absl import flags
import bs4
from packaging import version
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import regex_util
import requests

FLAGS = flags.FLAGS

_VERSION = flags.DEFINE_string('hadoop_version', None, 'Version of Hadoop.')
_URL_OVERRIDE = flags.DEFINE_string(
    'hadoop_bin_url', None, 'Specify to override url from HADOOP_URL_BASE.'
)

_BLOCKSIZE_OVERRIDE = flags.DEFINE_integer(
    'hadoop_hdfs_blocksize',
    128,
    'Blocksize in MiB to be used by the HDFS filesystem. '
    'This is the chunksize in which the HDFS file will be divided into.',
)

_DFS_REPLICATION_OVERRIDE = flags.DEFINE_integer(
    'hadoop_hdfs_replication',
    None,
    'Default block replication. The actual number of replications can be '
    'specified when the file is created. The default is used if replication is '
    'not specified in create time.',
)

_HADOOP_NAMENODE_OPTS = flags.DEFINE_string(
    'hadoop_namenode_opts',
    None,
    'Additional options to be passed to the HDFS NameNode.',
)

DATA_FILES = [
    'hadoop/core-site.xml.j2',
    'hadoop/yarn-site.xml.j2',
    'hadoop/hdfs-site.xml.j2',
    'hadoop/mapred-site.xml.j2',
    'hadoop/hadoop-env.sh.j2',
    'hadoop/workers.j2',
]
START_HADOOP_SCRIPT = 'hadoop/start-hadoop.sh.j2'

HADOOP_URL_BASE = 'https://downloads.apache.org/hadoop/common'
HADOOP_STABLE_URL = HADOOP_URL_BASE + '/stable'
HADOOP_TAR_PATTERN = re.compile(r'hadoop-([0-9.]+)\.t(ar\.)?gz')

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


@functools.lru_cache()
def HadoopVersion() -> version.Version:
  """Get provided or latest stable HadoopVersion."""
  if _VERSION.value:
    return version.Version(_VERSION.value)
  if _URL_OVERRIDE.value:
    extracted_version = re.search(
        r'[0-9]+\.[0-9]+\.[0-9]+', _URL_OVERRIDE.value
    )
    if extracted_version:
      return version.Version(extracted_version.group(0))
    else:
      raise errors.Config.InvalidValue(
          'Cannot parse version out of --hadoop_bin_url please pass '
          '--hadoop_version as well.'
      )
  response = requests.get(HADOOP_STABLE_URL)
  if not response.ok:
    raise errors.Setup.MissingExecutableError(
        'Could not load ' + HADOOP_STABLE_URL
    )
  soup = bs4.BeautifulSoup(response.content, 'html.parser')
  link = soup.find('a', href=HADOOP_TAR_PATTERN)
  if link:
    match = re.match(HADOOP_TAR_PATTERN, link['href'])
    if match:
      return version.Version(match.group(1))
  raise errors.Setup.MissingExecutableError(
      'Could not find valid hadoop version at ' + HADOOP_STABLE_URL
  )


def _GetHadoopURL():
  """Gets the Hadoop download url based on flags.

  The default is to look for the version `--hadoop_version` to download.

  Returns:
    The Hadoop download url.
  """

  return '{0}/hadoop-{1}/hadoop-{1}.tar.gz'.format(
      HADOOP_URL_BASE, HadoopVersion()
  )


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  for resource in DATA_FILES + [START_HADOOP_SCRIPT]:
    data.ResourcePath(resource)


def _Install(vm):
  # Not all benhmarks know they are installing Hadooop so re-validate here.
  CheckPrerequisites()
  vm.Install('openjdk')
  vm.Install('curl')
  hadoop_url = FLAGS.hadoop_bin_url or _GetHadoopURL()

  vm.RemoteCommand(
      (
          'mkdir {0} && curl -L {1} | tar -C {0} --strip-components=1 -xzf -'
      ).format(HADOOP_DIR, hadoop_url)
  )


def YumInstall(vm):
  """Installs Hadoop on the VM."""
  vm.InstallPackages('snappy')
  _Install(vm)


def AptInstall(vm):
  """Installs Hadoop on the VM."""
  vm.InstallPackages('libsnappy1v5')
  _Install(vm)


def InstallGcsConnector(vm, install_dir=HADOOP_LIB_DIR):
  """Install the GCS connector for Hadoop, which allows I/O to GCS."""
  connector_url = (
      'https://storage.googleapis.com/hadoop-lib/gcs/'
      'gcs-connector-hadoop{}-latest.jar'.format(HadoopVersion().major)
  )
  vm.RemoteCommand('cd {} && curl -O {}'.format(install_dir, connector_url))


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


def _RenderConfig(
    vm,
    master,
    workers,
    memory_fraction=YARN_MEMORY_FRACTION,
    configure_s3=False,
):
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
  block_size = _BLOCKSIZE_OVERRIDE.value * 1024 * 1024

  dfs_data_paths = None
  mapreduce_cluster_local_paths = None

  if vm.scratch_disks:
    scratch_dir = posixpath.join(vm.GetScratchDir(), 'hadoop')
    dfs_data_paths = ','.join([
        'file://' + posixpath.join(vm.GetScratchDir(i), 'hadoop', 'dfs', 'data')
        for i in range(len(vm.scratch_disks))
    ])
    mapreduce_cluster_local_paths = ','.join([
        posixpath.join(vm.GetScratchDir(i), 'hadoop', 'mapred', 'local')
        for i in range(len(vm.scratch_disks))
    ])
    # according to mapred-default.xml, the paths for mapreduce.cluster.local.dir
    # need to be existing, otherwise they will be ignored.
    _MakeFolders(
        mapreduce_cluster_local_paths,
        vm,
    )
  else:
    scratch_dir = posixpath.join('/tmp/pkb/local_scratch', 'hadoop')

  optional_tools = None
  if configure_s3:
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
      'configure_s3': configure_s3,
      'optional_tools': optional_tools,
      'block_size': block_size,
      'dfs_data_paths': dfs_data_paths,
      'mapreduce_cluster_local_paths': mapreduce_cluster_local_paths,
      'hadoop_namenode_opts': _HADOOP_NAMENODE_OPTS.value,
      'dfs_replication': _DFS_REPLICATION_OVERRIDE.value,
  }

  for file_name in DATA_FILES:
    file_path = data.ResourcePath(file_name)
    if file_name == 'hadoop/workers.j2' and HadoopVersion() < version.Version(
        '3'
    ):
      file_name = 'hadoop/slaves.j2'
    remote_path = posixpath.join(HADOOP_CONF_DIR, os.path.basename(file_name))
    if file_name.endswith('.j2'):
      vm.RenderTemplate(file_path, os.path.splitext(remote_path)[0], context)
    else:
      vm.RemoteCopy(file_path, remote_path)


def _MakeFolders(paths_split_by_comma, vm):
  vm.RemoteCommand(
      ('mkdir -p {}').format(' '.join(paths_split_by_comma.split(',')))
  )


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
      _RenderConfig, master=master, workers=workers, configure_s3=configure_s3
  )
  background_tasks.RunThreaded(fn, vms)

  master.RemoteCommand(
      "rm -f {0} && ssh-keygen -q -t rsa -N '' -f {0}".format(
          HADOOP_PRIVATE_KEY
      )
  )

  public_key = master.RemoteCommand('cat {}.pub'.format(HADOOP_PRIVATE_KEY))[0]

  def AddKey(vm):
    vm.RemoteCommand('echo "{}" >> ~/.ssh/authorized_keys'.format(public_key))

  # Add unmanaged Hadoop bin path to the environment PATH so that
  # hadoop/yarn/hdfs commands can be ran without specifying the full path.
  def ExportHadoopBinPath(vm):
    vm.RemoteCommand(
        'echo "export PATH=$PATH:{}" >> ~/.bashrc && source ~/.bashrc'.format(
            HADOOP_BIN
        )
    )

  background_tasks.RunThreaded(AddKey, vms)
  background_tasks.RunThreaded(ExportHadoopBinPath, vms)

  context = {
      'hadoop_dir': HADOOP_DIR,
      'vm_ips': [vm.internal_ip for vm in vms],
      'start_yarn': start_yarn,
  }

  # HDFS setup and formatting, YARN startup
  script_path = posixpath.join(HADOOP_DIR, 'start-hadoop.sh')
  master.RenderTemplate(
      data.ResourcePath(START_HADOOP_SCRIPT), script_path, context=context
  )
  master.RemoteCommand('bash {}'.format(script_path))

  logging.info('Sleeping 10s for Hadoop nodes to join.')
  time.sleep(10)

  logging.info('Checking HDFS status.')
  hdfs_online_count = _GetHDFSOnlineNodeCount(master)
  if hdfs_online_count != len(workers):
    raise ValueError(
        'Not all nodes running HDFS: {} < {}'.format(
            hdfs_online_count, len(workers)
        )
    )
  else:
    logging.info('HDFS running on all %d workers', len(workers))

  if start_yarn:
    logging.info('Checking YARN status.')
    yarn_online_count = _GetYARNOnlineNodeCount(master)
    if yarn_online_count != len(workers):
      raise ValueError(
          'Not all nodes running YARN: {} < {}'.format(
              yarn_online_count, len(workers)
          )
      )
    else:
      logging.info('YARN running on all %d workers', len(workers))


def GetHadoopData() -> dict[str, Any]:
  metadata = {}
  if _HADOOP_NAMENODE_OPTS.value:
    metadata['hadoop_namenode_opts'] = _HADOOP_NAMENODE_OPTS.value
  return metadata
