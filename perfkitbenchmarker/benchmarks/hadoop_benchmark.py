# Copyright 2014 Google Inc. All rights reserved.
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

"""Runs terasort on hadoop.

Cluster Setup:
http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html

TODO(user): Make hadoop scale when the number of nodes changes. Also
investigate other settings and verfiy that we are seeing good performance.
"""

import logging
import os.path
import time

from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

flags.DEFINE_integer('terasort_num_rows', 100000000,
                     'Number of 100-byte rows used in terasort.')

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'hadoop_benchmark',
                  'description': 'Runs Terasort',
                  'scratch_disk': True,
                  'num_machines': 9}

JRE_PKG = 'openjdk-7-jre-headless'
HADOOP_VERSION = '2.5.2'
HADOOP_URL = ('http://apache.mirrors.tds.net/hadoop/common/hadoop-%s/'
              'hadoop-%s.tar.gz') % (HADOOP_VERSION, HADOOP_VERSION)

DATA_FILES = ['core-site.xml.j2', 'yarn-site.xml.j2', 'hdfs-site.xml',
              'mapred-site.xml', 'hadoop-env.sh', 'slaves.j2']
NUM_BYTES_PER_ROW = 100


def GetInfo():
  return BENCHMARK_INFO


def _ConfDir(vm):
  return '/home/%s/hadoop-%s/conf' % (vm.user_name, HADOOP_VERSION)


def InstallHadoop(vm, master_ip, worker_ips):
  """Download and configure hadoop on a single VM.

  Args:
    vm: The BaseVirtualMachine object representing the VM on which to install
        hadoop.
    master_ip: A string of the master VM's ip.
    worker_ips: A list of all slave ips.
  """
  vm.RemoteCommand('wget %s' % HADOOP_URL)
  vm.RemoteCommand('tar xvzf hadoop-%s.tar.gz' % HADOOP_VERSION)
  vm.InstallPackage(JRE_PKG)
  vm.RemoteCommand('mkdir hadoop-%s/conf' % HADOOP_VERSION)

  # Set available memory to 90% of that on the system
  memory = int((vm.total_memory_kb / 1024) * .9)

  context = {
      'master_ip': master_ip,
      'node_memory': memory,
      'vcpus': vm.num_cpus,
      'worker_ips': worker_ips,
      'scratch_dir': vm.GetScratchDir(),
  }

  for file_name in DATA_FILES:
    file_path = data.ResourcePath(file_name)
    remote_path = os.path.join('hadoop-%s/conf' % HADOOP_VERSION,
                               file_name)
    if file_name.endswith('.j2'):
      vm.RenderTemplate(file_path, os.path.splitext(remote_path)[0], context)
    else:
      vm.RemoteCopy(file_path, remote_path)


def StartDatanode(vm):
  """Start the hadoop daemons on a datanode."""
  conf_dir = _ConfDir(vm)
  vm.RemoteCommand(('hadoop-%s/sbin/hadoop-daemon.sh --config %s '
                    '--script hdfs start datanode') % (HADOOP_VERSION,
                                                       conf_dir))
  vm.RemoteCommand(('hadoop-%s/sbin/yarn-daemon.sh --config '
                    '%s start nodemanager') % (HADOOP_VERSION, conf_dir))


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run hadoop.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms

  master = vms[0]
  slaves = vms[1:]

  master_ip = master.internal_ip
  slave_ips = [vm.internal_ip for vm in slaves]

  args = [((vm, master_ip, slave_ips), {}) for vm in vms]
  vm_util.RunThreaded(InstallHadoop, args)

  conf_dir = _ConfDir(master)
  master.RemoteCommand('yes | hadoop-%s/bin/hdfs namenode -format' %
                       HADOOP_VERSION)
  master.RemoteCommand(('hadoop-%s/sbin/hadoop-daemon.sh --config %s '
                        '--script hdfs start namenode') % (HADOOP_VERSION,
                                                           conf_dir))
  master.RemoteCommand(('hadoop-%s/sbin/yarn-daemon.sh --config %s '
                        'start resourcemanager') % (HADOOP_VERSION, conf_dir))

  vm_util.RunThreaded(StartDatanode, slaves)


def Run(benchmark_spec):
  """Spawn hadoop and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  vms = benchmark_spec.vms
  master = vms[0]

  hadoop_cmd = ('hadoop-%s/bin/hadoop jar '
                'hadoop-%s/share/hadoop/mapreduce/'
                'hadoop-mapreduce-examples-%s.jar ') % (HADOOP_VERSION,
                                                        HADOOP_VERSION,
                                                        HADOOP_VERSION)
  master.RemoteCommand('%s teragen %s /teragen'
                       % (hadoop_cmd, FLAGS.terasort_num_rows))
  num_cpus = 0
  for vm in vms[1:]:
    num_cpus += vm.num_cpus
  start_time = time.time()
  _, stderr = master.RemoteCommand('%s terasort /teragen /terasort'
                                   % hadoop_cmd)
  logging.info('Terasort ourput: %s', stderr)
  time_elapsed = time.time() - start_time
  data_processed_in_mbytes = FLAGS.terasort_num_rows * NUM_BYTES_PER_ROW / (
      1024 * 1024.0)
  master.RemoteCommand(hadoop_cmd + 'teravalidate /terasort /teravalidate')

  master.RemoteCommand('hadoop-%s/bin/hadoop fs -rm -r /teragen' %
                       HADOOP_VERSION)
  master.RemoteCommand('hadoop-%s/bin/hadoop fs -rm -r /terasort' %
                       HADOOP_VERSION)
  master.RemoteCommand('hadoop-%s/bin/hadoop fs -rm -r /teravalidate' %
                       HADOOP_VERSION)
  metadata = {'num_rows': FLAGS.terasort_num_rows,
              'data_size_in_bytes': FLAGS.terasort_num_rows * NUM_BYTES_PER_ROW}
  return [['Terasort Throughput Per Core',
           data_processed_in_mbytes / time_elapsed / num_cpus,
           'MB/sec', metadata],
          ['Terasort Throughput Total Time', time_elapsed, 'sec', metadata]]


def StopDatanode(vm):
  """Stop the hadoop daemons on a datanode."""
  conf_dir = _ConfDir(vm)
  vm.RemoteCommand(('hadoop-%s/sbin/hadoop-daemon.sh --config %s '
                    '--script hdfs stop datanode') % (HADOOP_VERSION, conf_dir))
  vm.RemoteCommand(('hadoop-%s/sbin/yarn-daemon.sh --config %s '
                    'stop nodemanager') % (HADOOP_VERSION, conf_dir))


def CleanNode(vm):
  """Uninstall packages and delete files needed for hadoop on a single VM."""
  logging.info('Hadoop Cleanup up on %s', vm)
  vm.UninstallPackage(JRE_PKG)
  vm.RemoteCommand('rm -rf /scratch/*')
  vm.RemoteCommand('rm -rf /home/%s/hadoop-%s*' % (vm.user_name,
                                                   HADOOP_VERSION))


def Cleanup(benchmark_spec):
  """Uninstall packages required for hadoop and remove benchmark files.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  master = vms[0]
  slaves = vms[1:]

  logging.info('Stopping Hadoop.')
  conf_dir = _ConfDir(master)
  master.RemoteCommand(('hadoop-%s/sbin/hadoop-daemon.sh --config %s '
                        '--script hdfs stop namenode') % (HADOOP_VERSION,
                                                          conf_dir))
  master.RemoteCommand(('hadoop-%s/sbin/yarn-daemon.sh --config %s '
                        'stop resourcemanager') % (HADOOP_VERSION,
                                                   conf_dir))

  vm_util.RunThreaded(StopDatanode, slaves)
  vm_util.RunThreaded(CleanNode, vms)
