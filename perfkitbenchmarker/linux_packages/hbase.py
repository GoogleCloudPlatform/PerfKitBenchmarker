# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing HBase installation and cleanup functions.

HBase is a scalable NoSQL database built on Hadoop.
https://hbase.apache.org/
"""

import functools
import os
import posixpath

from perfkitbenchmarker import data
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import hadoop

HBASE_VERSION = '1.0.3'
HBASE_URL = ('http://www.us.apache.org/dist/hbase/hbase-{0}/'
             'hbase-{0}-bin.tar.gz').format(HBASE_VERSION)

DATA_FILES = ['hbase/hbase-site.xml.j2', 'hbase/regionservers.j2',
              'hbase/hbase-env.sh.j2']

HBASE_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'hbase')
HBASE_BIN = posixpath.join(HBASE_DIR, 'bin')
HBASE_CONF_DIR = posixpath.join(HBASE_DIR, 'conf')


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  for resource in DATA_FILES:
    data.ResourcePath(resource)


def _Install(vm):
  vm.Install('hadoop')
  vm.Install('curl')
  vm.RemoteCommand(('mkdir {0} && curl -L {1} | '
                    'tar -C {0} --strip-components=1 -xzf -').format(
                        HBASE_DIR, HBASE_URL))


def YumInstall(vm):
  """Installs HBase on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs HBase on the VM."""
  _Install(vm)


def _RenderConfig(vm, master_ip, zk_ips, regionserver_ips):
  # Use the same heap configuration as Cassandra
  memory_mb = vm.total_memory_kb // 1024
  hbase_memory_mb = max(min(memory_mb // 2, 1024),
                        min(memory_mb // 4, 8192))
  context = {
      'master_ip': master_ip,
      'worker_ips': regionserver_ips,
      'zk_quorum_ips': zk_ips,
      'hadoop_private_key': hadoop.HADOOP_PRIVATE_KEY,
      'hbase_memory_mb': hbase_memory_mb,
      'scratch_dir': vm.GetScratchDir(),
  }

  for file_name in DATA_FILES:
    file_path = data.ResourcePath(file_name)
    remote_path = posixpath.join(HBASE_CONF_DIR,
                                 os.path.basename(file_name))
    if file_name.endswith('.j2'):
      vm.RenderTemplate(file_path, os.path.splitext(remote_path)[0], context)
    else:
      vm.RemoteCopy(file_path, remote_path)


def ConfigureAndStart(master, regionservers, zk_nodes):
  """Configure HBase on a cluster.

  Args:
    master: VM. Master VM.
    regionservers: List of VMs.
  """
  vms = [master] + regionservers

  def LinkNativeLibraries(vm):
    vm.RemoteCommand(('mkdir {0}/lib/native && '
                      'ln -s {1} {0}/lib/native/Linux-amd64-64').format(
                          HBASE_DIR,
                          posixpath.join(hadoop.HADOOP_DIR, 'lib', 'native')))
  vm_util.RunThreaded(LinkNativeLibraries, vms)
  fn = functools.partial(_RenderConfig, master_ip=master.internal_ip,
                         zk_ips=[vm.internal_ip for vm in zk_nodes],
                         regionserver_ips=[regionserver.internal_ip
                                           for regionserver in regionservers])
  vm_util.RunThreaded(fn, vms)

  master.RemoteCommand('{0} dfs -mkdir /hbase'.format(
      posixpath.join(hadoop.HADOOP_BIN, 'hdfs')))

  master.RemoteCommand(posixpath.join(HBASE_BIN, 'start-hbase.sh'))


def Stop(master):
  """Stop HBase.

  Args:
    master: VM. Master VM.
  """
  master.RemoteCommand(posixpath.join(HBASE_BIN, 'stop-hbase.sh'))
