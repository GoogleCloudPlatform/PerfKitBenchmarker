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
import logging
import os
import posixpath

import re
import urllib2

from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import hadoop
from perfkitbenchmarker.linux_packages import INSTALL_DIR


FLAGS = flags.FLAGS

flags.DEFINE_string('hbase_version', '1.3.2.1', 'HBase version.')
flags.DEFINE_boolean('hbase_use_stable', False,
                     'Whether to use the current stable release of HBase.')

HBASE_URL_BASE = 'http://www.us.apache.org/dist/hbase'
HBASE_PATTERN = r'>(hbase-([\d\.]+)-bin.tar.gz)<'
HBASE_VERSION_PATTERN = re.compile('HBase (.*)$', re.IGNORECASE | re.MULTILINE)

DATA_FILES = ['hbase/hbase-site.xml.j2', 'hbase/regionservers.j2',
              'hbase/hbase-env.sh.j2']

HBASE_DIR = posixpath.join(INSTALL_DIR, 'hbase')
HBASE_BIN = posixpath.join(HBASE_DIR, 'bin')
HBASE_CONF_DIR = posixpath.join(HBASE_DIR, 'conf')


def _GetHBaseURL():
  """Gets the HBase download url based on flags.

  The default is to look for the version `--hbase_version` to download.
  If `--hbase_stable` is set will look for the latest stable version.

  Returns:
    The HBase download url.

  Raises:
    ValueError: If the download link cannot be found on the download page or
      if the download version does not match the hbase_version flag.
  """
  url = '{}/{}/'.format(
      HBASE_URL_BASE,
      'stable' if FLAGS.hbase_use_stable else FLAGS.hbase_version)
  response = urllib2.urlopen(url)
  html = response.read()
  m = re.search(HBASE_PATTERN, html)
  if not m:
    raise ValueError('Response {} from url {}, no {} in {}'.format(
        response.getcode(), url, HBASE_PATTERN, html))
  link, the_version = m.groups()
  if not FLAGS.hbase_use_stable and the_version != FLAGS.hbase_version:
    raise ValueError('Found version {} in {} expected {}'.format(
        the_version, url, FLAGS.hbase_version))
  return url + link


def GetHBaseVersion(vm):
  txt, _ = vm.RemoteCommand(posixpath.join(HBASE_BIN, 'hbase') + ' version')
  m = HBASE_VERSION_PATTERN.search(txt)
  if m:
    return m.group(1)
  else:
    # log as an warning, don't throw exception so as to continue on
    logging.warn('Could not find HBase version from %s', txt)
    return None


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
  hbase_url = _GetHBaseURL()
  vm.RemoteCommand(('mkdir {0} && curl -L {1} | '
                    'tar -C {0} --strip-components=1 -xzf -').format(
                        HBASE_DIR, hbase_url))


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
