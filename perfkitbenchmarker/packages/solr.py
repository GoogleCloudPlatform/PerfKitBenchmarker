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


"""Module containing Apache Solr installation and cleanup functions."""

import posixpath
import time

from perfkitbenchmarker import vm_util

SOLR_HOME_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'solr-5.2.1')
SOLR_TAR_URL = ('archive.apache.org/dist/lucene/solr/5.2.1/solr-5.2.1.tgz')


def _Install(vm):
  """Installs the Apache Solr on the VM."""
  vm.Install('openjdk7')
  vm.RobustRemoteCommand('cd {0} && '
                         'wget -O solr.tar.gz {2} && '
                         'tar -zxf solr.tar.gz'.format(
                             vm_util.VM_TMP_DIR, SOLR_HOME_DIR, SOLR_TAR_URL))


def YumInstall(vm):
  """Installs the Apache Solr on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the Apache Solr on the VM."""
  _Install(vm)


def ReloadConfiguration(vm, solr_core_dir):
  vm.RemoteCommand('cd {0} && '
                   'mkdir -p {1} && '
                   'cp -R server/solr/* {1}'.format(
                       SOLR_HOME_DIR, solr_core_dir))


def StartWithZookeeper(vm, port, java_heap_size,
                       reload_conf=True):
  """Starts SolrCloud on a node with a Zookeeper.
  To be used on the first node."""
  vm.AllowPort(port)
  vm.AllowPort(port + 1000)
  solr_core_dir = posixpath.join(vm.GetScratchDir(), 'solr_cores')
  if reload_conf:
    ReloadConfiguration(vm, solr_core_dir)
  vm.RobustRemoteCommand('cd {0} && '
                         'bin/solr start -cloud -p {1} '
                         '-s {2} -m {3}'.format(
                             SOLR_HOME_DIR, port, solr_core_dir,
                             java_heap_size))
  time.sleep(15)


def Start(vm, port, zookeeper_node, zookeeper_port, java_heap_size,
          reload_conf=True):
  """Starts SolrCloud on a node and joins a specified Zookeeper."""
  vm.AllowPort(port)
  solr_core_dir = posixpath.join(vm.GetScratchDir(), 'solr_cores')
  if reload_conf:
    ReloadConfiguration(vm, solr_core_dir)
  vm.RobustRemoteCommand('cd {0} && '
                         'bin/solr start -cloud -p {1} '
                         '-z {2}:{3} -s {4} -m {5}'.format(
                             SOLR_HOME_DIR, port, zookeeper_node.ip_address,
                             zookeeper_port, solr_core_dir, java_heap_size))
  time.sleep(15)


def CreateCollection(vm, collection_name, shards_num, port):
  """Creates collection with a basic_config set."""
  vm.RobustRemoteCommand('cd {0} && '
                         'bin/solr create_collection -c {1} '
                         '-d basic_configs -shards {2} -p {3}'.format(
                             SOLR_HOME_DIR, collection_name,
                             shards_num, port))
  time.sleep(20)


def Stop(vm, port):
  vm.RemoteCommand('cd {0} && '
                   'bin/solr stop -p {1}'.format(SOLR_HOME_DIR, port))
  solr_core_dir = posixpath.join(vm.GetScratchDir(), 'solr_cores')
  vm.RemoteCommand('rm -R {0}'.format(solr_core_dir))
