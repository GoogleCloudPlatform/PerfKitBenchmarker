# Copyright 2015 Google Inc. All rights reserved.
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


"""Module containing Apache Solr 5.2.1 installation and cleanup functions."""

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


def SetSchema(vm, schema):
  """Sets Solr schema."""
  vm.RemoteCommand('echo """{0}""" > {1}/server/solr/configsets/'
                   'basic_configs/conf/schema.xml'.format(
                       schema, SOLR_HOME_DIR))


def StartWithZookeeper(vm, fw, port):
  """Starts SolrCloud on a node with a Zookeeper.
  To be used on the first node."""
  fw.AllowPort(vm, port)
  fw.AllowPort(vm, port + 1000)
  vm.RemoteCommand('cd {0} && '
                   'export PATH=$PATH:/usr/sbin && '
                   'bin/solr start -cloud -p {1}'.format(SOLR_HOME_DIR, port))
  time.sleep(15)


def Start(vm, fw, port, zookeeper_node, zookeeper_port):
  """Starts SolrCloud on a node and joins a specified Zookeeper."""
  fw.AllowPort(vm, port)
  vm.RobustRemoteCommand('cd {0} && '
                         'bin/solr start -cloud -p {1} -z {2}:{3}'.format(
                             SOLR_HOME_DIR, port, zookeeper_node.ip_address,
                             zookeeper_port))
  time.sleep(15)


def CreateCollection(vm, collection_name, shards_num):
  """Creates collection with a basic_config set."""
  vm.RobustRemoteCommand('cd {0} && '
                         'bin/solr create_collection -c {1} '
                         '-d basic_configs -shards {2}'.format(
                             SOLR_HOME_DIR, collection_name,
                             shards_num))
  time.sleep(20)


def Stop(vm, port):
  vm.RemoteCommand('cd {0} && '
                   'bin/solr stop -p {1}'.format(SOLR_HOME_DIR, port))
