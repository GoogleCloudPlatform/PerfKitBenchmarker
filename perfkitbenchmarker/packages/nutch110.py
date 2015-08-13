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


"""Module containing Apache Nutch 1.10 installation and cleanup functions."""

import posixpath

from perfkitbenchmarker import vm_util

NUTCH_HOME_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'apache-nutch-1.10')

APACHE_NUTCH_TAR_URL = ('archive.apache.org/dist/nutch/1.10/apache-nutch-1.10-'
                        'src.tar.gz')

java_home = ''


def _Install(vm):
  """Installs the Apache Nutch on the VM."""
  global java_home
  vm.Install('openjdk7')
  vm.Install('ant')
  java_home, _ = vm.RemoteCommand('readlink -f $(which java) | '
                                  'cut -d "/" -f 1-5')
  java_home = java_home.rstrip()
  vm.RobustRemoteCommand('cd {0} && '
                         'wget -O apache-nutch-src.tar.gz {2} && '
                         'tar -xzf apache-nutch-src.tar.gz'.format(
                             vm_util.VM_TMP_DIR, NUTCH_HOME_DIR,
                             APACHE_NUTCH_TAR_URL))


def YumInstall(vm):
  """Installs the Apache Nutch on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the Apache Nutch on the VM."""
  _Install(vm)


def ConfigureNutchSite(vm, nutch_site, solr_node=None, solr_port=None,
                       solr_collection=None):
  """Configures the Nutch to be used on a VM for indexing into Solr."""
  vm.RemoteCommand('cd {0} && '
                   'export JAVA_HOME={5} && '
                   'echo """{1}""" > conf/nutch-site.xml && '
                   'sed -i "/<value>http/c\\<value>http://{2}:'
                   '{3}/solr/{4}</value>" '
                   'conf/nutch-site.xml && '
                   'ant'.format(
                       NUTCH_HOME_DIR, nutch_site, solr_node.ip_address,
                       solr_port, solr_collection, java_home))


def BuildIndex(vm, data_path):
  """Builds Solr Index using Nutch's crawled data."""
  vm.RemoteCommand('cd {0}/runtime/local && '
                   'bin/nutch index {1}/crawldb/ -linkdb {1}/linkdb -dir '
                   '{1}/segments/'.format(NUTCH_HOME_DIR, data_path))
