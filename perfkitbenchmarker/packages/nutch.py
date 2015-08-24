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


"""Module containing Apache Nutch installation and cleanup functions."""

import posixpath

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages.openjdk7 import JAVA_HOME
from perfkitbenchmarker.packages.ant import ANT_HOME_DIR

NUTCH_HOME_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'apache-nutch-1.10')

APACHE_NUTCH_TAR_URL = ('archive.apache.org/dist/nutch/1.10/apache-nutch-1.10-'
                        'src.tar.gz')



def _Install(vm):
  """Installs the Apache Nutch on the VM."""
  vm.Install('openjdk7')
  vm.Install('ant')
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


def ConfigureNutchSite(vm, nutch_site):
  """Configures the Nutch-site."""
  vm.RemoteCommand('cd {0} && '
                   'export JAVA_HOME={2} && '
                   'echo """{1}""" > conf/nutch-site.xml && '
                   '{3}/bin/ant'.format(
                       NUTCH_HOME_DIR, nutch_site, JAVA_HOME,
                       ANT_HOME_DIR))


def BuildIndex(vm, data_path):
  """Builds Solr Index using Nutch's crawled data."""
  vm.RemoteCommand('cd {0}/runtime/local && '
                   'export JAVA_HOME={1} && '
                   'bin/nutch index {2}/crawldb/ -linkdb {2}/linkdb -dir '
                   '{2}/segments/'.format(NUTCH_HOME_DIR, JAVA_HOME, data_path))
