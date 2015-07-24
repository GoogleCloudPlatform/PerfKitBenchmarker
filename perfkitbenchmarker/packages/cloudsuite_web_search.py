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


"""Module containing iperf installation and cleanup functions."""

import posixpath

from perfkitbenchmarker import vm_util

APACHE_NUTCH_TAR_URL = ('/home/vstefano/test1/apache-nutch-1.10-src.tar.gz')
NUTCH_SITE_URL = ('/home/vstefano/test1/apache-nutch-1.10/conf/nutch-site.xml')
CRAWLED_URL = ('/home/vstefano/test1/hadoop-2.7.1/crawl7')

SOLR_TAR_URL = ('/home/vstefano/test1/solr-5.2.1.tgz')
SCHEMA_URL = ('/home/vstefano/test1/schema.xml')

CLOUDSUITE_WEB_SEARCH_DIR = posixpath.join(vm_util.VM_TMP_DIR,
                                           'cloudsuite-web-search')
NUTCH_HOME_DIR = posixpath.join(CLOUDSUITE_WEB_SEARCH_DIR, 'apache-nutch-1.10')
SOLR_HOME_DIR = posixpath.join(CLOUDSUITE_WEB_SEARCH_DIR, 'solr-5.2.1')


def _Install(vm):
  """Installs the iperf package on the VM."""
  vm.Install('openjdk7')
  vm.Install('lsof')
  vm.Install('curl')
  vm.RobustRemoteCommand('mkdir -p {0} &&'
                         'tar -C {0} -zxf {1} && '
                         'cd {2} && '
                         'cp {3} server/solr/configsets/'
                         'basic_configs/conf/'.format(
                             CLOUDSUITE_WEB_SEARCH_DIR, SOLR_TAR_URL,
                             SOLR_HOME_DIR, SCHEMA_URL))


def YumInstall(vm):
  """Installs the iperf package on the VM."""
  _Install(vm)


def AptInstall(vm):
  """Installs the iperf package on the VM."""
  _Install(vm)
