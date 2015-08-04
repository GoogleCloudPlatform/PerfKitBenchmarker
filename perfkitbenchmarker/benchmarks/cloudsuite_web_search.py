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

"""Runs CloudSuite Web Search benchmark.

Docs:
http://parsa.epfl.ch/cloudsuite/

Runs CloudSuite Web Search to collect the statistics that show
the operations completed per second and the minimum, maximum,
average, 90th, and 99th response times.
"""

import logging
import posixpath
import re
import time

from perfkitbenchmarker import vm_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'cloudsuite_web_search',
                  'description': 'Run CloudSuite Web Search',
                  'scratch_disk': True,
                  'num_machines': 3}

CLOUDSUITE_WEB_SEARCH_DIR = posixpath.join(vm_util.VM_TMP_DIR,
                                           'cloudsuite-web-search')
NUTCH_HOME_DIR = posixpath.join(CLOUDSUITE_WEB_SEARCH_DIR, 'apache-nutch-1.10')
FABAN_HOME_DIR = posixpath.join(CLOUDSUITE_WEB_SEARCH_DIR, 'faban')
FABAN_OUTPUT_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'outputFaban')
SOLR_HOME_DIR = posixpath.join(CLOUDSUITE_WEB_SEARCH_DIR, 'solr-5.2.1')
PACKAGES_URL = ('http://parsa.epfl.ch/cloudsuite/software/perfkit/web_search')
# following variables should be changed to URLs and used with wget command
APACHE_NUTCH_TAR_URL = posixpath.join(PACKAGES_URL,
                                      'apache-nutch-1.10-src''.tar.gz')
NUTCH_SITE_URL = posixpath.join(PACKAGES_URL, 'nutch-site.xml')
CRAWLED_URL = posixpath.join(PACKAGES_URL, 'crawl.tar.gz')
SOLR_TAR_URL = posixpath.join(PACKAGES_URL, 'solr-5.2.1.tgz')
SCHEMA_URL = posixpath.join(PACKAGES_URL, 'schema.xml')
FABAN_TAR_URL = posixpath.join(PACKAGES_URL, 'faban-kit-latest.tar.gz')
SEARCH_DRIVER_URL = posixpath.join(PACKAGES_URL, 'search.tar.gz')

java_home = ''


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Install cloudsuite web search and start the server on all machines.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  global java_home
  vms = benchmark_spec.vms
  fw = benchmark_spec.firewall
  vms[0].Install('ant')
  for vm in vms:
    fw.AllowPort(vm, 8983)
    fw.AllowPort(vm, 9983)
    fw.AllowPort(vm, 9980)
    vm.Install('openjdk7')
    vm.Install('lsof')
    vm.Install('curl')
    vm.Install('wget')
    vm.RobustRemoteCommand('mkdir -p {0} && ')
  vms[1].RobustRemoteCommand('cd {0} && '
                             'wget -O solr.tar.gz {2} && '
                             'tar -C {0} -zxf solr.tar.gz && '
                             'wget {3} && '
                             'cp schema.xml {1}/server/solr/configsets/'
                             'basic_configs/conf/'.format(
                                 CLOUDSUITE_WEB_SEARCH_DIR,
                                 SOLR_HOME_DIR, SOLR_TAR_URL,
                                 SCHEMA_URL))
  vms[2].RobustRemoteCommand('cd {0} && '
                             'wget -O solr.tar.gz {2} && '
                             'tar -C {0} -zxf solr.tar.gz && '
                             'wget {3} && '
                             'cp schema.xml {1}/server/solr/configsets/'
                             'basic_configs/conf/'.format(
                                 CLOUDSUITE_WEB_SEARCH_DIR,
                                 SOLR_HOME_DIR, SOLR_TAR_URL,
                                 SCHEMA_URL))
  vms[1].RobustRemoteCommand('cd {0} && '
                             'export PATH=$PATH:/usr/sbin && '
                             'bin/solr start -cloud && '
                             'echo $?'.format(
                                 SOLR_HOME_DIR))
  java_home, _ = vms[0].RemoteCommand('readlink -f $(which java) | '
                                      'cut -d "/" -f 1-5')
  java_home = java_home.rstrip()
  time.sleep(15)
  vms[2].RobustRemoteCommand('cd {0} && '
                             'bin/solr start -cloud -p 8983 -z {1}:9983'.format(
                                 SOLR_HOME_DIR, vms[1].ip_address))
  time.sleep(15)
  vms[1].RobustRemoteCommand('cd {0} && '
                             'bin/solr create_collection -c cloudsuite_web_'
                             'search -d basic_configs -shards 2'.format(
                                 SOLR_HOME_DIR))
  time.sleep(20)
  vms[0].RobustRemoteCommand('cd {3} && '
                             'export JAVA_HOME={6} && '
                             'wget -O apache-nutch-src.tar.gz {4} && '
                             'tar -C {3} -xzf apache-nutch-src.tar.gz && '
                             'wget {1} && '
                             'cp nutch-site.xml {0}/conf/ && '
                             'cd {0} && '
                             'sed -i "/<value>http/c\\<value>http://{5}:'
                             '8983/solr/cloudsuite_web_search</value>" '
                             'conf/nutch-site.xml && '
                             'ant && '
                             'cd runtime/local && '
                             'wget {2}  && '
                             'tar -zxf crawl.tar.gz && '
                             'bin/nutch index crawl/crawldb/ -linkdb '
                             'crawl/linkdb/ -dir crawl/segments/'.format(
                                 NUTCH_HOME_DIR, NUTCH_SITE_URL,
                                 CRAWLED_URL, CLOUDSUITE_WEB_SEARCH_DIR,
                                 APACHE_NUTCH_TAR_URL, vms[1].ip_address,
                                 java_home))
  vms[0].RemoteCommand('cd {0} && '
                       'export JAVA_HOME={7} && '
                       'wget {1} && '
                       'tar -C {0} -xzf faban-kit-latest.tar.gz && '
                       'wget {3} && '
                       'tar -C {2} -xzf search.tar.gz && '
                       'cd {2} && '
                       'master/bin/startup.sh && '
                       'sed -i "/faban.home/c\\faban.home={2}" '
                       'search/build.properties && '
                       'sed -i "/ant.home/c\\ant.home='
                       '/usr/share/ant" search/build.properties && '
                       'sed -i "/faban.url/c\\faban.url='
                       'http://localhost:9980/" search/build.properties && '
                       'cd search && '
                       'ant deploy && '
                       'sed -i "/<ipAddress1>/c\<ipAddress1>{4}'
                       '</ipAddress1>" deploy/run.xml && '
                       'sed -i "/<ipAddress2>/c\<ipAddress2>{5}'
                       '</ipAddress2>" deploy/run.xml && '
                       'sed -i "/<logFile>/c\<logFile>{2}/logs/queries.out'
                       '</logFile>" deploy/run.xml && '
                       'sed -i "/<outputDir>/c\<outputDir>{6}'
                       '</outputDir>" deploy/run.xml && '
                       'sed -i "/<termsFile>/c\<termsFile>{2}'
                       '/search/src/sample/searchdriver/terms_en.out'
                       '</termsFile>" deploy/run.xml && '
                       'sed -i "/<fa:rampUp>/c\<fa:rampUp>10'
                       '</fa:rampUp>" deploy/run.xml && '
                       'sed -i "/<fa:rampDown>/c\<fa:rampDown>10'
                       '</fa:rampDown>" deploy/run.xml && '
                       'sed -i "/<fa:steadyState>/c\<fa:steadyState>60'
                       '</fa:steadyState>" deploy/run.xml'.format(
                           CLOUDSUITE_WEB_SEARCH_DIR, FABAN_TAR_URL,
                           FABAN_HOME_DIR, SEARCH_DRIVER_URL,
                           vms[1].ip_address, vms[2].ip_address,
                           FABAN_OUTPUT_DIR, java_home))
  time.sleep(20)


def Run(benchmark_spec):
  """Run cloudsuite web search on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  vms[0].RemoteCommand('cd {0} && '
                       'export FABAN_HOME={0} && '
                       'cd search && '
                       'sh run.sh'.format(
                           FABAN_HOME_DIR, FABAN_OUTPUT_DIR))
  stdout, _ = vms[0].RemoteCommand('cat {0}/*/summary.xml'.format(
                                   FABAN_OUTPUT_DIR))
  results = []
  p90 = re.findall(r'\<p90th\>(\d+\.?\d*)', stdout)
  sum_p90 = 0.0
  for value in p90:
      sum_p90 += float(value)
  sum_p90 *= 100
  results.append(sample.Sample('p90th', sum_p90, '%'))

  logging.info('CloudSuite Web Search Results:')

  return results


def Cleanup(benchmark_spec):
  """Cleanup CloudSuite Web Search on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vms[0].RemoteCommand('cd {0} && '
                       'export JAVA_HOME={1} && '
                       'master/bin/shutdown.sh'.format(
                           FABAN_HOME_DIR, java_home))
  vms[1].RemoteCommand('cd {0} && '
                       'bin/solr stop -p 8983'.format(
                           SOLR_HOME_DIR))
  vms[2].RemoteCommand('cd {0} && '
                       'bin/solr stop -p 8983'.format(
                           SOLR_HOME_DIR))
