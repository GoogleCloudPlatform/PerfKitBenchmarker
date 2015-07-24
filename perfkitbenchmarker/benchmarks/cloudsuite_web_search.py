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

from perfkitbenchmarker.packages.cloudsuite_web_search \
    import CLOUDSUITE_WEB_SEARCH_DIR
from perfkitbenchmarker.packages.cloudsuite_web_search \
    import SOLR_HOME_DIR

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'cloudsuite_web_search',
                  'description': 'Run CloudSuite Web Search',
                  'scratch_disk': True,
                  'num_machines': 3}

APACHE_NUTCH_TAR_URL = ('/home/vstefano/test1/apache-nutch-1.10-src.tar.gz')
NUTCH_SITE_URL = ('/home/vstefano/test1/apache-nutch-1.10/conf/nutch-site.xml')
CRAWLED_URL = ('/home/vstefano/test1/hadoop-2.7.1/crawl7')
NUTCH_HOME_DIR = posixpath.join(CLOUDSUITE_WEB_SEARCH_DIR, 'apache-nutch-1.10')
FABAN_HOME_DIR = posixpath.join(CLOUDSUITE_WEB_SEARCH_DIR, 'faban')
FABAN_TAR_URL = ('/home/vstefano/search-release/faban-kit-latest.tar.gz')
SEARCH_DRIVER_URL = ('/home/vstefano/search-release/search')
FABAN_OUTPUT_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'outputFaban')


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Install cloudsuite web search and start the server on all machines.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vms[0].Install('ant')
  for vm in vms:
    vm.Install('cloudsuite_web_search')
  vms[1].RobustRemoteCommand('cd {0} && '
                             'export PATH=$PATH:/usr/sbin && '
                             'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-'
                             '1.7.0.9.x86_64 && '
                             'cp /home/vstefano/test1/solr-5.2.1/bin/solr '
                             'bin/ && '
                             'echo -e "\\n\\n\\n\\n\\n\\nbasic_configs\\n" | '
                             'bin/solr start -e cloud > '
                             '/home/vstefano/solrcloud.txt 2>&1 && '
                             'echo $? > /home/vstefano/exitCode.txt'.format(
                                 SOLR_HOME_DIR))
  core_node1, _ = vms[1].RemoteCommand('cd {0} && '
                                       'cat example/cloud/node1/solr/'
                                       'gettingstarted_shard1_replica1/'
                                       'core.properties | grep coreNodeName '
                                       '| cut -d "=" -f 2'.format(
                                           SOLR_HOME_DIR))
  core_node2, _ = vms[1].RemoteCommand('cd {0} && '
                                       'cat example/cloud/node2/solr/'
                                       'gettingstarted_shard1_replica2/'
                                       'core.properties | grep coreNodeName '
                                       '| cut -d "=" -f 2'.format(
                                           SOLR_HOME_DIR))
  core_node1 = core_node1.strip()
  core_node2 = core_node2.strip()
  time.sleep(15)
  vms[2].RobustRemoteCommand('cd {0} && '
                             'export PATH=$PATH:/usr/sbin && '
                             'mkdir -p example/cloud/node3/solr && '
                             'cp server/solr/solr.xml '
                             'example/cloud/node3/solr && '
                             'bin/solr start -cloud '
                             '-s example/cloud/node3/solr '
                             '-p 8983 -z {1}:9983 && '
                             'mkdir -p example/cloud/node4/solr && '
                             'cp server/solr/solr.xml '
                             'example/cloud/node4/solr && '
                             'bin/solr start -cloud '
                             '-s example/cloud/node4/solr '
                             '-p 7574 -z {1}:9983 && '
                             'curl {1}:8983/solr/admin/collections?action='
                             'ADDREPLICA --data "collection=gettingstarted'
                             '&shard=shard1&node={2}:8983_solr" && '
                             'curl {1}:8983/solr/admin/collections?action='
                             'ADDREPLICA --data "collection=gettingstarted&'
                             'shard=shard1&node={2}:7574_solr"'.format(
                                 SOLR_HOME_DIR, vms[1].ip_address,
                                 vms[2].ip_address))
  time.sleep(15)
  vms[2].RobustRemoteCommand('cd {0} && '
                             'export PATH=$PATH:/usr/sbin && '
                             'curl {1}:8983/solr/admin/collections?action='
                             'DELETEREPLICA --data "collection=gettingstarted&'
                             'shard=shard1&replica={3}" && '
                             'curl {1}:8983/solr/admin/collections?action='
                             'DELETEREPLICA --data "collection=gettingstarted&'
                             'shard=shard1&replica={4}"'.format(
                                 SOLR_HOME_DIR, vms[1].ip_address,
                                 vms[2].ip_address, core_node1, core_node2))
  time.sleep(20)
  vms[0].RobustRemoteCommand('tar -C {3} -xzf {4} && '
                             'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-'
                             '1.7.0.9.x86_64 && '
                             'cd {0} && '
                             'cp {1} conf/ && '
                             'sed -i "/<value>http/c\\<value>http://{5}:'
                             '8983/solr/gettingstarted</value>" '
                             'conf/nutch-site.xml && '
                             'ant && '
                             'cd runtime/local && '
                             'bin/nutch index {2}/crawldb/ -linkdb '
                             '{2}/linkdb/ -dir {2}/segments/'.format(
                                 NUTCH_HOME_DIR, NUTCH_SITE_URL,
                                 CRAWLED_URL, CLOUDSUITE_WEB_SEARCH_DIR,
                                 APACHE_NUTCH_TAR_URL, vms[1].ip_address))
  vms[0].RemoteCommand('tar -C {0} -xzf {1} && '
                       'cd {2} && '
                       'cp -R {3} {2} && '
                       'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-'
                       '1.7.0.9.x86_64 && '
                       'master/bin/startup.sh && '
                       'sed -i "/faban.home/c\\faban.home={2}" '
                       'search/build.properties && '
                       'sed -i "/ant.home/c\\ant.home='
                       '/usr/share/ant" search/build.properties && '
                       'sed -i "/faban.url/c\\faban.url='
                       'http://localhost:9980/" search/build.properties && '
                       'cp search/build.properties /home/vstefano/ && '
                       'cd search && '
                       'ant deploy -l /home/vstefano/logsAnt.log && '
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
                       'cp deploy/run.xml /home/vstefano'.format(
                           CLOUDSUITE_WEB_SEARCH_DIR, FABAN_TAR_URL,
                           FABAN_HOME_DIR, SEARCH_DRIVER_URL, vms[1].ip_address,
                           vms[2].ip_address, FABAN_OUTPUT_DIR))
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
                       'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-'
                       '1.7.0.9.x86_64 && '
                       'cd search && '
                       'sh run.sh && '
                       'ls -la {1} >> /home/vstefano/testRun.txt && '
                       'ls -la {1}/*/ >> /home/vstefano/testRun.txt'.format(
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
  vms[1].RemoteCommand('cd {0} && '
                       'bin/solr stop -p 7574'.format(
                           SOLR_HOME_DIR))
  vms[2].RemoteCommand('cd {0} && '
                       'bin/solr stop -p 7574'.format(
                           SOLR_HOME_DIR))
  vms[1].RemoteCommand('cd {0} && '
                       'bin/solr stop -p 8983'.format(
                           SOLR_HOME_DIR))
  vms[2].RemoteCommand('cd {0} && '
                       'bin/solr stop -p 8983'.format(
                           SOLR_HOME_DIR))
