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

import posixpath
import re
import time

from perfkitbenchmarker import vm_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample

from perfkitbenchmarker.packages import solr as solr
from perfkitbenchmarker.packages import nutch as nutch
from perfkitbenchmarker.packages import faban
from perfkitbenchmarker.packages.ant import ANT_HOME_DIR

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'cloudsuite_web_search',
                  'description': 'Run CloudSuite Web Search',
                  'scratch_disk': True,
                  'num_machines': 3}

CLOUDSUITE_WEB_SEARCH_DIR = posixpath.join(vm_util.VM_TMP_DIR,
                                           'cloudsuite-web-search')

FABAN_OUTPUT_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'outputFaban')

PACKAGES_URL = ('http://parsa.epfl.ch/cloudsuite/software/perfkit/web_search')
NUTCH_SITE_URL = posixpath.join(PACKAGES_URL, 'nutch-site.xml')
CRAWLED_URL = posixpath.join(PACKAGES_URL, 'crawl.tar.gz')
SCHEMA_URL = posixpath.join(PACKAGES_URL, 'schema.xml')
SEARCH_DRIVER_URL = posixpath.join(PACKAGES_URL, 'search.tar.gz')

SOLR_PORT = 8983


def GetInfo():
  return BENCHMARK_INFO


def _PrepareSolr(solr_nodes, fw):
  """Starts and configures SolrCloud."""
  stdout, _ = solr_nodes[0].RemoteCommand('cd {0} && '
                                          'wget --quiet {1} && '
                                          'cat schema.xml'.format(
                                              CLOUDSUITE_WEB_SEARCH_DIR,
                                              SCHEMA_URL))
  for vm in solr_nodes:
    vm.Install('solr')
    solr.SetSchema(vm, stdout.replace('"', '\\"'))
    if vm == solr_nodes[0]:
      solr.StartWithZookeeper(vm, fw, SOLR_PORT)
    else:
      solr.Start(vm, fw, SOLR_PORT, solr_nodes[0], SOLR_PORT + 1000)
  solr.CreateCollection(solr_nodes[0], 'cloudsuite_web_search', len(solr_nodes))


def _BuildIndex(indexer, solr_node):
  """Downloads data and builds Solr index from it."""
  indexer.Install('nutch')
  indexer.RemoteCommand('cd {0} && '
                        'wget {1} && '
                        'sed -i "/<value>http/c\\<value>http://{2}:'
                        '{3}/solr/cloudsuite_web_search</value>" '
                        'nutch-site.xml'.format(
                            CLOUDSUITE_WEB_SEARCH_DIR,
                            NUTCH_SITE_URL, solr_node.ip_address,
                            SOLR_PORT))
  stdout, _ = indexer.RemoteCommand('cd {0} && '
                                    'cat nutch-site.xml'.format(
                                        CLOUDSUITE_WEB_SEARCH_DIR))
  nutch.ConfigureNutchSite(indexer, stdout.replace('"', '\\"'), solr_node,
                           SOLR_PORT, 'cloudsuite_web_search')
  scratch_dir = indexer.GetScratchDir()
  indexer.RobustRemoteCommand('cd {0} && '
                              'wget {1}  && '
                              'tar -zxf crawl.tar.gz'.format(
                                  scratch_dir, CRAWLED_URL))
  nutch.BuildIndex(indexer, posixpath.join(scratch_dir, 'crawl'))


def _PrepareClient(client, fw, solr_nodes):
  """Prepares client machine in the benchmark."""
  client.Install('faban')
  faban.Start(client, fw)
  client.RemoteCommand('cd {0} && '
                       'wget {1} && '
                       'tar -xzf search.tar.gz'.format(
                           faban.FABAN_HOME_DIR, SEARCH_DRIVER_URL))
  client.RemoteCommand('cd {0}/search && '
                       'sed -i "/faban.home/c\\faban.home={0}" '
                       'build.properties && '
                       'sed -i "/ant.home/c\\ant.home='
                       '{1}" build.properties && '
                       'sed -i "/faban.url/c\\faban.url='
                       'http://localhost:9980/" build.properties'.format(
                           faban.FABAN_HOME_DIR, ANT_HOME_DIR))
  client.RemoteCommand('cd {0}/search && '
                       'sed -i "/<ipAddress1>/c\<ipAddress1>{1}'
                       '</ipAddress1>" deploy/run.xml && '
                       'sed -i "/<ipAddress2>/c\<ipAddress2>{2}'
                       '</ipAddress2>" deploy/run.xml && '
                       'sed -i "/<logFile>/c\<logFile>{0}/logs/queries.out'
                       '</logFile>" deploy/run.xml && '
                       'sed -i "/<outputDir>/c\<outputDir>{3}'
                       '</outputDir>" deploy/run.xml && '
                       'sed -i "/<termsFile>/c\<termsFile>{0}'
                       '/search/src/sample/searchdriver/terms_en.out'
                       '</termsFile>" deploy/run.xml && '
                       'sed -i "/<fa:rampUp>/c\<fa:rampUp>100'
                       '</fa:rampUp>" deploy/run.xml && '
                       'sed -i "/<fa:rampDown>/c\<fa:rampDown>100'
                       '</fa:rampDown>" deploy/run.xml && '
                       'sed -i "/<fa:steadyState>/c\<fa:steadyState>600'
                       '</fa:steadyState>" deploy/run.xml '.format(
                           faban.FABAN_HOME_DIR, solr_nodes[0].ip_address,
                           solr_nodes[1].ip_address, FABAN_OUTPUT_DIR))
  client.RemoteCommand('cd {0}/search && '
                       'export JAVA_HOME={1} && '
                       '{2}/bin/ant deploy'.format(
                           faban.FABAN_HOME_DIR, faban.JAVA_HOME,
                           ANT_HOME_DIR))
  time.sleep(20)


def Prepare(benchmark_spec):
  """Install cloudsuite web search and start the server on all machines.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  fw = benchmark_spec.firewall
  for vm in vms:
    vm.Install('wget')
    vm.RemoteCommand('mkdir -p {0}'.format(
                     CLOUDSUITE_WEB_SEARCH_DIR))
  _PrepareSolr(vms[1:], fw)
  _PrepareClient(vms[0], fw, vms[1:])
  _BuildIndex(vms[0], vms[1])


def Run(benchmark_spec):
  """Run cloudsuite web search on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  client = benchmark_spec.vms[0]
  client.RobustRemoteCommand('cd {0} && '
                             'export FABAN_HOME={0} && '
                             'cd search && '
                             'sh run.sh'.format(
                                 faban.FABAN_HOME_DIR))

  def ParseOutput(client):
    stdout, _ = client.RemoteCommand('cat {0}/*/summary.xml'.format(
                                     FABAN_OUTPUT_DIR))
    ops_per_sec = re.findall(r'\<metric unit="ops/sec"\>(\d+\.?\d*)', stdout)
    sum_ops_per_sec = 0.0
    for value in ops_per_sec:
        sum_ops_per_sec += float(value)
    p90 = re.findall(r'\<p90th\>(\d+\.?\d*)', stdout)
    sum_p90 = 0.0
    for value in p90:
        sum_p90 += float(value)
    p99 = re.findall(r'\<p99th\>(\d+\.?\d*)', stdout)
    sum_p99 = 0.0
    for value in p99:
        sum_p99 += float(value)
    return sum_ops_per_sec, sum_p90, sum_p99

  results = []
  sum_ops_per_sec, sum_p90, sum_p99 = ParseOutput(client)
  results.append(sample.Sample('Operations per second', sum_ops_per_sec,
                               'ops/s'))
  results.append(sample.Sample('90th percentile latency', sum_p90, 's'))
  results.append(sample.Sample('99th percentile latency', sum_p99, 's'))

  return results


def Cleanup(benchmark_spec):
  """Cleanup CloudSuite Web Search on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  scratch_dir = vms[0].GetScratchDir()
  vms[0].RemoteCommand('rm {0}/crawl.tar.gz && '
                       'rm -R {0}/crawl'.format(scratch_dir))
  faban.Stop(vms[0])
  solr.Stop(vms[1], SOLR_PORT)
  solr.Stop(vms[2], SOLR_PORT)
