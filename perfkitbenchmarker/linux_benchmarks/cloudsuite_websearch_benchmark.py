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

from perfkitbenchmarker import configs
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample

from perfkitbenchmarker.linux_packages import solr as solr
from perfkitbenchmarker.linux_packages import faban
from perfkitbenchmarker.linux_packages.ant import ANT_HOME_DIR

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'cloudsuite_websearch'
BENCHMARK_CONFIG = """
cloudsuite_websearch:
  description: Run CloudSuite Web Search
  vm_groups:
    workers:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
    clients:
      vm_spec: *default_single_core
"""

CLOUDSUITE_WEB_SEARCH_DIR = posixpath.join(vm_util.VM_TMP_DIR,
                                           'cloudsuite-web-search')

FABAN_OUTPUT_DIR = posixpath.join(vm_util.VM_TMP_DIR, 'outputFaban')

PACKAGES_URL = 'http://lsi-www.epfl.ch/parsa'
INDEX_URL = posixpath.join(PACKAGES_URL, 'index')
SOLR_CONFIG_URL = posixpath.join(PACKAGES_URL, 'solrconfig.xml')
SCHEMA_URL = posixpath.join(PACKAGES_URL, 'schema.xml')
SEARCH_DRIVER_URL = posixpath.join(PACKAGES_URL, 'search.tar.gz')

SOLR_PORT = 8983

CLASSPATH = ('$FABAN_HOME/lib/fabanagents.jar:$FABAN_HOME/lib/fabancommon.jar:'
             '$FABAN_HOME/lib/fabandriver.jar:$JAVA_HOME/lib/tools.jar:'
             '$FABAN_HOME/search/build/lib/search.jar')

flags.DEFINE_string('cs_websearch_client_heap_size', '2g',
                    'Java heap size for Faban client in the usual java format.'
                    ' Default: 2g.')
flags.DEFINE_string('cs_websearch_server_heap_size', '3g',
                    'Java heap size for Solr server in the usual java format.'
                    ' Default: 3g.')
flags.DEFINE_enum('cs_websearch_query_distr', 'Random', ['Random', 'Ziphian'],
                  'Distribution of query terms. '
                  'Random and Ziphian distributions are available. '
                  'Default: Random.')
flags.DEFINE_integer('cs_websearch_num_clients', 1,
                     'Number of client machines.', lower_bound=1)
flags.DEFINE_integer('cs_websearch_ramp_up', 90,
                     'Benchmark ramp up time in seconds.', lower_bound=1)
flags.DEFINE_integer('cs_websearch_steady_state', 60,
                     'Benchmark steady state time in seconds.', lower_bound=1)
flags.DEFINE_integer('cs_websearch_scale', 50,
                     'Number of simulated web search users.',
                     lower_bound=1)


def CheckPrerequisites():
  """Verifies that the required resources are present.
  Raises:
      perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  if FLAGS.num_vms < 1:
    raise ValueError('Cloudsuite Web Search requires at least 1 client VM.')

  def _CheckHeapSize(heap_size_str):
    m = re.match(r'(\d+)([mg])', heap_size_str)
    if not m:
      raise ValueError('Invalid heap size: {0}'.format(heap_size_str))

  _CheckHeapSize(FLAGS.cs_websearch_server_heap_size)
  _CheckHeapSize(FLAGS.cs_websearch_client_heap_size)


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  num_clients = FLAGS.cs_websearch_num_clients
  config['vm_groups']['clients']['vm_count'] = num_clients
  return config


def _PrepareSolr(solr_nodes):
  """Starts and configures SolrCloud."""
  basic_configs_dir = posixpath.join(solr.SOLR_HOME_DIR, 'server/solr/'
                                     'configsets/basic_configs/conf')
  server_heap_size = FLAGS.cs_websearch_server_heap_size
  for vm in solr_nodes:
    vm.Install('solr')
    vm.RemoteCommand('cd {0} && wget {1}'.format(
        basic_configs_dir,
        SCHEMA_URL))
    vm.RemoteCommand('cd {0} && wget {1}'.format(
        basic_configs_dir,
        SOLR_CONFIG_URL))
    if vm == solr_nodes[0]:
      solr.StartWithZookeeper(vm, SOLR_PORT, server_heap_size)
    else:
      solr.Start(vm, SOLR_PORT, vm, SOLR_PORT + 1000, server_heap_size)
  solr.CreateCollection(solr_nodes[0], 'cloudsuite_web_search',
                        len(solr_nodes), SOLR_PORT)


def _BuildIndex(solr_nodes):
  """Downloads Solr index and set it up."""
  for vm in solr_nodes:
    vm.RemoteCommand('cd {0} && '
                     'bin/solr stop -p {1}'.format(
                         solr.SOLR_HOME_DIR, SOLR_PORT))

  def DownloadIndex(vm):
    solr_core_dir = posixpath.join(vm.GetScratchDir(), 'solr_cores')
    vm.RobustRemoteCommand('cd {0} && '
                           'wget -O - {1} | '
                           'tar zxvf - -C {2}'.format(
                               solr_core_dir, INDEX_URL,
                               'cloudsuite_web_search*'))

  vm_util.RunThreaded(DownloadIndex, solr_nodes, len(solr_nodes))
  server_heap_size = FLAGS.cs_websearch_server_heap_size
  for vm in solr_nodes:
    if vm == solr_nodes[0]:
      solr.StartWithZookeeper(vm, SOLR_PORT, server_heap_size, False)
    else:
      solr.Start(vm, SOLR_PORT, solr_nodes[0], SOLR_PORT + 1000,
                 server_heap_size, False)


def _PrepareClient(clients):
  """Prepares client machine in the benchmark."""
  client_master = clients[0]
  for client in clients:
    client.Install('faban')
    client.RemoteCommand('cd {0} && '
                         'wget -O - {1} | '
                         'tar -xzf -'.format(
                             faban.FABAN_HOME_DIR, SEARCH_DRIVER_URL))
    client.RemoteCommand('cd {0}/search && '
                         'sed -i "/faban.home/c\\faban.home={0}" '
                         'build.properties && '
                         'sed -i "/ant.home/c\\ant.home='
                         '{1}" build.properties && '
                         'sed -i "/faban.url/c\\faban.url='
                         'http://localhost:9980/" build.properties'.format(
                             faban.FABAN_HOME_DIR, ANT_HOME_DIR))
  faban.Start(client_master)
  time.sleep(20)


def Prepare(benchmark_spec):
  """Install cloudsuite web search and start the server on all machines.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  servers = benchmark_spec.vm_groups['workers']
  clients = benchmark_spec.vm_groups['clients']

  def PrepareVM(vm):
    vm.Install('wget')
    vm.RemoteCommand('mkdir -p {0}'.format(
                     CLOUDSUITE_WEB_SEARCH_DIR))

  vm_util.RunThreaded(PrepareVM, vms)

  _PrepareSolr(servers)
  _PrepareClient(clients)
  _BuildIndex(servers)


def Run(benchmark_spec):
  """Run cloudsuite web search on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  clients = benchmark_spec.vm_groups['clients']
  client_master = clients[0]
  distribution = FLAGS.cs_websearch_query_distr
  terms_file = None
  search_driver = None
  if distribution == 'Random':
    terms_file = 'terms_random'
    search_driver = 'Random.java'
  elif distribution == 'Ziphian':
    terms_file = 'terms_ordered'
    search_driver = 'Ziphian.java'
  else:
    raise AssertionError('Unknown distribution: {0}'.format(distribution))
  agents = ''
  for client in clients:
    client.RemoteCommand('cd {0}/search && '
                         'cp distributions/{1} '
                         'src/sample/searchdriver/SearchDriver.java'.format(
                             faban.FABAN_HOME_DIR, search_driver))
    target = None
    if client != client_master:
      target = 'deploy.jar'
      agents += ' '
    else:
      target = 'deploy'
    client.RemoteCommand('cd {0}/search && '
                         'export JAVA_HOME={1} && '
                         '{2}/bin/ant {3}'.format(
                             faban.FABAN_HOME_DIR, faban.JAVA_HOME,
                             ANT_HOME_DIR, target))
    agents += client.ip_address + ':' + str(1)
  client_master.RemoteCommand('cd {0}/search && '
                              'sed -i "/<ipAddress1>/c\<ipAddress1>{1}'
                              '</ipAddress1>" deploy/run.xml && '
                              'sed -i "/<portNumber1>/c\<portNumber1>{3}'
                              '</portNumber1>" deploy/run.xml && '
                              'sed -i "/<outputDir>/c\<outputDir>{2}'
                              '</outputDir>" deploy/run.xml && '
                              'sed -i "/<termsFile>/c\<termsFile>{0}'
                              '/search/src/sample/searchdriver/{4}'
                              '</termsFile>" deploy/run.xml && '
                              'sed -i "/<fa:scale>/c\<fa:scale>{7}'
                              '</fa:scale>" deploy/run.xml && '
                              'sed -i "/<agents>/c\<agents>{8}'
                              '</agents>" deploy/run.xml && '
                              'sed -i "/<fa:rampUp>/c\<fa:rampUp>{5}'
                              '</fa:rampUp>" deploy/run.xml && '
                              'sed -i "/<fa:rampDown>/c\<fa:rampDown>60'
                              '</fa:rampDown>" deploy/run.xml && '
                              'sed -i "/<fa:steadyState>/c\<fa:steadyState>{6}'
                              '</fa:steadyState>" deploy/run.xml '.format(
                                  faban.FABAN_HOME_DIR,
                                  benchmark_spec.vms[0].ip_address,
                                  FABAN_OUTPUT_DIR, SOLR_PORT, terms_file,
                                  FLAGS.cs_websearch_ramp_up,
                                  FLAGS.cs_websearch_steady_state,
                                  FLAGS.cs_websearch_scale, agents))
  agent_id = 1
  client_heap_size = FLAGS.cs_websearch_client_heap_size
  driver_dir = posixpath.join(faban.FABAN_HOME_DIR, 'search')
  policy_path = posixpath.join(driver_dir, 'config/security/driver.policy')
  for vm in clients:
    if vm == client_master:
      faban.StartRegistry(vm, CLASSPATH, policy_path)
      faban.StartAgent(vm, CLASSPATH, driver_dir, 'SearchDriver', agent_id,
                       client_heap_size, policy_path, client_master.ip_address)
    else:
      faban.StartAgent(vm, CLASSPATH, driver_dir, 'SearchDriver', agent_id,
                       client_heap_size, policy_path, client_master.ip_address)
    agent_id = agent_id + 1
  benchmark_config = posixpath.join(faban.FABAN_HOME_DIR,
                                    'search/deploy/run.xml')
  faban.StartMaster(client_master, CLASSPATH, client_heap_size,
                    policy_path, benchmark_config)

  def ParseOutput(client):
    stdout, _ = client.RemoteCommand('cd {0} && '
                                     'cd `ls -Art | tail -n 1` && '
                                     'cat summary.xml'.format(
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
  sum_ops_per_sec, sum_p90, sum_p99 = ParseOutput(client_master)
  results.append(sample.Sample('Operations per second', sum_ops_per_sec,
                               'ops/s'))
  results.append(sample.Sample('90th percentile latency', sum_p90, 's'))
  results.append(sample.Sample('99th percentile latency', sum_p99, 's'))
  faban.StopRegistry(client_master)
  return results


def Cleanup(benchmark_spec):
  """Cleanup CloudSuite Web Search on the target vm (by uninstalling).

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  servers = benchmark_spec.vm_groups['workers']
  clients = benchmark_spec.vm_groups['clients']
  client_master = clients[0]
  faban.Stop(client_master)
  for vm in servers:
    solr.Stop(vm, SOLR_PORT)
