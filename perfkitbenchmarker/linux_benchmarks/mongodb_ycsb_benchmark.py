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

"""Run YCSB against MongoDB.

YCSB is a load generator for many 'cloud' databases. MongoDB is a NoSQL
database.

MongoDB homepage: http://www.mongodb.org/
YCSB homepage: https://github.com/brianfrankcooper/YCSB/wiki
"""

import functools
import posixpath

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import ycsb

# See http://api.mongodb.org/java/2.13/com/mongodb/WriteConcern.html
flags.DEFINE_string('mongodb_writeconcern', 'acknowledged',
                    'MongoDB write concern.')
flags.DEFINE_integer('mongodb_readahead_kb', None,
                     'Configure block device readahead settings.')


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'mongodb_ycsb'
BENCHMARK_CONFIG = """
mongodb_ycsb:
  description: Run YCSB against a single MongoDB node.
  vm_groups:
    workers:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
      vm_count: 1
    clients:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['clients']['vm_count'] = FLAGS.ycsb_client_vms
  return config


def _GetDataDir(vm):
  return posixpath.join(vm.GetScratchDir(), 'mongodb-data')


def _PrepareServer(vm):
  """Installs MongoDB on the server."""
  vm.Install('mongodb_server')
  data_dir = _GetDataDir(vm)
  vm.RemoteCommand('mkdir {0} && chmod a+rwx {0}'.format(data_dir))
  vm.RemoteCommand(
      "sudo sed -i -e '/bind_ip/ s/^/#/; s,^dbpath=.*,dbpath=%s,' %s" %
      (data_dir,
       vm.GetPathToConfig('mongodb_server')))
  if FLAGS.mongodb_readahead_kb is not None:
    vm.SetReadAhead(FLAGS.mongodb_readahead_kb * 2,
                    [d.GetDevicePath() for d in vm.scratch_disks])
  vm.RemoteCommand('sudo service %s restart' %
                   vm.GetServiceName('mongodb_server'))


def _PrepareClient(vm):
  """Install YCSB on the client VM."""
  vm.Install('ycsb')
  # Disable logging for MongoDB driver, which is otherwise quite verbose.
  log_config = """<configuration><root level="WARN"/></configuration>"""

  vm.RemoteCommand("echo '{0}' > {1}/logback.xml".format(
      log_config, ycsb.YCSB_DIR))


def Prepare(benchmark_spec):
  """Install MongoDB on one VM and YCSB on another.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  server_partials = [functools.partial(_PrepareServer, mongo_vm)
                     for mongo_vm in benchmark_spec.vm_groups['workers']]
  client_partials = [functools.partial(_PrepareClient, client)
                     for client in benchmark_spec.vm_groups['clients']]

  vm_util.RunThreaded((lambda f: f()), server_partials + client_partials)
  benchmark_spec.executor = ycsb.YCSBExecutor('mongodb', cp=ycsb.YCSB_DIR)


def Run(benchmark_spec):
  """Run YCSB with against MongoDB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  server = benchmark_spec.vm_groups['workers'][0]
  kwargs = {
      'mongodb.url': 'mongodb://%s:27017/' % server.internal_ip,
      'mongodb.writeConcern': FLAGS.mongodb_writeconcern}
  samples = list(benchmark_spec.executor.LoadAndRun(
      benchmark_spec.vm_groups['clients'],
      load_kwargs=kwargs, run_kwargs=kwargs))
  if FLAGS.mongodb_readahead_kb is not None:
    for s in samples:
      s.metadata['readahdead_kb'] = FLAGS.mongodb_readahead_kb
  return samples


def Cleanup(benchmark_spec):
  """Remove MongoDB and YCSB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  def CleanupServer(server):
    server.RemoteCommand('sudo service %s stop' %
                         server.GetServiceName('mongodb_server'))
    server.RemoteCommand('rm -rf %s' % _GetDataDir(server))

  CleanupServer(benchmark_spec.vm_groups['workers'][0])
