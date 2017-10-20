# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs sysbench --oltp.

Manual: http://sysbench.sourceforge.net/docs/#database_mode
"""

import logging
import re

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample

DEFAULT_MAX_CONNECTIONS = 151
flags.DEFINE_integer(
    'mysql_max_connections', DEFAULT_MAX_CONNECTIONS,
    'The maximum permitted number of simultaneous client connections. '
    'By default, this is 151.')
FLAGS = flags.FLAGS

BENCHMARK_NAME = 'sysbench_oltp'
BENCHMARK_CONFIG = """
sysbench_oltp:
  description: Runs Sysbench OLTP
  vm_groups:
    default:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
"""

# TODO(user): Validate that the oltp-table-size stresses PD.
SYSBENCH_CMD = ('sudo sysbench '
                '--test=oltp --db-driver=mysql '
                '--mysql-table-engine=myisam '
                '--oltp-table-size=1000000 '
                '--mysql-user=root '
                '--max-requests=0 '
                '--max-time=60 '
                '--mysql-password=perfkitbenchmarker ')


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  """Setup MySQL and Sysbench on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  vm.Install('mysql')
  vm.RemoteCommand('sudo service %s status' % vm.GetServiceName('mysql'))
  vm.RemoteCommand('chmod 777 %s' % vm.GetScratchDir())
  vm.RemoteCommand('sudo service %s stop' % vm.GetServiceName('mysql'))
  vm.RemoteCommand('sudo sed -i '
                   '"s/datadir=\\/var\\/lib\\/mysql/datadir=\\%s\\/mysql/" '
                   '%s' % (vm.GetScratchDir(), vm.GetPathToConfig('mysql')))
  vm.RemoteCommand('sudo cp -R -p /var/lib/mysql %s/' % vm.GetScratchDir())
  vm.RemoteCommand('sudo service %s restart' % vm.GetServiceName('mysql'))
  vm.RemoteCommand('sudo service %s status' % vm.GetServiceName('mysql'))
  vm.RemoteCommand(
      'sudo mysql -u root --password=perfkitbenchmarker '
      '-e "create database sbtest";')
  vm.Install('sysbench')
  vm.RemoteCommand(SYSBENCH_CMD + 'prepare')


def Run(benchmark_spec):
  """Run sysbench oltp.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('Sysbench OLTP Results:')
  mysql_max_connections = FLAGS.mysql_max_connections
  if mysql_max_connections < vm.num_cpus:
    logging.info('--mysql_max_connection should be at least num_cpus %d.'
                 ' Setting max_connection to num_cpus.',
                 vm.num_cpus)
    mysql_max_connections = vm.num_cpus
  if mysql_max_connections != DEFAULT_MAX_CONNECTIONS:
    vm.RemoteCommand(
        'sudo mysql -u root --password=perfkitbenchmarker '
        '-e "SET GLOBAL max_connections=%d";' % mysql_max_connections)
  sysbench_cmd = SYSBENCH_CMD + '--num-threads=%s ' % vm.num_cpus
  stdout, _ = vm.RemoteCommand(sysbench_cmd + 'run', should_log=True)
  match = re.search('\\s+transactions:.+\\(([0-9]+\\.[0-9]+)', stdout)
  value = float(match.group(1))
  metadata = {'mysql_max_connections': mysql_max_connections}
  return [sample.Sample('OLTP Transaction Rate', value, 'Transactions/sec',
                        metadata)]


def Cleanup(benchmark_spec):
  """Cleanup Sysbench and MySQL on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('Sysbench-read cleanup on %s', vm)
  vm.RemoteCommand(SYSBENCH_CMD + 'cleanup')
