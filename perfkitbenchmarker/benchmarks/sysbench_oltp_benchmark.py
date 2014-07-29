#!/usr/bin/env python
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

"""Runs sysbench --oltp.

Manual: http://sysbench.sourceforge.net/docs/#database_mode
"""

import re

import gflags as flags
import logging

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'sysbench_oltp_benchmark',
                  'description': 'Runs sysbench oltp ',
                  'scratch_disk': True,
                  'num_machines': 1}

# TODO(user): Validate that the oltp-table-size stresses PD.
SYSBENCH_CMD = ('sudo sysbench '
                '--test=oltp --db-driver=mysql '
                '--mysql-table-engine=myisam '
                '--oltp-table-size=1000000 '
                '--mysql-socket=/var/run/mysqld/mysqld.sock '
                '--mysql-user=root '
                '--max-requests=0 '
                '--max-time=60 '
                '--mysql-password=perfkitbenchmarker ')


def GetInfo():
  return BENCHMARK_INFO


def Prepare(benchmark_spec):
  """Setup MySQL and Sysbench on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  # TODO(user): Randomize the password.
  vm.RemoteCommand('echo "mysql-server-5.5 mysql-server/root_password password '
                   'perfkitbenchmarker" | sudo debconf-set-selections')
  vm.RemoteCommand('echo "mysql-server-5.5 mysql-server/root_password_again '
                   'password perfkitbenchmarker" | sudo debconf-set-selections')
  vm.InstallPackage('mysql-server')
  vm.RemoteCommand('sudo /etc/init.d/mysql status')
  vm.RemoteCommand('chmod 777 %s' % vm.GetScratchDir())
  vm.RemoteCommand('sudo /etc/init.d/mysql stop')
  vm.RemoteCommand('sudo sed -i \'s/\\/var\\/lib\\/mysql/\\%s\\/mysql/g\''
                   ' /etc/mysql/my.cnf' % vm.GetScratchDir())
  vm.RemoteCommand('sudo cp -R -p /var/lib/mysql %s/' % vm.GetScratchDir())
  vm.RemoteCommand('sudo /etc/init.d/mysql restart')
  vm.RemoteCommand('sudo /etc/init.d/mysql status')
  vm.RemoteCommand(
      'sudo mysql -u root --password=perfkitbenchmarker -e "create '
      'database sbtest";')
  vm.InstallPackage('sysbench')
  vm.RemoteCommand(SYSBENCH_CMD + 'prepare')


def Run(benchmark_spec):
  """Run sysbench oltp.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  vms = benchmark_spec.vms
  vm = vms[0]
  logging.info('Sysbench OLTP Results:')
  sysbench_cmd = SYSBENCH_CMD + '--num-threads=%s ' % vm.num_cpus
  stdout, _ = vm.RemoteCommand(sysbench_cmd + 'run', should_log=True)
  match = re.search('\\s+transactions:.+\\(([0-9]+\\.[0-9]+)', stdout)
  value = float(match.group(1))
  return [('OLTP Transaction Rate', value, 'Transactions/sec')]


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
  vm.UninstallPackage('sysbench')
  vm.UninstallPackage('mysql-server')
