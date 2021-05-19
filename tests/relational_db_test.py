# Copyright 20121 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.relational_db."""

import unittest
from unittest import mock

from absl import flags
from perfkitbenchmarker import relational_db
from perfkitbenchmarker.configs import benchmark_config_spec
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_COMPONENT = 'test_component'


def CreateTestLinuxVm():
  vm_spec = pkb_common_test_case.CreateTestVmSpec()
  return pkb_common_test_case.TestLinuxVirtualMachine(vm_spec=vm_spec)


class FakeRelationalDb(relational_db.BaseRelationalDb):

  def GetEndpoint(self):
    pass

  def GetPort(self):
    pass

  def _Create(self):
    pass

  def _Delete(self):
    pass

  def GetDefaultEngineVersion(self, _):
    pass

  def _FailoverHA(self):
    pass


class RelationalDbUnamangedTestCase(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super(RelationalDbUnamangedTestCase, self).setUp()
    self.minimal_spec = {
        'cloud': 'GCP',
        'engine': 'mysql',
        'engine_version': '5.7',
        'db_spec': {
            'GCP': {
                'machine_type': 'n1-standard-1'
            }
        },
        'db_disk_spec': {
            'GCP': {
                'disk_size': 500
            }
        }
    }

    self.spec = benchmark_config_spec._RelationalDbSpec(
        _COMPONENT, flag_values=FLAGS, **self.minimal_spec)

  def testMakePostgresCommand(self):
    FLAGS['use_managed_db'].parse(False)
    db = FakeRelationalDb(self.spec)
    db.endpoint = '1.1.1.1'
    self.assertEqual(
        db.MakePostgresCommand('postgresql', 'Select 1', use_localhost=False),
        'psql \'host=1.1.1.1 user=root password=perfkitbenchmarker dbname=postgresql\' -c "Select 1"'
    )

  def testMakePostgresCommandWithLocalHost(self):
    FLAGS['use_managed_db'].parse(False)
    db = FakeRelationalDb(self.spec)
    db.endpoint = '1.1.1.1'
    self.assertEqual(
        db.MakePostgresCommand('postgresql', 'Select 1', use_localhost=True),
        'psql \'host=localhost user=root password=perfkitbenchmarker dbname=postgresql\' -c "Select 1"'
    )

  def testMakeMysqlCommand(self):
    FLAGS['use_managed_db'].parse(False)
    db = FakeRelationalDb(self.spec)
    db.endpoint = '1.1.1.1'
    self.assertEqual(
        db.MakeMysqlCommand('Select 1', use_localhost=False),
        'mysql -h 1.1.1.1 -P 3306 -u root -pperfkitbenchmarker -e "Select 1"')

  def testMakeMysqlCommandWithLocalHost(self):
    FLAGS['use_managed_db'].parse(False)
    db = FakeRelationalDb(self.spec)
    db.endpoint = '1.1.1.1'
    self.assertEqual(
        db.MakeMysqlCommand('Select 1', use_localhost=True),
        'mysql -h localhost -P 3306 -u root -pperfkitbenchmarker -e "Select 1"')

  def testMakeSqlserverCommand(self):
    FLAGS['use_managed_db'].parse(False)
    db = FakeRelationalDb(self.spec)
    db.endpoint = '1.1.1.1'
    self.assertEqual(
        db.MakeSqlserverCommand('Select 1', use_localhost=False),
        '/opt/mssql-tools/bin/sqlcmd -S 1.1.1.1 -U root -P perfkitbenchmarker -Q "Select 1"'
    )

  def testMakeSqlserverCommandWithLocalHost(self):
    FLAGS['use_managed_db'].parse(False)
    db = FakeRelationalDb(self.spec)
    db.endpoint = '1.1.1.1'
    self.assertEqual(
        db.MakeSqlserverCommand('Select 1', use_localhost=True),
        '/opt/mssql-tools/bin/sqlcmd -S localhost -U root -P perfkitbenchmarker -Q "Select 1"'
    )

  def testInstallMYSQLServer(self):
    FLAGS['use_managed_db'].parse(False)
    FLAGS['innodb_buffer_pool_size'].parse(100)
    db = FakeRelationalDb(self.spec)
    db.endpoint = '1.1.1.1'
    db.client_vm = CreateTestLinuxVm()
    db.server_vm = CreateTestLinuxVm()
    db.server_vm.IS_REBOOTABLE = False
    db.client_vm.IS_REBOOTABLE = False
    db.server_vm.GetScratchDir = mock.MagicMock(return_value='scratch')
    with mock.patch.object(db.server_vm, 'RemoteCommand') as remote_command:
      db._InstallMySQLServer()
    command = [
        mock.call('chmod 777 scratch'),
        mock.call('sudo service None stop'),
        mock.call('sudo mkdir -p /scratch/mysql'),
        mock.call('sudo mkdir -p /scratch/tmp'),
        mock.call('sudo chown mysql:mysql /scratch/mysql'),
        mock.call('sudo chown mysql:mysql /scratch/tmp'),
        mock.call('sudo rsync -avzh /var/lib/mysql/ /scratch/mysql'),
        mock.call('sudo rsync -avzh /tmp/ /scratch/tmp'),
        mock.call('df', should_log=True),
        mock.call(
            'echo "alias /var/lib/mysql -> /scratch/mysql," | sudo tee -a /etc/apparmor.d/tunables/alias'
        ),
        mock.call(
            'echo "alias /tmp -> /scratch/tmp," | sudo tee -a /etc/apparmor.d/tunables/alias'
        ),
        mock.call(
            'sudo sed -i "s|# Allow data files dir access|  /scratch/mysql/ r, /scratch/mysql/** rwk, /scratch/tmp/ r, /scratch/tmp/** rwk, /proc/*/status r, /sys/devices/system/node/ r, /sys/devices/system/node/node*/meminfo r, /sys/devices/system/node/*/* r, /sys/devices/system/node/* r, # Allow data files dir access|g" /etc/apparmor.d/usr.sbin.mysqld'
        ),
        mock.call('sudo apparmor_parser -r /etc/apparmor.d/usr.sbin.mysqld'),
        mock.call('sudo systemctl restart apparmor'),
        mock.call(
            'sudo sed -i "s|datadir\t\t= /var/lib/mysql|datadir\t\t= /scratch/mysql|g" None'
        ),
        mock.call(
            'sudo sed -i "s|tmpdir\t\t= /tmp|tmpdir\t\t= /scratch/tmp|g" None'),
        mock.call(
            'echo "\ninnodb_buffer_pool_size = 100G\ninnodb_flush_method = O_DIRECT\ninnodb_flush_neighbors = 0\ninnodb_log_file_size = 1000M" | sudo tee -a None'
        ),
        mock.call(
            'echo "\nskip-name-resolve\nconnect_timeout        = 86400\nwait_timeout        = 86400\ninteractive_timeout        = 86400" | sudo tee -a None'
        ),
        mock.call('sudo sed -i "s/bind-address/#bind-address/g" None'),
        mock.call(
            'sudo sed -i "s/max_allowed_packet\t= 16M/max_allowed_packet\t= 1024M/g" None'
        ),
        mock.call('echo "\nlog_error_verbosity        = 3" | sudo tee -a None'),
        mock.call(
            'sudo cat /etc/mysql/mysql.conf.d/mysql.sock',
            ignore_failure=True,
            should_log=True),
        mock.call('sudo service None restart'),
        mock.call('sudo cat None', should_log=True),
        mock.call(
            'mysql -h localhost -P 3306 -u root -pperfkitbenchmarker -e "SET GLOBAL max_connections=8000;"'
        ),
        mock.call(
            'mysql -h localhost -P 3306 -u root -pperfkitbenchmarker -e "CREATE USER \'root\'@\'None\' IDENTIFIED BY \'perfkitbenchmarker\';"'
        ),
        mock.call(
            'mysql -h localhost -P 3306 -u root -pperfkitbenchmarker -e "GRANT ALL PRIVILEGES ON *.* TO \'root\'@\'None\';"'
        ),
        mock.call(
            'mysql -h localhost -P 3306 -u root -pperfkitbenchmarker -e "FLUSH PRIVILEGES;"'
        )
    ]

    self.assertCountEqual(  # use assertCountEqual because order is undefined
        remote_command.call_args_list, command)


if __name__ == '__main__':
  unittest.main()
