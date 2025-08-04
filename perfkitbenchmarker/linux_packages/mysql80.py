# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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


"""Module containing mysql installation and cleanup functions."""

import logging
import re

from perfkitbenchmarker import data
from perfkitbenchmarker import os_types
from perfkitbenchmarker import virtual_machine


MYSQL_PSWD = 'perfkitbenchmarker'
PACKAGE_NAME = 'mysql'

# OS dependent service defaults.
MYSQL_SERVICE_NAME = 'MYSQL_SERVICE_NAME'
MYSQL_CONFIG_PATH = 'MYSQL_CONFIG_PATH'
MYSQL_LOG_PATH = 'MYSQL_LOG_PATH'
OS_DEPENDENT_DEFAULTS = {
    'debian': {
        MYSQL_SERVICE_NAME: 'mysql',
        MYSQL_CONFIG_PATH: '/etc/mysql/mysql.conf.d/mysqld.cnf',
        MYSQL_LOG_PATH: '/var/log/mysql/error.log',
    },
    'centos': {
        MYSQL_SERVICE_NAME: 'mysqld',
        MYSQL_CONFIG_PATH: '/etc/my.cnf',
        MYSQL_LOG_PATH: '/var/log/mysqld.log',
    },
}


def YumInstall(vm):
  """Installs the mysql package on the VM."""
  if vm.OS_TYPE not in os_types.AMAZONLINUX_TYPES:
    vm.RemoteCommand('sudo dnf config-manager --set-enabled crb')
    vm.RemoteCommand('sudo dnf install -y epel-release epel-next-release')
  vm.RemoteCommand(
      'sudo yum -y install '
      'https://dev.mysql.com/get/mysql80-community-release-el9-5.noarch.rpm'
  )
  vm.RemoteCommand('sudo dnf config-manager --enable mysql80-community')
  vm.RemoteCommand('sudo dnf config-manager --enable mysql-tools-community')
  vm.RemoteCommand(
      'sudo yum install -y mysql-community-server mysql-community-client luajit'
      ' libaio screen mysql-community-libs'
  )


def AptInstall(vm):
  """Installs the mysql package on the VM."""
  vm.RemoteCommand(
      'wget -c https://repo.mysql.com//mysql-apt-config_0.8.17-1_all.deb'
  )
  vm.RemoteCommand(
      'echo mysql-apt-config mysql-apt-config/select-server'
      ' select mysql-8.0 | sudo debconf-set-selections'
  )
  vm.RemoteCommand(
      'echo mysql-apt-config mysql-apt-config/select-product'
      ' select Ok | sudo debconf-set-selections'
  )
  vm.RemoteCommand(
      'sudo -E DEBIAN_FRONTEND=noninteractive dpkg -i'
      ' mysql-apt-config_0.8.17-1_all.deb'
  )

  _, stderr = vm.RemoteCommand('sudo apt-get update', ignore_failure=True)

  if stderr:
    if 'public key is not available:' in stderr:
      # This error is due to mysql updated the repository and the public
      # key is not updated.
      # Import the updated public key
      match = re.match('.*NO_PUBKEY ([A-Z0-9]*)', stderr)
      if match:
        key = match.group(1)
        vm.RemoteCommand(
            'sudo apt-key adv '
            f'--keyserver keyserver.ubuntu.com --recv-keys {key}'
        )
      else:
        raise RuntimeError('No public key found by regex.')
    else:
      raise RuntimeError(stderr)

  vm.RemoteCommand(
      'echo "mysql-server-8.0 mysql-server/root_password password '
      f'{MYSQL_PSWD}" | sudo debconf-set-selections'
  )
  vm.RemoteCommand(
      'echo "mysql-server-8.0 mysql-server/root_password_again '
      f'password {MYSQL_PSWD}" | sudo debconf-set-selections'
  )
  vm.InstallPackages('mysql-server')


def YumGetPathToConfig(vm):
  """Returns the path to the mysql config file."""
  raise NotImplementedError


def AptGetPathToConfig(vm):
  """Returns the path to the mysql config file."""
  del vm
  return '/etc/mysql/mysql.conf.d/mysqld.cnf'


def YumGetServiceName(vm):
  """Returns the name of the mysql service."""
  raise NotImplementedError


def AptGetServiceName(vm):
  """Returns the name of the mysql service."""
  del vm
  return 'mysql'


def ConfigureSystemSettings(vm: virtual_machine.VirtualMachine):
  """Percona system settings.

  These system settings are what Percona (consulting firm) applies to
  the mysql instances that they build for their customers.
  Currently tested for centos_stream9 and Ubuntu only.

  Args:
    vm: The VM to configure.
  """
  if vm.OS_TYPE not in os_types.LINUX_OS_TYPES:
    logging.error(
        'System settings not configured for unsupported OS: %s', vm.os_info)  # pytype: disable=attribute-error
    return
  sysctl_append = 'sudo tee -a /etc/sysctl.conf'
  vm.RemoteCommand(f'echo "vm.swappiness=1" | {sysctl_append}')
  vm.RemoteCommand(f'echo "vm.dirty_ratio=15" | {sysctl_append}')
  vm.RemoteCommand(f'echo "vm.dirty_background_ratio=5" | {sysctl_append}')
  vm.RemoteCommand(f'echo "net.core.somaxconn=65535" | {sysctl_append}')
  vm.RemoteCommand(
      f'echo "net.core.netdev_max_backlog=65535" | {sysctl_append}')
  vm.RemoteCommand(
      f'echo "net.ipv4.tcp_max_syn_backlog=65535" | {sysctl_append}')
  vm.RemoteCommand(
      f'echo "net.ipv4.ip_local_port_range=4000 65000" | {sysctl_append}')
  vm.RemoteCommand(f'echo "net.ipv4.tcp_tw_reuse=1" | {sysctl_append}')
  vm.RemoteCommand(f'echo "net.ipv4.tcp_fin_timeout=5" | {sysctl_append}')
  vm.RemoteCommand('sudo sysctl -p')

  limits_append = 'sudo tee -a /etc/security/limits.conf'
  vm.RemoteCommand(f'echo "*     soft    nofile  64000" | {limits_append}')
  vm.RemoteCommand(f'echo "*     hard    nofile  64000" | {limits_append}')
  vm.RemoteCommand(f'echo "*     soft    memlock unlimited" | {limits_append}')
  vm.RemoteCommand(f'echo "*     hard    memlock unlimited" | {limits_append}')

  auth_append = 'sudo tee -a /etc/pam.d/login'
  vm.RemoteCommand(f'echo "session required pam_limits.so" | {auth_append}')

  vm.Reboot()


def GetOSDependentDefaults(os_type: str) -> dict[str, str]:
  """Returns the OS family."""
  if os_type in os_types.CENTOS_TYPES or os_type in os_types.AMAZONLINUX_TYPES:
    return OS_DEPENDENT_DEFAULTS['centos']
  else:
    return OS_DEPENDENT_DEFAULTS['debian']


def ConfigureAndRestart(
    vm: virtual_machine.VirtualMachine,
    buffer_pool_size: str,
    server_id: int,
    config_template: str,
):
  """Configure and restart mysql."""
  remote_temp_config = '/tmp/my.cnf'
  remote_final_config = GetOSDependentDefaults(vm.OS_TYPE)[MYSQL_CONFIG_PATH]
  config_d_service = 'mysql/mysqld.service'
  remote_temp_d_service = '/tmp/mysqld'
  remote_final_d_service = '/lib/systemd/system/mysqld.service'
  logrotation = 'mysql/logrotation'
  remote_temp_logrotation = '/tmp/logrotation'
  remote_final_logrotation = '/etc/logrotate.d/mysqld'
  remote_final_log_dir = GetOSDependentDefaults(vm.OS_TYPE)[MYSQL_LOG_PATH]
  service_name = GetOSDependentDefaults(vm.OS_TYPE)[MYSQL_SERVICE_NAME]
  context = {
      'scratch_dir': vm.GetScratchDir(),
      'server_id': str(server_id),
      'buffer_pool_size': buffer_pool_size,
      'log_dir': remote_final_log_dir,
  }
  vm.RenderTemplate(
      data.ResourcePath(config_template), remote_temp_config, context
  )
  vm.RemoteCommand(f'sudo cp {remote_temp_config} {remote_final_config}')
  vm.PushDataFile(config_d_service, remote_temp_d_service)
  vm.RemoteCommand(f'sudo cp {remote_temp_d_service} {remote_final_d_service}')
  vm.PushDataFile(logrotation, remote_temp_logrotation)
  vm.RemoteCommand(
      f'sudo cp {remote_temp_logrotation} {remote_final_logrotation}'
  )
  vm.RemoteCommand(f'sudo chmod 0644 {remote_final_logrotation}')
  vm.RemoteCommand('sudo systemctl daemon-reload')
  vm.RemoteCommand(f'sudo systemctl stop {service_name}')
  vm.RemoteCommand(f'sudo systemctl start {service_name}')


def UpdatePassword(vm: virtual_machine.VirtualMachine, new_password: str):
  """Update the password of the root user."""
  log_path = GetOSDependentDefaults(vm.OS_TYPE)[MYSQL_LOG_PATH]
  password = vm.RemoteCommand(
      f'sudo grep "A temporary password" {log_path} | '
      'sed "s/.*generated for root@localhost: //"'
  )[0].strip()
  if not password:
    password = MYSQL_PSWD
  vm.RemoteCommand(
      f"""sudo mysql --connect-timeout=10 -uroot --password='{password}' """
      """--connect-expired-password -e "alter user 'root'@'localhost' """
      f'''identified by '{new_password}';"'''
  )
  vm.RemoteCommand('sudo touch /root/.my.cnf')
  vm.RemoteCommand(
      f"""echo "[client]\nuser=root\npassword='{new_password}'" | """
      """sudo tee -a /root/.my.cnf"""
  )
  tmp_path = '/tmp/update_passwords.sql'
  vm.RenderTemplate(
      data.ResourcePath('mysql/update_passwords.sql.j2'),
      tmp_path,
      {'password': new_password},
  )
  vm.RemoteCommand(f'sudo mysql -uroot -p"{new_password}"< {tmp_path}')


def CreateDatabase(
    vm: virtual_machine.VirtualMachine, password: str, db_name: str
):
  """Create test db."""
  tmp_path = '/tmp/create_db.sql'
  vm.RenderTemplate(
      data.ResourcePath('mysql/create_db.sql.j2'),
      tmp_path,
      {'database_name': db_name, 'password': password},
  )
  vm.RemoteCommand(f'sudo mysql -uroot -p"{password}"< {tmp_path}')


def SetupReplica(
    vm: virtual_machine.VirtualMachine, password: str, master_ip: str):
  """Setup replica mysql server."""
  tmp_path = '/tmp/setup_repl.sql'
  vm.RenderTemplate(
      data.ResourcePath('mysql/setup_repl.sql.j2'),
      tmp_path,
      {'password': password, 'private_ip': master_ip},
  )
  vm.RemoteCommand(f'sudo mysql -uroot -p"{password}"< {tmp_path}')
