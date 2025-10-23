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

"""Module containing postgres installation functions."""

import os

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import os_types


FLAGS = flags.FLAGS

ALLOWED_VERSIONS = ['postgresql16', 'postgresql17']
_POSTGRESQL_VERSION = flags.DEFINE_enum(
    'postgresql_version',
    'postgresql16',
    enum_values=ALLOWED_VERSIONS,
    help='The postgres version used for benchmark test',
)

SYSBENCH_PASSWORD = 'Syb3enCh#1'
SHARED_BUFFERS_CONF = {
    'SIZE_10G': {
        'shared_buffers': '10GB',
        'effective_cache_size': '30GB',
        'max_memory': '40G',
        'nr_hugepages': '5632',
    },
    'SIZE_100G': {
        'shared_buffers': '100GB',
        'effective_cache_size': '112.5GB',
        'max_memory': '150G',
        'nr_hugepages': '52736',
    },
    'SIZE_80G': {  # run with 40M table size and 8 tables to write 80GB of data
        'shared_buffers': '80GB',
        'effective_cache_size': '90GB',
        'max_memory': '120G',
        'nr_hugepages': '45000',
    },
    'SIZE_42G': {  # run with 21M table size and 8 tables to write 42GB of data
        'shared_buffers': '42GB',
        'effective_cache_size': '89.6GB',
        'max_memory': '108.8G',
        'nr_hugepages': '23000',
    },
}


def GetPostgresVersion():
  """Returns the last two characters of the postgresql_version flag."""
  return _POSTGRESQL_VERSION.value[-2:]


def GetOSDependentDefaultsConfig(os_type: str) -> dict[str, str]:
  """Returns a dictionary of OS-dependent settings based on flags.

  Args:
    os_type: The OS type of the VM.
  """
  version = GetPostgresVersion()

  configs = {
      'centos': {
          'postgres_path': f'/usr/pgsql-{version}',
          'data_dir': f'/var/lib/pgsql/{version}/data',
          'conf_dir': f'/var/lib/pgsql/{version}/data',
          'disk_mount_point': f'/var/lib/pgsql/{version}',
          'postgres_service_name': f'postgresql-{version}',
      },
      'debian': {
          'postgres_path': f'/usr/lib/postgresql/{version}',
          'data_dir': f'/etc/postgresql/{version}/data/data',
          'conf_dir': f'/etc/postgresql/{version}/main',
          'disk_mount_point': f'/etc/postgresql/{version}/data',
          'postgres_service_name': 'postgresql',
          'postgres_template_service_name': f'postgresql@{version}-main',
      },
      'amazonlinux': {
          'data_dir': '/var/lib/pgsql/data',
          'conf_dir': '/var/lib/pgsql/data',
          'disk_mount_point': '/var/lib/pgsql',
          'postgres_service_name': 'postgresql',
      },
  }
  if os_type in configs:
    return configs[os_type]
  else:
    raise ValueError(f'Unsupported OS type: {os_type}')


def ConfigureSystemSettings(vm):
  """Tune OS for postgres."""
  sysctl_append = 'sudo tee -a /etc/sysctl.conf'
  sysctl_data = (
      'vm.swappiness=1\nvm.dirty_ratio=15\nvm.dirty_background_ratio=5\n'
      'net.core.somaxconn=65535\nnet.core.netdev_max_backlog=65535\n'
      'net.ipv4.tcp_max_syn_backlog=65535\n'
      'net.ipv4.ip_local_port_range=4000 65000\nnet.ipv4.tcp_tw_reuse=1\n'
      'net.ipv4.tcp_fin_timeout=5'
  )
  vm.RemoteCommand(f'echo """{sysctl_data}""" | {sysctl_append}')
  vm.RemoteCommand('sudo sysctl -p')

  limits_append = 'sudo tee -a /etc/security/limits.conf'
  vm.RemoteCommand(f'echo "*     soft    nofile  64000" | {limits_append}')
  vm.RemoteCommand(f'echo "*     hard    nofile  64000" | {limits_append}')

  vm.RemoteCommand(
      'echo "session    required pam_limits.so" | sudo tee -a /etc/pam.d/login'
  )
  vm.RemoteCommand('sudo cat /sys/kernel/mm/transparent_hugepage/enabled')
  vm.Reboot()


def YumInstall(vm):
  """Installs the postgres package on the VM."""
  if vm.OS_TYPE not in os_types.AMAZONLINUX_TYPES:
    vm.RemoteCommand('sudo dnf config-manager --set-enabled crb')
    vm.RemoteCommand('sudo dnf install -y epel-release epel-next-release')
    if vm.is_aarch64:
      repo = 'EL-9-aarch64'
    else:
      repo = 'EL-9-x86_64'
    vm.RemoteCommand(
        'sudo yum install -y https://download.postgresql.org/pub/repos/yum/'
        f'reporpms/{repo}/pgdg-redhat-repo-latest.noarch.rpm --skip-broken'
    )
    vm.RemoteCommand('sudo dnf -qy module disable postgresql')
  else:
    vm.RemoteCommand('sudo dnf update')
  postgresql_version = _POSTGRESQL_VERSION.value
  postgresql_devel = f'{postgresql_version}-devel'
  if vm.OS_TYPE in os_types.AMAZONLINUX_TYPES:
    postgresql_devel = 'postgresql-devel'
  vm.RemoteCommand(
      f'sudo yum install -y {postgresql_version}-server {postgresql_version}'
      f' {postgresql_version}-contrib {postgresql_devel}'
  )
  vm.RemoteCommand(
      f'echo "export PATH=/usr/pgsql-{GetPostgresVersion()}/bin:$PATH" |'
      ' sudo tee -a ~/.bashrc'
  )
  vm.RemoteCommand('pg_config --version')


def AptInstall(vm):
  """Installs the postgres package on the VM."""
  vm.RemoteCommand('sudo apt-get install -y postgresql-common')
  vm.RemoteCommand(
      'sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y'
  )
  vm.RemoteCommand('sudo apt-get update')
  version_number = GetPostgresVersion()
  vm.RemoteCommand(
      f'sudo apt-get install -y postgresql-contrib-{version_number}'
  )
  vm.RemoteCommand(f'sudo apt-get -y install postgresql-{version_number}')


def InitializeDatabase(vm):
  """Initialize the database."""
  if vm.OS_TYPE in os_types.AMAZONLINUX_TYPES:
    vm.RemoteCommand('sudo postgresql-setup --initdb')
    return
  postgres_path = GetOSDependentDefaults(vm.OS_TYPE)['postgres_path']
  data_path = GetOSDependentDefaults(vm.OS_TYPE)['data_dir']
  vm.RemoteCommand(
      f'sudo mkdir -p {data_path} && sudo chown postgres:root'
      f' {data_path}'
  )
  vm.RemoteCommand(
      'sudo -u postgres'
      f' {postgres_path}/bin/initdb -D'
      f' {data_path}'
  )


def GetOSDependentDefaults(os_type: str) -> dict[str, str]:
  """Returns the OS family."""
  if os_type in os_types.CENTOS_TYPES:
    return GetOSDependentDefaultsConfig('centos')
  elif os_type in os_types.AMAZONLINUX_TYPES:
    return GetOSDependentDefaultsConfig('amazonlinux')
  else:
    return GetOSDependentDefaultsConfig('debian')


def IsUbuntu(vm):
  """Returns whether the VM is Debian."""
  return vm.OS_TYPE in os_types.UBUNTU_OS_TYPES


def ConfigureAndRestart(vm, run_uri, buffer_size, conf_template_path):
  """Configure and restart postgres.

  Args:
    vm: virtual machine to configure postgres on.
    run_uri: run uri to use for password generation.
    buffer_size: buffer size to use for postgres.
    conf_template_path: path to the postgres conf template file.
  """
  conf_path = GetOSDependentDefaults(vm.OS_TYPE)['conf_dir']
  data_path = GetOSDependentDefaults(vm.OS_TYPE)['data_dir']
  buffer_size_key = f'SIZE_{buffer_size}'
  remote_temp_config = '/tmp/my.cnf'
  postgres_conf_path = os.path.join(conf_path, 'postgresql-custom.conf')
  pg_hba_conf_path = os.path.join(conf_path, 'pg_hba.conf')
  database_queries_path = os.path.join(conf_path, 'queries.sql')
  database_setup_queries = 'postgresql/database_setup_queries.sql.j2'
  context = {
      'listen_address': vm.internal_ip,
      'shared_buffers': SHARED_BUFFERS_CONF[buffer_size_key]['shared_buffers'],
      'effective_cache_size': SHARED_BUFFERS_CONF[buffer_size_key][
          'effective_cache_size'
      ],
      'data_directory': data_path,
      'host_address': vm.internal_ip,
      'password': GetPsqlUserPassword(run_uri),
  }
  vm.RenderTemplate(
      data.ResourcePath(conf_template_path),
      remote_temp_config,
      context,
  )
  vm.RemoteCommand(f'sudo cp {remote_temp_config} {postgres_conf_path}')
  vm.RemoteCommand(
      'sudo echo -e "\ninclude = postgresql-custom.conf" | sudo tee -a'
      f' {os.path.join(conf_path, "postgresql.conf")}'
  )
  vm.RenderTemplate(
      data.ResourcePath('postgresql/pg_hba.conf.j2'),
      '/tmp/pg_hba.conf',
      {},
  )
  vm.RemoteCommand(f'sudo cp /tmp/pg_hba.conf {pg_hba_conf_path}')
  vm.RenderTemplate(
      data.ResourcePath(database_setup_queries),
      '/tmp/queries.sql',
      {
          'repl_user_password': GetPsqlUserPassword(run_uri),
          'sysbench_user_password': GetPsqlUserPassword(run_uri),
          'pmm_user_password': GetPsqlUserPassword(run_uri),
      },
  )
  vm.RemoteCommand(f'sudo cp /tmp/queries.sql {database_queries_path}')
  vm.RemoteCommand(f'sudo chmod 755 {database_queries_path}')
  # changes made to /proc do not persist after a reboot
  vm.RemoteCommand('sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')
  postgres_service_name = GetOSDependentDefaults(vm.OS_TYPE)[
      'postgres_service_name'
  ]
  UpdateHugePages(vm, buffer_size_key)

  if IsUbuntu(vm):
    postgres_service_name = GetOSDependentDefaults(
        vm.OS_TYPE
    )['postgres_template_service_name']
  UpdateMaxMemory(vm, buffer_size_key, postgres_service_name)

  vm.RemoteCommand(
      'sudo su - postgres -c "openssl req -new -x509 -days 365 -nodes -text'
      f' -out {data_path}/server.crt -keyout'
      f' {data_path}/server.key -subj "/CN=`hostname`""'
  )
  vm.RemoteCommand(f'sudo chmod 755 {postgres_conf_path}')
  vm.RemoteCommand(
      'sudo systemctl restart'
      f' {GetOSDependentDefaults(vm.OS_TYPE)["postgres_service_name"]}'
  )
  vm.RemoteCommand(f'sudo systemctl status {postgres_service_name}')
  vm.RemoteCommand(
      f'sudo su - postgres -c "psql -a -f {database_queries_path}"'
  )


def UpdateHugePages(vm, buffer_size_key):
  vm.RemoteCommand(
      'sudo sysctl -w'
      f' vm.nr_hugepages={SHARED_BUFFERS_CONF[buffer_size_key]["nr_hugepages"]}'
  )
  vm.RemoteCommand(
      'sudo sysctl -w vm.hugetlb_shm_group=$(getent group postgres | cut -d:'
      ' -f3)'
  )
  vm.RemoteCommand('cat /proc/meminfo | grep -i "^hugepage"')
  vm.RemoteCommand('sudo cat /proc/sys/vm/hugetlb_shm_group')


def UpdateMaxMemory(vm, buffer_size_key, postgres_service_name):
  vm.RemoteCommand(
      'sudo systemctl set-property'
      f' {postgres_service_name}.service'
      f' MemoryMax={SHARED_BUFFERS_CONF[buffer_size_key]["max_memory"]}'
  )
  vm.RemoteCommand(
      f'cat /etc/systemd/system.control/{postgres_service_name}.service.d/50-MemoryMax.conf'
  )


def SetupReplica(
    primary_vm, replica_vm, replica_id, run_uri, buffer_size, conf_template_path
):
  """Setup postgres replica."""
  buffer_size_key = f'SIZE_{buffer_size}'
  data_path = GetOSDependentDefaults(replica_vm.OS_TYPE)['data_dir']
  conf_path = GetOSDependentDefaults(replica_vm.OS_TYPE)['conf_dir']
  replica_vm.RemoteCommand(f'sudo mkdir -p {data_path}')
  replica_vm.RemoteCommand(f'sudo chown postgres:root {data_path}')
  replica_vm.RemoteCommand(
      'sudo su - postgres -c'
      f' "PGPASSWORD="{GetPsqlUserPassword(run_uri)}"'
      f' pg_basebackup -h {primary_vm.internal_ip} -U repl -p 5432 -v -D'
      f' {data_path} -S slot{replica_id} -Fp -P -Xs -R"'
  )
  context = {
      'listen_address': 'localhost',
      'shared_buffers': SHARED_BUFFERS_CONF[buffer_size_key]['shared_buffers'],
      'effective_cache_size': SHARED_BUFFERS_CONF[buffer_size_key][
          'effective_cache_size'
      ],
      'data_directory': data_path,
  }
  remote_temp_config = '/tmp/my.cnf'
  postgres_conf_path = os.path.join(conf_path, 'postgresql-custom.conf')
  replica_vm.RenderTemplate(
      data.ResourcePath(conf_template_path),
      remote_temp_config,
      context,
  )
  replica_vm.RemoteCommand(f'sudo cp {remote_temp_config} {postgres_conf_path}')
  replica_vm.RemoteCommand(
      'sudo echo -e "\ninclude = postgresql-custom.conf" | sudo tee -a'
      f' {os.path.join(conf_path, "postgresql.conf")}'
  )
  postgres_service_name = GetOSDependentDefaults(replica_vm.OS_TYPE)[
      'postgres_service_name'
  ]
  UpdateHugePages(replica_vm, buffer_size_key)
  UpdateMaxMemory(replica_vm, buffer_size_key, postgres_service_name)
  replica_vm.RemoteCommand(
      'sudo sync; echo 3 | sudo tee -a /proc/sys/vm/drop_caches'
  )
  if IsUbuntu(replica_vm):
    postgres_service_name = GetOSDependentDefaults(
        replica_vm.OS_TYPE
    )['postgres_template_service_name']
    UpdateMaxMemory(replica_vm, buffer_size_key, postgres_service_name)

  replica_vm.RemoteCommand(f'sudo chown -R postgres:root {data_path}')
  replica_vm.RemoteCommand(f'sudo chown -R postgres:root {conf_path}')
  replica_vm.RemoteCommand(
      f'sudo chmod 700 {data_path}'
  )
  replica_vm.RemoteCommand(
      'sudo systemctl restart'
      f' {GetOSDependentDefaults(replica_vm.OS_TYPE)["postgres_service_name"]}'
  )


def GetPsqlUserPassword(run_uri):
  return run_uri + '_' + SYSBENCH_PASSWORD
