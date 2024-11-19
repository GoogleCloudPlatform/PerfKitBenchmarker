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

from perfkitbenchmarker import data
from perfkitbenchmarker import os_types


SYSBENCH_PASSWORD = 'Syb3enCh#1'
SHARED_BUFFERS_CONF = {
    'SIZE_10GB': {
        'shared_buffers': '10GB',
        'effective_cache_size': '30GB',
        'max_memory': '40G',
        'nr_hugepages': '5632',
    },
    'SIZE_100GB': {
        'shared_buffers': '100GB',
        'effective_cache_size': '112.5GB',
        'max_memory': '150G',
        'nr_hugepages': '52736',
    },
}
OS_DEPENDENT_DEFAULTS = {
    'centos': {
        'postgres_path': '/usr/pgsql-16',
        'data_dir': '/var/lib/pgsql/16/data',
        'conf_dir': '/var/lib/pgsql/16/data',
        'disk_mount_point': '/var/lib/pgsql/16',
        'postgres_service_name': 'postgresql-16'
    },
    'debian': {
        'postgres_path': '/usr/lib/postgresql/16',
        'data_dir': '/etc/postgresql/16/data/data',
        'conf_dir': '/etc/postgresql/16/main',
        'disk_mount_point': '/etc/postgresql/16/data',
        'postgres_service_name': 'postgresql',
    },
    'amazonlinux': {
        'data_dir': '/var/lib/pgsql/data',
        'conf_dir': '/var/lib/pgsql/data',
        'disk_mount_point': '/var/lib/pgsql',
        'postgres_service_name': 'postgresql',
    }
}


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
  thp_append = 'sudo tee -a /usr/lib/systemd/system/disable-thp.service'
  vm.RemoteCommand('sudo touch /usr/lib/systemd/system/disable-thp.service')
  disable_huge_pages = f"""[Unit]
    Description=Disable Transparent Huge Pages (THP)
    DefaultDependencies=no
    After=sysinit.target local-fs.target
    Before={GetOSDependentDefaults(vm.OS_TYPE)["postgres_service_name"]}.service

    [Service]
    Type=oneshot
    ExecStart=/bin/sh -c 'echo never | tee /sys/kernel/mm/transparent_hugepage/enabled > /dev/null'

    [Install]
    WantedBy=basic.target
    """
  vm.RemoteCommand(f'echo "{disable_huge_pages}" | {thp_append}')
  vm.RemoteCommand(
      'sudo chown root:root /usr/lib/systemd/system/disable-thp.service'
  )
  vm.RemoteCommand(
      'sudo chmod 0600 /usr/lib/systemd/system/disable-thp.service'
  )
  vm.RemoteCommand('sudo systemctl daemon-reload')
  vm.RemoteCommand(
      'sudo systemctl enable disable-thp.service && sudo systemctl start'
      ' disable-thp.service'
  )
  vm.Reboot()


def YumInstall(vm):
  """Installs the postgres package on the VM."""
  if vm.OS_TYPE not in os_types.AMAZONLINUX_TYPES:
    vm.RemoteCommand('sudo dnf config-manager --set-enabled crb')
    vm.RemoteCommand('sudo dnf install -y epel-release epel-next-release')
    vm.RemoteCommand(
        'sudo yum install -y https://download.postgresql.org/pub/repos/yum/'
        'reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm --skip-broken'
    )
    vm.RemoteCommand('sudo dnf -qy module disable postgresql')
  else:
    vm.RemoteCommand('sudo dnf update')
  vm.RemoteCommand(
      'sudo yum install -y postgresql16-server postgresql16'
      ' postgresql16-contrib'
  )


def AptInstall(vm):
  """Installs the postgres package on the VM."""
  vm.RemoteCommand('sudo apt-get install -y postgresql-common')
  vm.RemoteCommand(
      'sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y'
  )
  vm.RemoteCommand('sudo apt-get update')
  vm.RemoteCommand('sudo apt-get install -y postgresql-contrib-16')
  vm.RemoteCommand('sudo apt-get -y install postgresql-16')


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
    return OS_DEPENDENT_DEFAULTS['centos']
  elif os_type in os_types.AMAZONLINUX_TYPES:
    return OS_DEPENDENT_DEFAULTS['amazonlinux']
  else:
    return OS_DEPENDENT_DEFAULTS['debian']


def ConfigureAndRestart(vm, run_uri):
  """Configure and restart postgres.

  Args:
    vm: virtual machine to configure postgres on.
    run_uri: run uri to use for password generation.
  """
  conf_path = GetOSDependentDefaults(vm.OS_TYPE)['conf_dir']
  data_path = GetOSDependentDefaults(vm.OS_TYPE)['data_dir']
  conf_template_config = 'postgresql/postgresql-custom.conf.j2'
  remote_temp_config = '/tmp/my.cnf'
  postgres_conf_path = os.path.join(conf_path, 'postgresql-custom.conf')
  pg_hba_conf_path = os.path.join(conf_path, 'pg_hba.conf')
  database_queries_path = os.path.join(conf_path, 'queries.sql')
  database_setup_queries = 'postgresql/database_setup_queries.sql.j2'
  context = {
      'listen_address': vm.internal_ip,
      'shared_buffers': SHARED_BUFFERS_CONF['SIZE_10GB']['shared_buffers'],
      'effective_cache_size': SHARED_BUFFERS_CONF['SIZE_10GB'][
          'effective_cache_size'
      ],
      'data_directory': data_path,
  }
  vm.RenderTemplate(
      data.ResourcePath(conf_template_config),
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
  vm.RemoteCommand(
      'sudo sysctl -w'
      f' vm.nr_hugepages={SHARED_BUFFERS_CONF["SIZE_10GB"]["nr_hugepages"]}'
  )
  vm.RemoteCommand(
      'sudo sysctl -w vm.hugetlb_shm_group=$(getent group postgres | cut -d:'
      ' -f3)'
  )
  vm.RemoteCommand('cat /proc/meminfo | grep -i "^hugepage"')
  vm.RemoteCommand('sudo cat /proc/sys/vm/hugetlb_shm_group')
  vm.RemoteCommand(
      'sudo systemctl set-property'
      f' {GetOSDependentDefaults(vm.OS_TYPE)["postgres_service_name"]}.service'
      f' MemoryMax={SHARED_BUFFERS_CONF["SIZE_10GB"]["max_memory"]}'
  )
  vm.RemoteCommand('sudo sync; echo 3 | sudo tee -a /proc/sys/vm/drop_caches')
  vm.RemoteCommand(
      f'cat /etc/systemd/system.control/{GetOSDependentDefaults(vm.OS_TYPE)["postgres_service_name"]}.service.d/50-MemoryMax.conf'
  )
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
  vm.RemoteCommand(
      f'sudo su - postgres -c "psql -a -f {database_queries_path}"'
  )


def SetupReplica(primary_vm, replica_vm, replica_id, run_uri):
  """Setup postgres replica."""
  data_path = GetOSDependentDefaults(replica_vm.OS_TYPE)['data_dir']
  replica_vm.RemoteCommand(f'sudo mkdir -p {data_path}')
  replica_vm.RemoteCommand(f'sudo chown postgres:root {data_path}')
  replica_vm.RemoteCommand(
      'sudo su - postgres -c'
      f' "PGPASSWORD="{GetPsqlUserPassword(run_uri)}"'
      f' pg_basebackup -h {primary_vm.internal_ip} -U repl -p 5432 -D'
      f' {data_path} -S slot{replica_id} -Fp -P -Xs -R"'
  )
  context = {
      'listen_address': 'localhost',
      'shared_buffers': SHARED_BUFFERS_CONF['SIZE_10GB']['shared_buffers'],
      'effective_cache_size': SHARED_BUFFERS_CONF['SIZE_10GB'][
          'effective_cache_size'
      ],
      'data_directory': data_path,
  }
  conf_path = GetOSDependentDefaults(replica_vm.OS_TYPE)['conf_dir']
  conf_template_config = 'postgresql/postgresql-custom.conf.j2'
  remote_temp_config = '/tmp/my.cnf'
  postgres_conf_path = os.path.join(conf_path, 'postgresql-custom.conf')
  replica_vm.RenderTemplate(
      data.ResourcePath(conf_template_config),
      remote_temp_config,
      context,
  )
  replica_vm.RemoteCommand(f'sudo cp {remote_temp_config} {postgres_conf_path}')
  # vm.RemoteCommand('sudo chmod 660 postgresql.conf ')
  replica_vm.RemoteCommand(
      'sudo sysctl -w'
      f' vm.nr_hugepages={SHARED_BUFFERS_CONF["SIZE_10GB"]["nr_hugepages"]}'
  )
  replica_vm.RemoteCommand(
      'sudo sysctl -w vm.hugetlb_shm_group=$(getent group postgres | cut -d:'
      ' -f3)'
  )
  replica_vm.RemoteCommand('cat /proc/meminfo |grep -i "^hugepage"')
  replica_vm.RemoteCommand('sudo cat /proc/sys/vm/hugetlb_shm_group')
  replica_vm.RemoteCommand(
      'sudo systemctl set-property'
      f' {GetOSDependentDefaults(replica_vm.OS_TYPE)["postgres_service_name"]}.service'
      f' MemoryMax={SHARED_BUFFERS_CONF["SIZE_10GB"]["max_memory"]}'
  )
  replica_vm.RemoteCommand(
      'sudo sync; echo 3 | sudo tee -a /proc/sys/vm/drop_caches'
  )
  replica_vm.RemoteCommand(
      f'cat /etc/systemd/system.control/{GetOSDependentDefaults(replica_vm.OS_TYPE)["postgres_service_name"]}.service.d/50-MemoryMax.conf'
  )
  replica_vm.RemoteCommand(
      'sudo systemctl restart'
      f' {GetOSDependentDefaults(replica_vm.OS_TYPE)["postgres_service_name"]}'
  )


def GetPsqlUserPassword(run_uri):
  return run_uri + '_' + SYSBENCH_PASSWORD
