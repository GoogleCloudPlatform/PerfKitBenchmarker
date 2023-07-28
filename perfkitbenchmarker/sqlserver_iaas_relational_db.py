# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Responsible for IAAS relational database provisioning.

This class is responsible to provide helper methods for IAAS relational
database.
"""

from absl import flags
from perfkitbenchmarker import db_util
from perfkitbenchmarker import iaas_relational_db
from perfkitbenchmarker import os_types
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

IS_READY_TIMEOUT = 600  # 10 minutes
MSSQL_PID = "developer"  # Edition of SQL server on Linux

DEFAULT_ENGINE_VERSION = sql_engine_utils.SQLSERVER_ENTERPRISE


class SQLServerIAASRelationalDb(iaas_relational_db.IAASRelationalDb):
  """Object representing a IAAS relational database Service."""

  ENGINE = sql_engine_utils.SQLSERVER

  def _SetupWindowsUnamangedDatabase(self):
    self.spec.database_username = "sa"
    self.spec.database_password = db_util.GenerateRandomDbPassword()
    ConfigureSQLServer(
        self.server_vm,
        self.spec.database_username,
        self.spec.database_password,
    )

  def _SetupLinuxUnmanagedDatabase(self):
    if self.spec.engine_version == "2022":
      mssql_name = "mssql2022"
    elif self.spec.engine_version == "2019":
      mssql_name = "mssql2019"
    else:
      raise NotImplementedError(
          "Invalid database engine version: {}. "
          "Only 2019 and 2022 are supported.".format(self.spec.engine_version)
      )

    self.spec.database_username = "sa"
    self.spec.database_password = vm_util.GenerateRandomWindowsPassword()

    self.server_vm.Install(mssql_name)
    self.server_vm.RemoteCommand(
        "sudo MSSQL_SA_PASSWORD={} "
        "MSSQL_PID={} /opt/mssql/bin/mssql-conf -n "
        "setup accept-eula".format(self.spec.database_password, MSSQL_PID)
    )

    self.server_vm.RemoteCommand("sudo systemctl restart mssql-server")

    self.server_vm.RemoteCommand("sudo mkdir -p /scratch/mssqldata")
    self.server_vm.RemoteCommand("sudo mkdir -p /scratch/mssqllog")
    self.server_vm.RemoteCommand("sudo mkdir -p /scratch/mssqltemp")

    self.server_vm.RemoteCommand("sudo chown mssql /scratch/mssqldata")
    self.server_vm.RemoteCommand("sudo chown mssql /scratch/mssqllog")
    self.server_vm.RemoteCommand("sudo chown mssql /scratch/mssqltemp")

    self.server_vm.RemoteCommand("sudo chgrp mssql /scratch/mssqldata")
    self.server_vm.RemoteCommand("sudo chgrp mssql /scratch/mssqllog")
    self.server_vm.RemoteCommand("sudo chgrp mssql /scratch/mssqltemp")

    self.server_vm.RemoteCommand(
        "sudo /opt/mssql/bin/mssql-conf set "
        "filelocation.defaultdatadir "
        "/scratch/mssqldata"
    )
    self.server_vm.RemoteCommand(
        "sudo /opt/mssql/bin/mssql-conf set "
        "filelocation.defaultlogdir "
        "/scratch/mssqllog"
    )
    self.server_vm.RemoteCommand("sudo systemctl restart mssql-server")

    if self.server_vm.OS_TYPE == os_types.RHEL8:
      _TuneForSQL(self.server_vm)

  @staticmethod
  def GetDefaultEngineVersion(engine):
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (my_sql or postgres).

    Returns:
      (string): Default version for the given database engine.
    """
    return DEFAULT_ENGINE_VERSION


def ConfigureSQLServer(vm, username: str, password: str):
  """Update the username and password on a SQL Server."""
  vm.RemoteCommand(f'sqlcmd -Q "ALTER LOGIN {username} ENABLE;"')
  vm.RemoteCommand(
      f"sqlcmd -Q \"ALTER LOGIN sa WITH PASSWORD = '{password}' ;\""
  )
  vm.RemoteCommand(
      "sqlcmd -Q \"EXEC xp_instance_regwrite N'HKEY_LOCAL_MACHINE', "
      "N'Software\\Microsoft\\MSSQLServer\\MSSQLServer', "
      "N'LoginMode', REG_DWORD, 2\""
  )
  vm.RemoteCommand(
      "sqlcmd -Q \"EXEC xp_instance_regwrite N'HKEY_LOCAL_MACHINE', "
      "N'Software\\Microsoft\\MSSQLServer\\MSSQLServer', "
      "N'BackupDirectory', REG_SZ, N'C:\\scratch'\""
  )

  vm.RemoteCommand(
      "sqlcmd -Q \"EXEC xp_instance_regwrite N'HKEY_LOCAL_MACHINE', "
      "N'Software\\Microsoft\\MSSQLServer\\MSSQLServer', "
      f"N'DefaultData', REG_SZ, N'{vm.assigned_disk_letter}:\\'\""
  )
  vm.RemoteCommand(
      "sqlcmd -Q \"EXEC xp_instance_regwrite N'HKEY_LOCAL_MACHINE', "
      "N'Software\\Microsoft\\MSSQLServer\\MSSQLServer', "
      f"N'DefaultLog', REG_SZ, N'{vm.assigned_disk_letter}:\\'\""
  )
  vm.RemoteCommand("net stop mssqlserver /y")
  vm.RemoteCommand("net start mssqlserver")


def _TuneForSQL(vm):
  """Set TuneD settings specific to SQL Server on RedHat."""
  tune_settings = (
      "# A TuneD configuration for SQL Server on Linux \n"
      "[main] \n"
      "summary=Optimize for Microsoft SQL Server \n"
      "include=throughput-performance \n\n"
      "[cpu] \n"
      "force_latency=5\n\n"
      "[sysctl]\n"
      "vm.swappiness = 1\n"
      "vm.dirty_background_ratio = 3\n"
      "vm.dirty_ratio = 80\n"
      "vm.dirty_expire_centisecs = 500\n"
      "vm.dirty_writeback_centisecs = 100\n"
      "vm.transparent_hugepages=always\n"
      "vm.max_map_count=1600000\n"
      "net.core.rmem_default = 262144\n"
      "net.core.rmem_max = 4194304\n"
      "net.core.wmem_default = 262144\n"
      "net.core.wmem_max = 1048576\n"
      "kernel.numa_balancing=0"
  )
  vm.RemoteCommand("sudo mkdir -p /usr/lib/tuned/mssql")
  vm.RemoteCommand(
      'echo "{}" | sudo tee /usr/lib/tuned/mssql/tuned.conf'.format(
          tune_settings
      )
  )

  vm.RemoteCommand("sudo chmod +x /usr/lib/tuned/mssql/tuned.conf")
  vm.RemoteCommand("sudo tuned-adm profile mssql")
  vm.RemoteCommand("sudo tuned-adm list")


def ConfigureSQLServerLinux(vm, username: str, password: str):
  """Update the username and password on a SQL Server."""
  vm.RemoteCommand(
      f'/opt/mssql-tools/bin/sqlcmd -Q "ALTER LOGIN {username} ENABLE;"'
  )
  vm.RemoteCommand(
      "/opt/mssql-tools/bin/sqlcmd -Q "
      f"\"ALTER LOGIN sa WITH PASSWORD = '{password}' ;\""
  )
  vm.RemoteCommand("sudo systemctl restart mssql-server")
