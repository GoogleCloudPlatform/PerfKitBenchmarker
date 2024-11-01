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

import ntpath
import os

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import db_util
from perfkitbenchmarker import errors
from perfkitbenchmarker import iaas_relational_db
from perfkitbenchmarker import os_types
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util

CONTROLLER_SCRIPT_PATH = (
    "relational_db_configs/sqlserver_ha_configs/controller.ps1"
)

FCI_CLUSTER_SCRIPT_PATH = (
    "relational_db_configs/sqlserver_ha_configs/fci_cluster.ps1"
)

FLAGS = flags.FLAGS

IS_READY_TIMEOUT = 600  # 10 minutes
MSSQL_PID = "developer"  # Edition of SQL server on Linux

DEFAULT_ENGINE_VERSION = sql_engine_utils.SQLSERVER_ENTERPRISE


class SQLServerIAASRelationalDb(iaas_relational_db.IAASRelationalDb):
  """Object representing a IAAS relational database Service."""
  TEMPDB_DISK_LETTER = "T"

  ENGINE = sql_engine_utils.SQLSERVER

  def __init__(self, relational_db_spec):
    """Initialize the SQLServer IAAS relational database object.

    Args:
      relational_db_spec: spec of the managed database.
    """
    super().__init__(relational_db_spec)
    self.ConfirmFlags()

  @property
  def replica_vms(self):
    """Server VM of replicas for hosting a managed database.

    Raises:
      RelationalDbPropertyNotSetError: if the server_vm is missing.

    Returns:
      The replica vms.
    """
    if not hasattr(self, "_replica_vms"):
      raise relational_db.RelationalDbPropertyNotSetError(
          "replica_vms is not set"
      )
    return self._replica_vms

  @replica_vms.setter
  def replica_vms(self, replica_vms):
    self._replica_vms = replica_vms

  @property
  def controller_vm(self):
    """Server VM of replicas for hosting a managed database.

    Raises:
      RelationalDbPropertyNotSetError: if the server_vm is missing.

    Returns:
      The replica vms.
    """
    if not hasattr(self, "_controller_vm"):
      raise relational_db.RelationalDbPropertyNotSetError(
          "controller_vm is not set"
      )
    return self._controller_vm

  @controller_vm.setter
  def controller_vm(self, controller_vm):
    self._controller_vm = controller_vm

  def ConfirmFlags(self):
    """Confirm flags to make sure all the requirements are met."""
    # If high availability mode, set high availability type if needed
    if self.spec.high_availability and self.spec.high_availability_type is None:
      self.spec.high_availability_type = "FCIS2D"

  def SetVms(self, vm_groups):
    super().SetVms(vm_groups)
    if "servers" in vm_groups:
      self.server_vm = vm_groups["servers"][0]

    if self.spec.high_availability:
      assert len(vm_groups["servers_replicas"]) >= 1
      self.replica_vms = vm_groups["servers_replicas"]
      self.controller_vm = vm_groups["controller"][0]

  def MoveSQLServerTempDb(self):
    """Moves the SQL Server temporary database to LocalSSD."""
    vms = [self.server_vm]
    if self.spec.high_availability_type == "AOAG" and self.replica_vms:
      vms.extend(self.replica_vms)

    # Moves the SQL Server temporary database to LocalSSD.
    for vm in vms:
      stdout, _ = vm.RemoteCommand(
          "Get-PSDrive -PSProvider FileSystem | Select Name"
      )

      drive_list = [
          str(drive.strip().replace("\r", ""))
          for drive in stdout.split("\n")
          if drive
      ]

      if self.TEMPDB_DISK_LETTER in drive_list:
        stdout, _ = vm.RemoteCommand(
            'sqlcmd -h -1 -Q "SET NOCOUNT '
            " ON; select f.name + CASE WHEN "
            "f.type = 1 THEN '.ldf' "
            "ELSE '.mdf' END "
            "FROM sys.master_files "
            "f WHERE f.database_id"
            " = DB_ID('tempdb');\""
        )
        tmp_db_files_list = [
            str(tmp_file.strip().replace("\r", ""))
            for tmp_file in stdout.split("\n")
            if tmp_file
        ]

        for tmp_db_file in tmp_db_files_list:
          tmp_db_name = tmp_db_file.split(".")[0]
          vm.RemoteCommand(
              'sqlcmd -Q "ALTER DATABASE tempdb '
              "MODIFY FILE (NAME = [{}], "
              "FILENAME = '{}:\\TEMPDB\\{}');\"".format(
                  tmp_db_name, self.TEMPDB_DISK_LETTER, tmp_db_file
              )
          )

        vm.RemoteCommand("Restart-Service MSSQLSERVER -Force")

  def _SetupWindowsUnamangedDatabase(self):
    self.spec.database_username = "sa"
    self.spec.database_password = db_util.GenerateRandomDbPassword()

    if self.spec.high_availability:
      if (self.spec.high_availability_type == "FCIMW"
          or self.spec.high_availability_type == "FCIS2D"):
        self.ConfigureSQLServerHaFci()
      elif self.spec.high_availability_type == "AOAG":
        self.ConfigureSQLServerHaAoag()
        ConfigureSQLServer(
            self.server_vm,
            self.spec.database_username,
            self.spec.database_password)
        ConfigureSQLServer(
            self.replica_vms[0],
            self.spec.database_username,
            self.spec.database_password)
        self.MoveSQLServerTempDb()
        self.EnsurePrimaryReplica()
    else:
      ConfigureSQLServer(
          self.server_vm,
          self.spec.database_username,
          self.spec.database_password)
      self.MoveSQLServerTempDb()

  def _SetEndpoint(self):
    """Set the DB endpoint for this instance during _PostCreate."""
    super()._SetEndpoint()
    if self.spec.high_availability:
      perf_domain = "perf" + FLAGS.run_uri[:6]
      self.endpoint = "fcidnn.{}.local".format(perf_domain)
    else:
      self.endpoint = self.server_vm.internal_ip

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
    self.spec.database_password = vm_util.GenerateRandomWindowsPassword(
        password_length=vm_util.PASSWORD_LENGTH, special_chars="*!@#+"
    )

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
    # Enabling FUA I/O subsystem capability per MSFT
    # https://learn.microsoft.com/en-us/sql/linux/sql-server-linux-performance-best-practices?view=sql-server-ver16
    self.server_vm.RemoteCommand(
        "sudo /opt/mssql/bin/mssql-conf set "
        "control.alternatewritethrough 0"
    )
    self.server_vm.RemoteCommand(
        "sudo /opt/mssql/bin/mssql-conf set "
        "control.writethrough 1"
    )
    self.server_vm.RemoteCommand(
        "sudo /opt/mssql/bin/mssql-conf set "
        "traceflag.traceflag 3979"
    )

    if self.server_vm.OS_TYPE == os_types.DEFAULT:
      self.server_vm.Install("mssql_tools")

    self.MoveSQLServerTempDBLinux()
    self.server_vm.RemoteCommand("sudo systemctl restart mssql-server")

    if self.server_vm.OS_TYPE == os_types.RHEL8:
      _TuneForSQL(self.server_vm)

  def MoveSQLServerTempDBLinux(self):
    vm = self.server_vm
    stdout, _ = vm.RemoteCommand(
        "/opt/mssql-tools/bin/sqlcmd -C "
        "-S localhost -U sa -P \'{}\' -h -1 -Q  \"SET NOCOUNT ON; "
        "SELECT f.name + SUBSTRING(f.physical_name, "
        "CHARINDEX('.', f.physical_name), LEN(f.physical_name) -1) "
        "FROM sys.master_files f "
        "WHERE f.database_id = DB_ID('tempdb');\""
        .format(self.spec.database_password)
        )

    tmp_db_files_list = [
        str(tmp_file.strip().replace("\r", ""))
        for tmp_file in stdout.split("\n")
        if tmp_file
    ]

    for tmp_db_file in tmp_db_files_list:
      tmp_db_name = tmp_db_file.split(".")[0]
      vm.RemoteCommand(
          "/opt/mssql-tools/bin/sqlcmd -C "
          "-S localhost -U sa -P \'{}\' -h -1 -Q "
          "\"ALTER DATABASE tempdb "
          "MODIFY FILE (NAME = [{}], "
          "FILENAME = '/scratch/mssqltemp/{}');\""
          .format(self.spec.database_password, tmp_db_name, tmp_db_file))

  @staticmethod
  def GetDefaultEngineVersion(engine):
    """Returns the default version of a given database engine.

    Args:
      engine (string): type of database (my_sql or postgres).

    Returns:
      (string): Default version for the given database engine.
    """
    return DEFAULT_ENGINE_VERSION

  def ConfigureSQLServerHaFci(self):
    """Create SQL server HA deployment for performance testing."""
    server_vm = self.server_vm
    client_vm = self.client_vm
    controller_vm = self.controller_vm
    replica_vms = self.replica_vms

    win_password = vm_util.GenerateRandomWindowsPassword(
        vm_util.PASSWORD_LENGTH, "*!@#%^+="
    )

    ip_address = self.CreateIpReservation()
    perf_domain = "perf" + FLAGS.run_uri[:6]

    # Install and configure AD components.
    self.PushAndRunPowershellScript(
        controller_vm, "setup_domain_controller.ps1",
        [win_password, perf_domain, self.spec.cloud])
    controller_vm.Reboot()
    self.PushAndRunPowershellScript(
        controller_vm, "add_user_to_domain_groups.ps1",
        [win_password, perf_domain])
    # Remove volumes and partitions created on the attached disks.
    # Disks will be initialized by S2D.
    self.PushAndRunPowershellScript(
        server_vm, "clean_disks.ps1")
    server_vm.Reboot()
    self.PushAndRunPowershellScript(
        server_vm, "set_dns_join_domain.ps1",
        [controller_vm.internal_ip, win_password, perf_domain])
    server_vm.Reboot()

    self.PushAndRunPowershellScript(
        replica_vms[0], "clean_disks.ps1")
    replica_vms[0].Reboot()

    self.PushAndRunPowershellScript(
        replica_vms[0], "set_dns_join_domain.ps1",
        [controller_vm.internal_ip, win_password, perf_domain])
    replica_vms[0].Reboot()

    self.PushAndRunPowershellScript(
        client_vm, "set_dns.ps1", [controller_vm.internal_ip])

    # Install all components needed to create and configure failover cluster.
    self.PushAndRunPowershellScript(
        controller_vm, "install_cluster_components.ps1")
    controller_vm.Reboot()
    self.PushAndRunPowershellScript(
        server_vm, "install_cluster_components.ps1")
    server_vm.Reboot()
    self.PushAndRunPowershellScript(
        replica_vms[0], "install_cluster_components.ps1")
    replica_vms[0].Reboot()

    # Setup cluster witness.
    self.PushAndRunPowershellScript(
        controller_vm, "setup_witness.ps1")
    self.PushAndRunPowershellScript(
        server_vm, "setup_fci_cluster.ps1",
        [replica_vms[0].hostname, win_password, perf_domain])

    # Ensure all nodes in the cluster have access to the witness share
    self.PushAndRunPowershellScript(
        controller_vm, "grant_witness_access.ps1")

    # Uninstall existing SQL server.
    # FCI cluster requires different installation to what comes with the image.
    self.PushAndRunPowershellScript(
        server_vm, "uninstall_sql_server.ps1")
    server_vm.Reboot()

    self.PushAndRunPowershellScript(
        replica_vms[0], "uninstall_sql_server.ps1")
    replica_vms[0].Reboot()
    server_vm.Reboot()

    if self.spec.high_availability_type == "FCIMW":
      # Configure MW cluster disks.
      self.PushAndRunPowershellScript(
          server_vm, "setup_mw_volume.ps1",
          [win_password, replica_vms[0].hostname, perf_domain])
    else:
      # Configure S2D pool and volumes.
      # All available storage (both PD and local SSD) will be used
      self.PushAndRunPowershellScript(
          server_vm, "setup_s2d_volumes.ps1",
          [win_password, replica_vms[0].hostname, perf_domain])

    # install SQL server into newly created cluster
    self.PushAndRunPowershellScript(
        server_vm, "setup_sql_server_first_node.ps1",
        [ip_address, win_password, perf_domain]
    )
    self.PushAndRunPowershellScript(
        replica_vms[0], "add_sql_server_second_node.ps1",
        [ip_address, win_password, perf_domain],
    )

    # Install SQL server updates.
    # Installation media present on the system is very outdated and it
    # does not support DNN connection for SQL.

    # Fetch the SQL server update link from MS site.
    sql_srv_vms = [server_vm, replica_vms[0]]
    sql_srv_updates_windows = {
        "2022": "KB5032679",
        "2019": "KB5031908",
        "2017": "KB5016884",
        "2016": "KB5029186",
        "2014": "KB5029185",
    }

    # for each database vm, fetch sql server update link and execute update

    for sql_srv_vm in sql_srv_vms:
      sql_server_version = self.GetSQLServerVersion(sql_srv_vm)
      if (sql_srv_updates_windows.get(sql_server_version) is not None):
        kb_number = sql_srv_updates_windows[sql_server_version]
        if (kb_number is not None and kb_number):
          self.PushAndRunPowershellScript(
              sql_srv_vm, "update_sql_server.ps1", [kb_number]
          )
          sql_srv_vm.Reboot()
    self.PushAndRunPowershellScript(
        server_vm, "check_sql_role_status_after_upgrade.ps1")

    # Update variables user for connection to SQL server.
    self.spec.database_password = win_password
    self.spec.endpoint = "fcidnn.{}.local".format(perf_domain)
    self.ReleaseIpReservation()

  def CreateIpReservation(self):
    """Create IP reservation for SQL server."""
    raise NotImplementedError("CreateIpReservation not implemented.")

  def ReleaseIpReservation(self):
    """Release IP reservation for SQL server."""
    raise NotImplementedError("ReleaseIpReservation not implemented.")

  def EnsurePrimaryReplica(self):
    """Move server_vm as primary node in AOAG."""
    # Set first server vm as primary if necessary
    out, _ = self.server_vm.RemoteCommand(
        """sqlcmd -Q \"
        SELECT
          role_desc
        FROM
          sys.dm_hadr_availability_replica_states ars
          JOIN sys.availability_groups ag ON ars.group_id = ag.group_id
        WHERE
          ag.name = '{}'
          AND is_local = 1;\"
        """.format(sql_engine_utils.SQLSERVER_AOAG_NAME))
    if "PRIMARY" not in out:
      self.server_vm.RemoteCommand(
          'sqlcmd -Q "ALTER AVAILABILITY GROUP [{}] FAILOVER"'.format(
              sql_engine_utils.SQLSERVER_AOAG_NAME))

  def ConfigureSQLServerHaAoag(self):
    """Create SQL server HA deployment for performance testing."""
    server_vm = self.server_vm
    client_vm = self.client_vm
    controller_vm = self.controller_vm
    replica_vms = self.replica_vms

    perf_domain = "perf" + FLAGS.run_uri[:6]
    win_password = vm_util.GenerateRandomWindowsPassword(
        vm_util.PASSWORD_LENGTH, "*!@#%^+=")

    # Install and configure AD components.
    self.PushAndRunPowershellScript(
        controller_vm, "setup_domain_controller.ps1",
        [win_password, perf_domain, self.spec.cloud])
    controller_vm.Reboot()
    self.PushAndRunPowershellScript(
        controller_vm, "add_user_to_domain_groups.ps1",
        [win_password, perf_domain])

    self.PushAndRunPowershellScript(
        server_vm, "set_dns_join_domain.ps1",
        [controller_vm.internal_ip, win_password, perf_domain])
    server_vm.Reboot()

    self.PushAndRunPowershellScript(
        replica_vms[0], "set_dns_join_domain.ps1",
        [controller_vm.internal_ip, win_password, perf_domain])
    replica_vms[0].Reboot()

    self.PushAndRunPowershellScript(
        client_vm, "set_dns.ps1", [controller_vm.internal_ip])

    # Install all components needed to create and configure failover cluster.
    self.PushAndRunPowershellScript(
        controller_vm, "install_cluster_components.ps1")
    controller_vm.Reboot()
    self.PushAndRunPowershellScript(
        server_vm, "install_cluster_components.ps1")
    server_vm.Reboot()
    self.PushAndRunPowershellScript(
        replica_vms[0], "install_cluster_components.ps1")
    replica_vms[0].Reboot()

    # Setup cluster witness.
    self.PushAndRunPowershellScript(
        controller_vm, "setup_witness.ps1")

    self.PushAndRunPowershellScript(
        server_vm, "setup_fci_cluster.ps1",
        [replica_vms[0].hostname, win_password, perf_domain])

    # Ensure all nodes in the cluster have access to the witness share
    self.PushAndRunPowershellScript(
        controller_vm, "grant_witness_access.ps1")

    retry_count = 0
    while retry_count < 3:
      try:
        server_vm.RemoteCommand(
            f"Enable-SqlAlwaysOn -ServerInstance {server_vm.name} -Force")
        break
      except errors.VirtualMachine.RemoteCommandError as e:
        retry_count += 1
        if retry_count >= 3:
          raise e

    retry_count = 0
    while retry_count < 3:
      try:
        replica_vms[0].RemoteCommand(
            f"Enable-SqlAlwaysOn -ServerInstance {replica_vms[0].name} -Force")
        break
      except errors.VirtualMachine.RemoteCommandError as e:
        retry_count += 1
        if retry_count >= 3:
          raise e

    # Create folder structure and dummy DB database for AOAG creation
    server_vm.RemoteCommand(r"mkdir F:\DATA; mkdir F:\Logs; mkdir F:\Backup")
    replica_vms[0].RemoteCommand(
        r"mkdir F:\DATA; mkdir F:\Logs; mkdir F:\Backup")

    server_vm.RemoteCommand("""sqlcmd -Q \"
        USE [master]
        GO
        ALTER LOGIN [sa] ENABLE
        GO
        CREATE DATABASE [{0}] ON PRIMARY (
          NAME = [{0}],
          FILENAME='F:\\Data\\{0}.mdf',
          SIZE = 256MB,
          MAXSIZE = UNLIMITED,
          FILEGROWTH = 256MB)
        LOG ON (
          NAME = '{0}_log',
          FILENAME='F:\\Logs\\{0}.ldf',
          SIZE = 256MB,
          MAXSIZE = UNLIMITED,
          FILEGROWTH = 256MB)
        GO
        USE [{0}]
        GO
        EXEC sp_changedbowner 'sa'
        GO
        USE [master]
        GO
        ALTER DATABASE [{0}] SET RECOVERY FULL
        GO
        BACKUP DATABASE [{0}] TO DISK = 'F:\\Backup\\{0}.bak'
        GO\"
        """.format(sql_engine_utils.SQLSERVER_AOAG_DB_NAME))

    # Change log on for MSSQLSERVICE to perflab\adminuser
    # Default PowerShell version doesn't have Set-Service -Credential option
    server_vm.RemoteCommand(
        'sc.exe config MSSQLSERVER obj= "{0}\\pkbadminuser" password='
        f' "{win_password}" type= own'.format(perf_domain)
    )
    replica_vms[0].RemoteCommand(
        'sc.exe config MSSQLSERVER obj= "{0}\\pkbadminuser" password='
        f' "{win_password}" type= own'.format(perf_domain)
    )

    # create AOAG
    # running all the AOAG query from SQL server errors with Login
    # failed to for user 'NT AUTHORITY\ANONYMOUS LOGON double
    server_vm.RemoteCommand(
        """sqlcmd -Q \"--- YOU MUST EXECUTE THE FOLLOWING SCRIPT IN SQLCMD MODE.
        USE [master]
        GO

        CREATE ENDPOINT [Hadr_endpoint]
          AS TCP (LISTENER_PORT = 5022)
            FOR DATA_MIRRORING (ROLE = ALL, ENCRYPTION = REQUIRED ALGORITHM AES)
        GO

        IF (SELECT state FROM sys.endpoints WHERE name = N'Hadr_endpoint') <> 0
        BEGIN
            ALTER ENDPOINT [Hadr_endpoint] STATE = STARTED
        END
        GO

        CREATE LOGIN [{0}\\pkbadminuser] FROM WINDOWS WITH DEFAULT_DATABASE=[master]
        GO
        ALTER SERVER ROLE [sysadmin] ADD MEMBER [{0}\\pkbadminuser]
        GO

        GRANT CONNECT ON ENDPOINT::[Hadr_endpoint] TO [{0}\\pkbadminuser]
        GO

        IF EXISTS(SELECT * FROM sys.server_event_sessions WHERE name='AlwaysOn_health')
        BEGIN
            ALTER EVENT SESSION [AlwaysOn_health] ON SERVER WITH (STARTUP_STATE=ON);
        END
        IF NOT EXISTS(SELECT * FROM sys.dm_xe_sessions WHERE name='AlwaysOn_health')
        BEGIN
            ALTER EVENT SESSION [AlwaysOn_health] ON SERVER STATE=START;
        END
        GO\"
        """.format(perf_domain)
        )

    replica_vms[0].RemoteCommand(
        """sqlcmd -Q \"--- YOU MUST EXECUTE THE FOLLOWING SCRIPT IN SQLCMD MODE.
        USE [master]
        GO

        ALTER LOGIN [sa] ENABLE
        GO

        CREATE ENDPOINT [Hadr_endpoint]
            AS TCP (LISTENER_PORT = 5022)
        FOR DATA_MIRRORING (ROLE = ALL, ENCRYPTION = REQUIRED ALGORITHM AES)
        GO

        IF (SELECT state FROM sys.endpoints WHERE name = N'Hadr_endpoint') <> 0
        BEGIN
            ALTER ENDPOINT [Hadr_endpoint] STATE = STARTED
        END
        GO

        CREATE LOGIN [{0}\\pkbadminuser] FROM WINDOWS WITH DEFAULT_DATABASE=[master]
        GO
        ALTER SERVER ROLE [sysadmin] ADD MEMBER [{0}\\pkbadminuser]
        GO

        GRANT CONNECT ON ENDPOINT::[Hadr_endpoint] TO [{0}\\pkbadminuser]
        GO

        IF EXISTS(SELECT * FROM sys.server_event_sessions WHERE name='AlwaysOn_health')
        BEGIN
            ALTER EVENT SESSION [AlwaysOn_health] ON SERVER WITH (STARTUP_STATE=ON);
        END
        IF NOT EXISTS(SELECT * FROM sys.dm_xe_sessions WHERE name='AlwaysOn_health')
        BEGIN
            ALTER EVENT SESSION [AlwaysOn_health] ON SERVER STATE=START;
        END
        GO\"
        """.format(perf_domain)
        )

    server_vm.RemoteCommand(
        """sqlcmd -Q \"--- YOU MUST EXECUTE THE FOLLOWING SCRIPT IN SQLCMD MODE.
        USE [master]
        GO

        CREATE AVAILABILITY GROUP [{3}]
        WITH (AUTOMATED_BACKUP_PREFERENCE = SECONDARY,
        DB_FAILOVER = OFF,
        DTC_SUPPORT = NONE,
        REQUIRED_SYNCHRONIZED_SECONDARIES_TO_COMMIT = 0)
        FOR DATABASE [{2}]

        REPLICA ON N'{0}' WITH (ENDPOINT_URL = N'TCP://{0}.{4}.local:5022', FAILOVER_MODE = AUTOMATIC, AVAILABILITY_MODE = SYNCHRONOUS_COMMIT, BACKUP_PRIORITY = 50, SEEDING_MODE = AUTOMATIC, SECONDARY_ROLE(ALLOW_CONNECTIONS = NO)),
            N'{1}' WITH (ENDPOINT_URL = N'TCP://{1}.{4}.local:5022', FAILOVER_MODE = AUTOMATIC, AVAILABILITY_MODE = SYNCHRONOUS_COMMIT, BACKUP_PRIORITY = 50, SEEDING_MODE = AUTOMATIC, SECONDARY_ROLE(ALLOW_CONNECTIONS = NO));
        GO\"
        """.format(self.server_vm.name,
                   self.replica_vms[0].name,
                   sql_engine_utils.SQLSERVER_AOAG_DB_NAME,
                   sql_engine_utils.SQLSERVER_AOAG_NAME,
                   perf_domain))

    replica_vms[0].RemoteCommand(
        """sqlcmd -Q \"--- YOU MUST EXECUTE THE FOLLOWING SCRIPT IN SQLCMD MODE.
        ALTER AVAILABILITY GROUP [{0}] JOIN;
        GO

        ALTER AVAILABILITY GROUP [{0}] GRANT CREATE ANY DATABASE;
        GO\"
        """.format(sql_engine_utils.SQLSERVER_AOAG_NAME))

    # Restart SQL Service for AOAG replication to begin
    server_vm.RemoteCommand("Restart-Service MSSQLSERVER -Force")
    replica_vms[0].RemoteCommand("Restart-Service MSSQLSERVER -Force")

    # Add DNN listener
    self.PushAndRunPowershellScript(
        server_vm,
        "add_dnn_listener.ps1",
        [sql_engine_utils.SQLSERVER_AOAG_NAME, "fcidnn", "1533"])

    server_vm.RemoteCommand(
        """sqlcmd -Q \"
        ALTER AVAILABILITY GROUP [{1}] REMOVE DATABASE [{0}];
        DROP DATABASE [{0}];\"
        """.format(sql_engine_utils.SQLSERVER_AOAG_DB_NAME,
                   sql_engine_utils.SQLSERVER_AOAG_NAME))

    # Update variables user for connection to SQL server.
    self.spec.database_password = win_password
    self.spec.endpoint = "fcidnn.{}.local".format(perf_domain)
    self.port = 1533

  def PushAndRunPowershellScript(
      self,
      vm: virtual_machine.VirtualMachine,
      script_name: str,
      cmd_parameters: list[str] | None = None,
      source_path: str = "relational_db_configs/sqlserver_ha_configs/",
  ) -> tuple[str, str]:
    """Pushes a powershell script to VM and run it.

    Args:
      vm: vm where script will be uploaded and executed
      script_name: name of the script to upload and execute
      cmd_parameters: optional command parameters
      source_path: script source location

    Returns:
      command execution output.
    """
    if cmd_parameters is None:
      cmd_parameters = []
    script_path_on_vm = ntpath.join(vm.temp_dir, script_name)
    script_path = data.ResourcePath(os.path.join(source_path, script_name))
    vm.PushFile(script_path, script_path_on_vm)

    return vm.RemoteCommand("powershell {} {}"
                            .format(script_path_on_vm,
                                    " ".join(cmd_parameters)))

  def RestartDatabase(self):
    """Restarts all the database services in the benchmark."""
    vms = [self.server_vm]
    if self.spec.high_availability and self.replica_vms:
      vms.append(self.replica_vms)
    for vm in vms:
      if vm.BASE_OS_TYPE == os_types.WINDOWS:
        vm.RemoteCommand("Restart-Service MSSQLSERVER -Force")
      else:
        vm.RemoteCommand("sudo systemctl restart mssql-server")

  def GetSQLServerVersion(
      self,
      vm: virtual_machine.VirtualMachine
  ) -> str:
    """Fetch SQL Server Version of the vm.

    Args:
      vm: vm

    Returns:
      SQL Server Version
    """

    return_value: str = None
    sql_srv_version_prefix = "sqlserver_"
    sql_srv_supported_versions = [
        "2022",
        "2019",
        "2017",
        "2016",
        "2014",
        "2012"
    ]

    for version in sql_srv_supported_versions:
      if vm.OS_TYPE.find(sql_srv_version_prefix+version) > 0:
        return_value = version
        break
    return return_value


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
  vm.RemoteCommand("net stop SQLSERVERAGENT /y", ignore_failure=True)
  vm.RemoteCommand("net stop mssqlserver /y")
  vm.RemoteCommand("net start mssqlserver")
  vm.RemoteCommand("net start SQLSERVERAGENT", ignore_failure=True)


def _TuneForSQL(vm):
  """Set TuneD settings specific to SQL Server on RedHat."""
  tune_settings = (
      "# A TuneD configuration for SQL Server on Linux \n"
      "[main] \n"
      "summary=Optimize for Microsoft SQL Server \n"
      "include=throughput-performance \n\n"
      "[cpu] \n"
      "force_latency=5\n\n"
      "[vm] \n"
      "# For multi-instance SQL deployments use 'madvise' instead of 'always'\n"
      "transparent_hugepages=always \n\n"
      "[sysctl]\n"
      "vm.swappiness = 1\n"
      "vm.dirty_background_ratio = 3\n"
      "vm.dirty_ratio = 80\n"
      "vm.dirty_expire_centisecs = 500\n"
      "vm.dirty_writeback_centisecs = 100\n"
      "vm.max_map_count=1600000\n"
      "net.core.rmem_default = 262144\n"
      "net.core.rmem_max = 4194304\n"
      "net.core.wmem_default = 262144\n"
      "net.core.wmem_max = 1048576\n"
      "kernel.numa_balancing=0\n\n"
      "[scheduler]\n"
      "sched_latency_ns=60000000\n"
      "sched_migration_cost_ns=500000\n"
      "sched_min_granularity_ns=15000000\n"
      "sched_wakeup_granularity_ns=2000000\n"
      "\n"
  )
  vm.RemoteCommand("sudo mkdir -p /usr/lib/tuned/mssql")
  vm.RemoteCommand(
      'echo "{}" | sudo tee /usr/lib/tuned/mssql/tuned.conf'.format(
          tune_settings
      )
  )

  vm.RemoteCommand("sudo chmod +x /usr/lib/tuned/mssql/tuned.conf")
  vm.RemoteCommand("sudo tuned-adm profile mssql")
  vm.RemoteCommand("sudo tuned-adm active")
  vm.Reboot()


def ConfigureSQLServerLinux(vm, username: str, password: str):
  """Update the username and password on a SQL Server."""
  vm.RemoteCommand(
      f'/opt/mssql-tools/bin/sqlcmd -C -Q "ALTER LOGIN {username} ENABLE;"'
  )
  vm.RemoteCommand(
      "/opt/mssql-tools/bin/sqlcmd -C -Q "
      f"\"ALTER LOGIN sa WITH PASSWORD = '{password}';\""
  )
  vm.RemoteCommand("sudo systemctl restart mssql-server")
