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
from typing import Optional

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import db_util
from perfkitbenchmarker import iaas_relational_db
from perfkitbenchmarker import os_types
from perfkitbenchmarker import relational_db
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.gcp import util as gcp_util

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

  ENGINE = sql_engine_utils.SQLSERVER

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

  def SetVms(self, vm_groups):
    super().SetVms(vm_groups)
    if "servers" in vm_groups:
      self.server_vm = vm_groups["servers"][0]

    if self.spec.high_availability:
      assert len(vm_groups["servers"]) >= 3
      self.controller_vm = vm_groups["servers"][-1]
      self.replica_vms = vm_groups["servers"][1:-1]

  def _SetupWindowsUnamangedDatabase(self):
    self.spec.database_username = "sa"
    self.spec.database_password = db_util.GenerateRandomDbPassword()

    if self.spec.high_availability:
      if (self.spec.high_availability_type == "FCIMW"
          or self.spec.high_availability_type == "FCIS2D"):
        self.ConfigureSQLServerHaFci()
    else:
      ConfigureSQLServer(
          self.server_vm,
          self.spec.database_username,
          self.spec.database_password,
          )
      self.MoveSQLServerTempDb()

  def _SetEndpoint(self):
    """Set the DB endpoint for this instance during _PostCreate."""
    super()._SetEndpoint()
    if self.spec.high_availability:
      self.endpoint = "fcidnn.perflab.local"
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

  def ConfigureSQLServerHaFci(self):
    """Create SQL server HA deployment for performance testing."""
    server_vm = self.server_vm
    client_vm = self.client_vm
    controller_vm = self.controller_vm
    replica_vms = self.replica_vms

    win_password = vm_util.GenerateRandomWindowsPassword(
        vm_util.PASSWORD_LENGTH, "*!@#%^+=")
    ip_name = "fci-ip-{}".format(FLAGS.run_uri)

    reserved_ip = server_vm.CreateIpReservation(ip_name)
    disk_size = 2000
    if server_vm.disk_specs:
      disk_size = server_vm.disk_specs[0].disk_size
    self.CreateAndAttachSharedDisk(server_vm, disk_size, "pd-ssd",
                                   server_vm.name, replica_vms[0].name)
    # Install and configure AD components.
    self.PushAndRunPowershellScript(controller_vm,
                                    "setup_domain_controller.ps1",
                                    [win_password])
    controller_vm.Reboot()

    # Remove volumes and partitions created on the attached disks.
    # Disks will be initialized by S2D.
    self.PushAndRunPowershellScript(server_vm, "clean_disks.ps1")

    self.PushAndRunPowershellScript(server_vm, "set_dns_join_domain.ps1",
                                    [controller_vm.internal_ip, win_password])
    server_vm.Reboot()

    self.PushAndRunPowershellScript(replica_vms[0], "clean_disks.ps1")
    self.PushAndRunPowershellScript(replica_vms[0], "set_dns_join_domain.ps1",
                                    [controller_vm.internal_ip, win_password])
    replica_vms[0].Reboot()

    self.PushAndRunPowershellScript(client_vm, "set_dns.ps1",
                                    [controller_vm.internal_ip])

    # Install all components needed to create and configure failover cluster.
    self.PushAndRunPowershellScript(controller_vm,
                                    "install_cluster_components.ps1")
    controller_vm.Reboot()
    self.PushAndRunPowershellScript(server_vm, "install_cluster_components.ps1")
    server_vm.Reboot()
    self.PushAndRunPowershellScript(replica_vms[0],
                                    "install_cluster_components.ps1")
    replica_vms[0].Reboot()

    # Setup cluster witness.
    self.PushAndRunPowershellScript(controller_vm, "setup_witness.ps1")
    self.PushAndRunPowershellScript(server_vm, "setup_fci_cluster.ps1",
                                    [replica_vms[0].name, controller_vm.name,
                                     win_password])

    # Ensure all nodes in the cluster have access to the witness share
    self.PushAndRunPowershellScript(controller_vm, "grant_witness_access.ps1")

    # Uninstall existing SQL server.
    # FCI cluster requires different installation to what comes with the image.
    server_vm.RemoteCommand("C:\\sql_server_install\\Setup.exe "
                            "/Action=Uninstall /FEATURES=SQL,AS,IS,RS "
                            "/INSTANCENAME=MSSQLSERVER /Q")
    server_vm.Reboot()
    replica_vms[0].RemoteCommand("C:\\sql_server_install\\Setup.exe "
                                 "/Action=Uninstall /FEATURES=SQL,AS,IS,RS "
                                 "/INSTANCENAME=MSSQLSERVER /Q")
    replica_vms[0].Reboot()

    if self.spec.high_availability_type == "FCIMW":
      self.PushAndRunPowershellScript(server_vm, "setup_mw_volume.ps1",
                                      [win_password])
    else:
      # Configure S2D pool and volumes.
      # All available storage (both PD and local SSD) will be used
      self.PushAndRunPowershellScript(server_vm, "setup_s2d_volumes.ps1",
                                      [win_password])

    # install SQL server into newly created cluster
    self.PushAndRunPowershellScript(server_vm,
                                    "setup_sql_server_first_node.ps1",
                                    [reserved_ip.ip_address, win_password])
    self.PushAndRunPowershellScript(replica_vms[0],
                                    "add_sql_server_second_node.ps1",
                                    [reserved_ip.ip_address, win_password])

    # Install SQL server updates.
    # Installation media present on the system is very outdated and it
    # does not support DNN connection for SQL.
    self.PushAndRunPowershellScript(server_vm, "update_sql_server.ps1")
    self.PushAndRunPowershellScript(replica_vms[0], "update_sql_server.ps1")

    # Update variables user for connection to SQL server.
    self.spec.database_password = win_password
    self.spec.endpoint = "fcidnn.perflab.local"

  def PushAndRunPowershellScript(
      self,
      vm: virtual_machine.VirtualMachine,
      script_name: str,
      cmd_parameters: Optional[list[str]] = None,
      source_path: str = "relational_db_configs/sqlserver_ha_configs/"
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

  def CreateAndAttachSharedDisk(self,
                                server_vm: virtual_machine.VirtualMachine,
                                disk_size: int,
                                disk_type: str,
                                server_vm_name: str,
                                replica_vm_name: str) -> None:
    """Releases existing IP reservation.

    Args:
      server_vm: server VM .
      disk_size: size of the disk to be created.
      disk_type: multi-writer disk type.
      server_vm_name: name of the first SQL node where disk will be attached.
      replica_vm_name: name of the second SQL node.
    """

    shared_disk_name = "pkb-{}-mw-disk-0".format(FLAGS.run_uri)
    cmd = gcp_util.GcloudCommand(
        server_vm,
        "beta",
        "compute",
        "disks",
        "create",
        shared_disk_name,
        f"--type={disk_type}",
        "--multi-writer",
        f"--zone={server_vm.zone}",
        f"--size={disk_size}",
    )
    cmd.Issue()

    cmd = gcp_util.GcloudCommand(
        server_vm,
        "compute",
        "instances",
        "attach-disk",
        server_vm_name,
        f"--disk={shared_disk_name}",
    )
    cmd.Issue()

    cmd = gcp_util.GcloudCommand(
        server_vm,
        "compute",
        "instances",
        "attach-disk",
        replica_vm_name,
        f"--disk={shared_disk_name}",
    )
    cmd.Issue()


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


def MoveSQLServerTempDb():
  """Moves the SQL Server temporary database to LocalSSD."""


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
