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

"""Module containing hammerdbcli functions on Windows."""

import ntpath
import posixpath
from typing import Any, List, Optional

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import hammerdb as linux_hammerdb

FLAGS = flags.FLAGS

P3RF_CLOUD_SQL_TEST_DIR = linux_hammerdb.P3RF_CLOUD_SQL_TEST_DIR

# Installation paths and sources etc.
HAMMERDB = 'HammerDB-{0}'
HAMMERDB_DIR = HAMMERDB + '-Win'
HAMMERDB_ZIP = HAMMERDB_DIR + '.zip'
HAMMERDB_URL = (
    'https://github.com/TPC-Council/HammerDB/releases/download/v{0}/'
    + HAMMERDB_ZIP
)

# import linux flags
HAMMERDB_SCRIPT = linux_hammerdb.HAMMERDB_SCRIPT
HAMMERDB_OPTIMIZED_SERVER_CONFIGURATION = (
    linux_hammerdb.HAMMERDB_OPTIMIZED_SERVER_CONFIGURATION
)
NON_OPTIMIZED = linux_hammerdb.NON_OPTIMIZED
MINIMUM_RECOVERY = linux_hammerdb.MINIMUM_RECOVERY

# Default run timeout
TIMEOUT = 60 * 60 * 20


class WindowsHammerDbTclScript(linux_hammerdb.HammerDbTclScript):
  """Represents a Hammerdb TCL script for Windows."""

  def Install(self, vm, tcl_script_parameters: Any):
    PushTestFile(vm, self.tcl_script_name, self.path)

    for parameter in self.needed_parameters:
      tcl_script_parameters.SearchAndReplaceInScript(
          vm, self.tcl_script_name, parameter
      )

  @vm_util.Retry(poll_interval=10, max_retries=3, timeout=TIMEOUT)
  def Run(self, vm, timeout: Optional[int] = 60 * 60 * 6) -> str:
    """Run hammerdbcli script."""
    hammerdb_exe_dir = ntpath.join(
        vm.temp_dir, HAMMERDB.format(linux_hammerdb.HAMMERDB_VERSION.value)
    )
    stdout, _ = vm.RobustRemoteCommand(
        f'cd {hammerdb_exe_dir} ; '
        f'.\\hammerdbcli.bat auto {self.tcl_script_name}',
        timeout=timeout,
    )

    self.CheckErrorFromHammerdb(stdout)
    return stdout


class WindowsTclScriptParameters(linux_hammerdb.TclScriptParameters):
  """Handle of the parameters that may be needed by a TCL script."""

  def SearchAndReplaceInScript(self, vm, script_name: str, parameter: str):
    SearchAndReplaceTclScript(
        vm, parameter, self.map_search_to_replace[parameter], script_name
    )


def _GetFileContent(vm, file_path: str) -> str:
  stdout, _ = vm.RemoteCommand(f' type {file_path}')
  return stdout


def ParseTpcCTimeProfileResultsFromFile(stdout: str) -> List[sample.Sample]:
  """Extracts latency result from time profile file."""
  return linux_hammerdb.ParseTpcCTimeProfileResultsFromFile(stdout)


def ParseTpcCTPMResultsFromFile(stdout: str) -> List[sample.Sample]:
  """Parse TPCC TPM metrics per seconds."""
  return linux_hammerdb.ParseTpcCTPMResultsFromFile(stdout)


def SetDefaultConfig(num_cpus: Optional[int]):
  return linux_hammerdb.SetDefaultConfig(num_cpus)


def ParseTpcCResults(stdout: str, vm) -> List[sample.Sample]:
  """Extract results from the TPC-C script."""
  tpcc_metrics = linux_hammerdb.ParseBasicTpcCResults(stdout)
  if linux_hammerdb.HAMMERDB_TPCC_TIME_PROFILE.value:
    tpcc_results = _GetFileContent(
        vm, ntpath.join(vm.temp_dir, '..', 'hdbxtprofile.log')
    )
    tpcc_metrics += ParseTpcCTimeProfileResultsFromFile(tpcc_results)

  if linux_hammerdb.TPCC_LOG_TRANSACTIONS.value:
    tpcc_results = _GetFileContent(
        vm, ntpath.join(vm.temp_dir, '..', 'hdbtcount.log')
    )
    tpcc_metrics += ParseTpcCTPMResultsFromFile(tpcc_results)
  return tpcc_metrics


def ParseTpcHResults(stdout: str) -> List[sample.Sample]:
  """Extract results from the TPC-H script."""
  return linux_hammerdb.ParseTpcHResults(stdout)


@vm_util.Retry(poll_interval=10, max_retries=3)
def SearchAndReplaceTclScript(vm, search: str, replace: str, script_name: str):
  hammerdb_exe_dir = ntpath.join(
      vm.temp_dir, HAMMERDB.format(linux_hammerdb.HAMMERDB_VERSION.value)
  )
  vm.RemoteCommand(
      f'cd {hammerdb_exe_dir} ; '
      f'(Get-Content {script_name}) '
      f'-replace "{search}", "{replace}" | '
      f'Set-Content {script_name} -encoding ASCII ; ',
      timeout=60 * 5,
  )


def Install(vm):
  """Installs hammerdbcli and dependencies on the VM."""
  version = linux_hammerdb.HAMMERDB_VERSION.value
  if version not in [linux_hammerdb.HAMMERDB_4_5, linux_hammerdb.HAMMERDB_4_3]:
    raise errors.Setup.InvalidFlagConfigurationError(
        f'Hammerdb version {version} is not supported on Windows. '
    )
  vm.Install('mssql_tools')
  zip_path = ntpath.join(vm.temp_dir, HAMMERDB_ZIP.format(version))
  vm.DownloadFile(HAMMERDB_URL.format(version), zip_path)
  vm.UnzipFile(zip_path, vm.temp_dir)


def SetupConfig(
    vm,
    db_engine: str,
    hammerdb_script: str,
    ip: str,
    port: int,
    password: str,
    user: str,
    is_managed_azure: bool,
):
  """Sets up the necessary scripts on the VM with the necessary parameters."""
  db_engine = sql_engine_utils.GetDbEngineType(db_engine)

  if db_engine not in linux_hammerdb.SCRIPT_MAPPING:
    raise ValueError(
        '{0} is currently not supported for running '
        'hammerdb benchmarks.'.format(db_engine)
    )

  if hammerdb_script not in linux_hammerdb.SCRIPT_MAPPING[db_engine]:
    raise ValueError(
        '{0} is not a known hammerdb script.'.format(hammerdb_script)
    )

  linux_scripts = linux_hammerdb.SCRIPT_MAPPING[db_engine][hammerdb_script]
  windows_scripts = [
      WindowsHammerDbTclScript(
          script.tcl_script_name,
          script.needed_parameters,
          script.path,
          script.script_type,
      )
      for script in linux_scripts
  ]

  for script in windows_scripts:
    script_parameters = WindowsTclScriptParameters(
        ip,
        port,
        password,
        user,
        is_managed_azure,
        hammerdb_script,
        script.script_type,
    )
    script.Install(vm, script_parameters)

  # Run all the build script or scripts before actual run phase
  for script in windows_scripts:
    if script.script_type == linux_hammerdb.BUILD_SCRIPT_TYPE:
      script.Run(vm, timeout=linux_hammerdb.HAMMERDB_BUILD_TIMEOUT.value + 600)


def Run(
    vm, db_engine: str, hammerdb_script: str, timeout: Optional[int] = TIMEOUT
) -> List[sample.Sample]:
  """Run the HammerDB Benchmark.

  Runs Hammerdb TPCC or TPCH script.
  TPCC gathers TPM (transactions per minute) and NOPM (new order per minute).
  Definitions can be found here:

  https://www.hammerdb.com/blog/uncategorized/why-both-tpm-and-nopm-performance-metrics/

  TPCH gathers the latency of the 22 TPCH queries.

  Args:
     vm:  The virtual machine to run on that has Install and SetupConfig already
       invoked on it.
     db_engine:  The type of database that the script is running on
     hammerdb_script:  An enumeration from HAMMERDB_SCRIPT indicating which
       script to run.  Must have been prior setup with SetupConfig method on the
       vm to work.
    timeout: Timeout when running hammerdbcli

  Returns:
     _HammerDBCliResults object with TPM and NOPM values.
  """
  db_engine = sql_engine_utils.GetDbEngineType(db_engine)

  linux_scripts = linux_hammerdb.SCRIPT_MAPPING[db_engine][hammerdb_script]
  windows_scripts = [
      WindowsHammerDbTclScript(
          script.tcl_script_name,
          script.needed_parameters,
          script.path,
          script.script_type,
      )
      for script in linux_scripts
  ]

  # Run the run phase script.
  script = [
      script
      for script in windows_scripts
      if script.script_type == linux_hammerdb.RUN_SCRIPT_TYPE
  ]
  if len(script) != 1:
    raise errors.Benchmarks.RunError(
        f'1 run script expected but {len(script)} found. Exiting.'
    )
  stdout = script[0].Run(vm, timeout=timeout)

  if hammerdb_script == linux_hammerdb.HAMMERDB_SCRIPT_TPC_H:
    return ParseTpcHResults(stdout)
  else:
    return ParseTpcCResults(stdout, vm)


def PushTestFile(vm, data_file: str, path: str):
  vm.PushFile(
      data.ResourcePath(posixpath.join(path, data_file)),
      ntpath.join(
          vm.temp_dir,
          HAMMERDB.format(linux_hammerdb.HAMMERDB_VERSION.value),
          data_file,
      ),
  )


def GetMetadata(db_engine: str):
  """Returns the meta data needed for hammerdb."""
  return linux_hammerdb.GetMetadata(db_engine)
