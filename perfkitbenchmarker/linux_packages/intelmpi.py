# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Installs the Intel MPI library."""

import logging
import posixpath
import re
from absl import flags
from perfkitbenchmarker import nfs_service
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import intel_repo

MPI_VERSION = flags.DEFINE_string('intelmpi_version', '2019.6-088',
                                  'MPI version.')
FLAGS = flags.FLAGS

_INTEL_ROOT = '/opt/intel'

PREPROVISIONED_DATA = {
    'intel-mpi-2018.4-057.rpm.tar.gz':
        'fbcf98d246760330ef7dd661d2b75e7f90783943dfea1285e5aaeb4f0e1abf9e',
    'intel-mpi-2018.4-057.deb.tar.gz':
        '9603b9241287874b5d4fe3ba806d1b672b1da576406a4c6dcd86e9d84e341bd1',
    'intel-mpi-2019.6-088.rpm.tar.gz':
        'a4683ff1bd66b57f9002ad63208a77aee172b5b1dc365d892af8d468d96ad8d5',
    'intel-mpi-2019.6-088.deb.tar.gz':
        'e5a8126dbc2d1aca889f987117be00dffe4344c58f8e622d3b8f5809cb5493e6',
}


def MpiVars(vm) -> str:
  """Returns the path to the mpivars.sh file.

  With different versions of Intel software installed the mpivars.sh for
  2019.6 can be under compilers_and_libraries_2020.0.166 while the symlink
  for compilers_and_libraries points to compilers_and_libraries_2018

  Args:
    vm: Virtual machine to look for mpivars.sh on.
  """
  if intel_repo.UseOneApi():
    return intel_repo.ONEAPI_VARS_FILE
  txt, _ = vm.RemoteCommand(
      f'readlink -f {_INTEL_ROOT}/compilers_and_libraries*/'
      'linux/mpi/intel64/bin/mpivars.sh | sort | uniq')
  files = txt.splitlines()
  if not files:
    raise ValueError('Could not find the mpivars.sh file')
  if len(files) > 1:
    logging.info('More than 1 mpivars.sh found, returning first: %s', files)
  return files[0]


def SourceMpiVarsCommand(vm):
  """Returns the command to source the mpivars.sh script."""
  cmd = f'. {MpiVars(vm)}'
  if FLAGS.aws_efa:  # Disable IntelMPI's libfabric to use AWS EFA's
    cmd += ' -ofi_internal=0'
  return cmd


def FixEnvironment(vm):
  """Changes system settings for optimal Intel MPI conditions.


  Sets the ptrace_scope to 0, for details see:
    https://www.kernel.org/doc/Documentation/security/Yama.txt

  Args:
    vm: The virtual machine to run on.
  """
  if not vm.TryRemoteCommand('ulimit -l | grep unlimited'):
    ulimit_fix_cmd = (f'echo "{vm.user_name} - memlock unlimited" | '
                      'sudo tee -a /etc/security/limits.conf')
    vm.RemoteCommand(ulimit_fix_cmd)
    logging.info('Rebooting to permamently set ulimit')
    vm.Reboot()
    vm.WaitForBootCompletion()
  _, stderr, exitcode = vm.RemoteCommandWithReturnCode(
      'sudo sysctl -w kernel.yama.ptrace_scope=0', ignore_failure=True)
  if exitcode:
    logging.info('Not setting yama ptrace as %s', stderr)


def _InstallViaPreprovisionedData(vm, version: str) -> bool:
  """Returns whether Intel MPI was installed via preprovisioned data."""
  packaging = 'rpm' if vm.BASE_OS_TYPE == 'rhel' else 'deb'
  tarball = f'intel-mpi-{version}.{packaging}.tar.gz'
  # if the tarball is not in preprovisioned data return False
  if tarball not in PREPROVISIONED_DATA:
    return False
  if not vm.ShouldDownloadPreprovisionedData('intelmpi', tarball):
    return False
  vm.InstallPreprovisionedPackageData('intelmpi', [tarball], '.')
  extract_dir = posixpath.join(vm_util.VM_TMP_DIR, 'packages')
  vm.RemoteCommand(f'mkdir -p {extract_dir}; '
                   f'tar -C {extract_dir} -xvzf {tarball}; '
                   f'rm {tarball}')
  vm.InstallPackages(f'{extract_dir}/*')
  return True


def Install(vm) -> None:
  """Installs Intel MPI."""
  if _InstallViaPreprovisionedData(vm, MPI_VERSION.value):
    logging.info('Installed MPI %s via preprovisioned data', MPI_VERSION.value)
  else:
    vm.Install('intel_repo')
    package_name = 'intel-oneapi-mpi' if intel_repo.UseOneApi() else 'intel-mpi'
    vm.InstallPackages(f'{package_name}-{MPI_VERSION.value}')
  FixEnvironment(vm)
  # Log the version of MPI and other associated values for debugging
  vm.RemoteCommand(f'. {MpiVars(vm)}; mpirun -V')


def AptInstall(vm) -> None:
  """Installs the MPI library."""
  Install(vm)
  # Ubuntu's POSIX dash shell does not have bash's "==" comparator
  vm.RemoteCommand(f'sudo sed -i "s/==/=/" {MpiVars(vm)}')


def TestInstall(vms) -> None:
  """Tests the MPI install.

  Args:
    vms: List of VMs in the cluster.
  """
  hosts = ','.join([vm.internal_ip for vm in vms])
  mpirun_cmd = f'mpirun -n {len(vms)} -ppn 1 -hosts {hosts} hostname'
  txt, _ = vms[0].RemoteCommand(f'{SourceMpiVarsCommand(vms[0])}; {mpirun_cmd}')
  hosts = sorted(set(txt.splitlines()))
  expected_hosts = sorted([vm.name for vm in vms])
  # In AWS the hostname 'pkb-<run_uri>-0' does not match the returned hostname
  # 'ip-<ip_addr>.<zone>.compute.internal so just check number of responses
  if len(hosts) != len(expected_hosts):
    raise ValueError(
        f'Expected hosts {len(expected_hosts)} but have {len(hosts)}')
  logging.info('Hosts: %s', ','.join(hosts))


def NfsExportIntelDirectory(vms) -> None:
  """NFS exports the /opt/intel from the headnode to the workers.

  Args:
    vms: List of VMs.  The first one is the headnode, the remainder will NFS
      mount the /opt/intel drive from the headnode.
  """
  nfs_service.NfsExportAndMount(vms, _INTEL_ROOT)
  # Still need to have clients ulimit and ptrace fixed
  vm_util.RunThreaded(FixEnvironment, vms[1:])
  TestInstall(vms)


def MpirunMpiVersion(vm, source_cmd: str = None) -> str:
  """Returns the Intel MPI version in use.

  Example output of "mpirun -V" (all on one line):
    Intel(R) MPI Library for Linux* OS,
    Version 2018 Update 4 Build 20180823 (id: 18555)
  This returns as "2018.4" to be nearly the same as the "2018.4-075" reported
  when using the Intel apt/yum repos.

  Args:
    vm: The virtual machine to run on.
    source_cmd: A file to source before executing mpirun.  If not set will use
      the one from SourceMpiVarsCommand.

  Raises:
    ValueError: If the MPI version could not be parsed.
  """
  source_cmd = source_cmd or SourceMpiVarsCommand(vm)
  txt, _ = vm.RemoteCommand(f'{source_cmd}; mpirun -V')
  match = re.search(r'Version (?P<year>\d+) Update (?P<update>\d+) ', txt)
  if match:
    return f'{match["year"]}.{match["update"]}'
  else:
    raise ValueError(f'Could not parse IntelMPI version from "{txt}"')
