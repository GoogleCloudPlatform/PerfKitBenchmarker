r"""Installs MPI library (Intel or OpenMPI) and compiles Intel MPI benchmarks (IMB) from source."""
import logging
import posixpath
from typing import List, Optional

from absl import flags
from perfkitbenchmarker.linux_packages import intel_repo
from perfkitbenchmarker.linux_packages import intelmpi

FLAGS = flags.FLAGS

COMPILE_FROM_SOURCE = flags.DEFINE_bool(
    'imb_compile_from_source', True,
    'Whether to compile the Intel MPI benchmarks from source.')

_INTEL_DIR = '/opt/intel'
_INTEL_COMPILER_DIR = posixpath.join(_INTEL_DIR,
                                     'compilers_and_libraries/linux')
_INTEL_COMPILER_DIR_2020 = posixpath.join(_INTEL_DIR,
                                          'compilers_and_libraries_2020/linux')

# TBB: Intel's "Thread Building Blocks" for multithreaded programs
# https://en.wikipedia.org/wiki/Threading_Building_Blocks
_INTEL_FIX_TBBROOT_CMD = (
    "sudo sed -i 's"
    "#TBBROOT=SUBSTITUTE_INSTALL_DIR_HERE#TBBROOT={compiler_dir}/tbb#' "
    '{compiler_dir}/tbb/bin/tbbvars.sh')

# Source for the Intel MPI benchmarks
_GITHUB_URL = 'https://github.com/intel/mpi-benchmarks.git'
_GITHUB_COMMIT = '2d752544461f04111efef0926efe46826d90f720'
# Directory for the MPI benchmarks
_MPI_BENCHMARK_DIR = 'mpi-benchmarks'
# Checks out the Intel MPI benchmarks
_GIT_CHECKOUT_CMD = (f'git clone -n {_GITHUB_URL}; cd mpi-benchmarks; '
                     f'git checkout {_GITHUB_COMMIT}')

# Patch file and command to add latency histogram to Intel test code
_PATCH_FILE = 'intelmpi.patch'
_GIT_PATCH_CMD = f'patch -d {_MPI_BENCHMARK_DIR} -p3 < ~/{_PATCH_FILE}'

# Enable verbose logging when mpirun fails due to a segfault
_ENABLE_VERBOSE_SEGFAULT_LOGS = ('echo 1 | sudo tee -a '
                                 '/proc/sys/kernel/print-fatal-signals')


def _InstallForIntelMpiLibrary(
    vm) -> None:
  """Compiles the Intel MPI benchmarks for Intel MPI library."""
  if intel_repo.UseOneApi():
    vm.InstallPackages('intel-oneapi-compiler-dpcpp-cpp')
    vm.InstallPackages('intel-oneapi-mpi-devel')  # for mpi.h
    source_cmds = f'. {intel_repo.ONEAPI_VARS_FILE}'
  else:
    source_cmds = (f'. {_INTEL_DIR}/mkl/bin/mklvars.sh intel64; '
                   f'. {_INTEL_COMPILER_DIR}/bin/compilervars.sh intel64')
    for compiler_dir in (_INTEL_COMPILER_DIR, _INTEL_COMPILER_DIR_2020):
      vm.RemoteCommand(
          _INTEL_FIX_TBBROOT_CMD.format(compiler_dir=compiler_dir),
          ignore_failure=True)
  vm.RemoteCommand(_GIT_CHECKOUT_CMD)
  vm.PushDataFile(_PATCH_FILE)
  vm.RemoteCommand(_GIT_PATCH_CMD)
  # Default make uses the Intel compiler (mpiicc) not available in repos
  # {source_cmds} filled in at runtime due to differences in 2018/19 vs 2021
  compile_benchmark_cmd = (
      f'cd {_MPI_BENCHMARK_DIR}; {source_cmds}; CC=mpicc CXX=mpicxx make')
  vm.RemoteCommand(compile_benchmark_cmd)
  vm.RemoteCommand(_ENABLE_VERBOSE_SEGFAULT_LOGS)


def _InstallForOpenMpiLibrary(
    vm) -> None:
  """Compiles the Intel MPI benchmarks for OpenMPI library."""
  vm.RemoteCommand(_GIT_CHECKOUT_CMD)
  vm.PushDataFile(_PATCH_FILE)
  vm.RemoteCommand(_GIT_PATCH_CMD)
  # When installing OpenMPI, openmpi.py runs ./configure.sh with --prefix=/usr.
  compile_benchmark_cmd = (
      f'cd {_MPI_BENCHMARK_DIR}; CC=/usr/bin/mpicc CXX=/usr/bin/mpicxx make')
  vm.RemoteCommand(compile_benchmark_cmd)
  vm.RemoteCommand(_ENABLE_VERBOSE_SEGFAULT_LOGS)


def Install(vm) -> None:
  """Installs MPI lib and compiles the Intel MPI benchmarks from source.

  Args:
    vm: Virtual machine to run on.
  """
  if FLAGS.mpi_vendor == 'intel':
    mpilib = 'intelmpi'
    install_benchmarks = _InstallForIntelMpiLibrary
  elif FLAGS.mpi_vendor == 'openmpi':
    if not COMPILE_FROM_SOURCE.value:
      raise ValueError(
          f'--mpi_vendor=openmpi requires --{COMPILE_FROM_SOURCE.name}')
    mpilib = 'openmpi'
    install_benchmarks = _InstallForOpenMpiLibrary

  vm.Install(mpilib)
  if not COMPILE_FROM_SOURCE.value:
    return
  logging.info('Installing Intel MPI benchmarks from source')
  vm.Install('build_tools')
  install_benchmarks(vm)


def _MpiRunCommandForIntelMpiLibrary(
    vm, hosts: List[str],
    total_processes: int, ppn: int, environment: List[str],
    global_environment: List[str], tune: bool) -> str:
  """String command to call mpirun using Intel MPI library.

  See Intel docs for details:
    https://software.intel.com/content/www/us/en/develop/documentation/mpi-developer-guide-linux/top/running-applications/controlling-process-placement.html

  "If the -ppn option is not specified, the process manager assigns as many
  processes to the first node as there are physical cores on it. Then the next
  node is used."

  If the ppn should not be specified in the command pass in ppn=0.  However you
  most likely want to pass it in so that the number of processes on each node
  is balanced.

  Args:
    vm: Virtual machine to run on.
    hosts: List of internal IP addresses to run on.
    total_processes: The total number of processes to use across all hosts.
    ppn: Number of processes per node to use when assigning processes per node.
    environment: List of environment variables to set, e.g. "FI_PROVIDER=tcp".
    global_environment: List of global environment variables to set via the
      '-genv' option to mpirun, e.g. "I_MPI_PIN_PROCESSOR_LIST=0".
    tune: Whether to pass -tune. If true, consider setting the
      I_MPI_TUNER_DATA_DIR environment variable.

  Returns:
    String command to use in a vm.RemoteCommand call.
  """
  cmd_elements = [f'{intelmpi.SourceMpiVarsCommand(vm)};']
  cmd_elements.extend(sorted(environment))
  cmd_elements.append('mpirun')
  if tune:
    cmd_elements.append('-tune')
  cmd_elements.extend(
      f'-genv {variable}' for variable in sorted(global_environment))
  cmd_elements.append(f'-n {total_processes}')
  # hosts MUST remain in same order so that latency file created on first host
  hosts_str = ','.join(hosts)
  cmd_elements.append(f'-hosts {hosts_str}')
  if ppn:
    cmd_elements.append(f'-ppn {ppn}')
  elif total_processes == len(hosts):
    # for single-threaded runs tell MPI to run one thread on each VM
    cmd_elements.append('-ppn 1')
  return ' '.join(cmd_elements)


def _MpiRunCommandForOpenMpiLibrary(hosts: List[str], total_processes: int,
                                    npernode: int,
                                    environment: List[str]) -> str:
  """String command to call mpirun using OpenMPI library.

  Args:
    hosts: List of internal IP addresses to run on.
    total_processes: Translates directly to mpirun's -n option.
    npernode: Translates directly to mpirun's -npernode option. If 0, then
      -npernode is set to total_processes//len(hosts).
    environment: List of envionrment variables to export via mpirun -x. E.g.
      "OMPI_MCA_btl=self,tcp" or "OMPI_MCA_rmaps_base_mapping_policy=core:PE=1".
      See https://www.open-mpi.org/doc/v3.0/man1/mpirun.1.php for details.

  Returns:
    String command to use in a vm.RemoteCommand call.
  """

  cmd_elements = [f'{env_var}' for env_var in environment]
  cmd_elements.append('mpirun')
  cmd_elements.extend(
      [f'-x {env_var.split("=", 1)[0]}' for env_var in environment])

  # Useful for verifying process mapping.
  cmd_elements.append('-report-bindings')
  cmd_elements.append('-display-map')

  cmd_elements.append(f'-n {total_processes}')
  if not npernode:
    npernode = total_processes // len(hosts)
  cmd_elements.append(f'-npernode {npernode}')
  cmd_elements.append('--use-hwthread-cpus')
  # Guarantee that each host has sufficient slots (conservatively).
  hosts_str = ','.join([f'{h}:slots={total_processes}' for h in hosts])
  cmd_elements.append(f'-host {hosts_str}')

  return ' '.join(cmd_elements)


def MpiRunCommand(vm,
                  hosts: List[str], total_processes: int, ppn: int,
                  environment: List[str], global_environment: List[str],
                  tune: bool) -> Optional[str]:
  """String command to call mpirun."""
  if FLAGS.mpi_vendor == 'intel':
    return _MpiRunCommandForIntelMpiLibrary(vm, hosts, total_processes, ppn,
                                            environment, global_environment,
                                            tune)
  elif FLAGS.mpi_vendor == 'openmpi':
    return _MpiRunCommandForOpenMpiLibrary(hosts, total_processes, ppn,
                                           environment)
