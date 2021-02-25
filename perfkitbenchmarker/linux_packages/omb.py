"""Installs and runs the OSU MPI micro-benchmark."""

import logging
import re
from typing import Any, Dict, List, Tuple
from absl import flags
import dataclasses

from perfkitbenchmarker import nfs_service
from perfkitbenchmarker.linux_packages import intelmpi

VERSION = '5.7'
_PKG_NAME = 'osu-micro-benchmarks'
_DATA_URL = ('https://mvapich.cse.ohio-state.edu/download/mvapich/'
             f'{_PKG_NAME}-{VERSION}.tar.gz')
_SRC_DIR = f'{_PKG_NAME}-{VERSION}'
_TARBALL = f'{_PKG_NAME}-{VERSION}.tar.gz'
_RUN_DIR = f'/usr/local/libexec/{_PKG_NAME}/mpi'

PREPROVISIONED_DATA = {
    _TARBALL: '1470ebe00eb6ca7f160b2c1efda57ca0fb26b5c4c61148a3f17e8e79fbf34590'
}
PACKAGE_DATA_URL = {_TARBALL: _DATA_URL}

# Regexs to match on headers in the benchmark's stdout.
_HEADERS = [
    re.compile(r'^# Window creation: (?P<window_creation>.*)'),
    re.compile(r'^# Synchronization: (?P<sync>.*)'),
    re.compile(r'^# Number of Sender threads: (?P<sender_threads>\d+)'),
    re.compile(r'^# Number of Receiver threads: (?P<receiver_threads>\d+)'),
    re.compile(r'# \[ pairs: (?P<pairs>\d+) \] '
               r'\[ window size: (?P<window_size>\d+) \]'),
]

_LATENCY_RE = re.compile(r'^(?P<size>\d+)\s+'
                         r'(?P<value>[\d\.]+)\s*$', re.X | re.MULTILINE)
# the "overall" value is the metric recorded.  Others are put into metadata
_COMPUTE_RE = re.compile(
    r'^(?P<size>^\d+)\s+'
    r'(?P<value>[\d\.]+)\s+'
    r'(?P<compute>[\d\.]+)\s+'
    r'(?P<comm>[\d\.]+)\s+'
    r'(?P<overlap>[\d\.]+)\s*$', re.X | re.MULTILINE)

# parameters to pass into the benchmark
_NUMBER_ITERATIONS = flags.DEFINE_integer(
    'omb_iterations', None, 'Number of iterations to run in a test.')
_SYNC_OPTION = flags.DEFINE_string('omb_sync_option', None,
                                   '--sync-option value to pass in')
_MESSAGE_SIZE = flags.DEFINE_string('omb_message_size', None,
                                    '--message-size value to pass in.')
_NUM_SERVER_THREADS = flags.DEFINE_integer('omb_server_threads', None,
                                           'Number of server threads to use.')
_NUM_RECEIVER_THREADS = flags.DEFINE_integer(
    'omb_receiver_threads', None, 'Number of server threads to use.')

LONG_RUNNING_TESTS = frozenset(['get_acc_latency', 'latency_mt'])


@dataclasses.dataclass(frozen=True)
class RunResult:
  name: str
  metadata: Dict[str, Any]
  data: List[Dict[str, float]]
  full_cmd: str
  units: str
  params: Dict[str, str]
  mpi_vendor: str
  mpi_version: str


def Install(vm) -> None:
  """Installs the omb package on the VM."""
  vm.AuthenticateVm()
  vm.Install('intelmpi')
  txt, _ = vm.RemoteCommand(f'{intelmpi.SourceMpiVarsCommand(vm)}; '
                            'which mpicc mpicxx')
  mpicc_path, mpicxx_path = txt.splitlines()
  vm.Install('build_tools')
  vm.InstallPreprovisionedPackageData('omb', [_TARBALL], '.')
  vm.RemoteCommand(f'tar -xvf {_TARBALL}')
  vm.RemoteCommand(f'cd {_SRC_DIR}; {intelmpi.SourceMpiVarsCommand(vm)}; '
                   f'./configure CC={mpicc_path} CXX={mpicxx_path}; '
                   'make; sudo make install')
  _TestInstall([vm])


def PrepareWorkers(vms) -> None:
  intelmpi.NfsExportIntelDirectory(vms)
  nfs_service.NfsExportAndMount(vms, _RUN_DIR)
  _TestInstall(vms)


def RunBenchmark(vms, name) -> RunResult:
  """Returns the RunResult of running the microbenchmark.

  Args:
    vms: List of VMs to use.
    name: The OSU microbenchmark to run.
  """
  params = {}
  if _NUMBER_ITERATIONS.value:
    params['--iterations'] = _NUMBER_ITERATIONS.value
  if _SYNC_OPTION.value:
    params['--sync-option'] = _SYNC_OPTION.value
  if _MESSAGE_SIZE.value:
    params['--message-size'] = _MESSAGE_SIZE.value
  if _NUM_RECEIVER_THREADS.value:
    value = str(_NUM_RECEIVER_THREADS.value)
    if _NUM_SERVER_THREADS.value:
      value += f':{_NUM_SERVER_THREADS.value}'
    # --num_threads errors out with 'Invalid option [-]'
    params['-t'] = value

  txt, full_cmd = _RunBenchmark(vms[0], name, len(vms), vms, params)

  return RunResult(
      name=name,
      metadata=_ParseBenchmarkMetadata(txt),
      data=_ParseBenchmarkData(txt),
      full_cmd=full_cmd,
      units='MB/s' if 'MB/s' in txt else 'usec',
      params=params,
      mpi_vendor='intel',
      mpi_version=intelmpi.MpirunMpiVersion(vms[0]))


def _RunBenchmark(vm,
                  name: str,
                  number_processes: int = None,
                  hosts: List[Any] = None,
                  options: Dict[str, Any] = None) -> Tuple[str, str]:
  """Runs the microbenchmark.

  Args:
    vm: The headnode to run on.
    name: The name of the microbenchmark.
    number_processes: The number of mpi processes to use.
    hosts: List of BaseLinuxVirtualMachines to use in the cluster.
    options: Optional dict of flags to pass into the benchmark.

  Returns:
    Tuple of the output text of mpirun and the command ran.
  """
  # Create the mpirun command
  txt, _ = vm.RemoteCommand(f'ls {_RUN_DIR}/*/osu_{name}')
  full_benchmark_path = txt.strip()
  mpirun_cmd = ['mpirun']
  # TODO(user) add support for perhost / rank options
  mpirun_cmd.append('-perhost 1')
  if number_processes:
    mpirun_cmd.append(f'-n {number_processes}')
  if hosts:
    host_ips = ','.join([vm.internal_ip for vm in hosts])
    mpirun_cmd.append(f'-hosts {host_ips}')
  mpirun_cmd.append(full_benchmark_path)
  if options:
    for key, value in sorted(options.items()):
      mpirun_cmd.append(f'{key} {value}')
  full_cmd = f'{intelmpi.SourceMpiVarsCommand(vm)}; {" ".join(mpirun_cmd)}'

  if name in LONG_RUNNING_TESTS:
    txt, _ = vm.RobustRemoteCommand(full_cmd)
  else:
    txt, _ = vm.RemoteCommand(full_cmd)
  return txt, full_cmd


def _TestInstall(vms):
  number_processes = len(vms)
  logging.info('Running hello world on %s process(es)', number_processes)
  hosts = vms if number_processes > 1 else None
  txt, _ = _RunBenchmark(vms[0], 'hello', number_processes, hosts=hosts)
  logging.info('Hello world output: %s', txt.splitlines()[-1])


def _ParseBenchmarkMetadata(txt: str) -> Dict[str, str]:
  """Returns the metadata found in the benchmark text output.

  For most benchmarks this is an empty dict.  An example of acc_latency's:
  {'sync': 'MPI_Win_flush', 'window_creation': 'MPI_Win_allocate'}

  Args:
    txt: The benchmark's text output.
  """

  def _MatchHeader(line: txt) -> Dict[str, str]:
    for header in _HEADERS:
      match = header.match(line)
      if match:
        return match.groupdict()
    return {}

  ret = {}
  for line in txt.splitlines():
    ret.update(_MatchHeader(line))
  return ret


def _ParseBenchmarkData(txt: str) -> List[Dict[str, float]]:
  data = [_ParseBenchmarkOutputLine(line) for line in txt.splitlines()]
  return [item for item in data if item]


def _ParseBenchmarkOutputLine(line: str) -> Dict[str, float]:
  """Returns a parsed line from the benchmark's output.

  The column names are known based on the number of items in a line.

  Args:
    line: A single text line from the benchmark output.
  """
  if not line or line.startswith('#'):
    return {}
  if 'Overall' in line:
    # barrier has a line ' Overall(us) Compute(us) Pure Comm.(us) Overlap(us)'
    return {}
  row = [float(item) for item in line.split()]
  if len(row) == 1:
    # barrier is just the latency
    return {'value': row[0]}
  if len(row) == 2:
    return {'size': int(row[0]), 'value': row[1]}
  if len(row) == 3:
    return {'size': int(row[0]), 'value': row[1], 'messages_per_second': row[2]}
  if len(row) == 4:
    # ibarrier does not have a size
    return {
        'value': row[0],
        'compute': row[1],
        'comm': row[2],
        'overlap': row[3],
    }
  if len(row) == 5:
    return {
        'size': int(row[0]),
        'value': row[1],
        'compute': row[2],
        'comm': row[3],
        'overlap': row[4]
    }
  raise ValueError(f'Output line not parseable: {row}')
