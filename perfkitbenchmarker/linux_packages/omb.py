"""Installs and runs the OSU MPI micro-benchmark."""

import dataclasses
import itertools
import logging
import re
import time
from typing import Any, Dict, Iterator, List, Optional, Pattern, Sequence, Tuple

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import nfs_service
from perfkitbenchmarker.linux_packages import intelmpi
from perfkitbenchmarker.linux_packages import openmpi

FLAGS = flags.FLAGS
flags.DEFINE_enum('mpi_vendor', 'intel', ['intel', 'openmpi'], 'MPI provider.')

flags.DEFINE_list(
    'omb_mpi_env', [], 'Comma separated list of environment variables, e.g. '
    '--omb_mpi_env=FI_PROVIDER=tcp,FI_LOG_LEVEL=info')

flags.DEFINE_list(
    'omb_mpi_genv', [], 'Comma separated list of global environment variables, '
    'i.e. environment variables to be applied to all nodes, e.g. '
    '--omb_mpi_genv=I_MPI_PIN_PROCESSOR_LIST=0,I_MPI_PIN=1 '
    'When running with Intel MPI, these translate to -genv mpirun options. '
    'When running with OpenMPI, both --omb_mpi_env and --omb_mpi_genv are '
    'treated the same via the -x mpirun option')

flags.register_validator(
    'omb_mpi_env',
    lambda env_params: all('=' in param for param in env_params),
    message='--omb_mpi_env values must be in format "key=value" or "key="')

flags.register_validator(
    'omb_mpi_genv',
    lambda genv_params: all('=' in param for param in genv_params),
    message='--omb_mpi_genv values must be in format "key=value" or "key="')

VERSION = '5.7.1'
_PKG_NAME = 'osu-micro-benchmarks'
_DATA_URL = ('http://mvapich.cse.ohio-state.edu/download/mvapich/'
             f'{_PKG_NAME}-{VERSION}.tgz')
_SRC_DIR = f'{_PKG_NAME}-{VERSION}'
_TARBALL = f'{_PKG_NAME}-{VERSION}.tgz'
_RUN_DIR = f'/usr/local/libexec/{_PKG_NAME}/mpi'

# Benchmarks that can only be run with a single thread per host
_SINGLE_THREADED_BENCHMARKS = frozenset({
    'acc_latency', 'bibw', 'bw', 'cas_latency', 'fop_latency', 'get_bw',
    'get_latency'
})

PREPROVISIONED_DATA = {
    _TARBALL: 'cb5ce4e2e68ed012d9952e96ef880a802058c87a1d840a2093b19bddc7faa165'
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

# parse MPI pinning
_MPI_STARTUP_PREFIX = r'^\[(?P<unused_cpuid>\d+)\] MPI startup\(\):\s+'
_MPI_PIN_RE = re.compile(_MPI_STARTUP_PREFIX + (r'(?P<rank>\d+)\s+'
                                                r'(?P<pid>\d+)\s+'
                                                r'(?P<nodename>\S+)\s+'
                                                r'.*?(?P<cpuids>[\d,-]+)'))
_PKB_NODE_RE = re.compile(r'pkb-(?P<pkbid>.*?)-(?P<nodeindex>\d+)')

# parameters to pass into the benchmark
_NUMBER_ITERATIONS = flags.DEFINE_integer(
    'omb_iterations', None, 'Number of iterations to run in a test.')
_SYNC_OPTION = flags.DEFINE_string('omb_sync_option', None,
                                   '--sync-option value to pass in')

_NUM_SERVER_THREADS = flags.DEFINE_integer('omb_server_threads', None,
                                           'Number of server threads to use.')
_NUM_RECEIVER_THREADS = flags.DEFINE_integer(
    'omb_receiver_threads', None, 'Number of server threads to use.')
flag_util.DEFINE_integerlist(
    'omb_mpi_processes', flag_util.IntegerList([1, 0]),
    'MPI processes to use per host.  1=One process, 0=only real cores')

_MPI_PERHOST = flags.DEFINE_integer('omb_perhost', 1, 'MPI option -perhost.')


@dataclasses.dataclass(frozen=True)
class _RunType:
  """Metadata about a benchmark.

  Attributes:
    columns: The columns in the output.
    value_column: The column that should be use as a sample.Sample value
    units: The units of the value_column.
    supports_full: Whether this benchmark supports --full.
    long_running: Whether this benchmark takes a long time to run.
  """
  columns: Tuple[str]
  value_column: str
  units: str = 'usec'
  supports_full: bool = True
  long_running: bool = False


# Benchmarks that support --full
_LATENCY = _RunType(
    ('size', 'latency', 'min_latency', 'max_latency', 'iterations'), 'latency')
_LATENCY_NOSIZE = _RunType(
    ('latency', 'min_latency', 'max_latency', 'iterations'), 'latency')
_COMPUTE = _RunType(
    ('size', 'overall', 'compute', 'collection_init', 'mpi_test', 'mpi_wait',
     'min_comm', 'max_comm', 'pure_comm', 'overlap'), 'mpi_wait')
_COMPUTE_NOSIZE = _RunType(
    ('overall', 'compute', 'collection_init', 'mpi_test', 'mpi_wait',
     'pure_comm', 'min_comm', 'max_comm', 'overlap'), 'mpi_wait')

# Benchmarks that do not support --full
_LATENCY_SIZE_ONLY = _RunType(('size', 'latency'), 'latency', 'usec', False)
_BANDWIDTH = _RunType(('size', 'bandwidth'), 'bandwidth', 'MB/s', False)
_BANDWIDTH_MESSAGES = _RunType(('size', 'bandwidth', 'messages_per_second'),
                               'bandwidth', 'MB/s', False)
# The get_acc_latency and latency_mt take a really long time to finish
_LATENCY_LONG_RUNNING = _RunType(('size', 'latency'), 'latency', 'usec', False,
                                 True)

BENCHMARKS: Dict[str, _RunType] = {
    'acc_latency': _LATENCY_SIZE_ONLY,
    'allgather': _LATENCY,
    'allgatherv': _LATENCY,
    'allreduce': _LATENCY,
    'alltoall': _LATENCY,
    'alltoallv': _LATENCY,
    'barrier': _LATENCY_NOSIZE,
    'bcast': _LATENCY,
    'bibw': _BANDWIDTH,
    'bw': _BANDWIDTH,
    'cas_latency': _LATENCY_SIZE_ONLY,
    'fop_latency': _LATENCY_SIZE_ONLY,
    'gather': _LATENCY,
    'gatherv': _LATENCY,
    'get_acc_latency': _LATENCY_LONG_RUNNING,
    'get_bw': _BANDWIDTH,
    'get_latency': _LATENCY_SIZE_ONLY,
    'iallgather': _COMPUTE,
    'iallgatherv': _COMPUTE,
    'iallreduce': _COMPUTE,
    'ialltoall': _COMPUTE,
    'ialltoallv': _COMPUTE,
    'ialltoallw': _COMPUTE,
    'ibarrier': _COMPUTE_NOSIZE,
    'ibcast': _COMPUTE,
    'igather': _COMPUTE,
    'igatherv': _COMPUTE,
    'ireduce': _COMPUTE,
    'iscatter': _COMPUTE,
    'iscatterv': _COMPUTE,
    'latency': _LATENCY_SIZE_ONLY,
    'latency_mp': _LATENCY_SIZE_ONLY,
    'latency_mt': _LATENCY_LONG_RUNNING,
    'mbw_mr': _BANDWIDTH_MESSAGES,
    'multi_lat': _LATENCY_SIZE_ONLY,
    'put_bibw': _BANDWIDTH,
    'put_bw': _BANDWIDTH,
    'put_latency': _LATENCY_SIZE_ONLY,
    'reduce': _LATENCY,
    'reduce_scatter': _LATENCY,
    'scatter': _LATENCY,
    'scatterv': _LATENCY
}


@dataclasses.dataclass(frozen=True)
class RunResult:
  """The parsed output of a benchmark run.

  Attributes:
    name: The metric name to use in a sample.Sample.
    metadata: Any output of the benchmark that describes the run.
    data: The parsed output data, for example [{'size': 1, 'latency': 10.9}].
    full_cmd: Command used to run the benchmark.
    units: Units of the value_column in the data.
    params: Any parameters passed along to the benchmark.
    mpi_vendor: Name of the MPI vendor.
    mpi_version: Version of the MPI library.
    value_column: The name of the column in the data rows that should be used
      for the sample.Sample value.
    number_processes: The total number of MPI processes used.
    run_time: Time in seconds to run the test.
    pinning: MPI processes pinning.
    perhost: MPI option -perhost.
    mpi_env: environment variables to set for mpirun command.
  """
  name: str
  metadata: Dict[str, Any]
  data: List[Dict[str, float]]
  full_cmd: str
  units: str
  params: Dict[str, str]
  mpi_vendor: str
  mpi_version: str
  value_column: str
  number_processes: int
  run_time: int
  pinning: List[str]
  perhost: int
  mpi_env: Dict[str, str]


@dataclasses.dataclass(frozen=True)
class RunRequest:
  test_name: str
  vms: List[Any]  # virtual machine
  message_size: Optional[str] = None  # default: run all message sizes


FLAGS = flags.FLAGS


def _InstallForIntelMpi(vm) -> None:
  """Installs the omb package with IntelMPI lib on the VM."""
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


def _InstallForOpenMpi(vm) -> None:
  """Installs the omb package with OpenMPI lib on the VM."""
  vm.Install('openmpi')
  txt, _ = vm.RemoteCommand('which mpicc mpicxx')
  mpicc_path, mpicxx_path = txt.splitlines()
  vm.Install('build_tools')
  vm.InstallPreprovisionedPackageData('omb', [_TARBALL], '.')
  vm.RemoteCommand(f'tar -xvf {_TARBALL}')
  vm.RemoteCommand(f'cd {_SRC_DIR}; ./configure CC={mpicc_path} '
                   f'CXX={mpicxx_path}; make; sudo make install')
  _TestInstall([vm])


def Install(vm) -> None:
  """Installs the omb package with specified MPI lib on the VM."""
  vm.AuthenticateVm()
  if FLAGS.mpi_vendor == 'intel':
    _InstallForIntelMpi(vm)
  else:
    _InstallForOpenMpi(vm)


def PrepareWorkers(vms) -> None:
  if FLAGS.mpi_vendor == 'intel':
    intelmpi.NfsExportIntelDirectory(vms)
  else:
    for vm in vms:
      vm.Install('openmpi')
  nfs_service.NfsExportAndMount(vms, _RUN_DIR)
  _TestInstall(vms)


def RunBenchmark(request: RunRequest) -> Iterator[RunResult]:
  """Yields the RunResult of running the microbenchmark.

  Args:
    request: Run configuration.
  """
  vms = request.vms
  name = request.test_name
  params = {}
  if _NUMBER_ITERATIONS.value:
    params['--iterations'] = _NUMBER_ITERATIONS.value
  if _SYNC_OPTION.value:
    params['--sync-option'] = _SYNC_OPTION.value
  if request.message_size:
    # flag does not work on cas_latency and fop_latency, always runs with size=8
    # for barrier and ibarrier does not appear to set a message size
    if ':' in str(request.message_size):
      params['-m'] = f'{request.message_size}'
    else:
      # Pass in '-m size:size' to only do one size.
      params['-m'] = f'{request.message_size}:{request.message_size}'
  if _NUM_RECEIVER_THREADS.value:
    value = str(_NUM_RECEIVER_THREADS.value)
    if _NUM_SERVER_THREADS.value:
      value += f':{_NUM_SERVER_THREADS.value}'
    # --num_threads errors out with 'Invalid option [-]'
    params['-t'] = value

  for processes_per_host in FLAGS.omb_mpi_processes:
    # special case processes_per_host=0 means use all the real cores
    processes_per_host = processes_per_host or vms[0].NumCpusForBenchmark(True)
    if name in _SINGLE_THREADED_BENCHMARKS and processes_per_host != 1:
      continue
    number_processes = processes_per_host * len(vms)
    try:
      start_time = time.time()
      txt, full_cmd = _RunBenchmark(vms[0], name, _MPI_PERHOST.value,
                                    number_processes, vms, params)
      run_time = time.time() - start_time
    except errors.VirtualMachine.RemoteCommandError:
      logging.exception('Error running %s benchmark with %s MPI proccesses',
                        name, number_processes)
      continue

    yield RunResult(
        name=name,
        metadata=_ParseBenchmarkMetadata(txt),
        data=_ParseBenchmarkData(name, txt),
        full_cmd=full_cmd,
        units='MB/s' if 'MB/s' in txt else 'usec',
        params=params,
        mpi_vendor=FLAGS.mpi_vendor,
        mpi_version=_GetMpiVersion(vms[0]),
        value_column=BENCHMARKS[name].value_column,
        number_processes=number_processes,
        run_time=run_time,
        pinning=ParseMpiPinning(txt.splitlines()),
        perhost=_MPI_PERHOST.value,
        mpi_env={
            k: v for k, v in [
                envvar.split('=', 1)
                for envvar in FLAGS.omb_mpi_env + FLAGS.omb_mpi_genv
            ]
        })


def _GetMpiVersion(vm) -> Optional[str]:
  """Returns the MPI version to use for the given OS type."""
  if FLAGS.mpi_vendor == 'intel':
    return intelmpi.MpirunMpiVersion(vm)
  elif FLAGS.mpi_vendor == 'openmpi':
    return openmpi.GetMpiVersion(vm)


def _RunBenchmarkWithIntelMpi(
    vm,
    name: str,
    perhost: int,
    environment: List[str],
    global_environment: List[str],
    number_processes: int = None,
    hosts: List[Any] = None,
    options: Dict[str, Any] = None) -> Tuple[str, str]:
  """Runs the microbenchmark using Intel MPI library."""
  # Create the mpirun command
  full_benchmark_path = _PathToBenchmark(vm, name)
  mpirun_cmd = []
  mpirun_cmd.extend(sorted(environment))
  mpirun_cmd.append('mpirun')
  mpirun_cmd.extend(
      f'-genv {variable}' for variable in sorted(global_environment))
  if perhost:
    mpirun_cmd.append(f'-perhost {perhost}')
  if number_processes:
    mpirun_cmd.append(f'-n {number_processes}')
  if hosts:
    host_ips = ','.join([vm.internal_ip for vm in hosts])
    mpirun_cmd.append(f'-hosts {host_ips}')
  mpirun_cmd.append(full_benchmark_path)
  if options:
    for key, value in sorted(options.items()):
      mpirun_cmd.append(f'{key} {value}')
  # _TestInstall runs the "hello" test which isn't a benchmark
  if name in BENCHMARKS and BENCHMARKS[name].supports_full:
    mpirun_cmd.append('--full')
  full_cmd = f'{intelmpi.SourceMpiVarsCommand(vm)}; {" ".join(mpirun_cmd)}'

  txt, _ = vm.RobustRemoteCommand(full_cmd)
  return txt, full_cmd


def _RunBenchmarkWithOpenMpi(vm,
                             name: str,
                             perhost: int,
                             environment: List[str],
                             global_environment: List[str],
                             number_processes: int = None,
                             hosts: List[Any] = None,
                             options: Dict[str, Any] = None) -> Tuple[str, str]:
  """Runs the microbenchmark using OpenMPI library."""
  # Create the mpirun command
  full_env = sorted(environment + global_environment)
  full_benchmark_path = _PathToBenchmark(vm, name)
  mpirun_cmd = [f'{env_var}' for env_var in full_env]
  mpirun_cmd.append('mpirun')
  mpirun_cmd.extend([f'-x {env_var.split("=", 1)[0]}' for env_var in full_env])

  # Useful for verifying process mapping.
  mpirun_cmd.append('-report-bindings')
  mpirun_cmd.append('-display-map')

  if number_processes:
    mpirun_cmd.append(f'-n {number_processes}')
  if perhost:
    mpirun_cmd.append(f'-npernode {perhost}')
  mpirun_cmd.append('--use-hwthread-cpus')
  if hosts:
    host_ips = ','.join(
        [f'{vm.internal_ip}:slots={number_processes}' for vm in hosts])
    mpirun_cmd.append(f'-host {host_ips}')
  mpirun_cmd.append(full_benchmark_path)
  if options:
    for key, value in sorted(options.items()):
      mpirun_cmd.append(f'{key} {value}')
  # _TestInstall runs the "hello" test which isn't a benchmark
  if name in BENCHMARKS and BENCHMARKS[name].supports_full:
    mpirun_cmd.append('--full')
  full_cmd = ' '.join(mpirun_cmd)

  txt, _ = vm.RobustRemoteCommand(full_cmd)
  return txt, full_cmd


def _RunBenchmark(vm,
                  name: str,
                  perhost: int,
                  number_processes: int = None,
                  hosts: List[Any] = None,
                  options: Dict[str, Any] = None) -> Tuple[str, str]:
  """Runs the microbenchmark.

  Args:
    vm: The headnode to run on.
    name: The name of the microbenchmark.
    perhost: MPI option -perhost.  Use 0 to not set value.
    number_processes: The number of mpi processes to use.
    hosts: List of BaseLinuxVirtualMachines to use in the cluster.
    options: Optional dict of flags to pass into the benchmark.

  Returns:
    Tuple of the output text of mpirun and the command ran.
  """
  run_impl = _RunBenchmarkWithIntelMpi
  if FLAGS.mpi_vendor == 'openmpi':
    run_impl = _RunBenchmarkWithOpenMpi
  return run_impl(vm, name, perhost, FLAGS.omb_mpi_env, FLAGS.omb_mpi_genv,
                  number_processes, hosts, options)


def _PathToBenchmark(vm, name: str) -> str:
  """Returns the full path to the benchmark."""
  txt, _ = vm.RemoteCommand(f'ls {_RUN_DIR}/*/osu_{name}')
  return txt.strip()


def _TestInstall(vms):
  number_processes = len(vms)
  logging.info('Running hello world on %s process(es)', number_processes)
  hosts = vms if number_processes > 1 else None
  txt, _ = _RunBenchmark(vms[0], 'hello', 1, number_processes, hosts=hosts)
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


def _LinesAfterMarker(line_re: Pattern[str], txt: str) -> List[str]:
  r"""Returns the text lines after the matching regex.

  _LinesAfterMarker(re.compile('^Hello'), "Line1\nHello\nLine2") == ['Line2']

  Args:
    line_re: Pattern to match the start of data
    txt: Text input
  """

  def StartBlock(line: str) -> bool:
    return not line_re.match(line)

  lines = list(itertools.dropwhile(StartBlock, txt.splitlines()))
  return lines[1:] if lines else []


def _ParseBenchmarkData(benchmark_name: str,
                        txt: str) -> List[Dict[str, float]]:
  """Returns the parsed metrics from the benchmark stdout.

  Text for benchmark_name='bw':

  # OSU MPI Bandwidth Test v5.7
  # Size      Bandwidth (MB/s)
  1                       0.48
  2                       0.98

  Returns [{'size': 1, 'bandwidth': 0.48}, {'size': 2, 'bandwidth': 0.98}]

  Args:
    benchmark_name: Name of the benchmark.
    txt: The standard output of the benchmark.
  """
  data = []
  for line in _LinesAfterMarker(re.compile('# OSU MPI.*'), txt):
    if not line or line.startswith('#') or 'Overall' in line:
      continue
    columns = BENCHMARKS[benchmark_name].columns
    row_data = [float(item) for item in line.split()]
    if len(columns) != len(row_data):
      raise ValueError(
          f'Expected {len(columns)} columns ({columns}) in the '
          f'{benchmark_name} benchmark, received {len(row_data)} ({row_data})')
    row_with_headers = dict(zip(columns, row_data))
    for int_column in ('size', 'iterations'):
      if int_column in row_with_headers:
        row_with_headers[int_column] = int(row_with_headers[int_column])
    data.append(row_with_headers)
  return data


def ParseMpiPinning(lines: Sequence[str]) -> List[str]:
  """Grabs MPI pinning to CPUs from the log files.

  Output format is (rank:node id:comma separated CPUs)
    0:1:0,24;1:1:1,25;2:1:2,26;3:1:3,27
  Rank 0 and 1 are on node 1 and are pinned to CPUs (0,24) and (1,25)

  Args:
    lines: Text lines from mpirun output.

  Returns:
    Text strings of the MPI pinning
  """

  def _CondenseMpiPinningMultiline(lines: Sequence[str]) -> Sequence[str]:
    """Returns multi-line MPI pinning info as one line."""
    condensed_lines = []
    for line in lines:
      m = _MPI_PIN_RE.search(line)
      if m:
        condensed_lines.append(line)
      else:
        if condensed_lines and not condensed_lines[-1].endswith('}'):
          condensed_lines[-1] += line.strip()
    return condensed_lines

  ret = []
  for line in _CondenseMpiPinningMultiline(lines):
    row = _MPI_PIN_RE.search(line)
    if not row:
      continue
    nodename = row['nodename']
    m2 = _PKB_NODE_RE.match(nodename)
    if m2:
      nodename = m2.group('nodeindex')
    cpuids = ','.join(str(x) for x in _IntRange(row['cpuids']))
    ret.append(':'.join((row['rank'], nodename, cpuids)))
  return ret


def _IntRange(mask: str) -> List[int]:
  """Converts an integer mask into a sorted list of integers.

  _IntRange('0,1,5-7') == [0, 1, 5, 6, 7]

  Args:
    mask: String integer mask as from MPI pinning list

  Returns:
    Sorted list of integers from the mask
  """
  ints = []
  for raw in mask.split(','):
    parts = raw.split('-')
    start = int(parts[0])
    end = start if len(parts) == 1 else int(parts[1])
    ints.extend(list(range(start, end + 1)))
  return sorted(ints)
