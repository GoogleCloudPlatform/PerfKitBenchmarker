"""Installs and runs the OSU MPI micro-benchmark."""

import itertools
import logging
import re
import time
from typing import Any, Dict, Iterator, List, Optional, Pattern, Tuple
from absl import flags
import dataclasses

from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import nfs_service
from perfkitbenchmarker.linux_packages import intelmpi

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

# Matches MPI debug output showing CPU pinning
_PINNING_RE = re.compile(
    r"""
     MPI\s+startup\(\):\s+
     (?P<rank>\d+)\s+
     (?P<pid>\d+)\s+
     (?P<node_name>\S+)\s+
     {?(?P<cpus>[^}\s]+)""", re.X)

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
_MPI_DEBUG = flags.DEFINE_integer(
    'omb_mpi_debug', 5, 'Debug level to use.  Set to 0 for no debugging')
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
_COMPUTE = _RunType(('size', 'overall', 'compute', 'collection_init',
                     'mpi_test', 'mpi_wait', 'pure_comm', 'overlap'),
                    'mpi_wait')
_COMPUTE_NOSIZE = _RunType(('overall', 'compute', 'collection_init', 'mpi_test',
                            'mpi_wait', 'pure_comm', 'overlap'), 'mpi_wait')

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
  pinning: str
  perhost: int


@dataclasses.dataclass(frozen=True)
class RunRequest:
  test_name: str
  vms: List[Any]  # virtual machine
  message_size: Optional[str] = None   # default: run all message sizes


FLAGS = flags.FLAGS


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
        mpi_vendor='intel',
        mpi_version=intelmpi.MpirunMpiVersion(vms[0]),
        value_column=BENCHMARKS[name].value_column,
        number_processes=number_processes,
        run_time=run_time,
        pinning=_ParseMpiPinningInfo(txt),
        perhost=_MPI_PERHOST.value)


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
  # Create the mpirun command
  full_benchmark_path = _PathToBenchmark(vm, name)
  mpirun_cmd = []
  if _MPI_DEBUG.value:
    mpirun_cmd.append(f'I_MPI_DEBUG={_MPI_DEBUG.value}')
  mpirun_cmd.append('mpirun')
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


def _ParseMpiPinningInfo(txt: str) -> Dict[str, List[Tuple[int, int]]]:
  """Returns the MPI pinning info for the run.

  Example:
    MPI startup(): libfabric provider: tcp;ofi_rxm
    MPI startup(): Rank    Pid      Node name       Pin cpu
    MPI startup(): 0       17077    pkb-a0b71860-0  {0,1,15}
    MPI startup(): 1       3475     pkb-a0b71860-1  {0,
                                       1,15}
    MPI startup(): 2       17078    pkb-a0b71860-0  {2,16,17}
    MPI startup(): 3       3476     pkb-a0b71860-1  {2,16,17}


  Returns {0: 'pkb-a0b71860-0:0,1,15',  1: 'pkb-a0b71860-1:0,1,15',
           2: 'pkb-a0b71860-0:2,16,17', 3: 'pkb-a0b71860-1:2,16,17'}

  Args:
    txt: Stdout from the run.
  """
  start_pinning = re.compile(
      r'.*MPI startup.*Rank\s+Pid\s+Node name\s+Pin cpu.*')
  ret = {}
  rank = None
  for line in _LinesAfterMarker(start_pinning, txt):
    match = _PINNING_RE.search(line)
    if match:
      rank = int(match['rank'])
      ret[rank] = f'{match["node_name"]}:{match["cpus"]}'
    else:
      # might be a line break, append to last result
      match = re.search(r'\s+(?P<cpus>\d+[,\d]*)', line)
      if match:
        ret[rank] += match['cpus']
      else:
        break
  return ret
