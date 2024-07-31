"""Installs the MPI library and runs the IMB-MPI1 tests.

Installation of the MPI library is handed off into imb.py, as
compilation of the benchmarks must be done differently depending on the MPI
library being used.

The run_benchmarks.sh script is copied to the remote server and runs the MPI
tests. The text output is parsed by MpiResultParser.
"""

import collections
import dataclasses
import logging
import os
import posixpath
import re
from typing import Any, Dict, Iterable, Iterator, List, Sequence, Tuple, Union
import uuid

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import temp_dir
from perfkitbenchmarker.linux_packages import imb
from perfkitbenchmarker.linux_packages import intelmpi
from perfkitbenchmarker.linux_packages import omb
from perfkitbenchmarker.linux_packages import openmpi

FLAGS = flags.FLAGS


@dataclasses.dataclass
class MpiData:
  """Data for an MPI run, including headers.

  A row of the MPI run could be a timeout error or the actual data for a run
  stored in the data attribute.  A typical value for that is
  {
    "throughput":10.13,
    "time_avg":193.73,
    "time_max":202.26,
    "time_min":186.85
  }
  """

  bytes: int | None = None
  repetitions: int | None = None
  data: Dict[str, Union[int, float]] | None = None
  is_error: bool = False
  histogram: Dict[float, int] | None = None


@dataclasses.dataclass
class MpiResult:
  """Individual runs of a MPI benchmark test.

  For example this could be the PingPong run results.
  """

  benchmark: str
  data: List[MpiData]
  groups: int | None = None
  processes_per_group: int | None = None
  mode: str | None = None
  group_layout: Dict[int, List[int]] | None = None


@dataclasses.dataclass
class MpiResponse:
  """Response to the RunMpiStats call."""

  mpi_run: str
  args: str
  vendor: str
  version: str
  results: List[MpiResult]
  mpi_pinning: List[str]
  mpi_env: Dict[str, str]


@dataclasses.dataclass
class MpiRequest:
  """Parameters for running an MPI test.

  See the FLAGS.mpi_XXX definitions in mpi_benchmark.py for the definitions.
  """

  # TODO(andytzhu): Get rid of Any, and handle importing linux_virtual_machine
  # without encountering circular dependencies
  vms: List[Any]  # virtual machine
  total_processes: int
  suite: str
  tests: List[str]
  ppn: int
  msglog_min: int
  msglog_max: int
  timeout: int
  off_cache_size: int
  off_cache_line_size: int | None
  iterations: int
  include_zero_byte: bool
  compile_from_source: bool
  environment: List[str] = dataclasses.field(default_factory=list)
  global_environment: List[str] = dataclasses.field(default_factory=list)
  record_latencies: bool = False
  npmin: int | None = None
  tune: bool = False
  multi: bool = False


# The same order as the output in the print_tail function in the patched code
LATENCY_HEADERS: List[str] = [
    'latency_min',
    'latency_p10',
    'latency_p25',
    'latency_p50',
    'latency_p75',
    'latency_p90',
    'latency_p95',
    'latency_p99',
    'latency_p99.5',
    'latency_p99.9',
    'latency_p99.99',
    'latency_max',
]

# Regexs for parsing I_MPI_DEBUG=4 and higher output
_MPI_STARTUP_PREFIX = r'^\[(?P<unused_cpuid>\d+)\] MPI startup\(\):\s+'
_MPI_ENV_RE = re.compile(
    _MPI_STARTUP_PREFIX + r'(?P<mpi_var>I_MPI.*?)=(?P<mpi_value>.*)'
)


def Install(vm) -> None:
  """See base class."""
  # Installs imb, which installs the specified MPI library and compiles
  # the patched MPI benchmark appropriately for the specified MPI library.
  vm.Install('imb')
  VerifyInstall([vm])
  logging.info('Successfully installed MPI on %s', vm)


def VerifyInstall(vms) -> None:
  """Runs a simple test to confirm MPI is installed correctly.

  Args:
    vms: List of virtual machines to include in the test.
  """
  request = MpiRequest(
      vms=vms,
      total_processes=vms[0].NumCpusForBenchmark(),
      suite='IMB-MPI1',
      tests=['PingPong'],
      ppn=vms[0].NumCpusForBenchmark(),
      msglog_min=10,
      msglog_max=11,
      timeout=20,
      off_cache_size=-1,
      off_cache_line_size=None,
      iterations=100,
      include_zero_byte=False,
      compile_from_source=True,
      record_latencies=False,
      multi=True,
  )
  RunMpiStats(vms[0], request)


def GetMpiVersion(vm) -> str | None:
  """Returns the MPI version to use for the given OS type."""
  if FLAGS.mpi_vendor == 'intel':
    return intelmpi.MPI_VERSION.value
  elif FLAGS.mpi_vendor == 'openmpi':
    return openmpi.GetMpiVersion(vm)


def RunMpiStats(vm, request: MpiRequest) -> MpiResponse:
  """Runs the MPI tests.

  The first return value is all the command line arguments used to run the
  text except for the names of the hosts.  This is so that the results in the
  database can all have a common value to filter on.

  Args:
    vm: virtual machine to run on
    request: an MpiRequest that has the parameters for this test

  Returns:
    MpiResponse named tuple.
  """
  hosts = [vm.internal_ip for vm in request.vms]

  mpirun = imb.MpiRunCommand(
      vm,
      hosts,
      request.total_processes,
      request.ppn,
      request.environment,
      request.global_environment,
      request.tune,
  )
  if request.record_latencies:
    latency_file = '/tmp/latency-{}-{}.txt'.format(
        request.tests[0], uuid.uuid4().hex[:8]
    )
  else:
    latency_file = None
  common = ' '.join(
      BuildMpiBenchmarkArgs(request, latency_file, bool(request.ppn))
  )
  try:
    stdout, stderr = vm.RobustRemoteCommand(mpirun + ' ' + common)
  except errors.VirtualMachine.RemoteCommandError:
    # tail last 100 lines of syslog as might tell us something
    for client_vm in request.vms:
      logging.info('VM syslog for %s', client_vm.name)
      client_vm.RemoteCommand(
          'sudo tail -n 100 /var/log/syslog /var/log/messages || exit'
      )
    raise
  if stderr:
    # SSH displays a warning but this could also contain mpirun errors
    logging.warning('Stderr when running MPI command: %s', stderr)
  lines = stdout.splitlines()
  results = list(MpiResultParser(lines))
  if latency_file:
    latencies = _GroupLatencyLines(vm, latency_file, request.iterations)
    if latencies:
      _CreateMpiDataForHistogram(latencies, results)
  return MpiResponse(
      mpi_run=mpirun,
      args=common,
      vendor=FLAGS.mpi_vendor,
      version=GetMpiVersion(vm),
      results=results,
      mpi_pinning=omb.ParseMpiPinning(lines),
      mpi_env=ParseMpiEnv(lines),
  )


class MpiResultParser(Iterable[MpiResult]):
  """Parses the output of the MPI tests.

  This is an iterator where each next item is an MpiResult.
  """

  _NAME = re.compile('^# Benchmarking (.*)')
  _GROUP1 = re.compile(
      r'.*?(?P<groups>\d+) groups of (?P<processes>\d+) processes'
  )
  _GROUP2 = re.compile(r'# #processes = (?P<processes>\d+)')
  _GROUP_LAYOUT = re.compile(r'# Group\s+(\d+):\s+(.*)')
  _GROUP_LAYOUT_FOLLOWON = re.compile(r'#\s+(\d+[\s\d]*)')
  _HEADERS = re.compile(r'.*#repetitions.*usec')
  _TIMEOUT = re.compile(r'(\d+) time-out')
  _MODE = re.compile(r'#\s+MODE: (\S+)')
  # for "t[usec]": https://software.intel.com/en-us/imb-user-guide-put-all-local
  _MPI_HEADER_MAPPING = {
      '#bytes': 'bytes',
      '#repetitions': 'repetitions',
      't_min[usec]': 'time_min',
      't_max[usec]': 'time_max',
      't_avg[usec]': 'time_avg',
      'Mbytes/sec': 'throughput',
      'Msg/sec': 'messages_per_sec',
      't_ovrl[usec]': 'time_overall',
      't_pure[usec]': 'time_pure',
      't_CPU[usec]': 'time_cpu',
      'overlap[%]': 'overlap_percent',
      't[usec]': 'time_avg',
  }
  # these columns are integers, others are floats
  _INT_COLUMNS = set(['bytes', 'repetitions'])

  def __init__(self, lines: Sequence[str]):
    # _lines is a iterator over the input parameter lines
    self._lines = (line.strip() for line in lines)

  def __iter__(self) -> Iterator[MpiResult]:
    """Yields the next MpiResult from the input lines."""
    while True:
      value = self._NextValue()
      if value:
        yield value
      else:
        break

  def _NextValue(self) -> MpiResult | None:
    """Returns the next MpiResult or None if no more entries."""
    name = self._BenchmarkName()
    if not name:
      return None
    logging.info('Parsing benchmark %s', name)
    groups, processes = self._NumberGroups()
    group_layout = self._GroupLayout()
    mode, headers = self._Headers()
    data = []
    # if the previous run timed out don't record the bogus latency numbers
    last_row_is_error: bool = False
    for row in self._Data(headers):
      if not last_row_is_error:
        data.append(row)
      last_row_is_error = row.is_error
    return MpiResult(name, data, groups, processes, mode, group_layout)

  def _BenchmarkName(self) -> str | None:
    for line in self._lines:
      m = self._NAME.match(line)
      if m:
        return m.group(1)

  def _NumberGroups(self) -> Tuple[int | None, int]:
    """Return a tuple of the number of MPI groups and processes for the test."""
    for line in self._lines:
      m = self._GROUP1.match(line)
      if m:
        # this MPI test has both the "groups" and "processes" attributes
        return int(m.group('groups')), int(m.group('processes'))
      m = self._GROUP2.match(line)
      if m:
        # This MPI test does not have a "groups" attribute, but "processes".
        return None, int(m.group('processes'))
    raise errors.Benchmarks.RunError('Did not find number of processes')

  def _GroupLayout(self) -> Dict[int, List[int]] | None:
    """Returns the MPI group CPU layout.

    Parses this input:

    # Group  0:     0    1
    #
    # Group  1:     2    3
    #
    #---------------------------------------------------

    Into {0: [0,1], 1: [2,3]}
    """
    layout = {}
    last_group_number = -1  # to satisfy pytyping
    for line in self._lines:
      m = self._GROUP_LAYOUT.match(line)
      if not m and not layout:
        # no group layout in this output
        return None
      if m:
        last_group_number = int(m.group(1))
        layout[last_group_number] = [int(cpu) for cpu in m.group(2).split()]
        continue
      # check for a continuation of the list of cpus
      m = self._GROUP_LAYOUT_FOLLOWON.match(line)
      if m:
        layout[last_group_number] = [int(cpu) for cpu in m.group(1).split()]
        continue
      if not re.match(r'^#\s*$', line):
        # Only other acceptable line is blank
        break
    return layout

  def _Headers(self) -> Tuple[str | None, Sequence[str]]:
    """Returns a tuple of (benchmark mode, List of headers for data)."""
    mode = None
    for line in self._lines:
      m = self._MODE.match(line)
      if m:
        mode = m.group(1)
        continue
      m = self._HEADERS.match(line)
      if m:
        return mode, line.split()
    raise errors.Benchmarks.RunError('No headers found')

  def _Data(self, headers: Sequence[str]) -> Iterator[MpiData]:
    """Yields MpiData for each row of a benchmark's results.

    Example input:
            0      1000000         1.17         1.17         1.17         0.00
    [ 0.83, 0.97, 0.98, 1.00, 1.02, 1.72, 1.75, 2.28, 3.12, 6.73, 65.19 ]
         1024      1000000         1.80         1.80         1.80       569.96
    [ 1.16, 1.27, 1.29, 1.81, 2.06, 2.17, 2.40, 3.46, 4.34, 10.27, 215.10 ]

    Will yield 2 MpiData records

    Args:
      headers: The headers for this row of data.
    """
    # Keep the last non-latency data row as the next row might contain the
    # percent latency numbers for it.
    on_deck: MpiData = None
    for line in self._lines:
      if not line:
        break
      m = self._TIMEOUT.match(line)
      if m:
        # This is a timeout error
        if on_deck:  # emit the last row if available
          yield on_deck
        yield MpiData(is_error=True, bytes=int(m.group(1)))
        on_deck: MpiData = None
      elif line.startswith('['):
        # This is [p_min, p10, p..] list of latencies
        values: List[float] = [
            float(part.strip()) for part in line[1:-1].split(',')
        ]
        percentiles: Dict[str, float] = dict(zip(LATENCY_HEADERS, values))
        if not on_deck:
          logging.warning(
              'Have percentiles but no previous mpidata %s', percentiles
          )
          continue
        if sum(values) == 0.0:
          # only tests that have been patched have the percentile metrics
          logging.info('No percentiles data for benchmark')
        else:
          if on_deck.data is None:
            raise ValueError('MpiData on_deck has None data')
          on_deck.data.update(percentiles)
        yield on_deck
        on_deck: MpiData = None
      else:
        # This is the regular MPI output of time_avg
        if on_deck:
          yield on_deck
        data = self._DataIntoMap(headers, line.split())
        number_bytes = data.pop('bytes', 0)
        repetitions = data.pop('repetitions', -1)
        on_deck = MpiData(
            bytes=number_bytes, repetitions=repetitions, data=data
        )
    if on_deck:
      # Last record in this stanza was a normal MPI row.
      yield on_deck

  def _DataIntoMap(
      self, headers: Sequence[str], data: Sequence[str]
  ) -> Dict[str, Union[int, float]]:
    """Converts the a row of data from the MPI results into a dict.

    Args:
      headers:  The column headers.
      data: A row of data from the MPI output.

    Returns:
      Dict of the header name to the value.
    """
    row = {}
    for raw_header, raw_value in zip(headers, data):
      new_header = self._MPI_HEADER_MAPPING[raw_header]
      row[new_header] = self._ConvertValue(new_header, raw_value)
    return row

  def _ConvertValue(self, header: str, value: str) -> Union[int, float]:
    return int(value) if header in self._INT_COLUMNS else float(value)


def BuildMpiBenchmarkArgs(
    request: MpiRequest, latency_file: str | None, ppn_set: bool
) -> List[str]:
  """Creates the common arguments to pass to mpirun.

  See https://software.intel.com/en-us/imb-user-guide-command-line-control

  Args:
    request: An MpiRequest object for the run's configuration.
    latency_file: If present the output file to record the individual packet
      latencies.
    ppn_set: Whether this benchmark was run with a set ppn.

  Returns:
    List of string arguments for mpirun.
  """
  args: List[str] = []
  if request.compile_from_source:
    args.append(posixpath.join('mpi-benchmarks', request.suite))
  else:
    args.append(request.suite)
  # only add -msglog if told to do so
  if (
      request.suite in ('IMB-MPI1', 'IMB-RMA', 'IMB-NBC')
      and request.msglog_max is not None
  ):
    if request.msglog_min is None:
      arg = request.msglog_max
    else:
      arg = '{}:{}'.format(request.msglog_min, request.msglog_max)
    args.append('-msglog {}'.format(arg))
  if request.suite != 'IMB-MT':
    # -multi is trinary: not present, 0, 1
    if request.multi:
      args.append('-multi 0')
    args.append('-time {}'.format(request.timeout))
    # only add -off_cache if told to do so
    if request.off_cache_size:
      arg = '-off_cache {}'.format(request.off_cache_size)
      if request.off_cache_line_size:
        arg += ',{}'.format(request.off_cache_line_size)
      args.append(arg)
    args.append('-iter {}'.format(request.iterations))
    if request.npmin is not None:
      args.append(f'-npmin {request.npmin}')
  # Setting iter_policy to off to collect the same number of samples every time.
  args.append('-iter_policy off')
  if not request.include_zero_byte:
    args.append('-zero_size off')
  # MPI benchmark tests will ignore this option if not present
  args.append('-show_tail yes')
  if latency_file:
    args.append(f'-dumpfile {latency_file}')
  if not ppn_set:
    # only use -map if the --mpi_ppn was not set
    number_hosts = len(request.vms)
    processes_per_host = request.total_processes // number_hosts
    args.append(f'-map {processes_per_host}x{number_hosts}')
  args.extend(request.tests)
  return args


def _CreateMpiDataForHistogram(
    grouped_lines: List[List[str]], results: List[MpiResult]
) -> None:
  """Adds histogram data from the histogram file to existing data.

  The MPI parsed results are passed in as some benchmarks runs can do many
  sub-runs of different MPI group values.  This code pairs up those runs done
  in order with the latency file that has all the runs concatenated together.

  Args:
    grouped_lines: The histogram text file lines grouped by sub-run.
    results: The parsed MPI results from the non-histogram data.
  """
  acceptable_mpi_data: List[MpiData] = []
  # MPI runs that time out should not have histogram data associated with it.
  for result in results:
    acceptable_mpi_data.extend(
        mpi_data for mpi_data in result.data if not mpi_data.is_error
    )
  histograms: List[MpiData] = []
  for lines in grouped_lines:
    histograms.extend(_CombineHistogramEntries(lines))
  if _MpiHistogramAcceptable(acceptable_mpi_data, histograms):
    for mpi_data, histogram in zip(acceptable_mpi_data, histograms):
      mpi_data.histogram = histogram.histogram


def _MpiHistogramAcceptable(
    mpi_data: List[MpiData], histograms: List[MpiData]
) -> bool:
  """Returns whether the parsed MpiResults MpiData matches with the histograms.

  Criteria:
    Number of MpiResults.data[] entries are the same.
    The number of bytes for each MpiData matches.
    The number of repetitions for each MpiData matches.

  Args:
    mpi_data: List of MpiData parsed for this run.
    histograms: List of MpiData histograms parsed for this run.
  """
  if len(mpi_data) != len(histograms):
    logging.warning(
        'Have %s parsed MPI data but only %s histograms',
        len(mpi_data),
        len(histograms),
    )
    return False
  for mpi_data, histogram in zip(mpi_data, histograms):
    bytes_same = mpi_data.bytes == histogram.bytes
    repetitions_same = mpi_data.repetitions == histogram.repetitions
    if not bytes_same or not repetitions_same:
      logging.warning(
          'Parsed MPI data %s does not match with histogram %s',
          mpi_data,
          histogram,
      )
      return False
  return True


def _CombineHistogramEntries(lines: Iterable[str]) -> Iterator[MpiData]:
  """Converts the -dumpfile latency file into MpiData.

  The latency file lines are in this form:
    integer_bytes latency_usec
  For example this is for a run with one latency value of 11.0usec for the
  bytes=1024 run and three values for bytes=2048 of 12.1,13.5, and 13.5 usec:
    1024 11
    2048 12.1
    2048 13.5
    2048 13.5

  The number of MpiDatas returned is equal to the unique number of bytes=###
  runs in the input.  The MpiData's "histogram" field will be populated with a
  dict where the key is the latency in microseconds and the value is the number
  of times that latency has been seen.

  Args:
    lines: The lines from the latency dump file.

  Yields:
    An MpiData that has the histogram of latencies for all runs of a particular
    number of bytes.
  """
  latencies = collections.defaultdict(list)
  for line in lines:
    # format of file is "integer_bytes latency_usec"
    parts = line.strip().split()
    if len(parts) == 2:
      latencies[int(parts[0])].append(float(parts[1]))
    else:
      logging.warning('Latency file line "%s" should have two parts', line)
  if not latencies:
    logging.warning('No latency entries found')
  for number_bytes, times in sorted(latencies.items()):
    histogram = collections.Counter()
    for item in times:
      # Round the sub-microsecond latency based on the latency value to reduce
      # the number of latency histogram keys.
      # Under 5 usec: 0.01usec accuracy.  5-40 usec: 0.1usec, 40+ usec: 1usec
      if item < 5:
        item = round(item, 2)
      elif item < 40:
        item = round(item, 1)
      else:
        item = round(item, 0)
      histogram[item] += 1
    yield MpiData(
        bytes=number_bytes,
        histogram=dict(histogram),
        repetitions=sum(histogram.values()),
    )


def _GroupLatencyLines(
    vm, latency_file: str, packets_per_run: int
) -> List[List[str]]:
  r"""Parses the histogram latency file copied from the remote VM.

  The latency file contains multiple sub-runs concatenated together.  Each of
  those runs is of length packets_per_run.  The returned file is chunked into
  groups of that size.

  Example: ("1\n2\n3\n4\n5\n6", 2) => [["1","2"],["3","4"],["5","6"]]

  Args:
    vm: The virtual machine that has the histogram file.
    latency_file: Path to the latency file on the VM.
    packets_per_run: The number of packets (lines) for each test run.

  Returns:
    List of lists of strings of length packets_per_run or an empty list if there
    is a problem dividing up the lines into groups.
  """
  local_file: str = os.path.join(
      temp_dir.GetRunDirPath(), os.path.basename(latency_file)
  )
  if vm.TryRemoteCommand(f'test -f {latency_file}'):
    vm.PullFile(local_file, latency_file)
  else:
    logging.warning(
        'Skipping gathering latency as %s file missing', latency_file
    )
    return []
  with open(local_file) as reader:
    lines = [line.strip() for line in reader.readlines()]
  number_groups = len(lines) // packets_per_run
  if packets_per_run * number_groups != len(lines):
    logging.warning(
        'File %s has %s lines, cannot be divided into size %s',
        local_file,
        len(lines),
        packets_per_run,
    )
    return []
  return [
      lines[i : i + packets_per_run]
      for i in range(0, len(lines), packets_per_run)
  ]


def ParseMpiEnv(lines: Sequence[str]) -> Dict[str, str]:
  """Reads the log file for environment parameters.

  Args:
    lines: Text lines from mpirun output.

  Returns:
    Dict of the MPI envirnoment variables
  """
  mpi_env = {}
  for line in lines:
    row = _MPI_ENV_RE.search(line)
    if not row:
      continue
    mpi_env[row['mpi_var']] = row['mpi_value']
  return mpi_env
