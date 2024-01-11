"""MPI benchmarking tests.

This could go to the public PKB once we have a handle on the metrics and if
there should be tuning on each of the clouds
"""

import logging
from typing import Any, Dict, Iterator, List, Tuple
from absl import flags

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import histogram_util

from perfkitbenchmarker.linux_packages import mpi

_BaseLinuxVirtualMachine = linux_virtual_machine.BaseLinuxVirtualMachine

# documents the individual MPI tests in each suite
_MPI_SUITE_TESTS = {
    'IMB-MPI1': [
        'Allgather', 'Allgatherv', 'Allreduce', 'Alltoall', 'Alltoallv',
        'Barrier', 'Bcast', 'Exchange', 'Gather', 'Gatherv', 'PingPing',
        'PingPong', 'Reduce', 'Reduce_scatter', 'Reduce_scatter_block',
        'Scatter', 'Scatterv', 'Sendrecv'
    ],
    'IMB-MPI2': [],
    'IMB-NBC': [
        'Iallgather', 'Iallgatherv', 'Iallreduce', 'Ialltoall', 'Ialltoallv',
        'Ibarrier', 'Ibcast', 'Igather', 'Igatherv', 'Ireduce',
        'Ireduce_scatter', 'Iscatter', 'Iscatterv'
    ],
    'IMB-RMA': [
        'Accumulate', 'All_get_all', 'All_put_all', 'Bidir_get', 'Bidir_put',
        'Compare_and_swap', 'Exchange_get', 'Exchange_put', 'Fetch_and_op',
        'Get_accumulate', 'One_get_all', 'One_put_all', 'Put_all_local',
        'Put_local', 'Truly_passive_put', 'Unidir_get', 'Unidir_put'
    ],
    'IMB-MT': [
        'AllReduceMT', 'BarrierMT', 'BcastMT', 'BiBandMT', 'ExchangeMT',
        'PingPingMT', 'PingPongMT', 'ReduceMT', 'SendRecvMT', 'UniBandMT'
    ]
}

flag_util.DEFINE_integerlist(
    'percentiles', [50, 90, 99], 'Latency percentiles to calculate.')

flags.DEFINE_list('mpi_suites', ['IMB-MPI1'],
                  'MPI benchmarks suites: {}.'.format(sorted(_MPI_SUITE_TESTS)))
_BENCHMARKS = flags.DEFINE_list(
    'mpi_benchmarks', [],
    ('List of MPI benchmarks.  Default is [], which means '
     'running all benchmarks in the suite.'))
flag_util.DEFINE_integerlist(
    'mpi_threads', [0, 1], 'Number of MPI processes to use per host.  For 0 '
    'use half the number of vCPUs.')
flags.DEFINE_integer('mpi_timeout', 60, 'MPI testing timeout (seconds).')
flags.DEFINE_integer(
    'mpi_iterations', 100000,
    'Number of times to run an individual benchmark for a given byte size.')
flags.DEFINE_bool('mpi_include_zero_byte', False,
                  'Whether to include a 0 byte payload in runs.')
_MSG_SIZES = flags.DEFINE_multi_integer(
    'mpi_msglog_sizes', [], ('List of 2^n byte sizes to use.  '
                             'Example: [2,8] will use 4 and 64 byte payloads.'))
_MSG_SIZE_MIN = flags.DEFINE_integer('mpi_msglog_min', 10,
                                     '2^n byte message min size.')
_MSG_SIZE_MAX = flags.DEFINE_integer('mpi_msglog_max', 11,
                                     '2^n byte message max size.')
flags.DEFINE_integer(
    'mpi_off_cache_size', -1,
    'Avoids cache-size (use --mpi_off_cache_size= to reuse '
    'cache, but that gives unrealistic numbers.  -1 uses the '
    'value in IMB_mem_info.h.')
flags.DEFINE_integer('mpi_off_cache_line_size', None,
                     'Size of a last level cache line.')
# For more info on --mpi_ppn changes the MPI rank assignment see
# https://software.intel.com/en-us/articles/controlling-process-placement-with-the-intel-mpi-library
flags.DEFINE_integer(
    'mpi_ppn', 0, 'Processes/Ranks per node. Defaults to not setting a ppn '
    'when running tests, instead relying on -map to place threads.')

flags.DEFINE_list(
    'mpi_env', ['I_MPI_DEBUG=6'],
    'Comma separated list of environment variables, e.g. '
    '--mpi_env=FI_PROVIDER=tcp,FI_LOG_LEVEL=info '
    'Default set to output MPI pinning debugging information.')
flags.DEFINE_list(
    'mpi_genv', [], 'Comma separated list of global environment variables, '
    'i.e. environment variables to be applied to all nodes, e.g. '
    '--mpi_genv=I_MPI_PIN_PROCESSOR_LIST=0,I_MPI_PIN=1')
flags.DEFINE_bool('mpi_record_latency', True,
                  'Whether to record the individual packet latencies.')
flags.DEFINE_integer(
    'mpi_npmin', None, 'Minimum number of processes to use. For IMB, this '
    'becomes -npmin. If unspecified, no attempt will be made to specify the '
    'minimum number of processes (i.e. the application defaults will prevail).')
flags.DEFINE_bool(
    'mpi_tune', False,
    'Whether to instruct the mpirun command to use data collected by an MPI '
    'tuning utility like mpitune, e.g. by passing -tune to mpirun. Consider '
    'using in conjunction with specifying the tuning data directory, e.g. for '
    'Intel MPI setting I_MPI_TUNER_DATA_DIR.')
flags.DEFINE_bool(
    'mpi_multi', True,
    'Whether to instruct the mpirun command to set -multi and run with '
    'multiple number of groups as opposed to just one.')

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'mpi'

BENCHMARK_CONFIG = """
mpi:
  description: Runs the MPI benchmarks
  vm_groups:
    default:
      vm_count: 2
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          zone: us-west1-a
        AWS:
          machine_type: c5.xlarge
          zone: us-west-1c
        Azure:
          machine_type: Standard_B2s
          zone: eastus
"""

# these columns in the MPI output data are surfaces as sample.Sample.metrics
_METRIC_NAMES = frozenset(['time_avg', 'time_overall'])

flags.register_validator(
    'mpi_suites',
    lambda suites: set(suites) <= set(_MPI_SUITE_TESTS),
    message='--mpi_suites values must be in {}'.format(
        sorted(_MPI_SUITE_TESTS.keys())))

flags.register_validator(
    'mpi_env',
    lambda env_params: all('=' in param for param in env_params),
    message='--mpi_env values must be in format "key=value" or "key="')

flags.register_validator(
    'mpi_genv',
    lambda genv_params: all('=' in param for param in genv_params),
    message='--mpi_genv values must be in format "key=value" or "key="')


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Returns the benchmark config to use.

  Args:
    user_config: Pre-defined config.

  Raises:
     InvalidFlagConfigurationError if user supplied flags are incorrect.
  """
  if _MSG_SIZES.value:
    if FLAGS['mpi_msglog_min'].present or FLAGS['mpi_msglog_max'].present:
      raise errors.Setup.InvalidFlagConfigurationError(
          'If --mpi_msglog_sizes set cannot set '
          '--mpi_msglog_min or --mpi_msglog_min')
  if _BENCHMARKS.value:
    all_tests = set()
    for tests in _MPI_SUITE_TESTS.values():
      all_tests.update(_LowerList(tests))
    unknown_tests = set(_LowerList(_BENCHMARKS.value)).difference(all_tests)
    if unknown_tests:
      raise errors.Setup.InvalidFlagConfigurationError(
          f'Unknown MPI benchmarks: "{",".join(sorted(unknown_tests))}"')
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['num_vms'].present:
    config['vm_groups']['default']['vm_count'] = FLAGS.num_vms
  return config


def Prepare(spec: benchmark_spec.BenchmarkSpec) -> None:
  vms = spec.vms
  vm_util.RunThreaded(lambda vm: vm.AuthenticateVm(), vms)
  logging.info('Installing mpi package')
  vm_util.RunThreaded(lambda vm: vm.Install('mpi'), vms)
  mpi.VerifyInstall(vms)


def Run(spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs all of the MPI tests.

  Args:
    spec: The benchmark spec.

  Returns:
    List of sample.Samples
  """
  vms = spec.vms
  # for --mpi_threads=0 the threads per host is half of the number of vCPUs
  samples = []
  # The count of real CPUs on the VM: for SMT runs (the default) it is one half
  # of the number of vCPUs.  When SMT is disabled it is the count of the CPUs.
  real_cpus = vms[0].NumCpusForBenchmark(True)
  for process_count in FLAGS.mpi_threads:
    process_count = process_count or real_cpus
    # Indicates whether the run is using the optimal HPC configuration of one
    # thread per real (1/2 of vCPUs) CPUs.
    on_real_cpus = process_count == real_cpus
    samples.extend(
        _RunTest(vms,
                 process_count * len(vms),  # this is num ranks
                 FLAGS.mpi_ppn, on_real_cpus))
  for item in samples:
    # TODO(user) reenable installing MKL when Intel repos work
    # google3/cloud/performance/artemis/internal_packages/internal_intelmpi.py;l=65
    item.metadata['installed_mkl'] = False
  return samples


def _RunTest(vms: List[_BaseLinuxVirtualMachine], total_processes: int,
             ppn: int, on_real_cpus: bool) -> List[sample.Sample]:
  """Runs the MPI test for this given number of processes per host.

  Args:
    vms: List of virtual machines to use in the test.
    total_processes: The total number of processes to run across all nodes.
    ppn: Processes per node.
    on_real_cpus: Whether the number of MPI processes is equal to the number of
      real CPUs (vCPUs / 2)

  Returns:
    List of sample.Samples.
  """
  # metadata that's constant for all runs
  samples = []
  for suite in FLAGS.mpi_suites:
    for request in _CreateRequestWithFlagParameters(
        vms=vms,
        total_processes=total_processes,
        suite=suite,
        tests=_GetTests(suite),
        ppn=ppn):
      response = mpi.RunMpiStats(vms[0], request)
      for item in _CreateSamples(response):
        item.metadata['mpi_suite'] = suite
        samples.append(item)
  # Fill in metadata common to all samples.
  hosts = [vm.internal_ip for vm in vms]
  for item in samples:
    item.metadata.update({
        'compile_from_source': FLAGS.imb_compile_from_source,
        'threads_half_cpus': on_real_cpus,
        'smt_enabled': vms[0].IsSmtEnabled(),
        'threads': total_processes,
        'number_nodes': len(hosts),
        'nodes': str(','.join(sorted(hosts))),
        'processes_per_host': total_processes // len(hosts),
        'ppn': ppn,
        'mpi_env': ','.join(sorted(FLAGS.mpi_env + FLAGS.mpi_genv)),
        'tune': FLAGS.mpi_tune,
    })
    for mpi_item in FLAGS.mpi_env + FLAGS.mpi_genv:
      key, value = mpi_item.split('=', 1)
      item.metadata['mpi_env_' + key] = value
  return samples


def _CreateSamples(response: mpi.MpiResponse) -> Iterator[sample.Sample]:
  """Generates samples for each result in the response."""
  for result in response.results:
    for row in result.data:
      for item in _MpiDataToSamples(row):
        item.metadata.update({
            'mpi_run': response.mpi_run,
            'mpi_args': response.args,
            'mpi_vendor': response.vendor,
            'mpi_version': response.version,
            'mpi_benchmark': result.benchmark,
        })
        if result.groups is not None:
          item.metadata['mpi_groups'] = result.groups
        if result.processes_per_group is not None:
          item.metadata['mpi_processes_per_group'] = result.processes_per_group
          if result.groups is not None:
            item.metadata[
                'mpi_ranks'] = result.processes_per_group * result.groups
          else:  # only one group => ranks = ppg
            item.metadata['mpi_ranks'] = result.processes_per_group
        if result.mode:
          item.metadata['mpi_mode'] = result.mode
        if result.group_layout:
          # Convert {0: [1,2], 1: [3,4]} into '0=1,2;1=3,4'
          layout = []
          for group_number, cpu_ids in sorted(result.group_layout.items()):
            layout.append(f'{group_number}='
                          f'{",".join(str(cpu) for cpu in cpu_ids)}')
          item.metadata['mpi_layout'] = ';'.join(layout)
        else:
          item.metadata['mpi_layout'] = None
        if response.mpi_pinning:
          item.metadata['mpi_pinning'] = ';'.join(response.mpi_pinning)
        if response.mpi_env:
          mpi_env = sorted(response.mpi_env.items())
          item.metadata['mpi_running_env'] = ';'.join(
              f'{key}={value}' for key, value in mpi_env)
        yield item


def _MpiDataToSamples(row: mpi.MpiData) -> List[sample.Sample]:
  """Returns the individual MPI result row as a list of Samples.

  MpiData stores the results of a run for a given benchmark ("PingPong") that
  specifies the:
    bytes=(integer payload byte size for the run)
    is_error=(whether this run was timed out)
  and has one or both of the following if the run did not time out:
    data={dict of latency percentages : latency in usec}
    histogram={dict of latency in usec : count of packets}

  This method returns [Samples] as the dict of latencies and the histogram dict
  are reported as individual samples.

  Args:
    row: A latency/histogram value for a given number of bytes.
  """
  if row.is_error:
    metadata = {'bytes': row.bytes, 'mpi_timeout': FLAGS.mpi_timeout}
    # value=1 so that the timeline chart can show a blip when this happens
    return [sample.Sample('timeout_error', 1, 'count', metadata)]
  found_metrics = _METRIC_NAMES.intersection(row.data)
  if not found_metrics:
    logging.warning('Skipping row %s as missing a required metric name %s', row,
                    _METRIC_NAMES)
    return []
  metric = list(found_metrics)[0]
  ret = [sample.Sample(metric, row.data[metric], 'usec', row.data)]
  if row.histogram:
    # change the key of the histogram to a string to match existing TCP_RR data
    metadata = {
        'histogram': {
            str(latency): count for latency, count in row.histogram.items()
        },
    }
    ret.append(sample.Sample('MPI_Latency_Histogram', 0, 'usec', metadata))
    latency_stats = histogram_util._HistogramStatsCalculator(row.histogram, FLAGS.percentiles)
    for stat, value in latency_stats.items():
        ret.append(sample.Sample(f'MPI_Latency_{stat}', float(value), 'usec', metadata))
  for item in ret:
    item.metadata.update({
        'bytes': row.bytes,
        'repetitions': row.repetitions,
    })
  return ret


def Cleanup(spec: benchmark_spec.BenchmarkSpec) -> None:
  del spec  # Unused


def _CreateRequestWithFlagParameters(vms: List[_BaseLinuxVirtualMachine],
                                     total_processes: int, suite: str,
                                     tests: List[str],
                                     ppn: int) -> Iterator[mpi.MpiRequest]:
  """Yields an MpiRequest using settings passed in as flags.

  If told to record MPI latencies (--mpi_record_latency) then must create
  individual runs for each byte length.  Flags of --mpi_msglog_min=10,
  --mpi_msglog_max=12 generates 3 MpiRequests of (msglog_min=10,msglog_max=10),
  (msglog_min=11,msglog_max=11), (msglog_min=12,msglog_max=12)

  Args:
    vms: List of VMs to run on.
    total_processes: The total number of MPI processes to run over all VMs.
    suite: The name of the MPI suite to run.
    tests: The individual MPI tests to run.  An MpiRequest is created for each.
    ppn: The Processes Per Node, passed along to mpirun.
  """
  msglog_sizes: List[Tuple[int, int]] = []
  if _MSG_SIZES.value:
    msglog_sizes = [(size, size) for size in _MSG_SIZES.value]
  else:
    if FLAGS.mpi_record_latency:
      # MUST pass in only one size at a time to the mpirun command
      # to get a single dump file for the run
      msglog_sizes = [
          (size, size)
          for size in range(FLAGS.mpi_msglog_min, FLAGS.mpi_msglog_max + 1)
      ]
    else:
      msglog_sizes = [(FLAGS.mpi_msglog_min, FLAGS.mpi_msglog_max)]
  for test in tests:
    for msglog_min, msglog_max in msglog_sizes:
      yield mpi.MpiRequest(
          vms=vms,
          total_processes=total_processes,
          suite=suite,
          tests=[test],
          ppn=ppn,
          msglog_min=msglog_min,
          msglog_max=msglog_max,
          timeout=FLAGS.mpi_timeout,
          off_cache_size=FLAGS.mpi_off_cache_size,
          off_cache_line_size=FLAGS.mpi_off_cache_line_size,
          iterations=FLAGS.mpi_iterations,
          include_zero_byte=FLAGS.mpi_include_zero_byte,
          compile_from_source=FLAGS.imb_compile_from_source,
          environment=FLAGS.mpi_env,
          global_environment=FLAGS.mpi_genv,
          record_latencies=FLAGS.mpi_record_latency,
          npmin=FLAGS.mpi_npmin,
          tune=FLAGS.mpi_tune,
          multi=FLAGS.mpi_multi)


def _LowerList(elements: List[str]) -> List[str]:
  """Returns the list with all items lowercased."""
  return [item.lower() for item in elements]


def _GetTests(suite: str) -> List[str]:
  """Returns the tests to run for this benchmark run.

  Args:
    suite: The MPI suite to use.

  Returns:
    List of individual benchmarks to run.
  """
  tests = _BENCHMARKS.value or _MPI_SUITE_TESTS[suite]
  all_tests = set(_LowerList(_MPI_SUITE_TESTS[suite]))
  return [test for test in tests if test.lower() in all_tests]
