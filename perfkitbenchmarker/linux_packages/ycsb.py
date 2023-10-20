# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Install, execute, and parse results from YCSB.

YCSB (the Yahoo! Cloud Serving Benchmark) is a common method of comparing NoSQL
database performance.
https://github.com/brianfrankcooper/YCSB

For PerfKitBenchmarker, we wrap YCSB to:

  * Pre-load a database with a fixed number of records.
  * Execute a collection of workloads under a staircase load.
  * Parse the results into PerfKitBenchmarker samples.

The 'YCSBExecutor' class handles executing YCSB on a collection of client VMs.
Generally, clients just need this class. For example, to run against
HBase 1.0:

  >>> executor = ycsb.YCSBExecutor('hbase-10')
  >>> samples = executor.LoadAndRun(loader_vms)

By default, this runs YCSB workloads A and B against the database, 32 threads
per client VM, with an initial database size of 1GB (1k records).
Each workload runs for at most 30 minutes.
"""

from collections.abc import Mapping, Sequence
import copy
import dataclasses
import datetime
import io
import logging
import os
import posixpath
import re
import time
from typing import Any
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import events
from perfkitbenchmarker import linux_packages
from perfkitbenchmarker import resource
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import maven
from perfkitbenchmarker.linux_packages import ycsb_stats

FLAGS = flags.FLAGS

YCSB_URL_TEMPLATE = (
    'https://github.com/brianfrankcooper/YCSB/releases/'
    'download/{0}/ycsb-{0}.tar.gz'
)
YCSB_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'ycsb')
YCSB_EXE = posixpath.join(YCSB_DIR, 'bin', 'ycsb')
HDRHISTOGRAM_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'hdrhistogram')
HDRHISTOGRAM_TAR_URL = (
    'https://github.com/HdrHistogram/HdrHistogram/archive/'
    'HdrHistogram-2.1.10.tar.gz'
)

flags.DEFINE_string(
    'ycsb_version', '0.17.0', 'YCSB version to use. Defaults to version 0.17.0.'
)
flags.DEFINE_string(
    'ycsb_tar_url',
    None,
    'URL to a YCSB tarball to use instead of the releases located on github.',
)
flags.DEFINE_enum(
    'ycsb_measurement_type',
    ycsb_stats.HISTOGRAM,
    ycsb_stats.YCSB_MEASUREMENT_TYPES,
    'Measurement type to use for ycsb. Defaults to histogram.',
)
flags.DEFINE_enum(
    'ycsb_measurement_interval',
    'op',
    ['op', 'intended', 'both'],
    'Measurement interval to use for ycsb. Defaults to op.',
)
flags.DEFINE_boolean(
    'ycsb_histogram',
    False,
    'Include individual '
    'histogram results from YCSB (will increase sample '
    'count).',
)
flags.DEFINE_boolean(
    'ycsb_load_samples', True, 'Include samples from pre-populating database.'
)
SKIP_LOAD_STAGE = flags.DEFINE_boolean(
    'ycsb_skip_load_stage',
    False,
    'If True, skip the data '
    'loading stage. It can be used when the database target '
    'already exists with pre-populated data.',
)
flags.DEFINE_boolean(
    'ycsb_skip_run_stage',
    False,
    'If True, skip the workload '
    'running stage. It can be used when you want to '
    'pre-populate a database target.',
)
flags.DEFINE_boolean(
    'ycsb_include_individual_results',
    False,
    'Include results from each client VM, rather than just combined results.',
)
flags.DEFINE_boolean(
    'ycsb_reload_database',
    True,
    'Reload database, otherwise skip load stage. '
    'Note, this flag is only used if the database '
    'is already loaded.',
)
flags.DEFINE_integer('ycsb_client_vms', 1, 'Number of YCSB client VMs.')
flags.DEFINE_list(
    'ycsb_workload_files',
    ['workloada', 'workloadb'],
    'Path to YCSB workload file to use during *run* '
    'stage only. Comma-separated list',
)
flags.DEFINE_list(
    'ycsb_load_parameters',
    [],
    'Passed to YCSB during the load stage. Comma-separated list '
    'of "key=value" pairs.',
)
flags.DEFINE_list(
    'ycsb_run_parameters',
    [],
    'Passed to YCSB during the run stage. Comma-separated list '
    'of "key=value" pairs.',
)
_THROUGHPUT_TIME_SERIES = flags.DEFINE_bool(
    'ycsb_throughput_time_series',
    False,
    'If true, run prints status which includes a throughput time series (1s '
    'granularity), and includes the results in the samples.',
)
flags.DEFINE_list(
    'ycsb_threads_per_client',
    ['32'],
    'Number of threads per '
    'loader during the benchmark run. Specify a list to vary the '
    'number of clients. For each thread count, optionally supply '
    'target qps per client, which cause ycsb to self-throttle.',
)
flags.DEFINE_integer(
    'ycsb_preload_threads',
    None,
    'Number of threads per '
    'loader during the initial data population stage. '
    'Default value depends on the target DB.',
)
flags.DEFINE_integer(
    'ycsb_record_count',
    None,
    'Pre-load with a total '
    'dataset of records total. Overrides recordcount value in '
    'all workloads of this run. Defaults to None, where '
    'recordcount value in each workload is used. If neither '
    'is not set, ycsb default of 0 is used.',
)
flags.DEFINE_integer(
    'ycsb_operation_count', None, 'Number of operations *per client VM*.'
)
flags.DEFINE_integer(
    'ycsb_timelimit',
    1800,
    'Maximum amount of time to run '
    'each workload / client count combination in seconds. '
    'Set to 0 for unlimited time.',
)
flags.DEFINE_integer(
    'ycsb_field_count',
    10,
    'Number of fields in a record. '
    'Defaults to 10, which is the default in ycsb v0.17.0.',
)
flags.DEFINE_integer(
    'ycsb_field_length',
    None,
    'Size of each field. Defaults to None which uses the ycsb default of 100.',
)
flags.DEFINE_enum(
    'ycsb_requestdistribution',
    None,
    ['uniform', 'zipfian', 'latest'],
    'Type of request distribution.  '
    'This will overwrite workload file parameter',
)
flags.DEFINE_float(
    'ycsb_readproportion',
    None,
    'The read proportion, Default is 0.5 in workloada and 0.95 in YCSB.',
)
flags.DEFINE_float(
    'ycsb_updateproportion',
    None,
    'The update proportion, Default is 0.5 in workloada and 0.05 in YCSB.',
)
flags.DEFINE_float(
    'ycsb_scanproportion',
    None,
    'The scan proportion, Default is 0 in workloada and 0 in YCSB.',
)
flags.DEFINE_boolean(
    'ycsb_dynamic_load',
    False,
    'Apply dynamic load to system under test and find out '
    'maximum sustained throughput (test length controlled by '
    'ycsb_operation_count and ycsb_timelimit) the '
    'system capable of handling. ',
)
flags.DEFINE_integer(
    'ycsb_dynamic_load_throughput_lower_bound',
    None,
    'Apply dynamic load to system under test. '
    'If not supplied, test will halt once reaching '
    'sustained load, otherwise, will keep running until '
    'reaching lower bound.',
)
flags.DEFINE_float(
    'ycsb_dynamic_load_sustain_throughput_ratio',
    0.95,
    'To consider throughput sustainable when applying '
    'dynamic load, the actual overall throughput measured '
    'divided by target throughput applied should exceed '
    'this ratio. If not, we will lower target throughput and '
    'retry.',
)
flags.DEFINE_integer(
    'ycsb_dynamic_load_sustain_timelimit',
    300,
    'Run duration in seconds for each throughput target '
    'if we have already reached sustained throughput.',
)
flags.DEFINE_integer(
    'ycsb_sleep_after_load_in_sec',
    0,
    'Sleep duration in seconds between load and run stage.',
)
_BURST_LOAD_MULTIPLIER = flags.DEFINE_integer(
    'ycsb_burst_load',
    None,
    'If set, applies burst load to the system, by running YCSB once, and then '
    'immediately running again with --ycsb_burst_load times the '
    'amount of load specified by the `target` parameter. Set to -1 for '
    'the max throughput from the client.',
)
_INCREMENTAL_TARGET_QPS = flags.DEFINE_integer(
    'ycsb_incremental_load',
    None,
    'If set, applies an incrementally increasing load until the target QPS is '
    'reached. This should be the aggregate load for all VMs. Running with '
    'this flag requires that there is not a QPS target passed in through '
    '--ycsb_run_parameters.',
)
_SHOULD_RECORD_COMMAND_LINE = flags.DEFINE_boolean(
    'ycsb_record_command_line',
    True,
    'Whether to record the command line used for kicking off the runs as part '
    'of metadata. When there are many VMs, this can get long and clutter the '
    'PKB log.',
)
_SHOULD_FAIL_ON_INCOMPLETE_LOADING = flags.DEFINE_boolean(
    'ycsb_fail_on_incomplete_loading',
    False,
    'Whether to fail the benchmarking if loading is not complete, '
    'e.g., there are insert failures.',
)
_INCOMPLETE_LOADING_METRIC = flags.DEFINE_string(
    'ycsb_insert_error_metric',
    'insert Return=ERROR',
    'Used with --ycsb_fail_on_incomplete_loading. Will fail the benchmark if '
    "this metric's value is non-zero. This metric should be an indicator of "
    'incomplete table loading. If insertion retries are enabled via '
    'core_workload_insertion_retry_limit, then the default metric may be '
    'non-zero even though the retried insertion eventually succeeded.',
)
_ERROR_RATE_THRESHOLD = flags.DEFINE_float(
    'ycsb_max_error_rate',
    1.00,
    'The maximum error rate allowed for the run. '
    'By default, this allows any number of errors.',
)
CPU_OPTIMIZATION = flags.DEFINE_bool(
    'ycsb_cpu_optimization',
    False,
    'Whether to run in CPU-optimized mode. The test will increase QPS until '
    'CPU is over --ycsb_cpu_optimization_target.',
)
CPU_OPTIMIZATION_TARGET_MIN = flags.DEFINE_float(
    'ycsb_cpu_optimization_target_min',
    0.65,
    'CPU-optimized mode: minimum target CPU utilization. The end sample must be'
    ' between this number and --ycsb_cpu_optimization_target, else an exception'
    ' will be thrown.',
)
CPU_OPTIMIZATION_TARGET = flags.DEFINE_float(
    'ycsb_cpu_optimization_target',
    0.70,
    'CPU-optimized mode: maximum target CPU utilization at which to stop the'
    ' test. The test will continue to increment until the utilization level is'
    ' reached, and then return the maximum throughput for that usage level.',
)
_CPU_OPTIMIZATION_SLEEP_MINS = flags.DEFINE_integer(
    'ycsb_cpu_optimization_sleep_mins',
    0,
    'CPU-optimized mode: time in minutes to sleep between run steps when'
    ' increasing target QPS.',
)
CPU_OPTIMIZATION_INCREMENT_MINS = flags.DEFINE_integer(
    'ycsb_cpu_optimization_workload_mins',
    30,
    'CPU-optimized mode: length of time to run YCSB until incrementing QPS.',
)
CPU_OPTIMIZATION_MEASUREMENT_MINS = flags.DEFINE_integer(
    'ycsb_cpu_optimization_measurement_mins',
    5,
    'CPU-optimized mode: length of time to measure average CPU at the end of'
    ' each step. For example, the default 5 means that only the last 5 minutes'
    ' of the test will be used for representative CPU utilization. Must be '
    ' less than or equal to --ycsb_cpu_optimization_workload_mins. Note that '
    ' some APIs can have a delay in reporting results, so increase this'
    ' accordingly.',
)
_LOWEST_LATENCY = flags.DEFINE_bool(
    'ycsb_lowest_latency_load',
    False,
    'Finds the lowest latency the database can sustain and returns the relevant'
    ' samples.',
)


def _ValidateCpuTargetFlags(flags_dict):
  return (
      flags_dict['ycsb_cpu_optimization_target']
      > flags_dict['ycsb_cpu_optimization_target_min']
  )


def _ValidateCpuMeasurementFlag(flags_dict):
  return (
      flags_dict['ycsb_cpu_optimization_measurement_mins']
      <= flags_dict['ycsb_cpu_optimization_workload_mins']
  )


flags.register_multi_flags_validator(
    ['ycsb_cpu_optimization_target', 'ycsb_cpu_optimization_target_min'],
    _ValidateCpuTargetFlags,
    'CPU optimization target must be greater than min target.',
)
flags.register_multi_flags_validator(
    [
        'ycsb_cpu_optimization_measurement_mins',
        'ycsb_cpu_optimization_workload_mins',
    ],
    _ValidateCpuMeasurementFlag,
    'CPU measurement minutes must be shorter than or equal to workload'
    ' duration.',
)

# Status line pattern
_STATUS_PATTERN = r'(\d+) sec: \d+ operations; (\d+(\.\d+)?) current ops\/sec'
_STATUS_GROUPS_PATTERN = r'\[(.+?): (.+?)\]'
# Status interval default is 10 sec, change to 1 sec.
_STATUS_INTERVAL_SEC = 1

# Default loading thread count for non-batching backends.
DEFAULT_PRELOAD_THREADS = 32

# Customer YCSB tar url. If not set, the official YCSB release will be used.
_ycsb_tar_url = None

# Parameters for incremental workload. Can be made into flags in the future.
_INCREMENTAL_STARTING_QPS = 500
_INCREMENTAL_TIMELIMIT_SEC = 60 * 5

# The upper-bound number of milliseconds above the measured minimum after which
# to stop the test.
_LOWEST_LATENCY_BUFFER = 1
_LOWEST_LATENCY_STARTING_QPS = 100
_LOWEST_LATENCY_PERCENTILE = 'p95'

_ThroughputTimeSeries = dict[int, float]
# Tuple of (percentile, latency, count)
_HdrHistogramTuple = tuple[float, float, int]


def SetYcsbTarUrl(url):
  global _ycsb_tar_url
  _ycsb_tar_url = url


def _GetVersion(version_str):
  """Returns the version from ycsb version string.

  Args:
    version_str: ycsb version string with format '0.<version>.0'.

  Returns:
    (int) version.
  """
  return int(version_str.split('.')[1])


def _GetVersionFromUrl(url):
  """Returns the version from ycsb url string.

  Args:
    url: ycsb url string with format
      'https://github.com/brianfrankcooper/YCSB/releases/'
      'download/0.<version>.0/ycsb-0.<version>.0.tar.gz' OR
      'https://storage.googleapis.com/<ycsb_client_jar>/ycsb-0.<version>.0.tar.gz'
      OR
      'https://storage.googleapis.com/externally_shared_files/ycsb-0.<version>.0-SNAPSHOT.tar.gz'

  Returns:
    (int) version.
  """
  # matches ycsb-0.<version>.0
  match = re.search(r'ycsb-0\.\d{2}\.0', url)
  return _GetVersion(match.group(0).strip('ycsb-'))


def _GetThreadsQpsPerLoaderList():
  """Returns the list of [client, qps] per VM to use in staircase load."""

  def _FormatThreadQps(thread_qps):
    thread_qps_pair = thread_qps.split(':')
    if len(thread_qps_pair) == 1:
      thread_qps_pair.append(0)
    return [int(val) for val in thread_qps_pair]

  return [
      _FormatThreadQps(thread_qps)
      for thread_qps in FLAGS.ycsb_threads_per_client
  ]


def GetWorkloadFileList() -> list[str]:
  """Returns the list of workload files to run.

  Returns:
    In order of preference:
      * The argument to --ycsb_workload_files.
      * Bundled YCSB workloads A and B.
  """
  return [data.ResourcePath(workload) for workload in FLAGS.ycsb_workload_files]


def _GetRunParameters() -> dict[str, str]:
  """Returns a dict of params from the --ycsb_run_parameters flag."""
  result = {}
  for kv in FLAGS.ycsb_run_parameters:
    param, value = kv.split('=', 1)
    result[param] = value
  return result


def CheckPrerequisites():
  """Verifies that the workload files are present and parameters are valid.

  Raises:
    IOError: On missing workload file.
    errors.Config.InvalidValue on unsupported YCSB version or configs.
  """
  for workload_file in GetWorkloadFileList():
    if not os.path.exists(workload_file):
      raise IOError('Missing workload file: {0}'.format(workload_file))

  if _ycsb_tar_url:
    ycsb_version = _GetVersionFromUrl(_ycsb_tar_url)
  elif FLAGS.ycsb_tar_url:
    ycsb_version = _GetVersionFromUrl(FLAGS.ycsb_tar_url)
  else:
    ycsb_version = _GetVersion(FLAGS.ycsb_version)

  if ycsb_version < 17:
    raise errors.Config.InvalidValue('must use YCSB version 0.17.0 or higher.')

  run_params = _GetRunParameters()

  # Following flags are mutully exclusive.
  run_target = 'target' in run_params
  per_thread_target = any(
      [':' in thread_qps for thread_qps in FLAGS.ycsb_threads_per_client]
  )
  dynamic_load = FLAGS.ycsb_dynamic_load

  if run_target + per_thread_target + dynamic_load > 1:
    raise errors.Config.InvalidValue(
        'Setting YCSB target in ycsb_threads_per_client '
        'or ycsb_run_parameters or applying ycsb_dynamic_load_* flags'
        ' are mutally exclusive.'
    )

  if FLAGS.ycsb_dynamic_load_throughput_lower_bound and not dynamic_load:
    raise errors.Config.InvalidValue(
        'To apply dynamic load, set --ycsb_dynamic_load.'
    )

  if _BURST_LOAD_MULTIPLIER.value and not run_target:
    raise errors.Config.InvalidValue(
        'Running in burst mode requires setting a target QPS using '
        '--ycsb_run_parameters=target=qps. Got None.'
    )

  if _INCREMENTAL_TARGET_QPS.value and run_target:
    raise errors.Config.InvalidValue(
        'Running in incremental mode requires setting a target QPS using '
        '--ycsb_incremental_load=target and not --ycsb_run_parameters.'
    )

  # Both HISTOGRAM and TIMESERIES do not output latencies on a per-interval
  # basis, so we use the more-detailed HDRHISTOGRAM.
  if (
      _THROUGHPUT_TIME_SERIES.value
      and FLAGS.ycsb_measurement_type != ycsb_stats.HDRHISTOGRAM
  ):
    raise errors.Config.InvalidValue(
        'Measuring a throughput histogram requires running with '
        '--ycsb_measurement_type=HDRHISTOGRAM. Other measurement types are '
        'unsupported unless additional parsing is added.'
    )

  if [
      dynamic_load,
      _BURST_LOAD_MULTIPLIER.value is not None,
      _INCREMENTAL_TARGET_QPS.value is not None,
      CPU_OPTIMIZATION.value,
      _LOWEST_LATENCY.value,
  ].count(True) > 1:
    raise errors.Setup.InvalidFlagConfigurationError(
        '--ycsb_dynamic_load, --ycsb_burst_load, --ycsb_incremental_load,'
        ' --ycsb_lowest_latency_load and --ycsb_cpu_optimization are mutually'
        ' exclusive.'
    )


@vm_util.Retry(poll_interval=1)
def Install(vm):
  """Installs the YCSB and, if needed, hdrhistogram package on the VM."""
  vm.Install('openjdk')
  # TODO(user): replace with Python 3 when supported.
  # https://github.com/brianfrankcooper/YCSB/issues/1459
  vm.Install('python')
  vm.InstallPackages('curl')
  ycsb_url = (
      _ycsb_tar_url
      or FLAGS.ycsb_tar_url
      or YCSB_URL_TEMPLATE.format(FLAGS.ycsb_version)
  )
  install_cmd = (
      'mkdir -p {0} && curl -L {1} | '
      'tar -C {0} --strip-components=1 -xzf - '
      # Log4j 2 < 2.16 is vulnerable to
      # https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228.
      # YCSB currently ships with a number of vulnerable jars. None are used by
      # PKB, so simply exclude them.
      # After https://github.com/brianfrankcooper/YCSB/pull/1583 is merged and
      # released, this will not be necessary.
      # TODO(user): Update minimum YCSB version and remove.
      "--exclude='**/log4j-core-2*.jar' "
  )
  vm.RemoteCommand(install_cmd.format(YCSB_DIR, ycsb_url))
  if _GetVersion(FLAGS.ycsb_version) >= 11:
    vm.Install('maven')
    vm.RemoteCommand(install_cmd.format(HDRHISTOGRAM_DIR, HDRHISTOGRAM_TAR_URL))
    # _JAVA_OPTIONS needed to work around this issue:
    # https://stackoverflow.com/questions/53010200/maven-surefire-could-not-find-forkedbooter-class
    # https://stackoverflow.com/questions/34170811/maven-connection-reset-error
    vm.RemoteCommand(
        'cd {hist_dir} && _JAVA_OPTIONS=-Djdk.net.URLClassPath.'
        'disableClassPathURLCheck=true,https.protocols=TLSv1.2 '
        '{mvn_cmd}'.format(
            hist_dir=HDRHISTOGRAM_DIR, mvn_cmd=maven.GetRunCommand('install')
        )
    )


def ParseWorkload(contents):
  """Parse a YCSB workload file.

  YCSB workloads are Java .properties format.
  http://en.wikipedia.org/wiki/.properties
  This function does not support all .properties syntax, in particular escaped
  newlines.

  Args:
    contents: str. Contents of the file.

  Returns:
    dict mapping from property key to property value for each property found in
    'contents'.
  """
  fp = io.StringIO(contents)
  result = {}
  for line in fp:
    if (
        line.strip()
        and not line.lstrip().startswith('#')
        and not line.lstrip().startswith('!')
    ):
      k, v = re.split(r'\s*[:=]\s*', line, maxsplit=1)
      result[k] = v.strip()
  return result


@vm_util.Retry(poll_interval=10, max_retries=10)
def PushWorkload(vm, workload_file, remote_path):
  """Pushes the workload file to the VM."""
  if os.path.basename(remote_path):
    vm.RemoteCommand('sudo rm -f ' + remote_path)
  vm.PushFile(workload_file, remote_path)


class YCSBExecutor:
  """Load data and run benchmarks using YCSB.

  See core/src/main/java/com/yahoo/ycsb/workloads/CoreWorkload.java for
  attribute descriptions.

  Attributes:
    database: str.
    loaded: boolean. If the database is already loaded.
    parameters: dict. May contain the following, plus database-specific fields
      (e.g., columnfamily for HBase).
      threads: int.
      target: int.
      fieldcount: int.
      fieldlengthdistribution: str.
      readallfields: boolean.
      writeallfields: boolean.
      readproportion: float.
      updateproportion: float.
      scanproportion: float.
      readmodifywriteproportion: float.
      requestdistribution: str.
      maxscanlength: int. Number of records to scan.
      scanlengthdistribution: str.
      insertorder: str.
      hotspotdatafraction: float.
      perclientparam: list.
      shardkeyspace: boolean. Default to False, indicates if clients should have
        their own keyspace.
    burst_time_offset_sec: When running with --ycsb_burst_load, the amount of
      seconds to offset time series measurements during the increased load.
  """

  FLAG_ATTRIBUTES = 'cp', 'jvm-args', 'target', 'threads'

  def __init__(self, database, parameter_files=None, **kwargs):
    self.database = database
    self.loaded = False
    self.measurement_type = FLAGS.ycsb_measurement_type
    self.hdr_dir = HDRHISTOGRAM_DIR

    self.parameter_files = parameter_files or []
    self.parameters = kwargs.copy()
    self.parameters['measurementtype'] = self.measurement_type
    self.parameters['measurement.interval'] = FLAGS.ycsb_measurement_interval

    # Self-defined parameters, pop them out of self.parameters, so they
    # are not passed to ycsb commands
    self.perclientparam = self.parameters.pop('perclientparam', None)
    self.shardkeyspace = self.parameters.pop('shardkeyspace', False)

    self.burst_time_offset_sec = 0

  def _BuildCommand(self, command_name, parameter_files=None, **kwargs):
    """Builds the YCSB command line."""
    command = [YCSB_EXE, command_name, self.database]

    parameters = self.parameters.copy()
    parameters.update(kwargs)

    # Adding -s prints status which includes average throughput per sec.
    if _THROUGHPUT_TIME_SERIES.value and command_name == 'run':
      command.append('-s')
      parameters['status.interval'] = _STATUS_INTERVAL_SEC

    # These are passed as flags rather than properties, so they
    # are handled differently.
    for flag in self.FLAG_ATTRIBUTES:
      value = parameters.pop(flag, None)
      if value is not None:
        command.extend(('-{0}'.format(flag), str(value)))

    for param_file in list(self.parameter_files) + list(parameter_files or []):
      command.extend(('-P', param_file))

    for parameter, value in parameters.items():
      command.extend(('-p', '{0}={1}'.format(parameter, value)))

    return 'cd %s && %s' % (YCSB_DIR, ' '.join(command))

  @property
  def _default_preload_threads(self):
    """The default number of threads to use for pre-populating the DB."""
    if FLAGS['ycsb_preload_threads'].present:
      return FLAGS.ycsb_preload_threads
    return DEFAULT_PRELOAD_THREADS

  def _Load(self, vm, **kwargs):
    """Execute 'ycsb load' on 'vm'."""
    kwargs.setdefault('threads', self._default_preload_threads)
    if FLAGS.ycsb_record_count:
      kwargs.setdefault('recordcount', FLAGS.ycsb_record_count)
    for pv in FLAGS.ycsb_load_parameters:
      param, value = pv.split('=', 1)
      kwargs[param] = value
    command = self._BuildCommand('load', **kwargs)
    stdout, stderr = vm.RobustRemoteCommand(command)
    return ycsb_stats.ParseResults(
        str(stderr + stdout), self.measurement_type, _ERROR_RATE_THRESHOLD.value
    )

  def _LoadThreaded(self, vms, workload_file, **kwargs):
    """Runs "Load" in parallel for each VM in VMs.

    Args:
      vms: List of virtual machine instances. client nodes.
      workload_file: YCSB Workload file to use.
      **kwargs: Additional key-value parameters to pass to YCSB.

    Returns:
      List of sample.Sample objects.
    Raises:
      IOError: If number of results is not equal to the number of VMs.
    """
    results = []

    kwargs.setdefault('threads', self._default_preload_threads)
    if FLAGS.ycsb_record_count:
      kwargs.setdefault('recordcount', FLAGS.ycsb_record_count)
    if FLAGS.ycsb_field_count:
      kwargs.setdefault('fieldcount', FLAGS.ycsb_field_count)
    if FLAGS.ycsb_field_length:
      kwargs.setdefault('fieldlength', FLAGS.ycsb_field_length)

    with open(workload_file) as fp:
      workload_meta = ParseWorkload(fp.read())
      workload_meta.update(kwargs)
      workload_meta.update(
          stage='load',
          clients=len(vms) * kwargs['threads'],
          threads_per_client_vm=kwargs['threads'],
          workload_name=os.path.basename(workload_file),
      )
      self.workload_meta = workload_meta
    record_count = int(workload_meta.get('recordcount', '1000'))
    n_per_client = int(record_count) // len(vms)
    loader_counts = [
        n_per_client + (1 if i < (record_count % len(vms)) else 0)
        for i in range(len(vms))
    ]

    remote_path = posixpath.join(
        linux_packages.INSTALL_DIR, os.path.basename(workload_file)
    )

    args = [((vm, workload_file, remote_path), {}) for vm in dict.fromkeys(vms)]
    background_tasks.RunThreaded(PushWorkload, args)

    kwargs['parameter_files'] = [remote_path]

    def _Load(loader_index):
      start = sum(loader_counts[:loader_index])
      kw = copy.deepcopy(kwargs)
      kw.update(insertstart=start, insertcount=loader_counts[loader_index])
      if self.perclientparam is not None:
        kw.update(self.perclientparam[loader_index])
      results.append(self._Load(vms[loader_index], **kw))
      logging.info('VM %d (%s) finished', loader_index, vms[loader_index])

    start = time.time()
    background_tasks.RunThreaded(_Load, list(range(len(vms))))
    events.record_event.send(
        type(self).__name__,
        event='load',
        start_timestamp=start,
        end_timestamp=time.time(),
        metadata=copy.deepcopy(kwargs),
    )

    if len(results) != len(vms):
      raise IOError(
          'Missing results: only {0}/{1} reported\n{2}'.format(
              len(results), len(vms), results
          )
      )

    samples = []
    if FLAGS.ycsb_include_individual_results and len(results) > 1:
      for i, result in enumerate(results):
        samples.extend(
            ycsb_stats.CreateSamples(
                ycsb_result=result,
                ycsb_version=FLAGS.ycsb_version,
                include_command_line=_SHOULD_RECORD_COMMAND_LINE.value,
                result_type='individual',
                result_index=i,
                **workload_meta,
            )
        )

    # hdr histograms not collected upon load, only upon run
    combined = ycsb_stats.CombineResults(results, self.measurement_type, {})
    samples.extend(
        ycsb_stats.CreateSamples(
            ycsb_result=combined,
            ycsb_version=FLAGS.ycsb_version,
            include_histogram=FLAGS.ycsb_histogram,
            include_command_line=_SHOULD_RECORD_COMMAND_LINE.value,
            result_type='combined',
            **workload_meta,
        )
    )

    return samples

  def _Run(self, vm, **kwargs):
    """Run a single workload from a client vm."""
    for pv in FLAGS.ycsb_run_parameters:
      param, value = pv.split('=', 1)
      kwargs[param] = value
    command = self._BuildCommand('run', **kwargs)
    # YCSB version greater than 0.7.0 output some of the
    # info we need to stderr. So we have to combine these 2
    # output to get expected results.
    hdr_files_dir = kwargs.get('hdrhistogram.output.path', None)
    if hdr_files_dir:
      vm.RemoteCommand('mkdir -p {0}'.format(hdr_files_dir))
    stdout, stderr = vm.RobustRemoteCommand(command)
    return ycsb_stats.ParseResults(
        str(stderr + stdout),
        self.measurement_type,
        _ERROR_RATE_THRESHOLD.value,
        self.burst_time_offset_sec,
    )

  def _RunThreaded(self, vms, **kwargs):
    """Run a single workload using `vms`."""
    target = kwargs.pop('target', None)
    if target is not None:
      target_per_client = target // len(vms)
      targets = [
          target_per_client + (1 if i < (target % len(vms)) else 0)
          for i in range(len(vms))
      ]
    else:
      targets = [target for _ in vms]

    results = []

    if self.shardkeyspace:
      record_count = int(self.workload_meta.get('recordcount', '1000'))
      n_per_client = int(record_count) // len(vms)
      loader_counts = [
          n_per_client + (1 if i < (record_count % len(vms)) else 0)
          for i in range(len(vms))
      ]

    def _Run(loader_index):
      """Run YCSB on an individual VM."""
      vm = vms[loader_index]
      params = copy.deepcopy(kwargs)
      params['target'] = targets[loader_index]
      if self.perclientparam is not None:
        params.update(self.perclientparam[loader_index])
      if self.shardkeyspace:
        start = sum(loader_counts[:loader_index])
        end = start + loader_counts[loader_index]
        params.update(insertstart=start, recordcount=end)
      results.append(self._Run(vm, **params))
      logging.info('VM %d (%s) finished', loader_index, vm)

    background_tasks.RunThreaded(_Run, list(range(len(vms))))

    if len(results) != len(vms):
      raise IOError(
          'Missing results: only {0}/{1} reported\n{2}'.format(
              len(results), len(vms), results
          )
      )

    return results

  def _GetRunLoadTarget(self, current_load, is_sustained=False):
    """Get load target.

    If service cannot sustain current load, adjust load applied to the serivce
    based on ycsb_dynamic_load_sustain_throughput_ratio.
    If service is capable of handling current load and we are still above
    ycsb_dynamic_load_throughput_lower_bound, keep reducing the load
    (step size=2*(1-ycsb_dynamic_load_sustain_throughput_ratio)) and
    run test for reduced duration based on ycsb_dynamic_load_sustain_timelimit.

    Args:
      current_load: float. Current client load (QPS) applied to system under
        test.
      is_sustained: boolean. Indicate if system is capable of sustaining the
        load.

    Returns:
      Total client load (QPS) to apply to system under test.
    """
    lower_bound = FLAGS.ycsb_dynamic_load_throughput_lower_bound
    step = (1 - FLAGS.ycsb_dynamic_load_sustain_throughput_ratio) * 2

    if (
        (not bool(lower_bound) and is_sustained)
        or (lower_bound and current_load < lower_bound)
        or (current_load is None)
    ):
      return None
    elif is_sustained:
      return current_load * (1 - step)
    else:
      return current_load / FLAGS.ycsb_dynamic_load_sustain_throughput_ratio

  def RunStaircaseLoads(self, vms, workloads, **kwargs):
    """Run each workload in 'workloads' in succession.

    A staircase load is applied for each workload file, for each entry in
    ycsb_threads_per_client.

    Args:
      vms: List of VirtualMachine objects to generate load from.
      workloads: List of workload file names.
      **kwargs: Additional parameters to pass to each run.  See constructor for
        options.

    Returns:
      List of sample.Sample objects.
    """
    all_results = []
    parameters = {}
    for workload_index, workload_file in enumerate(workloads):
      if FLAGS.ycsb_operation_count:
        parameters = {'operationcount': FLAGS.ycsb_operation_count}
      if FLAGS.ycsb_record_count:
        parameters['recordcount'] = FLAGS.ycsb_record_count
      if FLAGS.ycsb_field_count:
        parameters['fieldcount'] = FLAGS.ycsb_field_count
      if FLAGS.ycsb_field_length:
        parameters['fieldlength'] = FLAGS.ycsb_field_length
      if FLAGS.ycsb_timelimit:
        parameters['maxexecutiontime'] = FLAGS.ycsb_timelimit
      hdr_files_dir = posixpath.join(self.hdr_dir, str(workload_index))
      if FLAGS.ycsb_measurement_type == ycsb_stats.HDRHISTOGRAM:
        parameters['hdrhistogram.fileoutput'] = True
        parameters['hdrhistogram.output.path'] = hdr_files_dir
      if FLAGS.ycsb_requestdistribution:
        parameters['requestdistribution'] = FLAGS.ycsb_requestdistribution
      if FLAGS.ycsb_readproportion is not None:
        parameters['readproportion'] = FLAGS.ycsb_readproportion
      if FLAGS.ycsb_updateproportion is not None:
        parameters['updateproportion'] = FLAGS.ycsb_updateproportion
      if FLAGS.ycsb_scanproportion is not None:
        parameters['scanproportion'] = FLAGS.ycsb_scanproportion
      parameters.update(kwargs)
      remote_path = posixpath.join(
          linux_packages.INSTALL_DIR, os.path.basename(workload_file)
      )

      with open(workload_file) as fp:
        workload_meta = ParseWorkload(fp.read())
        workload_meta.update(kwargs)
        workload_meta.update(
            workload_name=os.path.basename(workload_file),
            workload_index=workload_index,
            stage='run',
        )

      args = [
          ((vm, workload_file, remote_path), {}) for vm in dict.fromkeys(vms)
      ]
      background_tasks.RunThreaded(PushWorkload, args)

      parameters['parameter_files'] = [remote_path]

      # _GetThreadsQpsPerLoaderList() passes tuple of (client_count, target=0)
      # if no target is passed via flags.
      for client_count, target_qps_per_vm in _GetThreadsQpsPerLoaderList():

        @vm_util.Retry(
            retryable_exceptions=ycsb_stats.CombineHdrLogError,
            timeout=-1,
            max_retries=5,
        )
        def _DoRunStairCaseLoad(
            client_count, target_qps_per_vm, workload_meta, is_sustained=False
        ):
          parameters['threads'] = client_count
          if target_qps_per_vm:
            parameters['target'] = int(target_qps_per_vm * len(vms))
          if is_sustained:
            parameters['maxexecutiontime'] = (
                FLAGS.ycsb_dynamic_load_sustain_timelimit
            )
          start = time.time()
          results = self._RunThreaded(vms, **parameters)
          events.record_event.send(
              type(self).__name__,
              event='run',
              start_timestamp=start,
              end_timestamp=time.time(),
              metadata=copy.deepcopy(parameters),
          )
          client_meta = workload_meta.copy()
          client_meta.update(parameters)
          client_meta.update(
              clients=len(vms) * client_count,
              threads_per_client_vm=client_count,
          )
          # Values passed in via this flag do not get recorded in metadata.
          # The target passed in is applied to each client VM, so multiply by
          # len(vms).
          for pv in FLAGS.ycsb_run_parameters:
            param, value = pv.split('=', 1)
            if param == 'target':
              value = int(value) * len(vms)
            client_meta[param] = value

          if FLAGS.ycsb_include_individual_results and len(results) > 1:
            for i, result in enumerate(results):
              all_results.extend(
                  ycsb_stats.CreateSamples(
                      ycsb_result=result,
                      ycsb_version=FLAGS.ycsb_version,
                      include_histogram=FLAGS.ycsb_histogram,
                      include_command_line=_SHOULD_RECORD_COMMAND_LINE.value,
                      result_type='individual',
                      result_index=i,
                      **client_meta,
                  )
              )

          if self.measurement_type == ycsb_stats.HDRHISTOGRAM:
            combined_log = ycsb_stats.CombineHdrHistogramLogFiles(
                self.hdr_dir, parameters['hdrhistogram.output.path'], vms
            )
            parsed_hdr = ycsb_stats.ParseHdrLogs(combined_log)
            combined = ycsb_stats.CombineResults(
                results, self.measurement_type, parsed_hdr
            )
          else:
            combined = ycsb_stats.CombineResults(
                results, self.measurement_type, {}
            )
          run_samples = list(
              ycsb_stats.CreateSamples(
                  ycsb_result=combined,
                  ycsb_version=FLAGS.ycsb_version,
                  include_command_line=_SHOULD_RECORD_COMMAND_LINE.value,
                  include_histogram=FLAGS.ycsb_histogram,
                  result_type='combined',
                  **client_meta,
              )
          )

          overall_throughput = 0
          for s in run_samples:
            if s.metric == 'overall Throughput':
              overall_throughput += s.value
          return overall_throughput, run_samples

        target_throughput, run_samples = _DoRunStairCaseLoad(
            client_count, target_qps_per_vm, workload_meta
        )

        # Uses 5 * unthrottled throughput as starting point.
        target_throughput *= 5
        all_results.extend(run_samples)
        is_sustained = False
        while FLAGS.ycsb_dynamic_load:
          actual_throughput, run_samples = _DoRunStairCaseLoad(
              client_count,
              target_throughput // len(vms),
              workload_meta,
              is_sustained,
          )
          is_sustained = FLAGS.ycsb_dynamic_load_sustain_throughput_ratio < (
              actual_throughput / target_throughput
          )
          for s in run_samples:
            s.metadata['sustained'] = is_sustained
          all_results.extend(run_samples)
          target_throughput = self._GetRunLoadTarget(
              actual_throughput, is_sustained
          )
          if target_throughput is None:
            break

    return all_results

  def Load(self, vms, workloads=None, load_kwargs=None):
    """Load data using YCSB."""
    if SKIP_LOAD_STAGE.value:
      return []

    workloads = workloads or GetWorkloadFileList()
    load_samples = []
    assert workloads, 'no workloads'

    def _HasInsertFailures(result_samples):
      for s in result_samples:
        if s.metric == _INCOMPLETE_LOADING_METRIC.value and s.value > 0:
          return True
      return False

    if FLAGS.ycsb_reload_database or not self.loaded:
      load_samples += list(
          self._LoadThreaded(vms, workloads[0], **(load_kwargs or {}))
      )
      if _SHOULD_FAIL_ON_INCOMPLETE_LOADING.value and _HasInsertFailures(
          load_samples
      ):
        raise errors.Benchmarks.RunError(
            'There are insert failures, so the table loading is incomplete'
        )

      self.loaded = True
    if FLAGS.ycsb_sleep_after_load_in_sec > 0:
      logging.info(
          'Sleeping %s seconds after load stage.',
          FLAGS.ycsb_sleep_after_load_in_sec,
      )
      time.sleep(FLAGS.ycsb_sleep_after_load_in_sec)
    if FLAGS.ycsb_load_samples:
      return load_samples
    else:
      return []

  def Run(
      self, vms, workloads=None, run_kwargs=None, database=None
  ) -> list[sample.Sample]:
    """Runs each workload/client count combination."""
    if FLAGS.ycsb_skip_run_stage:
      return []
    workloads = workloads or GetWorkloadFileList()
    assert workloads, 'no workloads'
    if not run_kwargs:
      run_kwargs = {}
    if _BURST_LOAD_MULTIPLIER.value:
      samples = self._RunBurstMode(vms, workloads, run_kwargs)
    elif _INCREMENTAL_TARGET_QPS.value:
      samples = self._RunIncrementalMode(vms, workloads, run_kwargs)
    elif CPU_OPTIMIZATION.value:
      samples = self._RunCpuMode(vms, workloads, run_kwargs, database)
    elif _LOWEST_LATENCY.value:
      samples = self._RunLowestLatencyMode(vms, workloads, run_kwargs)
    else:
      samples = list(self.RunStaircaseLoads(vms, workloads, **run_kwargs))
    if (
        FLAGS.ycsb_sleep_after_load_in_sec > 0
        and not SKIP_LOAD_STAGE.value
    ):
      for s in samples:
        s.metadata['sleep_after_load_in_sec'] = (
            FLAGS.ycsb_sleep_after_load_in_sec
        )
    return samples

  def _SetRunParameters(self, params: Mapping[str, Any]) -> None:
    """Sets the --ycsb_run_parameters flag."""
    # Ideally YCSB should be refactored to include a function that just takes
    # commands for a run, but that will be a large refactor.
    FLAGS['ycsb_run_parameters'].unparse()
    FLAGS['ycsb_run_parameters'].parse([f'{k}={v}' for k, v in params.items()])

  def _RunBurstMode(self, vms, workloads, run_kwargs=None):
    """Runs YCSB in burst mode, where the second run has increased QPS."""
    run_params = _GetRunParameters()
    initial_qps = int(run_params.get('target', 0))

    samples = list(self.RunStaircaseLoads(vms, workloads, **run_kwargs))
    # Attach metadata for identifying pre burst load.
    for s in samples:
      s.metadata['ycsb_burst_multiplier'] = 1

    self.burst_time_offset_sec = FLAGS.ycsb_timelimit

    if _BURST_LOAD_MULTIPLIER.value == -1:
      run_params.pop('target')  # Set to unlimited
    else:
      run_params['target'] = initial_qps * _BURST_LOAD_MULTIPLIER.value
    self._SetRunParameters(run_params)
    burst_samples = list(self.RunStaircaseLoads(vms, workloads, **run_kwargs))
    for s in burst_samples:
      s.metadata['ycsb_burst_multiplier'] = _BURST_LOAD_MULTIPLIER.value

    return samples + burst_samples

  def _GetIncrementalQpsTargets(self, target_qps: int) -> list[int]:
    """Returns incremental QPS targets."""
    qps = _INCREMENTAL_STARTING_QPS
    result = []
    while qps < target_qps:
      result.append(qps)
      qps *= 1.5
    return result

  def _SetClientThreadCount(self, count: int) -> None:
    FLAGS['ycsb_threads_per_client'].unparse()
    FLAGS['ycsb_threads_per_client'].parse([str(count)])

  def _RunIncrementalMode(
      self,
      vms: Sequence[virtual_machine.VirtualMachine],
      workloads: Sequence[str],
      run_kwargs: Mapping[str, str] = None,
  ) -> list[sample.Sample]:
    """Runs YCSB by gradually incrementing target QPS.

    Note that this requires clients to be overprovisioned, as the target QPS
    for YCSB is generally a "throttling" mechanism where the threads try to send
    as much QPS as possible and then get throttled. If clients are
    underprovisioned then it's possible for the run to not hit the desired
    target, which may be undesired behavior.

    See
    https://cloud.google.com/datastore/docs/best-practices#ramping_up_traffic
    for an example of why this is needed.

    Args:
      vms: The client VMs to generate the load.
      workloads: List of workloads to run.
      run_kwargs: Extra run arguments.

    Returns:
      A list of samples of benchmark results.
    """
    run_params = _GetRunParameters()
    ending_qps = _INCREMENTAL_TARGET_QPS.value
    ending_length = FLAGS.ycsb_timelimit
    ending_threadcount = int(FLAGS.ycsb_threads_per_client[0])
    incremental_targets = self._GetIncrementalQpsTargets(ending_qps)
    logging.info('Incremental targets: %s', incremental_targets)

    # Warm-up phase is shorter and doesn't need results parsing
    FLAGS['ycsb_timelimit'].parse(_INCREMENTAL_TIMELIMIT_SEC)
    for target in incremental_targets:
      target /= len(vms)
      run_params['target'] = int(target)
      self._SetClientThreadCount(min(ending_threadcount, int(target)))
      self._SetRunParameters(run_params)
      self.RunStaircaseLoads(vms, workloads, **run_kwargs)

    # Reset back to the original workload args
    FLAGS['ycsb_timelimit'].parse(ending_length)
    ending_qps /= len(vms)
    run_params['target'] = int(ending_qps)
    self._SetClientThreadCount(ending_threadcount)
    self._SetRunParameters(run_params)
    return list(self.RunStaircaseLoads(vms, workloads, **run_kwargs))

  def _RunLowestLatencyMode(
      self,
      vms: Sequence[virtual_machine.VirtualMachine],
      workloads: Sequence[str],
      run_kwargs: Mapping[str, str] = None,
  ) -> list[sample.Sample]:
    """Finds the lowest sustainable latency of the target.

    Args:
      vms: The client VMs to generate the load.
      workloads: List of workloads to run.
      run_kwargs: Extra run arguments.

    Returns:
      A list of samples of benchmark results.
    """

    @dataclasses.dataclass
    class _ThroughputLatencyResult:
      throughput: int = 0
      read_latency: float = float('inf')
      update_latency: float = float('inf')
      samples: list[sample.Sample] = dataclasses.field(default_factory=list)

    def _ExtractStats(samples: list[sample.Sample]) -> tuple[int, float, float]:
      """Returns the throughput and latency recorded in the samples."""
      throughput, read_latency, update_latency = 0, 0, 0
      for result in samples:
        if result.metric == 'overall Throughput':
          throughput = result.value
        elif result.metric == f'read {_LOWEST_LATENCY_PERCENTILE} latency':
          read_latency = result.value
        elif result.metric == f'update {_LOWEST_LATENCY_PERCENTILE} latency':
          update_latency = result.value
      return int(throughput), read_latency, update_latency

    run_params = _GetRunParameters()
    target = _LOWEST_LATENCY_STARTING_QPS
    read_latency_threshold = 0
    update_latency_threshold = 0
    result = _ThroughputLatencyResult()
    while True:
      run_params['target'] = target
      self._SetClientThreadCount(
          min(target, int(FLAGS.ycsb_threads_per_client[0]))
      )
      self._SetRunParameters(run_params)
      samples = self.RunStaircaseLoads([vms[0]], workloads, **run_kwargs)
      # Currently uses p95 latencies, but could be generalized in the future.
      throughput, read_latency, update_latency = _ExtractStats(samples)
      # Assume that we see lowest latency at the lowest starting throughput
      if target == _LOWEST_LATENCY_STARTING_QPS:
        read_latency_threshold = read_latency + _LOWEST_LATENCY_BUFFER
        update_latency_threshold = update_latency + _LOWEST_LATENCY_BUFFER
      logging.info(
          'Run had throughput %s ops/s, read %s latency %s ms, update %s'
          ' latency %s ms',
          throughput,
          _LOWEST_LATENCY_PERCENTILE,
          read_latency,
          _LOWEST_LATENCY_PERCENTILE,
          update_latency,
      )
      if (
          read_latency > read_latency_threshold
          or update_latency > update_latency_threshold
      ):
        logging.info(
            'Found lowest latency at %s ops/s. Run had higher read and/or'
            ' update latency than threshold %s read %s ms, update %s ms.',
            result.throughput,
            _LOWEST_LATENCY_PERCENTILE,
            read_latency_threshold,
            update_latency_threshold,
        )
        for s in result.samples:
          s.metadata['ycsb_lowest_latency_buffer'] = _LOWEST_LATENCY_BUFFER
          s.metadata['ycsb_lowest_latency_percentile'] = (
              _LOWEST_LATENCY_PERCENTILE
          )
        return result.samples
      result = _ThroughputLatencyResult(
          throughput, read_latency, update_latency, samples
      )
      target += 100

  def _RunCpuMode(
      self,
      vms: list[virtual_machine.VirtualMachine],
      workloads: Sequence[str],
      run_kwargs: Mapping[str, Any],
      database: resource.BaseResource,
  ) -> list[sample.Sample]:
    """Runs YCSB until the CPU utilization is over the recommended amount.

    Args:
      vms: The client VMs that will be used to push the load.
      workloads: List of workloads to run.
      run_kwargs: A mapping of additional YCSB run args to pass to the test.
      database: This class must implement CalculateTheoreticalMaxThroughput and
        GetAverageCpuUsage.

    Returns:
      A list of samples from the YCSB test at the specified CPU utilization.
    """

    def _ExtractThroughput(samples: list[sample.Sample]) -> float:
      """Gets the throughput recorded in the samples."""
      for result in samples:
        if result.metric == 'overall Throughput':
          return result.value
      return 0.0

    def _GetReadAndUpdateProportion(workload: str) -> tuple[float, float]:
      """Gets the starting throughput to start the test with."""
      with open(workload) as f:
        workload_args = ParseWorkload(f.read())
      return float(workload_args['readproportion']), float(
          workload_args['updateproportion']
      )

    def _ExecuteWorkload(target_qps: int) -> list[sample.Sample]:
      """Executes the workload after setting run-specific args."""
      run_kwargs['target'] = target_qps
      run_kwargs['maxexecutiontime'] = (
          CPU_OPTIMIZATION_INCREMENT_MINS.value * 60
      )
      return self.RunStaircaseLoads(vms, workloads=workloads, **run_kwargs)

    def _RunCpuModeSingleWorkload(workload: str) -> list[sample.Sample]:
      """Runs the CPU utilization test for a single workload."""
      read_percent, update_percent = _GetReadAndUpdateProportion(workload)
      theoretical_max_qps = database.CalculateTheoreticalMaxThroughput(
          read_percent, update_percent
      )
      lower_bound = 0
      upper_bound = theoretical_max_qps * 2
      while lower_bound <= upper_bound:
        target_qps = int((upper_bound + lower_bound) / 2)
        run_samples = _ExecuteWorkload(target_qps)
        measured_qps = _ExtractThroughput(run_samples)
        end_timestamp = datetime.datetime.fromtimestamp(
            run_samples[0].timestamp, tz=datetime.timezone.utc
        )
        cpu_utilization = database.GetAverageCpuUsage(
            CPU_OPTIMIZATION_MEASUREMENT_MINS.value, end_timestamp
        )
        run_samples.append(
            sample.Sample(
                'CPU Normalized Throughput',
                measured_qps / cpu_utilization * CPU_OPTIMIZATION_TARGET.value,
                'ops/sec',
                copy.copy(run_samples[0].metadata),
            )
        )
        logging.info(
            'Run had throughput target %s and measured throughput %s, with CPU'
            ' utilization %s.',
            target_qps,
            measured_qps,
            cpu_utilization,
        )
        if cpu_utilization < CPU_OPTIMIZATION_TARGET_MIN.value:
          lower_bound = target_qps
        elif cpu_utilization > CPU_OPTIMIZATION_TARGET.value:
          upper_bound = target_qps
        else:
          logging.info(
              'Found CPU utilization percentage between target %s and %s',
              CPU_OPTIMIZATION_TARGET_MIN.value,
              CPU_OPTIMIZATION_TARGET.value,
          )
          for s in run_samples:
            s.metadata.update({
                'ycsb_cpu_optimization': True,
                'ycsb_cpu_utilization': cpu_utilization,
                'ycsb_cpu_target': CPU_OPTIMIZATION_TARGET.value,
                'ycsb_cpu_target_min': CPU_OPTIMIZATION_TARGET_MIN.value,
                'ycsb_cpu_increment_minutes': (
                    CPU_OPTIMIZATION_INCREMENT_MINS.value
                ),
            })
          return run_samples

        # Sleep between steps for some workloads.
        if _CPU_OPTIMIZATION_SLEEP_MINS.value:
          logging.info(
              'Run phase finished, sleeping for %s minutes before starting the '
              'next run.',
              _CPU_OPTIMIZATION_SLEEP_MINS.value,
          )
          time.sleep(_CPU_OPTIMIZATION_SLEEP_MINS.value * 60)

    results = []
    for workload in workloads:
      results += _RunCpuModeSingleWorkload(workload)
    return results

  def LoadAndRun(self, vms, workloads=None, load_kwargs=None, run_kwargs=None):
    """Load data using YCSB, then run each workload/client count combination.

    Loads data using the workload defined by 'workloads', then
    executes YCSB for each workload file in 'workloads', for each
    client count defined in FLAGS.ycsb_threads_per_client.

    Generally database benchmarks using YCSB should only need to call this
    method.

    Args:
      vms: List of virtual machines. VMs to use to generate load.
      workloads: List of strings. Workload files to use. If unspecified,
        GetWorkloadFileList() is used.
      load_kwargs: dict. Additional arguments to pass to the load stage.
      run_kwargs: dict. Additional arguments to pass to the run stage.

    Returns:
      List of sample.Sample objects.
    """
    load_samples = []
    if not SKIP_LOAD_STAGE.value:
      load_samples = self.Load(
          vms, workloads=workloads, load_kwargs=load_kwargs
      )
    run_samples = []
    if not FLAGS.ycsb_skip_run_stage:
      run_samples = self.Run(vms, workloads=workloads, run_kwargs=run_kwargs)
    return load_samples + run_samples
