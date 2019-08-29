# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""MySQL Service Benchmarks.

This is a set of benchmarks that measures performance of MySQL Databases on
managed MySQL services.

- On AWS, we will use RDS+MySQL.
- On GCP, we will use Cloud SQL v2 (Performance Edition).

As other cloud providers deliver a managed MySQL service, we will add it here.

As of May 2017 to make this benchmark run for GCP you must install the
gcloud beta component. This is necessary because creating a Cloud SQL instance
with a non-default storage size is in beta right now. This can be removed when
this feature is part of the default components.
See https://cloud.google.com/sdk/gcloud/reference/beta/sql/instances/create
for more information.
To run this benchmark for GCP it is required to install a non-default gcloud
component. Otherwise this benchmark will fail.

To ensure that gcloud beta is installed, type
        'gcloud components list'
into the terminal. This will output all components and status of each.
Make sure that
  name: gcloud Beta Commands
  id:  beta
has status: Installed.
If not, run
        'gcloud components install beta'
to install it. This will allow this benchmark to properly create an instance.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import re
import time

from perfkitbenchmarker import configs
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import publisher
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
import six


FLAGS = flags.FLAGS

# The default values for flags and BENCHMARK_CONFIG are not a recommended
# configuration for comparing sysbench performance.  Rather these values
# are set to provide a quick way to verify functionality is working.
# A broader set covering different permuations on much larger data sets
# is prefereable for comparison.
flags.DEFINE_string('sysbench_testname', 'oltp_read_write',
                    'The built in oltp lua script to run')
flags.DEFINE_integer('sysbench_tables', 4,
                     'The number of tables used in sysbench oltp.lua tests')
flags.DEFINE_integer('sysbench_table_size', 100000,
                     'The number of rows of each table used in the oltp tests')
flags.DEFINE_integer('sysbench_scale', 100,
                     'Scale parameter as used by TPCC benchmark.')
flags.DEFINE_integer('sysbench_warmup_seconds', 10,
                     'The duration of the warmup run in which results are '
                     'discarded, in seconds.')
flags.DEFINE_integer('sysbench_run_seconds', 10,
                     'The duration of the actual run in which results are '
                     'collected, in seconds.')
flag_util.DEFINE_integerlist(
    'sysbench_thread_counts',
    flag_util.IntegerList([64]),
    'array of thread counts passed to sysbench, one at a time',
    module_name=__name__)
flags.DEFINE_integer('sysbench_latency_percentile', 100,
                     'The latency percentile we ask sysbench to compute.')
flags.DEFINE_integer('sysbench_report_interval', 2,
                     'The interval, in seconds, we ask sysbench to report '
                     'results.')
flags.DEFINE_integer('sysbench_pre_failover_seconds', 0,
                     'If non zero, then after the sysbench workload is '
                     'complete, a failover test will be performed.  '
                     'When a failover test is run, the database will be driven '
                     'using the last entry in sysbench_thread_counts.  After '
                     'sysbench_pre_failover_seconds, a failover will be '
                     'triggered.  Time will be measured until sysbench '
                     'is able to connect again.')
flags.DEFINE_integer('sysbench_post_failover_seconds', 0,
                     'When non Zero, will run the benchmark an additional '
                     'amount of time after failover is complete.  Useful '
                     'for detecting if there are any differences in TPS because'
                     'of failover.')

BENCHMARK_DATA = {
    'sysbench-tpcc.tar.gz':
        'a116f0a6f58212b568bd339e65223eaf5ed59437503700002f016302d8a9c6ed',
}

_MAP_WORKLOAD_TO_VALID_UNIQUE_PARAMETERS = {
    'tpcc': set(
        ['scale']
    ),
    'oltp_read_write': set(
        ['table_size']
    )
}


BENCHMARK_NAME = 'sysbench'
BENCHMARK_CONFIG = """
sysbench:
  description: Sysbench OLTP benchmarks.
  relational_db:
    engine: mysql
    db_spec:
      GCP:
        machine_type: db-n1-standard-16
        zone: us-central1-c
      AWS:
        machine_type: db.m4.4xlarge
        zone: us-west-1a
      Azure:
        machine_type:
          tier: Standard
          compute_units: 800
        zone: westus
    db_disk_spec:
      GCP:
        disk_size: 100
        disk_type: pd-ssd
      AWS:
        disk_size: 6144
        disk_type: gp2
      Azure:
        #From AZ command line:
        #Valid storage sizes range from minimum of 128000 MB and additional
        #increments of 128000 MB up to maximum of 1024000 MB.
        disk_size: 128
    vm_groups:
      clients:
        vm_spec:
          GCP:
            machine_type: n1-standard-16
            zone: us-central1-c
          AWS:
            machine_type: m4.4xlarge
            zone: us-west-1a
          Azure:
            machine_type: Standard_A4m_v2
            zone: westus
        disk_spec: *default_500_gb
      servers:
        os_type: ubuntu1604
        vm_spec:
          GCP:
            machine_type: n1-standard-16
            zone: us-central1-c
          AWS:
            machine_type: m4.4xlarge
            zone: us-west-1a
          Azure:
            machine_type: Standard_A4m_v2
            zone: westus
        disk_spec: *default_500_gb
      replications:
        os_type: ubuntu1604
        vm_spec:
          GCP:
            machine_type: n1-standard-16
            zone: us-central1-b
          AWS:
            machine_type: m4.4xlarge
            zone: us-east-1a
          Azure:
            machine_type: Standard_A4m_v2
            zone: eastus
        disk_spec: *default_500_gb
"""

# Constants defined for Sysbench tests.
DISABLE = 'disable'
UNIFORM = 'uniform'

SECONDS_UNIT = 'seconds'

_MAX_FAILOVER_DURATION_SECONDS = 60 * 60  # 1 hour
_FAILOVER_TEST_TPS_FREQUENCY_SECONDS = 10


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def _ParseSysbenchOutput(sysbench_output):
  """Parses sysbench output.

  Extract relevant TPS and latency numbers, and populate the final result
  collection with these information.

  Specifically, we are interested in tps and latency numbers reported by each
  reporting interval.

  Args:
    sysbench_output: The output from sysbench.
  Returns:
    Three arrays, the tps, latency and qps numbers.

  """
  tps_numbers = []
  latency_numbers = []
  qps_numbers = []

  sysbench_output_io = six.StringIO(sysbench_output)
  for line in sysbench_output_io:
    # parse a line like (it's one line - broken up in the comment to fit):
    # [ 6s ] thds: 16 tps: 650.51 qps: 12938.26 (r/w/o: 9046.18/2592.05/1300.03)
    # lat (ms,99%): 40.37 err/s: 0.00 reconn/s: 0.00
    if re.match(r'^\[', line):
      match = re.search('tps: (.*?) ', line)
      tps_numbers.append(float(match.group(1)))
      match = re.search(r'lat \(.*?\): (.*?) ', line)
      latency_numbers.append(float(match.group(1)))
      match = re.search(r'qps: (.*?) \(.*?\) ', line)
      qps_numbers.append(float(match.group(1)))
      if line.startswith('SQL statistics:'):
        break

  return tps_numbers, latency_numbers, qps_numbers


def AddMetricsForSysbenchOutput(
    sysbench_output, results, metadata, metric_prefix=''):
  """Parses sysbench output.

  Extract relevant TPS and latency numbers, and populate the final result
  collection with these information.

  Specifically, we are interested in tps and latency numbers reported by each
  reporting interval.

  Args:
    sysbench_output: The output from sysbench.
    results: The dictionary to store results based on sysbench output.
    metadata: The metadata to be passed along to the Samples class.
    metric_prefix:  An optional prefix to append to each metric generated.
  """
  tps_numbers, latency_numbers, qps_numbers = (
      _ParseSysbenchOutput(sysbench_output))

  tps_metadata = metadata.copy()
  tps_metadata.update({metric_prefix + 'tps': tps_numbers})
  tps_sample = sample.Sample(metric_prefix + 'tps_array', -1,
                             'tps', tps_metadata)

  latency_metadata = metadata.copy()
  latency_metadata.update({metric_prefix + 'latency': latency_numbers})
  latency_sample = sample.Sample(metric_prefix + 'latency_array', -1, 'ms',
                                 latency_metadata)

  qps_metadata = metadata.copy()
  qps_metadata.update({metric_prefix + 'qps': qps_numbers})
  qps_sample = sample.Sample(metric_prefix + 'qps_array', -1, 'qps',
                             qps_metadata)

  results.append(tps_sample)
  results.append(latency_sample)
  results.append(qps_sample)


def _GetSysbenchCommand(duration, benchmark_spec, sysbench_thread_count):
  """Returns the sysbench command as a string."""
  if duration <= 0:
    raise ValueError('Duration must be greater than zero.')

  db = benchmark_spec.relational_db
  run_cmd_tokens = ['nice',  # run with a niceness of lower priority
                    '-15',   # to encourage cpu time for ssh commands
                    'sysbench',
                    FLAGS.sysbench_testname,
                    '--tables=%d' % FLAGS.sysbench_tables,
                    ('--table_size=%d' % FLAGS.sysbench_table_size
                     if _IsValidFlag('table_size') else ''),
                    ('--scale=%d' % FLAGS.sysbench_scale
                     if _IsValidFlag('scale') else ''),
                    '--db-ps-mode=%s' % DISABLE,
                    '--rand-type=%s' % UNIFORM,
                    '--threads=%d' % sysbench_thread_count,
                    '--percentile=%d' % FLAGS.sysbench_latency_percentile,
                    '--report-interval=%d' % FLAGS.sysbench_report_interval,
                    '--max-requests=0',
                    '--time=%d' % duration,
                    '--db-driver=mysql',
                    db.MakeSysbenchConnectionString(),
                    'run']
  run_cmd = ' '.join(run_cmd_tokens)
  return run_cmd


def _IssueSysbenchCommand(vm, duration, benchmark_spec, sysbench_thread_count):
  """Issues a sysbench run command given a vm and a duration.

      Does nothing if duration is <= 0

  Args:
    vm: The test VM to issue command to.
    duration: the duration of the sysbench run.
    benchmark_spec: The benchmark specification. Contains all data that is
                    required to run the benchmark.
    sysbench_thread_count: count of number of threads to use in --threads
                           parameter to sysbench.

  Returns:
    stdout, stderr: the result of the command.
  """
  stdout = ''
  stderr = ''
  if duration > 0:
    run_cmd = _GetSysbenchCommand(
        duration,
        benchmark_spec,
        sysbench_thread_count)
    stdout, stderr = vm.RobustRemoteCommand(run_cmd, timeout=duration + 60)
    logging.info('Sysbench results: \n stdout is:\n%s\nstderr is\n%s',
                 stdout, stderr)

  return stdout, stderr


def _IssueSysbenchCommandWithReturnCode(
    vm, duration, benchmark_spec, sysbench_thread_count, show_results=True):
  """Run sysbench workload as specified by the benchmark_spec."""
  stdout = ''
  stderr = ''
  retcode = -1
  if duration > 0:
    run_cmd = _GetSysbenchCommand(
        duration,
        benchmark_spec,
        sysbench_thread_count)
    stdout, stderr, retcode = vm.RemoteCommandWithReturnCode(
        run_cmd,
        should_log=show_results,
        ignore_failure=True,
        suppress_warning=True,
        timeout=duration + 60)
    if show_results:
      logging.info('Sysbench results: \n stdout is:\n%s\nstderr is\n%s',
                   stdout, stderr)

  return stdout, stderr, retcode


def _IssueMysqlPingCommandWithReturnCode(vm, benchmark_spec):
  """Ping mysql with mysqladmin."""
  db = benchmark_spec.relational_db
  run_cmd_tokens = ['mysqladmin',
                    '--count=1',
                    '--sleep=1',
                    'status',
                    db.MakeMysqlConnectionString()]
  run_cmd = ' '.join(run_cmd_tokens)
  stdout, stderr, retcode = vm.RemoteCommandWithReturnCode(
      run_cmd,
      should_log=False,
      ignore_failure=True,
      suppress_warning=True)

  return stdout, stderr, retcode


def _RunSysbench(
    vm, metadata, benchmark_spec, sysbench_thread_count):
  """Runs the Sysbench OLTP test.

  Args:
    vm: The VM that will issue the sysbench test.
    metadata: The PKB metadata to be passed along to the final results.
    benchmark_spec: The benchmark specification. Contains all data that is
                    required to run the benchmark.
    sysbench_thread_count: The number of client threads that will connect.

  Returns:
    Results: A list of results of this run.
  """
  results = []

  # Now run the sysbench OLTP test and parse the results.
  # First step is to run the test long enough to cover the warmup period
  # as requested by the caller. Second step is the 'real' run where the results
  # are parsed and reported.

  warmup_seconds = FLAGS.sysbench_warmup_seconds
  if warmup_seconds > 0:
    logging.info('Sysbench warm-up run, duration is %d', warmup_seconds)
    _IssueSysbenchCommand(vm, warmup_seconds, benchmark_spec,
                          sysbench_thread_count)

  run_seconds = FLAGS.sysbench_run_seconds
  logging.info('Sysbench real run, duration is %d', run_seconds)
  stdout, _ = _IssueSysbenchCommand(vm, run_seconds, benchmark_spec,
                                    sysbench_thread_count)

  logging.info('\n Parsing Sysbench Results...\n')
  AddMetricsForSysbenchOutput(stdout, results, metadata)

  return results


def _GetDatabaseSize(vm, benchmark_spec):
  """Get the size of the database in MB."""
  db = benchmark_spec.relational_db
  get_db_size_cmd = (
      'mysql %s '
      '-e \''
      'SELECT table_schema AS "Database", '
      'ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS "Size (MB)" '
      'FROM information_schema.TABLES '
      'GROUP BY table_schema; '
      '\'' % db.MakeMysqlConnectionString())

  stdout, _ = vm.RemoteCommand(get_db_size_cmd)
  logging.info('Query database size results: \n%s', stdout)
  # example stdout is tab delimited but shown here with spaces:
  # Database  Size (MB)
  # information_schema  0.16
  # mysql 5.53
  # performance_schema  0.00
  # sbtest  0.33
  size_mb = 0
  for line in stdout.splitlines()[1:]:
    _, word_size_mb = line.split()
    size_mb += float(word_size_mb)

  return size_mb


def _PerformFailoverTest(
    vm, metadata, benchmark_spec, sysbench_thread_count):
  """Runs the failover test - drive the workload while failing over."""
  results = []
  threaded_args = [
      (_FailoverWorkloadThread, [
          vm, benchmark_spec, sysbench_thread_count, results, metadata], {}),
      (_FailOverThread, [benchmark_spec], {})]
  vm_util.RunParallelThreads(threaded_args, len(threaded_args), 0)
  return results


class _Stopwatch(object):
  """Stopwatch class for tracking elapsed time."""

  def __init__(self):
    self.start_time = time.time()

  @property
  def elapsed_seconds(self):
    return time.time() - self.start_time

  def ShouldContinue(self):
    return self.elapsed_seconds < _MAX_FAILOVER_DURATION_SECONDS


def _WaitForWorkloadToFail(
    stopwatch, vm, benchmark_spec, sysbench_thread_count):
  """Run the sysbench workload continuously until it fails."""
  retcode = 0
  did_tps_drop_to_zero = False

  while (retcode == 0 and
         not did_tps_drop_to_zero and
         stopwatch.ShouldContinue()):
    # keep the database busy until the expected time until failover
    # the command will fail once the database connection is lost
    stdout, _, retcode = _IssueSysbenchCommandWithReturnCode(
        vm, _FAILOVER_TEST_TPS_FREQUENCY_SECONDS, benchmark_spec,
        sysbench_thread_count)

    # the tps will drop to 0 before connection failure on AWS
    tps_array, _, _ = _ParseSysbenchOutput(stdout)
    did_tps_drop_to_zero = any({x == 0 for x in tps_array})

  did_all_succeed = retcode == 0 and not did_tps_drop_to_zero
  if did_all_succeed:
    return False

  return True


def _WaitForPingToFail(stopwatch, vm, benchmark_spec):
  """Run a ping workload continuously until it fails."""
  logging.info('\n Pinging until failure...\n')
  retcode = 0
  while retcode == 0 and stopwatch.ShouldContinue():
    _, _, retcode = _IssueMysqlPingCommandWithReturnCode(
        vm, benchmark_spec)

  # exited without reaching failure
  if retcode == 0:
    return False

  return True


def _WaitForPingToSucceed(stopwatch, vm, benchmark_spec):
  """Run a ping workload continuously until it succeeds."""
  logging.info('\n Pinging until success...\n')
  # issue ping while it fails, until it succeeds
  retcode = 1
  while retcode != 0 and stopwatch.ShouldContinue():
    _, _, retcode = _IssueMysqlPingCommandWithReturnCode(
        vm, benchmark_spec)

  # exited without reaching success
  if retcode == 1:
    return False

  return True


def _WaitUntilSysbenchWorkloadConnect(
    stopwatch, vm, benchmark_spec, sysbench_thread_count):
  """Run the sysbench workload until it connects - and then exit."""

  logging.info('\n Keep trying to connect sysbench until success...\n')
  # try issuing sysbench command on 1 second intervals until it succeeds
  retcode = 1
  while retcode != 0 and stopwatch.ShouldContinue():
    _, _, retcode = _IssueSysbenchCommandWithReturnCode(
        vm, 1, benchmark_spec,
        sysbench_thread_count, show_results=False)

  if retcode == 1:
    return False

  return True


def _GatherPostFailoverTPS(
    vm, benchmark_spec, sysbench_thread_count, results, metadata):
  """Gathers metrics on post failover performance."""
  if not FLAGS.sysbench_post_failover_seconds:
    return
  logging.info('\n Gathering Post Failover Data...\n')
  stdout, _ = _IssueSysbenchCommand(
      vm, FLAGS.sysbench_post_failover_seconds, benchmark_spec,
      sysbench_thread_count)
  logging.info('\n Parsing Sysbench Results...\n')
  AddMetricsForSysbenchOutput(stdout, results, metadata, 'failover_')


def _FailoverWorkloadThread(
    vm,
    benchmark_spec,
    sysbench_thread_count,
    results,
    metadata):
  """Entry point for thraed that drives the workload for failover test.

  The failover test requires 2 threads of execution.   The failover thread
  will cause a failover after a certain timeout.

  The workload thread is responsible for driving the database while waiting
  for the failover to occur.   After failover, t then measures how long
  it takes for the database to become operable again.  Finally, the
  database is run under the regular workload again to gather statistics
  on how the database behaves post-failover.

  Args:
    vm:  The vm of the client driving the database
    benchmark_spec:  Benchmark spec including the database spec
    sysbench_thread_count:  How many client threads to drive the database with
                            while waiting for failover
    results:  A list to append any sample to
    metadata:  Metadata to be used in sample construction

  Returns:
    True if the thread made it through the expected workflow without timeouts
    False if database failover could not be detected or it timed out.
  """
  logging.info('\n Running sysbench to drive database while '
               'waiting for failover \n')

  stopwatch = _Stopwatch()
  if not _WaitForWorkloadToFail(
      stopwatch, vm, benchmark_spec, sysbench_thread_count):
    return False

  if not _WaitForPingToFail(stopwatch, vm, benchmark_spec):
    return False

  time_until_failover = stopwatch.elapsed_seconds
  results.append(sample.Sample('time_until_failover',
                               time_until_failover, 's',
                               metadata))

  if not _WaitForPingToSucceed(stopwatch, vm, benchmark_spec):
    return False

  time_failover_to_ping = stopwatch.elapsed_seconds - time_until_failover
  results.append(sample.Sample('time_failover_to_ping',
                               time_failover_to_ping, 's',
                               metadata))

  if not _WaitUntilSysbenchWorkloadConnect(
      stopwatch, vm, benchmark_spec, sysbench_thread_count):
    return False

  time_failover_to_connect = stopwatch.elapsed_seconds - time_until_failover
  results.append(sample.Sample('time_failover_to_connect',
                               time_failover_to_connect, 's',
                               metadata))

  _GatherPostFailoverTPS(
      vm, benchmark_spec, sysbench_thread_count, results, metadata)

  return True


def _FailOverThread(benchmark_spec):
  """Entry point for thread that performs the db failover.

  The failover test requires 2 threads of execution.   The
  workload thread waits for the database to fail and then waits for the database
  to be useful again.   The failover thread is responsible for programmatically
  making the database fail. There is a pause time before failure so that
  we can make sure the database is under sufficient load at the point of
  failure. Doing a failover on an idle database would not be an
  interesting test.

  Args:
      benchmark_spec:   The benchmark spec of the database to failover
  """
  time.sleep(FLAGS.sysbench_pre_failover_seconds)
  db = benchmark_spec.relational_db
  db.Failover()


def _PrepareSysbench(client_vm, benchmark_spec):
  """Prepare the Sysbench OLTP test with data loading stage.

  Args:
    client_vm: The client VM that will issue the sysbench test.
    benchmark_spec: The benchmark specification. Contains all data that is
                    required to run the benchmark.
  Returns:
    results: A list of results of the data loading step.
  """

  _InstallLuaScriptsIfNecessary(client_vm)

  results = []

  db = benchmark_spec.relational_db

  # every cloud provider has a different mechansim for setting root password
  # on initialize.  GCP doesnt support setting password at creation though.
  # So initialize root password here early as possible.
  if FLAGS.cloud == 'GCP' and FLAGS.use_managed_db:
    set_db_root_password_command = 'mysqladmin -h %s -u root password %s' % (
        db.endpoint,
        db.spec.database_password)
    stdout, stderr = client_vm.RemoteCommand(set_db_root_password_command)
    logging.info('Root password is set to %s.', db.spec.database_password)

  # Create the sbtest database for Sysbench.
  create_sbtest_db_cmd = ('mysql %s '
                          '-e \'create database sbtest;\'') % (
                              db.MakeMysqlConnectionString())
  stdout, stderr = client_vm.RemoteCommand(create_sbtest_db_cmd)
  logging.info('sbtest db created, stdout is %s, stderr is %s', stdout, stderr)
  # Provision the Sysbench test based on the input flags (load data into DB)
  # Could take a long time if the data to be loaded is large.
  data_load_start_time = time.time()
  # Data loading is write only so need num_threads less than or equal to the
  # amount of tables - capped at 64 threads for when number of tables
  # gets very large.
  num_threads = min(FLAGS.sysbench_tables, 64)

  data_load_cmd_tokens = ['nice',  # run with a niceness of lower priority
                          '-15',   # to encourage cpu time for ssh commands
                          'sysbench',
                          FLAGS.sysbench_testname,
                          '--tables=%d' % FLAGS.sysbench_tables,
                          ('--table_size=%d' % FLAGS.sysbench_table_size
                           if _IsValidFlag('table_size') else ''),
                          ('--scale=%d' % FLAGS.sysbench_scale
                           if _IsValidFlag('scale') else ''),
                          '--threads=%d' % num_threads,
                          '--db-driver=mysql',
                          db.MakeSysbenchConnectionString(),
                          'prepare']
  data_load_cmd = ' '.join(data_load_cmd_tokens)

  # Sysbench output is in stdout, but we also get stderr just in case
  # something went wrong.
  stdout, stderr = client_vm.RobustRemoteCommand(data_load_cmd)
  load_duration = time.time() - data_load_start_time
  logging.info('It took %d seconds to finish the data loading step',
               load_duration)
  logging.info('data loading results: \n stdout is:\n%s\nstderr is\n%s',
               stdout, stderr)

  db.mysql_db_size_MB = _GetDatabaseSize(client_vm, benchmark_spec)
  metadata = CreateMetadataFromFlags(db)

  results.append(sample.Sample(
      'sysbench data load time',
      load_duration,
      SECONDS_UNIT,
      metadata))

  return results


def _InstallLuaScriptsIfNecessary(vm):
  if FLAGS.sysbench_testname == 'tpcc':
    vm.InstallPreprovisionedBenchmarkData(
        BENCHMARK_NAME, ['sysbench-tpcc.tar.gz'], '~')
    vm.RemoteCommand('tar -zxvf sysbench-tpcc.tar.gz')


def _IsValidFlag(flag):
  return (flag in
          _MAP_WORKLOAD_TO_VALID_UNIQUE_PARAMETERS[FLAGS.sysbench_testname])


def CreateMetadataFromFlags(db):
  """Create meta data with all flags for sysbench."""
  metadata = {
      'sysbench_testname': FLAGS.sysbench_testname,
      'sysbench_tables': FLAGS.sysbench_tables,
      'sysbench_table_size': FLAGS.sysbench_table_size,
      'sysbench_scale': FLAGS.sysbench_scale,
      'sysbench_warmup_seconds': FLAGS.sysbench_warmup_seconds,
      'sysbench_run_seconds': FLAGS.sysbench_run_seconds,
      'sysbench_latency_percentile': FLAGS.sysbench_latency_percentile,
      'sysbench_report_interval': FLAGS.sysbench_report_interval,
      'mysql_db_size_MB': db.mysql_db_size_MB,
      'sysbench_pre_failover_seconds': FLAGS.sysbench_post_failover_seconds,
      'sysbench_post_failover_seconds': FLAGS.sysbench_pre_failover_seconds
  }
  return metadata


def UpdateBenchmarkSpecWithFlags(benchmark_spec):
  """Updates benchmark_spec with flags that are used in the run stage."""
  benchmark_spec.tables = FLAGS.sysbench_tables
  benchmark_spec.sysbench_table_size = FLAGS.sysbench_table_size


def Prepare(benchmark_spec):
  """Prepare the MySQL DB Instances, configures it.

     Prepare the client test VM, installs SysBench, configures it.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  # We would like to always cleanup server side states.
  # If we don't set this, our cleanup function will only be called when the VM
  # is static VM, but we have server side states to cleanup regardless of the
  # VM type.

  benchmark_spec.always_call_cleanup = True

  client_vm = benchmark_spec.vm_groups['clients'][0]

  UpdateBenchmarkSpecWithFlags(benchmark_spec)

  # Setup common test tools required on the client VM
  client_vm.Install('sysbench1')

  prepare_results = _PrepareSysbench(client_vm, benchmark_spec)
  print(prepare_results)


def Run(benchmark_spec):
  """Run the MySQL Service benchmark and publish results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    Results.
  """
  logging.info('Start benchmarking MySQL Service, '
               'Cloud Provider is %s.', FLAGS.cloud)
  client_vm = benchmark_spec.vms[0]
  db = benchmark_spec.relational_db

  for thread_count in FLAGS.sysbench_thread_counts:
    metadata = CreateMetadataFromFlags(db)
    metadata['sysbench_thread_count'] = thread_count
    # The run phase is common across providers. The VMs[0] object contains all
    # information and states necessary to carry out the run.
    run_results = _RunSysbench(client_vm, metadata, benchmark_spec,
                               thread_count)
    print(run_results)
    publisher.PublishRunStageSamples(benchmark_spec, run_results)

  if (FLAGS.use_managed_db and
      benchmark_spec.relational_db.spec.high_availability and
      FLAGS.sysbench_pre_failover_seconds):
    last_client_count = FLAGS.sysbench_thread_counts[
        len(FLAGS.sysbench_thread_counts) - 1]
    failover_results = _PerformFailoverTest(client_vm, metadata, benchmark_spec,
                                            last_client_count)
    print(failover_results)
    publisher.PublishRunStageSamples(benchmark_spec, failover_results)

  # all results have already been published
  # database results take a long time to gather.  If later client counts
  # or failover tests fail, still want the data from the earlier tests.
  # so, results are published as they are found.
  return []


def Cleanup(benchmark_spec):
  """Clean up MySQL Service benchmark related states on server and client.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  del benchmark_spec
