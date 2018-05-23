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

import logging
import re
import StringIO
import time

from perfkitbenchmarker import configs
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample


FLAGS = flags.FLAGS

flags.DEFINE_string('sysbench_testname', 'oltp_read_write',
                    'The built in oltp lua script to run')
flags.DEFINE_integer('sysbench_tables', 4,
                     'The number of tables used in sysbench oltp.lua tests')
flags.DEFINE_integer('sysbench_table_size', 100000,
                     'The number of rows of each table used in the oltp tests')
flags.DEFINE_integer('sysbench_warmup_seconds', 120,
                     'The duration of the warmup run in which results are '
                     'discarded, in seconds.')
flags.DEFINE_integer('sysbench_run_seconds', 480,
                     'The duration of the actual run in which results are '
                     'collected, in seconds.')
flag_util.DEFINE_integerlist(
    'sysbench_thread_counts',
    flag_util.IntegerList([1, 2, 4, 8, 16, 32, 64]),
    'array of thread counts passed to sysbench, one at a time')
flags.DEFINE_integer('sysbench_latency_percentile', 100,
                     'The latency percentile we ask sysbench to compute.')
flags.DEFINE_integer('sysbench_report_interval', 2,
                     'The interval, in seconds, we ask sysbench to report '
                     'results.')

BENCHMARK_NAME = 'sysbench'
BENCHMARK_CONFIG = """
sysbench:
  description: Sysbench OLTP benchmarks.
  managed_relational_db:
    engine: mysql
    vm_spec:
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
    disk_spec:
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
    default:
      os_type: ubuntu1604
      vm_spec:
        AWS:
          machine_type: m4.4xlarge
          zone: us-west-1a
        Azure:
          machine_type: Standard_A8m_v2
          zone: westus
        GCP:
          machine_type: n1-standard-16
          zone: us-central1-c
"""

# Constants defined for Sysbench tests.
DISABLE = 'disable'
UNIFORM = 'uniform'

SECONDS_UNIT = 'seconds'


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def ParseSysbenchOutput(sysbench_output, results, metadata):
  """Parses sysbench output.

  Extract relevant TPS and latency numbers, and populate the final result
  collection with these information.

  Specifically, we are interested in tps and latency numbers reported by each
  reporting interval.

  Args:
    sysbench_output: The output from sysbench.
    results: The dictionary to store results based on sysbench output.
    metadata: The metadata to be passed along to the Samples class.
  """
  tps_numbers = []
  latency_numbers = []

  sysbench_output_io = StringIO.StringIO(sysbench_output)
  for line in sysbench_output_io:
    # parse a line like (it's one line - broken up in the comment to fit):
    # [ 6s ] thds: 16 tps: 650.51 qps: 12938.26 (r/w/o: 9046.18/2592.05/1300.03)
    # lat (ms,99%): 40.37 err/s: 0.00 reconn/s: 0.00
    if re.match(r'^\[', line):
      match = re.search('tps: (.*?) ', line)
      tps_numbers.append(float(match.group(1)))
      match = re.search(r'lat \(.*?\): (.*?) ', line)
      latency_numbers.append(float(match.group(1)))
      if line.startswith('SQL statistics:'):
        break

  tps_metadata = metadata.copy()
  tps_metadata.update({'tps': tps_numbers})
  tps_sample = sample.Sample('tps_array', -1, 'tps', tps_metadata)

  latency_metadata = metadata.copy()
  latency_metadata.update({'latency': latency_numbers})
  latency_sample = sample.Sample('latency_array', -1, 'ms',
                                 latency_metadata)

  results.append(tps_sample)
  results.append(latency_sample)


def _IssueSysbenchCommand(vm, duration, benchmark_spec, sysbench_thread_count):
  """Issues a sysbench run command given a vm and a duration.

      Does nothing if duration is <= 0

  Args:
    vm: The test VM to issue command to.
    duration: the duration of the sysbench run.
    benchmark_spec: The benchmark specification. Contains all data that is
                    required to run the benchmark.
    sysbench_thread_count: count of number of threads to use in --threads
                           parameter to sysbench

  Returns:
    stdout, stderr: the result of the command.
  """
  stdout = ''
  stderr = ''
  if duration > 0:
    db = benchmark_spec.managed_relational_db
    run_cmd_tokens = ['sysbench',
                      FLAGS.sysbench_testname,
                      '--tables=%d' % FLAGS.sysbench_tables,
                      '--table_size=%d' % FLAGS.sysbench_table_size,
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
    stdout, stderr = vm.RobustRemoteCommand(run_cmd)
    logging.info('Sysbench results: \n stdout is:\n%s\nstderr is\n%s',
                 stdout, stderr)

  return stdout, stderr


def _RunSysbench(vm, metadata, benchmark_spec, sysbench_thread_count):
  """Runs the Sysbench OLTP test.

  Args:
    vm: The client VM that will issue the sysbench test.
    metadata: The PKB metadata to be passed along to the final results.
    benchmark_spec: The benchmark specification. Contains all data that is
                    required to run the benchmark.

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
  ParseSysbenchOutput(stdout, results, metadata)

  return results


def _PrepareSysbench(vm, metadata, benchmark_spec):
  """Prepare the Sysbench OLTP test with data loading stage.

  Args:
    vm: The client VM that will issue the sysbench test.
    metadata: The PKB metadata to be passed along to the final results.
    benchmark_spec: The benchmark specification. Contains all data that is
                    required to run the benchmark.
  Returns:
    results: A list of results of the data loading step.
  """
  results = []

  db = benchmark_spec.managed_relational_db

  # every cloud provider has a different mechansim for setting root password
  # on initialize.  GCP doesnt support setting password at creation though.
  # So initialize root password here early as possible.
  if FLAGS.cloud == 'GCP':
    set_db_root_password_command = 'mysqladmin -h %s -u root password %s' % (
        db.endpoint,
        db.spec.database_password)
    stdout, stderr = vm.RemoteCommand(set_db_root_password_command)
    logging.info('Root password is set to %s.', db.spec.database_password)

  # Create the sbtest database for Sysbench.
  create_sbtest_db_cmd = ('mysql %s '
                          '-e \'create database sbtest;\'') % (
                              db.MakeMysqlConnectionString())
  stdout, stderr = vm.RemoteCommand(create_sbtest_db_cmd)
  logging.info('sbtest db created, stdout is %s, stderr is %s',
               stdout, stderr)
  # Provision the Sysbench test based on the input flags (load data into DB)
  # Could take a long time if the data to be loaded is large.
  data_load_start_time = time.time()
  # Data loading is write only so need num_threads less than or equal to the
  # amount of tables.
  num_threads = FLAGS.sysbench_tables

  data_load_cmd_tokens = ['sysbench',
                          FLAGS.sysbench_testname,
                          '--tables=%d' % FLAGS.sysbench_tables,
                          '--table-size=%d' % FLAGS.sysbench_table_size,
                          '--threads=%d' % num_threads,
                          '--db-driver=mysql',
                          db.MakeSysbenchConnectionString(),
                          'prepare']
  data_load_cmd = ' '.join(data_load_cmd_tokens)

  # Sysbench output is in stdout, but we also get stderr just in case
  # something went wrong.
  stdout, stderr = vm.RobustRemoteCommand(data_load_cmd)
  load_duration = time.time() - data_load_start_time
  logging.info('It took %d seconds to finish the data loading step',
               load_duration)
  logging.info('data loading results: \n stdout is:\n%s\nstderr is\n%s',
               stdout, stderr)

  results.append(sample.Sample(
      'sysbench data load time',
      load_duration,
      SECONDS_UNIT,
      metadata))

  return results


def CreateMetadataFromFlags():
  """Create meta data with all flags for sysbench."""
  metadata = {
      'sysbench_testname': FLAGS.sysbench_testname,
      'sysbench_tables': FLAGS.sysbench_tables,
      'sysbench_table_size': FLAGS.sysbench_table_size,
      'sysbench_warmup_seconds': FLAGS.sysbench_warmup_seconds,
      'sysbench_run_seconds': FLAGS.sysbench_run_seconds,
      'sysbench_latency_percentile': FLAGS.sysbench_latency_percentile,
      'sysbench_report_interval': FLAGS.sysbench_report_interval
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

  vm = benchmark_spec.vms[0]

  UpdateBenchmarkSpecWithFlags(benchmark_spec)
  metadata = CreateMetadataFromFlags()

  # Setup common test tools required on the client VM
  vm.Install('sysbench1')
  vm.Install('mysql')

  prepare_results = _PrepareSysbench(vm, metadata, benchmark_spec)
  print prepare_results


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
  vm = benchmark_spec.vms[0]

  results = []
  for thread_count in FLAGS.sysbench_thread_counts:
    metadata = CreateMetadataFromFlags()
    metadata['sysbench_thread_count'] = thread_count
    # The run phase is common across providers. The VMs[0] object contains all
    # information and states necessary to carry out the run.
    run_results = _RunSysbench(vm, metadata, benchmark_spec, thread_count)
    print run_results
    results += run_results

  return results


def Cleanup(benchmark_spec):
  """Clean up MySQL Service benchmark related states on server and client.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  del benchmark_spec
