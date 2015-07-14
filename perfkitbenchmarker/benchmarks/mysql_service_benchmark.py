# Copyright 2014 Google Inc. All rights reserved.
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

This is a set of benchmarks that measures performance of MySQL DataBases.

Currently it focuses on managed MySQL services.

"""
import json
import logging
import re
import StringIO
import time

from perfkitbenchmarker import benchmark_spec as benchmark_spec_class
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
flags.DEFINE_enum(
    'db_instance_cores', '8', ['1', '4', '8', '16'],
    'The number of cores to be provisioned for the DB instance.')

flags.DEFINE_integer('oltp_tables_count', 4,
                     'The number of tables used in sysbench oltp.lua tests')
flags.DEFINE_integer('oltp_table_size', 100000,
                     'The number of rows of each table used in the oltp tests')
flags.DEFINE_integer('sysbench_warmup_seconds', 120,
                     'The duration of the warmup run in which results are '
                     'discarded.')
flags.DEFINE_integer('sysbench_run_seconds', 480,
                     'The duration of the actual run in which results are '
                     'collected.')
flags.DEFINE_integer('sysbench_thread_count', 16,
                     'The number of test threads on the client side.')
flags.DEFINE_integer('sysbench_latency_percentile', 99,
                     'The latency percentile we ask sysbench to compute.')
flags.DEFINE_integer('sysbench_report_interval', 2,
                     'The interval we ask sysbench to report results.')


BENCHMARK_INFO = {'name': 'mysql_service',
                  'description': 'MySQL service benchmarks.',
                  'scratch_disk': False,
                  'num_machines': 1}

# Query DB creation status once every 15 seconds
DB_CREATION_STATUS_QUERY_INTERVAL = 15

# How many times we will wait for the service to create the DB
# total wait time is therefore: "query interval * query limit"
DB_CREATION_STATUS_QUERY_LIMIT = 100

# Constants defined for Sysbench tests.
RAND_INIT_ON = 'on'
DISABLE = 'disable'
UNIFORM = 'uniform'
OFF = 'off'
MYSQL_ROOT_USER = 'root'
MYSQL_ROOT_PASSWORD = ''

PREPARE_SCRIPT_PATH = '/usr/share/doc/sysbench/tests/db/parallel_prepare.lua'
OLTP_SCRIPT_PATH = '/usr/share/doc/sysbench/tests/db/oltp.lua'

SYSBENCH_RESULT_NAME_PREPARE = 'prepare time'
LATENCY_UNIT = 'seconds'

# These are the constants that should be specified in GCP's cloud SQL command.
DEFAULT_BACKUP_START_TIME = '07:00'
GCP_MY_SQL_VERSION = 'MYSQL_5_6'
GCP_PRICING_PLAN = 'PACKAGE'


def GetInfo():
  return BENCHMARK_INFO


class DBInstanceCreationError(Exception):
    pass


def _PercentileCalculator(numbers):
  numbers_sorted = sorted(numbers)
  count = len(numbers_sorted)
  total = sum(numbers_sorted)
  result = {}
  result['p1'] = numbers_sorted[int(count * 0.01)]
  result['p5'] = numbers_sorted[int(count * 0.05)]
  result['p50'] = numbers_sorted[int(count * 0.5)]
  result['p90'] = numbers_sorted[int(count * 0.9)]
  result['p99'] = numbers_sorted[int(count * 0.99)]
  result['p99.9'] = numbers_sorted[int(count * 0.999)]
  if count > 0:
    average = total / float(count)
    result['average'] = average
    if count > 1:
      total_of_squares = sum([(i - average) ** 2 for i in numbers])
      result['stddev'] = (total_of_squares / (count - 1)) ** 0.5
    else:
      result['stddev'] = 0

  return result


def _ParseSysbenchOutput(sysbench_output):
  """Parses sysbench output and extract relevant TPS and latency numbers.

  Specifically, we are interested in tps numbers reported by each reporting
  interval, and the summary latency numbers printed at the end of the run in
  "General Statistics" -> "Response Time".
  """
  all_tps = []
  seen_general_statistics = False
  seen_response_time = False

  sysbench_output_io = StringIO.StringIO(sysbench_output)
  for line in sysbench_output_io.readlines():
    if re.match('^\[', line):
      tps = re.findall('tps: (.*?),', line)
      all_tps.append(float(tps[0]))
      continue

    if line.startswith('General statistics:'):
      seen_general_statistics = True
      continue

    if seen_general_statistics:
      if re.match('^ +response time:.*', line):
        seen_response_time = True
        continue

    if seen_general_statistics and seen_response_time:
      if re.findall('min: +(.*)', line):
        min_response_time = re.findall('min: +(.*)', line)[0]
      if re.findall('avg: +(.*)', line):
        avg_response_time = re.findall('avg: +(.*)', line)[0]
      if re.findall('max: +(.*)', line):
        max_response_time = re.findall('max: +(.*)', line)[0]
      if re.findall('.* percentile: +(.*)', line):
        percentile_response_time = re.findall('.* percentile: +(.*)', line)[0]

  tps_line = ', '.join(map(str, all_tps))
  logging.info('All TPS numbers: \n %s', tps_line)
  tps_percentile = _PercentileCalculator(all_tps)
  logging.info('p1 tps %f', tps_percentile['p1'])
  logging.info('p5 tps %f', tps_percentile['p5'])
  logging.info('p50 tps %f', tps_percentile['p50'])
  logging.info('p90 tps %f', tps_percentile['p90'])
  logging.info('p99 tps %f', tps_percentile['p99'])
  logging.info('p99.9 tps %f', tps_percentile['p99.9'])
  logging.info('tps average %f', tps_percentile['average'])
  logging.info('tps stddev %f', tps_percentile['stddev'])

  logging.info('response time min %s', min_response_time)
  logging.info('response time max %s', max_response_time)
  logging.info('response time average %s', avg_response_time)
  logging.info('%d percentile response time %s',
               FLAGS.sysbench_latency_percentile, percentile_response_time)


class RDSMySQLBenchmark(object):
  """MySQL benchmark based on the RDS service on AWS."""

  def Prepare(self, vm):
    logging.info('Preparing MySQL Service benchmarks for RDS.')

  def Run(self, vm, metadata):
    results = []
    return results

  def Cleanup(self, vm):
    """Clean up RDS instances.
    """
    pass


class GoogleCloudSQLBenchmark(object):
  """MySQL benchmark based on the Google Cloud SQL service."""

  def Prepare(self, vm):
    logging.info('Preparing MySQL Service benchmarks for Google Cloud SQL.')

    vm.db_instance_name = 'pkb%s' % FLAGS.run_uri
    db_tier = 'db-n1-standard-%s' % FLAGS.db_instance_cores
    # Currently, we create DB instance in the same zone as the test VM.
    db_instance_zone = vm.zone
    # Currently GCP REQUIRES you to connect to the DB instance via external IP
    # (i.e., using external IPs of the DB instance AND the VM instance).
    authorized_network = '%s/32' % vm.ip_address
    create_db_cmd = [FLAGS.gcloud_path,
                     'sql',
                     'instances',
                     'create', vm.db_instance_name,
                     '--quiet',
                     '--format=json',
                     '--async',
                     '--activation-policy=ALWAYS',
                     '--assign-ip',
                     '--authorized-networks=%s' % authorized_network,
                     '--backup-start-time=%s' % DEFAULT_BACKUP_START_TIME,
                     '--enable-bin-log',
                     '--tier=%s' % db_tier,
                     '--gce-zone=%s' % db_instance_zone,
                     '--database-version=%s' % GCP_MY_SQL_VERSION,
                     '--pricing-plan=%s' % GCP_PRICING_PLAN]

    stdout, _, _ = vm_util.IssueCommand(create_db_cmd)
    response = json.loads(stdout)
    if response['operation'] is None or response['operationType'] != 'CREATE':
      raise DBInstanceCreationError('Invalid operation or unrecognized '
                                    'operationType in DB creation response. '
                                    ' stdout is %s' % stdout)

    status_query_cmd = [FLAGS.gcloud_path,
                        'sql',
                        'instances',
                        'describe', vm.db_instance_name,
                        '--format', 'json']

    stdout, _, _ = vm_util.IssueCommand(status_query_cmd)
    response = json.loads(stdout)

    query_count = 1
    while True:
      state = response['state']
      if state is None:
        raise ValueError('Cannot parse response from status query command. '
                         'The state is missing. stdout is %s' % stdout)

      if state == 'RUNNABLE':
        break
      else:
        if query_count > DB_CREATION_STATUS_QUERY_LIMIT:
          raise DBInstanceCreationError('DB creation timed-out, we have '
                                        'waited at least %s * %s seconds.' % (
                                            DB_CREATION_STATUS_QUERY_INTERVAL,
                                            DB_CREATION_STATUS_QUERY_LIMIT))

        logging.info('Querying db creation status, current state is %s, query '
                     'count is %d', state, query_count)
        time.sleep(DB_CREATION_STATUS_QUERY_INTERVAL)

        stdout, _, _ = vm_util.IssueCommand(status_query_cmd)
        response = json.loads(stdout)
        query_count += 1

    logging.info('Successfully created the DB instance. Complete response is '
                 '%s', response)

    vm.db_instance_ip = response['ipAddresses'][0]['ipAddress']
    logging.info('DB IP address is: %s', vm.db_instance_ip)


  def Run(self, vm, metadata):
    results = []

    # Prepares the Sysbench test based on the input flags (load data into DB)
    # Could take a long time if the data to be loaded is large.
    # sysbench --test=/usr/share/doc/sysbench/tests/db/parallel_prepare.lua
    # --oltp_tables_count=3 --oltp-table-size=100000
    # --rand-init=on --num-threads=3 --mysql-user=root
    # --mysql-host=146.148.35.217 --mysql-password="" run

    prepare_start_time = time.time()
    prepare_cmd_tokens = ['sysbench',
                          '--test=%s' % PREPARE_SCRIPT_PATH,
                          '--oltp_tables_count=%d' % FLAGS.oltp_tables_count,
                          '--oltp-table-size=%d' % FLAGS.oltp_table_size,
                          '--rand-init=%s' % RAND_INIT_ON,
                          '--num-threads=%d' % FLAGS.oltp_tables_count,
                          '--mysql-user=%s' % MYSQL_ROOT_USER,
                          '--mysql-password="%s"' % MYSQL_ROOT_PASSWORD,
                          '--mysql-host=%s' % vm.db_instance_ip,
                          'run']
    prepare_cmd = ' '.join(prepare_cmd_tokens)
    # Sysbench output is in stdout, but we also get stderr just in case
    # something went wrong.
    stdout, stderr = vm.RobustRemoteCommand(prepare_cmd)
    prepare_duration = time.time() - prepare_start_time
    logging.info('It took %d seconds to finish the prepare step',
                 prepare_duration)
    logging.info('Prepare results: \n stdout is:\n%s\nstderr is\n%s',
                 stdout,
                 stderr)

    results.append(sample.Sample(
        SYSBENCH_RESULT_NAME_PREPARE,
        prepare_duration,
        LATENCY_UNIT,
        metadata))

    # Now run the sysbench OLTP test and parse the results.

    # First step is to run the test long enough to cover the warmup period
    # as requested by the caller.
    if FLAGS.sysbench_warmup_seconds > 0:
      run_cmd_tokens = ['sysbench',
                        '--test=%s' % OLTP_SCRIPT_PATH,
                        '--oltp_tables_count=%d' % FLAGS.oltp_tables_count,
                        '--oltp-table-size=%d' % FLAGS.oltp_table_size,
                        '--rand-init=%s' % RAND_INIT_ON,
                        '--db-ps-mode=%s' % DISABLE,
                        '--oltp-dist-type=%s' % UNIFORM,
                        '--oltp-read-only=%s' % OFF,
                        '--num-threads=%d' % FLAGS.sysbench_thread_count,
                        '--percentile=%d' % FLAGS.sysbench_latency_percentile,
                        '--report-interval=%d' % FLAGS.sysbench_report_interval,
                        '--max-time=%d' % FLAGS.sysbench_warmup_seconds,
                        '--mysql-user=%s' % MYSQL_ROOT_USER,
                        '--mysql-password="%s"' % MYSQL_ROOT_PASSWORD,
                        '--mysql-host=%s' % vm.db_instance_ip,
                        'run']
      run_cmd = ' '.join(run_cmd_tokens)
      stdout, stderr = vm.RobustRemoteCommand(run_cmd)
      logging.info('Warmup results: \n stdout is:\n%s\nstderr is\n%s',
                   stdout, stderr)
      logging.info('\n Parsing Sysbench Results...\n')
      _ParseSysbenchOutput(stdout)

    # Then we launch the actual test.

    return results

  def Cleanup(self, vm):
    if hasattr(vm, 'db_instance_name'):
      delete_db_cmd = [FLAGS.gcloud_path,
                       'sql',
                       'instances',
                       'delete', vm.db_instance_name,
                       '--quiet']

      stdout, stderr, status = vm_util.IssueCommand(delete_db_cmd)
      logging.info('DB cleanup command issued, stdout is %s, stderr is %s '
                   'status is %s', stdout, stderr, status)
    else:
      logging.info('db_instance_name does not exist, no need to cleanup.')


MYSQL_SERVICE_BENCHMARK_DICTIONARY = {
    benchmark_spec_class.GCP: GoogleCloudSQLBenchmark(),
    benchmark_spec_class.AWS: RDSMySQLBenchmark()}


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

  vms = benchmark_spec.vms

  # Setup common test tools required on the client VM
  vms[0].Install('sysbench05plus')

  # Prepare service specific states (create DB instance, configure it, etc)
  MYSQL_SERVICE_BENCHMARK_DICTIONARY[FLAGS.cloud].Prepare(vms[0])

  # Create the sbtest database to prepare the DB for Sysbench.
  # mysql -u root -h IP -e 'create database sbtest;'
  create_sbtest_db_cmd = ('mysql -u root -h %s '
                          '-e \'create database sbtest;\'') % (
                              vms[0].db_instance_ip)
  stdout, stderr = vms[0].RemoteCommand(create_sbtest_db_cmd)
  logging.info('sbtest db created, stdout is %s, stderr is %s', stdout, stderr)


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
  vms = benchmark_spec.vms
  metadata = {}
  metadata['oltp_tables_count'] = FLAGS.oltp_tables_count
  metadata['oltp_table_size'] = FLAGS.oltp_table_size
  metadata['db_instance_cores'] = FLAGS.db_instance_cores

  results = MYSQL_SERVICE_BENCHMARK_DICTIONARY[FLAGS.storage].Run(vms[0],
                                                                  metadata)
  print results
  return results


def Cleanup(benchmark_spec):
  """Clean up MySQL Service benchmark related states on server and client.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  MYSQL_SERVICE_BENCHMARK_DICTIONARY[FLAGS.storage].Cleanup(vms[0])
