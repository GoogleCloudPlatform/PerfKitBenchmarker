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
from perfkitbenchmarker.aws import aws_network

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

AWS_PREFIX = ['aws', '--output=json']
# Map from FLAGs.db_instance_cores to RDS DB Type
RDS_CORE_TO_DB_CLASS_MAP = {
    '1': 'db.m3.medium',
    '4': 'db.m3.xlarge',
    '8': 'db.m3.2xlarge',
    '16': 'db.r3.4xlarge',  # m3 series doesn't have 16 core.
}

RDS_DB_ENGINE = 'MySQL'
RDS_DB_ENGINE_VERSION = '5.6.23'
RDS_DB_STORAGE_TYPE_GP2 = 'gp2'
# Storage IOPS capacity of the DB instance.
# Currently this is fixed because the cloud provider GCP does not support
# changing this setting. As soon as it supports changing the storage size, we
# will expose a flag here to allow caller to select a storage size.
# Default GCP storage size is 1TB PD-SSD which supports 10K Read or 15K Write
# IOPS (12.5K mixed).
# To support 12.5K IOPS on EBS-GP, we need 4170 GB disk.
RDS_DB_STORAGE_GP2_SIZE = 4170

# Constants defined for Sysbench tests.
RAND_INIT_ON = 'on'
DISABLE = 'disable'
UNIFORM = 'uniform'
OFF = 'off'
MYSQL_ROOT_USER = 'root'
MYSQL_ROOT_PASSWORD = 'Perfkit8'

PREPARE_SCRIPT_PATH = '/usr/share/doc/sysbench/tests/db/parallel_prepare.lua'
OLTP_SCRIPT_PATH = '/usr/share/doc/sysbench/tests/db/oltp.lua'

SYSBENCH_RESULT_NAME_PREPARE = 'sysbench prepare time'
SYSBENCH_RESULT_NAME_TPS = 'sysbench tps'
SYSBENCH_RESULT_NAME_LATENCY = 'sysbench latency'
NA_UNIT = 'NA'
SECONDS_UNIT = 'seconds'
MS_UNIT = 'milliseconds'

# These are the constants that should be specified in GCP's cloud SQL command.
DEFAULT_BACKUP_START_TIME = '07:00'
GCP_MY_SQL_VERSION = 'MYSQL_5_6'
GCP_PRICING_PLAN = 'PACKAGE'

PERCENTILES_LIST = [1, 5, 50, 90, 99, 99.9]
RESPONSE_TIME_TOKENS = ['min', 'avg', 'max', 'percentile']


def GetInfo():
  return BENCHMARK_INFO


class DBInstanceCreationError(Exception):
    pass


def _PercentileCalculator(numbers):
  numbers_sorted = sorted(numbers)
  count = len(numbers_sorted)
  total = sum(numbers_sorted)
  result = {}
  for percentile in PERCENTILES_LIST:
    percentile_string = 'p%s' % str(percentile)
    result[percentile_string] = numbers_sorted[
        int(count * float(percentile) / 100)]

  if count > 0:
    average = total / float(count)
    result['average'] = average
    if count > 1:
      total_of_squares = sum([(i - average) ** 2 for i in numbers])
      result['stddev'] = (total_of_squares / (count - 1)) ** 0.5
    else:
      result['stddev'] = 0

  return result


def _ParseSysbenchOutput(sysbench_output, results, metadata):
  """Parses sysbench output and extract relevant TPS and latency numbers, and
  populate the final result collection with these information.

  Specifically, we are interested in tps numbers reported by each reporting
  interval, and the summary latency numbers printed at the end of the run in
  "General Statistics" -> "Response Time".
  """
  all_tps = []
  seen_general_statistics = False
  seen_response_time = False

  response_times = {}

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
      for token in RESPONSE_TIME_TOKENS:
        search_string = '.*%s: +(.*)ms' % token
        if re.findall(search_string, line):
          response_times[token] = float(re.findall(search_string, line)[0])

  tps_line = ', '.join(map(str, all_tps))
  # Print all tps data points in the log for reference. And report
  # percentiles of these tps data in the final result set.
  logging.info('All TPS numbers: \n %s', tps_line)

  tps_percentile = _PercentileCalculator(all_tps)
  for percentile in PERCENTILES_LIST:
    percentile_string = 'p%s' % str(percentile)
    logging.info('%s tps %f', percentile_string,
                 tps_percentile[percentile_string])
    metric_name = ('%s %s') % (SYSBENCH_RESULT_NAME_TPS, percentile_string)
    results.append(sample.Sample(
        metric_name,
        tps_percentile[percentile_string],
        NA_UNIT,
        metadata))

  # Also report average, stddev, and coefficient of variation
  for token in ['average', 'stddev']:
    logging.info('tps %s %f', token, tps_percentile[token])
    metric_name = ('%s %s') % (SYSBENCH_RESULT_NAME_TPS, token)
    results.append(sample.Sample(
        metric_name,
        tps_percentile[token],
        NA_UNIT,
        metadata))

  if tps_percentile['average'] > 0:
    cv = tps_percentile['stddev'] / tps_percentile['average']
    logging.info('tps coefficient of variation %f', cv)
    metric_name = ('%s %s') % (SYSBENCH_RESULT_NAME_TPS, 'cv')
    results.append(sample.Sample(
        metric_name,
        cv,
        NA_UNIT,
        metadata))

  # Now, report the latency numbers.
  for token in RESPONSE_TIME_TOKENS:
    logging.info('%s_response_time is %f', token, response_times[token])
    metric_name = '%s %s' % (SYSBENCH_RESULT_NAME_LATENCY, token)

    if token == 'percentile':
      metric_name = '%s %s' % (metric_name, FLAGS.sysbench_latency_percentile)

    results.append(sample.Sample(
        metric_name,
        response_times[token],
        MS_UNIT,
        metadata))


class RDSMySQLBenchmark(object):
  """MySQL benchmark based on the RDS service on AWS."""

  def Prepare(self, vm):
    logging.info('Preparing MySQL Service benchmarks for RDS.')
    logging.info('vm.zone is %s', vm.zone)
    logging.info('vm.network.vpc id is %s', vm.network.vpc.id)
    logging.info('vm.network.subnet id is %s', vm.network.subnet.id)
    logging.info('vm.network.subnet.vpc_id is %s', vm.network.subnet.vpc_id)
    logging.info('vm security group id is %s', vm.group_id)

    # First is to create a second subnet in the same VPC as the VM but in a
    # different zone. RDS requires two subnets in two different zones to create
    # a DB instance, EVEN IF you do not specify multi-AZ in your DB creation
    # request.

    # Get a list of zones and pick one that's different from the zone VM is in.
    new_subnet_zone = ''
    get_zones_cmd = AWS_PREFIX + ['ec2', 'describe-availability-zones']
    stdout, _, _ = vm_util.IssueCommand(get_zones_cmd)
    response = json.loads(stdout)
    all_zones = response['AvailabilityZones']
    for zone in all_zones:
      if zone['ZoneName'] != vm.zone:
        new_subnet_zone = zone['ZoneName']
        break

    if new_subnet_zone == '':
      raise DBInstanceCreationError('Cannot find a zone to create the required '
                                    'second subnet for the DB instance.')

    # Now create a new subnet in the zone that's different from where the VM is
    logging.info('Creating a second subnet in zone %s', new_subnet_zone)
    new_subnet = aws_network.AwsSubnet(new_subnet_zone, vm.network.vpc.id,
                                       '10.0.1.0/24')
    new_subnet.Create()
    logging.info('Successfully created a new subnet, subnet id is: %s',
                 new_subnet.id)
    # Remember this so we can cleanup properly.
    vm.extra_subnet_for_db = new_subnet

    # Now we can create a new DB subnet group that has two subnets in it.
    db_subnet_group_name = 'pkb%s' % FLAGS.run_uri
    create_db_subnet_group_cmd = AWS_PREFIX + [
        'rds',
        'create-db-subnet-group',
        '--db-subnet-group-name', db_subnet_group_name,
        '--db-subnet-group-description', 'pkb_subnet_group_for_db',
        '--subnet-ids', vm.network.subnet.id, new_subnet.id]
    stdout, stderr, _ = vm_util.IssueCommand(create_db_subnet_group_cmd)
    logging.info('Created a DB subnet group, stdout is:\n%s\nstderr is:\n%s',
                 stdout, stderr)
    vm.db_subnet_group_name = db_subnet_group_name

    # open up tcp port 3306 in the VPC's security group, we need that to connect
    # to the DB.
    open_port_cmd = AWS_PREFIX + [
        'ec2',
        'authorize-security-group-ingress',
        '--group-id', vm.group_id,
        '--source-group', vm.group_id,
        '--protocol', 'tcp',
        '--port', '3306']
    stdout, stderr, _ = vm_util.IssueCommand(open_port_cmd)
    logging.info('Granted tcp 3306 ingress, stdout is:\n%s\nstderr is:\n%s',
                 stdout, stderr)

    # Finally, it's time to create the DB instance!
    vm.db_instance_id = 'pkb-DB-%s' % FLAGS.run_uri
    db_class = RDS_CORE_TO_DB_CLASS_MAP['%s' % FLAGS.db_instance_cores]

    create_db_cmd = AWS_PREFIX + [
        'rds',
        'create-db-instance',
        '--db-instance-identifier', vm.db_instance_id,
        '--db-instance-class', db_class,
        '--engine', RDS_DB_ENGINE,
        '--engine-version', RDS_DB_ENGINE_VERSION,
        '--storage-type', RDS_DB_STORAGE_TYPE_GP2,
        '--allocated-storage', RDS_DB_STORAGE_GP2_SIZE,
        '--vpc-security-group-ids', vm.group_id,
        '--master-username', MYSQL_ROOT_USER,
        '--master-user-password', MYSQL_ROOT_PASSWORD,
        '--availability-zone', vm.zone,
        '--db-subnet-group-name', vm.db_subnet_group_name]

    logging.info('Will call this command to create db \n %s',
                 ' '.join(create_db_cmd))

    """stdout, stderr, _ = vm_util.IssueCommand(create_db_cmd)
    response = json.loads(stdout)

    db_creation_status = ''
    if response['DBInstances'] is not None:
      db_creation_status = response['DBInstances'][0]['DBInstanceStatus']

    if (db_creation_status != 'creating' and
        db_creation_status != 'available' and
        db_creation_status != 'modifying'):
      raise DBInstanceCreationError('Invalid operation or unrecognized '
                                      'operationType in DB creation response. '
                                      ' stdout is\n%s, stderr is\n%s' % (
                                          stdout, stderr))
    """

  def Run(self, vm, metadata):
    results = []
    return results


  def Cleanup(self, vm):
    """Clean up RDS instances, cleanup the extra subnet created for the
       creation of the RDS instance.
    """
    if hasattr(vm, 'db_subnet_group_name'):
      delete_db_subnet_group_cmd = AWS_PREFIX + [
          'rds',
          'delete-db-subnet-group',
          '--db-subnet-group-name', vm.db_subnet_group_name]
      stdout, stderr, _ = vm_util.IssueCommand(delete_db_subnet_group_cmd)
      logging.info('Deleted the db subnet group. stdout is:\n%s, stderr: \n%s',
                   stdout, stderr)

    if hasattr(vm, 'extra_subnet_for_db'):
      delete_extra_subnet_cmd = AWS_PREFIX + [
          'ec2',
          'delete-subnet',
          '--subnet-id=%s' % vm.extra_subnet_for_db.id]
      stdout, stderr, _ = vm_util.IssueCommand(delete_extra_subnet_cmd)
      logging.info('Deleted the extra subnet. stdout is:\n%s, stderr: \n%s',
                   stdout, stderr)

    # Now, you can delete the DB instance. vm.db_instance_id is the id to call.
    # you need to keep querying the status of the deletion here before you let
    # this go. RDS DB deletion takes some time to finish. And you have to
    # wait until this DB is deleted before you proceed becuase this DB holds
    # references to the VPC, the subnets, etc.
    # wrap below in a loop:
    # try:
    # response = json.loads(stdout).
    # db_status=response['DBInstancesStatus']
    # if db_status is None:
    # break
    # else: wait for a few seconds, add wait count, check if timed out.
    # except:
    #  break //stdout cannot be parsed into json. great.
    #
    # Outside the loop, look at stderr, expect it to have DB instance Not found
    # if it does not have, log a warning but proceed forward.


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

    # Set the root password to a common one that can be referred to in common
    # code across providers.
    set_password_cmd = [FLAGS.gcloud_path,
                        'sql',
                        'instances',
                        'set-root-password',
                        vm.db_instance_name,
                        '--password', MYSQL_ROOT_PASSWORD]
    stdout, stderr, _ = vm_util.IssueCommand(set_password_cmd)
    logging.info('Set root password completed. Stdout:\n%s\nStderr:\n%s',
                 stdout, stderr)


  def Run(self, vm, metadata):
    results = []

    # Prepares the Sysbench test based on the input flags (load data into DB)
    # Could take a long time if the data to be loaded is large.
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
        SECONDS_UNIT,
        metadata))

    # Now run the sysbench OLTP test and parse the results.

    for i in xrange(1, 3):
      # First step is to run the test long enough to cover the warmup period
      # as requested by the caller. Then we do the "real" run, parse and report
      # the results.
      duration = 0
      if i == 1 and FLAGS.sysbench_warmup_seconds > 0:
        duration = FLAGS.sysbench_warmup_seconds
        logging.info('Sysbench warm-up run, duration is %d', duration)
      elif i == 2:
        duration = FLAGS.sysbench_run_seconds
        logging.info('Sysbench real run, duration is %d', duration)

      if duration > 0:
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
                          '--report-interval=%d' %
                          FLAGS.sysbench_report_interval,
                          '--max-time=%d' % duration,
                          '--mysql-user=%s' % MYSQL_ROOT_USER,
                          '--mysql-password="%s"' % MYSQL_ROOT_PASSWORD,
                          '--mysql-host=%s' % vm.db_instance_ip,
                          'run']
        run_cmd = ' '.join(run_cmd_tokens)
        stdout, stderr = vm.RobustRemoteCommand(run_cmd)
        logging.info('Sysbench results: \n stdout is:\n%s\nstderr is\n%s',
                     stdout, stderr)

      if i == 2:
        # We only need to parse the results for the "real" run.
        logging.info('\n Parsing Sysbench Results...\n')
        _ParseSysbenchOutput(stdout, results, metadata)

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

  if hasattr(vms[0], 'db_instance_ip'):
      # Create the sbtest database to prepare the DB for Sysbench.
    create_sbtest_db_cmd = ('mysql -h %s -u %s -p%s '
                            '-e \'create database sbtest;\'') % (
                                vms[0].db_instance_ip,
                                MYSQL_ROOT_USER,
                                MYSQL_ROOT_PASSWORD)
    stdout, stderr = vms[0].RemoteCommand(create_sbtest_db_cmd)
    logging.info('sbtest db created, stdout is %s, stderr is %s',
                 stdout, stderr)
  else:
    logging.error('Prepare has likely failed, db_instance_ip is not found.')


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
  metadata['sysbench_warm_up_seconds'] = FLAGS.sysbench_warmup_seconds
  metadata['sysbench_run_seconds'] = FLAGS.sysbench_run_seconds
  metadata['sysbench_thread_count'] = FLAGS.sysbench_thread_count
  metadata['sysbench_latency_percentile'] = FLAGS.sysbench_latency_percentile
  metadata['sysbench_report_interval'] = FLAGS.sysbench_report_interval

  results = MYSQL_SERVICE_BENCHMARK_DICTIONARY[FLAGS.cloud].Run(vms[0],
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
  MYSQL_SERVICE_BENCHMARK_DICTIONARY[FLAGS.cloud].Cleanup(vms[0])
