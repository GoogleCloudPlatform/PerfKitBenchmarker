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
- On GCP, we will use Cloud SQL v2 (Performance Edition). As of July 2015, you
 will need to request to whitelist your GCP project to get access to Cloud SQL
 v2. Follow instructions on your GCP's project console to do that.

As other cloud providers deliver a managed MySQL service, we will add it here.

"""
import json
import logging
import re
import StringIO
import time
import uuid

from perfkitbenchmarker import providers
from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import os_types
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.aws import aws_network
from perfkitbenchmarker.providers.aws import util

FLAGS = flags.FLAGS
flags.DEFINE_enum(
    'mysql_svc_db_instance_cores', '4', ['1', '4', '8', '16'],
    'The number of cores to be provisioned for the DB instance.')

flags.DEFINE_integer('mysql_svc_oltp_tables_count', 4,
                     'The number of tables used in sysbench oltp.lua tests')
flags.DEFINE_integer('mysql_svc_oltp_table_size', 100000,
                     'The number of rows of each table used in the oltp tests')
flags.DEFINE_integer('sysbench_warmup_seconds', 120,
                     'The duration of the warmup run in which results are '
                     'discarded, in seconds.')
flags.DEFINE_integer('sysbench_run_seconds', 480,
                     'The duration of the actual run in which results are '
                     'collected, in seconds.')
flags.DEFINE_integer('sysbench_thread_count', 16,
                     'The number of test threads on the client side.')
flags.DEFINE_integer('sysbench_latency_percentile', 99,
                     'The latency percentile we ask sysbench to compute.')
flags.DEFINE_integer('sysbench_report_interval', 2,
                     'The interval, in seconds, we ask sysbench to report '
                     'results.')

BENCHMARK_NAME = 'mysql_service'
BENCHMARK_CONFIG = """
mysql_service:
  description: MySQL service benchmarks.
  vm_groups:
    default:
      vm_spec: *default_single_core
"""

# Query DB creation status once every 15 seconds
DB_STATUS_QUERY_INTERVAL = 15

# How many times we will wait for the service to create the DB
# total wait time is therefore: "query interval * query limit"
DB_STATUS_QUERY_LIMIT = 200

# Map from FLAGs.mysql_svc_db_instance_cores to RDS DB Type
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
RDS_DB_STORAGE_GP2_SIZE = '4170'

# A list of status strings that are possible during RDS DB creation.
RDS_DB_CREATION_PENDING_STATUS = frozenset(
    ['creating', 'modifying', 'backing-up', 'rebooting'])

# Constants defined for Sysbench tests.
RAND_INIT_ON = 'on'
DISABLE = 'disable'
UNIFORM = 'uniform'
OFF = 'off'
MYSQL_ROOT_USER = 'root'
MYSQL_ROOT_PASSWORD_PREFIX = 'Perfkit8'
MYSQL_PORT = '3306'

NORMAL_SYSBENCH_PATH_PREFIX = '/usr'
PREPARE_SCRIPT_PATH = '/share/doc/sysbench/tests/db/parallel_prepare.lua'
OLTP_SCRIPT_PATH = '/share/doc/sysbench/tests/db/oltp.lua'

SYSBENCH_RESULT_NAME_DATA_LOAD = 'sysbench data load time'
SYSBENCH_RESULT_NAME_TPS = 'sysbench tps'
SYSBENCH_RESULT_NAME_LATENCY = 'sysbench latency'
NA_UNIT = 'NA'
SECONDS_UNIT = 'seconds'
MS_UNIT = 'milliseconds'

# These are the constants that should be specified in GCP's cloud SQL command.
DEFAULT_BACKUP_START_TIME = '07:00'
GCP_MY_SQL_VERSION = 'MYSQL_5_6'
GCP_PRICING_PLAN = 'PACKAGE'

RESPONSE_TIME_TOKENS = ['min', 'avg', 'max', 'percentile']


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


class DBStatusQueryError(Exception):
  pass


def _GenerateRandomPassword():
  """ Generates a random password to be used by the DB instance.
  Args:
    None
  Returns:
    A string that can be used as password to a DB instance.
  """
  return '%s%s' % (MYSQL_ROOT_PASSWORD_PREFIX, str(uuid.uuid4())[-8:])


def ParseSysbenchOutput(sysbench_output, results, metadata):
  """Parses sysbench output.

  Extract relevant TPS and latency numbers, and populate the final result
  collection with these information.

  Specifically, we are interested in tps numbers reported by each reporting
  interval, and the summary latency numbers printed at the end of the run in
  "General Statistics" -> "Response Time".

  Example Sysbench output:

  sysbench 0.5:  multi-threaded system evaluation benchmark
  <... lots of output we don't care here ...>
  Threads started!

  [   2s] threads: 16, tps: 526.38, reads: 7446.79, writes: 2105.52, response
  time: 210.67ms (99%), errors: 0.00, reconnects:  0.00
  < .... lots of tps output every 2 second, we need all those>

  < ... lots of other output we don't care for now...>
  General statistics:
      total time:                          17.0563s
      total number of events:              10000
      total time taken by event execution: 272.6053s
      response time:
           min:                                 18.31ms
           avg:                                 27.26ms
           max:                                313.50ms
           approx.  99 percentile:              57.15ms
  < We care about the response time section above, these are latency numbers>
  < then there are some outputs after this, we don't care either>

  Args:
    sysbench_output: The output from sysbench.
    results: The dictionary to store results based on sysbench output.
    metadata: The metadata to be passed along to the Samples class.
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

  tps_percentile = sample.PercentileCalculator(all_tps)
  for percentile in sample.PERCENTILES_LIST:
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


def _GetSysbenchCommandPrefix():
  """ Decides what the prefix is for sysbench command based on os type.
  Args:
    None.
  Returns:
    A string representing the sysbench command prefix.
  """
  if FLAGS.os_type == os_types.RHEL:
    return vm_util.VM_TMP_DIR
  else:
    return NORMAL_SYSBENCH_PATH_PREFIX


def _IssueSysbenchCommand(vm, duration):
  """ Issues a sysbench run command given a vm and a duration.

      Does nothing if duration is <= 0

  Args:
    vm: The test VM to issue command to.
    duration: the duration of the sysbench run.

  Returns:
    stdout, stderr: the result of the command.
  """
  stdout = ''
  stderr = ''
  oltp_script_path = '%s%s' % (_GetSysbenchCommandPrefix(), OLTP_SCRIPT_PATH)
  if duration > 0:
    run_cmd_tokens = ['sysbench',
                      '--test=%s' % oltp_script_path,
                      '--mysql_svc_oltp_tables_count=%d' %
                      FLAGS.mysql_svc_oltp_tables_count,
                      '--oltp-table-size=%d' %
                      FLAGS.mysql_svc_oltp_table_size,
                      '--rand-init=%s' % RAND_INIT_ON,
                      '--db-ps-mode=%s' % DISABLE,
                      '--oltp-dist-type=%s' % UNIFORM,
                      '--oltp-read-only=%s' % OFF,
                      '--num-threads=%d' % FLAGS.sysbench_thread_count,
                      '--percentile=%d' % FLAGS.sysbench_latency_percentile,
                      '--report-interval=%d' %
                      FLAGS.sysbench_report_interval,
                      '--max-requests=0',
                      '--max-time=%d' % duration,
                      '--mysql-user=%s' % vm.db_instance_master_user,
                      '--mysql-password="%s"' %
                      vm.db_instance_master_password,
                      '--mysql-host=%s' % vm.db_instance_address,
                      'run']
    run_cmd = ' '.join(run_cmd_tokens)
    stdout, stderr = vm.RobustRemoteCommand(run_cmd)
    logging.info('Sysbench results: \n stdout is:\n%s\nstderr is\n%s',
                 stdout, stderr)

  return stdout, stderr


def _RunSysbench(vm, metadata):
  """ Runs the Sysbench OLTP test.

  The test is run on the DB instance as indicated by the vm.db_instance_address.

  Args:
    vm: The client VM that will issue the sysbench test.
    metadata: The PKB metadata to be passed along to the final results.

  Returns:
    Results: A list of results of this run.
  """
  results = []

  if not hasattr(vm, 'db_instance_address'):
    logging.error(
        'Prepare has likely failed, db_instance_address is not found.')
    raise DBStatusQueryError('RunSysbench: DB instance address not found.')

  # Create the sbtest database for Sysbench.
  # str(uuid.uuid4())[-8:]
  create_sbtest_db_cmd = ('mysql -h %s -u %s -p%s '
                          '-e \'create database sbtest;\'') % (
                              vm.db_instance_address,
                              vm.db_instance_master_user,
                              vm.db_instance_master_password)
  stdout, stderr = vm.RemoteCommand(create_sbtest_db_cmd)
  logging.info('sbtest db created, stdout is %s, stderr is %s',
               stdout, stderr)

  # Provision the Sysbench test based on the input flags (load data into DB)
  # Could take a long time if the data to be loaded is large.
  data_load_start_time = time.time()
  prepare_script_path = '%s%s' % (_GetSysbenchCommandPrefix(),
                                  PREPARE_SCRIPT_PATH)
  data_load_cmd_tokens = ['sysbench',
                          '--test=%s' % prepare_script_path,
                          '--mysql_svc_oltp_tables_count=%d' %
                          FLAGS.mysql_svc_oltp_tables_count,
                          '--oltp-table-size=%d' %
                          FLAGS.mysql_svc_oltp_table_size,
                          '--rand-init=%s' % RAND_INIT_ON,
                          '--num-threads=%d' %
                          FLAGS.mysql_svc_oltp_tables_count,
                          '--mysql-user=%s' % vm.db_instance_master_user,
                          '--mysql-password="%s"' %
                          vm.db_instance_master_password,
                          '--mysql-host=%s' % vm.db_instance_address,
                          'run']
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
      SYSBENCH_RESULT_NAME_DATA_LOAD,
      load_duration,
      SECONDS_UNIT,
      metadata))

  # Now run the sysbench OLTP test and parse the results.
  for phase in ['warm-up', 'run']:
    # First step is to run the test long enough to cover the warmup period
    # as requested by the caller. Then we do the "real" run, parse and report
    # the results.
    duration = 0
    if phase == 'warm-up' and FLAGS.sysbench_warmup_seconds > 0:
      duration = FLAGS.sysbench_warmup_seconds
      logging.info('Sysbench warm-up run, duration is %d', duration)
    elif phase == 'run':
      duration = FLAGS.sysbench_run_seconds
      logging.info('Sysbench real run, duration is %d', duration)

    stdout, stderr = _IssueSysbenchCommand(vm, duration)

    if phase == 'run':
      # We only need to parse the results for the "real" run.
      logging.info('\n Parsing Sysbench Results...\n')
      ParseSysbenchOutput(stdout, results, metadata)

  return results


def _RDSParseDBInstanceStatus(json_response):
  """Parses a JSON response from an RDS DB status query command.

  Args:
    json_response: The response from the DB status query command in JSON.

  Returns:
    A list of sample.Sample objects.
  """
  status = ''
  # Sometimes you look for 'DBInstance', some times you need to look for
  # 'DBInstances' and then take the first element
  if 'DBInstance' in json_response:
    status = json_response['DBInstance']['DBInstanceStatus']
  else:
    if 'DBInstances' in json_response:
      status = json_response['DBInstances'][0]['DBInstanceStatus']

  return status


class RDSMySQLBenchmark(object):
  """MySQL benchmark based on the RDS service on AWS."""

  def Prepare(self, vm):
    """Prepares the DB and everything for the AWS-RDS provider.

    Args:
      vm: The VM to be used as the test client.

    """
    logging.info('Preparing MySQL Service benchmarks for RDS.')

    # TODO: Refactor the RDS DB instance creation and deletion logic out
    # to a new class called RDSDBInstance that Inherits from
    # perfkitbenchmarker.resource.BaseResource.
    # And do the same for GCP.

    # First is to create another subnet in the same VPC as the VM but in a
    # different zone. RDS requires two subnets in two different zones to create
    # a DB instance, EVEN IF you do not specify multi-AZ in your DB creation
    # request.

    # Get a list of zones and pick one that's different from the zone VM is in.
    new_subnet_zone = None
    get_zones_cmd = util.AWS_PREFIX + ['ec2', 'describe-availability-zones']
    stdout, _, _ = vm_util.IssueCommand(get_zones_cmd)
    response = json.loads(stdout)
    all_zones = response['AvailabilityZones']
    for zone in all_zones:
      if zone['ZoneName'] != vm.zone:
        new_subnet_zone = zone['ZoneName']
        break

    if new_subnet_zone is None:
      raise DBStatusQueryError('Cannot find a zone to create the required '
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
    create_db_subnet_group_cmd = util.AWS_PREFIX + [
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
    open_port_cmd = util.AWS_PREFIX + [
        'ec2',
        'authorize-security-group-ingress',
        '--group-id', vm.group_id,
        '--source-group', vm.group_id,
        '--protocol', 'tcp',
        '--port', MYSQL_PORT]
    stdout, stderr, _ = vm_util.IssueCommand(open_port_cmd)
    logging.info('Granted DB port ingress, stdout is:\n%s\nstderr is:\n%s',
                 stdout, stderr)

    # Finally, it's time to create the DB instance!
    vm.db_instance_id = 'pkb-DB-%s' % FLAGS.run_uri
    db_class = \
        RDS_CORE_TO_DB_CLASS_MAP['%s' % FLAGS.mysql_svc_db_instance_cores]
    vm.db_instance_master_user = MYSQL_ROOT_USER
    vm.db_instance_master_password = _GenerateRandomPassword()

    create_db_cmd = util.AWS_PREFIX + [
        'rds',
        'create-db-instance',
        '--db-instance-identifier', vm.db_instance_id,
        '--db-instance-class', db_class,
        '--engine', RDS_DB_ENGINE,
        '--engine-version', RDS_DB_ENGINE_VERSION,
        '--storage-type', RDS_DB_STORAGE_TYPE_GP2,
        '--allocated-storage', RDS_DB_STORAGE_GP2_SIZE,
        '--vpc-security-group-ids', vm.group_id,
        '--master-username', vm.db_instance_master_user,
        '--master-user-password', vm.db_instance_master_password,
        '--availability-zone', vm.zone,
        '--db-subnet-group-name', vm.db_subnet_group_name]

    status_query_cmd = util.AWS_PREFIX + [
        'rds',
        'describe-db-instances',
        '--db-instance-id', vm.db_instance_id]

    stdout, stderr, _ = vm_util.IssueCommand(create_db_cmd)
    logging.info('Request to create the DB has been issued, stdout:\n%s\n'
                 'stderr:%s\n', stdout, stderr)
    response = json.loads(stdout)

    db_creation_status = _RDSParseDBInstanceStatus(response)

    for status_query_count in xrange(1, DB_STATUS_QUERY_LIMIT + 1):
      if db_creation_status == 'available':
        break

      if db_creation_status not in RDS_DB_CREATION_PENDING_STATUS:
        raise DBStatusQueryError('Invalid status in DB creation response. '
                                 ' stdout is\n%s, stderr is\n%s' % (
                                     stdout, stderr))

      logging.info('Querying db creation status, current state is %s, query '
                   'count is %d', db_creation_status, status_query_count)
      time.sleep(DB_STATUS_QUERY_INTERVAL)

      stdout, stderr, _ = vm_util.IssueCommand(status_query_cmd)
      response = json.loads(stdout)
      db_creation_status = _RDSParseDBInstanceStatus(response)
    else:
      raise DBStatusQueryError('DB creation timed-out, we have '
                               'waited at least %s * %s seconds.' % (
                                   DB_STATUS_QUERY_INTERVAL,
                                   DB_STATUS_QUERY_LIMIT))

    # We are good now, db has been created. Now get the endpoint address.
    # On RDS, you always connect with a DNS name, if you do that from a EC2 VM,
    # that DNS name will be resolved to an internal IP address of the DB.
    if 'DBInstance' in response:
      vm.db_instance_address = response['DBInstance']['Endpoint']['Address']
    else:
      if 'DBInstances' in response:
        vm.db_instance_address = \
            response['DBInstances'][0]['Endpoint']['Address']

    logging.info('Successfully created an RDS DB instance. Address is %s',
                 vm.db_instance_address)
    logging.info('Complete output is:\n %s', response)

  def Cleanup(self, vm):
    """Clean up RDS instances, cleanup the extra subnet created for the
       creation of the RDS instance.

    Args:
      vm: The VM that was used as the test client, which also stores states
          for clean-up.
    """

    # Now, we can delete the DB instance. vm.db_instance_id is the id to call.
    # We need to keep querying the status of the deletion here before we let
    # this go. RDS DB deletion takes some time to finish. And we have to
    # wait until this DB is deleted before we proceed because this DB holds
    # references to various other resources: subnet groups, subnets, vpc, etc.
    delete_db_cmd = util.AWS_PREFIX + [
        'rds',
        'delete-db-instance',
        '--db-instance-identifier', vm.db_instance_id,
        '--skip-final-snapshot']

    logging.info('Deleting db instance %s...', vm.db_instance_id)

    # Note below, the status of this deletion command is validated below in the
    # loop. both stdout and stderr are checked.
    stdout, stderr, _ = vm_util.IssueCommand(delete_db_cmd)
    logging.info('Request to delete the DB has been issued, stdout:\n%s\n'
                 'stderr:%s\n', stdout, stderr)

    status_query_cmd = util.AWS_PREFIX + [
        'rds',
        'describe-db-instances',
        '--db-instance-id', vm.db_instance_id]

    db_status = None
    for status_query_count in xrange(1, DB_STATUS_QUERY_LIMIT + 1):
      try:
        response = json.loads(stdout)
        db_status = _RDSParseDBInstanceStatus(response)

        if db_status == 'deleting':
          logging.info('DB is still in the deleting state, status_query_count '
                       'is %d', status_query_count)
          # Wait for a few seconds and query status
          time.sleep(DB_STATUS_QUERY_INTERVAL)
          stdout, stderr, _ = vm_util.IssueCommand(status_query_cmd)
        else:
          logging.info('DB deletion status is no longer in deleting, it is %s',
                       db_status)
          break
      except:
        # stdout cannot be parsed into json, it might simply be empty because
        # deletion has been completed.
        break
    else:
      logging.warn('DB is still in deleting state after long wait, bail.')

    db_instance_deletion_failed = False
    if db_status == 'deleted' or re.findall('DBInstanceNotFound', stderr):
      # Sometimes we get a 'deleted' status from DB status query command,
      # but even more times, the DB status query command would fail with
      # an "not found" error, both are positive confirmation that the DB has
      # been deleted.
      logging.info('DB has been successfully deleted, got confirmation.')
    else:
      # We did not get a positive confirmation that the DB is deleted even after
      # long wait, we have to bail. But we will log an error message, and
      # then raise an exception at the end of this function so this particular
      # run will show as a failed run to the user and allow them to examine
      # the logs
      db_instance_deletion_failed = True
      logging.error(
          'RDS DB instance %s failed to be deleted, we did not get '
          'final confirmation from stderr, which is:\n %s', vm.db_instance_id,
          stderr)

    if hasattr(vm, 'db_subnet_group_name'):
      delete_db_subnet_group_cmd = util.AWS_PREFIX + [
          'rds',
          'delete-db-subnet-group',
          '--db-subnet-group-name', vm.db_subnet_group_name]
      stdout, stderr, _ = vm_util.IssueCommand(delete_db_subnet_group_cmd)
      logging.info('Deleted the db subnet group. stdout is:\n%s, stderr: \n%s',
                   stdout, stderr)

    if hasattr(vm, 'extra_subnet_for_db'):
      vm.extra_subnet_for_db.Delete()

    if db_instance_deletion_failed:
      raise DBStatusQueryError('Failed to get confirmation of DB instance '
                               'deletion! Check the log for details!')


class GoogleCloudSQLBenchmark(object):
  """MySQL benchmark based on the Google Cloud SQL service."""

  def Prepare(self, vm):
    """Prepares the DB and everything for the provider GCP (Cloud SQL)

    Args:
      vm: The VM to be used as the test client

    """
    # TODO: Refactor the GCP Cloud SQL instance creation and deletion logic out
    # to a new class called GCPCloudSQLInstance that Inherits from
    # perfkitbenchmarker.resource.BaseResource.

    logging.info('Preparing MySQL Service benchmarks for Google Cloud SQL.')

    vm.db_instance_name = 'pkb%s' % FLAGS.run_uri
    db_tier = 'db-n1-standard-%s' % FLAGS.mysql_svc_db_instance_cores
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
      raise DBStatusQueryError('Invalid operation or unrecognized '
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
        if query_count > DB_STATUS_QUERY_LIMIT:
          raise DBStatusQueryError('DB creation timed-out, we have '
                                   'waited at least %s * %s seconds.' % (
                                       DB_STATUS_QUERY_INTERVAL,
                                       DB_STATUS_QUERY_LIMIT))

        logging.info('Querying db creation status, current state is %s, query '
                     'count is %d', state, query_count)
        time.sleep(DB_STATUS_QUERY_INTERVAL)

        stdout, _, _ = vm_util.IssueCommand(status_query_cmd)
        response = json.loads(stdout)
        query_count += 1

    logging.info('Successfully created the DB instance. Complete response is '
                 '%s', response)

    vm.db_instance_address = response['ipAddresses'][0]['ipAddress']
    logging.info('DB IP address is: %s', vm.db_instance_address)

    # Set the root password to a common one that can be referred to in common
    # code across providers.
    vm.db_instance_master_user = MYSQL_ROOT_USER
    vm.db_instance_master_password = _GenerateRandomPassword()
    set_password_cmd = [FLAGS.gcloud_path,
                        'sql',
                        'instances',
                        'set-root-password',
                        vm.db_instance_name,
                        '--password', vm.db_instance_master_password]
    stdout, stderr, _ = vm_util.IssueCommand(set_password_cmd)
    logging.info('Set root password completed. Stdout:\n%s\nStderr:\n%s',
                 stdout, stderr)

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
    providers.GCP: GoogleCloudSQLBenchmark(),
    providers.AWS: RDSMySQLBenchmark()}


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
  metadata = {
      'mysql_svc_oltp_tables_count': FLAGS.mysql_svc_oltp_tables_count,
      'mysql_svc_oltp_table_size': FLAGS.mysql_svc_oltp_table_size,
      'mysql_svc_db_instance_cores': FLAGS.mysql_svc_db_instance_cores,
      'sysbench_warm_up_seconds': FLAGS.sysbench_warmup_seconds,
      'sysbench_run_seconds': FLAGS.sysbench_run_seconds,
      'sysbench_thread_count': FLAGS.sysbench_thread_count,
      'sysbench_latency_percentile': FLAGS.sysbench_latency_percentile,
      'sysbench_report_interval': FLAGS.sysbench_report_interval
  }

  # The run phase is common across providers. The VMs[0] object contains all
  # information and states necessary to carry out the run.
  results = _RunSysbench(vms[0], metadata)
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
