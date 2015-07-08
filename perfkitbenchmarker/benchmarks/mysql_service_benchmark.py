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
import time

from perfkitbenchmarker import benchmark_spec as benchmark_spec_class
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
flags.DEFINE_enum(
    'db_instance_cores', '8', ['1', '4', '8', '16'],
    'The number of cores to be provisioned for the DB instance.')


BENCHMARK_INFO = {'name': 'mysql_service',
                  'description': 'MySQL service benchmarks.',
                  'scratch_disk': False,
                  'num_machines': 1}

# Query DB creation status once every 15 seconds
DB_CREATION_STATUS_QUERY_INTERVAL = 15

# How many times we will wait for the service to create the DB
# total wait time is therefore: "query interval * query limit"
DB_CREATION_STATUS_QUERY_LIMIT = 100

DEFAULT_BACKUP_START_TIME = '07:00'
# These are the constants that should be specified in GCP's cloud SQL command.
GCP_MY_SQL_VERSION = 'MYSQL_5_6'
GCP_PRICING_PLAN = 'PACKAGE'


def GetInfo():
  return BENCHMARK_INFO


class DBInstanceCreationError(Exception):
    pass


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

    self.db_instance_name = 'pkb%s' % FLAGS.run_uri
    db_tier = 'db-n1-standard-%s' % FLAGS.db_instance_cores
    # Currently, we create DB instance in the same zone as the test VM.
    db_instance_zone = vm.zone
    # Currently GCP REQUIRES you to connect to the DB instance via external IP
    # (i.e., using external IPs of the DB instance AND the VM instance).
    authorized_network = '%s/32' % vm.ip_address
    create_db_cmd = [FLAGS.gcloud_path,
                     'sql',
                     'instances',
                     'create', self.db_instance_name,
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
                        'describe', self.db_instance_name,
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

    self.db_instance_ip = response['ipAddresses'][0]['ipAddress']
    logging.info('DB IP address is: %s',
                 self.db_instance_ip)

  def Run(self, vm, metadata):
    results = []
    return results

  def Cleanup(self, vm):
    if hasattr(self, 'db_instance_name'):
      delete_db_cmd = [FLAGS.gcloud_path,
                       'sql',
                       'instances',
                       'delete', self.db_instance_name,
                       '--quiet']

      stdout, stderr, _ = vm_util.IssueCommand(delete_db_cmd)
      logging.info('DB cleanup command issued, stdout is %s, stderr is %s',
                   stdout, stderr)
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
