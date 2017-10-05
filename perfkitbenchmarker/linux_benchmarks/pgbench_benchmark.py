# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Pgbench"""

import time

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import flag_util
from perfkitbenchmarker import sample

flags.DEFINE_integer(
    'pgbench_scale_factor', 1, 'scale factor used to fill the database',
    lower_bound=1)
flags.DEFINE_integer(
    'pgbench_seconds_per_test', 10, 'number of seconds to run each test phase',
    lower_bound=1)
flags.DEFINE_integer(
    'pgbench_seconds_to_pause_before_steps', 30,
    'number of seconds to pause before each client load step')
flag_util.DEFINE_integerlist(
    'pgbench_client_counts',
    flag_util.IntegerList([1, 2, 4, 8, 16, 32, 64]),
    'array of client counts passed to pgbench',
    on_nonincreasing=flag_util.IntegerListParser.WARN)
FLAGS = flags.FLAGS


BENCHMARK_NAME = 'pgbench'
BENCHMARK_CONFIG = """
pgbench:
  description: test managed relational database provisioning
  managed_relational_db:
    database: postgres
    vm_spec:
      GCP:
        machine_type:
          cpus: 16
          memory: 64GiB
        zone: us-central1-c
      AWS:
        machine_type: db.m4.4xlarge
        zone: us-west-1c
      Azure:
        zone: westus
    disk_spec:
      GCP:
        disk_size: 1000
        disk_type: pd-ssd
      AWS:
        disk_size: 6144
        disk_type: gp2
      Azure:
        disk_size: 1000
  vm_groups:
    default:
      vm_spec:
        AWS:
          machine_type: m4.4xlarge
          image: ami-09d2fb69
          zone: us-west-1c
        Azure:
          machine_type: Standard_A8m_v2
          image: Canonical:UbuntuServer:16.04-LTS:latest
          zone: westus
        GCP:
          machine_type: n1-standard-16
          image: ubuntu-1604-xenial-v20170815a
          image_project: ubuntu-os-cloud
          zone: us-central1-c
"""


TEST_DB_NAME = 'perftest'
DEFAULT_DB_NAME = 'postgres'
MAX_JOBS = 16  # TODO(ferneyhough): use test VM num_cpus


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  pass


def UpdateBenchmarkSpecWithPrepareStageFlags(benchmark_spec):
  benchmark_spec.scale_factor = FLAGS.pgbench_scale_factor


def UpdateBenchmarkSpecWithRunStageFlags(benchmark_spec):
  benchmark_spec.seconds_per_test = FLAGS.pgbench_seconds_per_test
  benchmark_spec.seconds_to_pause = FLAGS.pgbench_seconds_to_pause_before_steps
  benchmark_spec.client_counts = FLAGS.pgbench_client_counts


def Prepare(benchmark_spec):
  vm = benchmark_spec.vms[0]
  vm.Install('pgbench')

  UpdateBenchmarkSpecWithPrepareStageFlags(benchmark_spec)

  db = benchmark_spec.managed_relational_db
  connection_string = db.MakePsqlConnectionString(DEFAULT_DB_NAME)

  CreateDatabase(benchmark_spec, DEFAULT_DB_NAME, TEST_DB_NAME)

  connection_string = db.MakePsqlConnectionString(TEST_DB_NAME)
  stdout, _ = vm.RobustRemoteCommand('pgbench {0} -i -s {1}'.format(
      connection_string, benchmark_spec.scale_factor))


def DoesDatabaseExist(benchmark_spec, connection_string, database):
  command = 'psql {0} -lqt | cut -d \| -f 1 | grep -qw {1}'.format(
      connection_string, database)
  _, _, return_value = benchmark_spec.vms[0].RemoteCommand(
      command, ignore_failure=True, with_return_value=True)
  return return_value == 0


def CreateDatabase(benchmark_spec, default_database, new_database):
  db = benchmark_spec.managed_relational_db
  connection_string = db.MakePsqlConnectionString(default_database)

  if DoesDatabaseExist(benchmark_spec, connection_string, new_database):
    command = 'psql {0} -c "DROP DATABASE {1};"'.format(
        connection_string, new_database)
    stdout, _ = benchmark_spec.vms[0].RemoteCommand(command, should_log=True)

  command = 'psql {0} -c "CREATE DATABASE {1};"'.format(
      connection_string, new_database)
  stdout, _ = benchmark_spec.vms[0].RemoteCommand(command, should_log=True)


def _MakeSamplesFromOutput(pgbench_stderr, num_clients, num_jobs,
                           additional_metadata):
  lines = pgbench_stderr.splitlines()[2:]
  tps_numbers = [float(line.split(' ')[3]) for line in lines]
  latency_numbers = [float(line.split(' ')[6]) for line in lines]

  metadata = additional_metadata.copy()
  metadata.update({'clients': num_clients, 'jobs': num_jobs})
  tps_metadata = metadata.copy()
  tps_metadata.update({'tps': tps_numbers})
  latency_metadata = metadata.copy()
  latency_metadata.update({'latency': latency_numbers})

  tps_sample = sample.Sample('tps_array', -1, 'tps', tps_metadata)
  latency_sample = sample.Sample('latency_array', -1, 'ms', latency_metadata)
  return [tps_sample, latency_sample]


def Run(benchmark_spec):
  UpdateBenchmarkSpecWithRunStageFlags(benchmark_spec)

  db = benchmark_spec.managed_relational_db
  connection_string = db.MakePsqlConnectionString(TEST_DB_NAME)

  common_metadata = {
      'scale_factor': benchmark_spec.scale_factor,
      'seconds_per_test': benchmark_spec.seconds_per_test,
      'seconds_to_pause_before_steps': benchmark_spec.seconds_to_pause,
  }

  samples = []
  for client in benchmark_spec.client_counts:
    time.sleep(benchmark_spec.seconds_to_pause)
    jobs = min(client, 16)
    command = ('pgbench {0} --client={1} --jobs={2} --time={3} --progress=1 '
               '--report-latencies'.format(
                   connection_string,
                   client,
                   jobs,
                   benchmark_spec.seconds_per_test))
    stdout, stderr = benchmark_spec.vms[0].RobustRemoteCommand(
        command, should_log=True)
    samples.extend(_MakeSamplesFromOutput(
        stderr, client, jobs, common_metadata))
  return samples


def Cleanup(benchmark_spec):
  pass
