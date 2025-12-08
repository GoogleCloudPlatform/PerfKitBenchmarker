# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
# limitations under the License

"""Benchbase Benchmark.

This is a set of benchmarks that measures OLTP performance of managed
postgres databases using the Benchbase(https://github.com/cmu-db/benchbase)
framework.
"""

from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker.linux_packages import benchbase
# Needed in order to register spec:
from perfkitbenchmarker.providers.aws import aws_aurora_dsql_db  # pylint: disable=unused-import


SPANNER_POSTGRES = sql_engine_utils.SPANNER_POSTGRES
BENCHMARK_NAME: str = 'benchbase'
BENCHMARK_CONFIG: str = """
benchbase:
  description: Runs Benchbase benchmark.
  relational_db:
    cloud: AWS
    engine: aurora-dsql-postgres
    db_spec:
      GCP:
        machine_type: db-n1-standard-16
        zone: us-central1-f
      AWS:
        zone: us-east-1a
    vm_groups:
      clients:
        vm_spec:
          GCP:
            machine_type: n2-standard-21
            zone: us-central1-c
          AWS:
            machine_type: m5.8xlarge
            zone: us-east-1
        disk_spec:
          GCP:
            disk_size: 500
            disk_type: pd-ssd
          AWS:
            disk_size: 500
            disk_type: gp3
"""

FLAGS = flags.FLAGS


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Loads and returns benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  return config


# TODO(shuninglin): need to implement auth logic(automatic password gen)
# for DSQL
def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepares the benchmark by installing BenchBase and loading data.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """

  vms = benchmark_spec.vms
  client_vm = vms[0]

  # Install BenchBase on the client VM
  client_vm.Install('benchbase')

  # Create the configuration file on the client VM
  benchbase.CreateConfigFile(client_vm)
  if FLAGS.db_engine == sql_engine_utils.AURORA_DSQL_POSTGRES:
    dsql: aws_aurora_dsql_db.AwsAuroraDsqlRelationalDb = (
        benchmark_spec.relational_db
    )
    # Ideally we want to use endpoint from get-cluster command but it's not
    # returning endpoint as documented. That said this hard-coded endpoint
    # construction is also documented so should be reliable.
    # https://docs.aws.amazon.com/aurora-dsql/latest/userguide/SECTION_authentication-token.html#authentication-token-cli
    endpoint: str = f'{dsql.cluster_id}.dsql.{dsql.region}.on.aws'
    benchbase.OverrideEndpoint(client_vm, endpoint)
  profile: str = (
      'postgres'
      if FLAGS.db_engine == sql_engine_utils.SPANNER_POSTGRES
      else 'auroradsql'
  )
  load_command: str = (
      f'source /etc/profile.d/maven.sh && cd {benchbase.BENCHBASE_DIR} && mvn'
      f' clean compile exec:java -P {profile} -Dexec.args="-b tpcc -c'
      f' {benchbase.CONFIG_FILE_NAME} --create=true --load=true'
      ' --execute=false"'
  )

  client_vm.RemoteCommand(load_command)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs the BenchBase benchmark.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of sample.Sample objects.
  """
  client_vm = benchmark_spec.vms[0]
  profile: str = (
      'postgres'
      if FLAGS.db_engine == sql_engine_utils.SPANNER_POSTGRES
      else 'auroradsql'
  )
  # TODO(shuninglin): implement the three-phase warmup run - cool down - formal
  # run, right now it's just one run
  run_command: str = (
      f'source /etc/profile.d/maven.sh && cd {benchbase.BENCHBASE_DIR} && mvn'
      f' clean compile exec:java -P {profile} -Dexec.args="-b tpcc -c'
      f' {benchbase.CONFIG_FILE_NAME} --create=false --load=false'
      ' --execute=true"'
  )
  client_vm.RemoteCommand(run_command)
  # TODO(shuninglin): Parse results from the output files

  samples: List[sample.Sample] = []
  return samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleans up the benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  # Nothing to do in cleanup for now.
  del benchmark_spec  # Unused for now.
  pass
