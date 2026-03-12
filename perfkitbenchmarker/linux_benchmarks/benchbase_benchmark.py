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

import logging
import time
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker.linux_packages import benchbase
from perfkitbenchmarker.providers.aws import aws_aurora_dsql_db


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
      default:
        os_type: ubuntu2404
        vm_spec: *default_dual_core
        vm_count: 1
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
  flags:
    gcloud_scopes: cloud-platform
"""

FLAGS = flags.FLAGS
_SECONDS_IN_MINUTE = 60
_COOLDOWN_DURATION = benchbase.BENCHBASE_COOLDOWN_DURATION
_WARMUP_DURATION = benchbase.BENCHBASE_WARMUP_DURATION
_WORKLOAD_DURATION = benchbase.BENCHBASE_WORKLOAD_DURATION
_WAREHOUSES = benchbase.BENCHBASE_WAREHOUSES
_RECOVERY_POINT_ARN = aws_aurora_dsql_db.AWS_AURORA_DSQL_RECOVERY_POINT_ARN
# Timeout value derived from testing run:
# https://screenshot.googleplex.com/8USL62xn9oSvABj
_SPANNER_ANALYZE_TIMEOUT = 60 * 60 * 3


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Loads and returns benchmark config.

  Args:
    user_config: user supplied configuration (flags and config file)

  Returns:
    loaded benchmark configuration
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if _COOLDOWN_DURATION.value > 0 and _WARMUP_DURATION.value <= 0:
    raise errors.Config.InvalidValue(
        'benchbase_warmup_duration must be positive if'
        ' benchbase_cooldown_duration is positive.'
    )
  return config


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

  dsql_create_from_raw: bool = (
      FLAGS.db_engine == sql_engine_utils.AURORA_DSQL_POSTGRES
      and _RECOVERY_POINT_ARN.value is None
  )
  if (
      dsql_create_from_raw
      or FLAGS.db_engine == sql_engine_utils.SPANNER_POSTGRES
  ):
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

  if FLAGS.db_engine == sql_engine_utils.SPANNER_POSTGRES:
    benchmark_spec.relational_db.RunDDLQuery(
        'ANALYZE;', timeout=_SPANNER_ANALYZE_TIMEOUT
    )
  elif FLAGS.db_engine == sql_engine_utils.AURORA_DSQL_POSTGRES:
    queries = [
        'ANALYZE public.customer;',
        'ANALYZE public.district;',
        'ANALYZE public.history;',
        'ANALYZE public.item;',
        'ANALYZE public.new_order;',
        'ANALYZE public.oorder;',
        'ANALYZE public.order_line;',
        'ANALYZE public.stock;',
        'ANALYZE public.warehouse;',
    ]
    for query in queries:
      benchmark_spec.relational_db.RunSqlQuery(query)


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
  run_command: str = (
      f'source /etc/profile.d/maven.sh && cd {benchbase.BENCHBASE_DIR} && mvn'
      f' clean compile exec:java -P {profile} -Dexec.args="-b tpcc -c'
      f' {benchbase.CONFIG_FILE_NAME} --create=false --load=false'
      ' --execute=true"'
  )
  if _WARMUP_DURATION.value > 0:
    benchbase.UpdateTime(client_vm, _WARMUP_DURATION.value)
    logging.info(
        'Running benchbase warmup for %d minutes',
        _WARMUP_DURATION.value,
    )
    client_vm.RemoteCommand(run_command)
    if _COOLDOWN_DURATION.value > 0:
      logging.info(
          'Running benchbase cooldown for %d minutes',
          _COOLDOWN_DURATION.value,
      )
      time.sleep(_COOLDOWN_DURATION.value * _SECONDS_IN_MINUTE)
    benchbase.UpdateTime(client_vm, _WORKLOAD_DURATION.value)

  logging.info(
      'Running benchbase workload for %d minutes',
      _WORKLOAD_DURATION.value,
  )
  client_vm.RemoteCommand(run_command)
  metadata = benchmark_spec.relational_db.GetResourceMetadata()
  return benchbase.ParseResults(client_vm, metadata)


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleans up the benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  # Nothing to do in cleanup for now.
  del benchmark_spec  # Unused for now.
  pass
