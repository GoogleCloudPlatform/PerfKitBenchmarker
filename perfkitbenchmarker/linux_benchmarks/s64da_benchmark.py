# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""s64da benchmark for PostgreSQL databases.

s64da benchmark is a HTAP database benchmark for Postgres.

This implementation of s64da in PKB uses the ManagedRelationalDB
resource. A client VM is also required. To change the specs of the
database server, change the vm_spec nested inside
managed_relational_db spec. To change the specs of the client,
change the vm_spec nested directly inside the pgbench spec.

Documentations of s64da
https://github.com/swarm64/s64da-benchmark-toolkit
"""

import itertools
import textwrap
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import s64da

_SCALE_FACTOR = flags.DEFINE_integer(
    's64da_scale_factor',
    1,
    'scale factor used to fill the database. '
    '1 scale factor roughly equals to 3GB.',
    lower_bound=1,
)

_BENCHMARK_TYPE = flags.DEFINE_enum(
    's64da_benchmark_type',
    'htap',
    ['htap'],
    'Benchmark to run with s64da. Only htap is currently supported.',
)

SCHEMAS = [
    'psql_native',
    's64da_native',
    's64da_native_enhanced',
    's64da_performance',
]

SCHEMA_PARTITIONS = ['', '_partitioned_id_hashed', '_partitioned_date_week']

ALL_SCHEMAS = [
    ''.join(i) for i in itertools.product(SCHEMAS, SCHEMA_PARTITIONS)
]

_SCHEMA = flags.DEFINE_enum(
    's64da_schema',
    'psql_native',
    ALL_SCHEMAS,
    'Schema to use for benchmarking the database.',
)

_MAX_JOB = flags.DEFINE_integer(
    's64da-max-jobs',
    8,
    'Limit the overall loading parallelism to this amount of jobs.',
)

_OLTP_WORKERS = flags.DEFINE_integer(
    's64da-oltp-workers',
    1,
    'The number of OLTP workers executing '
    'TPC-C transactions (i.e. simulated clients), default: 1',
)

_OLAP_WORKERS = flags.DEFINE_integer(
    's64da-olap-workers',
    1,
    'The number of OLAP workers running modified TPC-H queries, default: 1.',
)

_DURATION = flags.DEFINE_integer(
    's64da-duration',
    60,
    'The number of seconds the benchmark should run for, default: 60 seconds',
)

_RAMP_UP_DURATION = flags.DEFINE_integer(
    's64da-ramp_up_duration',
    10,
    'The number of seconds the benchmark should ramp up for.',
)

_OLAP_TIMEOUT = flags.DEFINE_string(
    's64da-olap-timeout',
    '5min',
    'Timeout for OLAP queries in seconds, default: 5min',
)

_RUN_OLTP_ON_REPLICA = flags.DEFINE_bool(
    's64da-run_oltp_on_replica', False, 'Run the benchmark on read replica.'
)

FLAGS = flags.FLAGS

BENCHMARK_NAME = 's64da'
BENCHMARK_CONFIG = textwrap.dedent("""\
  s64da:
    description: s64da benchmark for PostgreSQL databases
    relational_db:
      engine: postgres
      db_spec:
        GCP:
          machine_type:
            cpus: 16
            memory: 64GiB
          zone: us-central1-c
        AWS:
          machine_type: db.m4.4xlarge
          zone: us-west-1c
        Azure:
          machine_type:
            tier: Standard
            compute_units: 800
          zone: eastus
      db_disk_spec:
        GCP:
          disk_size: 1000
          disk_type: pd-ssd
        AWS:
          disk_size: 6144
          disk_type: gp2
        Azure:
          # Valid storage sizes range from minimum of 128000 MB
          # and additional increments of 128000 MB up to maximum of 1024000 MB.
          disk_size: 128
      vm_groups:
        clients:
          vm_spec:
            GCP:
              machine_type: n1-standard-16
              zone: us-central1-c
            AWS:
              machine_type: m4.4xlarge
              zone: us-west-1c
            Azure:
              machine_type: Standard_A4m_v2
              zone: eastus
          disk_spec: *default_500_gb
        servers:
          vm_spec:
            GCP:
              machine_type: n1-standard-16
              zone: us-central1-c
            AWS:
              machine_type: m4.4xlarge
              zone: us-west-1c
            Azure:
              machine_type: Standard_A4m_v2
              zone: eastus
          disk_spec: *default_500_gb
  """)

DEFAULT_DB_NAME = 'postgres'


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepares the client and server VM for the pgbench test.

  This function installs pgbench on the client and creates and populates
  the test database on the server.

  If DEFAULT_DB_NAME exists, it will be dropped and recreated,
  else it will be created. pgbench will populate the database
  with sample data using FLAGS.pgbench_scale_factor.

  Args:
    benchmark_spec: benchmark_spec object which contains the database server and
      client_vm.
  """
  vm = benchmark_spec.vms[0]
  vm.Install('s64da')
  db = benchmark_spec.relational_db
  s64da.PrepareBenchmark(
      vm,
      db,
      _BENCHMARK_TYPE.value,
      _SCHEMA.value,
      _SCALE_FACTOR.value,
      _MAX_JOB.value,
  )


def GetMetadata() -> Dict[str, Any]:
  """Get Metadata for s64da."""
  return {
      's64da_benchmark_type': _BENCHMARK_TYPE.value,
      's64da_schema': _SCHEMA.value,
      's64da_scale_factor': _SCALE_FACTOR.value,
      's64da_max_job': _MAX_JOB.value,
      's64da-oltp-workers': _OLTP_WORKERS.value,
      's64da-olap-workers': _OLAP_WORKERS.value,
      's64da-duration': _DURATION.value,
      's64da-ramp_up_duration': _RAMP_UP_DURATION.value,
      's64da-olap-timeout': _OLAP_TIMEOUT.value,
      's64da-run_oltp_on_replica': _RUN_OLTP_ON_REPLICA.value,
  }


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Run the s64da benchmark.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  vm = benchmark_spec.vms[0]
  db = benchmark_spec.relational_db
  metadata = GetMetadata()

  samples = s64da.RunBenchmark(
      vm,
      db,
      _BENCHMARK_TYPE.value,
      _OLTP_WORKERS.value,
      _OLAP_WORKERS.value,
      _DURATION.value,
      _OLAP_TIMEOUT.value,
      _RAMP_UP_DURATION.value,
      _RUN_OLTP_ON_REPLICA.value,
  )
  for s in samples:
    s.metadata.update(metadata)
  return samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec):
  """Cleans up the VM to its original state, currently is a placeholder.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  del benchmark_spec
