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

"""Run YCSB benchmark against Google Cloud Spanner.

By default, this benchmark provision 1 single-CPU VM and spawn 1 thread
to test Spanner. Configure the number of VMs via --ycsb_client_vms.

In some cases, sleep time in between loading and running may be required for
best performance. See https://cloud.google.com/spanner/docs/pre-warm-database
for best practices.
"""

from typing import Any
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.providers.gcp import gcp_spanner

BENCHMARK_NAME = 'cloud_spanner_ycsb'

BENCHMARK_DESCRIPTION = 'YCSB'
BENCHMARK_TABLE = 'usertable'
BENCHMARK_ZERO_PADDING = 12

BENCHMARK_CONFIG = f"""
cloud_spanner_ycsb:
  description: >
      Run YCSB against Google Cloud Spanner.
      Configure the number of VMs via --ycsb_client_vms.
  relational_db:
    cloud: GCP
    engine: spanner-googlesql
    spanner_nodes: 1
    spanner_description: {BENCHMARK_DESCRIPTION}
    enable_freeze_restore: True
    vm_groups:
      default:
        os_type: ubuntu2204  # Python 2
        vm_spec: *default_dual_core
        vm_count: 1
  flags:
    openjdk_version: 8
    gcloud_scopes: cloud-platform
"""

FLAGS = flags.FLAGS
flags.DEFINE_integer(
    'cloud_spanner_ycsb_batchinserts',
    1,
    'The Cloud Spanner batch inserts used in the YCSB benchmark.',
)
flags.DEFINE_integer(
    'cloud_spanner_ycsb_boundedstaleness',
    0,
    'The Cloud Spanner bounded staleness used in the YCSB benchmark.',
)
flags.DEFINE_enum(
    'cloud_spanner_ycsb_readmode',
    'query',
    ['query', 'read'],
    'The Cloud Spanner read mode used in the YCSB benchmark.',
)
flags.DEFINE_list(
    'cloud_spanner_ycsb_custom_vm_install_commands',
    [],
    'A list of strings. If specified, execute them on every '
    'VM during the installation phase.',
)


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['relational_db']['vm_groups']['default'][
        'vm_count'
    ] = FLAGS.ycsb_client_vms
  return config


def CheckPrerequisites(_):
  """Validates correct flag usages before running this benchmark."""
  if ycsb.CPU_OPTIMIZATION.value and (
      ycsb.CPU_OPTIMIZATION_MEASUREMENT_MINS.value
      <= gcp_spanner.CPU_API_DELAY_MINUTES
  ):
    raise errors.Setup.InvalidFlagConfigurationError(
        f'measurement_mins {ycsb.CPU_OPTIMIZATION_MEASUREMENT_MINS.value} must'
        ' be greater than CPU_API_DELAY_MINUTES'
        f' {gcp_spanner.CPU_API_DELAY_MINUTES}'
    )


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run cloud spanner benchmarks.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  benchmark_spec.always_call_cleanup = True

  vms = benchmark_spec.vms

  # Install required packages and copy credential files
  background_tasks.RunThreaded(_Install, vms)

  benchmark_spec.executor = ycsb.YCSBExecutor('cloudspanner')

  spanner: gcp_spanner.GcpSpannerInstance = benchmark_spec.relational_db
  spanner.CreateTables(_BuildSchema())


def _LoadDatabase(
    executor: ycsb.YCSBExecutor,
    spanner: gcp_spanner.GcpSpannerInstance,
    vms: list[virtual_machine.VirtualMachine],
    load_kwargs: dict[str, Any],
) -> list[sample.Sample]:
  """Loads the database with the specified infrastructure capacity."""
  if spanner.restored or ycsb.SKIP_LOAD_STAGE.value:
    return []
  spanner.UpdateCapacityForLoad()
  results = list(executor.Load(vms, load_kwargs=load_kwargs))
  spanner.UpdateCapacityForRun()
  return results


def Run(benchmark_spec):
  """Spawn YCSB and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  vms = benchmark_spec.vms
  spanner: gcp_spanner.GcpSpannerInstance = benchmark_spec.relational_db
  executor: ycsb.YCSBExecutor = benchmark_spec.executor

  run_kwargs = {
      'table': BENCHMARK_TABLE,
      'zeropadding': BENCHMARK_ZERO_PADDING,
      'cloudspanner.instance': spanner.instance_id,
      'cloudspanner.database': spanner.database,
      'cloudspanner.readmode': FLAGS.cloud_spanner_ycsb_readmode,
      'cloudspanner.boundedstaleness': (
          FLAGS.cloud_spanner_ycsb_boundedstaleness
      ),
      'cloudspanner.batchinserts': FLAGS.cloud_spanner_ycsb_batchinserts,
  }
  # Uses overridden cloud spanner endpoint in gcloud configuration
  end_point = spanner.GetApiEndPoint()
  if end_point:
    run_kwargs['cloudspanner.host'] = end_point

  load_kwargs = run_kwargs.copy()
  load_kwargs['core_workload_insertion_retry_limit'] = 100
  samples = []
  metadata = {'ycsb_client_type': 'java'}

  samples += _LoadDatabase(executor, spanner, vms, load_kwargs)

  samples += list(executor.Run(vms, run_kwargs=run_kwargs, database=spanner))

  for result in samples:
    result.metadata.update(metadata)
    result.metadata.update(spanner.GetResourceMetadata())

  return samples


def Cleanup(benchmark_spec):
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  del benchmark_spec


def _BuildSchema():
  """BuildSchema.

  Returns:
    A string of DDL for creating a Spanner table.
  """
  fields = ',\n'.join(
      [f'field{i} STRING(MAX)' for i in range(FLAGS.ycsb_field_count)]
  )
  return f"""
  CREATE TABLE {BENCHMARK_TABLE} (
    id     STRING(MAX),
    {fields}
  ) PRIMARY KEY(id)
  """


def _Install(vm):
  """Installs YCSB on the VM."""
  vm.Install('ycsb')

  # Run custom VM installation commands.
  for command in FLAGS.cloud_spanner_ycsb_custom_vm_install_commands:
    _, _ = vm.RemoteCommand(command)
