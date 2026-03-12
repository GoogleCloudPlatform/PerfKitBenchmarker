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
# limitations under the License.

"""Run YCSB against managed MongoDB.

MongoDB homepage: http://www.mongodb.com
YCSB homepage: https://github.com/brianfrankcooper/YCSB
"""

import logging
from typing import Any

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import non_relational_db
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import ycsb


_READ_PREFERENCE = flags.DEFINE_string(
    'managed_mongodb_ycsb_read_preference',
    'primary',
    'The read preference to use for the MongoDB cluster.',
)
_WRITE_CONCERN = flags.DEFINE_string(
    'managed_mongodb_ycsb_write_concern',
    'majority',
    'The write concern to use for the MongoDB cluster.',
)
_BATCH_SIZE = flags.DEFINE_integer(
    'managed_mongodb_batch_size', None, 'The batch size to use for loading'
)

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'managed_mongodb_ycsb'
# 0.18.0-SNAPSHOT MongoDB YCSB requires using an older version of maven (<3.8)
# to build, see https://github.com/brianfrankcooper/YCSB/issues/1278
BENCHMARK_CONFIG = """
managed_mongodb_ycsb:
  description: Run YCSB against a managed MongoDB instance.
  non_relational_db:
    service_type: firestore
  vm_groups:
    clients:
      os_type: ubuntu2204  # Python 2
      vm_spec: *default_dual_core
      vm_count: 1
  flags:
    openjdk_version: 11
    create_and_boot_post_task_delay: 5
    ycsb_commit: 58d587888b12e61d68b09efa21b7cb3f74cc046a
    ycsb_version: 0.18.0-SNAPSHOT
    ycsb_binding: mongodb
    maven_version: 3.6.3
    gcloud_scopes: >
      trace
      datastore
      cloud-platform"""

_LinuxVM = linux_virtual_machine.BaseLinuxVirtualMachine
_ManagedMongoDb = non_relational_db.BaseManagedMongoDb


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Validates the user config dictionary."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['clients']['vm_count'] = FLAGS.ycsb_client_vms
  return config


def _PrepareClient(vm: _LinuxVM) -> None:
  """Install YCSB on the client VM."""
  vm.RemoteCommand('killall -9 java', ignore_failure=True)
  vm.Install('ycsb')
  vm.Install('mongosh')


def _GetMongoDbURL(benchmark_spec: bm_spec.BenchmarkSpec) -> str:
  """Returns the connection string used to connect to the instance."""
  instance: _ManagedMongoDb = benchmark_spec.non_relational_db
  read_preference = _READ_PREFERENCE.value
  write_concern = _WRITE_CONCERN.value
  return (
      f'"{instance.GetConnectionString()}'
      f'&readPreference={read_preference}'
      f'&w={write_concern}"'
  )


def _GetCommonYcsbArgs(benchmark_spec: bm_spec.BenchmarkSpec) -> dict[str, Any]:
  """Returns the args to use for YCSB."""
  instance: _ManagedMongoDb = benchmark_spec.non_relational_db
  args = {
      'mongodb.url': _GetMongoDbURL(benchmark_spec),
  }
  if instance.tls_enabled:
    jvm_args = instance.GetJvmTrustStoreArgs()
    if jvm_args:
      args['jvm-args'] = f'"{jvm_args}"'
  return args


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Prepare the clients and load the database."""
  clients = benchmark_spec.vm_groups['clients']
  db: _ManagedMongoDb = benchmark_spec.non_relational_db
  executor = ycsb.YCSBExecutor('mongodb', cp=ycsb.YCSB_DIR)

  if db.tls_enabled:
    db.SetupClientTls()

  background_tasks.RunThreaded(_PrepareClient, clients)

  if db.user_managed:
    logging.info('Skipping load for user managed cluster.')
    return []

  load_kwargs = _GetCommonYcsbArgs(benchmark_spec)
  load_kwargs.update({
      'mongodb.upsert': True,
      'core_workload_insertion_retry_limit': 10,
  })
  if _BATCH_SIZE.value and _BATCH_SIZE.value > 1:
    load_kwargs['batchsize'] = _BATCH_SIZE.value
  return executor.Load(clients, load_kwargs=load_kwargs)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Run YCSB against MongoDB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  clients = benchmark_spec.vm_groups['clients']
  instance: _ManagedMongoDb = benchmark_spec.non_relational_db

  run_kwargs = _GetCommonYcsbArgs(benchmark_spec)
  run_kwargs.update({
      'readallfields': True,
      'writeallfields': True,
  })
  executor = ycsb.YCSBExecutor('mongodb')
  samples = list(
      executor.Run(
          clients,
          run_kwargs=run_kwargs,
      )
  )
  for s in samples:
    s.metadata.update(instance.GetResourceMetadata())
    s.metadata['read_preference'] = _READ_PREFERENCE.value
    s.metadata['write_concern'] = _WRITE_CONCERN.value
  return samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  # Delete the clients first since deleting the database is sometimes slow.
  # Client delete will get retried in the teardown phase.
  clients = benchmark_spec.vm_groups['clients']
  background_tasks.RunThreaded(lambda vm: vm.Delete(), clients)
