# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run YCSB against MongoDB.

YCSB is a load generator for many 'cloud' databases. MongoDB is a NoSQL
database.

MongoDB homepage: http://www.mongodb.org/
YCSB homepage: https://github.com/brianfrankcooper/YCSB/wiki
"""

from collections.abc import Sequence
import functools
import json
import posixpath
from typing import Any
from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_virtual_machine
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import mongosh
from perfkitbenchmarker.linux_packages import ycsb

# See http://api.mongodb.org/java/2.13/com/mongodb/WriteConcern.html
flags.DEFINE_string(
    'mongodb_writeconcern', 'acknowledged', 'MongoDB write concern.'
)
flags.DEFINE_integer(
    'mongodb_readahead_kb', None, 'Configure block device readahead settings.'
)

_LinuxVM = linux_virtual_machine.BaseLinuxVirtualMachine


FLAGS = flags.FLAGS

BENCHMARK_NAME = 'mongodb_ycsb'
BENCHMARK_CONFIG = """
mongodb_ycsb:
  description: Run YCSB against a single MongoDB node.
  vm_groups:
    servers:
      vm_spec: *default_single_core
      disk_spec:
        GCP:
          disk_size: 500
          disk_type: pd-ssd
          mount_point: /scratch
        AWS:
          disk_size: 500
          disk_type: gp3
          mount_point: /scratch
        Azure:
          disk_size: 500
          disk_type: Premium_LRS
          mount_point: /scratch
      vm_count: 2
    arbiter:
      vm_spec: *default_single_core
      vm_count: 1
    clients:
      vm_spec: *default_single_core
      vm_count: 1
  flags:
    openjdk_version: 8
"""

_LinuxVM = linux_virtual_machine.BaseLinuxVirtualMachine


def GetConfig(user_config: dict[str, Any]) -> dict[str, Any]:
  """Validates the user config dictionary."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['clients']['vm_count'] = FLAGS.ycsb_client_vms
  server_count = config['vm_groups']['servers']['vm_count']
  arbiter_count = config['vm_groups']['arbiter']['vm_count']
  if server_count != 2:
    raise errors.Config.InvalidValue(
        'Servers VM count must be 2, one for primary and one for secondary.'
        f' Got {server_count}.'
    )
  if arbiter_count != 1:
    raise errors.Config.InvalidValue(
        f'Arbiter VM count must be 1, got {arbiter_count}.'
    )
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['clients']['vm_count'] = FLAGS.ycsb_client_vms
  return config


def _GetDataDir(vm: _LinuxVM) -> str:
  return posixpath.join(vm.GetScratchDir(), 'mongodb-data')


def _PrepareServer(vm: _LinuxVM) -> None:
  """Installs MongoDB on the server."""
  vm.Install('mongodb_server')
  vm.Install('mongosh')

  data_dir = _GetDataDir(vm)
  vm.RemoteCommand(f'sudo rm -rf {data_dir}')
  vm.RemoteCommand(f'mkdir {data_dir} && chmod a+rwx {data_dir}')
  vm.RemoteCommand(
      f'sudo sed -i "s|dbPath:.*|dbPath: {data_dir}|"'
      f' {vm.GetPathToConfig("mongodb_server")}'
  )

  if FLAGS.mongodb_readahead_kb is not None:
    vm.SetReadAhead(
        FLAGS.mongodb_readahead_kb * 2,
        [d.GetDevicePath() for d in vm.scratch_disks],
    )

  # Too many connections fails if we don't set file descriptor limit higher.
  vm.RemoteCommand('ulimit -n 64000 && sudo systemctl start mongod')


def _PrepareArbiter(vm: _LinuxVM) -> None:
  """Installs MongoDB on the arbiter."""
  vm.Install('mongodb_server')
  vm.Install('mongosh')
  vm.RemoteCommand('ulimit -n 64000 && sudo systemctl start mongod')


def _PrepareReplicaSet(
    server_vms: Sequence[_LinuxVM], arbiter_vm: _LinuxVM
) -> None:
  """Prepares the replica set for the benchmark.

  This benchmark currently uses a primary-secondary-arbiter replica set
  configuration. The secondary keeps a full replica of the data while the
  arbiter does not. The arbiter is still able to vote. See
  https://www.mongodb.com/docs/manual/core/replica-set-architecture-three-members
  for more information.

  Args:
    server_vms: The primary (index 0) and secondary (index 1) server VMs to use.
    arbiter_vm: The arbiter VM to use.
  """
  args = {
      '_id': '"rs0"',
      'members': [
          {
              '_id': 0,
              'host': f'"{server_vms[0].internal_ip}:27017"',
          },
          {
              '_id': 1,
              'host': f'"{server_vms[1].internal_ip}:27017"',
          },
          {
              '_id': 2,
              'host': f'"{arbiter_vm.internal_ip}:27017"',
              'arbiterOnly': True,
          },
      ],
  }
  mongosh.RunCommand(server_vms[0], f'rs.initiate({json.dumps(args)})')
  mongosh.RunCommand(server_vms[0], 'rs.conf()')


def _PrepareClient(vm: _LinuxVM) -> None:
  """Install YCSB on the client VM."""
  vm.Install('ycsb')
  vm.Install('mongosh')
  # Disable logging for MongoDB driver, which is otherwise quite verbose.
  log_config = """<configuration><root level="WARN"/></configuration>"""

  vm.RemoteCommand(
      "echo '{0}' > {1}/logback.xml".format(log_config, ycsb.YCSB_DIR)
  )


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Install MongoDB on one VM and YCSB on another.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """
  servers = benchmark_spec.vm_groups['servers']
  arbiter = benchmark_spec.vm_groups['arbiter'][0]
  clients = benchmark_spec.vm_groups['clients']
  server_partials = [
      functools.partial(_PrepareServer, mongo_vm) for mongo_vm in servers
  ]
  arbiter_partial = [functools.partial(_PrepareArbiter, arbiter)]
  client_partials = [
      functools.partial(_PrepareClient, client) for client in clients
  ]
  background_tasks.RunThreaded(
      (lambda f: f()), server_partials + arbiter_partial + client_partials
  )

  _PrepareReplicaSet(servers, arbiter)

  benchmark_spec.executor = ycsb.YCSBExecutor('mongodb', cp=ycsb.YCSB_DIR)
  server = benchmark_spec.vm_groups['servers'][0]
  benchmark_spec.mongodb_url = f'mongodb://{server.internal_ip}:27017/'


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Run YCSB with against MongoDB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  kwargs = {
      'mongodb.url': benchmark_spec.mongodb_url,
      'mongodb.writeConcern': FLAGS.mongodb_writeconcern,
      # Avoids some stale connection issues at the beginning of the load.
      'core_workload_insertion_retry_limit': 10,
  }
  samples = list(
      benchmark_spec.executor.LoadAndRun(
          benchmark_spec.vm_groups['clients'],
          load_kwargs=kwargs,
          run_kwargs=kwargs,
      )
  )
  if FLAGS.mongodb_readahead_kb is not None:
    for s in samples:
      s.metadata['readahdead_kb'] = FLAGS.mongodb_readahead_kb
  return samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Remove MongoDB and YCSB.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
      required to run the benchmark.
  """

  def CleanupServer(server: _LinuxVM) -> None:
    server.RemoteCommand(
        'sudo service %s stop' % server.GetServiceName('mongodb_server')
    )
    server.RemoteCommand('rm -rf %s' % _GetDataDir(server))

  CleanupServer(benchmark_spec.vm_groups['workers'][0])
