# Copyright 2015 Google Inc. All rights reserved.
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

"""Runs YCSB against Cassandra.

This benchmark runs two workloads against Cassandra using YCSB (the Yahoo! Cloud
Serving Benchmark).
Cassandra described in perfkitbenchmarker.packages.cassandra
YCSB and workloads described in perfkitbenchmarker.packages.ycsb.
"""

import functools
import logging
import os

from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages import cassandra
from perfkitbenchmarker.packages import ycsb

FLAGS = flags.FLAGS


BENCHMARK_INFO = {'name': 'cassandra_ycsb',
                  'description': 'Run YCSB against Cassandra. Specify the '
                  'Cassandra cluster size with --num_vms. Specify the number '
                  'of YCSB VMs with --ycsb_client_vms.',
                  'scratch_disk': True,
                  'num_machines': None}


# TODO: Add flags.
REPLICATION_FACTOR = 3
WRITE_CONSISTENCY = 'QUORUM'
READ_CONSISTENCY = 'QUORUM'
KEYSPACE_NAME = 'usertable'
COLUMN_FAMILY = 'data'

CREATE_TABLE_SCRIPT = 'cassandra/create-ycsb-table.cql.j2'


def GetInfo():
  info = BENCHMARK_INFO.copy()
  num_vms = max(FLAGS.num_vms, 3)
  info['num_machines'] = num_vms + FLAGS.ycsb_client_vms
  return info


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  if FLAGS['num_vms'].present and FLAGS.num_vms < 3:
    raise ValueError('cassandra_ycsb requires at least 3 Cassandra VMs.')
  cassandra.CheckPrerequisites()
  ycsb.CheckPrerequisites()
  data.ResourcePath(CREATE_TABLE_SCRIPT)


def _InstallCassandra(vm, seed_vms):
  """Install and start Cassandra on 'vm'."""
  vm.Install('cassandra')
  cassandra.Configure(vm, seed_vms=seed_vms)


def _CreateYCSBTable(vm, keyspace=KEYSPACE_NAME, column_family=COLUMN_FAMILY,
                     replication_factor=REPLICATION_FACTOR):
  """Creates a Cassandra table for use with YCSB."""
  template_path = data.ResourcePath(CREATE_TABLE_SCRIPT)
  remote_path = os.path.join(
      cassandra.CASSANDRA_DIR,
      os.path.basename(os.path.splitext(template_path)[0]))
  vm.RenderTemplate(template_path, remote_path,
                    context={'keyspace': keyspace,
                             'column_family': column_family,
                             'replication_factor': replication_factor})

  cassandra_cli = os.path.join(cassandra.CASSANDRA_DIR, 'bin', 'cassandra-cli')
  command = '{0} -f {1} -h {2}'.format(cassandra_cli, remote_path,
                                       vm.internal_ip)
  vm.RemoteCommand(command, should_log=True)


def _GetVMsByRole(vms):
  """Gets a dictionary mapping role to a list of VMs."""
  cassandra_vms = vms[:-FLAGS.ycsb_client_vms]
  return {'vms': vms,
          'cassandra_vms': cassandra_vms,
          'seed_vm': cassandra_vms[0],
          'non_seed_cassandra_vms': cassandra_vms[1:],
          'loaders': vms[-FLAGS.ycsb_client_vms:]}


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run YCSB against Cassandra.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  by_role = _GetVMsByRole(benchmark_spec.vms)

  loaders = by_role['loaders']
  assert loaders, vms

  # Cassandra cluster
  cassandra_vms = by_role['cassandra_vms']
  assert cassandra_vms, 'No Cassandra VMs: {0}'.format(by_role)
  seed_vm = by_role['seed_vm']
  assert seed_vm, 'No seed VM: {0}'.format(by_role)

  cassandra_install_fns = [functools.partial(_InstallCassandra,
                                             vm, seed_vms=[seed_vm])
                           for vm in cassandra_vms]
  ycsb_install_fns = [functools.partial(vm.Install, 'ycsb')
                      for vm in loaders]

  vm_util.RunThreaded(lambda f: f(), cassandra_install_fns + ycsb_install_fns)

  cassandra.StartCluster(seed_vm, by_role['non_seed_cassandra_vms'])

  _CreateYCSBTable(seed_vm)


def Run(benchmark_spec):
  """Spawn YCSB and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  vms = benchmark_spec.vms
  loaders = _GetVMsByRole(vms)['loaders']
  cassandra_vms = _GetVMsByRole(vms)['cassandra_vms']
  logging.debug('Loaders: %s', loaders)
  vms = benchmark_spec.vms

  executor = ycsb.YCSBExecutor(
      'cassandra-10',
      hosts=','.join(vm.internal_ip for vm in cassandra_vms))

  kwargs = {'hosts': ','.join(vm.internal_ip for vm in cassandra_vms),
            'columnfamily': COLUMN_FAMILY,
            'cassandra.readconsistencylevel': READ_CONSISTENCY,
            'cassandra.scanconsistencylevel': READ_CONSISTENCY,
            'cassandra.writeconsistencylevel': WRITE_CONSISTENCY,
            'cassandra.deleteconsistencylevel': WRITE_CONSISTENCY}

  metadata = {'ycsb_client_vms': FLAGS.ycsb_client_vms,
              'num_vms': len(cassandra_vms),
              'scratch_disk_type': FLAGS.scratch_disk_type,
              'scratch_disk_size': FLAGS.scratch_disk_size}

  samples = list(executor.LoadAndRun(loaders,
                                     load_kwargs=kwargs,
                                     run_kwargs=kwargs))

  for sample in samples:
    sample.metadata.update(metadata)

  return samples


def Cleanup(benchmark_spec):
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  cassandra_vms = _GetVMsByRole(benchmark_spec.vms)['cassandra_vms']
  vm_util.RunThreaded(cassandra.Stop, cassandra_vms)
  vm_util.RunThreaded(cassandra.CleanNode, cassandra_vms)
