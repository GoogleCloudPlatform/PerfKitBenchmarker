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

"""Runs YCSB against HBase.


HBase is a scalable NoSQL database built on Hadoop.
https://hbase.apache.org/

A running installation consists of:
  * An HDFS NameNode.
  * HDFS DataNodes.
  * An HBase master node.
  * HBase regionservers.
  * A zookeeper cluster (https://zookeeper.apache.org/).

See: http://hbase.apache.org/book.html#_distributed.

This benchmark provisions:
  * A single node functioning as HDFS NameNode, HBase master, and zookeeper
    quorum member.
  * '--num_vms - 1' nodes serving as both HDFS DataNodes and HBase region
    servers (so region servers and data are co-located).
By default only the master node runs Zookeeper. Some regionservers may be added
to the zookeeper quorum with the --hbase_zookeeper_nodes flag.


HBase web UI on 15030.
HDFS web UI on  50070.
"""

import functools
import logging
import os
import posixpath

from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import hadoop
from perfkitbenchmarker.linux_packages import hbase
from perfkitbenchmarker.linux_packages import ycsb

FLAGS = flags.FLAGS

flags.DEFINE_integer('hbase_zookeeper_nodes', 1, 'Number of Zookeeper nodes.')
flags.DEFINE_boolean('hbase_use_snappy', True,
                     'Whether to use snappy compression.')

BENCHMARK_NAME = 'hbase_ycsb'
BENCHMARK_CONFIG = """
hbase_ycsb:
  description: >
      Run YCSB against HBase. Specify the HBase
      cluster size with --num_vms. Specify the number of YCSB VMs
      with --ycsb_client_vms.
  vm_groups:
    clients:
      vm_spec: *default_single_core
    master:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
    workers:
      vm_spec: *default_single_core
      disk_spec: *default_500_gb
"""

HBASE_SITE = 'hbase-site.xml'

CREATE_TABLE_SCRIPT = 'hbase/create-ycsb-table.hbaseshell.j2'
TABLE_NAME = 'usertable'
COLUMN_FAMILY = 'cf'
TABLE_SPLIT_COUNT = 200


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  num_vms = max(FLAGS.num_vms, 2)
  if FLAGS['num_vms'].present and FLAGS.num_vms < 2:
    raise ValueError('hbase_ycsb requires at least 2 HBase VMs.')
  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['clients']['vm_count'] = FLAGS.ycsb_client_vms
  if FLAGS['num_vms'].present:
    config['vm_groups']['workers']['vm_count'] = num_vms - 1
  return config


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  hbase.CheckPrerequisites()
  hadoop.CheckPrerequisites()
  ycsb.CheckPrerequisites()


def CreateYCSBTable(vm, table_name=TABLE_NAME, family=COLUMN_FAMILY,
                    n_splits=TABLE_SPLIT_COUNT, limit_filesize=True,
                    use_snappy=True):
  """Create a table for use with YCSB.

  Args:
    vm: Virtual machine from which to create the table.
    table_name: Name for the table.
    family: Column family name.
    limit_filesize: boolean. Should the filesize be limited to 4GB?
    n_splits: Initial number of regions for the table. Default follows
      HBASE-4163.
  """
  # See: https://issues.apache.org/jira/browse/HBASE-4163
  template_path = data.ResourcePath(CREATE_TABLE_SCRIPT)
  remote = posixpath.join(hbase.HBASE_DIR,
                          os.path.basename(os.path.splitext(template_path)[0]))
  vm.RenderTemplate(template_path, remote,
                    context={'table_name': table_name,
                             'family': family,
                             'limit_filesize': limit_filesize,
                             'n_splits': n_splits,
                             'use_snappy': use_snappy})
  # TODO(connormccoy): on HBase update, add '-n' flag.
  command = "{0}/hbase shell {1}".format(hbase.HBASE_BIN, remote)
  vm.RemoteCommand(command, should_log=True)


def _GetVMsByRole(vm_groups):
  """Partition "vms" by role in the benchmark.

  * The first VM is the master.
  * The first FLAGS.hbase_zookeeper_nodes form the Zookeeper quorum.
  * The last FLAGS.ycsb_client_vms are loader nodes.
  * The nodes which are neither the master nor loaders are HBase region servers.

  Args:
    vm_groups: The benchmark_spec's vm_groups dict.

  Returns:
    A dictionary with keys 'vms', 'hbase_vms', 'master', 'zk_quorum', 'workers',
    and 'clients'.
  """
  hbase_vms = vm_groups['master'] + vm_groups['workers']
  vms = hbase_vms + vm_groups['clients']
  return {'vms': vms,
          'hbase_vms': hbase_vms,
          'master': vm_groups['master'][0],
          'zk_quorum': hbase_vms[:FLAGS.hbase_zookeeper_nodes],
          'workers': vm_groups['workers'],
          'clients': vm_groups['clients']}


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run hadoop.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  by_role = _GetVMsByRole(benchmark_spec.vm_groups)

  loaders = by_role['clients']
  assert loaders, 'No loader VMs: {0}'.format(by_role)

  # HBase cluster
  hbase_vms = by_role['hbase_vms']
  assert hbase_vms, 'No HBase VMs: {0}'.format(by_role)
  master = by_role['master']
  zk_quorum = by_role['zk_quorum']
  assert zk_quorum, 'No zookeeper quorum: {0}'.format(by_role)
  workers = by_role['workers']
  assert workers, 'No workers: {0}'.format(by_role)

  hbase_install_fns = [functools.partial(vm.Install, 'hbase')
                       for vm in hbase_vms]
  ycsb_install_fns = [functools.partial(vm.Install, 'ycsb')
                      for vm in loaders]

  vm_util.RunThreaded(lambda f: f(), hbase_install_fns + ycsb_install_fns)

  hadoop.ConfigureAndStart(master, workers, start_yarn=False)
  hbase.ConfigureAndStart(master, workers, zk_quorum)

  CreateYCSBTable(master, use_snappy=FLAGS.hbase_use_snappy)

  # Populate hbase-site.xml on the loaders.
  master.PullFile(
      vm_util.GetTempDir(),
      posixpath.join(hbase.HBASE_CONF_DIR, HBASE_SITE))

  def PushHBaseSite(vm):
    conf_dir = posixpath.join(ycsb.YCSB_DIR, 'hbase-binding', 'conf')
    vm.RemoteCommand('mkdir -p {}'.format(conf_dir))
    vm.PushFile(
        os.path.join(vm_util.GetTempDir(), HBASE_SITE),
        posixpath.join(conf_dir, HBASE_SITE))

  vm_util.RunThreaded(PushHBaseSite, loaders)


def Run(benchmark_spec):
  """Spawn YCSB and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  by_role = _GetVMsByRole(benchmark_spec.vm_groups)
  loaders = by_role['clients']
  logging.info('Loaders: %s', loaders)

  executor = ycsb.YCSBExecutor('hbase-10')

  metadata = {'ycsb_client_vms': len(loaders),
              'hbase_cluster_size': len(by_role['hbase_vms']),
              'hbase_zookeeper_nodes': FLAGS.hbase_zookeeper_nodes}

  # By default YCSB uses a BufferedMutator for Puts / Deletes.
  # This leads to incorrect update latencies, since since the call returns
  # before the request is acked by the server.
  # Disable this behavior during the benchmark run.
  run_kwargs = {'columnfamily': COLUMN_FAMILY,
                'clientbuffering': 'false'}
  load_kwargs = run_kwargs.copy()

  # During the load stage, use a buffered mutator with a single thread.
  # The BufferedMutator will handle multiplexing RPCs.
  load_kwargs['clientbuffering'] = 'true'
  if not FLAGS['ycsb_preload_threads'].present:
    load_kwargs['threads'] = 1
  samples = list(executor.LoadAndRun(loaders,
                                     load_kwargs=load_kwargs,
                                     run_kwargs=run_kwargs))
  for sample in samples:
    sample.metadata.update(metadata)

  return samples


def Cleanup(benchmark_spec):
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  by_role = _GetVMsByRole(benchmark_spec.vm_groups)
  hbase.Stop(by_role['master'])
  hadoop.StopHDFS(by_role['master'])
  vm_util.RunThreaded(hadoop.CleanDatanode, by_role['workers'])
