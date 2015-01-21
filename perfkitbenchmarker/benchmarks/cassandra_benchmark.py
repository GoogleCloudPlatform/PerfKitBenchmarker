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

"""Runs cassandra.

Cassandra homepage: http://cassandra.apache.org
"""

import logging
import math
import os.path
import re
import time


from perfkitbenchmarker import benchmark_spec as bs
from perfkitbenchmarker import data
from perfkitbenchmarker import disk
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util


flags.DEFINE_integer('num_keys', 0,
                     'Number of keys used in cassandra-stress tool.')


FLAGS = flags.FLAGS

DEFAULT_CLUSTER_SIZE = 4

# Disks and machines are set in config file.
BENCHMARK_INFO = {'name': 'cassandra',
                  'description': 'Run Cassandra',
                  'scratch_disk': False,
                  'num_machines': DEFAULT_CLUSTER_SIZE}

CASSANDRA_DIR = 'dsc-cassandra-2.0.0'
CASSANDRA_TAR = '%s-bin.tar.gz' % CASSANDRA_DIR
JAVA_TAR = 'server-jre-7u40-linux-x64.tar.gz'
CASSANDRA_YAML = 'cassandra.yaml'
CASSANDRA_PID = 'cassandra_pid'

REQUIRED_JAVA_VERSION = '1.7.0_40'

LOADER_NODE = 'loader'
DATA_NODE = 'cas'
WAITING_IN_SECONDS = 30
SLEEP_BETWEEN_CHECK_IN_SECONDS = 5

# Stress test options.
CONSISTENCY_LEVEL = 'quorum'
REPLICATION_FACTOR = '3'
RETRIES = '1000'
THREADS = '30'
MAX_RETRY_START_CLUSTER = 5
NUM_KEYS_PER_CORE = 10000000


def GetInfo():
  return BENCHMARK_INFO


def UnpackCassandra(vm):
  """Unpacking cassandra on target vm.

  Args:
    vm: The target vm.
  """
  vm.RemoteCommand('tar -xzmf ./%s' % CASSANDRA_TAR)


def PrepareVm(vm):
  """Preparing data files and Java on target vm.

  Args:
    vm: The target vm.
  """
  vm.RemoteCommand('curl -OL http://downloads.datastax.com/community/%s'
                   % CASSANDRA_TAR)
  try:
    vm.PrepareJava(JAVA_TAR, REQUIRED_JAVA_VERSION)
  except data.ResourceNotFound:
    raise errors.Benchmarks.PrepareException('%s not found' % JAVA_TAR)


def Prepare(benchmark_spec):
  """Install Cassandra and Java on target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vm_dict = benchmark_spec.vm_dict
  logging.info('VM dictionary %s', vm_dict)

  if vm_dict['default']:
    logging.info('No config file is provided, use default settings: '
                 '1 loader node, 3 data nodes')
    vm_dict[LOADER_NODE] = [vm_dict['default'][-1]]
    vm_dict[DATA_NODE] = vm_dict['default'][:3]
    disk_spec = disk.BaseDiskSpec(
        500, bs.DISK_TYPE[benchmark_spec.cloud][bs.STANDARD],
        '/cassandra_data')
    for vm in vm_dict[DATA_NODE]:
      vm.CreateScratchDisk(disk_spec)

  logging.info('Authorizing loader[0] permission to access all other vms.')
  vm_dict[LOADER_NODE][0].AuthenticateVm()

  logging.info('Preparing data files and Java on all vms.')
  vm_util.RunThreaded(PrepareVm, benchmark_spec.vms)


def AbandonPastDeployment(vm):
  """Abandon past deployment (if any) and remove files.

  Args:
    vm: The target vm.
  """
  vm.RemoteCommand(
      'sudo kill -9 `ps -e | grep java | awk \'{print $1;}\'`',
      ignore_failure=True)
  vm.RemoveFile('/tmp/*')
  vm.RemoveFile('%s/*' % vm.GetScratchDir())


def ConfigureCassandraEnvScript(vm):
  """Configuring cassandra-env.sh on target vm.

  Args:
    vm: The target vm.
  """
  vm.RemoteCommand('sudo chmod 755 %s' % CASSANDRA_DIR)
  vm.RemoteCommand('chmod +w %s/conf/cassandra-env.sh' %
                   CASSANDRA_DIR)
  context = {'ip_address': vm.internal_ip}

  file_path = data.ResourcePath('cassandra-env.sh.j2')

  vm.RenderTemplate(file_path,
                    os.path.join(CASSANDRA_DIR, 'conf', 'cassandra-env.sh'),
                    context=context)


def GenerateCassandraYaml(vm, seed_vm):
  """Generate cassandra.yaml file on target vm.

  Args:
    vm: The target vm.
    seed_vm: The first vm in data nodes.
  """
  context = {'cluster_name': 'cassandracloudbenchmark',
             'data_path': vm.GetScratchDir(),
             'seeds': seed_vm.internal_ip,
             'concurrent_writes': vm.num_cpus * 8,
             'eth0_address': vm.internal_ip}

  file_path = data.ResourcePath('cassandra.yaml.j2')
  vm.RenderTemplate(file_path,
                    os.path.join(CASSANDRA_DIR, 'conf', 'cassandra.yaml'),
                    context=context)


def AdjustJNALocation(vm):
  """Adjusting the location of Java Native Access library on vm.

  Args:
    vm: The target vm.
  """
  vm.RemoteCommand('[ ! -f %s/lib/jna.jar ] && sudo ln -s '
                   '/usr/share/java/jna.jar %s/lib ' % (
                       CASSANDRA_DIR, CASSANDRA_DIR), ignore_failure=True)


def StartCassandraOnDataNodes(vm):
  """Start Cassandra on data nodes.

  Args:
    vm: The target vm.
  """
  vm.RemoteCommand(
      'sudo nohup %s/bin/cassandra -p "./%s" 1> out 2> err & ' % (
          CASSANDRA_DIR, CASSANDRA_PID))


def StartCassandraDataNodeServer(vm, seed_vm):
  """Setting Cassandra up.

  Args:
    vm: The target vm.
    seed_vm: The first vm in data nodes.
  """
  ConfigureCassandraEnvScript(vm)
  GenerateCassandraYaml(vm, seed_vm)
  AdjustJNALocation(vm)
  StartCassandraOnDataNodes(vm)


def VerifyCluster(benchmark_spec, vm):
  """Verify the cluster is up.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
    vm: The target vm.

  Returns:
    A boolean indicates the cluster is ready.
  """
  logging.info('Verify Cluster')
  num_expected_data_nodes = len(benchmark_spec.vm_dict[DATA_NODE])
  ret, _ = vm.RemoteCommand(
      '%s/bin/nodetool status | grep UN | wc -l' % CASSANDRA_DIR)
  num_actual_data_nodes = int(ret)
  if num_actual_data_nodes != num_expected_data_nodes:
    logging.warning('Expecting %s data nodes, but only %s are up.',
                    num_expected_data_nodes,
                    num_actual_data_nodes)
  return num_actual_data_nodes == num_expected_data_nodes


def VerifyNode(vm, seed_vm):
  """Verify Cassandra is setup on the target vm.

  Verify Cassandra is setup on the target vm.  If not, try to start Cassandra
  on the VM again.

  Args:
    vm: The target vm.
    seed_vm: The first vm in data nodes.
  """
  ret, _ = vm.RemoteCommand(
      'ps -e | grep java | awk "{print $1}"')
  if not ret:
    logging.warning('Restarting Cassandra on %s', vm.hostname)
    StartCassandraDataNodeServer(vm, seed_vm)


def RunTestOnLoader(vm, data_nodes_ip_addresses):
  """Running Cassandra-stress test on loader node.

  Cassandra-stress tool page:
  http://www.datastax.com/documentation/cassandra/2.0/cassandra/tools/toolsCStress_t.html

  Args:
    vm: The target vm.
    data_nodes_ip_addresses: Ip addresses for all data nodes seperated by ','.
  """
  vm.RemoteCommand(
      './%s/tools/bin/cassandra-stress '
      '--file "./%s.results" --nodes %s '
      '--replication-factor %s --consistency-level %s '
      '--num-keys %s -K %s -t %s' % (
          CASSANDRA_DIR, vm.hostname, data_nodes_ip_addresses,
          REPLICATION_FACTOR, CONSISTENCY_LEVEL,
          FLAGS.num_keys, RETRIES, THREADS), ignore_failure=True)


def InsertTest(benchmark_spec, vm):
  """Start Cassandra test.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
    vm: The target vm.
  """
  logging.info('Creating Keyspace.')
  data_nodes_ip_addresses = [
      data_vm.internal_ip
      for data_vm in benchmark_spec.vm_dict[DATA_NODE]]
  data_nodes_ip_addresses = ','.join(data_nodes_ip_addresses)
  vm.RemoteCommand(
      './%s/tools/bin/cassandra-stress '
      '--nodes %s --replication-factor %s '
      '--consistency-level %s --num-keys 1 > /dev/null' % (
          CASSANDRA_DIR, data_nodes_ip_addresses,
          REPLICATION_FACTOR, CONSISTENCY_LEVEL))
  logging.info('Waiting %s for keyspace to propagate.', WAITING_IN_SECONDS)
  time.sleep(WAITING_IN_SECONDS)
  if not FLAGS.num_keys:
    logging.info('Num keys not set, using %s*num_cpu in stress test.',
                 NUM_KEYS_PER_CORE)
    FLAGS.num_keys = str(
        NUM_KEYS_PER_CORE * benchmark_spec.vm_dict[DATA_NODE][0].num_cpus)

  logging.info('Executing the benchmark.')
  args = [((loader_vm, data_nodes_ip_addresses), {})
          for loader_vm in benchmark_spec.vm_dict[LOADER_NODE]]
  vm_util.RunThreaded(RunTestOnLoader, args)


def WaitLoaderForFinishing(vm):
  """Watch loader node and wait it for finishing test.

  Args:
    vm: The target vm.
  """
  while True:
    resp, _ = vm.RemoteCommand('tail -n 1 *results')
    if re.findall(r'END', resp):
      break
    if re.findall(r'FAILURE', resp):
      vm.PullFile(vm_util.GetTempDir(), '*results')
      raise errors.Benchmarks.RunError(
          'cassandra-stress tool failed, check %s/ for details.'
          % vm_util.GetTempDir())
    time.sleep(SLEEP_BETWEEN_CHECK_IN_SECONDS)


def CollectResultFile(vm, interval_op_rate_list, interval_key_rate_list,
                      latency_median_list, latency_95th_list,
                      latency_99_9th_list,
                      total_operation_time_list):
  """Collect result file on vm.

  Args:
    vm: The target vm.
    interval_op_rate_list: The list stores interval_op_rate.
    interval_key_rate_list: The list stores interval_key_rate.
    latency_median_list: The list stores latency median.
    latency_95th_list: The list stores latency 95th percentile.
    latency_99_9th_list: The list stores latency 99.9th percentile.
    total_operation_time_list: The list stores total operation time.
  """
  vm.PullFile(vm_util.GetTempDir(), '*results')
  resp, _ = vm.RemoteCommand('tail *results')
  match = re.findall(r'[\w\t ]: +([\d\.:]+)', resp)
  interval_op_rate_list.append(int(match[0]))
  interval_key_rate_list.append(int(match[1]))
  latency_median_list.append(float(match[2]))
  latency_95th_list.append(float(match[3]))
  latency_99_9th_list.append(float(match[4]))
  raw_time_data = match[5].split(':')
  total_operation_time_list.append(
      int(raw_time_data[0]) * 3600 + int(raw_time_data[1]) * 60 + int(
          raw_time_data[2]))


def InitializeCurrentDeployment(benchmark_spec):
  """Abandon previous deployment (if any).

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
  """
  logging.info('Unpacking cassandra on all vms.')
  vm_util.RunThreaded(UnpackCassandra, benchmark_spec.vms)
  logging.info('Killing past deployments.')
  vm_util.RunThreaded(AbandonPastDeployment,
                      benchmark_spec.vm_dict[DATA_NODE])


def StartCassandraServers(benchmark_spec):
  """Start all data nodes as Cassandra servers.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
  """
  vm_dict = benchmark_spec.vm_dict
  logging.info('Setting Cassandra up.')
  args = [((vm, vm_dict[DATA_NODE][0]), {}) for vm in vm_dict[DATA_NODE]]
  vm_util.RunThreaded(StartCassandraDataNodeServer, args)
  logging.info('Verifying cluster.')
  cluster_ready = VerifyCluster(benchmark_spec,
                                vm_dict[DATA_NODE][0])
  retry_count = 0
  while not cluster_ready and retry_count < MAX_RETRY_START_CLUSTER:
    retry_count += 1
    args = [((vm, vm_dict[DATA_NODE][0]), {}) for vm in vm_dict[DATA_NODE]]
    vm_util.RunThreaded(VerifyNode, args)
    logging.info('Wait %s seconds for retried node.',
                 WAITING_IN_SECONDS)
    time.sleep(WAITING_IN_SECONDS)
    cluster_ready = VerifyCluster(benchmark_spec,
                                  vm_dict[DATA_NODE][0])

  if not cluster_ready:
    raise errors.Benchmarks.RunError(
        'Failed start cluster after %s retries.' % MAX_RETRY_START_CLUSTER)
  logging.info('Cluster ready.')


def RunCassandraStressTest(benchmark_spec):
  """Start all loader nodes as Cassandra clients and run stress test.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
  """
  InsertTest(benchmark_spec, benchmark_spec.vm_dict[LOADER_NODE][0])
  logging.info('Tests running. Watching progress.')
  vm_util.RunThreaded(WaitLoaderForFinishing,
                      benchmark_spec.vm_dict[LOADER_NODE])


def CollectResults(benchmark_spec):
  """Collect and parse test results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  logging.info('Gathering results.')
  vm_dict = benchmark_spec.vm_dict
  interval_op_rate_list = []
  interval_key_rate_list = []
  latency_median_list = []
  latency_95th_list = []
  latency_99_9th_list = []
  total_operation_time_list = []
  args = [((vm, interval_op_rate_list, interval_key_rate_list,
            latency_median_list, latency_95th_list,
            latency_99_9th_list,
            total_operation_time_list), {}) for vm in vm_dict[LOADER_NODE]]
  vm_util.RunThreaded(CollectResultFile, args)
  results = []
  metadata = {'num_keys': FLAGS.num_keys,
              'num_data_nodes': len(vm_dict[DATA_NODE]),
              'num_loader_nodes': len(vm_dict[LOADER_NODE])}
  results.append(['Interval_op_rate', math.fsum(interval_op_rate_list),
                  'operations per second', metadata])
  results.append(['Interval_key_rate', math.fsum(interval_key_rate_list),
                  'operations per second', metadata])
  results.append(['Latency median',
                  math.fsum(latency_median_list) / len(vm_dict[LOADER_NODE]),
                  'ms', metadata])
  results.append(['Latency 95th percentile',
                  math.fsum(latency_95th_list) / len(vm_dict[LOADER_NODE]),
                  'ms', metadata])
  results.append(['Latency 99.9th percentile',
                  math.fsum(latency_99_9th_list) / len(vm_dict[LOADER_NODE]),
                  'ms', metadata])
  results.append(['Total operation time',
                  math.fsum(total_operation_time_list) / len(
                      vm_dict[LOADER_NODE]), 'seconds', metadata])
  return results


def Run(benchmark_spec):
  """Run Cassandra on target vms.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  InitializeCurrentDeployment(benchmark_spec)
  StartCassandraServers(benchmark_spec)
  RunCassandraStressTest(benchmark_spec)
  return CollectResults(benchmark_spec)


def CleanupVm(vm):
  """Cleanup Cassandra on the target vm.

  Args:
    vm: The target vm.
  """
  vm.RemoteCommand('rm -rf %s' % CASSANDRA_DIR, ignore_failure=True)
  vm.RemoteCommand('rm -rf /usr/lib/jvm', ignore_failure=True)
  vm.RemoteCommand('rm %s' % CASSANDRA_TAR, ignore_failure=True)
  vm.RemoteCommand('rm %s' % JAVA_TAR, ignore_failure=True)


def Cleanup(benchmark_spec):
  """Cleanup function.

  Args:
    benchmark_spec: The benchmark specification. Contains all data
        that is required to run the benchmark.
  """
  vm_dict = benchmark_spec.vm_dict
  vm_util.RunThreaded(AbandonPastDeployment, vm_dict[DATA_NODE])
  vm_util.RunThreaded(CleanupVm, benchmark_spec.vms)
