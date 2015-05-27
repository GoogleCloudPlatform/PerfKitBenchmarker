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

"""Runs terasort on hadoop.

Cluster Setup:
http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html

TODO(user): Make hadoop scale when the number of nodes changes. Also
investigate other settings and verfiy that we are seeing good performance.
"""

import logging
import posixpath
import time

from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.packages import hadoop

flags.DEFINE_integer('terasort_num_rows', 100000000,
                     'Number of 100-byte rows used in terasort.')

FLAGS = flags.FLAGS

BENCHMARK_INFO = {'name': 'hadoop_terasort',
                  'description': 'Runs Terasort. Control the number of VMs '
                  'with --num_vms.',
                  'scratch_disk': True,
                  'num_machines': 9}

NUM_BYTES_PER_ROW = 100
NUM_MB_PER_ROW = NUM_BYTES_PER_ROW / (1024.0 ** 2)


def GetInfo():
  info = BENCHMARK_INFO.copy()
  if FLAGS['num_vms'].present:
    info['num_machines'] = FLAGS.num_vms
  return info


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  hadoop.CheckPrerequisites()


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run hadoop.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  master = vms[0]
  workers = vms[1:]

  def InstallHadoop(vm):
    vm.Install('hadoop')
  vm_util.RunThreaded(InstallHadoop, vms)
  hadoop.ConfigureAndStart(master, workers)


def Run(benchmark_spec):
  """Spawn hadoop and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  vms = benchmark_spec.vms
  master = vms[0]

  mapreduce_example_jar = posixpath.join(
      hadoop.HADOOP_DIR, 'share', 'hadoop', 'mapreduce',
      'hadoop-mapreduce-examples-{0}.jar'.format(hadoop.HADOOP_VERSION))
  hadoop_cmd = '{0} jar {1}'.format(
      posixpath.join(hadoop.HADOOP_BIN, 'yarn'),
      mapreduce_example_jar)
  master.RemoteCommand('{0} teragen {1} /teragen'.format(
      hadoop_cmd, FLAGS.terasort_num_rows))
  num_cpus = sum(vm.num_cpus for vm in vms[1:])
  start_time = time.time()
  stdout, _ = master.RemoteCommand(hadoop_cmd + ' terasort /teragen /terasort')
  logging.info('Terasort output: %s', stdout)
  time_elapsed = time.time() - start_time
  data_processed_in_mbytes = FLAGS.terasort_num_rows * NUM_MB_PER_ROW
  master.RemoteCommand(hadoop_cmd + ' teravalidate /terasort /teravalidate')

  # Clean up
  master.RemoteCommand(
      '{0} dfs -rm -r -f /teragen /teravalidate /terasort'.format(
          posixpath.join(hadoop.HADOOP_BIN, 'hdfs')))

  metadata = {'num_rows': FLAGS.terasort_num_rows,
              'data_size_in_bytes': FLAGS.terasort_num_rows * NUM_BYTES_PER_ROW,
              'num_vms': len(vms)}
  return [sample.Sample('Terasort Throughput Per Core',
                        data_processed_in_mbytes / time_elapsed / num_cpus,
                        'MB/sec',
                        metadata),
          sample.Sample('Terasort Total Time', time_elapsed, 'sec', metadata)]


def Cleanup(benchmark_spec):
  """Uninstall packages required for Hadoop and remove benchmark files.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  master = vms[0]
  workers = vms[1:]

  logging.info('Stopping Hadoop.')
  hadoop.StopAll(master)
  vm_util.RunThreaded(hadoop.CleanDatanode, workers)
