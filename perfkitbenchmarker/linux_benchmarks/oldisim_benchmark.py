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

"""Runs oldisim.

oldisim is a framework to support benchmarks that emulate Online Data-Intensive
(OLDI) workloads, such as web search and social networking. oldisim includes
sample workloads built on top of this framework.

With its default config, oldisim models an example search topology. A user query
is first processed by a front-end server, which then eventually fans out the
query to a large number of leaf nodes. The latency is measured at the root of
the tree, and often increases with the increase of fan-out. oldisim reports a
scaling efficiency for a given topology. The scaling efficiency is defined
as queries per second (QPS) at the current fan-out normalized to QPS at fan-out
1 with ISO root latency.

Sample command line:

./pkb.py --benchmarks=oldisim --project='YOUR_PROJECT' --oldisim_num_leaves=4
--oldisim_fanout=1,2,3,4 --oldisim_latency_target=40
--oldisim_latency_metric=avg

The above command will build a tree with one root node and four leaf nodes. The
average latency target is 40ms. The root node will vary the fanout from 1 to 4
and measure the scaling efficiency.
"""

import logging
import re
import time

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

from perfkitbenchmarker.linux_packages import oldisim_dependencies

FLAGS = flags.FLAGS

flags.DEFINE_integer('oldisim_num_leaves', 4, 'number of leaf nodes',
                     lower_bound=1, upper_bound=64)
flags.DEFINE_list('oldisim_fanout', [],
                  'a list of fanouts to be tested. '
                  'a root can connect to a subset of leaf nodes (fanout). '
                  'the value of fanout has to be smaller than num_leaves.')
flags.DEFINE_enum('oldisim_latency_metric', 'avg',
                  ['avg', '50p', '90p', '95p', '99p', '99.9p'],
                  'Allowable metrics for end-to-end latency')
flags.DEFINE_float('oldisim_latency_target', '30', 'latency target in ms')

NUM_DRIVERS = 1
NUM_ROOTS = 1
BENCHMARK_NAME = 'oldisim'
BENCHMARK_CONFIG = """
oldisim:
  description: >
      Run oldisim. Specify the number of leaf
      nodes with --oldisim_num_leaves
  vm_groups:
    default:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
  """Decide number of vms needed to run oldisim."""
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  config['vm_groups']['default']['vm_count'] = (FLAGS.oldisim_num_leaves
                                                + NUM_DRIVERS + NUM_ROOTS)
  return config


def InstallAndBuild(vm):
  """Install and build oldisim on the target vm.

  Args:
    vm: A vm instance that runs oldisim.
  """
  logging.info('prepare oldisim on %s', vm)
  vm.Install('oldisim_dependencies')


def Prepare(benchmark_spec):
  """Install and build oldisim on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms

  leaf_vms = [vm for vm_idx, vm in enumerate(vms)
              if vm_idx >= (NUM_DRIVERS + NUM_ROOTS)]

  if vms:
    vm_util.RunThreaded(InstallAndBuild, vms)

  # Launch job on the leaf nodes.
  leaf_server_bin = oldisim_dependencies.BinaryPath('LeafNode')
  for vm in leaf_vms:
    leaf_cmd = '%s --threads=%s' % (leaf_server_bin, vm.num_cpus)
    vm.RemoteCommand('%s &> /dev/null &' % leaf_cmd)


def SetupRoot(root_vm, leaf_vms):
  """Connect a root node to a list of leaf nodes.

  Args:
    root_vm: A root vm instance.
    leaf_vms: A list of leaf vm instances.
  """
  fanout_args = ' '.join(['--leaf=%s' % i.internal_ip
                          for i in leaf_vms])
  root_server_bin = oldisim_dependencies.BinaryPath('ParentNode')
  root_cmd = '%s --threads=%s %s' % (root_server_bin, root_vm.num_cpus,
                                     fanout_args)
  logging.info('Root cmdline: %s', root_cmd)
  root_vm.RemoteCommand('%s &> /dev/null &' % root_cmd)


def ParseOutput(oldisim_output):
  """Parses the output from oldisim.

  Args:
    oldisim_output: A string containing the text of oldisim output.

  Returns:
    A tuple of (peak_qps, peak_lat, target_qps, target_lat).
  """

  re_peak = re.compile(r'peak qps = (?P<qps>\S+), latency = (?P<lat>\S+)')
  re_target = re.compile(r'measured_qps = (?P<qps>\S+), latency = (?P<lat>\S+)')

  for line in oldisim_output.splitlines():
    match = re.search(re_peak, line)
    if match:
      peak_qps = float(match.group('qps'))
      peak_lat = float(match.group('lat'))
      target_qps = float(peak_qps)
      target_lat = float(peak_lat)
      continue
    match = re.search(re_target, line)
    if match:
      target_qps = float(match.group('qps'))
      target_lat = float(match.group('lat'))
  return peak_qps, peak_lat, target_qps, target_lat


def RunLoadTest(benchmark_spec, fanout):
  """Run Loadtest for a given topology.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
    fanout: Request is first processed by a root node, which then
        fans out to a subset of leaf nodes.

  Returns:
    A tuple of (peak_qps, peak_lat, target_qps, target_lat).
  """
  assert fanout <= FLAGS.oldisim_num_leaves, (
      'The number of leaf nodes a root node connected to is defined by the '
      'flag fanout. Its current value %s is bigger than the total number of '
      'leaves %s.' % (fanout, FLAGS.oldisim_num_leaves))

  vms = benchmark_spec.vms
  driver_vms = []
  root_vms = []
  leaf_vms = []

  for vm_index, vm in enumerate(vms):
    if vm_index < NUM_DRIVERS:
      driver_vms.append(vm)
    elif vm_index < (NUM_DRIVERS + NUM_ROOTS):
      root_vms.append(vm)
    else:
      leaf_vms.append(vm)
  leaf_vms = leaf_vms[:fanout]

  for root_vm in root_vms:
    SetupRoot(root_vm, leaf_vms)

  driver_vm = driver_vms[0]
  driver_binary = oldisim_dependencies.BinaryPath('DriverNode')
  launch_script = oldisim_dependencies.Path('workloads/search/search_qps.sh')
  driver_args = ' '.join(['--server=%s' % i.internal_ip
                          for i in root_vms])
  # Make sure server is up.
  time.sleep(5)
  driver_cmd = '%s -s %s:%s -t 30 -- %s %s --threads=%s --depth=16' % (
      launch_script, FLAGS.oldisim_latency_metric, FLAGS.oldisim_latency_target,
      driver_binary, driver_args, driver_vm.num_cpus)
  logging.info('Driver cmdline: %s', driver_cmd)
  stdout, _ = driver_vm.RemoteCommand(driver_cmd, should_log=True)
  return ParseOutput(stdout)


def Run(benchmark_spec):
  """Run oldisim on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample objects.
  """
  results = []
  qps_dict = dict()
  vms = benchmark_spec.vms
  vm = vms[0]

  fanout_list = set([1, FLAGS.oldisim_num_leaves])
  for fanout in map(int, FLAGS.oldisim_fanout):
    if fanout > 1 and fanout < FLAGS.oldisim_num_leaves:
      fanout_list.add(fanout)

  metadata = {'num_cpus': vm.num_cpus}
  for fanout in sorted(fanout_list):
    qps = RunLoadTest(benchmark_spec, fanout)[2]
    qps_dict[fanout] = qps
    if fanout == 1:
      base_qps = qps
    name = 'Scaling efficiency of %s leaves' % fanout
    scaling_efficiency = round(min(qps_dict[fanout] / base_qps, 1), 2)
    results.append(sample.Sample(name, scaling_efficiency, '', metadata))

  return results


def Cleanup(benchmark_spec):  # pylint: disable=unused-argument
  """Cleanup oldisim on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  for vm_index, vm in enumerate(vms):
    if vm_index >= NUM_DRIVERS and vm_index < (NUM_DRIVERS + NUM_ROOTS):
      vm.RemoteCommand('sudo pkill ParentNode')
    elif vm_index >= (NUM_DRIVERS + NUM_ROOTS):
      vm.RemoteCommand('sudo pkill LeafNode')
