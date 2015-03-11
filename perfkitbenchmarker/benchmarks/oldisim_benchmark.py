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

./pkb.py --benchmarks=oldisim --project='YOUR_PROJECT' --num_leaves=4
--fanout=1,2,3,4 --latency_target=40 --latency_metric=avg

The above command will build a tree with one root node and four leave nodes. The
average latency target is 40ms. The root node will vary the fanout from 1 to 4
and measure the scaling efficiency.
"""

import logging
import os
import re
import time

from perfkitbenchmarker import data
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util


FLAGS = flags.FLAGS

flags.DEFINE_integer('num_leaves', 4, 'number of leaf nodes',
                     lower_bound=1, upper_bound=64)
flags.DEFINE_list('fanout', [],
                  'a list of fanouts to be tested. '
                  'a root can connect to a subset of leaf nodes (fanout). '
                  'the value of fanout has to be smaller than num_leaves.')
flags.DEFINE_enum('latency_metric', 'avg',
                  ['avg', '50p', '90p', '95p', '99p', '99.9p'],
                  'Allowable metrics for end-to-end latency')
flags.DEFINE_float('latency_target', '30', 'latency target in ms')

BENCHMARK_INFO = {'name': 'oldisim',
                  'description': 'Run oldisim',
                  'scratch_disk': False,
                  'num_machines': None}  # Set in GetInfo()
OLDISIM_GIT = 'https://github.com/GoogleCloudPlatform/oldisim.git'
NUM_DRIVERS = 1
NUM_ROOTS = 1
OLDISIM_DIR = 'oldisim'
OLDISIM_TAR = 'oldisim.tar.gz'
BINARY_BASE = 'release/workloads/search'


def GetInfo():
  """Decide number of vms needed to run oldisim."""
  BENCHMARK_INFO['num_machines'] = (FLAGS.num_leaves + NUM_DRIVERS + NUM_ROOTS)
  return BENCHMARK_INFO


def InstallAndBuild(vm):
  """Install and build oldisim on the target vm.

  Args:
    vm: A vm instance that runs oldisim.
  """
  logging.info('prepare oldisim on %s', vm)
  vm.Install('oldisim_dependencies')
  local_tar_file_path = data.ResourcePath(OLDISIM_TAR)
  vm.PushFile(local_tar_file_path)
  vm.RemoteCommand('tar xvfz %s' % OLDISIM_TAR)
  #vm.RemoteCommand('git clone --recursive %s' % OLDISIM_GIT)
  vm.RemoteCommand('cd %s && scons' % OLDISIM_DIR)


def Prepare(benchmark_spec):
  """Install and build oldisim on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  vms = benchmark_spec.vms
  vm_index = 0
  leaf_vms = []

  for vm in vms:
    vm.vm_id = vm_index
    if vm.vm_id < NUM_DRIVERS:
      vm.vm_type = 'driver'
    elif vm.vm_id < (NUM_DRIVERS + NUM_ROOTS):
      vm.vm_type = 'root'
    else:
      vm.vm_type = 'leaf'
      leaf_vms.append(vm)
    vm_index += 1

  if vms:
    vm_util.RunThreaded(InstallAndBuild, vms)

  # Launch job on the leaf nodes.
  leaf_server_bin = os.path.join(OLDISIM_DIR, BINARY_BASE, 'LeafNode')
  for vm in leaf_vms:
    leaf_cmd = '%s --threads=%s' % (leaf_server_bin, vm.num_cpus)
    vm.RemoteCommand('%s &> /dev/null &' % leaf_cmd)


def SetupRoot(root_vm, leaf_vms):
  """Connect a root node to a list of leave nodes.

  Args:
    root_vm: A root vm instance.
    leaf_vms: A list of leaf vm instances.
  """
  root_vm.RemoteCommand('pkill ParentNode || true')
  fanout_args = ' '.join(['--leaf=%s' % i.internal_ip
                          for i in leaf_vms])
  root_server_bin = os.path.join(OLDISIM_DIR, BINARY_BASE, 'ParentNode')
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
      peak_qps = match.group('qps')
      peak_lat = match.group('lat')
      target_qps = peak_qps
      target_lat = peak_lat
      continue
    match = re.search(re_target, line)
    if match:
      target_qps = match.group('qps')
      target_lat = match.group('lat')
  return (peak_qps, peak_lat, target_qps, target_lat)


def RunLoadtest(benchmark_spec, fanout):
  """Run Loadtest for a given topology.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
    fanout: Request is first processed by a root node, which then
        fans out to a subset of leaf nodes.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  assert fanout <= FLAGS.num_leaves, (
      'The number of leaf nodes a root node connected to is defined by the '
      'flag fanout. Its current value %s is bigger than the total number of '
      'leaves %s.' % (fanout, FLAGS.num_leaves))

  vms = benchmark_spec.vms
  driver_vms = [vm for vm in vms if vm.vm_type == 'driver']
  root_vms = [vm for vm in vms if vm.vm_type == 'root']
  leaf_vms = [vm for vm in vms if vm.vm_type == 'leaf'][:fanout]

  for root_vm in root_vms:
    SetupRoot(root_vm, leaf_vms)

  driver_vm = driver_vms[0]
  driver_binary = os.path.join(OLDISIM_DIR, BINARY_BASE, 'DriverNode')
  launch_script = os.path.join(OLDISIM_DIR, 'workloads/search/search_qps.sh')
  driver_args = ' '.join(['--server=%s' % i.internal_ip
                          for i in root_vms])
  time.sleep(5)
  driver_cmd = '%s -s %s:%s -t 30 -- %s %s --threads=%s --depth=16' % (
      launch_script, FLAGS.latency_metric, FLAGS.latency_target,
      driver_binary, driver_args, driver_vm.num_cpus)
  logging.info('Driver cmdline: %s', driver_cmd)
  stdout, _ = driver_vm.RemoteCommand('%s' % driver_cmd, should_log=True)
  return ParseOutput(stdout)


def Run(benchmark_spec):
  """Run oldisim on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
  """
  results = []
  qps_dict = dict()
  vms = benchmark_spec.vms
  vm = vms[0]

  fanout_list = set([1, FLAGS.num_leaves])
  for fanout in map(int, FLAGS.fanout):
    if fanout > 1 and fanout < FLAGS.num_leaves:
      fanout_list.add(fanout)

  metadata = {'machine_type': vm.machine_type, 'num_cpus': vm.num_cpus}
  for fanout in sorted(fanout_list):
    qps = float(RunLoadtest(benchmark_spec, fanout)[2])
    qps_dict[fanout] = qps
    if fanout == 1:
      base_qps = qps
    name = 'Scaling efficiency of %s leaves' % fanout
    scaling_efficiency = '%.2f' % min(qps_dict[fanout] / base_qps, 1)
    results.append((str(name), float(scaling_efficiency), '', metadata))

  return results


def Cleanup(benchmark_spec):  # pylint: disable=unused-argument
  """Cleanup oldisim on the target vm.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  pass
