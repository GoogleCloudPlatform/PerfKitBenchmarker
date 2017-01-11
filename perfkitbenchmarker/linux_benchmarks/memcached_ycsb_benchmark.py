# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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

"""Runs YCSB against memcached.

This benchmark runs two workloads against memcached using YCSB (the Yahoo! Cloud
Serving Benchmark).
memcached is described in perfkitbenchmarker.linux_packages.memcached_server
YCSB and workloads described in perfkitbenchmarker.linux_packages.ycsb.
"""

import functools

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import memcached_server
from perfkitbenchmarker.linux_packages import ycsb

FLAGS = flags.FLAGS


BENCHMARK_NAME = 'memcached_ycsb'
BENCHMARK_CONFIG = """
memcached_ycsb:
  description: >
    Run YCSB against an memcached
    installation. Specify the number of YCSB client VMs with
    --ycsb_client_vms and the number of YCSB server VMS with
    --num_vms.
  vm_groups:
    servers:
      vm_spec: *default_single_core
    clients:
      vm_spec: *default_single_core
"""


def GetConfig(user_config):
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  if FLAGS['ycsb_client_vms'].present:
    config['vm_groups']['clients']['vm_count'] = FLAGS.ycsb_client_vms

  config['vm_groups']['servers']['vm_count'] = FLAGS.num_vms

  return config


def CheckPrerequisites(benchmark_config):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  ycsb.CheckPrerequisites()


def Prepare(benchmark_spec):
  """Prepare the virtual machines to run YCSB against memcached.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  loaders = benchmark_spec.vm_groups['clients']
  assert loaders, benchmark_spec.vm_groups

  # Memcached cluster
  memcached_vms = benchmark_spec.vm_groups['servers']
  assert memcached_vms, 'No memcached VMs: {0}'.format(benchmark_spec.vm_groups)

  memcached_install_fns = [functools.partial(memcached_server.ConfigureAndStart,
                                             vm)
                           for vm in memcached_vms]
  ycsb_install_fns = [functools.partial(vm.Install, 'ycsb')
                      for vm in loaders]

  vm_util.RunThreaded(lambda f: f(), memcached_install_fns + ycsb_install_fns)
  benchmark_spec.executor = ycsb.YCSBExecutor(
      'memcached',
      **{'memcached.hosts': ','.join([vm.internal_ip for vm in memcached_vms])})


def Run(benchmark_spec):
  """Spawn YCSB and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """
  loaders = benchmark_spec.vm_groups['clients']
  memcached_vms = benchmark_spec.vm_groups['servers']

  metadata = {'ycsb_client_vms': FLAGS.ycsb_client_vms,
              'num_vms': len(memcached_vms),
              'cache_size': FLAGS.memcached_size_mb}

  samples = list(benchmark_spec.executor.LoadAndRun(loaders))

  for sample in samples:
    sample.metadata.update(metadata)

  return samples


def Cleanup(benchmark_spec):
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  memcached_vms = benchmark_spec.vm_groups['servers']
  vm_util.RunThreaded(memcached_server.StopMemcached, memcached_vms)
