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

"""Runs YCSB against different memcached-like offerings.

This benchmark runs two workloads against memcached using YCSB (the Yahoo! Cloud
Serving Benchmark).
memcached is described in perfkitbenchmarker.linux_packages.memcached_server
YCSB and workloads described in perfkitbenchmarker.linux_packages.ycsb.
"""

import functools
import logging

from perfkitbenchmarker import configs
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import memcached_server
from perfkitbenchmarker.linux_packages import ycsb
from perfkitbenchmarker.providers.aws import aws_network

FLAGS = flags.FLAGS

flags.DEFINE_enum('memcached_managed', providers.GCP,
                  [providers.GCP, providers.AWS],
                  'Managed memcached provider (GCP/AWS) to use.')

flags.DEFINE_enum('memcached_scenario', 'custom',
                  ['custom', 'managed'],
                  'select one scenario to run: \n'
                  'custom: Provision VMs and install memcached ourselves. \n'
                  'managed: Use the specified provider\'s managed memcache.')

flags.DEFINE_enum('memcached_elasticache_region', 'us-west-1',
                  ['ap-northeast-1', 'ap-northeast-2', 'ap-southeast-1',
                   'ap-southeast-2', 'ap-south-1', 'cn-north-1', 'eu-central-1',
                   'eu-west-1', 'us-gov-west-1', 'sa-east-1', 'us-east-1',
                   'us-east-2', 'us-west-1', 'us-west-2'],
                  'The region to use for AWS ElastiCache memcached servers.')

flags.DEFINE_enum('memcached_elasticache_node_type', 'cache.m3.medium',
                  ['cache.t2.micro', 'cache.t2.small', 'cache.t2.medium',
                   'cache.m3.medium', 'cache.m3.large', 'cache.m3.xlarge',
                   'cache.m3.2xlarge', 'cache.m4.large', 'cache.m4.xlarge',
                   'cache.m4.2xlarge', 'cache.m4.4xlarge', 'cache.m4.10xlarge'],
                  'The node type to use for AWS ElastiCache memcached servers.')

flags.DEFINE_integer('memcached_elasticache_num_servers', 1,
                     'The number of memcached instances for AWS ElastiCache.')


BENCHMARK_NAME = 'memcached_ycsb'
BENCHMARK_CONFIG = """
memcached_ycsb:
  description: >
    Run YCSB against an memcached
    installation. Specify the number of YCSB client VMs with
    --ycsb_client_vms and the number of YCSB server VMS with
    --num_vms.
  flags:
    ycsb_client_vms: 1
    num_vms: 1
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
  clients = benchmark_spec.vm_groups['clients']
  assert clients, benchmark_spec.vm_groups

  hosts = []

  if FLAGS.memcached_scenario == 'managed':
    # We need to delete the managed memcached backend when we're done
    benchmark_spec.always_call_cleanup = True

    if FLAGS.memcached_managed == providers.GCP:
      raise NotImplementedError("GCP managed memcached backend not implemented "
                                "yet")
    elif FLAGS.memcached_managed == providers.AWS:
      cluster_id = 'pkb%s' % FLAGS.run_uri
      service = providers.aws.elasticache.ElastiCacheMemcacheService(
          aws_network.AwsNetwork.GetNetwork(clients[0]),
          cluster_id, FLAGS.memcached_elasticache_region,
          FLAGS.memcached_elasticache_node_type,
          FLAGS.memcached_elasticache_num_servers)
    service.Create()
    hosts = service.GetHosts()
    benchmark_spec.service = service
    benchmark_spec.metadata = service.GetMetadata()
  else:
    # custom scenario
    # Install memcached on all the servers
    servers = benchmark_spec.vm_groups['servers']
    assert servers, 'No memcached servers: {0}'.format(benchmark_spec.vm_groups)
    memcached_install_fns = \
        [functools.partial(memcached_server.ConfigureAndStart, vm)
         for vm in servers]
    vm_util.RunThreaded(lambda f: f(), memcached_install_fns)
    hosts = ['%s:%s' % (vm.internal_ip, memcached_server.MEMCACHED_PORT)
             for vm in servers]
    benchmark_spec.metadata = {'ycsb_client_vms': FLAGS.ycsb_client_vms,
                               'ycsb_server_vms': len(servers),
                               'cache_size': FLAGS.memcached_size_mb}

  assert len(hosts) > 0

  ycsb_install_fns = [functools.partial(vm.Install, 'ycsb')
                      for vm in clients]
  vm_util.RunThreaded(lambda f: f(), ycsb_install_fns)
  benchmark_spec.executor = ycsb.YCSBExecutor(
      'memcached',
      **{'memcached.hosts': ','.join(hosts)})


def Run(benchmark_spec):
  """Spawn YCSB and gather the results.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.

  Returns:
    A list of sample.Sample instances.
  """

  logging.info('Start benchmarking memcached service, scenario is %s.',
               FLAGS.memcached_scenario)

  clients = benchmark_spec.vm_groups['clients']

  samples = list(benchmark_spec.executor.LoadAndRun(clients))

  for sample in samples:
    sample.metadata.update(benchmark_spec.metadata)

  return samples


def Cleanup(benchmark_spec):
  """Cleanup.

  Args:
    benchmark_spec: The benchmark specification. Contains all data that is
        required to run the benchmark.
  """
  if FLAGS.memcached_scenario == 'managed':
    service = benchmark_spec.service
    service.Destroy()
  else:
    # Custom scenario
    servers = benchmark_spec.vm_groups['servers']
    vm_util.RunThreaded(memcached_server.StopMemcached, servers)
