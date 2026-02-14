# Copyright 2024 PerfKitBenchmarker Authors. All rights reserved.
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

"""Redis HA Benchmark with HAProxy on GKE.

This benchmark deploys a Regional GKE cluster with:
1. Redis Primary (Zone A)
2. Redis Replica (Zone B)
3. HAProxy (Separate Node)
4. Memtier Client (Separate Node)

It configures HAProxy to listen on two ports:
- 6379: Proxies to Primary (Writes)
- 6380: Proxies to Replica (Reads)

It runs two concurrent memtier_benchmark instances:
- Instance 1: Writes to HAProxy:6379
- Instance 2: Reads from HAProxy:6380

Supports 'Optimized' mode via --gke_redis_ha_enable_optimization, which:
- Injects optimization.gke.io labels/annotations.
- Compiles Redis from source.
- Waits for GKE to inject redis.conf.
"""

import json
import logging
import time
from typing import List, Dict, Any

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import haproxy
from perfkitbenchmarker.linux_packages import memtier
from perfkitbenchmarker.linux_packages import redis_server
from perfkitbenchmarker.resources.kubernetes import kubernetes_virtual_machine

FLAGS = flags.FLAGS

flags.DEFINE_bool(
    'gke_redis_ha_enable_optimization',
    False,
    'Enable GKE workload optimization labels for Redis pods.',
)
flags.DEFINE_string(
    'gke_redis_ha_machine_type',
    None,
    'Machine type for GKE optimization configuration annotation.',
)
flags.DEFINE_bool(
    'gke_redis_ha_regional_cluster',
    True,
    'If true, creates a Regional GKE cluster and spreads Primary/Replica across'
    ' zones. If false, creates a Zonal cluster with all nodes in the same'
    ' zone.',
)

BENCHMARK_NAME = 'gke_redis_ha_haproxy'
BENCHMARK_CONFIG = """
gke_redis_ha_haproxy:
  description: Run Redis HA benchmark with HAProxy on GKE.
  vm_groups:
    primary:
      vm_spec: *default_dual_core
      vm_count: 1
      disk_spec: *default_50_gb
    replica:
      vm_spec: *default_dual_core
      vm_count: 1
      disk_spec: *default_50_gb
    haproxy:
      vm_spec: *default_dual_core
      vm_count: 1
    clients:
      vm_spec: *default_dual_core
      vm_count: 1
  container_cluster:
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: n2-standard-4
        zone: us-east1-b
    nodepools:
      primary:
        vm_count: 1
        vm_spec:
          GCP:
            machine_type: n2-standard-4
            zone: us-east1-b
      replica:
        vm_count: 1
        vm_spec:
          GCP:
            machine_type: n2-standard-4
            zone: us-east1-c
      haproxy:
        vm_count: 1
        vm_spec:
          GCP:
            machine_type: n2-standard-4
            zone: us-east1-b
      clients:
        vm_count: 1
        vm_spec:
          GCP:
            machine_type: n2-standard-4
            zone: us-east1-b
"""

# Store the original methods for monkey patching
_original_build_pod_body = None
_original_is_ready = None
_original_prepare_vm_environment = None


def _PrepareVMEnvironmentWithGKEOptimization(self):
  """Modified PrepareVMEnvironment to handle Redis Docker image and clients with package installation."""
  # Check if optimization is enabled and VM type
  is_redis = hasattr(self, 'vm_group') and self.vm_group in [
      'primary',
      'replica',
  ]
  is_client = hasattr(self, 'vm_group') and self.vm_group == 'clients'

  if FLAGS.gke_redis_ha_enable_optimization and (is_redis or is_client):
    vm_type = 'server' if is_redis else 'client'
    logging.info(
        f'Skipping PrepareVMEnvironment for {vm_type} container on {self.name}'
    )
    # Packages are already installed by the container startup script
    # Just verify sudo is available (should have been installed during container startup)
    sudo_check, _ = self.RemoteCommand('which sudo', ignore_failure=True)
    if not sudo_check:
      logging.warning('sudo not found, attempting to install...')
      self.RemoteCommand(
          'apt-get update && apt-get install -y sudo', ignore_failure=True
      )
    return

  _original_prepare_vm_environment(self)


def _IsReadyWithGKEOptimization(self) -> bool:
  """Modified _IsReady to support GKE optimization container naming."""
  # If optimization is disabled, use original method
  if not FLAGS.gke_redis_ha_enable_optimization:
    return _original_is_ready(self)

  # Use original pod checking logic
  exists_cmd = [
      FLAGS.kubectl,
      '--kubeconfig=%s' % FLAGS.kubeconfig,
      'get',
      'pod',
      '-o=json',
      self.name,
  ]
  logging.info('Waiting for POD %s (GKE Optimized HA)', self.name)
  pod_info, _, _ = vm_util.IssueCommand(exists_cmd, raise_on_failure=False)
  if pod_info:
    pod_info = json.loads(pod_info)
    containers = pod_info['spec']['containers']
    if len(containers) == 1:
      pod_status = pod_info['status']['phase']
      container_name = containers[0]['name']
      # Allow standard PKB naming OR redis (for GKE optimization)
      if (
          container_name.startswith(self.name) or container_name == 'redis'
      ) and pod_status == 'Running':
        logging.info('POD is up and running (GKE Optimized HA check).')
        return True
  return False


def _BuildPodBodyWithGKEOptimization(self):
  """Build pod or deployment body with GKE optimization (Auto method)."""
  pod_body_json = _original_build_pod_body(self)
  pod_body = json.loads(pod_body_json)

  # Check if this is a Redis server VM (primary or replica)
  is_redis = hasattr(self, 'vm_group') and self.vm_group in [
      'primary',
      'replica',
  ]
  is_client = hasattr(self, 'vm_group') and self.vm_group == 'clients'

  # Apply Host Networking if optimization is enabled (for both clients and servers)
  if FLAGS.gke_redis_ha_enable_optimization and (is_redis or is_client):
    if 'spec' in pod_body:
      pod_body['spec']['hostNetwork'] = True
      pod_body['spec']['dnsPolicy'] = 'ClusterFirstWithHostNet'

  # Handle client pods
  if FLAGS.gke_redis_ha_enable_optimization and is_client:
    if 'spec' in pod_body and 'containers' in pod_body['spec']:
      for container in pod_body['spec']['containers']:
        container['name'] = 'redis'
        container['command'] = ['/bin/bash', '-c']
        client_startup_script = (
            'echo "Installing required packages for PKB client..." && apt-get'
            ' update > /dev/null 2>&1 && apt-get install -y sudo fdisk sysstat'
            ' iproute2 netcat-openbsd > /dev/null 2>&1 && mkdir -p /tmp/pkb &&'
            ' echo "Packages installed successfully" && exec sleep infinity'
        )
        container['args'] = [client_startup_script]
    return json.dumps(pod_body)

  if FLAGS.gke_redis_ha_enable_optimization and is_redis:
    machine_type = FLAGS.gke_redis_ha_machine_type or 'c4d-standard-8'

    # Add optimization label
    if 'metadata' not in pod_body:
      pod_body['metadata'] = {}
    if 'labels' not in pod_body['metadata']:
      pod_body['metadata']['labels'] = {}
    pod_body['metadata']['labels'][
        'optimization.gke.io/workload'
    ] = 'redis-7-caching'

    # Add configuration annotation
    if 'annotations' not in pod_body['metadata']:
      pod_body['metadata']['annotations'] = {}
    pod_body['metadata']['annotations']['optimization.gke.io/configuration'] = (
        json.dumps({'machineType': machine_type})
    )

    # Configure container for optimization - compile Redis from source
    if 'spec' in pod_body and 'containers' in pod_body['spec']:
      for container in pod_body['spec']['containers']:
        container['image'] = 'ubuntu:latest'
        container['name'] = 'redis'

        redis_version = redis_server._VERSION.value or '7.2.6'
        redis_git_repo = (
            redis_server._GIT_REPO.value or 'https://github.com/redis/redis.git'
        )
        redis_type = redis_server._REDIS_TYPE.value
        server_binary = (
            'valkey-server' if redis_type == 'valkey' else 'redis-server'
        )

        container['command'] = ['/bin/sh', '-c']
        startup_script = (
            'set +e; echo "Installing build dependencies..."; apt-get update >'
            ' /dev/null 2>&1; apt-get install -y build-essential git sudo'
            ' fdisk sysstat iproute2 procps > /dev/null 2>&1; mkdir -p'
            ' /opt/pkb /tmp/pkb; echo "Cloning Redis from'
            f' {redis_git_repo}..."; git clone {redis_git_repo} /opt/pkb/redis'
            ' > /dev/null 2>&1; cd /opt/pkb/redis && git checkout'
            f' {redis_version} > /dev/null 2>&1; echo "Compiling Redis..."; cd'
            ' /opt/pkb/redis && make > /dev/null 2>&1; echo "Waiting for GKE'
            ' to inject optimization config..."; MAX_WAIT=60; WAITED=0; while'
            ' [ ! -f /etc/gke/optimization/redis.conf ] && [ $WAITED -lt'
            ' $MAX_WAIT ]; do   sleep 1; WAITED=$((WAITED + 1)); done; if [ -f'
            ' /etc/gke/optimization/redis.conf ]; then   echo "✅ GKE'
            ' optimization config found";   TOTAL_MEM_KB=$(grep MemTotal'
            " /proc/meminfo | awk '{print $2}');  "
            f' REDIS_CMD="/opt/pkb/redis/src/{server_binary}'
            ' /etc/gke/optimization/redis.conf";   REDIS_CMD="$REDIS_CMD'
            ' --protected-mode no --bind 0.0.0.0";   if ['
            ' "${redis_aof:-False}" = "True" ]; then    '
            ' MAX_MEM_KB=$((TOTAL_MEM_KB - 11024384));   else    '
            ' MAX_MEM_KB=$((TOTAL_MEM_KB * 7 / 10));   fi;  '
            ' REDIS_CMD="$REDIS_CMD --maxmemory ${MAX_MEM_KB}kb";  '
            f' EVICTION_POLICY="{redis_server._EVICTION_POLICY.value or ""}";  '
            ' if [ -n "$EVICTION_POLICY" ]; then     REDIS_CMD="$REDIS_CMD'
            ' --maxmemory-policy $EVICTION_POLICY";   fi;   $REDIS_CMD >'
            ' /tmp/redis.log 2>&1 &   REDIS_PID=$!;   echo "Redis started with'
            ' PID $REDIS_PID"; else   echo "ERROR: GKE optimization config not'
            ' found"; fi; exec sleep infinity'
        )
        container['args'] = [startup_script]

        # Remove privileged mode
        if 'securityContext' in container:
          container['securityContext'].pop('privileged', None)

    # Clear nodeSelector and tolerations
    if 'spec' in pod_body:
      if 'nodeSelector' in pod_body['spec']:
        pod_body['spec']['nodeSelector'] = {}
      if 'tolerations' in pod_body['spec']:
        pod_body['spec']['tolerations'] = []

    return json.dumps(pod_body)

  return json.dumps(pod_body)


def GetConfig(user_config):
  global _original_build_pod_body
  global _original_is_ready
  global _original_prepare_vm_environment

  # Monkey patch if optimization is enabled
  if (
      FLAGS.gke_redis_ha_enable_optimization
      and FLAGS.vm_platform == 'Kubernetes'
  ):
    if not _original_build_pod_body:
      _original_build_pod_body = (
          kubernetes_virtual_machine.KubernetesVirtualMachine._BuildPodBody
      )
      kubernetes_virtual_machine.KubernetesVirtualMachine._BuildPodBody = (
          _BuildPodBodyWithGKEOptimization
      )

    if not _original_is_ready:
      _original_is_ready = (
          kubernetes_virtual_machine.KubernetesVirtualMachine._IsReady
      )
      kubernetes_virtual_machine.KubernetesVirtualMachine._IsReady = (
          _IsReadyWithGKEOptimization
      )

    if not _original_prepare_vm_environment:
      _original_prepare_vm_environment = (
          kubernetes_virtual_machine.DebianBasedKubernetesVirtualMachine.PrepareVMEnvironment
      )
      kubernetes_virtual_machine.DebianBasedKubernetesVirtualMachine.PrepareVMEnvironment = (
          _PrepareVMEnvironmentWithGKEOptimization
      )

  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  # Simple Zonal Cluster Configuration
  if FLAGS.zone:
    zone = FLAGS.zone[0]
    logging.info(f'Configuring Zonal Cluster in {zone}')
    logging.info(f'  All nodepools will be in {zone}')

    if (
        'container_cluster' in config
        and 'nodepools' in config['container_cluster']
    ):
      nodepools = config['container_cluster']['nodepools']
      # Set all nodepools to the same zone
      for nodepool_name in ['primary', 'replica', 'haproxy', 'clients']:
        if nodepool_name in nodepools:
          nodepools[nodepool_name]['vm_spec']['GCP']['zone'] = zone

  return config


def Prepare(benchmark_spec):
  """Install Redis, HAProxy and configure replication."""
  primary_vm = benchmark_spec.vm_groups['primary'][0]
  replica_vm = benchmark_spec.vm_groups['replica'][0]
  haproxy_vm = benchmark_spec.vm_groups['haproxy'][0]
  client_vms = benchmark_spec.vm_groups['clients']

  # Install/Start Redis
  if FLAGS.gke_redis_ha_enable_optimization:
    # Wait for Redis to start (it starts in container entrypoint)
    for vm in [primary_vm, replica_vm]:
      logging.info(f'Waiting for Redis to start on {vm.name} (Optimized)...')
      port = redis_server.GetRedisPorts(vm)[0]
      localhost = vm.GetLocalhostAddr()
      cli_binary = redis_server.GetRedisCliBinary()

      # Simple wait loop
      for _ in range(30):
        cmd = f'/opt/pkb/redis/src/{cli_binary} -h {localhost} -p {port} ping'
        out, _ = vm.RemoteCommand(cmd, ignore_failure=True)
        if 'PONG' in out:
          break
        vm.RemoteCommand('sleep 2')
      else:
        raise Exception(f'Redis failed to start on {vm.name}')
  else:
    # Install Redis on primary and replica
    for vm in [primary_vm, replica_vm]:
      vm.Install('redis_server')
    # Start Redis
    background_tasks.RunThreaded(redis_server.Start, [primary_vm, replica_vm])

  # Configure Replication
  redis_server.ConfigureReplication(
      replica_vm, primary_vm.internal_ip, redis_server.DEFAULT_PORT
  )

  # Install and Configure HAProxy
  haproxy.Install(haproxy_vm)
  haproxy.Configure(haproxy_vm, primary_vm.internal_ip, replica_vm.internal_ip)
  haproxy.Start(haproxy_vm)

  # Install Memtier on clients
  for vm in client_vms:
    vm.Install('memtier')


def Run(benchmark_spec):
  """Run memtier benchmark against HAProxy/Primary/Replica."""
  haproxy_vm = benchmark_spec.vm_groups['haproxy'][0]
  primary_vm = benchmark_spec.vm_groups['primary'][0]
  client_vms = benchmark_spec.vm_groups['clients']

  # Get original settings
  total_clients = FLAGS.memtier_clients[0]
  original_ratio = FLAGS.memtier_ratio

  results = []

  # Debug: Check maxmemory config and system memory
  logging.info('Checking Redis maxmemory configuration and system memory...')
  cli_binary = redis_server.GetRedisCliBinary()
  primary_vm.RemoteCommand(
      f'{redis_server.GetRedisDir()}/src/{cli_binary} -h localhost -p 6379'
      ' CONFIG GET maxmemory'
  )
  primary_vm.RemoteCommand(
      f'{redis_server.GetRedisDir()}/src/{cli_binary} -h localhost -p 6379 INFO'
      ' memory'
  )
  primary_vm.RemoteCommand('free -m')

  # Run 1: Baseline - 1:1 ratio to Primary
  # MUST use allkeys-lru because baseline benchmark uses it (see default_benchmark_config.yaml)
  logging.info(
      'Setting eviction policy to allkeys-lru for Baseline run (matching'
      ' baseline config)...'
  )
  primary_vm.RemoteCommand(
      f'{redis_server.GetRedisDir()}/src/{cli_binary} -h localhost -p 6379'
      ' CONFIG SET maxmemory-policy allkeys-lru'
  )

  logging.info('Running baseline 1:1 workload to Primary...')
  FLAGS.memtier_ratio = '1:1'
  baseline_results = memtier.RunOverAllClientVMs(
      client_vms,
      haproxy_vm.internal_ip,
      [6379],  # Primary via HAProxy
      pipeline=FLAGS.memtier_pipeline[0],
      threads=FLAGS.memtier_threads[0],
      clients=total_clients,
      password=None,
  )
  # Tag baseline results
  for sample in baseline_results:
    sample.metadata['workload_type'] = 'baseline_primary_1:1'
  results.extend(baseline_results)

  # Delay between Test 1 and Test 2
  logging.info('Sleeping for 4 minutes to allow cluster to settle...')
  time.sleep(240)

  # Clear Redis between tests
  logging.info('Clearing Redis data before next test...')
  primary_vm.RemoteCommand(
      f'{redis_server.GetRedisDir()}/src/{cli_binary} -h localhost -p 6379'
      ' FLUSHALL'
  )

  # Run 2: HA Test - Writes only to Primary
  # Ensure allkeys-lru is set
  logging.info('Ensuring eviction policy is allkeys-lru for Write run...')
  primary_vm.RemoteCommand(
      f'{redis_server.GetRedisDir()}/src/{cli_binary} -h localhost -p 6379'
      ' CONFIG SET maxmemory-policy allkeys-lru'
  )

  logging.info('Running HA writes to Primary...')
  # Use full client capacity for write test to find max throughput
  write_clients = total_clients
  FLAGS.memtier_ratio = '1:0'
  write_results = memtier.RunOverAllClientVMs(
      client_vms,
      haproxy_vm.internal_ip,
      [6379],
      pipeline=FLAGS.memtier_pipeline[0],
      threads=FLAGS.memtier_threads[0],
      clients=write_clients,
      password=None,
  )
  # Tag write results
  for sample in write_results:
    sample.metadata['workload_type'] = 'ha_writes_primary'
  results.extend(write_results)

  # Delay between Test 2 and Test 3
  logging.info('Sleeping for 4 minutes to allow cluster to settle...')
  time.sleep(240)

  # No FLUSHALL between writes and reads - we want reads to access the written data

  # Run 3: HA Test - Reads from Replica
  # Keeping allkeys-lru as per user request for consistency
  logging.info('Running HA reads from Replica...')
  # Use full client capacity for read test
  read_clients = total_clients
  FLAGS.memtier_ratio = '0:1'
  read_results = memtier.RunOverAllClientVMs(
      client_vms,
      haproxy_vm.internal_ip,
      [6380],
      pipeline=FLAGS.memtier_pipeline[0],
      threads=FLAGS.memtier_threads[0],
      clients=read_clients,
      password=None,
  )
  # Tag read results
  for sample in read_results:
    sample.metadata['workload_type'] = 'ha_reads_replica'
  results.extend(read_results)

  # Restore original ratio
  FLAGS.memtier_ratio = original_ratio

  return memtier.AggregateMemtierResults(results, {})


def Cleanup(benchmark_spec):
  """Cleanup."""
  global _original_build_pod_body
  global _original_is_ready
  global _original_prepare_vm_environment

  # Restore monkey patches
  if FLAGS.vm_platform == 'Kubernetes':
    if _original_build_pod_body:
      kubernetes_virtual_machine.KubernetesVirtualMachine._BuildPodBody = (
          _original_build_pod_body
      )
      _original_build_pod_body = None
    if _original_is_ready:
      kubernetes_virtual_machine.KubernetesVirtualMachine._IsReady = (
          _original_is_ready
      )
      _original_is_ready = None
    if _original_prepare_vm_environment:
      kubernetes_virtual_machine.DebianBasedKubernetesVirtualMachine.PrepareVMEnvironment = (
          _original_prepare_vm_environment
      )
      _original_prepare_vm_environment = None
