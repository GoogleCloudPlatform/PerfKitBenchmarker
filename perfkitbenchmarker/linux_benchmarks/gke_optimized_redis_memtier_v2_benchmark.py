# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Run memtier_benchmark against Redis with GKE optimization V2.

This version uses Google's recommended Auto optimization method, which relies
on GKE's Mutating Admission Controller to inject the optimal Redis configuration
when proper labels and annotations are set on the Pod.

Redis homepage: http://redis.io/
memtier_benchmark homepage: https://github.com/RedisLabs/memtier_benchmark
"""

from typing import Any, Dict, List
import json
import logging

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import memtier
from perfkitbenchmarker.linux_packages import redis_server
from perfkitbenchmarker.resources.kubernetes import kubernetes_virtual_machine

# location for top command output
_TOP_OUTPUT = 'top.txt'
# location for top script
_TOP_SCRIPT = 'top_script.sh'

FLAGS = flags.FLAGS
CLIENT_OS_TYPE = flags.DEFINE_string(
    'gke_redis_v2_client_os_type',
    None,
    'If provided, overrides the memtier client os type.',
)
SERVER_OS_TYPE = flags.DEFINE_string(
    'gke_redis_v2_server_os_type',
    None,
    'If provided, overrides the redis server os type.',
)
flags.DEFINE_string(
    'gke_redis_v2_client_machine_type',
    None,
    'If provided, overrides the memtier client machine type.',
)
flags.DEFINE_string(
    'gke_redis_v2_server_machine_type',
    None,
    'If provided, overrides the redis server machine type.',
)
GKE_REDIS_V2_MEASURE_CPU = flags.DEFINE_bool(
    'gke_redis_v2_measure_cpu',
    False,
    'If true, measure cpu usage on the server via top tool. Defaults to False.',
)
# GKE-specific optimization flags
flags.DEFINE_string(
    'gke_redis_v2_machine_type',
    None,
    'Machine type for GKE optimization configuration annotation.',
)
flags.DEFINE_bool(
    'gke_redis_v2_enable_optimization',
    True,
    'Enable GKE workload optimization labels for Redis pods.',
)


BENCHMARK_NAME = 'gke_optimized_redis_memtier_v2'
BENCHMARK_CONFIG = """
gke_optimized_redis_memtier_v2:
  description: >
      Run memtier_benchmark against Redis with GKE optimization V2 (Auto method).
  flags:
    memtier_protocol: redis
    create_and_boot_post_task_delay: 5
    memtier_data_size: 1024
    memtier_pipeline: 1
    placement_group_style: none
    redis_aof: False
    sar: True
    # GKE optimized defaults
    redis_server_version: '7.2.6'
    redis_server_enable_snapshots: False
    redis_server_io_threads_do_reads: True
  vm_groups:
    servers:
      vm_spec: *default_dual_core
      vm_count: 1
      disk_spec: *default_50_gb
    clients:
      vm_spec: *default_dual_core
      vm_count: 1
"""

_BenchmarkSpec = benchmark_spec.BenchmarkSpec

# Store the original methods for monkey patching
_original_build_pod_body = None
_original_is_ready = None
_original_prepare_vm_environment = None


def _PrepareVMEnvironmentWithGKEOptimization(self):
  """Modified PrepareVMEnvironment to handle Redis Docker image and clients with package installation."""
  # Check if optimization is enabled and VM type
  is_server = hasattr(self, 'vm_group') and self.vm_group == 'servers'
  is_client = hasattr(self, 'vm_group') and self.vm_group == 'clients'

  if FLAGS.gke_redis_v2_enable_optimization and (is_server or is_client):
    vm_type = 'server' if is_server else 'client'
    logging.info(f'Skipping PrepareVMEnvironment for {vm_type} container on {self.name}')
    # Packages are already installed by the container startup script
    # Just verify sudo is available (should have been installed during container startup)
    sudo_check, _ = self.RemoteCommand('which sudo', ignore_failure=True)
    if not sudo_check:
      logging.warning('sudo not found, attempting to install...')
      self.RemoteCommand('apt-get update && apt-get install -y sudo', ignore_failure=True)
    return

  _original_prepare_vm_environment(self)


def _IsReadyWithGKEOptimization(self) -> bool:
  """Modified _IsReady to support GKE optimization container naming."""
  # If optimization is disabled, use original method
  if not FLAGS.gke_redis_v2_enable_optimization:
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
  logging.info('Waiting for POD %s (GKE Optimized V2)', self.name)
  pod_info, _, _ = vm_util.IssueCommand(exists_cmd, raise_on_failure=False)
  if pod_info:
    pod_info = json.loads(pod_info)
    containers = pod_info['spec']['containers']
    if len(containers) == 1:
      pod_status = pod_info['status']['phase']
      container_name = containers[0]['name']
      # Allow standard PKB naming OR redis (for GKE optimization)
      if (
          (container_name.startswith(self.name) or container_name == 'redis')
          and pod_status == 'Running'
      ):
        logging.info('POD is up and running (GKE Optimized V2 check).')
        return True
  return False


def _BuildPodBodyWithGKEOptimization(self):
  """Build pod or deployment body with GKE optimization V2 (Auto method)."""
  pod_body_json = _original_build_pod_body(self)
  pod_body = json.loads(pod_body_json)

  # Add logging to debug
  logging.info(f'_BuildPodBodyWithGKEOptimization V2 called for VM: {self.name}')
  if hasattr(self, 'vm_group'):
    logging.info(f'VM group: {self.vm_group}')

  # Check if this is a server VM using vm_group attribute
  is_server = hasattr(self, 'vm_group') and self.vm_group == 'servers'
  is_client = hasattr(self, 'vm_group') and self.vm_group == 'clients'

  # Apply Host Networking if optimization is enabled (for both clients and servers)
  if FLAGS.gke_redis_v2_enable_optimization and (is_server or is_client):
    if 'spec' in pod_body:
      pod_body['spec']['hostNetwork'] = True
      pod_body['spec']['dnsPolicy'] = 'ClusterFirstWithHostNet'
      logging.info(f'Enabled hostNetwork=True and dnsPolicy=ClusterFirstWithHostNet for {self.name}')

  # Handle client pods - they need packages but not Redis optimization
  if FLAGS.gke_redis_v2_enable_optimization and is_client:
    logging.info(f'Installing packages for client VM: {self.name}')
    if 'spec' in pod_body and 'containers' in pod_body['spec']:
      for container in pod_body['spec']['containers']:
        # Keep original Ubuntu image for clients
        # Use "redis" as container name to match PKB's expectations
        container['name'] = 'redis'

        # Add startup script to install necessary packages
        container['command'] = ['/bin/bash', '-c']
        client_startup_script = (
            'echo "Installing required packages for PKB client..." && '
            'apt-get update > /dev/null 2>&1 && '
            'apt-get install -y sudo fdisk sysstat iproute2 netcat-openbsd > /dev/null 2>&1 && '
            'mkdir -p /tmp/pkb && '
            'echo "Packages installed successfully" && '
            # Keep the container running
            'exec sleep infinity'
        )
        container['args'] = [client_startup_script]
        logging.info(f'Client container configured with name "redis" and package installation script')
    return json.dumps(pod_body)

  if FLAGS.gke_redis_v2_enable_optimization and is_server:
    logging.info(f'Applying GKE optimization V2 (Auto method) to server VM: {self.name}')

    machine_type = FLAGS.gke_redis_v2_machine_type
    if not machine_type:
      machine_type = FLAGS.gke_redis_v2_server_machine_type
    if not machine_type:
      if hasattr(self, 'machine_type'):
        machine_type = self.machine_type
      else:
        machine_type = 'c4d-standard-8'

    # Add optimization label
    if 'metadata' not in pod_body:
      pod_body['metadata'] = {}
    if 'labels' not in pod_body['metadata']:
      pod_body['metadata']['labels'] = {}
    pod_body['metadata']['labels']['optimization.gke.io/workload'] = 'redis-7-caching'

    # Add configuration annotation
    if 'annotations' not in pod_body['metadata']:
      pod_body['metadata']['annotations'] = {}
    pod_body['metadata']['annotations']['optimization.gke.io/configuration'] = json.dumps({
        'machineType': machine_type
    })

    logging.info(f'Set GKE optimization label and annotation for machine type: {machine_type}')

    # Configure container for optimization - compile Redis from source
    if 'spec' in pod_body and 'containers' in pod_body['spec']:
      for container in pod_body['spec']['containers']:
        # Use Ubuntu base image (same as baseline) to compile Redis from source
        # This ensures exact version control via --redis_server_version flag
        container['image'] = 'ubuntu:latest'
        # Container name must begin with 'redis' for GKE optimization
        container['name'] = 'redis'

        # Get Redis version and git repo from flags
        redis_version = redis_server._VERSION.value or '8.0.5'
        redis_git_repo = redis_server._GIT_REPO.value or 'https://github.com/redis/redis.git'
        redis_type = redis_server._REDIS_TYPE.value
        
        logging.info(f'Optimized: Compiling Redis from {redis_git_repo} at version {redis_version} (Type: {redis_type})')

        # Determine binary names based on type
        server_binary = 'valkey-server' if redis_type == 'valkey' else 'redis-server'
        cli_binary = 'valkey-cli' if redis_type == 'valkey' else 'redis-cli'

        # V2: Compile Redis from source, then use GKE optimization config
        container['command'] = ['/bin/sh', '-c']
        startup_script = (
            'set +e; '
            # Install build dependencies and tools
            'echo "Installing build dependencies and PKB tools..."; '
            'apt-get update > /dev/null 2>&1; '
            'apt-get install -y build-essential git sudo fdisk sysstat iproute2 procps > /dev/null 2>&1; '
            'mkdir -p /opt/pkb /tmp/pkb; '
            'echo "Dependencies installed successfully"; '
            # Clone and compile Redis from source
            f'echo "Cloning Redis from {redis_git_repo}..."; '
            f'git clone {redis_git_repo} /opt/pkb/redis > /dev/null 2>&1; '
            f'cd /opt/pkb/redis && git checkout {redis_version} > /dev/null 2>&1; '
            'echo "Compiling Redis..."; '
            'cd /opt/pkb/redis && make > /dev/null 2>&1; '
            'echo "Redis compiled successfully"; '
            # Wait for GKE to inject the optimization config file
            'echo "Waiting for GKE to inject optimization config..."; '
            'MAX_WAIT=60; '
            'WAITED=0; '
            'while [ ! -f /etc/gke/optimization/redis.conf ] && [ $WAITED -lt $MAX_WAIT ]; do '
            '  sleep 1; '
            '  WAITED=$((WAITED + 1)); '
            'done; '
            'if [ -f /etc/gke/optimization/redis.conf ]; then '
            '  echo "✅ GKE optimization config found:"; '
            '  cat /etc/gke/optimization/redis.conf; '
            # Calculate maxmemory dynamically (same logic as baseline redis_server.py)
            # This ensures fair comparison - only difference is GKE optimization config
            '  echo \"Calculating maxmemory (AUTO - same as baseline)...\"; '
            '  TOTAL_MEM_KB=$(grep MemTotal /proc/meminfo | awk \'{print $2}\'); '
            '  echo \"Total node memory: ${TOTAL_MEM_KB} KB\"; '
            
            # Build Redis start command with proper flags matching baseline
            # Start with base command
            f'  REDIS_CMD="/opt/pkb/redis/src/{server_binary} /etc/gke/optimization/redis.conf"; '
            '  REDIS_CMD="$REDIS_CMD --protected-mode no"; '
            '  REDIS_CMD="$REDIS_CMD --bind 0.0.0.0"; '
            
            # Check if AOF is enabled from environment variable passed by PKB
            # This matches baseline's check of REDIS_AOF.value flag
            '  if [ "${redis_aof:-False}" = "True" ]; then '
            '    MAX_MEM_KB=$((TOTAL_MEM_KB - 11024384)); '  # Deduct 10.5GB for AOF overhead (same as baseline)
            '    echo \"AOF enabled: maxmemory = total - 10.5GB = ${MAX_MEM_KB} KB\"; '
            '  else '
            '    MAX_MEM_KB=$((TOTAL_MEM_KB * 7 / 10)); '  # 70% of total memory (same as baseline)
            '    echo \"AOF disabled: maxmemory = 70% of total = ${MAX_MEM_KB} KB\"; '
            '  fi; '
            
            # Add maxmemory to command
            '  REDIS_CMD="$REDIS_CMD --maxmemory ${MAX_MEM_KB}kb"; '
            
            # Only add eviction policy if it's set (matching baseline behavior at line 290)
            f'  EVICTION_POLICY="{redis_server._EVICTION_POLICY.value or ""}"; '
            '  if [ -n "$EVICTION_POLICY" ]; then '
            '    REDIS_CMD="$REDIS_CMD --maxmemory-policy $EVICTION_POLICY"; '
            '    echo \"Eviction policy: ${EVICTION_POLICY}\"; '
            '  else '
            '    echo \"No eviction policy set (using Redis default)\"; '
            '  fi; '
            
            '  echo \"Starting Redis with GKE config + AUTO maxmemory (${MAX_MEM_KB}KB)...\"; '
            '  echo \"Command: $REDIS_CMD\"; '
            # Start Redis with dynamically built command
            '  $REDIS_CMD > /tmp/redis.log 2>&1 & '
            '  REDIS_PID=$!; '
            '  echo "Redis started with PID $REDIS_PID"; '
            '  sleep 3; '
            '  if ps -p $REDIS_PID > /dev/null; then '
            '    echo "Redis is running successfully"; '
            '  else '
            '    echo "WARNING: Redis failed to start. Check logs:"; '
            '    cat /tmp/redis.log; '
            '  fi; '
            'else '
            '  echo "ERROR: GKE optimization config not found after ${MAX_WAIT}s"; '
            '  echo "GKE workload optimization may not be enabled on this cluster"; '
            '  ls -la /etc/gke/ || echo "  /etc/gke/ directory does not exist"; '
            '  ls -la /etc/gke/optimization/ || echo "  /etc/gke/optimization/ directory does not exist"; '
            'fi; '
            # Always run sleep infinity to keep container alive
            'exec sleep infinity'
        )
        container['args'] = [startup_script]

        # Remove privileged mode as Redis container doesn't need it
        if 'securityContext' in container:
          container['securityContext'].pop('privileged', None)

        logging.info(f'Container configured with redis:7 image for GKE optimization V2 (Auto method)')

    # Remove PKB node selector and any other selectors to allow GKE to schedule on optimized nodes
    if 'spec' in pod_body:
      # Clear nodeSelector completely to avoid conflicts with GKE optimization placement
      if 'nodeSelector' in pod_body['spec']:
        pod_body['spec']['nodeSelector'] = {}
        logging.info('Cleared nodeSelector to allow GKE optimization scheduling')
      
      # Clear tolerations to avoid conflicts
      if 'tolerations' in pod_body['spec']:
        pod_body['spec']['tolerations'] = []
        logging.info('Cleared tolerations to allow GKE optimization scheduling')

    logging.info(f'GKE optimization V2 labels/annotations applied')
    # Return Pod body for PKB to create
    return json.dumps(pod_body)
  else:
    if not FLAGS.gke_redis_v2_enable_optimization:
      logging.info('GKE optimization V2 disabled by flag')
    elif not is_server:
      logging.info(f'Skipping optimization V2 for non-server VM: {self.name}')

    # Return regular Pod for non-optimized cases
    return json.dumps(pod_body)


def CheckPrerequisites(_):
  """Verifies that benchmark setup is correct."""
  if FLAGS.redis_server_cluster_mode and not FLAGS.memtier_cluster_mode:
    raise errors.Setup.InvalidFlagConfigurationError(
        '--redis_server_cluster_mode must be used with --memtier_cluster_mode'
    )
  if len(redis_server.GetRedisPorts()) >= 0 and (
      len(FLAGS.memtier_pipeline) > 1
      or len(FLAGS.memtier_threads) > 1
      or len(FLAGS.memtier_clients) > 1
  ):
    raise errors.Setup.InvalidFlagConfigurationError(
        'There can only be 1 setting for pipeline, threads and clients if '
        'there are multiple redis endpoints. Consider splitting up the '
        'benchmarking.'
    )


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config spec."""
  global _original_build_pod_body
  global _original_is_ready
  global _original_prepare_vm_environment

  # Monkey patch EARLY - before pod creation happens!
  if FLAGS.vm_platform == 'Kubernetes':
    if not _original_build_pod_body:
      _original_build_pod_body = kubernetes_virtual_machine.KubernetesVirtualMachine._BuildPodBody
      kubernetes_virtual_machine.KubernetesVirtualMachine._BuildPodBody = _BuildPodBodyWithGKEOptimization

    if not _original_is_ready:
      _original_is_ready = kubernetes_virtual_machine.KubernetesVirtualMachine._IsReady
      kubernetes_virtual_machine.KubernetesVirtualMachine._IsReady = _IsReadyWithGKEOptimization

    if not _original_prepare_vm_environment:
      _original_prepare_vm_environment = kubernetes_virtual_machine.DebianBasedKubernetesVirtualMachine.PrepareVMEnvironment
      kubernetes_virtual_machine.DebianBasedKubernetesVirtualMachine.PrepareVMEnvironment = _PrepareVMEnvironmentWithGKEOptimization

  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
  if CLIENT_OS_TYPE.value:
    config['vm_groups']['clients']['os_type'] = CLIENT_OS_TYPE.value

  if SERVER_OS_TYPE.value:
    config['vm_groups']['servers']['os_type'] = SERVER_OS_TYPE.value

  if FLAGS.gke_redis_v2_client_machine_type:
    vm_spec = config['vm_groups']['clients']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.gke_redis_v2_client_machine_type
  if FLAGS.gke_redis_v2_server_machine_type:
    vm_spec = config['vm_groups']['servers']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud]['machine_type'] = FLAGS.gke_redis_v2_server_machine_type

  # For Kubernetes platform, ensure container_cluster configuration exists
  # For Kubernetes platform, ensure container_cluster configuration exists
  if FLAGS.vm_platform == 'Kubernetes':
    if 'container_cluster' not in config:
      config['container_cluster'] = {}
    
    # Ensure default cluster fields are present (type, cloud, vm_spec)
    # This is needed because config_override might create a partial container_cluster dict
    if 'type' not in config['container_cluster']:
      config['container_cluster']['type'] = 'Kubernetes'
    if 'cloud' not in config['container_cluster']:
      config['container_cluster']['cloud'] = FLAGS.cloud
    if 'vm_spec' not in config['container_cluster']:
      default_machine_type = FLAGS.gke_redis_v2_server_machine_type or 'n2-standard-4'
      config['container_cluster']['vm_spec'] = {
        'GCP': {
          'machine_type': default_machine_type,
          'zone': FLAGS.zone
        }
      }
    if 'nodepools' not in config['container_cluster']:
      config['container_cluster']['nodepools'] = {}

    # Ensure nodepools exist for servers and clients
    if 'nodepools' not in config['container_cluster']:
      config['container_cluster']['nodepools'] = {}

    # Configure server nodepool
    if 'servers' not in config['container_cluster']['nodepools']:
      server_machine_type = FLAGS.gke_redis_v2_server_machine_type or 'n2-standard-4'
      config['container_cluster']['nodepools']['servers'] = {
        'vm_spec': {
          'GCP': {
            'machine_type': server_machine_type,
            'zone': FLAGS.zone
          }
        },
        'vm_count': 1
      }
    elif FLAGS.gke_redis_v2_server_machine_type:
      config['container_cluster']['nodepools']['servers']['vm_spec']['GCP']['machine_type'] = FLAGS.gke_redis_v2_server_machine_type

    # Configure client nodepool
    if 'clients' not in config['container_cluster']['nodepools']:
      client_machine_type = FLAGS.gke_redis_v2_client_machine_type or 'n2-standard-4'
      config['container_cluster']['nodepools']['clients'] = {
        'vm_spec': {
          'GCP': {
            'machine_type': client_machine_type,
            'zone': FLAGS.zone
          }
        },
        'vm_count': 1
      }
    elif FLAGS.gke_redis_v2_client_machine_type:
      config['container_cluster']['nodepools']['clients']['vm_spec']['GCP']['machine_type'] = FLAGS.gke_redis_v2_client_machine_type

  if not redis_server.REDIS_AOF.value:
    config['vm_groups']['servers']['disk_count'] = 0
  return config


def PrepareSystem(bm_spec: _BenchmarkSpec) -> None:
  """Set system-wide parameters."""
  # Monkey patching is now done in GetConfig (before provision stage)
  server_vms = bm_spec.vm_groups['servers']  # for cluster mode
  background_tasks.RunThreaded(redis_server.PrepareSystem, server_vms)


def InstallPackages(bm_spec: _BenchmarkSpec) -> None:
  """Install Redis on the servers and memtier on the clients."""
  client_vms = bm_spec.vm_groups['clients']
  server_vms = bm_spec.vm_groups['servers']

  # Always install memtier on clients
  background_tasks.RunThreaded(
      lambda client: client.Install('memtier'), client_vms
  )

  # Redis is compiled from source inside the container during startup
  # No need to install here - the container startup script handles it
  logging.info('Redis will be compiled from source during container startup')


def StartServices(bm_spec: _BenchmarkSpec) -> None:
  """Start Redis servers."""
  server_count = len(bm_spec.vm_groups['servers'])
  if server_count != 1 and not redis_server.CLUSTER_MODE.value:
    raise errors.Benchmarks.PrepareException(
        f'Expected servers vm count to be 1, got {server_count}'
    )
  client_vms = bm_spec.vm_groups['clients']
  server_vm = bm_spec.vm_groups['servers'][0]
  all_server_vms = bm_spec.vm_groups['servers']  # for cluster mode

  for vm in all_server_vms:
    if FLAGS.gke_redis_v2_enable_optimization:
      # With Redis Docker image and GKE Auto optimization, Redis should already be running
      # Wait for Redis to be available
      port = redis_server.GetRedisPorts(vm)[0]
      localhost = vm.GetLocalhostAddr()

      # Wait for Redis to start (container startup script needs time)
      logging.info('Waiting for Redis to start with GKE Auto optimization...')

      # Check if config file was created by GKE
      config_check, _ = vm.RemoteCommand('ls -la /etc/gke/optimization/redis.conf', ignore_failure=True)
      logging.info(f'GKE Config file check: {config_check}')

      # Check config file contents if it exists
      if 'redis.conf' in config_check:
        config_contents, _ = vm.RemoteCommand('cat /etc/gke/optimization/redis.conf', ignore_failure=True)
        logging.info(f'GKE Auto-generated config contents:\n{config_contents}')
      else:
        logging.warning('GKE optimization config not found - GKE workload optimization may not be enabled')

      max_attempts = 30
      cli_binary = redis_server.GetRedisCliBinary()
      for attempt in range(max_attempts):
        check_redis_cmd = f'/opt/pkb/redis/src/{cli_binary} -h {localhost} -p {port} ping'
        redis_running, _ = vm.RemoteCommand(check_redis_cmd, ignore_failure=True)
        if 'PONG' in redis_running:
          logging.info(f'Redis is running (attempt {attempt + 1}/{max_attempts})')
          break

        if attempt < max_attempts - 1:
          vm.RemoteCommand('sleep 2')
      else:
        # Final diagnostic before failing
        server_binary = 'valkey-server' if FLAGS.gke_redis_v2_enable_optimization and redis_server._REDIS_TYPE.value == 'valkey' else 'redis-server'
        final_ps, _ = vm.RemoteCommand(f'ps aux | grep {server_binary}', ignore_failure=True)
        logging.error(f'Final process check: {final_ps}')
        raise errors.Benchmarks.PrepareException('Redis failed to start within timeout')

      # Verify the current configuration
      io_threads_cmd = f'/opt/pkb/redis/src/{cli_binary} -h {localhost} -p {port} CONFIG GET io-threads'
      io_threads_result, _ = vm.RemoteCommand(io_threads_cmd, ignore_failure=True)
      io_threads_do_reads_cmd = f'/opt/pkb/redis/src/{cli_binary} -h {localhost} -p {port} CONFIG GET io-threads-do-reads'
      io_threads_do_reads_result, _ = vm.RemoteCommand(io_threads_do_reads_cmd, ignore_failure=True)

      logging.info(f'Redis configuration from GKE Auto optimization:')
      logging.info(f'  io-threads: {io_threads_result.strip()}')
      logging.info(f'  io-threads-do-reads: {io_threads_do_reads_result.strip()}')
    else:
      # Standard startup when optimization is disabled
      if redis_server.IO_THREADS.value:
        io_threads_list = [int(io) for io in redis_server.IO_THREADS.value]
        if io_threads_list:
          redis_server.CURRENT_IO_THREADS = io_threads_list[0]
          logging.info(f'Setting io_threads to {redis_server.CURRENT_IO_THREADS} from flag')
      redis_server.Start(vm)

  if redis_server.CLUSTER_MODE.value:
    redis_server.StartCluster(all_server_vms)

  # Load the redis server with preexisting data.
  bm_spec.redis_endpoint_ip = bm_spec.vm_groups['servers'][0].internal_ip
  ports = redis_server.GetRedisPorts(server_vm)
  ports_group_of_four = [ports[i : i + 4] for i in range(0, len(ports), 4)]
  assert bm_spec.redis_endpoint_ip
  for ports_group in ports_group_of_four:
    background_tasks.RunThreaded(
        lambda port: memtier.Load(
            [client_vms[0]], bm_spec.redis_endpoint_ip, port
        ),
        ports_group,
        10,
    )


def Run(bm_spec: _BenchmarkSpec) -> List[sample.Sample]:
  """Run memtier_benchmark against Redis."""
  client_vms = bm_spec.vm_groups['clients']
  server_vm: virtual_machine.BaseVirtualMachine | None = None
  if 'servers' in bm_spec.vm_groups:
    server_vm = bm_spec.vm_groups['servers'][0]
  measure_cpu_on_server_vm = server_vm and GKE_REDIS_V2_MEASURE_CPU.value

  benchmark_metadata = {}

  if measure_cpu_on_server_vm:
    top_cmd = (
        f'top -b -d 1 -n {memtier.MEMTIER_RUN_DURATION.value} > {_TOP_OUTPUT} &'
    )
    server_vm.RemoteCommand(f'echo "{top_cmd}" > {_TOP_SCRIPT}')
    server_vm.RemoteCommand(f'bash {_TOP_SCRIPT}')

  assert bm_spec.redis_endpoint_ip

  # No io-threads sweeping for V2 (config is controlled by GKE)
  raw_results = memtier.RunOverAllThreadsPipelinesAndClients(
      client_vms,
      bm_spec.redis_endpoint_ip,
      redis_server.GetRedisPorts(server_vm),
  )
  redis_metadata = redis_server.GetMetadata(server_vm)

  for server_result in raw_results:
    server_result.metadata.update(redis_metadata)
    server_result.metadata.update(benchmark_metadata)
    # Add GKE optimization V2 metadata
    server_result.metadata['gke_optimization_v2_enabled'] = FLAGS.gke_redis_v2_enable_optimization
    server_result.metadata['gke_optimization_method'] = 'auto'
    server_result.metadata['gke_machine_type'] = FLAGS.gke_redis_v2_machine_type or FLAGS.gke_redis_v2_server_machine_type or 'auto'

  top_results = []
  if measure_cpu_on_server_vm:
    top_results = _GetTopResults(server_vm)

  return raw_results + top_results


def Cleanup(bm_spec: _BenchmarkSpec) -> None:
  """Cleanup after benchmark."""
  global _original_build_pod_body
  global _original_is_ready
  global _original_prepare_vm_environment

  # Restore the original methods if we patched them
  if FLAGS.vm_platform == 'Kubernetes':
    if _original_build_pod_body:
      kubernetes_virtual_machine.KubernetesVirtualMachine._BuildPodBody = _original_build_pod_body
      _original_build_pod_body = None
    
    if _original_is_ready:
      kubernetes_virtual_machine.KubernetesVirtualMachine._IsReady = _original_is_ready
      _original_is_ready = None

    if _original_prepare_vm_environment:
      kubernetes_virtual_machine.DebianBasedKubernetesVirtualMachine.PrepareVMEnvironment = _original_prepare_vm_environment
      _original_prepare_vm_environment = None

  del bm_spec


def _GetTopResults(server_vm) -> List[sample.Sample]:
  """Get and parse CPU output from top command."""
  if not GKE_REDIS_V2_MEASURE_CPU.value:
    return []
  cpu_usage, _ = server_vm.RemoteCommand(f'grep Cpu {_TOP_OUTPUT}')

  samples = []
  row_index = 0
  for row in cpu_usage.splitlines():
    line = row.strip()
    columns = line.split(',')
    idle_value, _ = columns[3].strip().split(' ')
    samples.append(
        sample.Sample(
            'CPU Idle time',
            idle_value,
            '%Cpu(s)',
            {
                'time_series_sec': row_index,
                'cpu_idle_percent': idle_value,
            },
        )
    )
    row_index += 1
  return samples
