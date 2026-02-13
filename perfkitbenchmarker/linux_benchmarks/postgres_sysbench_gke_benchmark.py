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

"""Run Sysbench against PostgreSQL on GKE.

This benchmark measures the performance of PostgreSQL deployed on Google
Kubernetes Engine (GKE) using Sysbench. It supports multiple machine types
and optimization profiles.

The benchmark follows the "Pod as VM" pattern similar to the Redis benchmark,
deploying PostgreSQL as a StatefulSet and using either client VMs or pods
for load generation.
"""

import functools
import logging
import os
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import data
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import sysbench
from perfkitbenchmarker.linux_packages import postgresql
from perfkitbenchmarker.linux_benchmarks import sysbench_benchmark

FLAGS = flags.FLAGS

# PostgreSQL configuration flags
flags.DEFINE_string(
    'postgres_gke_shared_buffers',
    '15GB',
    'PostgreSQL shared_buffers size (baseline: 15GB, optimized: 35GB)'
)
flags.DEFINE_integer(
    'postgres_gke_max_connections',
    1000,
    'PostgreSQL max_connections'
)
flags.DEFINE_string(
    'postgres_gke_effective_cache_size',
    '30GB',
    'PostgreSQL effective_cache_size (baseline: 30GB, optimized: 40GB)'
)
flags.DEFINE_string(
    'postgres_gke_work_mem',
    '64MB',
    'PostgreSQL work_mem (baseline: 64MB, optimized: 32MB)'
)
flags.DEFINE_integer(
    'postgres_gke_max_worker_processes',
    20,
    'PostgreSQL max_worker_processes (baseline: 20, optimized: 32)'
)
flags.DEFINE_integer(
    'postgres_gke_max_parallel_workers_per_gather',
    8,
    'PostgreSQL max_parallel_workers_per_gather (baseline: 8, optimized: 12)'
)
flags.DEFINE_string(
    'postgres_gke_wal_buffers',
    '64MB',
    'PostgreSQL wal_buffers (baseline: 64MB, optimized: 32MB)'
)
flags.DEFINE_string(
    'postgres_gke_max_wal_size',
    '1GB',
    'PostgreSQL max_wal_size (baseline: 1GB, optimized: 16GB)'
)
flags.DEFINE_integer(
    'postgres_gke_autovacuum_max_workers',
    3,
    'PostgreSQL autovacuum_max_workers (baseline: 3, optimized: 8)'
)

# Infrastructure flags
flags.DEFINE_string(
    'postgres_gke_server_machine_type',
    None,
    'Machine type for PostgreSQL server nodes'
)
flags.DEFINE_string(
    'postgres_gke_client_machine_type',
    None,
    'Machine type for client nodes'
)
flags.DEFINE_integer(
    'postgres_gke_disk_size',
    500,
    'Disk size in GB for PostgreSQL data'
)
flags.DEFINE_string(
    'postgres_gke_disk_type',
    None,
    'Disk type (auto-selected based on machine type if not specified)'
)
flags.DEFINE_enum(
    'postgres_gke_optimization_profile',
    'baseline',
    ['baseline', 'v1', 'v2', 'v3', 'v4', 'v6', 'v1+v6', 'v1+v6+v4', 'v1+v6+v4+hostnetwork'],
    'Optimization profile to use'
)
flags.DEFINE_bool(
    'postgres_gke_use_init_container',
    True,
    'Whether to use init container for system updates (baseline: True, v2: False)'
)
flags.DEFINE_enum(
    'postgres_gke_client_mode',
    'pod',
    ['vm', 'pod'],
    'Whether to use VMs or pods for client workload generation'
)
# Note: sysbench_load_threads is already defined in sysbench_benchmark.py

BENCHMARK_NAME = 'postgres_sysbench_gke'
BENCHMARK_CONFIG = """
postgres_sysbench_gke:
  description: >
    Run Sysbench against PostgreSQL on GKE.
    Supports multiple machine types and optimization profiles.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: e2-standard-2
        zone: us-central1-a
    nodepools:
      postgres:
        vm_spec:
          GCP:
            machine_type: c4-standard-16
            zone: us-central1-a
            boot_disk_size: 500
            boot_disk_type: hyperdisk-balanced
        vm_count: 1
      clients:
        vm_spec:
          GCP:
            machine_type: c4-standard-16
            zone: us-central1-a
            boot_disk_size: 100
            boot_disk_type: hyperdisk-balanced
        vm_count: 1
  vm_groups:
    clients:
      vm_spec: *default_dual_core
      vm_count: 1
  flags:
    # Sysbench defaults matching baseline
    sysbench_tables: 10
    sysbench_table_size: 4000000
    sysbench_run_threads: 512
    sysbench_run_seconds: 300
    sysbench_report_interval: 10
    sysbench_testname: oltp_read_write
"""

# Machine type to disk type mapping
MACHINE_DISK_MAPPING = {
    'c4': 'hyperdisk-balanced',
    'c4d': 'hyperdisk-balanced',
    'c4a': 'hyperdisk-balanced',
    'n2': 'pd-ssd',
    'n2d': 'pd-ssd',
}

# Optimization profiles
OPTIMIZATION_PROFILES = {
    'baseline': {
        'postgres': {
            'shared_buffers': '15GB',
            'max_connections': 1000,
            'effective_cache_size': '30GB',
            'work_mem': '64MB',
            'max_worker_processes': 20,
            'max_parallel_workers_per_gather': 8,
            'max_parallel_workers': 12,
            'wal_buffers': '64MB',
            'max_wal_size': '16GB',
            'autovacuum_max_workers': 3,
            'effective_io_concurrency': 100,
            'random_page_cost': 1.1,
            'checkpoint_timeout': '5min',
            'checkpoint_completion_target': 0.9,
        },
        'resources': {
            'cpu_request': '14',
            'cpu_limit': '15',
            'memory_request': '45Gi',
            'memory_limit': '55Gi',
        },
        'use_init_container': True,
        'node_image': 'UBUNTU_CONTAINERD',
        'client_image': 'ubuntu:20.04',
    },
    'v1': {
        'postgres': {
            'shared_buffers': '15GB',
            'max_connections': 1000,
            'effective_cache_size': '30GB',
            'work_mem': '64MB',
            'max_worker_processes': 20,
            'max_parallel_workers_per_gather': 8,
            'max_parallel_workers': 12,
            'wal_buffers': '64MB',
            'max_wal_size': '16GB',
            'autovacuum_max_workers': 3,
            'effective_io_concurrency': 100,
            'random_page_cost': 1.1,
            'checkpoint_timeout': '5min',
            'checkpoint_completion_target': 0.9,
        },
        'resources': {
            'cpu_request': '14',
            'cpu_limit': '15',
            'memory_request': '45Gi',
            'memory_limit': '55Gi',
        },
        'use_init_container': True,
        'node_image': 'COS_CONTAINERD',
        'client_image': 'ubuntu:24.04',
    },
    'v2': {
        'postgres': {
            'shared_buffers': '15GB',
            'max_connections': 1000,
            'effective_cache_size': '30GB',
            'work_mem': '64MB',
            'max_worker_processes': 20,
            'max_parallel_workers_per_gather': 8,
            'max_parallel_workers': 12,
            'wal_buffers': '64MB',
            'max_wal_size': '16GB',
            'autovacuum_max_workers': 3,
            'effective_io_concurrency': 100,
            'random_page_cost': 1.1,
            'checkpoint_timeout': '5min',
            'checkpoint_completion_target': 0.9,
        },
        'resources': {
            'cpu_request': '14',
            'cpu_limit': '15',
            'memory_request': '45Gi',
            'memory_limit': '55Gi',
        },
        'use_init_container': False,
        'node_image': 'UBUNTU_CONTAINERD',
        'client_image': 'ubuntu:20.04',
    },
    'v3': {
        'postgres': {
            'shared_buffers': '15GB',
            'max_connections': 1000,
            'effective_cache_size': '30GB',
            'work_mem': '64MB',
            'max_worker_processes': 20,
            'max_parallel_workers_per_gather': 8,
            'max_parallel_workers': 12,
            'wal_buffers': '64MB',
            'max_wal_size': '16GB',
            'autovacuum_max_workers': 3,
            'effective_io_concurrency': 100,
            'random_page_cost': 1.1,
            'checkpoint_timeout': '5min',
            'checkpoint_completion_target': 0.9,
        },
        'resources': {
            'cpu_request': '14',
            'cpu_limit': '15',
            'memory_request': '45Gi',
            'memory_limit': '55Gi',
        },
        'use_init_container': True,
        'node_image': 'UBUNTU_CONTAINERD',
        'client_image': 'ubuntu:20.04',
        'kernel_params': {
            'vm.swappiness': 1,
            'vm.dirty_ratio': 10,
            'vm.dirty_background_ratio': 5,
            'net.core.netdev_max_backlog': 4000,
        },
    },
    'v4': {
        'postgres': {
            'shared_buffers': '15GB',
            'max_connections': 1000,
            'effective_cache_size': '30GB',
            'work_mem': '64MB',
            'max_worker_processes': 20,
            'max_parallel_workers_per_gather': 8,
            'max_parallel_workers': 12,
            'wal_buffers': '64MB',
            'max_wal_size': '16GB',
            'autovacuum_max_workers': 3,
            'effective_io_concurrency': 100,
            'random_page_cost': 1.1,
            'checkpoint_timeout': '5min',
            'checkpoint_completion_target': 0.9,
            'huge_pages': 'on',
        },
        'resources': {
            'cpu_request': '14',
            'cpu_limit': '15',
            'memory_request': '15Gi',
            'memory_limit': '15Gi',
        },
        'use_init_container': True,
        'node_image': 'UBUNTU_CONTAINERD',
        'client_image': 'ubuntu:20.04',
        'hugepages': {
            'hugepage_size2m': 19456,
            'hugepage_size1g': 0,
        },
    },
    'v6': {
        'postgres': {
            'shared_buffers': '35GB',
            'max_connections': 1000,
            'effective_cache_size': '50GB',
            'work_mem': '256MB',
            'max_worker_processes': 32,
            'max_parallel_workers_per_gather': 12,
            'max_parallel_workers': 24,
            'wal_buffers': '512MB',
            'max_wal_size': '16GB',
            'autovacuum_max_workers': 6,
            'effective_io_concurrency': 200,
            'random_page_cost': 1.1,
            'checkpoint_timeout': '15min',
            'checkpoint_completion_target': 0.9,
            'wal_level': 'replica',
        },
        'resources': {
            'cpu_request': '14',
            'cpu_limit': '15',
            'memory_request': '45Gi',
            'memory_limit': '55Gi',
        },
        'use_init_container': True,
        'node_image': 'UBUNTU_CONTAINERD',
        'client_image': 'ubuntu:20.04',
    },
    'v1+v6': {
        'postgres': {
            'shared_buffers': '35GB',
            'max_connections': 1000,
            'effective_cache_size': '50GB',
            'work_mem': '256MB',
            'max_worker_processes': 32,
            'max_parallel_workers_per_gather': 12,
            'max_parallel_workers': 24,
            'wal_buffers': '512MB',
            'max_wal_size': '16GB',
            'autovacuum_max_workers': 6,
            'effective_io_concurrency': 200,
            'random_page_cost': 1.1,
            'checkpoint_timeout': '15min',
            'checkpoint_completion_target': 0.9,
            'wal_level': 'replica',
        },
        'resources': {
            'cpu_request': '14',
            'cpu_limit': '15',
            'memory_request': '45Gi',
            'memory_limit': '55Gi',
        },
        'use_init_container': True,
        'node_image': 'COS_CONTAINERD',
        'client_image': 'ubuntu:24.04',
    },
    'v1+v6+v4': {
        'postgres': {
            'shared_buffers': '35GB',
            'max_connections': 1000,
            'effective_cache_size': '50GB',
            'work_mem': '256MB',
            'max_worker_processes': 32,
            'max_parallel_workers_per_gather': 12,
            'max_parallel_workers': 24,
            'wal_buffers': '512MB',
            'max_wal_size': '16GB',
            'autovacuum_max_workers': 6,
            'effective_io_concurrency': 200,
            'random_page_cost': 1.1,
            'checkpoint_timeout': '15min',
            'checkpoint_completion_target': 0.9,
            'wal_level': 'replica',
            'huge_pages': 'on',
            'synchronous_commit': 'on',
            'log_line_prefix': '%t [%p]: [%l-1] user=%u,db=%d ',
            'log_checkpoints': 'on',
            'log_connections': 'on',
            'log_disconnections': 'on',
            'log_lock_waits': 'on',
            'log_temp_files': '0',
            'log_autovacuum_min_duration': '0',
            'log_error_verbosity': 'default',
            'client_min_messages': 'notice',
            'log_min_messages': 'warning',
            'log_min_error_statement': 'error',
            'log_min_duration_statement': '1000',
        },
        'resources': {
            'cpu_request': '14',
            'cpu_limit': '15',
            'memory_request': '15Gi',
            'memory_limit': '15Gi',
        },
        'use_init_container': True,
        'node_image': 'COS_CONTAINERD',
        'client_image': 'ubuntu:24.04',
        'hugepages': {
            'hugepage_size2m': 19456,
            'hugepage_size1g': 0,
        },
    },
    'v1+v6+v4+hostnetwork': {
        'postgres': {
            'shared_buffers': '35GB',
            'max_connections': 1000,
            'effective_cache_size': '50GB',
            'work_mem': '256MB',
            'max_worker_processes': 32,
            'max_parallel_workers_per_gather': 12,
            'max_parallel_workers': 24,
            'wal_buffers': '512MB',
            'max_wal_size': '16GB',
            'autovacuum_max_workers': 6,
            'effective_io_concurrency': 200,
            'random_page_cost': 1.1,
            'checkpoint_timeout': '15min',
            'checkpoint_completion_target': 0.9,
            'wal_level': 'replica',
            'huge_pages': 'on',
            'synchronous_commit': 'on',
            'log_line_prefix': '%t [%p]: [%l-1] user=%u,db=%d ',
            'log_checkpoints': 'on',
            'log_connections': 'on',
            'log_disconnections': 'on',
            'log_lock_waits': 'on',
            'log_temp_files': '0',
            'log_autovacuum_min_duration': '0',
            'log_error_verbosity': 'default',
            'client_min_messages': 'notice',
            'log_min_messages': 'warning',
            'log_min_error_statement': 'error',
            'log_min_duration_statement': '1000',
        },
        'resources': {
            'cpu_request': '14',
            'cpu_limit': '15',
            'memory_request': '15Gi',
            'memory_limit': '15Gi',
        },
        'use_init_container': True,
        'node_image': 'COS_CONTAINERD',
        'client_image': 'ubuntu:24.04',
        'hugepages': {
            'hugepage_size2m': 19456,
            'hugepage_size1g': 0,
        },
        'host_network': True,
    }
}


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
    """Load and return benchmark config spec.

    Args:
        user_config: User provided configuration overrides.

    Returns:
        Merged benchmark configuration.
    """
    config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

    # Apply machine type overrides
    if FLAGS.postgres_gke_server_machine_type:
        # Update postgres nodepool
        vm_spec = config['container_cluster']['nodepools']['postgres']['vm_spec']
        for cloud in vm_spec:
            vm_spec[cloud]['machine_type'] = FLAGS.postgres_gke_server_machine_type

    if FLAGS.postgres_gke_client_machine_type:
        # Update nodepool
        client_vm_spec = config['container_cluster']['nodepools']['clients']['vm_spec']
        for cloud in client_vm_spec:
            client_vm_spec[cloud]['machine_type'] = FLAGS.postgres_gke_client_machine_type

        # Update VM group (following Redis pattern)
        if 'vm_groups' in config and 'clients' in config['vm_groups']:
            vm_group_spec = config['vm_groups']['clients']['vm_spec']
            for cloud in vm_group_spec:
                vm_group_spec[cloud]['machine_type'] = FLAGS.postgres_gke_client_machine_type

    # Auto-select disk type based on machine type for SERVER
    server_machine = config['container_cluster']['nodepools']['postgres']['vm_spec']['GCP']['machine_type']
    server_family = server_machine.split('-')[0]
    if FLAGS.postgres_gke_disk_type:
        server_disk_type = FLAGS.postgres_gke_disk_type
    else:
        server_disk_type = MACHINE_DISK_MAPPING.get(server_family, 'pd-ssd')

    # Auto-select disk type based on machine type for CLIENT
    client_machine = config['container_cluster']['nodepools']['clients']['vm_spec']['GCP']['machine_type']
    client_family = client_machine.split('-')[0]
    client_disk_type = MACHINE_DISK_MAPPING.get(client_family, 'pd-ssd')

    # Update boot disk configuration for postgres nodepool
    postgres_vm_spec = config['container_cluster']['nodepools']['postgres']['vm_spec']
    for cloud in postgres_vm_spec:
        postgres_vm_spec[cloud]['boot_disk_type'] = server_disk_type
        postgres_vm_spec[cloud]['boot_disk_size'] = FLAGS.postgres_gke_disk_size

    # Update boot disk configuration for clients nodepool
    clients_vm_spec = config['container_cluster']['nodepools']['clients']['vm_spec']
    for cloud in clients_vm_spec:
        clients_vm_spec[cloud]['boot_disk_type'] = client_disk_type
        # Client disk size is smaller (100GB default)
        clients_vm_spec[cloud]['boot_disk_size'] = 100

    # Apply HugePages system config if needed
    profile = OPTIMIZATION_PROFILES[FLAGS.postgres_gke_optimization_profile]
    if 'hugepages' in profile:
        logging.info('Enabling HugePages via GKE System Config')
        FLAGS.gke_node_system_config = data.ResourcePath(
            'container/postgres_sysbench/hugepages-node-config.yaml'
        )

    return config


def _GetPostgresPassword() -> str:
    """Get PostgreSQL password from run_uri."""
    return postgresql.GetPsqlUserPassword(FLAGS.run_uri)


def _GetPostgreSQLConfig() -> Dict[str, Any]:
    """Get effective PostgreSQL configuration based on profile and flags.

    Returns:
        Dictionary of PostgreSQL configuration parameters.
    """
    # Start with baseline
    profile = OPTIMIZATION_PROFILES[FLAGS.postgres_gke_optimization_profile]
    pg_config = OPTIMIZATION_PROFILES['baseline']['postgres'].copy()

    # Apply profile overrides (this is where v6 settings should apply)
    if 'postgres' in profile:
        pg_config.update(profile['postgres'])
        # Explicitly ensure huge_pages is carried over if present
        if 'huge_pages' in profile['postgres']:
            pg_config['huge_pages'] = profile['postgres']['huge_pages']

    # Apply flag overrides ONLY if explicitly set by user (not using defaults)
    # Use FLAGS['flag_name'].present to check if user explicitly set the flag
    if FLAGS['postgres_gke_shared_buffers'].present:
        pg_config['shared_buffers'] = FLAGS.postgres_gke_shared_buffers
    if FLAGS['postgres_gke_max_connections'].present:
        pg_config['max_connections'] = FLAGS.postgres_gke_max_connections
    if FLAGS['postgres_gke_effective_cache_size'].present:
        pg_config['effective_cache_size'] = FLAGS.postgres_gke_effective_cache_size
    if FLAGS['postgres_gke_work_mem'].present:
        pg_config['work_mem'] = FLAGS.postgres_gke_work_mem
    if FLAGS['postgres_gke_max_worker_processes'].present:
        pg_config['max_worker_processes'] = FLAGS.postgres_gke_max_worker_processes
    if FLAGS['postgres_gke_max_parallel_workers_per_gather'].present:
        pg_config['max_parallel_workers_per_gather'] = FLAGS.postgres_gke_max_parallel_workers_per_gather
    if FLAGS['postgres_gke_wal_buffers'].present:
        pg_config['wal_buffers'] = FLAGS.postgres_gke_wal_buffers
    if FLAGS['postgres_gke_max_wal_size'].present:
        pg_config['max_wal_size'] = FLAGS.postgres_gke_max_wal_size
    if FLAGS['postgres_gke_autovacuum_max_workers'].present:
        pg_config['autovacuum_max_workers'] = FLAGS.postgres_gke_autovacuum_max_workers

    return pg_config


def _PreparePostgreSQLCluster(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
    """Deploy PostgreSQL on the Kubernetes cluster.

    Args:
        bm_spec: Benchmark specification.
    """
    cluster = bm_spec.container_cluster
    profile = OPTIMIZATION_PROFILES[FLAGS.postgres_gke_optimization_profile]
    pg_config = _GetPostgreSQLConfig()

    # Determine disk type for storage class
    # Get machine type from config or flag
    if FLAGS.postgres_gke_server_machine_type:
        machine_type = FLAGS.postgres_gke_server_machine_type
    else:
        try:
            # Try to get from benchmark config
            machine_type = bm_spec.config.container_cluster.nodepools['postgres'].vm_spec['GCP']['machine_type']
        except (KeyError, AttributeError):
            # Default to c4-standard-16 if we can't find it
            machine_type = 'c4-standard-16'
            logging.warning('Could not determine machine type from config, using default: %s', machine_type)

    machine_family = machine_type.split('-')[0] if machine_type else 'c4'
    disk_type = FLAGS.postgres_gke_disk_type or MACHINE_DISK_MAPPING.get(machine_family, 'pd-ssd')

    hugepages = profile.get('hugepages')

    # Prepare template parameters
    resources = profile.get('resources', {})
    logging.info('DEBUG: Applied Resources from Profile: %s', resources)
    
    template_params = {
        'namespace': 'default',
        'postgres_version': '16',
        'postgres_version': '16',
        'postgres_user': 'benchmark',
        'postgres_password': _GetPostgresPassword(),
        'postgres_database': 'benchmark',
        'disk_size': f'{FLAGS.postgres_gke_disk_size}Gi',
        'disk_type': disk_type,
        'use_init_container': profile.get('use_init_container', True),
        'host_network': profile.get('host_network', False),
        'client_image': profile.get('client_image', 'ubuntu:20.04'),
        # Resource configuration from profile
        'cpu_request': resources.get('cpu_request', '6'),
        'cpu_limit': resources.get('cpu_limit', '10'),
        'memory_request': resources.get('memory_request', '15Gi'),
        'memory_limit': resources.get('memory_limit', '20Gi'),
        'hugepages': hugepages,
        **pg_config,  # Include all PostgreSQL parameters
    }

    # Dynamic Memory Adjustment:
    # If HugePages are enabled (38Gi), we MUST reduce the standard memory request/limit
    # to fit within the node's capacity (e.g., 64GB for standard-16).
    # 38Gi (HugePages) + 45Gi (Request) = 83Gi > 64GB -> Failure!
    if hugepages and hugepages.get('hugepage_size2m', 0) > 0:
        logging.info('HugePages enabled: Reducing standard memory request to 15Gi to fit node capacity.')
        template_params['memory_request'] = '15Gi'
        template_params['memory_limit'] = '15Gi'
    else:
        # If no HugePages, we can use the full memory for standard RAM
        logging.info('HugePages disabled: Using full memory profile (45Gi/55Gi).')
        template_params['memory_request'] = resources.get('memory_request', '45Gi')
        template_params['memory_limit'] = resources.get('memory_limit', '55Gi')

    # Apply manifests
    with kubernetes_helper.CreateRenderedManifestFile(
        'container/postgres_sysbench/postgres_all.yaml.j2',
        template_params
    ) as rendered_manifest:
        cluster.ApplyManifest(rendered_manifest.name)

    # Wait a bit for resources to be created
    import time
    logging.info('Waiting 30 seconds for resources to be created...')
    time.sleep(30)

    # Check initial pod status for debugging
    logging.info('Checking PostgreSQL deployment status...')
    try:
        pod_status_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                         'get', 'pods', '-n', 'default', '-o', 'wide']
        stdout, _, _ = vm_util.IssueCommand(pod_status_cmd, raise_on_failure=False)
        logging.info('Initial pod status:\n%s', stdout)

        # Also check PVC status
        pvc_status_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                         'get', 'pvc', '-n', 'default', '-o', 'wide']
        stdout, _, _ = vm_util.IssueCommand(pvc_status_cmd, raise_on_failure=False)
        logging.info('PVC status:\n%s', stdout)
    except Exception as e:
        logging.warning('Failed to get initial status: %s', e)

    # Wait for PostgreSQL pod to be ready (not StatefulSet ready replicas)
    try:
        # First wait for pod to exist and be running
        logging.info('Waiting for PostgreSQL pod to be ready (up to 30 minutes)...')
        
        @vm_util.Retry(max_retries=3, retryable_exceptions=(errors.VmUtil.IssueCommandTimeoutError, errors.VmUtil.IssueCommandError))
        def _WaitForPodReady():
            cluster.WaitForResource(
                'pod/postgres-standalone-0',  # resource_name
                'Ready',  # condition_name
                namespace='default',
                timeout=1800  # 30 minutes for large deployments with HugePages
            )
        
        _WaitForPodReady()
        logging.info('PostgreSQL pod is ready')

        # Give it more time to stabilize (important for large shared_buffers)
        logging.info('Waiting 60 seconds for PostgreSQL to fully stabilize...')
        time.sleep(60)

        # Verify PostgreSQL is actually accepting connections
        logging.info('Verifying PostgreSQL connectivity...')
        check_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                    'exec', '-n', 'default', 'postgres-standalone-0', '--', 'pg_isready', '-U', 'benchmark', '-d', 'benchmark']
        vm_util.IssueCommand(check_cmd)

        # Check if we can execute a query
        query_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                    'exec', '-n', 'default', 'postgres-standalone-0', '--', 'bash', '-c', f'PGPASSWORD={_GetPostgresPassword()} psql -U benchmark -d benchmark -c "SELECT 1"']
        vm_util.IssueCommand(query_cmd)
        logging.info('PostgreSQL query test successful')

    except Exception as e:
        # If waiting fails, gather debug info
        logging.error('PostgreSQL pod failed to become ready: %s', e)

        # Get pod details
        describe_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                       'describe', 'pod', '-n', 'default', '-l', 'app=postgres-standalone']
        stdout, _, _ = vm_util.IssueCommand(describe_cmd, raise_on_failure=False)
        logging.error('Pod description:\n%s', stdout)

        # Get pod logs
        logs_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                   'logs', '-n', 'default', '-l', 'app=postgres-standalone', '--tail=100']
        stdout, _, _ = vm_util.IssueCommand(logs_cmd, raise_on_failure=False)
        logging.error('Pod logs:\n%s', stdout)

        # Get events
        events_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                     'get', 'events', '-n', 'default', '--sort-by=.lastTimestamp']
        stdout, _, _ = vm_util.IssueCommand(events_cmd, raise_on_failure=False)
        logging.error('Recent events:\n%s', stdout)

        raise

    # Get Service IP
    # Get Pod IP (more reliable for private IP requirement)
    get_ip_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                 'get', 'pod', 'postgres-standalone-0', '-n', 'default', '-o', 'jsonpath={.status.podIP}']
    stdout, _, _ = vm_util.IssueCommand(get_ip_cmd)
    service_ip = stdout.strip() if stdout else 'postgres-standalone-0'

    bm_spec.postgres_service_ip = service_ip
    logging.info(f'PostgreSQL service available at: {service_ip}')


def _PrepareSysbenchClient(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
    """Prepare Sysbench on client VMs or pods.

    Args:
        bm_spec: Benchmark specification.
    """
    if FLAGS.postgres_gke_client_mode == 'vm':
        # Install sysbench on client VMs
        client_vms = bm_spec.vm_groups['clients']

        def install_sysbench(vm):
            # Install all dependencies needed for sysbench with retries
            # Retry loop for apt-get update to handle transient mirror sync issues
            vm.RemoteCommand('for i in {1..5}; do sudo apt-get update && break || sleep 15; done')
            vm.RemoteCommand('sudo apt-get install -y git build-essential automake libtool pkg-config')
            vm.RemoteCommand('sudo apt-get install -y libmysqlclient-dev libpq-dev')
            vm.RemoteCommand('sudo apt-get install -y postgresql-client')
            vm.Install('sysbench')

        background_tasks.RunThreaded(install_sysbench, client_vms)

    else:  # pod mode
        # Deploy client pod and install sysbench
        cluster = bm_spec.container_cluster
        profile = OPTIMIZATION_PROFILES[FLAGS.postgres_gke_optimization_profile]

        template_params = {
            'namespace': 'default',
            'client_image': profile.get('client_image', 'ubuntu:20.04'),
        }

        with kubernetes_helper.CreateRenderedManifestFile(
            'container/postgres_sysbench/client_pod.yaml.j2',
            template_params
        ) as rendered_manifest:
            cluster.ApplyManifest(rendered_manifest.name)

        # Wait for client pod - WaitForResource accepts namespace parameter
        cluster.WaitForResource('pod/postgres-client', 'Ready', namespace='default')

        # Install sysbench and dependencies in pod
        install_commands = [
            'for i in {1..5}; do apt-get update && break || sleep 15; done',
            'export DEBIAN_FRONTEND=noninteractive; apt-get install -y git build-essential automake libtool pkg-config',
            'export DEBIAN_FRONTEND=noninteractive; apt-get install -y libmysqlclient-dev libpq-dev',
            'export DEBIAN_FRONTEND=noninteractive; apt-get install -y sysbench postgresql-client',
        ]
        for cmd in install_commands:
            kubectl_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                          'exec', '-n', 'default', 'postgres-client', '--', 'bash', '-c', cmd]
            _, _, _ = vm_util.IssueCommand(kubectl_cmd)


def _LoadDatabase(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
    """Load initial data into PostgreSQL using Sysbench.

    Args:
        bm_spec: Benchmark specification.
    """
    postgres_ip = bm_spec.postgres_service_ip

    # Build sysbench prepare command
    sysbench_params = sysbench.SysbenchInputParameters(
        db_driver='pgsql',
        tables=FLAGS.sysbench_tables,
        table_size=FLAGS.sysbench_table_size,
        threads=FLAGS.sysbench_load_threads,
        db_user='benchmark',
        db_password=_GetPostgresPassword(),
        db_name='benchmark',
        host_ip=postgres_ip,
        port=5432,
        built_in_test=True,
        test=f'{sysbench.LUA_SCRIPT_PATH}oltp_read_write.lua',
    )

    if FLAGS.postgres_gke_client_mode == 'vm':
        # Run on client VM
        prepare_cmd = sysbench.BuildLoadCommand(sysbench_params)
        client_vm = bm_spec.vm_groups['clients'][0]
        client_vm.RemoteCommand(prepare_cmd)
    else:
        # Run in client pod
        # Manually construct command to avoid VM-specific paths and secure password
        lua_script = '/usr/share/sysbench/oltp_read_write.lua'
        
        cmd = (
            f'sysbench {lua_script} '
            f'--db-driver=pgsql '
            f'--tables={FLAGS.sysbench_tables} '
            f'--table_size={FLAGS.sysbench_table_size} '
            f'--threads={FLAGS.sysbench_load_threads} '
            f'--pgsql-user=benchmark '
            f'--pgsql-db=benchmark '
            f'--pgsql-host={postgres_ip} '
            f'--pgsql-port=5432 '
            f'prepare'
        )
        
        kubectl_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                      'exec', '-n', 'default', 'postgres-client', '--', 
                      'bash', '-c', f'PGPASSWORD={_GetPostgresPassword()} {cmd}']
        vm_util.IssueCommand(kubectl_cmd)

    logging.info('Database loaded successfully')


def Prepare(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
    """Prepare PostgreSQL and Sysbench for benchmarking.

    Args:
        bm_spec: Benchmark specification.
    """
    prepare_fns = [
        functools.partial(_PreparePostgreSQLCluster, bm_spec),
        functools.partial(_PrepareSysbenchClient, bm_spec),
    ]

    background_tasks.RunThreaded(lambda f: f(), prepare_fns)

    # Load database after both PostgreSQL and client are ready
    _LoadDatabase(bm_spec)


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
    """Run Sysbench against PostgreSQL.

    Args:
        bm_spec: Benchmark specification.

    Returns:
        List of performance samples.
    """
    postgres_ip = bm_spec.postgres_service_ip
    samples = []

    # Get list of workload types to run
    workload_types = FLAGS.sysbench_testname.split(',') if ',' in FLAGS.sysbench_testname else [FLAGS.sysbench_testname]

    for workload in workload_types:
        # Build sysbench run command
        sysbench_params = sysbench.SysbenchInputParameters(
            db_driver='pgsql',
            tables=FLAGS.sysbench_tables,
            table_size=FLAGS.sysbench_table_size,
            threads=FLAGS.sysbench_run_threads,
            report_interval=FLAGS.sysbench_report_interval,
            db_user='benchmark',
            db_password=_GetPostgresPassword(),
            db_name='benchmark',
            host_ip=postgres_ip,
            port=5432,
            built_in_test=True,
            test=f'{sysbench.LUA_SCRIPT_PATH}{workload}.lua',
        )

        # Execute benchmark
        if FLAGS.postgres_gke_client_mode == 'vm':
            run_cmd = sysbench.BuildRunCommand(sysbench_params)
            client_vm = bm_spec.vm_groups['clients'][0]
            stdout, _ = client_vm.RemoteCommand(run_cmd, timeout=FLAGS.sysbench_run_seconds + 60)
            logging.info('Sysbench completed successfully on VM')
        else:
            # Stability: Update statistics and flush buffers
            # Same logic as HA benchmark for consistency
            logging.info("Running ANALYZE to update statistics for benchmark tables...")
            for i in range(1, FLAGS.sysbench_tables + 1):
                table_name = f"sbtest{i}"
                analyze_cmd = f'PGPASSWORD={_GetPostgresPassword()} psql -h {postgres_ip} -U benchmark -d benchmark -c "ANALYZE {table_name};"'
                kubectl_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                              'exec', '-n', 'default', 'postgres-client', '--', 
                              'bash', '-c', analyze_cmd]
                vm_util.IssueCommand(kubectl_cmd)

            logging.info("Executing 3 Checkpoints to flush buffers...")
            checkpoint_cmd = f'PGPASSWORD={_GetPostgresPassword()} psql -h {postgres_ip} -U benchmark -d benchmark -c "CHECKPOINT;"'
            kubectl_chk = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                          'exec', '-n', 'default', 'postgres-client', '--', 
                          'bash', '-c', checkpoint_cmd]
            
            for i in range(3):
                logging.info(f"Issuing Checkpoint {i+1}/3")
                vm_util.IssueCommand(kubectl_chk)
                import time
                time.sleep(5)

            logging.info("Sleeping for 40 seconds to allow cluster to settle...")
            time.sleep(40)

            # Manually construct command for Pod mode
            lua_script = f'/usr/share/sysbench/{workload}.lua'
            
            run_cmd = (
                f'sysbench {lua_script} '
                f'--db-driver=pgsql '
                f'--tables={FLAGS.sysbench_tables} '
                f'--table_size={FLAGS.sysbench_table_size} '
                f'--threads={FLAGS.sysbench_run_threads} '
                f'--report-interval={FLAGS.sysbench_report_interval} '
                f'--time={FLAGS.sysbench_run_seconds} '
                f'--pgsql-user=benchmark '
                f'--pgsql-db=benchmark '
                f'--pgsql-host={postgres_ip} '
                f'--pgsql-port=5432 '
                f'run'
            )
            
            kubectl_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                          'exec', '-n', 'default', 'postgres-client', '--', 
                          'bash', '-c', f'PGPASSWORD={_GetPostgresPassword()} {run_cmd}']
            stdout, _, _ = vm_util.IssueCommand(kubectl_cmd, timeout=FLAGS.sysbench_run_seconds + 120)
            logging.info('Sysbench completed successfully on pod')

        # Log output for debugging
        logging.debug('Sysbench output (first 500 chars): %s', stdout[:500] if stdout else 'No output')

        # Parse results
        metadata = sysbench.GetMetadata(sysbench_params)
        metadata.update({
            'optimization_profile': FLAGS.postgres_gke_optimization_profile,
            'postgres_shared_buffers': _GetPostgreSQLConfig()['shared_buffers'],
            'postgres_effective_cache_size': _GetPostgreSQLConfig()['effective_cache_size'],
            'machine_type': FLAGS.postgres_gke_server_machine_type or 'c4-standard-16',
            'disk_type': FLAGS.postgres_gke_disk_type or 'auto',
            'workload_type': workload,
        })

        # Parse sysbench output
        try:
            time_series_samples = sysbench.ParseSysbenchTimeSeries(stdout, metadata)
            samples.extend(time_series_samples)
            logging.info('Parsed %d time series samples', len(time_series_samples))
        except Exception as e:
            logging.warning('Failed to parse time series: %s', e)

        try:
            latency_samples = sysbench.ParseSysbenchLatency([stdout], metadata)
            samples.extend(latency_samples)
            logging.info('Parsed %d latency samples', len(latency_samples))
        except Exception as e:
            logging.warning('Failed to parse latency: %s', e)

        try:
            transaction_samples = sysbench.ParseSysbenchTransactions(stdout, metadata)
            samples.extend(transaction_samples)
            logging.info('Parsed %d transaction samples', len(transaction_samples))
        except Exception as e:
            logging.warning('Failed to parse transactions: %s', e)

        if not samples:
            logging.error('No samples parsed from sysbench output. Output was: %s', stdout[:1000])

    logging.info('Total samples collected: %d', len(samples))
    return samples


def Cleanup(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
    """Clean up PostgreSQL resources.

    Args:
        bm_spec: Benchmark specification.
    """
    logging.info('Cleaning up PostgreSQL resources...')

    # 1. Delete StatefulSet
    cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
           'delete', 'statefulset', 'postgres-standalone', '-n', 'default', '--ignore-not-found']
    vm_util.IssueCommand(cmd)

    # 2. Delete the Client Pod
    cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 'delete', 'pod', 'postgres-client', '-n', 'default', '--ignore-not-found']
    vm_util.IssueCommand(cmd)

    # 3. Explicitly delete all PVCs to ensure disks are released
    cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
           'delete', 'pvc', '--all', '-n', 'default', '--ignore-not-found']
    vm_util.IssueCommand(cmd)

    logging.info('Cleanup complete.')