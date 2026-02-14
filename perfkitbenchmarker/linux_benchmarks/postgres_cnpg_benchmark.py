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

"""Run Sysbench against PostgreSQL HA (CNPG) on GKE.

This benchmark measures the performance of a High Availability PostgreSQL cluster
managed by the CloudNativePG (CNPG) operator. It deploys a Primary + 2 Replicas
topology.
"""

import logging
import base64
import time
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import vm_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import sysbench
from perfkitbenchmarker.linux_packages import postgresql
import hashlib

FLAGS = flags.FLAGS

flags.DEFINE_string('postgres_cnpg_version', '16.1', 'PostgreSQL version.')
flags.DEFINE_integer('postgres_cnpg_instances', 3, 'Number of PostgreSQL instances.')
flags.DEFINE_string('postgres_cnpg_storage_class', 'hyperdisk-balanced', 'StorageClass.')
flags.DEFINE_string('postgres_cnpg_disk_size', '500Gi', 'Disk size.')
flags.DEFINE_boolean('postgres_cnpg_regional', False, 'Regional GKE cluster.')

flags.DEFINE_enum(
    'postgres_cnpg_optimization_profile',
    'baseline',
    ['baseline', 'v1', 'v2', 'v3', 'v4', 'v6', 'v1+v6', 'v1+v6+v4', 'v1+v6+v4+hostnetwork'],
    'Optimization profile to use'
)

CNPG_OPERATOR_URL = "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.23/releases/cnpg-1.23.2.yaml"
BENCHMARK_NAME = 'postgres_cnpg_benchmark'

BENCHMARK_CONFIG = """
postgres_cnpg_benchmark:
  description: >
    Run Sysbench against HA PostgreSQL (CNPG) on GKE.
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
        vm_count: 3
      clients:
        vm_spec:
          GCP:
            machine_type: c4-standard-16
            zone: us-central1-a
            boot_disk_size: 100
            boot_disk_type: hyperdisk-balanced
        vm_count: 1
  flags:
    sysbench_tables: 10
    sysbench_table_size: 4000000
    sysbench_run_threads: 512
    sysbench_run_seconds: 300
    sysbench_load_threads: 16
    sysbench_testname: oltp_read_write
"""

MACHINE_DISK_MAPPING = {
    'c4': 'hyperdisk-balanced',
    'c4d': 'hyperdisk-balanced',
    'c4a': 'hyperdisk-balanced',
    'n2': 'pd-ssd',
    'n2d': 'pd-ssd',
}

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
            'huge_pages': 'off',
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
            'huge_pages': 'off',
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
    config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)
    
    # Apply HugePages system config if needed
    profile_name = FLAGS.postgres_cnpg_optimization_profile
    if profile_name in OPTIMIZATION_PROFILES:
        profile = OPTIMIZATION_PROFILES[profile_name]
        if 'hugepages' in profile:
            logging.info('Enabling HugePages via GKE System Config')
            # Use the existing file from the other benchmark (shared resource)
            FLAGS.gke_node_system_config = data.ResourcePath(
                'container/postgres_sysbench/hugepages-node-config.yaml'
            )
            
    return config

def _GetPostgresPassword() -> str:
    """Get PostgreSQL password from run_uri."""
    return postgresql.GetPsqlUserPassword(FLAGS.run_uri)


def _GetMD5PostgresPassword(user, password):
    """
    Generate an MD5 password hash compatible with PostgreSQL.
    Format: 'md5' + md5(password + user)
    """
    payload = (password + user).encode('utf-8')
    return 'md5' + hashlib.md5(payload).hexdigest()


def _InstallCNPGOperator(cluster, sc_name):
    if sc_name == 'hyperdisk-balanced':
        logging.info('Applying Hyperdisk StorageClass...')
        sc_file = data.ResourcePath('container/postgres_cnpg/hyperdisk_storageclass.yaml')
        cmd_sc = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 'apply', '-f', sc_file]
        vm_util.IssueCommand(cmd_sc)
    elif sc_name == 'pd-ssd':
        logging.info('Applying PD-SSD StorageClass...')
        sc_file = data.ResourcePath('container/postgres_cnpg/pd_ssd_storageclass.yaml')
        cmd_sc = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 'apply', '-f', sc_file]
        vm_util.IssueCommand(cmd_sc)

    logging.info('Installing CloudNativePG Operator from %s', CNPG_OPERATOR_URL)
    cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 'apply', '--server-side', '-f', CNPG_OPERATOR_URL]
    vm_util.IssueCommand(cmd)
    
    logging.info('Waiting for CNPG Operator to be ready...')
    cluster.WaitForResource(
        'deployment/cnpg-controller-manager', 'Available', namespace='cnpg-system', timeout=600
    )

    # Delete Validating Webhook to bypass strict memory validation (e.g. Memory < shared_buffers with HugePages)
    logging.info('Deleting CNPG Validating Webhook to bypass validation checks...')
    cmd_webhook = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 'delete',
                   'validatingwebhookconfiguration', 'cnpg-validating-webhook-configuration',
                   '--ignore-not-found=true']
    vm_util.IssueCommand(cmd_webhook)

def _PrepareCluster(bm_spec) -> None:
    cluster = bm_spec.container_cluster
    
    if FLAGS.postgres_cnpg_regional:
        topology_key = 'topology.kubernetes.io/zone'
        logging.info("Regional Cluster: Using Zone Anti-Affinity")
    else:
        topology_key = 'kubernetes.io/hostname'
        logging.info("Zonal Cluster: Using Node Anti-Affinity")

    # Load Profile
    profile_name = FLAGS.postgres_cnpg_optimization_profile
    profile = OPTIMIZATION_PROFILES.get(profile_name, OPTIMIZATION_PROFILES['baseline'])
    logging.info('Using Optimization Profile: %s', profile_name)

    hugepages = profile.get('hugepages')
    resources = profile.get('resources', {})
    
    # Dynamic Memory Adjustment for HugePages
    hugepages_2mi_request = ''
    if hugepages and hugepages.get('hugepage_size2m', 0) > 0:
        count_2m = hugepages['hugepage_size2m']
        size_mi = count_2m * 2
        hugepages_2mi_request = f"{size_mi}Mi"
        logging.info(f'HugePages enabled: Requesting {hugepages_2mi_request} (Count: {count_2m})')
        logging.info('Reducing standard memory request to 15Gi to fit node capacity.')
        memory_request = '15Gi'
        memory_limit = '15Gi'
    else:
        logging.info('HugePages disabled: Using full memory profile.')
        memory_request = resources.get('memory_request', '45Gi')
        memory_limit = resources.get('memory_limit', '55Gi')

    cluster_params = {
        'namespace': 'default',
        'postgres_version': FLAGS.postgres_cnpg_version,
        'instances': FLAGS.postgres_cnpg_instances,
        'postgres_user': 'benchmark',
        'postgres_password': _GetPostgresPassword(),
        'postgres_database': 'benchmark',
        'storage_class': FLAGS.postgres_cnpg_storage_class,
        'disk_size': FLAGS.postgres_cnpg_disk_size,
        'topology_key': topology_key,
        # Resources
        'cpu_request': resources.get('cpu_request', '14'),
        'cpu_limit': resources.get('cpu_limit', '15'),
        'memory_request': memory_request,
        'memory_limit': memory_limit,
        # HugePages Resource String
        'hugepages_2mi_request': hugepages_2mi_request,
        # Optimization Flags
        'hugepages': hugepages,
        'host_network': profile.get('host_network', False),
        'use_init_container': profile.get('use_init_container', True),
        # Postgres Params from Profile
        'postgres_parameters': profile['postgres']
    }
    
    logging.info(f"DEBUG: cluster_params keys: {list(cluster_params.keys())}")
    logging.info(f"DEBUG: hugepages_2mi_request value: '{cluster_params['hugepages_2mi_request']}'")

    with kubernetes_helper.CreateRenderedManifestFile(
        'container/postgres_cnpg/postgres_cluster.yaml.j2',
        cluster_params
    ) as rendered_manifest:
        cluster.ApplyManifest(rendered_manifest.name)

def _WaitForResources(bm_spec):
    cluster = bm_spec.container_cluster
    logging.info('Waiting for Postgres Cluster to be healthy...')
    
    start_time = time.time()
    while time.time() - start_time < 900:
        cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 
               'get', 'cluster', 'gke-pg-cluster', '-n', 'default', 
               '-o', 'jsonpath={.status.instances}/{.status.readyInstances}']
        stdout, _, _ = vm_util.IssueCommand(cmd, raise_on_failure=False)
        
        if stdout.strip() == f"{FLAGS.postgres_cnpg_instances}/{FLAGS.postgres_cnpg_instances}":
            logging.info('Postgres Cluster Healthy: %s', stdout.strip())
            break
        logging.info('Cluster Status: %s. Waiting...', stdout.strip())
        time.sleep(15)

    bm_spec.postgres_service_ip = "gke-pg-cluster-rw"
    logging.info('Using Postgres RW Service: %s', bm_spec.postgres_service_ip)
    
    logging.info('Verifying Connectivity to Primary...')
    start_time = time.time()
    connected = False
    while time.time() - start_time < 900:
        try:
             check_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                        'exec', '-n', 'default', 'postgres-client', '--', 
                        'pg_isready', '-h', bm_spec.postgres_service_ip, '-U', 'benchmark', '-d', 'benchmark']
             vm_util.IssueCommand(check_cmd, raise_on_failure=True)
             
             query_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig,
                        'exec', '-n', 'default', 'postgres-client', '--', 
                        'bash', '-c', f'PGPASSWORD={_GetPostgresPassword()} psql -h {bm_spec.postgres_service_ip} -U benchmark -d benchmark -c "SELECT 1"']
             vm_util.IssueCommand(query_cmd, raise_on_failure=True)
             
             logging.info('Connectivity Verified.')
             connected = True
             break
        except Exception:
             logging.info('Waiting for DB connectivity...')
             time.sleep(10)
             
    logging.info('Connectivity Wait Loop Finished.')
    if not connected:
        raise errors.Benchmarks.PrepareException("Timeout waiting for DB connectivity to %s" % bm_spec.postgres_service_ip)

def _ConfigureDatabasePermissions(bm_spec):
    logging.info("Configuring Database Permissions (MD5)...")
    cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 
           'get', 'secret', 'gke-pg-cluster-superuser', '-n', 'default', 
           '-o', 'jsonpath={.data.password}']
    stdout, _, _ = vm_util.IssueCommand(cmd)
    password = base64.b64decode(stdout).decode('utf-8')
    
    # Calculate secure MD5 hash for the dynamic password
    # This ensures we don't log plaintext passwords and matches Postgres format
    md5_hash = _GetMD5PostgresPassword('benchmark', _GetPostgresPassword())
    
    user_cmds = f"ALTER USER benchmark WITH PASSWORD '{md5_hash}'; GRANT pg_monitor, pg_checkpoint TO benchmark;"
    bash_script = f"export PGPASSWORD={password}; psql -h gke-pg-cluster-rw -U postgres -d postgres -c \"{user_cmds}\""
    
    exec_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 
                'exec', '-n', 'default', 'postgres-client', '--', 'bash', '-c', bash_script]
                
    @vm_util.Retry(max_retries=6, retryable_exceptions=(errors.VmUtil.IssueCommandError,))
    def _RunAlter():
        vm_util.IssueCommand(exec_cmd)
        
    _RunAlter()
    logging.info("Permissions configured.")

def _PrepareSysbenchClient(bm_spec):
    cluster = bm_spec.container_cluster
    
    # Load profile to get client image
    profile_name = FLAGS.postgres_cnpg_optimization_profile
    profile = OPTIMIZATION_PROFILES.get(profile_name, OPTIMIZATION_PROFILES['baseline'])
    client_image = profile.get('client_image', 'ubuntu:22.04') 
    
    template_params = {'namespace': 'default', 'client_image': client_image}
    
    try:
        with kubernetes_helper.CreateRenderedManifestFile(
            'container/postgres_sysbench/client_pod.yaml.j2', template_params
        ) as rendered_manifest:
            cluster.ApplyManifest(rendered_manifest.name)
    except errors.Resource.RetryableCreationError:
        pass

    cluster.WaitForResource('pod/postgres-client', 'Ready', namespace='default')

    install_commands = [
        'for i in {1..5}; do apt-get update && break || sleep 5; done',
        'apt-get install -y git build-essential automake libtool pkg-config libmysqlclient-dev libpq-dev sysbench postgresql-client'
    ]
    for cmd in install_commands:
        k_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 'exec', '-n', 'default', 'postgres-client', '--', 'bash', '-c', f'export DEBIAN_FRONTEND=noninteractive; {cmd}']
        vm_util.IssueCommand(k_cmd)

def Prepare(bm_spec) -> None:
    # Respect the Storage Class flag passed from CLI (e.g. by postgres-ha-cnpg-baseline-runcount.sh)
    sc_name = FLAGS.postgres_cnpg_storage_class
    logging.info(f"Using Storage Class from Flag: {sc_name}")

    _InstallCNPGOperator(bm_spec.container_cluster, sc_name)
    _PrepareCluster(bm_spec)
    _PrepareSysbenchClient(bm_spec)
    bm_spec.container_cluster.WaitForResource('pod/postgres-client', 'Ready', namespace='default', timeout=300)
    _WaitForResources(bm_spec)
    _ConfigureDatabasePermissions(bm_spec)

def Run(bm_spec) -> List[sample.Sample]:
    testname = FLAGS.sysbench_testname
    
    prepare_cmd = (
        f'sysbench {testname} --db-driver=pgsql --pgsql-host=gke-pg-cluster-rw --pgsql-port=5432 '
        f'--pgsql-user=benchmark --pgsql-password={_GetPostgresPassword()} --pgsql-db=benchmark '
        f'--tables={FLAGS.sysbench_tables} --table_size={FLAGS.sysbench_table_size} '
        f'--threads={FLAGS.sysbench_load_threads} prepare'
    )
    _RunSysbenchCommand(bm_spec, prepare_cmd, timeout=7200)

    # Stability: Update statistics and flush buffers
    logging.info("Running ANALYZE to update statistics for benchmark tables...")
    for i in range(1, FLAGS.sysbench_tables + 1):
        table_name = f"sbtest{i}"
        analyze_cmd = f'PGPASSWORD={_GetPostgresPassword()} psql -h gke-pg-cluster-rw -U benchmark -d benchmark -c "ANALYZE {table_name};"'
        _RunSysbenchCommand(bm_spec, analyze_cmd)

    logging.info("Executing 3 Checkpoints to flush buffers...")
    checkpoint_cmd = f'PGPASSWORD={_GetPostgresPassword()} psql -h gke-pg-cluster-rw -U benchmark -d benchmark -c "CHECKPOINT;"'
    for i in range(3):
        logging.info(f"Issuing Checkpoint {i+1}/3")
        _RunSysbenchCommand(bm_spec, checkpoint_cmd)
        time.sleep(5)

    logging.info("Sleeping for 40 seconds to allow cluster to settle...")
    time.sleep(40)
    
    run_cmd = (
        f'sysbench {testname} --db-driver=pgsql --pgsql-host=gke-pg-cluster-rw --pgsql-port=5432 '
        f'--pgsql-user=benchmark --pgsql-password={_GetPostgresPassword()} --pgsql-db=benchmark '
        f'--tables={FLAGS.sysbench_tables} --table_size={FLAGS.sysbench_table_size} '
        f'--threads={FLAGS.sysbench_run_threads} --time={FLAGS.sysbench_run_seconds} '
        f'--report-interval=10 run'
    )
    run_timeout = int(FLAGS.sysbench_run_seconds) + 600
    stdout = _RunSysbenchCommand(bm_spec, run_cmd, timeout=run_timeout)

    results = []
    metadata = {
        'profile': FLAGS.postgres_cnpg_optimization_profile,
        'threads': FLAGS.sysbench_run_threads
    }
    results.extend(sysbench.ParseSysbenchTransactions(stdout, metadata))
    results.extend(sysbench.ParseSysbenchLatency([stdout], metadata))
    return results

def _RunSysbenchCommand(bm_spec, cmd, timeout=None):
    kubectl_cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 'exec', '-n', 'default', 'postgres-client', '--', 'bash', '-c', cmd]
    stdout, _, _ = vm_util.IssueCommand(kubectl_cmd, timeout=timeout)
    return stdout

def Cleanup(bm_spec) -> None:
    """Cleanup resources."""
    logging.info('Cleaning up (Deleting Cluster CRD and PVCs)...')
    
    # 1. Delete the Postgres Cluster CRD (this triggers the operator to delete the pods)
    cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 'delete', 'cluster', 'gke-pg-cluster', '-n', 'default', '--ignore-not-found']
    vm_util.IssueCommand(cmd)

    # 2. Delete the Client Pod
    cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 'delete', 'pod', 'postgres-client', '-n', 'default', '--ignore-not-found']
    vm_util.IssueCommand(cmd)
    
    # 3. Explicitly delete all PVCs to ensure disks are released
    # Note: Sometimes the operator might recreate them faster than we delete if the cluster object isn't fully gone,
    # but doing this is critical for persistent storage cleanup.
    cmd = [FLAGS.kubectl, '--kubeconfig', FLAGS.kubeconfig, 'delete', 'pvc', '--all', '-n', 'default', '--ignore-not-found']
    vm_util.IssueCommand(cmd)

    logging.info('Cleanup complete.')
