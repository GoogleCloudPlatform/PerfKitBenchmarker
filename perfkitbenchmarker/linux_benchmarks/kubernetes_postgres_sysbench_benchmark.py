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

"""Run Sysbench against PostgreSQL on Kubernetes.

This benchmark measures the performance of PostgreSQL deployed on Kubernetes
using Sysbench. It supports multiple machine architectures and optimization
profiles across different environments.

This benchmark deploys PostgreSQL as a Kubernetes StatefulSet and uses native
client pods for Sysbench load generation.
"""

import functools
import logging
import os
import time
from typing import Any, Dict, List

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import postgresql
from perfkitbenchmarker.linux_packages import sysbench
from perfkitbenchmarker.resources.container_service import kubernetes_commands

FLAGS = flags.FLAGS


CLOUD_STORAGE_PROVISIONERS = {
    'GCP': 'kubernetes.io/gce-pd',
    'AWS': 'kubernetes.io/aws-ebs',
    'Azure': 'kubernetes.io/azure-disk',
}


# Infrastructure flags
flags.DEFINE_string(
    'postgres_kubernetes_server_machine_type',
    None,
    'Machine type for PostgreSQL server nodes',
)
flags.DEFINE_string(
    'postgres_kubernetes_client_machine_type',
    None,
    'Machine type for client nodes',
)
flags.DEFINE_string(
    'kubernetes_postgres_sysbench_template_path',
    'container/postgres_sysbench/postgres_all.yaml.j2',
    'Path to the PostgreSQL Kubernetes manifest template file.',
)
flags.DEFINE_bool(
    'postgres_kubernetes_use_init_container',
    True,
    'Whether to use init container for system updates (baseline: True, v2:'
    ' False)',
)
flags.DEFINE_string(
    'postgres_kubernetes_client_cpu_request',
    '4',
    'CPU request for Sysbench client pod',
)
flags.DEFINE_string(
    'postgres_kubernetes_client_memory_request',
    '10Gi',
    'Memory request for Sysbench client pod',
)
flags.DEFINE_string(
    'postgres_kubernetes_client_cpu_limit',
    '8',
    'CPU limit for Sysbench client pod',
)
flags.DEFINE_string(
    'postgres_kubernetes_client_memory_limit',
    '20Gi',
    'Memory limit for Sysbench client pod',
)

# Note: sysbench_load_threads is already defined in sysbench_benchmark.py

BENCHMARK_NAME = 'kubernetes_postgres_sysbench'
BENCHMARK_CONFIG = """
kubernetes_postgres_sysbench:
  description: >
    Run Sysbench against PostgreSQL on Kubernetes.
    Supports multiple machine types and optimization profiles.
  container_cluster:
    cloud: GCP
    type: Kubernetes
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: c4-standard-16
      AWS:
        machine_type: m7i.4xlarge
      Azure:
        machine_type: Standard_D16s_v5
    nodepools:
      postgres:
        vm_spec:
          GCP:
            machine_type: c4-standard-16
            zone: us-central1-a
            boot_disk_size: 500
            boot_disk_type: hyperdisk-balanced
          AWS:
            machine_type: m7i.4xlarge
            zone: us-east-1a
            boot_disk_size: 500
            boot_disk_type: gp3
          Azure:
            machine_type: Standard_D16s_v5
            zone: eastus-1
            boot_disk_size: 500
            boot_disk_type: Premium_LRS
        vm_count: 1
      clients:
        vm_spec:
          GCP:
            machine_type: c4-standard-16
            zone: us-central1-a
            boot_disk_size: 100
            boot_disk_type: hyperdisk-balanced
          AWS:
            machine_type: m7i.4xlarge
            zone: us-east-1a
            boot_disk_size: 100
            boot_disk_type: gp3
          Azure:
            machine_type: Standard_D16s_v5
            zone: eastus-1
            boot_disk_size: 100
            boot_disk_type: Premium_LRS
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


def CheckPrerequisites(benchmark_config) -> None:
  """Verifies that benchmark setup is correct."""
  template_path = FLAGS.kubernetes_postgres_sysbench_template_path
  if not os.path.exists(template_path):
    try:
      data.ResourcePath(template_path)
    except errors.Setup.InvalidSetupError as exc:
      raise errors.Setup.InvalidConfigurationError(
          f'Could not find manifest template file: {template_path}'
      ) from exc

  if FLAGS.gke_node_system_config:
    cloud = benchmark_config.container_cluster.cloud
    if cloud != 'GCP':
      raise errors.Setup.InvalidConfigurationError(
          f'gke_node_system_config is only supported on GCP, but cloud {cloud}'
          ' was requested.'
      )


def GetConfig(user_config: Dict[str, Any]) -> Dict[str, Any]:
  """Load and return benchmark config spec.

  Args:
      user_config: User provided configuration overrides.

  Returns:
      Merged benchmark configuration.
  """
  config = configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

  # Apply machine type overrides
  if FLAGS.postgres_kubernetes_server_machine_type:
    # Update postgres nodepool
    vm_spec = config['container_cluster']['nodepools']['postgres']['vm_spec']
    for cloud in vm_spec:
      vm_spec[cloud][
          'machine_type'
      ] = FLAGS.postgres_kubernetes_server_machine_type

    # Update default root nodepool (if it exists)
    if 'vm_spec' in config['container_cluster']:
      root_vm_spec = config['container_cluster']['vm_spec']
      for cloud in root_vm_spec:
        root_vm_spec[cloud][
            'machine_type'
        ] = FLAGS.postgres_kubernetes_server_machine_type

  if FLAGS.postgres_kubernetes_client_machine_type:
    # Update nodepool
    client_vm_spec = config['container_cluster']['nodepools']['clients'][
        'vm_spec'
    ]
    for cloud in client_vm_spec:
      client_vm_spec[cloud][
          'machine_type'
      ] = FLAGS.postgres_kubernetes_client_machine_type

  return config


def _GetPostgresPassword() -> str:
  """Get PostgreSQL password from run_uri."""
  return postgresql.GetPsqlUserPassword(FLAGS.run_uri)


def _PreparePostgreSQLCluster(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Deploy PostgreSQL on the Kubernetes cluster.

  Args:
      bm_spec: Benchmark specification.
  """
  cluster = bm_spec.container_cluster
  cloud = cluster.CLOUD
  storage_class_provisioner = CLOUD_STORAGE_PROVISIONERS.get(cloud)
  if not storage_class_provisioner:
    raise errors.Setup.InvalidConfigurationError(
        f'Unsupported cloud for storage provisioner: {cloud}'
    )

  default_cloud_disk_types = {
      'GCP': 'hyperdisk-balanced',
      'AWS': 'gp3',
      'Azure': 'Premium_LRS',
  }
  disk_type = FLAGS.data_disk_type or default_cloud_disk_types.get(
      cloud, 'hyperdisk-balanced'
  )

  template_params = {
      'namespace': 'default',
      'postgres_version': '16',
      'storage_class': 'postgres-storage-class',
      'create_storage_class': True,
      'storage_class_provisioner': storage_class_provisioner,
      'postgres_user': 'benchmark',
      'postgres_password': _GetPostgresPassword(),
      'postgres_database': 'benchmark',
      'disk_size': f'{FLAGS.data_disk_size or 100}Gi',
      'disk_type': disk_type,
      # Default baseline parameters so the default postgres_all.yaml.j2 works
      'use_init_container': FLAGS.postgres_kubernetes_use_init_container,
      'host_network': False,
      'client_image': 'ubuntu:20.04',
      'cpu_request': '6',
      'cpu_limit': '10',
      'memory_request': '15Gi',
      'memory_limit': '20Gi',
      'max_connections': 1000,
      'shared_buffers': '15GB',
      'effective_cache_size': '30GB',
      'work_mem': '64MB',
      'max_worker_processes': 20,
      'max_parallel_workers_per_gather': 8,
      'max_parallel_workers': 12,
      'wal_buffers': '64MB',
      'max_wal_size': '16GB',
      'checkpoint_timeout': '5min',
      'checkpoint_completion_target': '0.9',
      'effective_io_concurrency': 100,
      'random_page_cost': '1.1',
      'autovacuum_max_workers': 3,
  }

  # Allow hostNetwork pods in default namespace
  label_cmd = [
      FLAGS.kubectl,
      '--kubeconfig',
      FLAGS.kubeconfig,
      'label',
      'namespace',
      'default',
      'pod-security.kubernetes.io/enforce=privileged',
      '--overwrite',
  ]
  vm_util.IssueCommand(label_cmd)

  # Apply manifests
  kubernetes_commands.ApplyManifest(
      FLAGS.kubernetes_postgres_sysbench_template_path, **template_params
  )

  # Wait for PostgreSQL pod to be ready (not StatefulSet ready replicas)
  try:
    # First wait for pod to exist and be running
    logging.info('Waiting for PostgreSQL pod to be ready (up to 30 minutes)...')

    @vm_util.Retry(
        max_retries=3,
        retryable_exceptions=(
            errors.VmUtil.IssueCommandTimeoutError,
            errors.VmUtil.IssueCommandError,
        ),
    )
    def _WaitForPodReady():
      cluster.WaitForResource(
          'pod/postgres-standalone-0',  # resource_name
          'Ready',  # condition_name
          namespace='default',
          timeout=1800,  # 30 minutes for large deployments with HugePages
      )

    _WaitForPodReady()
    logging.info('PostgreSQL pod is ready')

    # Verify PostgreSQL is actually accepting connections using active polling
    logging.info('Polling for PostgreSQL connectivity...')

    @vm_util.Retry(
        max_retries=12,
        poll_interval=5,
        retryable_exceptions=(errors.VmUtil.IssueCommandError,),
    )
    def _WaitForPostgresReady():
      check_cmd = [
          FLAGS.kubectl,
          '--kubeconfig',
          FLAGS.kubeconfig,
          'exec',
          '-n',
          'default',
          'postgres-standalone-0',
          '--',
          'pg_isready',
          '-U',
          'benchmark',
          '-d',
          'benchmark',
      ]
      vm_util.IssueCommand(check_cmd)

      # Check if we can execute a query
      query_cmd = [
          FLAGS.kubectl,
          '--kubeconfig',
          FLAGS.kubeconfig,
          'exec',
          '-n',
          'default',
          'postgres-standalone-0',
          '--',
          'bash',
          '-c',
          'psql -U benchmark -d benchmark -c "SELECT 1"',
      ]
      vm_util.IssueCommand(query_cmd)

    _WaitForPostgresReady()
    logging.info('PostgreSQL connectivity and query test successful')

  except Exception as e:  # pylint: disable=broad-exception-caught
    # If waiting fails, gather debug info
    logging.error('PostgreSQL pod failed to become ready: %s', e)

    get_ss_cmd = [
        FLAGS.kubectl,
        '--kubeconfig',
        FLAGS.kubeconfig,
        'get',
        'statefulset',
        '-n',
        'default',
        '-o',
        'jsonpath={.items[0].metadata.name}',
    ]
    stdout, _, _ = vm_util.IssueCommand(get_ss_cmd, raise_on_failure=False)
    ss_name = stdout.strip() if stdout else 'postgres-standalone'

    describe_pods_cmd = [
        FLAGS.kubectl,
        '--kubeconfig',
        FLAGS.kubeconfig,
        'describe',
        'pods',
        '-n',
        'default',
    ]
    stdout, _, _ = vm_util.IssueCommand(
        describe_pods_cmd, raise_on_failure=False
    )
    logging.error('All Pods description:\n%s', stdout)

    describe_ss_cmd = [
        FLAGS.kubectl,
        '--kubeconfig',
        FLAGS.kubeconfig,
        'describe',
        'statefulset',
        ss_name,
        '-n',
        'default',
    ]
    stdout, _, _ = vm_util.IssueCommand(describe_ss_cmd, raise_on_failure=False)
    logging.error('StatefulSet description:\n%s', stdout)

    db_pod_name = f'{ss_name}-0'
    logs_cmd = [
        FLAGS.kubectl,
        '--kubeconfig',
        FLAGS.kubeconfig,
        'logs',
        db_pod_name,
        '-n',
        'default',
        '--tail=100',
    ]
    stdout, _, _ = vm_util.IssueCommand(logs_cmd, raise_on_failure=False)
    logging.error('Database Pod logs:\n%s', stdout)

    raise

  # Get Service IP
  # Get Pod IP (more reliable for private IP requirement)
  get_ip_cmd = [
      FLAGS.kubectl,
      '--kubeconfig',
      FLAGS.kubeconfig,
      'get',
      'pod',
      'postgres-standalone-0',
      '-n',
      'default',
      '-o',
      'jsonpath={.status.podIP}',
  ]
  stdout, _, _ = vm_util.IssueCommand(get_ip_cmd)
  service_ip = stdout.strip() if stdout else 'postgres-standalone-0'

  bm_spec.postgres_service_ip = service_ip  # pytype: disable=attribute-error
  logging.info('PostgreSQL service available at: %s', service_ip)


def _PrepareSysbenchClient(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Prepare Sysbench on client pods.

  Args:
      bm_spec: Benchmark specification.
  """
  # Deploy client pod and install sysbench
  cluster = bm_spec.container_cluster

  # Create K8s Secret for sysbench password (reviewer feedback)
  logging.info('Creating sysbench-passwords secret...')
  vm_util.IssueCommand([
      FLAGS.kubectl,
      '--kubeconfig',
      FLAGS.kubeconfig,
      'delete',
      'secret',
      'sysbench-passwords',
      '-n',
      'default',
      '--ignore-not-found',
  ])
  vm_util.IssueCommand([
      FLAGS.kubectl,
      '--kubeconfig',
      FLAGS.kubeconfig,
      'create',
      'secret',
      'generic',
      'sysbench-passwords',
      '--from-literal=benchmark-password=' + _GetPostgresPassword(),
      '-n',
      'default',
  ])

  template_params = {
      'namespace': 'default',
      'client_image': 'ubuntu:20.04',
      'client_cpu_request': FLAGS.postgres_kubernetes_client_cpu_request,
      'client_cpu_limit': FLAGS.postgres_kubernetes_client_cpu_limit,
      'client_memory_request': FLAGS.postgres_kubernetes_client_memory_request,
      'client_memory_limit': FLAGS.postgres_kubernetes_client_memory_limit,
  }

  kubernetes_commands.ApplyManifest(
      'container/postgres_sysbench/client_pod.yaml.j2', **template_params
  )

  # Wait for client pod - WaitForResource accepts namespace parameter
  cluster.WaitForResource('pod/postgres-client', 'Ready', namespace='default')

  # Install sysbench and dependencies in pod
  install_commands = [
      'for i in {1..5}; do apt-get update && break || sleep 15; done',
      (
          'export DEBIAN_FRONTEND=noninteractive; for i in {1..3}; do apt-get'
          ' install -y git build-essential automake libtool pkg-config && break'
          ' || sleep 15; done'
      ),
      (
          'export DEBIAN_FRONTEND=noninteractive; for i in {1..3}; do apt-get'
          ' install -y libmysqlclient-dev libpq-dev && break || sleep 15; done'
      ),
      (
          'export DEBIAN_FRONTEND=noninteractive; for i in {1..3}; do apt-get'
          ' install -y sysbench postgresql-client && break || sleep 15; done'
      ),
  ]

  @vm_util.Retry(
      max_retries=3, retryable_exceptions=(errors.VmUtil.IssueCommandError,)
  )
  def _RunInstallCmd(install_cmd):
    kubectl_cmd = [
        FLAGS.kubectl,
        '--kubeconfig',
        FLAGS.kubeconfig,
        'exec',
        '-n',
        'default',
        'postgres-client',
        '--',
        'bash',
        '-c',
        install_cmd,
    ]
    vm_util.IssueCommand(kubectl_cmd)

  for cmd in install_commands:
    _RunInstallCmd(cmd)


def _LoadDatabase(bm_spec: benchmark_spec.BenchmarkSpec) -> None:
  """Load initial data into PostgreSQL using Sysbench.

  Args:
      bm_spec: Benchmark specification.
  """
  postgres_ip = bm_spec.postgres_service_ip  # pytype: disable=attribute-error

  # Run in client pod
  # Manually construct command to avoid VM-specific paths and secure password
  lua_script = '/usr/share/sysbench/oltp_read_write.lua'

  cmd = (
      f'sysbench {lua_script} '
      '--db-driver=pgsql '
      f'--tables={FLAGS.sysbench_tables} '
      f'--table_size={FLAGS.sysbench_table_size} '
      f'--threads={FLAGS.sysbench_load_threads} '
      '--pgsql-user=benchmark '
      '--pgsql-db=benchmark '
      f'--pgsql-host={postgres_ip} '
      '--pgsql-port=5432 '
      'prepare'
  )

  kubectl_cmd = [
      FLAGS.kubectl,
      '--kubeconfig',
      FLAGS.kubeconfig,
      'exec',
      '-n',
      'default',
      'postgres-client',
      '--',
      'bash',
      '-c',
      cmd,
  ]
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


def _GetPostgresLiveConfig(postgres_ip: str) -> Dict[str, str]:
  """Queries remote database for actual active runtime configurations."""
  config = {}
  for param in ['shared_buffers', 'effective_cache_size']:
    cmd = (
        f'psql -h {postgres_ip} -U benchmark -d benchmark -t -A -c "SHOW'
        f' {param};"'
    )
    kubectl_cmd = [
        FLAGS.kubectl,
        '--kubeconfig',
        FLAGS.kubeconfig,
        'exec',
        '-n',
        'default',
        'postgres-client',
        '--',
        'bash',
        '-c',
        f'PGPASSWORD={_GetPostgresPassword()} {cmd}',
    ]
    stdout, _, _ = vm_util.IssueCommand(kubectl_cmd)
    config[param] = stdout.strip() if stdout else 'unknown'
  return config


def _WaitForDatabaseIdle(postgres_ip: str, timeout_sec: int = 60) -> None:
  """Polls database activity until active queries and autovacuums settle."""
  logging.info(
      'Polling PostgreSQL activity until database IO settles (up to %ds)...',
      timeout_sec,
  )
  start_time = time.time()
  query = (
      "SELECT count(*) FROM pg_stat_activity WHERE (state = 'active' OR"
      " backend_type = 'autovacuum worker') AND pid <> pg_backend_pid();"
  )
  cmd = f'psql -h {postgres_ip} -U benchmark -d benchmark -t -A -c "{query}"'
  kubectl_cmd = [
      FLAGS.kubectl,
      '--kubeconfig',
      FLAGS.kubeconfig,
      'exec',
      '-n',
      'default',
      'postgres-client',
      '--',
      'bash',
      '-c',
      f'PGPASSWORD={_GetPostgresPassword()} {cmd}',
  ]

  while time.time() - start_time < timeout_sec:
    stdout, _, _ = vm_util.IssueCommand(kubectl_cmd)
    active_count = (
        int(stdout.strip()) if stdout and stdout.strip().isdigit() else 0
    )
    if active_count == 0:
      logging.info('Database settled successfully (0 active workers).')
      return
    logging.info(
        'Database busy (%d active workers). Waiting 5s...', active_count
    )
    time.sleep(5)

  logging.info(
      'Database IO settle timeout reached. Proceeding to benchmark run.'
  )


def Run(bm_spec: benchmark_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Run Sysbench against PostgreSQL.

  Args:
      bm_spec: Benchmark specification.

  Returns:
      List of performance samples.
  """
  postgres_ip = bm_spec.postgres_service_ip  # pytype: disable=attribute-error
  samples = []

  # Get list of workload types to run
  workload_types = (
      FLAGS.sysbench_testname.split(',')
      if ',' in FLAGS.sysbench_testname
      else [FLAGS.sysbench_testname]
  )

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
    # Stability: Update statistics and flush buffers
    # Same logic as HA benchmark for consistency
    logging.info('Running ANALYZE to update statistics for benchmark tables...')
    for i in range(1, FLAGS.sysbench_tables + 1):
      table_name = f'sbtest{i}'
      analyze_cmd = (
          f'psql -h {postgres_ip} -U benchmark -d benchmark -c "ANALYZE'
          f' {table_name};"'
      )
      kubectl_cmd = [
          FLAGS.kubectl,
          '--kubeconfig',
          FLAGS.kubeconfig,
          'exec',
          '-n',
          'default',
          'postgres-client',
          '--',
          'bash',
          '-c',
          analyze_cmd,
      ]
      vm_util.IssueCommand(kubectl_cmd)

    logging.info('Executing 3 Checkpoints to flush buffers...')
    checkpoint_cmd = (
        f'psql -h {postgres_ip} -U benchmark -d benchmark -c "CHECKPOINT;"'
    )
    kubectl_chk = [
        FLAGS.kubectl,
        '--kubeconfig',
        FLAGS.kubeconfig,
        'exec',
        '-n',
        'default',
        'postgres-client',
        '--',
        'bash',
        '-c',
        checkpoint_cmd,
    ]

    for i in range(3):
      logging.info('Issuing Checkpoint %d/3', i + 1)
      vm_util.IssueCommand(kubectl_chk)
      time.sleep(5)

    # Settle period by intelligent database activity polling
    _WaitForDatabaseIdle(postgres_ip)

    # Manually construct command for Pod mode
    lua_script = f'/usr/share/sysbench/{workload}.lua'

    run_cmd = (
        f'sysbench {lua_script} '
        '--db-driver=pgsql '
        f'--tables={FLAGS.sysbench_tables} '
        f'--table_size={FLAGS.sysbench_table_size} '
        f'--threads={FLAGS.sysbench_run_threads} '
        f'--report-interval={FLAGS.sysbench_report_interval} '
        f'--time={FLAGS.sysbench_run_seconds} '
        '--pgsql-user=benchmark '
        '--pgsql-db=benchmark '
        f'--pgsql-host={postgres_ip} '
        '--pgsql-port=5432 '
        'run'
    )

    kubectl_cmd = [
        FLAGS.kubectl,
        '--kubeconfig',
        FLAGS.kubeconfig,
        'exec',
        '-n',
        'default',
        'postgres-client',
        '--',
        'bash',
        '-c',
        run_cmd,
    ]
    stdout, _, _ = vm_util.IssueCommand(
        kubectl_cmd, timeout=FLAGS.sysbench_run_seconds + 120
    )
    logging.info('Sysbench completed successfully on pod')

    # Log output for debugging
    logging.debug(
        'Sysbench output (first 500 chars): %s',
        stdout[:500] if stdout else 'No output',
    )

    # Parse sysbench output
    metadata = sysbench.GetMetadata(sysbench_params)
    live_pg_conf = _GetPostgresLiveConfig(postgres_ip)
    metadata.update({
        'template_path': FLAGS.kubernetes_postgres_sysbench_template_path,
        'postgres_shared_buffers': live_pg_conf.get(
            'shared_buffers', 'unknown'
        ),
        'postgres_effective_cache_size': live_pg_conf.get(
            'effective_cache_size', 'unknown'
        ),
        'machine_type': FLAGS.postgres_kubernetes_server_machine_type or 'auto',
        'disk_type': FLAGS.data_disk_type or 'auto',
        'workload_type': workload,
    })

    # Parse sysbench output
    try:
      time_series_samples = sysbench.ParseSysbenchTimeSeries(stdout, metadata)
      samples.extend(time_series_samples)
      logging.info('Parsed %d time series samples', len(time_series_samples))
    except ValueError as e:
      logging.warning('Failed to parse time series: %s', e)

    try:
      latency_samples = sysbench.ParseSysbenchLatency([stdout], metadata)
      samples.extend(latency_samples)
      logging.info('Parsed %d latency samples', len(latency_samples))
    except ValueError as e:
      logging.warning('Failed to parse latency: %s', e)

    try:
      transaction_samples = sysbench.ParseSysbenchTransactions(stdout, metadata)
      samples.extend(transaction_samples)
      logging.info('Parsed %d transaction samples', len(transaction_samples))
    except ValueError as e:
      logging.warning('Failed to parse transactions: %s', e)

    if not samples:
      logging.error(
          'No samples parsed from sysbench output. Output was: %s',
          stdout[:1000],
      )

  logging.info('Total samples collected: %d', len(samples))
  return samples


def Cleanup(_) -> None:
  """Clean up PostgreSQL resources."""
  pass
