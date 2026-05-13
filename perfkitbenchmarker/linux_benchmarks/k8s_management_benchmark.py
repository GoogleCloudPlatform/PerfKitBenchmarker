"""
Benchmark: K8s Management Plane Operations
===========================================
Covers GKE / EKS / AKS management plane operations as defined in:
  "Benchmark Methodology: GKE Management Plane Operations" (katmitchell@, Mar 2026)

Scenarios implemented
---------------------
  A. Concurrent Node Pool operations     – CreateNodePool, UpdateNodePool, DeleteNodePool
  B. Overlapping Cluster + Node Pool op  – CreateNodePool fired during ClusterUpdate
  C. Large-scale Node Pool provisioning  – up to MAX_NODE_POOLS node pools

Metrics collected per operation
--------------------------------
  - initiation_latency  : time from API call to async operation accepted
  - end_to_end_latency  : time from API call to operation DONE/SUCCEEDED
  - success / failure   : per-operation outcome
  - aggregate stats     : median, mean, min, max, stddev, success_rate (via PKB sample metadata)

Cloud coverage
--------------
  GCP  → GKE  (google-cloud-sdk / container_v1 client)
  AWS  → EKS  (boto3)
  Azure → AKS (azure-mgmt-containerservice)

Author: vendor implementation based on methodology doc
"""

import logging
import math
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, ALL_COMPLETED
from typing import Callable, List, Optional, Tuple

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

# ---------------------------------------------------------------------------
# Benchmark identity
# ---------------------------------------------------------------------------
BENCHMARK_NAME = 'k8s_management_benchmark'

BENCHMARK_CONFIG = """
k8s_management_benchmark:
  description: >
    Benchmarks GKE/EKS/AKS management plane operations: concurrent node pool
    create/upgrade/delete, overlapping cluster+nodepool ops, and large-scale
    provisioning.  No data-plane workloads required.
  container_cluster:
    type: Kubernetes
    # Minimal node count – benchmark is control-plane-only
    vm_count: 1
    vm_spec:
      GCP:
        machine_type: e2-standard-2
        zone: us-central1-a
      AWS:
        machine_type: t3.medium
        zone: us-east-1a
      Azure:
        machine_type: Standard_D2s_v3
        zone: eastus
"""

# ---------------------------------------------------------------------------
# Configurable flags
# ---------------------------------------------------------------------------
FLAGS = flags.FLAGS

flags.DEFINE_integer(
    'mgmt_concurrent_nodepools', 5,
    'Number of node pools to create/upgrade/delete concurrently in Scenario A.')

flags.DEFINE_integer(
    'mgmt_large_scale_nodepools', 50,
    'Number of node pools to provision in the large-scale Scenario C. '
    'Set up to 1000 for full stress test (ensure quota is available).')

flags.DEFINE_integer(
    'mgmt_nodes_per_nodepool', 1,
    'Number of nodes per node pool. Kept low to reduce quota consumption.')

flags.DEFINE_string(
    'mgmt_k8s_version', None,
    'Kubernetes version for the cluster (None = cloud default / latest).')

flags.DEFINE_string(
    'mgmt_nodepool_initial_version', None,
    'Initial node pool version (N-2). None = derive from cluster version.')

flags.DEFINE_string(
    'mgmt_nodepool_target_version', None,
    'Target node pool upgrade version (N-1 or latest). None = latest available.')

flags.DEFINE_integer(
    'mgmt_operation_timeout_sec', 2700,
    'Maximum seconds to wait for any single async management-plane operation.')

flags.DEFINE_integer(
    'mgmt_poll_interval_sec', 15,
    'Polling interval in seconds when waiting for async operations to complete.')

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# Operation result states
STATE_DONE      = 'DONE'
STATE_SUCCEEDED = 'SUCCEEDED'   # Azure uses this
STATE_FAILED    = 'FAILED'
STATE_RUNNING   = 'RUNNING'
STATE_CREATING  = 'CREATING'
STATE_UPDATING  = 'UPDATING'
STATE_DELETING  = 'DELETING'

TERMINAL_STATES = {STATE_DONE, STATE_SUCCEEDED, STATE_FAILED, 'ERROR', 'CANCELED'}


# ===========================================================================
# PKB lifecycle hooks
# ===========================================================================

def GetConfig(user_config):
    """Returns the benchmark configuration merged with user overrides."""
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
    """
    Verifies the cluster is reachable and collects version metadata.
    PKB has already created the cluster at this point.
    """
    cluster = benchmark_spec.container_cluster

    # PKB stores cloud on the cluster's CLOUD class attribute (e.g. 'AWS', 'GCP', 'Azure').
    # Fall back to FLAGS.cloud if the cluster doesn't expose it directly.
    cloud = getattr(cluster, 'CLOUD', None) or FLAGS.cloud
    cloud = cloud.upper() if cloud else 'AWS'

    logging.info('[Prepare] Cluster %s on %s - cluster is ready (PKB provisioned).', cluster.name, cloud)
    # PKB already waits for the cluster to be ready during _PostCreate.
    # Verify reachability via kubectl before proceeding.
    try:
        cluster.RunKubectl(['get', 'nodes', '--no-headers'])
        logging.info('[Prepare] kubectl get nodes succeeded - cluster is reachable.')
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning('[Prepare] kubectl get nodes warning: %s', exc)

    client = _get_cloud_client(cloud, cluster)

    # Resolve Kubernetes / node-pool versions dynamically if not pinned via flags
    initial_version, target_version = client.resolve_versions(
        flags_initial = FLAGS.mgmt_nodepool_initial_version,
        flags_target  = FLAGS.mgmt_nodepool_target_version,
    )

    # Stash on benchmark_spec so Run() can access them without re-querying
    benchmark_spec.mgmt_client          = client
    benchmark_spec.mgmt_initial_version = initial_version
    benchmark_spec.mgmt_target_version  = target_version

    logging.info('[Prepare] Node pool initial version: %s  →  target version: %s',
                 initial_version, target_version)


def Run(benchmark_spec):
    """
    Executes all three benchmark scenarios and returns a flat list of Samples.
    """
    client          = benchmark_spec.mgmt_client
    initial_version = benchmark_spec.mgmt_initial_version
    target_version  = benchmark_spec.mgmt_target_version
    results         = []

    # ------------------------------------------------------------------
    # Scenario A – Concurrent Node Pool operations
    # ------------------------------------------------------------------
    logging.info('=' * 60)
    logging.info('SCENARIO A: Concurrent Node Pool Operations')
    logging.info('=' * 60)
    results += _run_scenario_a(client, initial_version, target_version)

    # ------------------------------------------------------------------
    # Scenario B – Overlapping Cluster Update + Node Pool Create
    # ------------------------------------------------------------------
    logging.info('=' * 60)
    logging.info('SCENARIO B: Overlapping Cluster Update + NodePool Create')
    logging.info('=' * 60)
    results += _run_scenario_b(client, initial_version)

    # ------------------------------------------------------------------
    # Scenario C – Large-scale Node Pool provisioning
    # ------------------------------------------------------------------
    logging.info('=' * 60)
    logging.info('SCENARIO C: Large-Scale Node Pool Provisioning (%d pools)',
                 FLAGS.mgmt_large_scale_nodepools)
    logging.info('=' * 60)
    results += _run_scenario_c(client, initial_version)

    return results


def Cleanup(benchmark_spec):
    """
    Best-effort deletion of any node pools created during the run.
    PKB deletes the cluster itself; we only clean up leftover node pools.
    """
    client = getattr(benchmark_spec, 'mgmt_client', None)
    if client is None:
        return
    logging.info('[Cleanup] Removing any benchmark node pools…')
    try:
        client.delete_all_benchmark_nodepools()
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning('[Cleanup] Non-fatal error during node pool cleanup: %s', exc)


# ===========================================================================
# Scenario implementations
# ===========================================================================

def _run_scenario_a(
        client,
        initial_version: str,
        target_version: str,
) -> List[sample.Sample]:
    """
    Scenario A: Concurrent CreateNodePool, UpdateNodePool, DeleteNodePool.

    Steps
    -----
    1. Concurrently create N node pools (initial_version).
    2. Concurrently upgrade all N node pools (initial_version → target_version).
    3. Concurrently delete all N node pools.

    Returns one Sample per individual operation plus aggregate stat Samples.
    """
    n       = FLAGS.mgmt_concurrent_nodepools
    results = []

    # ---- Step A1: Concurrent Create ----------------------------------------
    pool_names = [_pool_name('a-create', i) for i in range(n)]
    create_ops = _run_concurrent_operations(
        operation_fn = lambda name: client.create_nodepool(
            name            = name,
            node_count      = FLAGS.mgmt_nodes_per_nodepool,
            node_version    = initial_version,
        ),
        items       = pool_names,
        op_label    = 'ScenarioA_CreateNodePool',
    )
    results += _ops_to_samples(create_ops, 'ScenarioA_CreateNodePool')

    # ---- Step A2: Concurrent Upgrade ----------------------------------------
    # Only upgrade pools that were successfully created
    created_pools = [op['name'] for op in create_ops if op['success']]
    upgrade_ops   = _run_concurrent_operations(
        operation_fn = lambda name: client.upgrade_nodepool(
            name           = name,
            target_version = target_version,
        ),
        items    = created_pools,
        op_label = 'ScenarioA_UpgradeNodePool',
    )
    results += _ops_to_samples(upgrade_ops, 'ScenarioA_UpgradeNodePool')

    # ---- Step A3: Concurrent Delete -----------------------------------------
    # Only delete pools that were successfully created — timed-out/failed creates
    # may have been rolled back by EKS and won't exist to delete.
    existing_pools = [op['name'] for op in create_ops if op['success']]
    if not existing_pools:
        logging.warning('[ScenarioA] No successfully created pools to delete.')
    delete_ops = _run_concurrent_operations(
        operation_fn = lambda name: client.delete_nodepool(name),
        items        = existing_pools,
        op_label     = 'ScenarioA_DeleteNodePool',
    )
    results += _ops_to_samples(delete_ops, 'ScenarioA_DeleteNodePool')

    return results


def _run_scenario_b(client, initial_version: str) -> List[sample.Sample]:
    """
    Scenario B: Initiate NodePool creation *during* an ongoing Cluster Update.

    Steps
    -----
    1. Fire a ClusterUpdate (async – do NOT wait for completion).
    2. Immediately fire a CreateNodePool.
    3. Record initiation latency for both, then poll both to completion.
    4. Record end-to-end latency and success/failure for each.
    """
    results   = []
    pool_name = _pool_name('b-overlap', 0)

    # Fire cluster update (async)
    cluster_op_start = time.time()
    cluster_op_id    = client.start_cluster_update_async()
    cluster_initiation_latency = time.time() - cluster_op_start

    results.append(sample.Sample(
        'ScenarioB_ClusterUpdate_InitiationLatency',
        cluster_initiation_latency,
        'seconds',
        {'operation': 'ClusterUpdate', 'phase': 'initiation'},
    ))
    logging.info('[ScenarioB] ClusterUpdate initiated (op_id=%s) in %.2fs',
                 cluster_op_id, cluster_initiation_latency)

    # Immediately fire CreateNodePool (overlapping)
    np_start   = time.time()
    np_op_id   = client.start_create_nodepool_async(
        name         = pool_name,
        node_count   = FLAGS.mgmt_nodes_per_nodepool,
        node_version = initial_version,
    )
    np_initiation_latency = time.time() - np_start

    results.append(sample.Sample(
        'ScenarioB_CreateNodePool_InitiationLatency',
        np_initiation_latency,
        'seconds',
        {'operation': 'CreateNodePool', 'phase': 'initiation', 'overlap': 'ClusterUpdate'},
    ))
    logging.info('[ScenarioB] CreateNodePool initiated (op_id=%s) in %.2fs',
                 np_op_id, np_initiation_latency)

    # Poll both operations to completion concurrently
    with ThreadPoolExecutor(max_workers=2) as executor:
        cluster_future = executor.submit(
            client.wait_for_operation, cluster_op_id, cluster_op_start)
        np_future = executor.submit(
            client.wait_for_operation, np_op_id, np_start)

        cluster_result = cluster_future.result()
        np_result      = np_future.result()

    results.append(sample.Sample(
        'ScenarioB_ClusterUpdate_EndToEndLatency',
        cluster_result['end_to_end_latency'],
        'seconds',
        {
            'operation': 'ClusterUpdate',
            'success':   str(cluster_result['success']),
            'final_state': cluster_result.get('final_state', 'unknown'),
        },
    ))
    results.append(sample.Sample(
        'ScenarioB_CreateNodePool_EndToEndLatency',
        np_result['end_to_end_latency'],
        'seconds',
        {
            'operation':  'CreateNodePool',
            'success':    str(np_result['success']),
            'overlap':    'ClusterUpdate',
            'final_state': np_result.get('final_state', 'unknown'),
        },
    ))

    # Cleanup the test node pool
    try:
        client.delete_nodepool(pool_name)
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning('[ScenarioB] Could not delete test node pool %s: %s', pool_name, exc)

    return results


def _run_scenario_c(client, initial_version: str) -> List[sample.Sample]:
    """
    Scenario C: Large-scale Node Pool provisioning (up to 1,000 node pools).

    All node pools are created concurrently (batched to avoid API flooding).
    After all complete, they are deleted concurrently to restore cluster state.
    """
    n          = FLAGS.mgmt_large_scale_nodepools
    batch_size = 50   # submit in batches to avoid rate-limiting
    results    = []

    pool_names  = [_pool_name('c-large', i) for i in range(n)]
    all_ops     = []

    logging.info('[ScenarioC] Creating %d node pools in batches of %d…', n, batch_size)
    for batch_start in range(0, n, batch_size):
        batch = pool_names[batch_start: batch_start + batch_size]
        batch_ops = _run_concurrent_operations(
            operation_fn = lambda name: client.create_nodepool(
                name         = name,
                node_count   = FLAGS.mgmt_nodes_per_nodepool,
                node_version = initial_version,
            ),
            items    = batch,
            op_label = f'ScenarioC_CreateNodePool_batch{batch_start // batch_size}',
        )
        all_ops += batch_ops

    results += _ops_to_samples(all_ops, 'ScenarioC_CreateNodePool')

    # Clean up – delete all created pools
    created = [op['name'] for op in all_ops if op['success']]
    logging.info('[ScenarioC] Deleting %d successfully created pools…', len(created))
    delete_ops = []
    for batch_start in range(0, len(created), batch_size):
        batch      = created[batch_start: batch_start + batch_size]
        batch_ops  = _run_concurrent_operations(
            operation_fn = lambda name: client.delete_nodepool(name),
            items        = batch,
            op_label     = f'ScenarioC_DeleteNodePool_batch{batch_start // batch_size}',
        )
        delete_ops += batch_ops
    results += _ops_to_samples(delete_ops, 'ScenarioC_DeleteNodePool')

    return results


# ===========================================================================
# Concurrency helpers
# ===========================================================================

def _run_concurrent_operations(
        operation_fn: Callable[[str], dict],
        items: List[str],
        op_label: str,
) -> List[dict]:
    """
    Runs operation_fn(name) concurrently for every item in `items`.

    Each callable must return a dict with at minimum:
        { 'name': str, 'success': bool,
          'initiation_latency': float, 'end_to_end_latency': float }

    Returns the list of result dicts (order not guaranteed).
    """
    results = []

    if not items:
        logging.info('[%s] No items to process, skipping.', op_label)
        return results

    n_workers = min(len(items), 50)   # cap thread pool to avoid OS limits

    logging.info('[%s] Launching %d concurrent operations...', op_label, len(items))
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        future_to_name = {executor.submit(operation_fn, name): name for name in items}
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result = future.result()
                results.append(result)
                logging.info('[%s] %-40s  e2e=%.2fs  init=%.2fs  ok=%s',
                             op_label, name,
                             result.get('end_to_end_latency', -1),
                             result.get('initiation_latency', -1),
                             result.get('success', False))
            except Exception as exc:  # pylint: disable=broad-except
                logging.error('[%s] Operation failed for %s: %s', op_label, name, exc)
                results.append({
                    'name':                name,
                    'success':             False,
                    'initiation_latency':  -1.0,
                    'end_to_end_latency':  -1.0,
                    'error':               str(exc),
                })
    return results


# ===========================================================================
# Sample construction + aggregate statistics
# ===========================================================================

def _ops_to_samples(ops: List[dict], metric_prefix: str) -> List[sample.Sample]:
    """
    Converts a list of operation result dicts into PKB Samples.

    Emits:
      - One Sample per operation for initiation and end-to-end latency
      - Aggregate stat Samples: median, mean, min, max, stddev, success_rate
    """
    samples = []

    init_latencies = []
    e2e_latencies  = []
    success_count  = 0

    for op in ops:
        meta = {
            'operation_name': op['name'],
            'success':        str(op['success']),
        }
        if 'error' in op:
            meta['error'] = op['error']
        if 'final_state' in op:
            meta['final_state'] = op['final_state']

        if op['success']:
            success_count += 1

        if op['initiation_latency'] >= 0:
            samples.append(sample.Sample(
                f'{metric_prefix}_InitiationLatency',
                op['initiation_latency'],
                'seconds',
                meta,
            ))
            init_latencies.append(op['initiation_latency'])

        if op['end_to_end_latency'] >= 0:
            samples.append(sample.Sample(
                f'{metric_prefix}_EndToEndLatency',
                op['end_to_end_latency'],
                'seconds',
                meta,
            ))
            e2e_latencies.append(op['end_to_end_latency'])

    # Aggregate stats
    total = len(ops)
    if total > 0:
        samples.append(sample.Sample(
            f'{metric_prefix}_SuccessRate',
            success_count / total * 100,
            'percent',
            {'total_ops': str(total), 'successful_ops': str(success_count)},
        ))

    for label, latencies in [('InitiationLatency', init_latencies),
                               ('EndToEndLatency',  e2e_latencies)]:
        if len(latencies) < 2:
            continue
        agg_meta = {'sample_count': str(len(latencies))}
        samples += [
            sample.Sample(f'{metric_prefix}_{label}_Median',
                          statistics.median(latencies), 'seconds', agg_meta),
            sample.Sample(f'{metric_prefix}_{label}_Mean',
                          statistics.mean(latencies),   'seconds', agg_meta),
            sample.Sample(f'{metric_prefix}_{label}_Min',
                          min(latencies),                'seconds', agg_meta),
            sample.Sample(f'{metric_prefix}_{label}_Max',
                          max(latencies),                'seconds', agg_meta),
            sample.Sample(f'{metric_prefix}_{label}_StdDev',
                          statistics.stdev(latencies),  'seconds', agg_meta),
        ]

    return samples


# ===========================================================================
# Cloud client factory
# ===========================================================================

def _get_cloud_client(cloud: str, cluster):
    """Returns the appropriate cloud-specific management client."""
    cloud_upper = cloud.upper()
    if cloud_upper == 'GCP':
        return GKEManagementClient(cluster)
    elif cloud_upper == 'AWS':
        return EKSManagementClient(cluster)
    elif cloud_upper == 'AZURE':
        return AKSManagementClient(cluster)
    else:
        raise errors.Benchmarks.PrepareException(
            f'Unsupported cloud for management plane benchmark: {cloud}')


# ===========================================================================
# Utility helpers
# ===========================================================================

def _pool_name(scenario_tag: str, index: int) -> str:
    """Generates a deterministic, PKB-safe node pool name."""
    return f'pkb-{scenario_tag}-{index:04d}'


def _wait_for_operation_generic(
        poll_fn:          Callable[[], str],
        op_id:            str,
        start_time:       float,
        timeout_sec:      int,
        poll_interval_sec: int,
) -> dict:
    """
    Generic async-operation poller.

    Args:
        poll_fn:  zero-arg callable that returns the current operation state string.
        op_id:    operation identifier (for logging).
        start_time: epoch time when the operation was initiated.
        timeout_sec: hard timeout.
        poll_interval_sec: sleep between polls.

    Returns:
        { 'success': bool, 'end_to_end_latency': float, 'final_state': str }
    """
    deadline = start_time + timeout_sec
    while True:
        state = poll_fn()
        if state in TERMINAL_STATES:
            end_to_end = time.time() - start_time
            success    = state in {STATE_DONE, STATE_SUCCEEDED}
            logging.info('Operation %s reached terminal state %s in %.2fs',
                         op_id, state, end_to_end)
            return {
                'success':           success,
                'end_to_end_latency': end_to_end,
                'final_state':       state,
            }
        if time.time() > deadline:
            logging.error('Operation %s timed out after %ds (last state: %s)',
                          op_id, timeout_sec, state)
            return {
                'success':           False,
                'end_to_end_latency': time.time() - start_time,
                'final_state':       'TIMEOUT',
            }
        logging.debug('Operation %s state=%s – polling again in %ds', op_id, state, poll_interval_sec)
        time.sleep(poll_interval_sec)


# ===========================================================================
# GKE Management Client (GCP)
# ===========================================================================

class GKEManagementClient:
    """
    Wraps the GKE container_v1 REST API for management plane operations.
    Requires:  google-cloud-container  (pip install google-cloud-container)
    """

    def __init__(self, cluster):
        from google.cloud import container_v1  # pylint: disable=import-outside-toplevel
        self._cluster  = cluster
        self._gke      = container_v1.ClusterManagerClient()
        self._project  = cluster.project
        self._location = cluster.zone           # e.g. 'us-central1-a' or 'us-central1'
        self._cluster_name = cluster.name
        self._parent   = f'projects/{self._project}/locations/{self._location}'
        self._cluster_path = f'{self._parent}/clusters/{self._cluster_name}'

    # ------------------------------------------------------------------
    # Version resolution
    # ------------------------------------------------------------------

    def resolve_versions(self, flags_initial, flags_target):
        from google.cloud import container_v1  # pylint: disable=import-outside-toplevel
        sc = self._gke.get_server_config(name=self._parent)
        valid_versions = [c.name for c in sc.channels
                          if c.channel == container_v1.ReleaseChannel.Channel.REGULAR]
        if not valid_versions:
            valid_versions = sc.valid_node_versions

        valid_versions.sort(reverse=True)   # latest first

        initial = flags_initial or (valid_versions[2] if len(valid_versions) >= 3
                                    else valid_versions[-1])
        target  = flags_target  or valid_versions[0]

        return initial, target

    # ------------------------------------------------------------------
    # Async helpers
    # ------------------------------------------------------------------

    def wait_for_operation(self, op_name: str, start_time: float) -> dict:
        def _poll():
            op = self._gke.get_operation({'name': op_name})
            return op.status.name   # RUNNING / DONE / ABORTING

        return _wait_for_operation_generic(
            poll_fn           = _poll,
            op_id             = op_name,
            start_time        = start_time,
            timeout_sec       = FLAGS.mgmt_operation_timeout_sec,
            poll_interval_sec = FLAGS.mgmt_poll_interval_sec,
        )

    def _wait(self, op, start_time: float) -> dict:
        return self.wait_for_operation(op.name, start_time)

    # ------------------------------------------------------------------
    # Node pool operations
    # ------------------------------------------------------------------

    def start_create_nodepool_async(self, name: str, node_count: int, node_version: str):
        from google.cloud import container_v1  # pylint: disable=import-outside-toplevel
        req = container_v1.CreateNodePoolRequest(
            parent    = self._cluster_path,
            node_pool = container_v1.NodePool(
                name             = name,
                version          = node_version,
                initial_node_count = node_count,
                config           = container_v1.NodeConfig(machine_type='e2-standard-2'),
            ),
        )
        op = self._gke.create_node_pool(request=req)
        return op.name   # operation resource name

    def create_nodepool(self, name: str, node_count: int, node_version: str) -> dict:
        start = time.time()
        op_id = self.start_create_nodepool_async(name, node_count, node_version)
        initiation_latency = time.time() - start
        result = self.wait_for_operation(op_id, start)
        return {
            'name':               name,
            'success':            result['success'],
            'initiation_latency': initiation_latency,
            'end_to_end_latency': result['end_to_end_latency'],
            'final_state':        result.get('final_state'),
        }

    def upgrade_nodepool(self, name: str, target_version: str) -> dict:
        from google.cloud import container_v1  # pylint: disable=import-outside-toplevel
        start = time.time()
        req = container_v1.UpdateNodePoolRequest(
            name         = f'{self._cluster_path}/nodePools/{name}',
            node_version = target_version,
        )
        op = self._gke.update_node_pool(request=req)
        initiation_latency = time.time() - start
        result = self.wait_for_operation(op.name, start)
        return {
            'name':               name,
            'success':            result['success'],
            'initiation_latency': initiation_latency,
            'end_to_end_latency': result['end_to_end_latency'],
            'final_state':        result.get('final_state'),
        }

    def delete_nodepool(self, name: str) -> dict:
        from google.cloud import container_v1  # pylint: disable=import-outside-toplevel
        start = time.time()
        req   = container_v1.DeleteNodePoolRequest(
            name=f'{self._cluster_path}/nodePools/{name}')
        op = self._gke.delete_node_pool(request=req)
        initiation_latency = time.time() - start
        result = self.wait_for_operation(op.name, start)
        return {
            'name':               name,
            'success':            result['success'],
            'initiation_latency': initiation_latency,
            'end_to_end_latency': result['end_to_end_latency'],
            'final_state':        result.get('final_state'),
        }

    def start_cluster_update_async(self) -> str:
        """Triggers a no-op-equivalent cluster update to exercise the control plane."""
        from google.cloud import container_v1  # pylint: disable=import-outside-toplevel
        cluster = self._gke.get_cluster({'name': self._cluster_path})
        req = container_v1.UpdateClusterRequest(
            name   = self._cluster_path,
            update = container_v1.ClusterUpdate(
                desired_logging_service = cluster.logging_service,  # same value – triggers op
            ),
        )
        op = self._gke.update_cluster(request=req)
        return op.name

    def delete_all_benchmark_nodepools(self):
        from google.cloud import container_v1  # pylint: disable=import-outside-toplevel
        cluster = self._gke.get_cluster({'name': self._cluster_path})
        for np in cluster.node_pools:
            if np.name.startswith('pkb-'):
                try:
                    self.delete_nodepool(np.name)
                except Exception as exc:  # pylint: disable=broad-except
                    logging.warning('Could not delete node pool %s: %s', np.name, exc)


# ===========================================================================
# EKS Management Client (AWS)
# ===========================================================================

class EKSManagementClient:
    """
    Wraps the AWS EKS boto3 API for management plane operations.
    Requires:  boto3  (pip install boto3)
    """

    def __init__(self, cluster):
        self._cluster      = cluster
        self._cluster_name = cluster.name
        self._region       = cluster.region
        # Do NOT store boto3 client as an instance attribute — PKB pickles
        # benchmark_spec (including mgmt_client) and boto3 clients are not picklable.
        # Use the _eks property below which creates the client lazily per-call.

    @property
    def _eks(self):
        import boto3  # pylint: disable=import-outside-toplevel
        if not hasattr(self, '_eks_client') or self._eks_client is None:
            self._eks_client = boto3.client('eks', region_name=self._region)
        return self._eks_client

    def __getstate__(self):
        # Exclude unpicklable boto3 client when PKB serialises benchmark_spec
        state = self.__dict__.copy()
        state.pop('_eks_client', None)
        state.pop('_cached_node_role_arn', None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._eks_client = None

    # ------------------------------------------------------------------
    # Version resolution
    # ------------------------------------------------------------------

    def resolve_versions(self, flags_initial, flags_target):
        # Get supported nodegroup versions directly from the cluster's version
        cluster_info = self._eks.describe_cluster(name=self._cluster_name)['cluster']
        cluster_version = cluster_info['version']  # e.g. '1.34'

        major, minor = cluster_version.split('.')
        minor = int(minor)

        # EKS supports N, N-1, N-2, N-3 nodegroup versions relative to cluster
        supported = [f'{major}.{minor - i}' for i in range(4)]
        logging.info('[EKS] Cluster version %s, supported nodegroup versions: %s',
                     cluster_version, supported)

        # initial = N-2, target = cluster version (N)
        initial = flags_initial or supported[2]   # N-2
        target  = flags_target  or supported[0]   # N (same as cluster = latest)
        return initial, target

    # ------------------------------------------------------------------
    # Async helpers
    # ------------------------------------------------------------------

    def wait_for_operation(self, op_id: str, start_time: float) -> dict:
        """
        op_id format: "<cluster_name>/<nodegroup_name>" or "<cluster_name>/__cluster__"
        """
        parts = op_id.split('/')
        is_cluster_op = (parts[-1] == '__cluster__')

        def _poll():
            if is_cluster_op:
                r = self._eks.describe_cluster(name=self._cluster_name)
                return r['cluster']['status']          # ACTIVE / UPDATING / FAILED
            else:
                ng_name = parts[-1]
                r = self._eks.describe_nodegroup(
                    clusterName   = self._cluster_name,
                    nodegroupName = ng_name,
                )
                return r['nodegroup']['status']        # ACTIVE / CREATING / UPDATING / DELETING / FAILED

        def _normalise(state):
            mapping = {
                'ACTIVE':   STATE_DONE,
                'FAILED':   STATE_FAILED,
                'DEGRADED': STATE_FAILED,
            }
            return mapping.get(state, STATE_RUNNING)

        def _poll_normalised():
            return _normalise(_poll())

        return _wait_for_operation_generic(
            poll_fn           = _poll_normalised,
            op_id             = op_id,
            start_time        = start_time,
            timeout_sec       = FLAGS.mgmt_operation_timeout_sec,
            poll_interval_sec = FLAGS.mgmt_poll_interval_sec,
        )

    # ------------------------------------------------------------------
    # Node group (node pool) operations
    # ------------------------------------------------------------------

    def _get_node_role_arn(self) -> str:
        """
        Returns a node IAM role ARN that has ec2.amazonaws.com in its trust policy.
        Looks up the role from the existing default nodegroup created by PKB/eksctl,
        which always has the correct trust relationship.
        Falls back to constructing the standard eksctl role name if no nodegroup exists.
        """
        if hasattr(self, '_cached_node_role_arn'):
            return self._cached_node_role_arn

        # Try to get role from the existing default nodegroup (most reliable)
        try:
            ngs = self._eks.list_nodegroups(clusterName=self._cluster_name)
            for ng_name in ngs.get('nodegroups', []):
                ng = self._eks.describe_nodegroup(
                    clusterName=self._cluster_name, nodegroupName=ng_name)
                role_arn = ng['nodegroup'].get('nodeRole')
                if role_arn:
                    logging.info('[EKS] Using node role from existing nodegroup %s: %s',
                                 ng_name, role_arn)
                    self._cached_node_role_arn = role_arn
                    return role_arn
        except Exception as exc:  # pylint: disable=broad-except
            logging.warning('[EKS] Could not look up node role from nodegroup: %s', exc)

        # Fallback: construct standard eksctl node role name
        import boto3  # pylint: disable=import-outside-toplevel
        iam = boto3.client('iam', region_name=self._eks.meta.region_name)
        paginator = iam.get_paginator('list_roles')
        prefix = f'eksctl-{self._cluster_name}-nodegroup'
        for page in paginator.paginate():
            for role in page['Roles']:
                if prefix in role['RoleName'] and 'NodeInstanceRole' in role['RoleName']:
                    self._cached_node_role_arn = role['Arn']
                    logging.info('[EKS] Found node instance role via IAM: %s', role['Arn'])
                    return self._cached_node_role_arn

        raise RuntimeError(
            f'Could not find a node IAM role with ec2.amazonaws.com trust for cluster '
            f'{self._cluster_name}. Ensure the default nodegroup exists or pass a role ARN.')

    def start_create_nodepool_async(self, name: str, node_count: int, node_version: str) -> str:
        cluster_info  = self._eks.describe_cluster(name=self._cluster_name)['cluster']
        subnet_ids    = cluster_info['resourcesVpcConfig']['subnetIds']
        # Use node role (ec2.amazonaws.com trust) not cluster ServiceRole
        node_role_arn = self._get_node_role_arn()

        self._eks.create_nodegroup(
            clusterName   = self._cluster_name,
            nodegroupName = name,
            scalingConfig = {'minSize': node_count, 'maxSize': node_count, 'desiredSize': node_count},
            subnets       = subnet_ids,
            nodeRole      = node_role_arn,
            version       = node_version,
            instanceTypes = ['t3.medium'],
        )
        return f'{self._cluster_name}/{name}'

    def create_nodepool(self, name: str, node_count: int, node_version: str) -> dict:
        start = time.time()
        op_id = self.start_create_nodepool_async(name, node_count, node_version)
        initiation_latency = time.time() - start
        result = self.wait_for_operation(op_id, start)
        return {'name': name, 'initiation_latency': initiation_latency, **result}

    def upgrade_nodepool(self, name: str, target_version: str) -> dict:
        start = time.time()
        self._eks.update_nodegroup_version(
            clusterName   = self._cluster_name,
            nodegroupName = name,
            version       = target_version,
        )
        initiation_latency = time.time() - start
        op_id  = f'{self._cluster_name}/{name}'
        result = self.wait_for_operation(op_id, start)
        return {'name': name, 'initiation_latency': initiation_latency, **result}

    def delete_nodepool(self, name: str) -> dict:
        start = time.time()
        self._eks.delete_nodegroup(
            clusterName   = self._cluster_name,
            nodegroupName = name,
        )
        initiation_latency = time.time() - start
        op_id  = f'{self._cluster_name}/{name}'
        result = self.wait_for_operation(op_id, start)
        return {'name': name, 'initiation_latency': initiation_latency, **result}

    def start_cluster_update_async(self) -> str:
        # Toggle cluster logging to trigger a real ClusterUpdate operation.
        # We enable 'api' logging if currently disabled, or disable if enabled.
        # This is the lightest possible cluster update — no infrastructure change.
        cluster          = self._eks.describe_cluster(name=self._cluster_name)['cluster']
        current_logging  = cluster.get('logging', {}).get('clusterLogging', [])

        # Find current state of 'api' log type
        api_enabled = False
        for entry in current_logging:
            if 'api' in entry.get('types', []) and entry.get('enabled'):
                api_enabled = True
                break

        # Toggle: if api logging is on, turn it off; if off, turn it on
        # This guarantees a meaningful change that EKS will accept
        new_enabled  = not api_enabled
        logging.info('[EKS] Toggling api logging from %s to %s to trigger ClusterUpdate',
                     api_enabled, new_enabled)

        self._eks.update_cluster_config(
            name    = self._cluster_name,
            logging = {
                'clusterLogging': [
                    {'types': ['api'], 'enabled': new_enabled}
                ]
            }
        )
        return f'{self._cluster_name}/__cluster__'

    def delete_all_benchmark_nodepools(self):
        resp = self._eks.list_nodegroups(clusterName=self._cluster_name)
        for ng in resp.get('nodegroups', []):
            if ng.startswith('pkb-'):
                try:
                    self.delete_nodepool(ng)
                except Exception as exc:  # pylint: disable=broad-except
                    logging.warning('Could not delete nodegroup %s: %s', ng, exc)


# ===========================================================================
# AKS Management Client (Azure)
# ===========================================================================

class AKSManagementClient:
    """
    Wraps the Azure ContainerServiceClient for management plane operations.
    Requires:  azure-mgmt-containerservice, azure-identity
    """

    def __init__(self, cluster):
        from azure.identity import DefaultAzureCredential           # pylint: disable=import-outside-toplevel
        from azure.mgmt.containerservice import ContainerServiceClient  # pylint: disable=import-outside-toplevel
        self._cluster         = cluster
        self._cluster_name    = cluster.name
        self._resource_group  = cluster.resource_group
        self._subscription_id = cluster.subscription_id
        cred = DefaultAzureCredential()
        self._aks = ContainerServiceClient(cred, self._subscription_id)

    # ------------------------------------------------------------------
    # Version resolution
    # ------------------------------------------------------------------

    def resolve_versions(self, flags_initial, flags_target):
        versions = sorted(
            [v.orchestrator_version
             for v in self._aks.container_services.list_orchestrators(
                 location='eastus', resource_type='managedClusters'
             ).orchestrators],
            reverse=True,
        )
        initial = flags_initial or (versions[2] if len(versions) >= 3 else versions[-1])
        target  = flags_target  or versions[0]
        return initial, target

    # ------------------------------------------------------------------
    # Async helpers
    # ------------------------------------------------------------------

    def wait_for_operation(self, op_id: str, start_time: float) -> dict:
        """
        op_id: "<resource_group>/<cluster_name>/<agentpool_name>"
               or "<resource_group>/<cluster_name>/__cluster__"
        """
        parts         = op_id.split('/')
        is_cluster_op = (parts[-1] == '__cluster__')

        def _poll():
            if is_cluster_op:
                c = self._aks.managed_clusters.get(self._resource_group, self._cluster_name)
                return c.provisioning_state   # Succeeded / Failed / Updating / Creating
            else:
                pool_name = parts[-1]
                ap = self._aks.agent_pools.get(
                    self._resource_group, self._cluster_name, pool_name)
                return ap.provisioning_state

        def _normalise(state):
            mapping = {
                'Succeeded': STATE_SUCCEEDED,
                'Failed':    STATE_FAILED,
                'Canceled':  'CANCELED',
            }
            return mapping.get(state, STATE_RUNNING)

        def _poll_normalised():
            return _normalise(_poll())

        return _wait_for_operation_generic(
            poll_fn           = _poll_normalised,
            op_id             = op_id,
            start_time        = start_time,
            timeout_sec       = FLAGS.mgmt_operation_timeout_sec,
            poll_interval_sec = FLAGS.mgmt_poll_interval_sec,
        )

    # ------------------------------------------------------------------
    # Agent pool (node pool) operations
    # ------------------------------------------------------------------

    def start_create_nodepool_async(self, name: str, node_count: int, node_version: str) -> str:
        from azure.mgmt.containerservice.models import AgentPool  # pylint: disable=import-outside-toplevel
        self._aks.agent_pools.begin_create_or_update(
            self._resource_group,
            self._cluster_name,
            name,
            AgentPool(
                count              = node_count,
                vm_size            = 'Standard_D2s_v3',
                orchestrator_version = node_version,
                mode               = 'User',
            ),
        )
        return f'{self._resource_group}/{self._cluster_name}/{name}'

    def create_nodepool(self, name: str, node_count: int, node_version: str) -> dict:
        start = time.time()
        op_id = self.start_create_nodepool_async(name, node_count, node_version)
        initiation_latency = time.time() - start
        result = self.wait_for_operation(op_id, start)
        return {'name': name, 'initiation_latency': initiation_latency, **result}

    def upgrade_nodepool(self, name: str, target_version: str) -> dict:
        from azure.mgmt.containerservice.models import AgentPool  # pylint: disable=import-outside-toplevel
        start = time.time()
        ap = self._aks.agent_pools.get(self._resource_group, self._cluster_name, name)
        ap.orchestrator_version = target_version
        self._aks.agent_pools.begin_create_or_update(
            self._resource_group, self._cluster_name, name, ap)
        initiation_latency = time.time() - start
        op_id  = f'{self._resource_group}/{self._cluster_name}/{name}'
        result = self.wait_for_operation(op_id, start)
        return {'name': name, 'initiation_latency': initiation_latency, **result}

    def delete_nodepool(self, name: str) -> dict:
        start = time.time()
        self._aks.agent_pools.begin_delete(
            self._resource_group, self._cluster_name, name)
        initiation_latency = time.time() - start
        op_id  = f'{self._resource_group}/{self._cluster_name}/{name}'
        result = self.wait_for_operation(op_id, start)
        return {'name': name, 'initiation_latency': initiation_latency, **result}

    def start_cluster_update_async(self) -> str:
        cluster = self._aks.managed_clusters.get(self._resource_group, self._cluster_name)
        self._aks.managed_clusters.begin_create_or_update(
            self._resource_group, self._cluster_name, cluster)
        return f'{self._resource_group}/{self._cluster_name}/__cluster__'

    def delete_all_benchmark_nodepools(self):
        pools = self._aks.agent_pools.list(self._resource_group, self._cluster_name)
        for pool in pools:
            if pool.name.startswith('pkb-'):
                try:
                    self.delete_nodepool(pool.name)
                except Exception as exc:  # pylint: disable=broad-except
                    logging.warning('Could not delete agent pool %s: %s', pool.name, exc)
