# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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


"""In-cluster Kubernetes load generator for the agent_sandbox benchmark.

Creates SandboxClaim custom resources at a configurable target QPS. Once the
claim is handled and the sandbox is ready, it runs a dummy workload in that
sandbox to simulate sandbox usage. It measures several indicators of
performance, including the time from claim creation until the sandbox is ready.

Unlike standard PKB benchmarks, this load generator is not "fire-and-forget."
Recording performance metrics and triggering workload execution concurrently
requires an efficient way of tracking state. Rather than opening dedicated
connections or polling the status of each claim individually (which does not
scale and can rapidly overload the Kubernetes control plane), it uses a single
shared Watch stream to monitor SandboxClaim status changes.

Where most PKB Kubernetes benchmarks rely on shelling out to the `kubectl` CLI,
this implementation uses the official Kubernetes Python client to establish a
long-lived watch connection.

Note: This script lives in the data files outside the core PKB source tree
and is not directly invoked by it. During the benchmark's Run phase, PKB
packages this script into a ConfigMap, which is then mounted and executed
inside a load generator pod in the cluster.
"""

from __future__ import annotations

import argparse
from concurrent import futures
import dataclasses
import json
import logging
import sys
import threading
import time
from typing import Any, Callable, Iterator, Optional, cast

# The `kubernetes` Python client is installed inside the in-cluster load
# generator pod (see load_runner_job.yaml.j2) rather than in PKB's host
# environment. This fallback allows host-side unit tests
# (agent_sandbox_loadgen_test) to import and test this module without adding
# `kubernetes` as a host-side PKB dependency.
# pylint: disable=g-import-not-at-top,g-bad-exception-name
try:
  from kubernetes import client as k8s_client  # pyrefly: ignore[missing-import]
  from kubernetes import config  # pyrefly: ignore[missing-import]
  from kubernetes import watch as kwatch  # pyrefly: ignore[missing-import]
  from kubernetes.client.exceptions import ApiException  # pyrefly: ignore[missing-import]
  from kubernetes.stream import stream  # pyrefly: ignore[missing-import]
except ImportError:
  k8s_client: Any = None
  config: Any = None
  kwatch: Any = None
  stream: Any = None

  class ApiException(Exception):  # type: ignore[no-redef]
    """Fallback exception when the kubernetes client is not installed."""

    status: int | None = None
    headers: dict[str, str] | None = None


# pylint: enable=g-import-not-at-top,g-bad-exception-name

# 429 retry parameters for create().
_CREATE_MAX_RETRIES = 5
_CREATE_RETRY_AFTER_DEFAULT = 1.0  # seconds
_CREATE_RETRY_AFTER_CAP = 5.0  # seconds


@dataclasses.dataclass(frozen=True)
class RunShape:
  """A fully resolved load shape: rate, window, and total claim count."""

  qps: float
  duration: float
  total: int


def resolve_run_shape(
    qps: Optional[float] = None,
    duration: Optional[float] = None,
    total: Optional[int] = None,
) -> RunShape:
  """Resolves any two of qps, duration, total into a complete RunShape.

  Args:
    qps: Target claims per second.
    duration: Submission window in seconds.
    total: Total number of claims to submit.

  Returns:
    A RunShape with all three fields populated.

  Raises:
    ValueError: If fewer than two values are provided.
  """
  if qps is not None and duration is not None and total is not None:
    # All three provided; respect them as-is.
    pass
  elif qps is not None and duration is not None:
    total = int(round(qps * duration))
  elif qps is not None and total is not None:
    duration = total / qps
  elif duration is not None and total is not None:
    qps = total / duration
  else:
    raise ValueError('Provide at least two of qps, duration, total.')
  return RunShape(qps=float(qps), duration=float(duration), total=int(total))


CLAIM_API_GROUP = 'extensions.agents.x-k8s.io'
CLAIM_API_VERSION = 'v1beta1'
CLAIM_PLURAL = 'sandboxclaims'
CLAIM_KIND = 'SandboxClaim'


class K8sClientFactory:
  """Configures kubeconfig auth and builds pooled Kubernetes API clients."""

  DEFAULT_CONNECTION_POOL_MAXSIZE = 64
  AUTH_KEY = 'authorization'
  BEARER_TOKEN_KEY = 'BearerToken'
  BEARER_PREFIX = 'Bearer'

  def __init__(
      self,
      connection_pool_maxsize: int = DEFAULT_CONNECTION_POOL_MAXSIZE,
  ) -> None:
    self.connection_pool_maxsize = connection_pool_maxsize

  @classmethod
  def load_kube_config(cls) -> None:
    """Loads kubeconfig for the current PKB run. Isolated for test mocking."""
    config.load_kube_config()

  @classmethod
  def register_bearer_token_auth(cls, cfg: Any) -> None:
    """Registers the exec-plugin bearer token under the key the client expects.

    kubernetes-client>=36 auth_settings() looks for the token under the
    'BearerToken' api_key, but load_kube_config() stores exec-plugin tokens
    under 'authorization'. Without this remap the Authorization header is never
    added to requests and every call goes out unauthenticated.

    Args:
      cfg: A Configuration object to update.
    """
    token = cfg.api_key.get(cls.AUTH_KEY, '')
    if token and cls.BEARER_TOKEN_KEY not in cfg.api_key:
      bearer_prefix_with_space = f'{cls.BEARER_PREFIX} '
      if token.startswith(bearer_prefix_with_space):
        token = token[len(bearer_prefix_with_space) :]
      cfg.api_key[cls.BEARER_TOKEN_KEY] = token
      cfg.api_key_prefix[cls.BEARER_TOKEN_KEY] = cls.BEARER_PREFIX

  def new_api_client(self) -> Any:
    """Builds a Kubernetes ApiClient with a sized connection pool and auth."""
    cfg = k8s_client.Configuration.get_default_copy()
    cfg.connection_pool_maxsize = self.connection_pool_maxsize
    self.register_bearer_token_auth(cfg)
    return k8s_client.ApiClient(cfg)

  def make_custom_objects_api(self) -> Any:
    """Builds a CustomObjectsApi client with a sized connection pool."""
    if k8s_client is None:
      return None
    return k8s_client.CustomObjectsApi(self.new_api_client())

  def make_core_v1_api(self) -> Any:
    """Builds a CoreV1Api client with a sized connection pool (for pod exec)."""
    if k8s_client is None:
      return None
    return k8s_client.CoreV1Api(self.new_api_client())


@dataclasses.dataclass
class ClaimRecord:
  """Timing record for one SandboxClaim."""

  name: str
  requested_at: float
  ready_at: Optional[float] = None
  error: Optional[str] = None
  warm_served: Optional[bool] = None
  exec_started_at: Optional[float] = None
  exec_completed_at: Optional[float] = None
  released_at: Optional[float] = None

  @property
  def startup_time_s(self) -> Optional[float]:
    if self.ready_at is None:
      return None
    return self.ready_at - self.requested_at

  @property
  def exec_duration_s(self) -> Optional[float]:
    if self.exec_started_at is None or self.exec_completed_at is None:
      return None
    return self.exec_completed_at - self.exec_started_at

  @property
  def delete_duration_s(self) -> Optional[float]:
    if self.exec_completed_at is None or self.released_at is None:
      return None
    return self.released_at - self.exec_completed_at

  @property
  def total_lifecycle_s(self) -> Optional[float]:
    if self.released_at is None:
      return None
    return self.released_at - self.requested_at


def _busy_loop_code(workload_duration: float) -> str:
  """Python source for a CPU busy-loop that runs ~workload_duration seconds."""
  return (
      'import time\n'
      'start = time.monotonic()\n'
      'count = 0\n'
      f'while time.monotonic() - start < {workload_duration}:\n'
      '    _ = (count * 2) + (count ** 2)\n'
      '    count += 1\n'
      'print(f"iterations={count}")\n'
  )


class WorkloadExecutor:
  """Runs a workload inside a Ready sandbox and blocks until it completes."""

  def prepare(self, claim_name: str, status: dict[str, Any]) -> Any:
    """Called eagerly in the watch consumer when the claim becomes Ready.

    Returns any pre-fetched data that execute() needs. The return value is
    passed through as the `prepared` argument to execute(). Default: None.

    Args:
      claim_name: The name of the claim that just became Ready.
      status: The SandboxClaim's status dictionary.
    """
    del claim_name, status  # Unused.
    return None

  def execute(
      self, claim_name: str, status: Optional[dict[str, Any]], prepared: Any
  ) -> None:
    raise NotImplementedError


class NoopExecutor(WorkloadExecutor):
  """No workload: used when workload_duration == 0 (immediate release)."""

  def execute(
      self, claim_name: str, status: Optional[dict[str, Any]], prepared: Any
  ) -> None:
    del claim_name, status, prepared  # Unused.
    return


class StreamExecExecutor(WorkloadExecutor):
  """Execs the workload into the sandbox pod via the Kubernetes API server.

  Uses connect_get_namespaced_pod_exec. Works from any host with kubeconfig.
  """

  def __init__(
      self,
      custom_objects_api: Any,
      namespace: str,
      workload_duration: float,
      sandbox_group: str = 'agents.x-k8s.io',
      sandbox_version: str = 'v1beta1',
      sandbox_plural: str = 'sandboxes',
      core_v1_api: Any = None,
      resolve_pod_annotation: bool = False,
  ) -> None:
    self._co = custom_objects_api
    self._core_api = core_v1_api
    self._resolve_pod_annotation = resolve_pod_annotation
    self._namespace = namespace
    self._workload_duration = workload_duration
    self._sandbox_group = sandbox_group
    self._sandbox_version = sandbox_version
    self._sandbox_plural = sandbox_plural

  def _resolve_pod_name(self, status: dict[str, Any]) -> Optional[str]:
    """Ready claim -> sandbox pod name.

    In agent-sandbox v1beta1 (v1.0.4+), the backing Pod name always equals
    status.sandbox.name (agents.x-k8s.io/pod-name is deprecated). If
    resolve_pod_annotation=True is explicitly requested, falls back to reading
    the Sandbox CR annotation.

    Args:
      status: The SandboxClaim's status dictionary.

    Returns:
      The resolved pod name as a string, or None if no sandbox is bound.
    """
    sandbox = status.get('sandbox') or {}
    sandbox_name = sandbox.get('name')
    if not sandbox_name:
      return None
    if not self._resolve_pod_annotation:
      return sandbox_name
    sb = self._co.get_namespaced_custom_object(
        self._sandbox_group,
        self._sandbox_version,
        self._namespace,
        self._sandbox_plural,
        sandbox_name,
    )
    annotations = (sb.get('metadata') or {}).get('annotations') or {}
    return annotations.get('agents.x-k8s.io/pod-name') or sandbox_name

  def prepare(self, claim_name: str, status: dict[str, Any]) -> Optional[str]:
    """Resolves the pod name while the Sandbox CR is guaranteed to exist."""
    return self._resolve_pod_name(status)

  def execute(
      self,
      claim_name: str,
      status: Optional[dict[str, Any]],
      prepared: Optional[str],
  ) -> None:
    del status  # Unused.

    pod_name = prepared
    if not pod_name:
      raise RuntimeError(f'no sandbox pod bound for claim {claim_name}')
    owns_client = self._core_api is None
    core_api = (
        K8sClientFactory(1).make_core_v1_api()
        if owns_client
        else self._core_api
    )
    try:
      ws = stream(
          core_api.connect_get_namespaced_pod_exec,
          pod_name,
          self._namespace,
          command=['python3', '-c', _busy_loop_code(self._workload_duration)],
          stderr=True,
          stdin=False,
          stdout=True,
          tty=False,
          _request_timeout=self._workload_duration + 60,
          _preload_content=False,
      )
      try:
        ws.run_forever(timeout=self._workload_duration + 60)
      finally:
        ws.close()
    finally:
      if owns_client:
        core_api.api_client.close()


class ClaimDriver:
  """Creates, watches, and deletes SandboxClaim custom resources."""

  def __init__(
      self,
      namespace: str,
      warmpool_name: Optional[str] = None,
      group: str = CLAIM_API_GROUP,
      version: str = CLAIM_API_VERSION,
      plural: str = CLAIM_PLURAL,
      claim_ttl_seconds: Optional[int] = None,
      max_concurrent: int = 64,
      load_config: bool = True,
      client_factory: Optional[K8sClientFactory] = None,
  ) -> None:
    if load_config:
      K8sClientFactory.load_kube_config()
    if client_factory is None:
      client_factory = K8sClientFactory(max_concurrent + 50)
    self._api = client_factory.make_custom_objects_api()
    self._delete_api = client_factory.make_custom_objects_api()
    self._namespace = namespace
    if warmpool_name and ',' in warmpool_name:
      self._warmpool_names = [
          w.strip() for w in warmpool_name.split(',') if w.strip()
      ]
    elif warmpool_name:
      self._warmpool_names = [warmpool_name]
    else:
      self._warmpool_names = []
    self._warmpool_name = (
        self._warmpool_names[0] if self._warmpool_names else warmpool_name
    )
    self._shard_idx = 0
    self._shard_lock = threading.Lock()
    self._group = group
    self._version = version
    self._plural = plural
    self._claim_ttl_seconds = claim_ttl_seconds or 0

  def _next_warmpool_name(self) -> Optional[str]:
    if not self._warmpool_names:
      return self._warmpool_name
    if len(self._warmpool_names) == 1:
      return self._warmpool_names[0]
    with self._shard_lock:
      target = self._warmpool_names[self._shard_idx % len(self._warmpool_names)]
      self._shard_idx += 1
      return target

  def _body(self, name: str) -> dict[str, Any]:
    """Builds the SandboxClaim request status body payload.

    Args:
      name: The name of the SandboxClaim.

    Returns:
      A dictionary representing the SandboxClaim custom resource body.
    """
    lifecycle: dict[str, Any] = {'shutdownPolicy': 'Delete'}
    if self._claim_ttl_seconds:
      lifecycle['ttlSecondsAfterFinished'] = self._claim_ttl_seconds
    # Post-#899 SandboxClaim API: a claim references a SandboxWarmPool
    # directly via warmPoolRef; the controller resolves the template through
    # the pool's sandboxTemplateRef. The old sandboxTemplateRef/warmpool
    # fields were removed upstream.
    spec = {
        'warmPoolRef': {'name': self._next_warmpool_name()},
        'lifecycle': lifecycle,
    }
    return {
        'apiVersion': f'{self._group}/{self._version}',
        'kind': CLAIM_KIND,
        'metadata': {'name': name},
        'spec': spec,
    }

  def create(
      self, name: str, sleeper: Callable[[float], None] = time.sleep
  ) -> None:
    """Creates a SandboxClaim, retrying on 429 up to _CREATE_MAX_RETRIES."""

    attempt = 0
    while True:
      try:
        self._api.create_namespaced_custom_object(
            self._group,
            self._version,
            self._namespace,
            self._plural,
            body=self._body(name),
        )
        return
      except ApiException as exc:
        if exc.status == 429 and attempt < _CREATE_MAX_RETRIES:
          attempt += 1
          retry_after = _CREATE_RETRY_AFTER_DEFAULT
          try:
            if exc.headers and exc.headers.get('Retry-After'):
              retry_after = float(exc.headers['Retry-After'])
          except (TypeError, ValueError):
            pass
          retry_after = min(retry_after, _CREATE_RETRY_AFTER_CAP)
          logging.warning(
              'Claim %s create got 429; retry %d/%d after %.1fs',
              name,
              attempt,
              _CREATE_MAX_RETRIES,
              retry_after,
          )
          sleeper(retry_after)
        else:
          raise

  def is_ready(self, status: dict[str, Any]) -> bool:
    for condition in status.get('conditions') or []:
      if condition.get('type') == 'Ready' and condition.get('status') == 'True':
        return True
    return False

  def served_warm(self, status: dict[str, Any]) -> Optional[bool]:
    """Warm-served iff the bound sandbox name carries the warm pool prefix.

    Cold-provisioned sandboxes are named after the claim; warm-served ones
    are named after the warm pool.

    Args:
      status: The SandboxClaim's status dictionary.

    Returns:
      True if warm-served, False if cold-provisioned, or None if no sandbox
      is bound yet.
    """
    sandbox = status.get('sandbox') or {}
    name = sandbox.get('name')
    if not name:
      return None
    if self._warmpool_names:
      return any(name.startswith(wp) for wp in self._warmpool_names)
    return bool(self._warmpool_name) and name.startswith(self._warmpool_name)

  def stream_claim_events(
      self,
      stop_event: threading.Event,
      timeout_seconds: int = 60,
  ) -> Iterator[dict[str, Any]]:
    """Generates {'name': str, 'status': dict} dicts from a Watch stream.

    Re-establishes the watch if it times out, until stop_event is set.
    Resumes from the last seen resourceVersion so no events are missed across
    reconnects (BOOKMARK events advance the cursor without being yielded).
    Isolated as a method so tests can substitute a fake generator.

    Args:
      stop_event: threading.Event; when set the generator stops after the
        current watch iteration completes.
      timeout_seconds: Per-watch-call timeout forwarded to the Watch stream.

    Yields:
      {'name': str, 'status': dict} for every SandboxClaim event.
    """

    resource_version = None
    while not stop_event.is_set():
      w = kwatch.Watch()
      try:
        stream_kwargs = dict(
            timeout_seconds=timeout_seconds,
            allow_watch_bookmarks=True,
        )
        if resource_version is not None:
          stream_kwargs['resource_version'] = resource_version
        for _raw_event in w.stream(
            self._api.list_namespaced_custom_object,
            self._group,
            self._version,
            self._namespace,
            self._plural,
            **stream_kwargs,
        ):
          event: dict[str, Any] = cast(dict[str, Any], _raw_event)
          if stop_event.is_set():
            w.stop()
            return
          obj = event['object']
          if event.get('type') == 'ERROR':
            code = (
                obj.get('code')
                if isinstance(obj, dict)
                else getattr(obj, 'code', None)
            )
            logging.warning('Watch stream ERROR event (code=%s): %s', code, obj)
            resource_version = None
            w.stop()
            time.sleep(1)
            break
          rv = (obj.get('metadata') or {}).get('resourceVersion')
          if rv:
            resource_version = rv
          if event.get('type') == 'BOOKMARK':
            continue
          name = obj['metadata']['name']
          status = obj.get('status', {}) or {}
          yield {'name': name, 'status': status}
      except ApiException as exc:
        if stop_event.is_set():
          return
        if exc.status == 410:
          logging.warning(
              'Watch resourceVersion too old (410 Gone); re-listing: %s', exc
          )
          resource_version = None
        else:
          logging.warning('Watch stream API error (will reconnect): %s', exc)
          time.sleep(1)
      except Exception as exc:  # pylint: disable=broad-exception-caught
        if stop_event.is_set():
          return
        logging.warning('Watch stream error (will reconnect): %s', exc)
        time.sleep(1)

  def delete(self, name: str) -> None:
    self._delete_api.delete_namespaced_custom_object(
        self._group, self._version, self._namespace, self._plural, name
    )

  @property
  def custom_objects_api(self):
    return self._api


class LoadGenerator:
  """Submits claims at a target QPS; records readiness via a Watch stream.

  Architecture:
    - A background thread consumes stream_claim_events() and records ready_at
      timestamps in shared maps (no per-claim API polling).
    - A bounded ThreadPoolExecutor submits creates only (one API call per task,
      no blocking wait inside the task).
    - After all creates are submitted, the run() method drains locally by
      polling the in-memory maps until all claims are accounted for or the
      ready_timeout elapses.
    - Cleanup deletes all created claims best-effort after the run.
  """

  def __init__(
      self,
      driver: ClaimDriver,
      ready_timeout: float,
      max_concurrent: int,
      workload_executor: Optional[WorkloadExecutor] = None,
      workload_duration: float = 0,
      claim_prefix: str = 'claim',
  ) -> None:
    self._driver = driver
    self._ready_timeout = ready_timeout
    self._max_concurrent = max_concurrent
    self._workload_duration = workload_duration
    self._executor = workload_executor or NoopExecutor()
    self._claim_prefix = claim_prefix
    self._lock = threading.Lock()
    self._in_flight = 0
    self._peak = 0
    self._clock = time.monotonic
    self._hold_pool = None

  @property
  def peak_concurrency(self):
    return self._peak

  def _watch_consumer(
      self,
      stop_event,
      ready_at_map,
      warm_map,
      requested_at_map,
      released,
      exec_started_map,
      exec_completed_map,
      released_at_map,
      errors_map,
  ):
    """Background thread: consume watch events and record readiness."""
    for event in self._driver.stream_claim_events(stop_event):
      name = event['name']
      status = event['status']
      if not self._driver.is_ready(status):
        continue
      with self._lock:
        if name not in requested_at_map:
          continue
        if name in ready_at_map:
          continue  # already recorded
        ready_at_map[name] = self._clock()
        warm_map[name] = self._driver.served_warm(status)
      hold_pool = self._hold_pool
      if hold_pool is not None:
        try:
          hold_pool.submit(
              self._hold_task,
              name,
              status,
              exec_started_map,
              exec_completed_map,
              released_at_map,
              released,
              errors_map,
          )
        except RuntimeError:
          # Pool was shut down concurrently during cleanup.
          pass

  def _hold_task(
      self,
      name,
      status,
      exec_started_map,
      exec_completed_map,
      released_at_map,
      released,
      errors_map,
  ):
    """Worker thread: prepare and run workload, then delete the claim."""
    try:
      prepared = self._executor.prepare(name, status)
    except Exception as exc:  # pylint: disable=broad-exception-caught
      logging.warning('Failed to prepare workload for claim %s: %s', name, exc)
      prepared = None
    exec_started = self._clock()
    exec_error = None
    try:
      self._executor.execute(name, status, prepared)
    except Exception as exc:  # pylint: disable=broad-exception-caught
      logging.warning('Workload exec failed for claim %s: %s', name, exc)
      # Leave exec timings unset so a bad exec does not skew exec_duration
      exec_started = None
      exec_error = type(exc).__name__
    finally:
      exec_completed = self._clock() if exec_started is not None else None
      deleted = False
      try:
        self._driver.delete(name)
        deleted = True
      except Exception as exc:  # pylint: disable=broad-exception-caught
        logging.warning('Failed to release claim %s: %s', name, exc)
      with self._lock:
        if exec_error is not None:
          errors_map[name] = exec_error
        exec_started_map[name] = exec_started
        exec_completed_map[name] = exec_completed
        released_at_map[name] = self._clock()
        if deleted:
          released.add(name)
        self._in_flight -= 1

  def run(
      self,
      shape: RunShape,
      clock: Callable[[], float] = time.monotonic,
      sleeper: Callable[[float], None] = time.sleep,
  ) -> list[ClaimRecord]:
    """Run the load shape, returning a list of ClaimRecord in submission order.

    Args:
      shape: RunShape describing qps, duration, and total claims.
      clock: Callable returning a float timestamp (injectable for tests).
      sleeper: Callable(seconds) for pacing sleeps (injectable for tests).

    Returns:
      List of ClaimRecord, one per submitted claim index, in order.
    """
    self._clock = clock

    requested_at_map = {}
    ready_at_map = {}
    warm_map = {}
    errors_map = {}
    created_set = set()
    released = set()
    exec_started_map = {}
    exec_completed_map = {}
    released_at_map = {}
    submitted_names = []

    stop_event = threading.Event()

    # Initialize hold_pool before starting watch_thread so that a Ready event
    # from a pre-existing claim cannot race against self._hold_pool being None.
    hold_pool = futures.ThreadPoolExecutor(
        max_workers=self._max_concurrent, thread_name_prefix='claim-hold'
    )
    self._hold_pool = hold_pool

    watch_thread = threading.Thread(
        target=self._watch_consumer,
        args=(
            stop_event,
            ready_at_map,
            warm_map,
            requested_at_map,
            released,
            exec_started_map,
            exec_completed_map,
            released_at_map,
            errors_map,
        ),
        daemon=True,
        name='claim-watch-consumer',
    )
    watch_thread.start()

    def _create_task(name):
      # Record requested_at and increment _in_flight BEFORE create() so the
      # watch consumer always finds the entry and _in_flight never goes negative
      # even if a Ready event arrives and completes before create() returns.
      with self._lock:
        requested_at_map[name] = clock()
        self._in_flight += 1
        self._peak = max(self._peak, self._in_flight)
      try:
        self._driver.create(name)
        with self._lock:
          created_set.add(name)
      except Exception as exc:  # pylint: disable=broad-exception-caught
        logging.warning(
            'Claim %s create failed: %s: %s', name, type(exc).__name__, exc
        )
        with self._lock:
          self._in_flight -= 1
          errors_map[name] = type(exc).__name__

    start = clock()
    last_create_time = start
    create_futs = []

    try:
      with futures.ThreadPoolExecutor(max_workers=self._max_concurrent) as pool:
        for idx in range(shape.total):
          name = f'{self._claim_prefix}-{idx}'
          submitted_names.append(name)
          deadline = start + (idx / shape.qps)
          now = clock()
          if now < deadline:
            sleeper(deadline - now)
          last_create_time = clock()
          create_futs.append(pool.submit(_create_task, name))
        # Wait for all create tasks to complete before starting drain.
        for fut in create_futs:
          fut.result()

      # Drain: wait until every created claim is accounted for, or until the
      # deadline elapses since the last create. "Accounted" means the claim
      # appears in released_at_map (i.e. _hold_task completed and recorded
      # released_at) or in errors_map (create or ready failed).
      drain_window = self._ready_timeout + self._workload_duration
      drain_deadline = last_create_time + drain_window
      while True:
        now = clock()
        with self._lock:
          accounted = sum(
              1 for n in created_set if n in released_at_map or n in errors_map
          )
          outstanding = len(created_set) - accounted
        if outstanding <= 0:
          break
        if now >= drain_deadline:
          # Mark remaining created-but-never-Ready claims as Timeout.
          # Claims that already reached Ready (name in ready_at_map) must
          # never be Timeout'd: their startup_time is already captured and
          # their hold will finish in the finally block below.
          with self._lock:
            for name in list(created_set):
              if (
                  name not in released
                  and name not in errors_map
                  and name not in ready_at_map
              ):
                errors_map[name] = 'Timeout'
          break
        sleeper(0.1)

    finally:
      # Always stop the watch thread and delete any created-but-not-released
      # claims. Runs on normal completion AND on KeyboardInterrupt/exception
      # so Ctrl-C or crashes do not leak SandboxClaims (and their pods).
      stop_event.set()
      # Join the watch thread first so any in-progress _watch_consumer
      # iteration finishes submitting hold tasks before we shut down the pool.
      watch_thread.join(timeout=2.0)
      if watch_thread.is_alive():
        logging.warning(
            'Watch consumer thread did not exit within 2s; '
            'proceeding with hold pool shutdown.'
        )
      # Null the pool reference before shutting down so a still-running watch
      # consumer cleanly skips task submission instead of raising RuntimeError.
      self._hold_pool = None
      hold_pool.shutdown(wait=True)

      with self._lock:
        stragglers = [n for n in created_set if n not in released]
      for name in stragglers:
        try:
          self._driver.delete(name)
        except Exception as exc:  # pylint: disable=broad-exception-caught
          logging.warning('Failed to delete claim %s: %s', name, exc)

    # Build ordered result list.
    records = []
    for name in submitted_names:
      released_at = released_at_map.get(name)
      error = errors_map.get(name)
      # A claim that completed its hold (released_at set) is a success.
      # If a stale 'Timeout' error was recorded before the hold finished,
      # drop it so released_at is authoritative. Other error types (e.g.
      # create failures, exec errors) are kept regardless.
      if released_at is not None and error == 'Timeout':
        error = None
      rec = ClaimRecord(
          name=name,
          requested_at=requested_at_map.get(name, 0.0),
          ready_at=ready_at_map.get(name),
          error=error,
          warm_served=warm_map.get(name),
          exec_started_at=exec_started_map.get(name),
          exec_completed_at=exec_completed_map.get(name),
          released_at=released_at,
      )
      records.append(rec)
    return records


def main(argv: Optional[list[str]] = None) -> None:
  """Entry point when running as an in-pod script.

  Parses CLI arguments, runs the load generator, and writes one JSON line per
  ClaimRecord to --output followed by a summary line with peak_concurrency.

  Args:
    argv: List of arguments.
  """
  parser = argparse.ArgumentParser(
      description='Agent sandbox in-pod load runner.'
  )
  parser.add_argument(
      '--namespace', required=True, help='Kubernetes namespace.'
  )
  parser.add_argument(
      '--template-name',
      default='agent-sandbox',
      help='SandboxWarmPool / SandboxTemplate name.',
  )
  parser.add_argument(
      '--qps', type=float, default=None, help='Target claims per second.'
  )
  parser.add_argument(
      '--total',
      type=int,
      default=None,
      help='Total number of claims to submit.',
  )
  parser.add_argument(
      '--duration',
      type=float,
      default=None,
      help='Submission window in seconds.',
  )
  parser.add_argument(
      '--max-concurrent',
      type=int,
      default=200,
      help='Maximum in-flight claims.',
  )
  parser.add_argument(
      '--workload-duration',
      type=float,
      default=20.0,
      help='Seconds to hold each sandbox after Ready (20s = default).',
  )
  parser.add_argument(
      '--ready-timeout',
      type=float,
      default=180,
      help='Per-claim ready timeout in seconds.',
  )
  parser.add_argument(
      '--claim-prefix',
      default='claim',
      help='Prefix for generated SandboxClaim names (e.g. claim-0).',
  )
  parser.add_argument(
      '--output', required=True, help='Path to write JSONL results.'
  )
  args = parser.parse_args(argv)

  logging.basicConfig(
      level=logging.INFO,
      format='%(asctime)s [%(levelname)s] %(message)s',
      stream=sys.stderr,
  )

  try:
    config.load_incluster_config()
  except config.ConfigException:
    config.load_kube_config()

  # Apply bearer-token fix (needed for both in-cluster and kubeconfig paths
  # with kubernetes-client>=36).
  cfg = k8s_client.Configuration.get_default_copy()
  K8sClientFactory.register_bearer_token_auth(cfg)
  k8s_client.Configuration.set_default(cfg)

  shape = resolve_run_shape(
      qps=args.qps,
      duration=args.duration,
      total=args.total,
  )

  workload_duration = args.workload_duration
  if workload_duration > 0:
    pool_size = args.max_concurrent + 50
    client_factory = K8sClientFactory(pool_size)
    executor = StreamExecExecutor(
        custom_objects_api=client_factory.make_custom_objects_api(),
        core_v1_api=client_factory.make_core_v1_api(),
        namespace=args.namespace,
        workload_duration=workload_duration,
    )
  else:
    executor = None

  driver = ClaimDriver(
      namespace=args.namespace,
      warmpool_name=args.template_name,
      max_concurrent=args.max_concurrent,
      load_config=False,
  )

  generator = LoadGenerator(
      driver=driver,
      ready_timeout=args.ready_timeout,
      max_concurrent=args.max_concurrent,
      workload_executor=executor,
      workload_duration=workload_duration,
      claim_prefix=args.claim_prefix,
  )

  records = generator.run(shape)
  peak_concurrency = generator.peak_concurrency

  with open(args.output, 'w') as f:
    for rec in records:
      line = json.dumps(dataclasses.asdict(rec))
      f.write(line + '\n')
      print(line, file=sys.stdout, flush=True)
    summary_line = json.dumps(
        {'summary': {'peak_concurrency': peak_concurrency}}
    )
    f.write(summary_line + '\n')
    print(summary_line, file=sys.stdout, flush=True)


if __name__ == '__main__':
  main()
