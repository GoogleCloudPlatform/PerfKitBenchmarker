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
"""SwapDaemonSet: PKB BaseResource for the swap-encryption privileged DaemonSet.

Manages the full lifecycle of the privileged benchmark pod used by the
swap_encryption benchmark:

  _Create()   — apply the Jinja2 manifest via kubernetes_commands.ApplyManifest
        and wait for the pod to reach Running + /tmp/pkb_ready.
  _Delete()   — run in-pod cleanup (swapoff, dmsetup remove, losetup teardown,
        pkill fio/stress-ng) then kubectl delete daemonset.
  PodExec()   — kubectl exec wrapper with transient-reset retry, OOM-kill (rc=137)
        detection, and automatic RecoverPod() after eviction or container
        restart.
  WaitForPod()  — polls for Running phase + sentinel; updates self.pod_name.
  RecoverPod()  — waits for DaemonSet to recreate / restart the container,
                  checking deletionTimestamp to avoid false-positive Running state.

Extracted from swap_encryption_benchmark.py to satisfy PKB resource pattern
(go/pkb-resources): infrastructure lifecycle belongs in BaseResource subclasses,
not in benchmark files.
"""

import logging
import textwrap
import time
from typing import Optional

from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

def _ParseCipher(dmsetup_status: str) -> str:
  """Extract cipher name from dmsetup status output."""
  parts = dmsetup_status.split()
  try:
    idx = parts.index('crypt')
    return parts[idx + 1] if idx + 1 < len(parts) else ''
  except ValueError:
    return ''


# Transient kubectl errors that are safe to retry automatically.
_TRANSIENT_KUBECTL_ERRORS = ('connection reset by peer', 'websocket: close')

# Errors indicating the container / pod is gone and needs full recovery.
_CONTAINER_GONE_KUBECTL_ERRORS = (
  'container not found',
  'procready not received',
  'unable to upgrade connection',
  'not found',
  'deleted state',
)


class SwapDaemonSet(resource.BaseResource):
  """PKB resource for the swap-encryption benchmark privileged DaemonSet.

  The DaemonSet runs a single privileged pod on the benchmark nodepool.
  It installs measurement tools (fio, cryptsetup, mdadm, sysstat, nvme-cli),
  verifies the swap device is active, then writes /tmp/pkb_ready.  All
  benchmark phases execute commands inside this pod via PodExec().

  Attributes:
      name: DaemonSet metadata.name (e.g. 'pkb-swap-benchmark').
      namespace: Kubernetes namespace (typically 'default').
      label: Pod label value for app= selector.
      nodepool: pkb_nodepool label value pinning the DaemonSet to the
    benchmark node.
      image: Container image (e.g. 'ubuntu:22.04').
      pod_name: Name of the currently active pod; updated by WaitForPod /
    RecoverPod on eviction.
      oom_events: Pod names that triggered rc=137 OOM-kill; read by Run()
    for the degradation gate.
      pod_lost: Pod names that went NotFound during PodExec; read by Run()
    for the degradation gate.
  """

  RESOURCE_TYPE = 'SwapDaemonSet'
  REQUIRED_ATTRS = []

  def __init__(
    self,
    name: str,
    namespace: str,
    label: str,
    nodepool: str,
    image: str,
  ) -> None:
    super().__init__()
    self.name = name
    self.namespace = namespace
    self.label = label
    self.nodepool = nodepool
    self.image = image
    # Active pod tracking — updated by WaitForPod / RecoverPod.
    self.pod_name: Optional[str] = None
    # Per-run accumulators read by Run() for the degradation gate.
    self.oom_events: list[str] = []
    self.pod_lost: list[str] = []

  # ── PKB lifecycle ─────────────────────────────────────────────────────────

  def _Create(self) -> None:
    """Apply the DaemonSet manifest and wait for the pod to be ready."""
    kubernetes_commands.ApplyManifest(
      'cluster/swap_encryption_daemonset.yaml.j2',
      ds_name=self.name,
      ds_namespace=self.namespace,
      ds_label=self.label,
      benchmark_nodepool=self.nodepool,
      image=self.image,
    )
    logging.info('[swap_encryption] Swap-infra DaemonSet applied')
    self.WaitForPod()  # raises PrepareException on timeout

  def _Delete(self) -> None:
    """Run in-pod teardown then delete the DaemonSet.

    Runs swapoff, dmsetup remove, losetup cleanup, and pkill inside the
    pod (best-effort, ignore_failure=True) before deleting the DaemonSet.
    This mirrors the original Cleanup() logic so no swap state is leaked.
    """
    # Try to get the pod name quickly if not set; ignore timeout during cleanup.
    if self.pod_name is None:
      try:
        self.WaitForPod(timeout=30)
      except errors.Benchmarks.PrepareException:
        logging.info('[swap_encryption] Pod not found during cleanup; proceeding with DaemonSet deletion.')

    if self.pod_name:
      self.PodExec(
        'swapoff -a 2>/dev/null || true',
        ignore_failure=True,
        _retries=0,
      )
      self.PodExec(
        textwrap.dedent("""\
          swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
          dmsetup remove --noudevrules --noudevsync \
                      swap_encrypted 2>/dev/null || true
        """),
        ignore_failure=True,
        _retries=0,
      )
      self.PodExec(
        textwrap.dedent("""\
          for backing in \
            /var/pkb_swap_backing \
            /run/pkb_swap_backing \
            /mnt/stateful_partition/pkb_swap_backing
          do
                      losetup -j "$backing" 2>/dev/null \
            | awk -F: '{print $1}' \
            | while read dev
                          do losetup -d "$dev" 2>/dev/null || true; done
                      rm -f "$backing"
          done
        """),
        ignore_failure=True,
        _retries=0,
      )
      self.PodExec(
        "pkill -9 'stress-ng|fio' 2>/dev/null || true",
        ignore_failure=True,
        _retries=0,
      )

    kubectl.RunKubectlCommand(
      [
        'delete',
        'daemonset',
        self.name,
        '-n',
        self.namespace,
        '--ignore-not-found',
      ],
      raise_on_failure=False,
    )
    logging.info('[swap_encryption] DaemonSet deleted')

  # ── Pod lifecycle helpers ─────────────────────────────────────────────────

  def WaitForPod(self, timeout: int = 600) -> str:
    """Wait until the DaemonSet pod is Running AND /tmp/pkb_ready exists.

    Two-phase poll:
          1. Wait for status.phase == Running.
          2. kubectl exec test -f /tmp/pkb_ready.

    The DaemonSet init script writes /tmp/pkb_ready only after verifying
    the swap device is active (up to 150 s) and installing all measurement
    tools (~1-2 min on cold APT cache).  The default 600 s covers
    worst-case APT latency on a freshly-booted node.

    Args:
          timeout: Maximum seconds to wait.

    Returns:
          Pod name on success; None on timeout.  Also updates self.pod_name.
    """
    deadline = time.time() + timeout
    last_phase = ''
    ready_pod = None

    while time.time() < deadline:
      # Step 1: wait for Running phase.
      if ready_pod is None:
        out, _, rc = kubectl.RunKubectlCommand(
          [
            'get',
            'pods',
            '-l',
            f'app={self.label}',
            '-n',
            self.namespace,
            '-o',
            (
              r'jsonpath={range .items[*]}'
              r'{.metadata.name}{"\t"}'
              r'{.status.phase}{"\n"}{end}'
            ),
          ],
          raise_on_failure=False,
        )
        if rc == 0 and out.strip():
          for line in out.strip().splitlines():
            parts = line.split('\t')
            if len(parts) == 2:
              pod_name = parts[0].strip()
              phase = parts[1].strip()
              if phase == 'Running':
                logging.info(
                  '[swap_encryption] Pod %s is Running — waiting for sentinel...',
                  pod_name,
                )
                ready_pod = pod_name
                break
              if phase != last_phase:
                logging.info(
                  '[swap_encryption] Pod %s phase: %s',
                  pod_name,
                  phase,
                )
                last_phase = phase
                if phase == 'Pending':
                  self._LogPodEvents(pod_name)
        else:
          logging.info(
            '[swap_encryption] Waiting for DaemonSet pod to'
            ' appear...'
          )

      # Step 2: poll for /tmp/pkb_ready sentinel.
      if ready_pod is not None:
        _, sentinel_err, sentinel_rc = kubectl.RunKubectlCommand(
          [
            'exec',
            ready_pod,
            '-n',
            self.namespace,
            '--',
            'test',
            '-f',
            '/tmp/pkb_ready',
          ],
          raise_on_failure=False,
        )
        if sentinel_rc == 0:
          logging.info(
            '[swap_encryption] Pod %s ready (swap device active)',
            ready_pod,
          )
          self.pod_name = ready_pod
          return ready_pod
        # Container crashed (CrashLoopBackOff / exited) — reset and
        # re-check pod phase on the next iteration.
        if 'container not found' in sentinel_err or (
          'unable to upgrade connection' in sentinel_err
        ):
          logging.info(
            '[swap_encryption] Pod %s: container not running (%s) — will re-check pod state',
            ready_pod,
            sentinel_err.strip(),
          )
          ready_pod = None
          last_phase = ''
        else:
          logging.info(
            '[swap_encryption] Pod %s: still installing tools...',
            ready_pod,
          )

      time.sleep(15)

    raise errors.Benchmarks.PrepareException(
        f'[swap_encryption] Benchmark pod not ready after {timeout}s. '
        'Check DaemonSet events and node status.'
    )

  def _LogPodEvents(self, pod_name: str) -> None:
    """Dump recent Kubernetes events for a pod to help diagnose hangs."""
    events_out, _, _ = kubectl.RunKubectlCommand(
      ['describe', 'pod', pod_name, '-n', self.namespace],
      raise_on_failure=False,
    )
    in_events = False
    lines = []
    for line in events_out.splitlines():
      if line.startswith('Events:'):
        in_events = True
      if in_events:
        lines.append(line)
    if lines:
      logging.info(
        '[swap_encryption] Pod events:\n%s', '\n'.join(lines[:30])
      )
    else:
      logging.info(
        '[swap_encryption] kubectl describe output:\n%s',
        events_out[-2000:] if len(events_out) > 2000 else events_out,
      )

  def GetResourceMetadata(self) -> dict:
    """Return metadata dict describing the active swap device and kernel.

    Called by the benchmark to attach swap-specific metadata to all samples.
    Raises on error rather than silently swallowing failures.

    Returns:
      Dict with keys: swap_device, kernel_version, swap_cipher.

    Raises:
      errors.Benchmarks.RunError: if the pod is not reachable or commands fail.
    """
    swap_dev = self._DetectSwapDevice()
    kver, _ = self.PodExec('uname -r')
    cipher = ''
    if swap_dev:
      dm_out, _ = self.PodExec(
          f'dmsetup status {swap_dev} 2>/dev/null || echo not_encrypted'
      )
      cipher = _ParseCipher(dm_out)
    return {
        'swap_device': swap_dev or 'unknown',
        'kernel_version': kver.strip(),
        'swap_cipher': cipher,
    }

  def _DetectSwapDevice(self) -> str:
    """Return the first active swap device name (e.g. 'dm-0') or empty string.

    Raises:
      errors.Benchmarks.RunError: propagated from PodExec on failure.
    """
    out, _ = self.PodExec("awk 'NR>1 {print $1}' /proc/swaps")
    dev = out.strip().split('\n')[0].strip()
    return dev.split('/')[-1] if dev else ''

  def _IsPodGone(self, pod: str) -> bool:
    """Return True if the named pod no longer exists in the cluster."""
    _, err, rc = kubectl.RunKubectlCommand(
      [
        'get',
        'pod',
        pod,
        '-n',
        self.namespace,
        '-o',
        'jsonpath={.metadata.name}',
      ],
      raise_on_failure=False,
      timeout=15,
    )
    return rc != 0 and 'not found' in (err or '').lower()

  def PodExec(
    self,
    cmd: str,
    ignore_failure: bool = False,
    timeout: int = 300,
    _retries: int = 2,
  ) -> tuple[str, str]:
    """Run a shell command inside the benchmark pod via kubectl exec.

    Handles:
          - Transient GKE websocket resets: automatic retry (up to _retries).
          - OOM kill (rc=137): records to self.oom_events, calls RecoverPod,
      does NOT retry the OOM-triggering command itself.
          - Container/pod gone: records to self.pod_lost, calls RecoverPod,
      retries the command on the recovered pod.

    Uses self.pod_name as the active pod; RecoverPod updates it on eviction.

    Args:
          cmd: Shell command string passed to bash -c.
          ignore_failure: When True, non-zero exit codes are logged but not
      raised.
          timeout: Seconds before PKB kills the kubectl exec process.  Pass a
      larger value for long-running jobs (fio, stress-ng, kernel build).
          _retries: Max automatic retries on transient websocket resets.

    Returns:
          Tuple of (stdout, stderr) strings.
    """
    active = self.pod_name

    for attempt in range(_retries + 1):
      out, err, rc = kubectl.RunKubectlCommand(
        [
          'exec',
          active,
          '-n',
          self.namespace,
          '--',
          'bash',
          '-c',
          cmd,
        ],
        raise_on_failure=False,
        raise_on_timeout=False,
        timeout=timeout,
      )

      # Retry transient GKE websocket resets.
      is_transient = rc != 0 and any(
        e in err for e in _TRANSIENT_KUBECTL_ERRORS
      )
      if is_transient and attempt < _retries:
        logging.info(
          '[swap_encryption] kubectl exec connection reset (attempt %d/%d); retrying in 10 s',
          attempt + 1,
          _retries + 1,
        )
        time.sleep(10)
        continue

      # rc=137 (SIGKILL): OOM killer terminated the container process.
      # Do NOT retry — log, recover, and return so the caller can decide.
      if rc == 137:
        if active not in self.oom_events:
          self.oom_events.append(active)
        # Kubernetes takes a few seconds to update pod state after
        # eviction — sleep before checking to avoid false-positive Running.
        logging.info(
          '[swap_encryption] rc=137 — sleeping 15 s for Kubernetes'
          ' to update pod state before recovery check'
        )
        time.sleep(15)
        if self._IsPodGone(active):
          logging.info(
            '[swap_encryption] OOM-eviction detected (rc=137, pod'
            ' gone) — recovering pod name for subsequent commands'
          )
        else:
          logging.info(
            '[swap_encryption] Container OOM-killed (rc=137, pod'
            ' still exists) — waiting for container restart'
          )
        new_pod = self.RecoverPod(active)
        if new_pod != active:
          logging.info(
            '[swap_encryption] Pod name updated: %s → %s',
            active,
            new_pod,
          )
          self.pod_name = new_pod
          active = new_pod
        break  # OOM cmd is never re-run on the recovered pod.

      # Container or pod gone: record loss, try RecoverPod, retry cmd.
      is_container_gone = rc != 0 and any(
        e in err.lower() for e in _CONTAINER_GONE_KUBECTL_ERRORS
      )
      if is_container_gone:
        if active and active not in self.pod_lost:
          self.pod_lost.append(active)
          logging.error(
            '[swap_encryption] Benchmark pod %s is gone (%s) — recording run as degraded',
            active,
            (err or '').strip()[:160],
          )
        if attempt < _retries:
          logging.info(
            '[swap_encryption] Container gone/restarting (attempt %d/%d) — waiting for pod to recover...',
            attempt + 1,
            _retries + 1,
          )
          new_pod = self.RecoverPod(active)
          if new_pod != active:
            logging.info(
              '[swap_encryption] Pod name updated: %s → %s',
              active,
              new_pod,
            )
            self.pod_name = new_pod
            active = new_pod
          continue
      break

    if rc != 0 and not ignore_failure:
      raise errors.VmUtil.IssueCommandError(
        f'[swap_encryption] PodExec failed (rc={rc}): {err}'
      )
    return out, err

  def RecoverPod(self, pod: str, timeout_sec: int = 600) -> str:
    """Wait for the DaemonSet to recover after OOM kill or eviction.

    Handles two scenarios:
          1. Container OOM restart: same pod name, container restarting in
             place (DaemonSet restartPolicy=Always).
          2. Pod eviction/deletion: pod is gone; DaemonSet creates a new pod
             with a DIFFERENT name.

    Checks metadata.deletionTimestamp in addition to status.phase to
    catch the Terminating state where phase may still read Running.

    Args:
          pod: Original pod name to monitor.
          timeout_sec: Maximum seconds to wait for recovery.

    Returns:
          The (possibly new) pod name once Running and /tmp/pkb_ready is
          present.
    """
    deadline = time.time() + timeout_sec
    logging.info(
      '[swap_encryption] Waiting for pod %s to recover (up to %ds)...',
      pod,
      timeout_sec,
    )

    # Phase 1: find a Running pod that is NOT being terminated.
    recovered_pod = pod
    while time.time() < deadline:
      # Query both phase and deletionTimestamp in a single call.
      status_out, status_err, status_rc = kubectl.RunKubectlCommand(
        [
          'get',
          'pod',
          pod,
          '-n',
          self.namespace,
          '-o',
          'jsonpath={.status.phase}|{.metadata.deletionTimestamp}',
        ],
        raise_on_failure=False,
        timeout=30,
      )
      fields = status_out.strip().split('|')
      phase = fields[0].strip() if fields else ''
      is_terminating = len(fields) > 1 and bool(fields[1].strip())

      # Genuine Running (not being deleted) — move to Phase 2.
      if status_rc == 0 and phase == 'Running' and not is_terminating:
        break

      # Pod gone or Terminating — look for a replacement by label.
      pod_gone_or_terminating = (
        status_rc != 0
        and 'not found' in (status_out + status_err).lower()
      ) or is_terminating
      if pod_gone_or_terminating:
        label_out, _, label_rc = kubectl.RunKubectlCommand(
          [
            'get',
            'pods',
            '-n',
            self.namespace,
            '-l',
            f'app={self.label}',
            '-o',
            (
              'jsonpath={range'
              ' .items[?(@.status.phase=="Running")]}'
              '{.metadata.name}{"\\n"}{end}'
            ),
          ],
          raise_on_failure=False,
          timeout=30,
        )
        new_pods = [
          p.strip()
          for p in label_out.strip().splitlines()
          if p.strip() and p.strip() != pod
        ]
        if label_rc == 0 and new_pods:
          recovered_pod = new_pods[0]
          logging.info(
            '[swap_encryption] Original pod %s gone/terminating; found replacement %s',
            pod,
            recovered_pod,
          )
          break

      time.sleep(10)
    else:
      raise errors.VmUtil.IssueCommandError(
          f'[swap_encryption] No Running pod found (original: {pod})'
          f' within {timeout_sec}s after OOM kill / eviction'
      )

    # Phase 2: wait for init script to finish (sentinel written last).
    while time.time() < deadline:
      ready_out, _, ready_rc = kubectl.RunKubectlCommand(
        [
          'exec',
          recovered_pod,
          '-n',
          self.namespace,
          '--',
          'bash',
          '-c',
          'test -f /tmp/pkb_ready && echo READY',
        ],
        raise_on_failure=False,
        timeout=30,
      )
      if ready_rc == 0 and 'READY' in ready_out:
        logging.info(
          '[swap_encryption] Pod %s recovered (swap device active)',
          recovered_pod,
        )
        self.pod_name = recovered_pod
        return recovered_pod
      time.sleep(15)

    raise errors.VmUtil.IssueCommandError(
        f'[swap_encryption] Pod {recovered_pod} did not become ready'
        f' within {timeout_sec}s after OOM kill / eviction'
    )
