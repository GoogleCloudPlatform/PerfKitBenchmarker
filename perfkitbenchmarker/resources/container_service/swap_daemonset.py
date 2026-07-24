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
  PodExec()   — kubectl exec wrapper with transient-reset retry,
        OOM-kill (rc=137) detection, and automatic RecoverPod() after
        eviction or container restart.
  WaitForPod()  — polls for Running phase + sentinel; updates self.pod_name.
  RecoverPod()  — waits for DaemonSet to recreate/restart the container,
                  checking deletionTimestamp to avoid false-positive Running
                  state.

Extracted from swap_encryption_benchmark.py to satisfy PKB resource pattern
(go/pkb-resources): infrastructure lifecycle belongs in BaseResource subclasses,
not in benchmark files.
"""

import logging
import textwrap
from typing import Any, Optional

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

_DAEMONSET_IMAGE = flags.DEFINE_string(
    'swap_encryption_daemonset_image',
    'ubuntu:22.04',
    'Container image for the privileged benchmark DaemonSet.',
)


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
      nodepool: pkb_nodepool label value pinning the DaemonSet to the benchmark
        node.
      image: Container image (e.g. 'ubuntu:22.04').
      pod_name: Name of the currently active pod; updated by WaitForPod /
        RecoverPod on eviction.
      oom_events: Pod names that triggered rc=137 OOM-kill; read by Run() for
        the degradation gate.
      pod_lost: Pod names that went NotFound during PodExec; read by Run() for
        the degradation gate.
  """

  RESOURCE_TYPE = 'SwapDaemonSet'
  REQUIRED_ATTRS = []

  def __init__(
      self,
      name: str,
      namespace: str,
      label: str,
      nodepool: str,
  ) -> None:
    super().__init__()
    self.name = name
    self.namespace = namespace
    self.label = label
    self.nodepool = nodepool
    self.image = _DAEMONSET_IMAGE.value
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
        logging.info(
            '[swap_encryption] Pod not found during cleanup; proceeding with'
            ' DaemonSet deletion.'
        )

    if self.pod_name:
      self.PodExec(
          'swapoff -a 2>/dev/null || true',
          ignore_failure=True,
          retries=0,
      )
      self.PodExec(
          textwrap.dedent("""\
            swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
            dmsetup remove --noudevrules --noudevsync \
                        swap_encrypted 2>/dev/null || true
          """),
          ignore_failure=True,
          retries=0,
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
          retries=0,
      )
      self.PodExec(
          "pkill -9 'stress-ng|fio' 2>/dev/null || true",
          ignore_failure=True,
          retries=0,
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
    """Wait until the DaemonSet pod is Running AND /tmp/pkb_ready exists."""
    state = {'last_phase': '', 'ready_pod': ''}

    @vm_util.Retry(
        poll_interval=15,
        max_retries=-1,
        timeout=timeout,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def _Poll():
      if not state['ready_pod']:
        out_v, _, rc = kubectl.RunKubectlCommand(
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
        if rc == 0 and out_v.strip():
          for line in out_v.strip().splitlines():
            parts = line.split('\t')
            if len(parts) == 2:
              pod_name = parts[0].strip()
              phase = parts[1].strip()
              if phase == 'Running':
                logging.info(
                    '[swap_encryption] Pod %s is Running — waiting for'
                    ' sentinel...',
                    pod_name,
                )
                state['ready_pod'] = pod_name
                break
              if phase != state['last_phase']:
                logging.info(
                    '[swap_encryption] Pod %s phase: %s', pod_name, phase
                )
                state['last_phase'] = phase
                if phase == 'Pending':
                  self._LogPodEvents(pod_name)
        if not state['ready_pod']:
          raise errors.Resource.RetryableCreationError('Pod not Running')

      if state['ready_pod']:
        _, sentinel_err, sentinel_rc = kubectl.RunKubectlCommand(
            [
                'exec',
                state['ready_pod'],
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
              state['ready_pod'],
          )
          return state['ready_pod']
        if (
            'container not found' in sentinel_err
            or 'unable to upgrade connection' in sentinel_err
        ):
          logging.info(
              '[swap_encryption] Pod %s: container not running (%s) — will'
              ' re-check pod state',
              state['ready_pod'],
              sentinel_err.strip(),
          )
          state['ready_pod'] = ''
          state['last_phase'] = ''
        raise errors.Resource.RetryableCreationError('Sentinel not ready')

    try:
      ready_pod = _Poll()
      self.pod_name = ready_pod
      return ready_pod
    except errors.Resource.RetryableCreationError as exc:
      raise errors.Benchmarks.PrepareException(
          f'[swap_encryption] Benchmark pod not ready after {timeout}s. '
          'Check DaemonSet events and node status.'
      ) from exc

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
      logging.info('[swap_encryption] Pod events:\n%s', '\n'.join(lines[:30]))
    else:
      logging.info(
          '[swap_encryption] kubectl describe output:\n%s',
          events_out[-2000:] if len(events_out) > 2000 else events_out,
      )

  def GetResourceMetadata(self) -> dict[Any, Any]:
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
      retries: int = 2,
  ) -> tuple[str, str]:
    """Run a shell command inside the benchmark pod via kubectl exec."""
    active = self.pod_name
    if not active:
      raise errors.Benchmarks.RunError(
          '[swap_encryption] No active pod name. Call WaitForPod first.'
      )
    active_ref = [active]

    @vm_util.Retry(
        poll_interval=10,
        max_retries=retries,
        retryable_exceptions=(errors.VmUtil.IssueCommandError,),
    )
    def _RunCmd():
      curr_active = active_ref[0]
      out_v, err_v, rc_v = kubectl.RunKubectlCommand(
          ['exec', curr_active, '-n', self.namespace, '--', 'bash', '-c', cmd],
          raise_on_failure=False,
          raise_on_timeout=False,
          timeout=timeout,
      )

      is_transient = rc_v != 0 and any(
          e in err_v for e in _TRANSIENT_KUBECTL_ERRORS
      )
      if is_transient:
        logging.info(
            '[swap_encryption] kubectl exec connection reset; retrying in 10 s'
        )
        raise errors.VmUtil.IssueCommandError('transient kubectl error')

      if rc_v == 137:
        if curr_active not in self.oom_events:
          self.oom_events.append(curr_active)

        @vm_util.Retry(
            poll_interval=5,
            max_retries=3,
            retryable_exceptions=(errors.Resource.RetryableCreationError,),
        )
        def _CheckPodGone():
          if not self._IsPodGone(curr_active):
            raise errors.Resource.RetryableCreationError(
                'Pod state not updated yet'
            )

        logging.info(
            '[swap_encryption] rc=137 — waiting for Kubernetes to update pod'
            ' state before recovery check'
        )
        try:
          _CheckPodGone()
        except errors.Resource.RetryableCreationError:
          pass

        if self._IsPodGone(curr_active):
          logging.info(
              '[swap_encryption] OOM-eviction detected (rc=137, pod gone) —'
              ' attempting recovery'
          )
          self.RecoverPod(curr_active)
        else:
          logging.warning(
              '[swap_encryption] rc=137 but pod %s is still Running — check'
              ' dmesg/syslog',
              curr_active,
          )
        return out_v, err_v, rc_v, True

      is_gone = rc_v != 0 and any(
          e in err_v for e in _CONTAINER_GONE_KUBECTL_ERRORS
      )
      if is_gone:
        logging.info(
            '[swap_encryption] kubectl exec failed: container/pod lost (%s).'
            ' Attempting recovery.',
            err_v.strip(),
        )
        if curr_active not in self.pod_lost:
          self.pod_lost.append(curr_active)
        self.RecoverPod(curr_active)
        if not self.pod_name:
          raise errors.Benchmarks.RunError('pod_name lost during recovery')
        active_ref[0] = self.pod_name
        raise errors.VmUtil.IssueCommandError(
            'Container lost, retrying command'
        )

      return out_v, err_v, rc_v, False

    try:
      out, err, rc, halt = _RunCmd()
    except errors.VmUtil.IssueCommandError as exc:
      if not ignore_failure:
        raise errors.VmUtil.IssueCommandError(
            f'Command {cmd} failed after {retries} retries'
        ) from exc
      return '', ''

    if halt or (rc != 0 and not ignore_failure):
      if not ignore_failure:
        raise errors.VmUtil.IssueCommandError(
            f'Command {cmd} failed with rc={rc}\nerr:\n{err}\nout:\n{out}'
        )

    return out, err

  def RecoverPod(self, pod: str, timeout_sec: int = 300) -> str:
    """Recover from an OOM-killed (rc=137) or evicted/preempted benchmark pod."""

    @vm_util.Retry(
        poll_interval=10,
        max_retries=-1,
        timeout=timeout_sec,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def _FindNewPod():
      if not self._IsPodGone(pod):
        logging.info(
            '[swap_encryption] Original pod %s still exists; waiting for it to'
            ' recover',
            pod,
        )
        return pod

      label_out, _, label_rc = kubectl.RunKubectlCommand(
          [
              'get',
              'pods',
              '-l',
              f'app={self.label}',
              '-o',
              (
                  'jsonpath={range'
                  ' .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}'
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
        recovered = new_pods[0]
        logging.info(
            '[swap_encryption] Original pod %s gone/terminating; found'
            ' replacement %s',
            pod,
            recovered,
        )
        return recovered
      raise errors.Resource.RetryableCreationError(
          'No replacement pod found yet'
      )

    try:
      recovered_pod = _FindNewPod()
    except errors.Resource.RetryableCreationError as exc:
      raise errors.VmUtil.IssueCommandError(
          f'[swap_encryption] No Running pod found (original: {pod}) within'
          f' {timeout_sec}s after OOM kill / eviction'
      ) from exc

    @vm_util.Retry(
        poll_interval=15,
        max_retries=-1,
        timeout=timeout_sec,
        retryable_exceptions=(errors.Resource.RetryableCreationError,),
    )
    def _WaitReady():
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
        return
      raise errors.Resource.RetryableCreationError('Pod not ready yet')

    try:
      _WaitReady()
    except errors.Resource.RetryableCreationError as exc:
      raise errors.VmUtil.IssueCommandError(
          f'[swap_encryption] Pod {recovered_pod} did not become ready within'
          f' {timeout_sec}s after OOM kill / eviction'
      ) from exc

    self.pod_name = recovered_pod
    return recovered_pod
