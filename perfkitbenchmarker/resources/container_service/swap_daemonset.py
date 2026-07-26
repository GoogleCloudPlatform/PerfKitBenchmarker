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
  PodExec()   — kubectl exec wrapper with transient-reset retry, OOM-kill
  (rc=137)
        detection, and automatic RecoverPod() after eviction or container
        restart.
  WaitForPod()  — polls for Running phase + sentinel; updates self.pod_name.
  RecoverPod()  — waits for DaemonSet to recreate / restart the container,
                  checking deletionTimestamp to avoid false-positive Running
                  state.

Extracted from swap_encryption_benchmark.py to satisfy PKB resource pattern
(go/pkb-resources): infrastructure lifecycle belongs in BaseResource subclasses,
not in benchmark files.
"""

import logging
import textwrap
import time
from typing import Any, Optional

from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker.resources.container_service import kubectl
from perfkitbenchmarker.resources.container_service import kubernetes_commands

_SWAP_DEVICE = flags.DEFINE_string(
    'swap_encryption_device',
    '',
    'Explicit swap block-device path on the cluster node, e.g. '
    '/dev/nvme1n1 or /dev/dm-0.  When empty the benchmark auto-detects '
    'via /proc/swaps after setup.',
)

_SWAP_SIZE_GB = flags.DEFINE_integer(
    'swap_encryption_swap_size_gb',
    32,
    'Size in GB of the swap space to configure on the node. '
    'Ignored when a ready swap device already exists.',
)

_SWAP_TYPE = flags.DEFINE_enum(
    'swap_encryption_swap_type',
    'auto',
    ['auto', 'hyperdisk', 'lssd', 'boot_disk', 'instance_store', 'io2'],
    'Swap backing storage target, one per methodology test-matrix row:\n'
    '  GKE:  boot_disk (swap file on the OS boot disk — pd-balanced or '
    'hyperdisk-balanced, chosen via --swap_encryption_boot_disk_type),\n'
    '        hyperdisk (dedicated hyperdisk-balanced data disk),\n'
    '        lssd (dedicated Local SSD RAID-0).\n'
    '  AWS:  instance_store (NVMe Instance Store, Nitro-encrypted),\n'
    '        io2 (EBS io2 data/root volume).\n'
    'dm-crypt is applied on the GKE targets when '
    '--swap_encryption_enable_dmcrypt is set; AWS targets are encrypted by '
    'Nitro at the hardware level.  auto = detect from cloud + instance type.',
)

_ENABLE_ZSWAP = flags.DEFINE_boolean(
    'swap_encryption_enable_zswap',
    False,
    'Enable zswap (lz4 compressor, 20%% max pool) before running tests.',
)

_MIN_FREE_KBYTES = flags.DEFINE_integer(
    'swap_encryption_min_free_kbytes',
    65536,
    'Value written to /proc/sys/vm/min_free_kbytes to trigger earlier '
    'swapping. Set 0 to leave the kernel default unchanged.',
)

_DAEMONSET_IMAGE = flags.DEFINE_string(
    'swap_encryption_daemonset_image',
    'ubuntu:22.04',
    'Container image used for the privileged benchmark DaemonSet pod.',
)

_IO2_ENCRYPTED = flags.DEFINE_boolean(
    'swap_encryption_io2_encrypted',
    True,
    'When True (default), the dedicated io2 swap volume is created with EBS '
    'encryption (Nitro/KMS) -> matrix row "io2 + hardware encryption". '
    'Set False for the unencrypted io2 baseline row. Only applies when '
    '--swap_encryption_swap_type=io2 on AWS/EKS.',
)

_IO2_KMS_KEY_ID = flags.DEFINE_string(
    'swap_encryption_io2_kms_key_id',
    '',
    'Optional KMS key id/ARN for the encrypted io2 volume. Empty = the '
    'account default aws/ebs key. Ignored unless io2_encrypted is True.',
)

_ENABLE_DMCRYPT = flags.DEFINE_boolean(
    'swap_encryption_enable_dmcrypt',
    True,
    'When True (default), configure dm-crypt on the swap device — the '
    '"encryption enabled" column of the test matrix.  Set False to use '
    'plain swap (encryption disabled column).',
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
  CLOUD = 'Unknown'
  REQUIRED_ATTRS = ['CLOUD']

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
    self.pod_name: Optional[str] = None
    self.oom_events: list[Optional[str]] = []
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
          Pod name (str) on success. Also updates self.pod_name.

    Raises:
          errors.Benchmarks.PrepareException: if the pod is not ready within
          timeout.
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
                    '[swap_encryption] Pod %s is Running — waiting for'
                    ' sentinel...',
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
              '[swap_encryption] Waiting for DaemonSet pod to appear...'
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
              '[swap_encryption] Pod %s: container not running (%s) — will'
              ' re-check pod state',
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
          retries: Max automatic retries on transient websocket resets.

    Returns:
          Tuple of (stdout, stderr) strings.

    Raises:
          RuntimeError: if pod_name is None.
    """
    active = self.pod_name
    if not active:
      raise RuntimeError('Cannot execute command: pod_name is None')

    out, err, rc = '', '', -1
    for attempt in range(retries + 1):
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
      if is_transient and attempt < retries:
        logging.info(
            '[swap_encryption] kubectl exec connection reset (attempt %d/%d);'
            ' retrying in 10 s',
            attempt + 1,
            retries + 1,
        )
        time.sleep(10)
        continue

      # rc=137 (SIGKILL): OOM killer terminated the container process.
      # Do NOT retry — log, recover, and return so the caller can decide.
      if rc == 137:
        if active and active not in self.oom_events:
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
        break  # OOM cmd is never re-run on the recovered pod.

      # Container or pod gone: record loss, try RecoverPod, retry cmd.
      is_container_gone = rc != 0 and any(
          e in err.lower() for e in _CONTAINER_GONE_KUBECTL_ERRORS
      )
      if is_container_gone:
        if active and active not in self.pod_lost:
          self.pod_lost.append(active)
          logging.error(
              '[swap_encryption] Benchmark pod %s is gone (%s) — recording run'
              ' as degraded',
              active,
              (err or '').strip()[:160],
          )
        if attempt < retries:
          logging.info(
              '[swap_encryption] Container gone/restarting (attempt %d/%d) —'
              ' waiting for pod to recover...',
              attempt + 1,
              retries + 1,
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
          status_rc != 0 and 'not found' in (status_out + status_err).lower()
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
              '[swap_encryption] Original pod %s gone/terminating; found'
              ' replacement %s',
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

  # ── Swap device setup ──────────────────────────────────────────────────────

  def EnableZswap(self) -> None:
    """Enable zswap with lz4 compressor and 20% pool limit inside the pod."""
    logging.info('[swap_encryption] Enabling zswap (lz4, 20%% pool)')
    for cmd in [
        'echo 1      > /sys/module/zswap/parameters/enabled',
        'echo lz4    > /sys/module/zswap/parameters/compressor',
        'echo 20     > /sys/module/zswap/parameters/max_pool_percent',
        'echo z3fold > /sys/module/zswap/parameters/zpool',
    ]:
      self.PodExec(cmd, ignore_failure=True)

  def GetActiveSwapDevice(self, explicit_device: str = '') -> str:
    """Return the active swap device path on the cluster node.

    Args:
      explicit_device: If non-empty, returned directly without probing.

    Returns:
      Full device path, e.g. '/dev/mapper/swap_encrypted'.

    Raises:
      errors.Benchmarks.RunError: if no active swap device is found.
    """
    if explicit_device:
      return explicit_device
    dm_out, _ = self.PodExec(
        textwrap.dedent("""
            ACTIVE=$(awk 'NR==2{print $1}' /proc/swaps 2>/dev/null)
            if [ -n "$ACTIVE" ]
            then
              echo "$ACTIVE"
            elif test -e /dev/mapper/swap_encrypted
            then
              echo /dev/mapper/swap_encrypted
            fi
        """),
        ignore_failure=True,
    )
    dev = dm_out.strip().splitlines()[-1].strip() if dm_out.strip() else ''
    if dev:
      return dev
    raise errors.Benchmarks.RunError(
        '[swap_encryption] No active swap device found in the benchmark pod. '
        'Use --swap_encryption_device to specify one.'
    )

  def SetupSwap(
      self,
      swap_type: str,
      enable_dmcrypt: bool,
      swap_size_gb: int,
      io2_volume_id: str = '',
  ) -> None:
    """Configure swap on the cluster node (GKE or EKS).

    Args:
      swap_type: One of 'auto', 'hyperdisk', 'lssd', 'boot_disk',
        'instance_store', 'io2'.
      enable_dmcrypt: When True apply dm-crypt on the swap device (GKE).
      swap_size_gb: Swap size in GB for loop-device / file-backed paths.
      io2_volume_id: EBS volume id for the io2 swap path (AWS only).

    Raises:
      errors.Benchmarks.RunError: on unknown cloud or setup failure.
    """
    if self.CLOUD == 'GCP':
      self._SetupGkeSwap(swap_type, enable_dmcrypt, swap_size_gb)
    elif self.CLOUD == 'AWS':
      self._SetupEksSwap(swap_type, swap_size_gb, io2_volume_id)
    else:
      raise errors.Benchmarks.RunError(
          f'[swap_encryption] Unknown cloud {self.CLOUD!r}; cannot set up swap.'
      )

  def _SetupGkeSwap(
      self, swap_type: str, enable_dmcrypt: bool, swap_size_gb: int
  ) -> None:
    """Dispatch to the correct GKE storage-type sub-function."""
    if swap_type == 'auto':
      lssd_out, _ = self.PodExec(
          "lsblk -d -o NAME,MODEL | grep -i 'local\\|nvme' | "
          "grep -v 'nvme0' | awk '{print $1}' | head -1",
          ignore_failure=True,
      )
      swap_type = 'lssd' if lssd_out.strip() else 'hyperdisk'
    if swap_type == 'lssd':
      self._SetupGkeLssdSwap(enable_dmcrypt, swap_size_gb)
    elif swap_type == 'boot_disk':
      self._SetupGkeBootdiskSwap(swap_size_gb, enable_dmcrypt)
    else:
      self._SetupGkeHyperdiskSwap(enable_dmcrypt, swap_size_gb)

  def _SetupGkeHyperdiskSwap(
      self, enable_dmcrypt: bool, swap_size_gb: int
  ) -> None:
    """Configure dm-crypt swap on hyperdisk-balanced (GKE default).

    Falls back to a loop device when no dedicated data disk is attached.

    Args:
      enable_dmcrypt: Whether to apply dm-crypt.
      swap_size_gb: Swap size in GB.
    """
    logging.info('[swap_encryption] GKE: setting up dm-crypt on hyperdisk')
    boot_out, _ = self.PodExec(
        'lsblk -no pkname "$(findmnt -n -o SOURCE /)" 2>/dev/null | head -1',
        ignore_failure=True,
    )
    boot_base = boot_out.strip() or 'nvme0n1'
    disk_out, _ = self.PodExec(
        'lsblk -d -o NAME,TYPE | awk \'$2=="disk"{print $1}\' '
        f"| grep -v '^{boot_base}$' | head -1",
        ignore_failure=True,
    )
    disk_name = disk_out.strip()
    if not disk_name:
      logging.info(
          '[swap_encryption] No dedicated data disk — falling back to '
          'loop device on /mnt/stateful_partition (dmcrypt=%s)',
          enable_dmcrypt,
      )
      self._SetupGkeLoopDeviceSwap(swap_size_gb)
      return
    disk = f'/dev/{disk_name}'
    logging.info(
        '[swap_encryption] GKE: swap target disk: %s  dmcrypt=%s',
        disk,
        enable_dmcrypt,
    )
    self.PodExec(
        textwrap.dedent(f"""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    wipefs -a {disk} 2>/dev/null || true
  """),
        ignore_failure=True,
    )
    if enable_dmcrypt:
      self.PodExec(
          textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {disk})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{disk}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted
      swapon /dev/mapper/swap_encrypted
    """),
      )
      logging.info(
          '[swap_encryption] GKE: dm-crypt swap active on '
          '/dev/mapper/swap_encrypted'
      )
    else:
      self.PodExec(f'mkswap {disk} && swapon {disk}')
      logging.info('[swap_encryption] GKE: plain swap active on %s', disk)

  def _SetupGkeLoopDeviceSwap(self, swap_size_gb: int) -> None:
    """Plain loop-device swap for single-disk GKE nodes.

    dm-crypt is skipped — blocked by COS kernel namespace restrictions.
    Phase 1 (fio) is skipped for plain loop devices.

    Args:
      swap_size_gb: Swap size in GB.
    """
    backing = '/mnt/stateful_partition/pkb_swap_backing'
    self.PodExec(
        textwrap.dedent(f"""
    losetup -j {backing} 2>/dev/null | awk -F: '{{print $1}}' | \
      while read dev; do swapoff "$dev" 2>/dev/null || true
        losetup -d "$dev" 2>/dev/null || true; done
    rm -f {backing}
  """),
        ignore_failure=True,
    )
    logging.info(
        '[swap_encryption] GKE: creating %dG backing file', swap_size_gb
    )
    self.PodExec(
        f'fallocate -l {swap_size_gb}G {backing} 2>/dev/null || '
        f'truncate -s {swap_size_gb}G {backing}',
    )
    loop_out, _ = self.PodExec(
        f'LOOP=$(losetup -f) && losetup --direct-io=on "$LOOP" {backing} '
        '&& echo "$LOOP"',
    )
    loop_dev = loop_out.strip()
    if not loop_dev.startswith('/dev/loop'):
      raise errors.Benchmarks.RunError(
          f'[swap_encryption] losetup failed – output: {loop_out!r}'
      )
    self.PodExec(f'mkswap {loop_dev}')
    self.PodExec(f'swapon {loop_dev}')
    logging.info(
        '[swap_encryption] GKE: plain loop swap active on %s (Phase 1 skipped)',
        loop_dev,
    )

  def _SetupGkeBootdiskSwap(
      self, swap_size_gb: int, enable_dmcrypt: bool
  ) -> None:
    """Swap on the OS boot disk — methodology Table 0 rows 1-4.

    Creates a loop-backed swap file on /mnt/stateful_partition.
    Requires Ubuntu node image (dm-crypt blocked on COS from pod).

    Args:
      swap_size_gb: Swap size in GB.
      enable_dmcrypt: Whether to apply dm-crypt.
    """
    backing = '/mnt/stateful_partition/pkb_swap_backing'
    logging.info(
        '[swap_encryption] GKE: boot-disk swap (%dG, dmcrypt=%s)',
        swap_size_gb,
        enable_dmcrypt,
    )
    self.PodExec(
        textwrap.dedent(f"""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    losetup -j {backing} 2>/dev/null | awk -F: '{{print $1}}' | while read d
    do swapoff "$d" 2>/dev/null || true; losetup -d "$d" 2>/dev/null || true; done
    rm -f {backing}
  """),
        ignore_failure=True,
    )
    self.PodExec(
        f'fallocate -l {swap_size_gb}G {backing} 2>/dev/null || '
        f'truncate -s {swap_size_gb}G {backing}',
    )
    loop_out, _ = self.PodExec(
        f'LOOP=$(losetup -f) && losetup --direct-io=on "$LOOP" {backing} '
        '&& echo "$LOOP"',
    )
    loop_dev = (
        loop_out.strip().splitlines()[-1].strip() if loop_out.strip() else ''
    )
    if not loop_dev.startswith('/dev/loop'):
      raise errors.Benchmarks.RunError(
          f'[swap_encryption] boot-disk losetup failed: {loop_out!r}'
      )
    if enable_dmcrypt:
      self.PodExec(
          textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {loop_dev})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{loop_dev}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted && swapon /dev/mapper/swap_encrypted
    """),
      )
      logging.info(
          '[swap_encryption] GKE: boot-disk dm-crypt swap active on '
          '/dev/mapper/swap_encrypted'
      )
    else:
      self.PodExec(f'mkswap {loop_dev} && swapon {loop_dev}')
      logging.info(
          '[swap_encryption] GKE: boot-disk plain swap active on %s', loop_dev
      )

  def _SetupGkeLssdSwap(self, enable_dmcrypt: bool, swap_size_gb: int) -> None:
    """Configure dm-crypt on LSSD RAID-0 array (go/gke-swap-lssd).

    Falls back to hyperdisk path if no clean LSSD found, or to the
    stateful-loop path if all LSSDs are partitioned.

    Args:
      enable_dmcrypt: Whether to apply dm-crypt.
      swap_size_gb: Swap size in GB.
    """
    logging.info('[swap_encryption] GKE: setting up LSSD RAID-0 swap')
    self.PodExec(
        textwrap.dedent("""
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    swapoff -a 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
  """),
        ignore_failure=True,
    )
    topo, _ = self.PodExec(
        'lsblk -o NAME,TYPE,SIZE,ROTA,MOUNTPOINT 2>/dev/null',
        ignore_failure=True,
    )
    logging.info(
        '[swap_encryption] block device topology:\n%s', (topo or '').strip()
    )
    lssd_out, _ = self.PodExec(
        textwrap.dedent("""
        for d in $(lsblk -dno NAME,ROTA | awk '$2==0{print $1}')
        do
          if lsblk -no TYPE "/dev/$d" 2>/dev/null | grep -q '^part$'; then continue; fi
          if lsblk -no MOUNTPOINT "/dev/$d" 2>/dev/null | grep -q '[^[:space:]]'; then continue; fi
          echo "/dev/$d"
        done
      """),
        ignore_failure=True,
    )
    devices = [d.strip() for d in lssd_out.strip().splitlines() if d.strip()]
    if not devices:
      logging.info(
          '[swap_encryption] No clean local SSD — falling back to hyperdisk'
      )
      self._SetupGkeHyperdiskSwap(enable_dmcrypt, swap_size_gb)
      return
    device_list = ' '.join(devices)
    n = len(devices)
    logging.info(
        '[swap_encryption] GKE: LSSD RAID-0 across %d device(s): %s '
        ' dmcrypt=%s',
        n,
        device_list,
        enable_dmcrypt,
    )
    self.PodExec(
        textwrap.dedent(f"""
    for dev in {device_list}
    do
      test -b "$dev" || continue
      devname=$(basename "$dev")
      for holder in /sys/block/$devname/holders/*
      do
        test -e "$holder" || continue
        h=$(basename "$holder")
        if echo "$h" | grep -q "^md"; then
          mdadm --stop /dev/$h 2>/dev/null || true
        else
          dmsetup remove --force --noudevrules --noudevsync /dev/$h 2>/dev/null || true
        fi
      done
      mounts=$(awk -v d="$dev" '$1==d{{print $2}}' /proc-host/mounts 2>/dev/null || true)
      for mp in $mounts; do umount -f "$mp" 2>/dev/null || true; done
    done
    swapoff -a 2>/dev/null || true
    swapoff /dev/mapper/pkb_swap 2>/dev/null || true
    swapoff /dev/mapper/swap_encrypted 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync pkb_swap 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    mdadm --stop --scan 2>/dev/null || true
    mdadm --zero-superblock {device_list} 2>/dev/null || true
    wipefs -a {device_list} 2>/dev/null || true
    losetup -D 2>/dev/null || true
    rm -f /mnt/stateful_partition/pkb_swap.img 2>/dev/null || true
    sleep 2
  """),
        ignore_failure=True,
    )
    raw_check_out, _ = self.PodExec(
        textwrap.dedent(f"""
        for dev in {device_list}
        do
          if lsblk -ln -o TYPE "$dev" 2>/dev/null | grep -q '^part$'
          then echo "[pkb-lssd] $dev is partitioned" >&2
          else echo "$dev"
          fi
        done
      """),
        ignore_failure=True,
    )
    raw_devices = [
        d.strip() for d in raw_check_out.strip().splitlines() if d.strip()
    ]
    if not raw_devices:
      logging.info(
          '[swap_encryption] GKE: all LSSD partitioned — falling back to '
          'stateful-loop'
      )
      self._SetupGkeLssdStatefulLoopSwap(enable_dmcrypt)
      return
    devices = raw_devices
    device_list = ' '.join(devices)
    n = len(devices)
    if n > 1:
      self.PodExec(
          textwrap.dedent(f"""
      mdadm --create /dev/md0 --force \\
        --level=0 --raid-devices={n} {device_list}
      test -b /dev/md0 || {{ echo "mdadm: /dev/md0 not created" >&2; exit 1; }}
    """),
      )
      swap_block_dev = '/dev/md0'
    else:
      swap_block_dev = devices[0]
    if enable_dmcrypt:
      self.PodExec(
          textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      udevadm control --stop-exec-queue 2>/dev/null || true
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {swap_block_dev})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{swap_block_dev}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      udevadm control --start-exec-queue 2>/dev/null || true
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted && swapon /dev/mapper/swap_encrypted
    """),
      )
      logging.info(
          '[swap_encryption] GKE: LSSD dm-crypt swap active on %s',
          swap_block_dev,
      )
    else:
      self.PodExec(f'mkswap {swap_block_dev} && swapon {swap_block_dev}')
      logging.info(
          '[swap_encryption] GKE: LSSD plain swap active on %s', swap_block_dev
      )

  def _SetupGkeLssdStatefulLoopSwap(self, enable_dmcrypt: bool) -> None:
    """Set up swap on the LSSD partition via a loop device.

    Used when the local NVMe device is partitioned by GKE startup scripts.

    Args:
      enable_dmcrypt: Whether to apply dm-crypt.
    """
    img_path = '/mnt/stateful_partition/pkb_swap.img'
    self.PodExec(
        textwrap.dedent(f"""
    swapoff -a 2>/dev/null || true
    dmsetup remove --force --noudevrules --noudevsync swap_encrypted 2>/dev/null || true
    losetup -D 2>/dev/null || true
    rm -f {img_path} 2>/dev/null || true
  """),
        ignore_failure=True,
    )
    size_out, _ = self.PodExec(
        "df -P /mnt/stateful_partition | awk 'NR==2{print $4}'",
        ignore_failure=True,
    )
    avail_kb = int(size_out.strip() or '0')
    swap_gb = max(16, int(avail_kb * 0.8 / 1024 / 1024))
    logging.info(
        '[swap_encryption] GKE: LSSD stateful-loop: %d GB at %s',
        swap_gb,
        img_path,
    )
    self.PodExec(
        textwrap.dedent(f"""
    fallocate -l {swap_gb}G {img_path} 2>/dev/null || \\
      dd if=/dev/zero of={img_path} bs=1G count={swap_gb}
    chmod 600 {img_path}
    losetup --direct-io=on -f {img_path}
  """),
        timeout=300,
    )
    loop_out, _ = self.PodExec(
        f"losetup -j {img_path} | awk -F: '{{print $1}}' | head -1",
        ignore_failure=True,
    )
    loop_dev = loop_out.strip()
    if not loop_dev.startswith('/dev/loop'):
      raise errors.Benchmarks.RunError(
          f'[swap_encryption] losetup failed for {img_path}: {loop_out!r}'
      )
    if enable_dmcrypt:
      self.PodExec(
          textwrap.dedent(f"""
      grep -q dm_crypt /proc/modules 2>/dev/null || {{
        KO=$(find /lib/modules/$(uname -r) -name 'dm-crypt.ko*' 2>/dev/null | head -1)
        [ -n "$KO" ] && insmod "$KO" 2>/dev/null || true
      }}
      udevadm control --stop-exec-queue 2>/dev/null || true
      KEY=$(dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -A n -t x1 | tr -d ' \\n')
      SIZE=$(blockdev --getsz {loop_dev})
      printf "0 %s crypt aes-xts-plain64 %s 0 %s 0\\n" "$SIZE" "$KEY" "{loop_dev}" | \\
        dmsetup create swap_encrypted --noudevrules --noudevsync
      udevadm control --start-exec-queue 2>/dev/null || true
      unset KEY
      dmsetup mknodes swap_encrypted 2>/dev/null || true
      mkswap /dev/mapper/swap_encrypted && swapon /dev/mapper/swap_encrypted
    """),
      )
      logging.info(
          '[swap_encryption] GKE: LSSD stateful-loop dm-crypt active: %s',
          loop_dev,
      )
    else:
      self.PodExec(f'mkswap {loop_dev} && swapon {loop_dev}')
      logging.info(
          '[swap_encryption] GKE: LSSD stateful-loop plain swap active: %s',
          loop_dev,
      )

  def _SetupEksSwap(
      self, swap_type: str, swap_size_gb: int, io2_volume_id: str = ''
  ) -> None:
    """Configure swap on EKS — Instance Store OR io2 root disk."""
    if swap_type in ('auto', 'instance_store'):
      self._SetupEksInstanceStoreSwap(swap_size_gb)
    elif swap_type == 'io2':
      self._SetupEksIo2Swap(io2_volume_id)
    else:
      raise errors.Benchmarks.RunError(
          f'[swap_encryption] Unknown EKS swap type {swap_type!r}.'
      )

  def _SetupEksInstanceStoreSwap(self, swap_size_gb: int) -> None:
    """Swap on AWS NVMe Instance Store (Nitro hardware-offloaded encryption)."""
    logging.info('[swap_encryption] EKS: setting up Instance Store swap')
    nvme_out, _ = self.PodExec(
        "nvme list 2>/dev/null | awk '/Instance Storage/{print $1}' | head -1"
        " || lsblk -d -o NAME,MODEL | grep -i 'instance\\|nvme' | grep -v"
        " 'nvme0' | awk '{print \"/dev/\"$1}' | head -1",
        ignore_failure=True,
    )
    device = nvme_out.strip()
    if not device:
      for candidate in ['/dev/nvme1n1', '/dev/nvme2n1', '/dev/xvdb']:
        exists_out, _ = self.PodExec(
            f'test -b {candidate} && echo yes || echo no', ignore_failure=True
        )
        if exists_out.strip() == 'yes':
          device = candidate
          break
    if not device:
      logging.info(
          '[swap_encryption] No Instance Store NVMe — creating swapfile'
      )
      self.SetupPlainSwapFile(swap_size_gb)
      return
    logging.info('[swap_encryption] EKS: Instance Store device: %s', device)
    self.PodExec(f'mkswap {device} && swapon {device}')
    logging.info(
        '[swap_encryption] EKS: Instance Store swap active on %s', device
    )

  def _SetupEksIo2Swap(self, io2_volume_id: str = '') -> None:
    """Swap on AWS EBS io2 volume.

    Identifies the io2 device by NVMe serial number matching io2_volume_id,
    falling back to first non-root EBS device if serial match fails.

    Args:
      io2_volume_id: EBS volume id for the io2 swap path.
    """
    logging.info('[swap_encryption] EKS: setting up io2 EBS swap')
    root_out, _ = self.PodExec(
        'lsblk -no pkname $(findmnt -n -o SOURCE /) 2>/dev/null || echo'
        ' nvme0n1',
        ignore_failure=True,
    )
    root_base = root_out.strip() or 'nvme0n1'
    device = ''
    target = io2_volume_id.replace('-', '')
    if target:
      ser_out, _ = self.PodExec(
          'for d in /sys/block/nvme*n1; do [ -e "$d" ] || continue; s=$(cat'
          ' "$d/device/serial" 2>/dev/null | tr -d "-" | tr -d " "); [ "$s" ='
          f' "{target}" ] && {{ echo "/dev/$(basename "$d")"; break; }}; done',
          ignore_failure=True,
      )
      device = ser_out.strip()
      if device:
        logging.info(
            '[swap_encryption] EKS: io2 matched by serial %s -> %s',
            target,
            device,
        )
    if not device:
      disk_out, _ = self.PodExec(
          'for d in /sys/block/nvme*n1 /sys/block/xvd[b-z] /sys/block/sd[b-z];'
          ' do [ -e "$d" ] || continue; n=$(basename "$d");'
          f' [ "$n" = "{root_base}" ] && continue;'
          ' m=$(cat "$d/device/model" 2>/dev/null);'
          ' echo "$m" | grep -qi "Elastic Block Store" || continue;'
          ' mnt=$(lsblk -no MOUNTPOINT "/dev/$n" 2>/dev/null | tr -d " ");'
          ' [ -n "$mnt" ] && continue; echo "/dev/$n"; break; done',
          ignore_failure=True,
      )
      device = disk_out.strip()
    if not device:
      logging.info(
          '[swap_encryption] No io2 EBS disk — creating plain swapfile'
      )
      self.SetupPlainSwapFile(32)
      return
    logging.info('[swap_encryption] EKS: io2 EBS device: %s', device)
    out, _ = self.PodExec(
        textwrap.dedent(f"""
    swapoff {device} 2>/dev/null || true
    wipefs -a {device} 2>/dev/null || true
    mkswap -f {device} && swapon {device}
    swapon --show
  """),
        ignore_failure=True,
    )
    if device not in out:
      raise errors.Benchmarks.RunError(
          f'[swap_encryption] io2 swap did not activate on {device}; '
          f'swapon --show: {out!r}'
      )
    logging.info('[swap_encryption] EKS: io2 EBS swap active on %s', device)

  def SetupPlainSwapFile(self, size_gb: int) -> None:
    """Fallback: create a loop-device-backed swapfile.

    A plain file on overlayfs cannot be used as swap (EINVAL). A loop
    device presents a proper block device to the mm subsystem.

    Args:
      size_gb: Swap file size in GB.
    """
    logging.info('[swap_encryption] Creating %dGB loop-device swap', size_gb)
    self.PodExec(
        textwrap.dedent(f"""
    fallocate -l {size_gb}G /tmp/pkb_swapfile && \\
    chmod 600 /tmp/pkb_swapfile && \\
    LOOP=$(losetup -f) && \\
    losetup "$LOOP" /tmp/pkb_swapfile && \\
    mkswap "$LOOP" && \\
    swapon "$LOOP" && \\
    echo "swap loop device: $LOOP"
  """),
    )


class GcpSwapDaemonSet(SwapDaemonSet):
  CLOUD = 'GCP'


class AwsSwapDaemonSet(SwapDaemonSet):
  CLOUD = 'AWS'
