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
"""SwapNodePool: PKB BaseResource for the swap-encryption benchmark nodepool.

Manages the lifecycle of:

  GKE nodepool  — gcloud container node-pools create with UBUNTU_CONTAINERD,
                  linuxConfig.swapConfig + sysctl via --system-config-from-file.
                  For LSSD machines: --local-nvme-ssd-block and
                  dedicatedLocalSsdProfile in the swap YAML.
                  For hyperdisk configs: boot-disk-provisioned-iops/throughput.

  Swap disk     — Optional dedicated hyperdisk attached post-nodepool creation
                  (for dm-crypt measurement on machines where the boot disk
                  cannot be used as a swap device directly).

  Default pool  — DeleteDefaultPool() removes the dummy e2-medium pool created
                  at cluster time once the DaemonSet pod is Running.

Extracted from swap_encryption_benchmark.py to satisfy PKB resource pattern
(go/pkb-resources): infrastructure lifecycle belongs in BaseResource subclasses.
"""

import logging
import os
import tempfile
import time

from perfkitbenchmarker import errors
from perfkitbenchmarker import resource
from perfkitbenchmarker.providers.gcp import util as gcp_util
from perfkitbenchmarker.resources.container_service import kubectl

# GCP Hyperdisk Balanced constraint: provisioned_iops <= 256 × throughput_MiB_s.
_HYPERDISK_MAX_IOPS_PER_MBPS = 256

_BENCHMARK_NODEPOOL = 'benchmark'
_DEFAULT_NODEPOOL = 'default-pool'


def _valid_hyperdisk_throughput(iops: int, throughput: int) -> int:
    """Return a throughput (MiB/s) satisfying GCP Hyperdisk Balanced constraints.

    Clamps throughput UP to the minimum required by the requested IOPS so that
    a mismatched flag pair cannot abort nodepool / disk creation with:
      "Requested provisioned throughput is too low for the provisioned iops".
    """
    min_tput = -(-int(iops) // _HYPERDISK_MAX_IOPS_PER_MBPS)  # ceil(iops/256)
    if throughput < min_tput:
        logging.warning(
            '[swap_encryption] boot/swap disk throughput %d MiB/s is too low'
            ' for %d IOPS; clamping to minimum %d MiB/s',
            throughput,
            iops,
            min_tput,
        )
        return min_tput
    return throughput


class _GcpZonalResource:
    """Minimal resource shim for gcp_util.GcloudCommand on compute operations.

    gcp_util.GcloudCommand auto-injects --project and --zone from the resource
    object.  GkeCluster._GcloudCommand() switches --zone → --region for
    multi-zone clusters, which is wrong for gcloud compute commands (--region
    creates regional resources).  This shim pins a single zone so all
    gcloud compute calls target the correct AZ.
    """

    def __init__(self, project: str, zone: str) -> None:
        self.project = project
        self.zone = zone


class SwapNodePool(resource.BaseResource):
    """PKB resource for the swap-encryption benchmark GKE nodepool and disk.

    _Create() runs the full setup sequence:
      1. gcloud container node-pools create with linuxConfig.swapConfig.
      2. Wait for the node to become Ready.
      3. (Optional) Create and attach a dedicated swap disk.

    _Delete() tears down in reverse:
      1. (Optional) Detach and delete the swap disk.
      2. gcloud container node-pools delete.

    DeleteDefaultPool() is a separate step called from Prepare() AFTER the
    DaemonSet pod is Running, since deleting the default pool while the
    benchmark node is still joining can trigger a brief API-server timeout.

    Attributes:
      cluster: PKB GkeCluster (or subclass) object; provides _GcloudCommand,
        name, project, zones/region.
      machine_type: GKE machine type (e.g. 'n4-highmem-32').
      node_image_type: GKE image type (e.g. 'UBUNTU_CONTAINERD').
      disk_type: Boot disk type (e.g. 'hyperdisk-balanced' or 'pd-ssd').
      disk_size_gb: Boot disk size in GiB (500 for hyperdisk, 100 for LSSD).
      disk_iops: Provisioned IOPS (hyperdisk-balanced only).
      disk_throughput: Provisioned throughput MiB/s (hyperdisk-balanced only).
      lssd: True if the machine type uses local NVMe SSDs.  Auto-detected from
        machine_type name when False.
      lssd_count: Number of local NVMe SSDs (--local-nvme-ssd-block count=N).
      add_swap_disk: True to create+attach a dedicated second disk for swap.
      swap_disk_size_gb: Size of the dedicated swap disk in GiB.
    """

    RESOURCE_TYPE = 'SwapNodePool'
    REQUIRED_ATTRS = []

    def __init__(
        self,
        cluster,
        machine_type: str,
        node_image_type: str,
        disk_type: str,
        disk_size_gb: int,
        disk_iops: int,
        disk_throughput: int,
        lssd: bool,
        lssd_count: int,
        add_swap_disk: bool,
        swap_disk_size_gb: int,
    ) -> None:
        super().__init__()
        self.cluster = cluster
        self.machine_type = machine_type
        self.node_image_type = node_image_type
        self.disk_type = disk_type
        self.disk_size_gb = disk_size_gb
        self.disk_iops = disk_iops
        self.disk_throughput = disk_throughput
        # Auto-detect LSSD from machine type name; explicit flag overrides.
        self.lssd = lssd or 'lssd' in machine_type.lower()
        self.lssd_count = lssd_count
        self.add_swap_disk = add_swap_disk
        self.swap_disk_size_gb = swap_disk_size_gb

    # ── PKB lifecycle ─────────────────────────────────────────────────────────

    def _Create(self) -> None:
        """Create the benchmark nodepool, wait for node, optionally attach disk."""
        self._CreateNodePool()
        self._WaitForNode()
        if self.add_swap_disk:
            self._AttachDisk()

    def _Delete(self) -> None:
        """Detach+delete the swap disk (if any) then delete the nodepool."""
        if self.add_swap_disk:
            self._DetachAndDeleteDisk()
        self._DeleteNodePool()

    # ── Nodepool helpers ──────────────────────────────────────────────────────

    def _CreateNodePool(self) -> None:
        """gcloud container node-pools create with linuxConfig.swapConfig YAML.

        Per Ajay review comment r3472513706:
          linuxConfig.swapConfig automatically enables
          kubeletConfig.memorySwapBehavior=LimitedSwap — no need to set
          kubeletConfig explicitly.
          For LSSD machines, dedicatedLocalSsdProfile.diskCount instructs GKE
          to use local NVMe as the swap device.
        Per Ajay review comment r3472549985:
          UBUNTU_CONTAINERD is required for dm-crypt measurement.
        """
        is_lssd = self.lssd
        # LSSD configs use a small boot disk (OS only; swap is on local NVMe).
        disk_size_gb = 100 if is_lssd else self.disk_size_gb

        cmd = self.cluster._GcloudCommand(
            'container',
            'node-pools',
            'create',
            _BENCHMARK_NODEPOOL,
            '--cluster',
            self.cluster.name,
        )
        cmd.flags['machine-type'] = self.machine_type
        cmd.flags['image-type'] = self.node_image_type
        cmd.flags['disk-type'] = self.disk_type
        cmd.flags['disk-size'] = disk_size_gb
        cmd.flags['num-nodes'] = 1
        cmd.flags['node-labels'] = f'pkb_nodepool={_BENCHMARK_NODEPOOL}'
        cmd.args += ['--no-enable-autoupgrade', '--no-enable-autorepair']

        # IOPS / throughput only for hyperdisk non-LSSD configs.
        if self.disk_type.startswith('hyperdisk') and not is_lssd:
            cmd.flags['boot-disk-provisioned-iops'] = self.disk_iops
            cmd.flags['boot-disk-provisioned-throughput'] = (
                _valid_hyperdisk_throughput(self.disk_iops, self.disk_throughput)
            )

        # Expose local NVMe as raw block devices for fio/mdadm direct access.
        if is_lssd:
            cmd.flags['local-nvme-ssd-block'] = f'count={self.lssd_count}'

        # Build linuxConfig YAML for --system-config-from-file.
        if is_lssd:
            swap_config_block = (
                '  swapConfig:\n'
                '    enabled: true\n'
                '    dedicatedLocalSsdProfile:\n'
                f'      diskCount: {self.lssd_count}\n'
            )
        else:
            swap_config_block = '  swapConfig:\n    enabled: true\n'
        swap_config_yaml = (
            'linuxConfig:\n'
            + swap_config_block
            + '  sysctl:\n'
            '    vm.min_free_kbytes: 200\n'
            '    vm.watermark_scale_factor: 500\n'
            '    vm.swappiness: 100\n'
        )

        system_config_tmp = None
        try:
            system_config_tmp = tempfile.NamedTemporaryFile(
                mode='w', suffix='.yaml', delete=False
            )
            system_config_tmp.write(swap_config_yaml)
            system_config_tmp.flush()
            cmd.flags['system-config-from-file'] = system_config_tmp.name
            logging.info(
                '[swap_encryption] system-config-from-file: lssd=%s'
                ' (written to %s):\n%s',
                is_lssd,
                system_config_tmp.name,
                swap_config_yaml,
            )
            logging.info(
                '[swap_encryption] Creating benchmark nodepool: %s / %s /'
                ' image=%s / disk=%dGiB / iops=%d / lssd=%s /'
                ' add_swap_disk=%s',
                _BENCHMARK_NODEPOOL,
                self.machine_type,
                self.node_image_type,
                disk_size_gb,
                self.disk_iops,
                is_lssd,
                self.add_swap_disk,
            )
            # LSSD nodepools take longer to provision (NVMe init before Ready).
            _, stderr, rc = cmd.Issue(timeout=1200, raise_on_failure=False)
        finally:
            if system_config_tmp is not None:
                try:
                    os.unlink(system_config_tmp.name)
                except OSError:
                    pass

        if rc != 0:
            low = (stderr or '').lower()
            # Idempotent prepare: if the nodepool already exists (re-running
            # --run_stage=prepare,run), reuse it instead of failing.
            if (
                'already exists' in low
                or 'alreadyexists' in low
                or 'code=409' in low
            ):
                logging.info(
                    '[swap_encryption] Benchmark nodepool already exists —'
                    ' reusing (idempotent prepare)'
                )
                return
            raise errors.Benchmarks.RunError(
                f'[swap_encryption] Failed to create benchmark nodepool'
                f' (rc={rc}): {stderr}'
            )
        logging.info('[swap_encryption] Benchmark nodepool ready')

    def _WaitForNode(self, timeout: int = 900) -> None:
        """Block until a node labelled pkb_nodepool=benchmark is Ready.

        gcloud container node-pools create returns when the API accepts the
        request; the node VM may take another 2-4 min to boot and pass
        readiness checks.  Deploying the DaemonSet before the node is Ready
        leaves the pod Pending indefinitely.
        """
        deadline = time.time() + timeout
        logging.info(
            '[swap_encryption] Waiting for benchmark node'
            ' (pkb_nodepool=benchmark) to be Ready...'
        )
        while time.time() < deadline:
            out, _, rc = kubectl.RunKubectlCommand(
                [
                    'get',
                    'nodes',
                    '-l',
                    f'pkb_nodepool={_BENCHMARK_NODEPOOL}',
                    '-o',
                    (
                        r'jsonpath={range .items[*]}'
                        r'{.metadata.name}{"\t"}'
                        r'{range .status.conditions[?(@.type=="Ready")]}'
                        r'{.status}{"\n"}{end}{end}'
                    ),
                ],
                raise_on_failure=False,
            )
            if rc == 0 and out.strip():
                for line in out.strip().splitlines():
                    parts = line.split('\t')
                    if len(parts) == 2 and parts[1].strip() == 'True':
                        logging.info(
                            '[swap_encryption] Benchmark node ready: %s',
                            parts[0].strip(),
                        )
                        return
            logging.info(
                '[swap_encryption] Benchmark node not yet Ready —'
                ' retrying in 15 s...'
            )
            time.sleep(15)
        raise errors.Benchmarks.RunError(
            f'[swap_encryption] Timed out waiting for benchmark node'
            f' (pkb_nodepool={_BENCHMARK_NODEPOOL}) to become Ready'
            f' after {timeout}s'
        )

    # ── Dedicated swap disk helpers ───────────────────────────────────────────

    def _AttachDisk(self) -> None:
        """Create a dedicated hyperdisk and attach it to the benchmark node.

        gcloud container node-pools create --additional-node-disk is not
        available in all gcloud SDK versions, so we create the disk via
        gcloud compute and attach it after the node is Ready.  In GKE the
        Kubernetes node name equals the GCE instance name.

        The disk is named pkb-swap-<cluster-name> to avoid collisions across
        concurrent PKB runs.  _Delete() calls _DetachAndDeleteDisk() to clean
        up.
        """
        cluster = self.cluster
        zone = self._cluster_zone()
        if not zone:
            raise errors.Benchmarks.RunError(
                '[swap_encryption] Cannot attach swap disk: cluster zone unknown'
            )
        project = cluster.project
        disk_name = f'pkb-swap-{cluster.name}'

        # Get the GCE instance name from the benchmark node's Kubernetes name.
        node_out, _, rc = kubectl.RunKubectlCommand(
            [
                'get',
                'nodes',
                '-l',
                f'pkb_nodepool={_BENCHMARK_NODEPOOL}',
                '-o',
                'jsonpath={.items[0].metadata.name}',
            ],
            raise_on_failure=False,
        )
        instance_name = node_out.strip()
        if rc != 0 or not instance_name:
            raise errors.Benchmarks.RunError(
                '[swap_encryption] Cannot find benchmark node for swap disk'
                ' attach'
            )
        logging.info(
            '[swap_encryption] Benchmark node instance: %s', instance_name
        )

        # Create the disk.
        logging.info(
            '[swap_encryption] Creating swap disk %s (%dGiB %s)',
            disk_name,
            self.swap_disk_size_gb,
            self.disk_type,
        )
        gcp_res = _GcpZonalResource(project, zone)
        create_cmd = gcp_util.GcloudCommand(
            gcp_res, 'compute', 'disks', 'create', disk_name
        )
        create_cmd.flags['type'] = self.disk_type
        create_cmd.flags['size'] = f'{self.swap_disk_size_gb}GB'
        create_cmd.args.append('--quiet')
        if self.disk_type.startswith('hyperdisk'):
            create_cmd.flags['provisioned-iops'] = self.disk_iops
            create_cmd.flags['provisioned-throughput'] = (
                _valid_hyperdisk_throughput(self.disk_iops, self.disk_throughput)
            )
        _, stderr, rc = create_cmd.Issue(timeout=120, raise_on_failure=False)
        if rc != 0:
            raise errors.Benchmarks.RunError(
                f'[swap_encryption] Failed to create swap disk {disk_name}:'
                f' {stderr}'
            )

        # Attach the disk to the benchmark node VM.
        logging.info(
            '[swap_encryption] Attaching swap disk %s to %s',
            disk_name,
            instance_name,
        )
        attach_cmd = gcp_util.GcloudCommand(
            gcp_res, 'compute', 'instances', 'attach-disk', instance_name
        )
        attach_cmd.flags['disk'] = disk_name
        attach_cmd.flags['device-name'] = 'pkb-swap'
        attach_cmd.args.append('--quiet')
        _, stderr, rc = attach_cmd.Issue(timeout=120, raise_on_failure=False)
        if rc != 0:
            raise errors.Benchmarks.RunError(
                f'[swap_encryption] Failed to attach swap disk to'
                f' {instance_name}: {stderr}'
            )
        logging.info(
            '[swap_encryption] Swap disk attached: %s → %s',
            disk_name,
            instance_name,
        )

    def _DetachAndDeleteDisk(self) -> None:
        """Detach and delete the dedicated swap disk created by _AttachDisk."""
        zone = self._cluster_zone()
        cluster = self.cluster
        if not zone or not getattr(cluster, 'project', None):
            return
        disk_name = f'pkb-swap-{cluster.name}'
        self._DeleteDiskByName(disk_name, cluster.project, zone)

    def _DeleteDiskByName(
        self, disk_name: str, project: str, zone: str
    ) -> bool:
        """Detach (if attached) and delete a GCE disk, robustly, with retries.

        Finds the attached instance from the disk's own `users` field rather
        than kubectl — kubectl is often unavailable during teardown (cluster
        being deleted), which previously left the disk attached and
        undeletable.  Returns True if the disk is gone.
        """
        for attempt in range(1, 5):
            gcp_res = _GcpZonalResource(project, zone)
            describe_cmd = gcp_util.GcloudCommand(
                gcp_res, 'compute', 'disks', 'describe', disk_name
            )
            describe_cmd.flags['format'] = 'value(users)'
            users, _, rc = describe_cmd.Issue(timeout=60, raise_on_failure=False)
            if rc != 0:
                logging.info(
                    '[swap_encryption] Swap disk %s not present —'
                    ' nothing to delete',
                    disk_name,
                )
                return True  # Already gone.
            user = users.strip()
            if user:
                inst = user.split('/')[-1]
                logging.info(
                    '[swap_encryption] Detaching swap disk %s from %s',
                    disk_name,
                    inst,
                )
                detach_cmd = gcp_util.GcloudCommand(
                    gcp_res, 'compute', 'instances', 'detach-disk', inst
                )
                detach_cmd.flags['disk'] = disk_name
                detach_cmd.args.append('--quiet')
                detach_cmd.Issue(timeout=120, raise_on_failure=False)
            delete_cmd = gcp_util.GcloudCommand(
                gcp_res, 'compute', 'disks', 'delete', disk_name
            )
            delete_cmd.args.append('--quiet')
            _, derr, drc = delete_cmd.Issue(timeout=180, raise_on_failure=False)
            if drc == 0:
                logging.info(
                    '[swap_encryption] Swap disk deleted: %s', disk_name
                )
                return True
            logging.warning(
                '[swap_encryption] Swap disk delete attempt %d/4 failed'
                ' (%s); retrying in 10 s',
                attempt,
                derr.strip()[:160],
            )
            time.sleep(10)
        logging.error(
            '[swap_encryption] Could NOT delete swap disk %s after retries'
            ' — delete it manually:\n'
            '  gcloud compute disks delete %s --zone %s --quiet',
            disk_name,
            disk_name,
            zone,
        )
        return False

    def _DeleteNodePool(self) -> None:
        """Delete the benchmark nodepool."""
        cmd = self.cluster._GcloudCommand(
            'container',
            'node-pools',
            'delete',
            _BENCHMARK_NODEPOOL,
            '--cluster',
            self.cluster.name,
        )
        cmd.args.append('--quiet')
        logging.info(
            '[swap_encryption] Deleting benchmark nodepool: %s',
            _BENCHMARK_NODEPOOL,
        )
        _, stderr, rc = cmd.Issue(timeout=600, raise_on_failure=False)
        if rc != 0:
            logging.warning(
                '[swap_encryption] Could not delete benchmark nodepool'
                ' (rc=%d): %s',
                rc,
                stderr,
            )
        else:
            logging.info('[swap_encryption] Benchmark nodepool deleted')

    def DeleteDefaultPool(self) -> None:
        """Delete the dummy e2-medium default nodepool.

        Called from Prepare() AFTER the DaemonSet pod is Running.  The default
        pool (e2-medium) was only needed to satisfy GKE's requirement that a
        cluster must have at least one nodepool at creation time.  Removing it
        stops its cost immediately.

        Deleting the default pool BEFORE the DaemonSet pod is Running can
        trigger a brief API-server I/O timeout (control plane busy with two
        concurrent nodepool ops).  Calling this method from Prepare() after
        daemonset.WaitForPod() ensures the cluster is fully stable.
        """
        cmd = self.cluster._GcloudCommand(
            'container',
            'node-pools',
            'delete',
            _DEFAULT_NODEPOOL,
            '--cluster',
            self.cluster.name,
        )
        cmd.args.append('--quiet')
        logging.info(
            '[swap_encryption] Deleting default nodepool: %s', _DEFAULT_NODEPOOL
        )
        _, stderr, rc = cmd.Issue(timeout=300, raise_on_failure=False)
        if rc != 0:
            logging.warning(
                '[swap_encryption] Could not delete default nodepool'
                ' (rc=%d): %s',
                rc,
                stderr,
            )
        else:
            logging.info('[swap_encryption] Default nodepool deleted')

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _cluster_zone(self) -> str:
        """Return the first zone (or region) from the cluster object."""
        cluster = self.cluster
        if getattr(cluster, 'zones', None):
            return cluster.zones[0]
        if getattr(cluster, 'region', None):
            return cluster.region
        return ''
