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
"""Tests for swap_encryption phases helpers.

RunPhase1Fio gains swap_type parameter and loop-device skip logic.
ParseFioJson gains a label parameter and uses io_bytes for zero-direction
benchmark duration.
RunPhase3b measures kernel build time under memory-capped cgroup.
"""

import json
import unittest
from unittest import mock

from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks.kubernetes.swap_encryption import phases
from tests import pkb_common_test_case


def _fio_json(
    job_name: str = 'rand_read_iops',
    rw: str = 'randread',
    iops: float = 1000.0,
    bw_kib: float = 4096.0,
    lat_mean_ns: float = 500000.0,
    lat_p50_ns: float = 400000.0,
    lat_p99_ns: float = 900000.0,
    lat_p999_ns: float = 1500000.0,
    io_bytes: int = 1073741824,
) -> str:
  """Build a minimal fio JSON string suitable for ParseFioJson."""
  direction = 'read' if 'read' in rw else 'write'
  zero_dir = 'write' if direction == 'read' else 'read'
  data = {
      'jobs': [{
          'jobname': job_name,
          direction: {
              'iops': iops,
              'bw': bw_kib,
              'io_bytes': io_bytes,
              'clat_ns': {
                  'mean': lat_mean_ns,
                  'percentile': {
                      '50.000000': lat_p50_ns,
                      '99.000000': lat_p99_ns,
                      '99.900000': lat_p999_ns,
                  },
              },
          },
          zero_dir: {
              'iops': 0.0,
              'bw': 0.0,
              'io_bytes': 0,
              'clat_ns': {'mean': 0.0, 'percentile': {}},
          },
      }]
  }
  return json.dumps(data)


class ParseFioJsonTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for phases.ParseFioJson()."""

  _META = {'benchmark': 'swap_encryption'}

  def test_read_job_returns_six_samples(self):
    out = _fio_json('rand_read_iops', 'randread')
    samples = phases.ParseFioJson(out, 'rand_read_iops', self._META)
    self.assertLen(samples, 6)

  def test_write_job_returns_six_samples(self):
    out = _fio_json('rand_write_iops', 'randwrite', io_bytes=500000000)
    samples = phases.ParseFioJson(out, 'rand_write_iops', self._META)
    self.assertLen(samples, 6)

  def test_zero_io_bytes_direction_skipped(self):
    # randread → write direction has io_bytes=0, should be skipped.
    out = _fio_json('rand_read_iops', 'randread')
    samples = phases.ParseFioJson(out, 'rand_read_iops', self._META)
    metric_names = {s.metric for s in samples}
    self.assertFalse(
        any('write' in m for m in metric_names),
        'Write metrics should be absent for a pure read job',
    )

  def test_iops_value_correct(self):
    out = _fio_json('rand_read_iops', 'randread', iops=12345.0)
    samples = phases.ParseFioJson(out, 'rand_read_iops', self._META)
    iops_sample = next(s for s in samples if s.metric.endswith('_iops'))
    self.assertAlmostEqual(iops_sample.value, 12345.0)

  def test_bandwidth_converted_to_mbps(self):
    out = _fio_json('seq_read_bw', 'read', bw_kib=1048576.0)  # 1024 MiB/s
    samples = phases.ParseFioJson(out, 'seq_read_bw', self._META)
    bw_sample = next(s for s in samples if '_bw_mbps' in s.metric)
    self.assertAlmostEqual(bw_sample.value, 1024.0)

  def test_latency_converted_to_microseconds(self):
    out = _fio_json('lat_read', 'randread', lat_mean_ns=1000000.0)
    samples = phases.ParseFioJson(out, 'lat_read', self._META)
    lat_sample = next(s for s in samples if '_lat_mean' in s.metric)
    self.assertAlmostEqual(lat_sample.value, 1000.0)

  def test_label_stored_in_metadata(self):
    out = _fio_json('rand_read_iops', 'randread')
    samples = phases.ParseFioJson(
        out, 'rand_read_iops', self._META, label='Random read IOPS'
    )
    for s in samples:
      self.assertEqual(s.metadata.get('fio_label'), 'Random read IOPS')

  def test_label_default_empty_string(self):
    out = _fio_json('rand_read_iops', 'randread')
    samples = phases.ParseFioJson(out, 'rand_read_iops', self._META)
    for s in samples:
      self.assertEqual(s.metadata.get('fio_label'), '')

  def test_job_name_stored_in_metadata(self):
    out = _fio_json('seq_write_bw', 'write', io_bytes=500000000)
    samples = phases.ParseFioJson(out, 'seq_write_bw', self._META)
    for s in samples:
      self.assertEqual(s.metadata.get('fio_job'), 'seq_write_bw')

  def test_invalid_json_raises_run_error(self):
    with self.assertRaises(errors.Benchmarks.RunError):
      phases.ParseFioJson('not json at all', 'rand_read_iops', self._META)

  def test_empty_string_raises_run_error(self):
    with self.assertRaises(errors.Benchmarks.RunError):
      phases.ParseFioJson('', 'rand_read_iops', self._META)

  def test_missing_jobs_key_returns_empty(self):
    out = json.dumps({'global options': {}})
    result = phases.ParseFioJson(out, 'rand_read_iops', self._META)
    self.assertEmpty(result)

  def test_all_io_bytes_zero_returns_empty(self):
    data = {
        'jobs': [{
            'read': {'iops': 0, 'bw': 0, 'io_bytes': 0, 'clat_ns': {}},
            'write': {'iops': 0, 'bw': 0, 'io_bytes': 0, 'clat_ns': {}},
        }]
    }
    result = phases.ParseFioJson(json.dumps(data), 'empty_job', self._META)
    self.assertEmpty(result)

  def test_mixed_rw_job_returns_twelve_samples(self):
    """Mixed read/write fio job produces 12 samples (6 read + 6 write)."""
    data = {
        'jobs': [{
            'read': {
                'iops': 500.0,
                'bw': 2048.0,
                'io_bytes': 536870912,
                'clat_ns': {
                    'mean': 500000.0,
                    'percentile': {
                        '50.000000': 400000.0,
                        '99.000000': 900000.0,
                        '99.900000': 1500000.0,
                    },
                },
            },
            'write': {
                'iops': 500.0,
                'bw': 2048.0,
                'io_bytes': 536870912,
                'clat_ns': {
                    'mean': 500000.0,
                    'percentile': {
                        '50.000000': 400000.0,
                        '99.000000': 900000.0,
                        '99.900000': 1500000.0,
                    },
                },
            },
        }]
    }
    samples = phases.ParseFioJson(json.dumps(data), 'rand_rw_mixed', self._META)
    self.assertLen(samples, 12)


class RunPhase1FioTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for phases.RunPhase1Fio()."""

  def _make_daemonset(self, fio_output: str = '') -> mock.MagicMock:
    ds = mock.MagicMock()
    # Default: rm succeeds, fio run succeeds, cat returns valid JSON
    ds.PodExec.return_value = (fio_output, '')
    return ds

  def _make_ds_with_jobs(self) -> mock.MagicMock:
    """Daemonset returning valid fio JSON for cat, empty for everything else."""
    ds = mock.MagicMock()

    def _exec(cmd, **_):
      if cmd.startswith('cat '):
        # Return valid single-read-job JSON.
        return (
            _fio_json('rand_read_iops', 'randread'),
            '',
        )
      return ('', '')

    ds.PodExec.side_effect = _exec
    return ds

  def test_loop_device_non_boot_disk_returns_empty(self):
    ds = self._make_daemonset()
    result = phases.RunPhase1Fio(
        ds, '/dev/loop0', {}, 60, swap_type='hyperdisk'
    )
    self.assertEmpty(result)
    # Should not call PodExec at all (no prefill, no fio).
    ds.PodExec.assert_not_called()

  def test_loop_device_boot_disk_runs_fio(self):
    ds = self._make_ds_with_jobs()
    phases.RunPhase1Fio(ds, '/dev/loop0', {}, 60, swap_type='boot_disk')
    # boot_disk loop is intentional — fio should run.
    self.assertGreater(ds.PodExec.call_count, 0)

  def test_non_loop_device_runs_all_jobs(self):
    ds = self._make_ds_with_jobs()
    phases.RunPhase1Fio(
        ds, '/dev/mapper/swap_encrypted', {}, 60, swap_type='hyperdisk'
    )
    # swapoff + prefill + 7×(rm + fio + cat) + mkswap+swapon = 1+1+21+1 = 24
    self.assertGreater(ds.PodExec.call_count, 10)

  def test_connection_reset_raises_run_error(self):
    ds = mock.MagicMock()

    def _exec(cmd, **_):
      if cmd.startswith('fio '):
        return ('', 'connection reset by peer')
      return ('', '')

    ds.PodExec.side_effect = _exec
    with self.assertRaises(errors.Benchmarks.RunError):
      phases.RunPhase1Fio(
          ds, '/dev/mapper/swap_encrypted', {}, 60, swap_type='hyperdisk'
      )

  def test_mkswap_swapon_called_after_all_jobs(self):
    ds = self._make_ds_with_jobs()
    phases.RunPhase1Fio(ds, '/dev/nvme0n1', {}, 60, swap_type='lssd')
    calls = [str(c) for c in ds.PodExec.call_args_list]
    self.assertTrue(
        any('mkswap' in c and 'swapon' in c for c in calls),
        'mkswap+swapon should be called after all fio jobs',
    )

  def test_default_swap_type_empty_string_non_loop_runs(self):
    ds = self._make_ds_with_jobs()
    # swap_type defaults to '' — non-loop device should run normally.
    phases.RunPhase1Fio(ds, '/dev/sda', {}, 60)
    self.assertGreater(ds.PodExec.call_count, 0)


class RunPhase3bTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for phases.RunPhase3b() — kernel build under memory constraint."""

  _META = {'benchmark': 'swap_encryption', 'swap_device': '/dev/dm-0'}

  def _make_ds(self, cgroup_mode: str = 'CGROUPV1') -> mock.MagicMock:
    """Return a mock daemonset that simulates a successful kernel build."""
    ds = mock.MagicMock()

    def _pod_exec(cmd, **_):
      # cgroup probe: return mode token on last line
      if 'cgroup.controllers' in cmd or 'memory.limit_in_bytes' in cmd:
        return (cgroup_mode, '')
      return ('', '')

    ds.PodExec = mock.Mock(side_effect=_pod_exec)
    return ds

  def test_returns_three_samples(self):
    """Constrained, unconstrained, and slowdown_ratio samples emitted."""
    ds = self._make_ds()
    # Patch time so elapsed > 0 for slowdown_ratio to be emitted.
    with mock.patch(
        'time.time', side_effect=[0.0, 60.0, 60.0, 60.0, 90.0, 90.0, 90.0]
    ):
      samples = phases.RunPhase3b(
          ds, self._META, kernel_version='6.1.38', kernel_memory_mb=512
      )
    self.assertLen(samples, 3)
    metric_names = {s.metric for s in samples}
    self.assertIn('kernel_build_elapsed_sec', metric_names)
    self.assertIn('kernel_build_slowdown_ratio', metric_names)

  def test_constrained_sample_has_memory_limit_metadata(self):
    ds = self._make_ds()
    samples = phases.RunPhase3b(
        ds, self._META, kernel_version='6.1.38', kernel_memory_mb=512
    )
    constrained = next(
        s
        for s in samples
        if s.metric == 'kernel_build_elapsed_sec'
        and s.metadata.get('build_variant') == 'constrained'
    )
    self.assertEqual(constrained.metadata['memory_limit_mb'], 512)
    self.assertEqual(constrained.metadata['kernel_version'], '6.1.38')

  def test_unconstrained_sample_has_unconstrained_label(self):
    ds = self._make_ds()
    samples = phases.RunPhase3b(
        ds, self._META, kernel_version='6.1.38', kernel_memory_mb=512
    )
    unconstrained = next(
        s
        for s in samples
        if s.metric == 'kernel_build_elapsed_sec'
        and s.metadata.get('build_variant') == 'unconstrained'
    )
    self.assertEqual(unconstrained.metadata['memory_limit_mb'], 'unconstrained')

  def test_slowdown_ratio_value(self):
    """ratio = constrained_elapsed / unconstrained_elapsed."""
    ds = self._make_ds()
    # constrained: 0→60 (60s), unconstrained: 60→90 (30s) → ratio=2.0
    with mock.patch(
        'time.time', side_effect=[0.0, 60.0, 60.0, 60.0, 90.0, 90.0, 90.0]
    ):
      samples = phases.RunPhase3b(
          ds, self._META, kernel_version='6.1.38', kernel_memory_mb=512
      )
    ratio_sample = next(
        s for s in samples if s.metric == 'kernel_build_slowdown_ratio'
    )
    self.assertAlmostEqual(ratio_sample.value, 2.0)

  def test_cgroup_none_still_returns_samples(self):
    """Builds run even when cgroup setup fails; ratio sample still emitted."""
    ds = self._make_ds(cgroup_mode='CGROUP_NONE')
    with mock.patch(
        'time.time', side_effect=[0.0, 60.0, 60.0, 60.0, 90.0, 90.0, 90.0]
    ):
      samples = phases.RunPhase3b(
          ds, self._META, kernel_version='6.1.38', kernel_memory_mb=512
      )
    self.assertLen(samples, 3)
    constrained = next(
        s
        for s in samples
        if s.metric == 'kernel_build_elapsed_sec'
        and s.metadata.get('build_variant') == 'constrained'
    )
    self.assertEqual(constrained.metadata['cgroup_mode'], 'CGROUP_NONE')

  def test_zero_unconstrained_time_skips_ratio(self):
    """No ratio sample when unconstrained elapsed is 0 (avoid division)."""
    ds = self._make_ds()
    # Both builds return instantly (elapsed=0)
    with mock.patch('time.time', return_value=0.0):
      samples = phases.RunPhase3b(
          ds, self._META, kernel_version='6.1.38', kernel_memory_mb=512
      )
    metric_names = {s.metric for s in samples}
    self.assertNotIn('kernel_build_slowdown_ratio', metric_names)
    self.assertLen(samples, 2)  # constrained + unconstrained only

  def test_explicit_kernel_version_stored_in_metadata(self):
    """kernel_version arg is recorded in all sample metadata."""
    ds = self._make_ds()
    samples = phases.RunPhase3b(
        ds, self._META, kernel_version='5.15.120', kernel_memory_mb=256
    )
    for s in samples:
      self.assertEqual(s.metadata.get('kernel_version'), '5.15.120')


# ---------------------------------------------------------------------------
# Helper tests
# ---------------------------------------------------------------------------


class ParseVmBytesToMbTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for phases._parse_vm_bytes_to_mb()."""

  def test_gigabytes_suffix(self):
    self.assertAlmostEqual(phases._parse_vm_bytes_to_mb('4G'), 4096.0)

  def test_megabytes_suffix(self):
    self.assertAlmostEqual(phases._parse_vm_bytes_to_mb('512M'), 512.0)

  def test_kilobytes_suffix(self):
    self.assertAlmostEqual(phases._parse_vm_bytes_to_mb('2048K'), 2.0)

  def test_terabytes_suffix(self):
    self.assertAlmostEqual(phases._parse_vm_bytes_to_mb('1T'), 1024.0 * 1024.0)

  def test_lowercase_suffix(self):
    self.assertAlmostEqual(phases._parse_vm_bytes_to_mb('2g'), 2048.0)

  def test_empty_string_returns_zero(self):
    self.assertAlmostEqual(phases._parse_vm_bytes_to_mb(''), 0.0)

  def test_invalid_string_returns_zero(self):
    self.assertAlmostEqual(phases._parse_vm_bytes_to_mb('invalid'), 0.0)

  def test_whitespace_stripped(self):
    self.assertAlmostEqual(phases._parse_vm_bytes_to_mb('  2G  '), 2048.0)

  def test_bare_bytes_converted_to_mb(self):
    # 1048576 bytes == 1 MB
    result = phases._parse_vm_bytes_to_mb('1048576')
    self.assertAlmostEqual(result, 1.0)


class PerWorkerVmBytesTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for phases._per_worker_vm_bytes()."""

  def test_splits_evenly(self):
    # 4096 MB / 4 workers = 1024 MB
    self.assertEqual(phases._per_worker_vm_bytes('4096M', 4), '1024M')

  def test_gigabyte_input_splits(self):
    # 2G = 2048 MB / 2 workers = 1024 MB
    self.assertEqual(phases._per_worker_vm_bytes('2G', 2), '1024M')

  def test_single_worker_returns_full(self):
    self.assertEqual(phases._per_worker_vm_bytes('2G', 1), '2048M')

  def test_zero_workers_treated_as_one(self):
    # workers clamped to max(1, 0) = 1
    self.assertEqual(phases._per_worker_vm_bytes('1G', 0), '1024M')

  def test_negative_workers_treated_as_one(self):
    self.assertEqual(phases._per_worker_vm_bytes('512M', -5), '512M')

  def test_zero_total_returns_original(self):
    # _parse_vm_bytes_to_mb('0M') = 0 → returns as-is
    self.assertEqual(phases._per_worker_vm_bytes('0M', 4), '0M')

  def test_minimum_one_mb_per_worker(self):
    # 1M / 4 workers = 0.25 → clamped to max(1, 0) = 1M
    self.assertEqual(phases._per_worker_vm_bytes('1M', 4), '1M')


class ParseVmstatTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for phases._parse_vmstat()."""

  _META = {'benchmark': 'swap_encryption'}

  # vmstat column layout (0-indexed):
  # r b swpd free buff cache si so bi bo in cs us sy id wa st
  _SINGLE_LINE = (
      'procs -----------memory---------- ---swap-- -----io---- -system-- cpu\n'
      ' r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs'
      ' us sy id  wa st\n'
      ' 0  0  10240 123456  12345 456789  100  200   50   100  200  400'
      ' 10  5  80   5  0\n'
  )

  def test_single_data_line_returns_ten_samples(self):
    samples = phases._parse_vmstat(self._SINGLE_LINE, self._META)
    self.assertLen(samples, 10)

  def test_empty_output_returns_empty(self):
    self.assertEmpty(phases._parse_vmstat('', self._META))

  def test_header_only_returns_empty(self):
    header = ' r  b   swpd   free   buff  cache   si   so    bi    bo\n'
    self.assertEmpty(phases._parse_vmstat(header, self._META))

  def test_swap_in_mean_value(self):
    samples = phases._parse_vmstat(self._SINGLE_LINE, self._META)
    si = next(s for s in samples if s.metric == 'swap_in_pages_per_sec')
    self.assertAlmostEqual(si.value, 100.0)

  def test_swap_out_mean_value(self):
    samples = phases._parse_vmstat(self._SINGLE_LINE, self._META)
    so = next(s for s in samples if s.metric == 'swap_out_pages_per_sec')
    self.assertAlmostEqual(so.value, 200.0)

  def test_peak_computed_over_multiple_lines(self):
    # Two data lines: si=100 and si=200 → max=200
    two_lines = (
        ' 0  0  10240 123456  12345 456789  100  200   50   100  200  400'
        ' 10  5  80   5  0\n'
        ' 0  0  10240 123456  12345 456789  200  400   50   100  200  400'
        ' 10  5  80   5  0\n'
    )
    samples = phases._parse_vmstat(two_lines, self._META)
    si_max = next(s for s in samples if s.metric == 'swap_in_pages_per_sec_max')
    self.assertAlmostEqual(si_max.value, 200.0)

  def test_mean_computed_over_multiple_lines(self):
    two_lines = (
        ' 0  0  10240 123456  12345 456789  100  200   50   100  200  400'
        ' 10  5  80   5  0\n'
        ' 0  0  10240 123456  12345 456789  200  400   50   100  200  400'
        ' 10  5  80   5  0\n'
    )
    samples = phases._parse_vmstat(two_lines, self._META)
    si_avg = next(s for s in samples if s.metric == 'swap_in_pages_per_sec')
    self.assertAlmostEqual(si_avg.value, 150.0)

  def test_metric_source_metadata(self):
    samples = phases._parse_vmstat(self._SINGLE_LINE, self._META)
    for s in samples:
      self.assertEqual(s.metadata.get('metric_source'), 'vmstat')

  def test_total_cpu_is_sum_of_us_sy_wa(self):
    # us=10, sy=5, wa=5 → total=20
    samples = phases._parse_vmstat(self._SINGLE_LINE, self._META)
    total = next(s for s in samples if s.metric == 'total_cpu_pct_avg')
    self.assertAlmostEqual(total.value, 20.0)


class ParsePidstatTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for phases._parse_pidstat()."""

  _META = {'benchmark': 'swap_encryption'}

  # pidstat output (10 cols, 0-indexed):
  # Timestamp UID PID %usr %system %guest %wait %CPU CPU Command
  # parts[7] = %CPU, parts[-1] = Command
  _KCRYPTD_LINE = (
      '15:32:01      0    1234    0.00    4.00    0.00    0.00    4.00'
      '     0  kcryptd\n'
  )
  _KSWAPD_LINE = (
      '15:32:01      0    1235    0.00    2.00    0.00    0.00    2.00'
      '     0  kswapd0\n'
  )
  _UNKNOWN_LINE = (
      '15:32:01      0    9999    0.00    1.00    0.00    0.00    1.00'
      '     0  bash\n'
  )

  def test_matching_process_returns_two_samples(self):
    samples = phases._parse_pidstat(self._KCRYPTD_LINE, self._META)
    self.assertLen(samples, 2)

  def test_unknown_process_returns_empty(self):
    self.assertEmpty(phases._parse_pidstat(self._UNKNOWN_LINE, self._META))

  def test_empty_output_returns_empty(self):
    self.assertEmpty(phases._parse_pidstat('', self._META))

  def test_cpu_avg_metric_name(self):
    samples = phases._parse_pidstat(self._KCRYPTD_LINE, self._META)
    names = {s.metric for s in samples}
    self.assertIn('cpu_pct_avg_kcryptd', names)

  def test_cpu_max_metric_name(self):
    samples = phases._parse_pidstat(self._KCRYPTD_LINE, self._META)
    names = {s.metric for s in samples}
    self.assertIn('cpu_pct_max_kcryptd', names)

  def test_cpu_value_correct(self):
    samples = phases._parse_pidstat(self._KCRYPTD_LINE, self._META)
    avg = next(s for s in samples if 'avg' in s.metric)
    self.assertAlmostEqual(avg.value, 4.0)

  def test_multiple_processes_returns_four_samples(self):
    both = self._KCRYPTD_LINE + self._KSWAPD_LINE
    samples = phases._parse_pidstat(both, self._META)
    self.assertLen(samples, 4)

  def test_metric_source_metadata(self):
    samples = phases._parse_pidstat(self._KCRYPTD_LINE, self._META)
    for s in samples:
      self.assertEqual(s.metadata.get('metric_source'), 'pidstat')

  def test_short_line_skipped(self):
    short = '15:32:01 0 1234 kcryptd\n'
    self.assertEmpty(phases._parse_pidstat(short, self._META))


class CgroupSwapLimitMbTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for phases._cgroup_swap_limit_mb()."""

  def _make_ds(self, output: str) -> mock.MagicMock:
    ds = mock.MagicMock()
    ds.PodExec.return_value = (output, '')
    return ds

  def test_v2_max_returns_infinity(self):
    ds = self._make_ds('V2=max')
    self.assertEqual(phases._cgroup_swap_limit_mb(ds), float('inf'))

  def test_v2_numeric_converted_to_mb(self):
    # 1073741824 bytes = 1024 MB
    ds = self._make_ds('V2=1073741824')
    self.assertAlmostEqual(phases._cgroup_swap_limit_mb(ds), 1024.0)

  def test_v2_zero_returns_zero(self):
    ds = self._make_ds('V2=0')
    self.assertAlmostEqual(phases._cgroup_swap_limit_mb(ds), 0.0)

  def test_v1_memsw_minus_mem(self):
    # memsw=2147483648 (2GB), mem=1073741824 (1GB) → swap=1GB=1024MB
    ds = self._make_ds('MEMSW=2147483648 MEM=1073741824')
    self.assertAlmostEqual(phases._cgroup_swap_limit_mb(ds), 1024.0)

  def test_v1_uncapped_returns_infinity(self):
    # memsw >= 1<<62 → uncapped
    ds = self._make_ds(f'MEMSW={1 << 62} MEM=1073741824')
    self.assertEqual(phases._cgroup_swap_limit_mb(ds), float('inf'))

  def test_empty_output_returns_minus_one(self):
    ds = self._make_ds('')
    self.assertAlmostEqual(phases._cgroup_swap_limit_mb(ds), -1.0)

  def test_pod_exec_exception_returns_minus_one(self):
    ds = mock.MagicMock()
    ds.PodExec.side_effect = RuntimeError('pod gone')
    self.assertAlmostEqual(phases._cgroup_swap_limit_mb(ds), -1.0)

  def test_v2_invalid_value_returns_minus_one(self):
    ds = self._make_ds('V2=notanumber')
    self.assertAlmostEqual(phases._cgroup_swap_limit_mb(ds), -1.0)


class AutoscaleVmBytesTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for phases._autoscale_vm_bytes()."""

  # /proc/meminfo with 32 GB RAM and 64 GB swap
  _MEMINFO_32G_64G = (
      'MemTotal:       33554432 kB\n'  # 32 GB
      'MemFree:        10000000 kB\n'
      'SwapTotal:      67108864 kB\n'  # 64 GB
      'SwapFree:       67108864 kB\n'
  )

  def _make_ds(
      self, meminfo: str = _MEMINFO_32G_64G, cgroup_out: str = 'V2=max'
  ) -> mock.MagicMock:
    ds = mock.MagicMock()

    def _exec(cmd, **_):
      if 'meminfo' in cmd:
        return (meminfo, '')
      return (cgroup_out, '')

    ds.PodExec.side_effect = _exec
    return ds

  def test_scales_up_small_request(self):
    # Requested 1G, RAM=32G → should scale up (1G < 0.95 × 32G)
    ds = self._make_ds()
    result = phases._autoscale_vm_bytes(ds, '1G', [])
    self.assertNotEqual(result, '1G')
    # Result should be in 'NNg' form, larger than 1G
    self.assertTrue(result.endswith('G'), f'Expected G suffix, got {result}')
    self.assertGreater(int(result[:-1]), 1)

  def test_caps_oversized_request(self):
    # RAM=32G, swap=64G → ceiling = 32 + 64 - 4 = 92 GB
    # Request 200G > ceiling → should be capped
    ds = self._make_ds()
    result = phases._autoscale_vm_bytes(ds, '200G', [])
    self.assertTrue(result.endswith('G'))
    self.assertLess(int(result[:-1]), 200)

  def test_locked_cgroup_caps_to_ram(self):
    # V2=0 → cgroup swap locked → result capped to ~0.9 × RAM
    ds = self._make_ds(cgroup_out='V2=0')
    reasons = []
    result = phases._autoscale_vm_bytes(ds, '100G', reasons)
    self.assertLen(reasons, 1)
    self.assertIn('cgroup swap is locked', reasons[0])
    self.assertTrue(result.endswith('G'))

  def test_pod_exec_exception_returns_original(self):
    ds = mock.MagicMock()
    ds.PodExec.side_effect = RuntimeError('network error')
    result = phases._autoscale_vm_bytes(ds, '4G', [])
    self.assertEqual(result, '4G')

  def test_missing_memtotal_returns_original(self):
    # meminfo without MemTotal line
    ds = self._make_ds(meminfo='SwapTotal: 67108864 kB\n')
    result = phases._autoscale_vm_bytes(ds, '4G', [])
    self.assertEqual(result, '4G')

  def test_adequate_request_unchanged(self):
    # Requested = 40G = 40960 MB; RAM=32G=32768MB; 40960 > 0.95×32768=31130
    # ceiling = 32768 + 65536 - 4096 = 94208 MB = 92G; 40960 < 94208 → no cap
    ds = self._make_ds()
    result = phases._autoscale_vm_bytes(ds, '40G', [])
    self.assertEqual(result, '40G')


if __name__ == '__main__':
  unittest.main()
