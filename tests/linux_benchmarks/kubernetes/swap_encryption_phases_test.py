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
"""Tests for swap_encryption_phases helpers (PR3).

PR3 update: RunPhase1Fio gains swap_type parameter and loop-device skip logic.
ParseFioJson gains a label parameter and uses io_bytes for zero-direction
detection.
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
    """Daemonset that returns valid fio JSON for cat, empty for everything else."""
    ds = mock.MagicMock()

    def _exec(cmd, **unused_kwargs):
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
    phases.RunPhase1Fio(
        ds, '/dev/loop0', {}, 60, swap_type='boot_disk'
    )
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

    def _exec(cmd, **unused_kwargs):
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


if __name__ == '__main__':
  unittest.main()
