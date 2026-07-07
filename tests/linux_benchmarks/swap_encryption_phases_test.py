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
"""Tests for swap_encryption_phases: ParseFioJson and RunPhase1Fio."""

import json
import unittest
from unittest import mock

from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import swap_encryption_phases as phases
from tests import pkb_common_test_case


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_META = {'swap_device': 'dm-0', 'node_type': 'n4-highmem-32'}


def _fio_json(
    rw='randread',
    iops=10000.0,
    bw_kbps=40960,
    lat_mean_ns=25000.0,
    p50_ns=24000,
    p99_ns=50000,
    p999_ns=75000,
) -> str:
  """Build minimal fio --output-format=json string for one job."""
  direction = 'read' if 'read' in rw else 'write'
  other = 'write' if direction == 'read' else 'read'
  job = {
      direction: {
          'iops': iops,
          'bw': bw_kbps,
          'clat_ns': {
              'mean': lat_mean_ns,
              'percentile': {
                  '50.000000': p50_ns,
                  '99.000000': p99_ns,
                  '99.900000': p999_ns,
              },
          },
      },
      other: {'iops': 0, 'bw': 0},
  }
  return json.dumps({'jobs': [job]})


# ---------------------------------------------------------------------------
# ParseFioJson tests
# ---------------------------------------------------------------------------


class ParseFioJsonTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for phases.ParseFioJson()."""

  def test_returns_iops_sample(self):
    out = _fio_json(rw='randread', iops=12345.0)
    samples = phases.ParseFioJson(out, '4k_randread', _BASE_META)
    metric_names = [s.metric for s in samples]
    self.assertIn('phase1_fio_4k_randread_read_iops', metric_names)

  def test_iops_value_correct(self):
    out = _fio_json(rw='randread', iops=12345.0)
    samples = phases.ParseFioJson(out, '4k_randread', _BASE_META)
    iops_sample = next(
        s for s in samples if s.metric.endswith('_iops')
    )
    self.assertAlmostEqual(iops_sample.value, 12345.0)

  def test_returns_bandwidth_sample_in_mbps(self):
    # bw=51200 KiB/s → 50.0 MB/s
    out = _fio_json(rw='randread', bw_kbps=51200)
    samples = phases.ParseFioJson(out, '4k_randread', _BASE_META)
    bw_sample = next(s for s in samples if '_bw_mbps' in s.metric)
    self.assertAlmostEqual(bw_sample.value, 50.0)
    self.assertEqual(bw_sample.unit, 'MB/s')

  def test_returns_latency_mean_in_microseconds(self):
    # lat_mean=25000 ns → 25.0 µs
    out = _fio_json(rw='randread', lat_mean_ns=25000.0)
    samples = phases.ParseFioJson(out, '4k_lat_read', _BASE_META)
    lat_sample = next(s for s in samples if '_lat_mean_us' in s.metric)
    self.assertAlmostEqual(lat_sample.value, 25.0)
    self.assertEqual(lat_sample.unit, 'us')

  def test_returns_latency_percentiles(self):
    out = _fio_json(rw='randread', p50_ns=24000, p99_ns=50000, p999_ns=75000)
    samples = phases.ParseFioJson(out, '4k_randread', _BASE_META)
    metric_names = [s.metric for s in samples]
    self.assertTrue(any('_lat_p50_us' in m for m in metric_names))
    self.assertTrue(any('_lat_p99_us' in m for m in metric_names))
    self.assertTrue(any('_lat_p999_us' in m for m in metric_names))

  def test_write_job_emits_write_direction_samples(self):
    out = _fio_json(rw='randwrite', iops=8000.0)
    samples = phases.ParseFioJson(out, '4k_randwrite', _BASE_META)
    metric_names = [s.metric for s in samples]
    self.assertTrue(any('_write_' in m for m in metric_names))
    self.assertFalse(any('_read_' in m for m in metric_names))

  def test_skips_zero_iops_direction(self):
    # 'write' direction has iops=0 and bw=0 → should be omitted
    out = _fio_json(rw='randread', iops=5000.0)
    samples = phases.ParseFioJson(out, '4k_randread', _BASE_META)
    metric_names = [s.metric for s in samples]
    self.assertFalse(any('_write_' in m for m in metric_names))

  def test_strips_preamble_before_json(self):
    preamble = 'kernel: some warning\nanother line\n'
    out = preamble + _fio_json(rw='randread', iops=1000.0)
    samples = phases.ParseFioJson(out, '4k_randread', _BASE_META)
    self.assertTrue(len(samples) > 0)

  def test_raises_run_error_when_no_json(self):
    with self.assertRaises(errors.Benchmarks.RunError):
      phases.ParseFioJson('no json here', '4k_randread', _BASE_META)

  def test_raises_run_error_on_invalid_json(self):
    with self.assertRaises(errors.Benchmarks.RunError):
      phases.ParseFioJson('{invalid json}', '4k_randread', _BASE_META)

  def test_returns_empty_list_when_jobs_empty(self):
    out = json.dumps({'jobs': []})
    samples = phases.ParseFioJson(out, '4k_randread', _BASE_META)
    self.assertEqual(samples, [])

  def test_metadata_propagated_to_samples(self):
    out = _fio_json(rw='randread', iops=1000.0)
    meta = {'swap_device': 'nvme1n1', 'cloud': 'aws'}
    samples = phases.ParseFioJson(out, '4k_randread', meta)
    for s in samples:
      self.assertEqual(s.metadata.get('cloud'), 'aws')
      self.assertEqual(s.metadata.get('swap_device'), 'nvme1n1')
      self.assertEqual(s.metadata.get('fio_job'), '4k_randread')

  def test_base_meta_not_mutated(self):
    out = _fio_json(rw='randread', iops=1000.0)
    meta = dict(_BASE_META)
    phases.ParseFioJson(out, '4k_randread', meta)
    self.assertNotIn('fio_job', meta)


# ---------------------------------------------------------------------------
# RunPhase1Fio tests
# ---------------------------------------------------------------------------


class RunPhase1FioTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for phases.RunPhase1Fio()."""

  def _make_ds(self, fio_output=None):
    """Return a mock daemonset whose PodExec returns fio JSON for fio calls."""
    ds = mock.Mock()
    if fio_output is None:
      fio_output = _fio_json(rw='randread', iops=5000.0)

    def _pod_exec(cmd, **_):
      if 'fio' in cmd:
        return (fio_output, '')
      return ('', '')

    ds.PodExec = mock.Mock(side_effect=_pod_exec)
    return ds

  def test_returns_list_of_samples(self):
    ds = self._make_ds()
    samples = phases.RunPhase1Fio(ds, '/dev/dm-0', _BASE_META, 30)
    self.assertIsInstance(samples, list)
    self.assertTrue(len(samples) > 0)

  def test_swapoff_called_before_fio(self):
    ds = self._make_ds()
    call_log = []

    def _track(cmd, **_):
      if 'swapoff' in cmd:
        call_log.append('swapoff')
      elif 'fio' in cmd:
        call_log.append('fio')
      return ('', '') if 'fio' not in cmd else (_fio_json(), '')

    ds.PodExec = mock.Mock(side_effect=_track)
    phases.RunPhase1Fio(ds, '/dev/dm-0', _BASE_META, 30)
    self.assertIn('swapoff', call_log)
    self.assertIn('fio', call_log)
    self.assertLess(call_log.index('swapoff'), call_log.index('fio'))

  def test_swapon_called_after_fio_jobs(self):
    ds = self._make_ds()
    call_log = []

    def _track(cmd, **_):
      if 'swapoff' in cmd:
        call_log.append('swapoff')
      elif 'fio' in cmd:
        call_log.append('fio')
        return (_fio_json(), '')
      elif 'swapon' in cmd:
        call_log.append('swapon')
      return ('', '')

    ds.PodExec = mock.Mock(side_effect=_track)
    phases.RunPhase1Fio(ds, '/dev/dm-0', _BASE_META, 30)
    self.assertIn('swapon', call_log)
    self.assertGreater(call_log.index('swapon'), call_log.index('fio'))

  def test_swapon_called_even_if_fio_raises(self):
    ds = mock.Mock()
    swapon_called = []

    def _pod_exec(cmd, **_):
      if 'swapoff' in cmd:
        return ('', '')
      if 'fio' in cmd:
        raise Exception('fio failed')
      if 'swapon' in cmd:
        swapon_called.append(True)
      return ('', '')

    ds.PodExec = mock.Mock(side_effect=_pod_exec)
    with self.assertRaises(Exception):
      phases.RunPhase1Fio(ds, '/dev/dm-0', _BASE_META, 30)
    self.assertTrue(swapon_called, 'swapon must be called in finally block')

  def test_runs_all_five_fio_jobs(self):
    ds = self._make_ds()
    fio_calls = []

    def _track(cmd, **_):
      if 'fio' in cmd:
        fio_calls.append(cmd)
        return (_fio_json(), '')
      return ('', '')

    ds.PodExec = mock.Mock(side_effect=_track)
    phases.RunPhase1Fio(ds, '/dev/dm-0', _BASE_META, 60)
    job_names = [
        '4k_randread', '4k_randwrite', '1m_seqread', '1m_seqwrite',
        '4k_lat_read',
    ]
    for name in job_names:
      self.assertTrue(
          any(name in c for c in fio_calls),
          f'Expected fio job {name!r} not found in calls',
      )


if __name__ == '__main__':
  unittest.main()
