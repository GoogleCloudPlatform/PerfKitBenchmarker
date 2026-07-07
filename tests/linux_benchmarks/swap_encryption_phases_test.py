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
"""Tests for swap_encryption phases helpers (PR2)."""

import json
import unittest
from unittest import mock

from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks.swap_encryption import phases
from tests import pkb_common_test_case


def _fio_json(
    job_name='4k_randread',
    rw='randread',
    iops=1000.0,
    bw_kbps=4096.0,
    lat_mean_ns=500000.0,
    lat_p50_ns=400000.0,
    lat_p99_ns=900000.0,
    lat_p999_ns=1500000.0,
):
  """Build a minimal fio JSON string suitable for ParseFioJson."""
  direction = 'read' if 'read' in rw else 'write'
  zero_dir = 'write' if direction == 'read' else 'read'
  data = {
      'jobs': [{
          'jobname': job_name,
          direction: {
              'iops': iops,
              'bw': bw_kbps,
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
              'clat_ns': {'mean': 0.0, 'percentile': {}},
          },
      }]
  }
  return json.dumps(data)


class ParseFioJsonTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for phases.ParseFioJson()."""

  _META = {'benchmark': 'swap_encryption'}

  def test_read_job_produces_iops_sample(self):
    out = _fio_json('4k_randread', 'randread', iops=5000.0)
    samples = phases.ParseFioJson(out, '4k_randread', self._META)
    metrics = {s.metric for s in samples}
    self.assertTrue(
        any('iops' in m.lower() for m in metrics),
        f'No IOPS sample found. Got: {metrics}',
    )

  def test_iops_value_correct(self):
    out = _fio_json('4k_randread', 'randread', iops=12345.0)
    samples = phases.ParseFioJson(out, '4k_randread', self._META)
    iops_sample = next(s for s in samples if 'iops' in s.metric.lower())
    self.assertAlmostEqual(iops_sample.value, 12345.0)

  def test_bandwidth_converted_to_mbps(self):
    out = _fio_json('1m_seqread', 'read', bw_kbps=1048576.0)
    samples = phases.ParseFioJson(out, '1m_seqread', self._META)
    bw_sample = next(s for s in samples if 'bw_mbps' in s.metric)
    self.assertAlmostEqual(bw_sample.value, 1024.0)

  def test_latency_mean_in_microseconds(self):
    out = _fio_json('4k_randread', 'randread', lat_mean_ns=1_000_000.0)
    samples = phases.ParseFioJson(out, '4k_randread', self._META)
    lat_sample = next(s for s in samples if 'lat_mean' in s.metric)
    self.assertAlmostEqual(lat_sample.value, 1000.0)

  def test_write_only_job_no_read_metrics(self):
    out = _fio_json('4k_randwrite', 'randwrite')
    samples = phases.ParseFioJson(out, '4k_randwrite', self._META)
    metrics = {s.metric for s in samples}
    self.assertFalse(
        any('read' in m for m in metrics),
        'Read metrics should be absent for a pure write job',
    )

  def test_job_name_in_metadata(self):
    out = _fio_json('4k_randread', 'randread')
    samples = phases.ParseFioJson(out, '4k_randread', self._META)
    for s in samples:
      self.assertEqual(s.metadata.get('fio_job'), '4k_randread')

  def test_invalid_json_raises_run_error(self):
    with self.assertRaises(errors.Benchmarks.RunError):
      phases.ParseFioJson('not json', '4k_randread', self._META)

  def test_empty_string_raises_run_error(self):
    with self.assertRaises(errors.Benchmarks.RunError):
      phases.ParseFioJson('', '4k_randread', self._META)

  def test_no_jobs_key_returns_empty(self):
    out = json.dumps({'global options': {}})
    result = phases.ParseFioJson(out, '4k_randread', self._META)
    self.assertEmpty(result)

  def test_base_meta_copied_into_samples(self):
    meta = {'benchmark': 'swap_encryption', 'cloud': 'gcp'}
    out = _fio_json('4k_randread', 'randread')
    samples = phases.ParseFioJson(out, '4k_randread', meta)
    for s in samples:
      self.assertEqual(s.metadata.get('cloud'), 'gcp')


class RunPhase1FioTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for phases.RunPhase1Fio()."""

  def _make_ds(self, fio_out=''):
    ds = mock.MagicMock()
    ds.PodExec.return_value = (fio_out, '')
    return ds

  def _make_ds_with_json(self):
    """Daemonset that returns valid fio JSON output."""
    ds = mock.MagicMock()
    ds.PodExec.return_value = (_fio_json('4k_randread', 'randread'), '')
    return ds

  def test_swapoff_called_before_fio(self):
    ds = self._make_ds_with_json()
    phases.RunPhase1Fio(ds, '/dev/mapper/swap_encrypted', {}, 60)
    calls = [str(c) for c in ds.PodExec.call_args_list]
    swapoff_idx = next(
        (i for i, c in enumerate(calls) if 'swapoff' in c), None
    )
    fio_idx = next((i for i, c in enumerate(calls) if 'fio ' in c), None)
    self.assertIsNotNone(swapoff_idx, 'swapoff not called')
    self.assertIsNotNone(fio_idx, 'fio not called')
    self.assertLess(swapoff_idx, fio_idx, 'swapoff must precede fio')

  def test_swapon_called_after_fio(self):
    ds = self._make_ds_with_json()
    phases.RunPhase1Fio(ds, '/dev/mapper/swap_encrypted', {}, 60)
    calls = [str(c) for c in ds.PodExec.call_args_list]
    fio_idx = next((i for i, c in enumerate(calls) if 'fio ' in c), None)
    swapon_idx = next(
        (i for i, c in enumerate(calls) if 'swapon' in c), None
    )
    self.assertIsNotNone(swapon_idx, 'swapon not called')
    self.assertGreater(swapon_idx, fio_idx, 'swapon must follow fio')

  def test_swapon_called_even_on_parse_error(self):
    """swapon must run in finally block even when fio output is invalid."""
    ds = mock.MagicMock()
    ds.PodExec.side_effect = [
        ('', ''),         # swapoff
        ('bad json', ''), # fio → triggers ParseFioJson error
        ('', ''),         # swapon (finally)
    ]
    with self.assertRaises(Exception):
      phases.RunPhase1Fio(ds, '/dev/mapper/swap_encrypted', {}, 60)
    calls = [str(c) for c in ds.PodExec.call_args_list]
    self.assertTrue(any('swapon' in c for c in calls), 'swapon must be called')

  def test_returns_samples_list(self):
    ds = self._make_ds_with_json()
    result = phases.RunPhase1Fio(ds, '/dev/mapper/swap_encrypted', {}, 60)
    self.assertIsInstance(result, list)

  def test_fio_runtime_used_in_command(self):
    ds = self._make_ds_with_json()
    phases.RunPhase1Fio(ds, '/dev/mapper/swap_encrypted', {}, 120)
    calls = [str(c) for c in ds.PodExec.call_args_list]
    fio_calls = [c for c in calls if 'fio ' in c]
    self.assertTrue(
        any('120' in c for c in fio_calls),
        'fio_runtime_sec=120 must appear in fio command',
    )


if __name__ == '__main__':
  unittest.main()
