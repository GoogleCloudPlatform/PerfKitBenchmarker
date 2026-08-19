# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Unit tests for swap_encryption_benchmark.

These cover the pure-Python parsing / cost / cloud-detection logic that runs
without a live cluster.  Anything that talks to kubectl is exercised by mocking
the module-level ``_PodExec`` helper, so the suite is fully CI-runnable with no
cloud credentials or cluster.

Run:  python -m pytest tests/linux_benchmarks/swap_encryption_benchmark_test.py
"""

import json
import unittest
from unittest import mock

from absl.testing import flagsaver
from perfkitbenchmarker.linux_benchmarks import swap_encryption_benchmark
from tests import pkb_common_test_case

# Convenience handle to the module under test.
_BM = swap_encryption_benchmark


def _by_metric(samples):
  """Return {metric_name: sample} for easy lookup in assertions."""
  return {s.metric: s for s in samples}


class ParseFioJsonTest(pkb_common_test_case.PkbCommonTestCase):
  """_ParseFioJson: fio --output-format=json -> Samples."""

  def _fio_json(self, read_io=1000, write_io=0):
    return json.dumps({
        "jobs": [{
            "read": {
                "io_bytes": read_io,
                "iops": 325000.0,
                "bw": 2800000,  # KiB/s
                "clat_ns": {
                    "mean": 24000.0,
                    "percentile": {
                        "50.000000": 21000.0,
                        "99.000000": 52000.0,
                        "99.900000": 120000.0,
                    },
                },
            },
            "write": {"io_bytes": write_io},
        }]
    })

  def testEmitsAllReadMetrics(self):
    samples = _BM._ParseFioJson(
        self._fio_json(), "swap_fio", "Swap fio", {"cloud": "aws"}
    )
    by = _by_metric(samples)
    self.assertIn("swap_fio_read_iops", by)
    self.assertAlmostEqual(by["swap_fio_read_iops"].value, 325000.0)
    self.assertEqual(by["swap_fio_read_iops"].unit, "iops")
    # bw is reported in KiB/s by fio and converted to MB/s (KiB/1024).
    self.assertAlmostEqual(
        by["swap_fio_read_bw_mbps"].value, 2734.375, places=2
    )
    # clat is ns -> converted to us (/1000).
    self.assertAlmostEqual(by["swap_fio_read_lat_mean"].value, 24.0)
    self.assertAlmostEqual(by["swap_fio_read_lat_p50"].value, 21.0)
    self.assertAlmostEqual(by["swap_fio_read_lat_p99"].value, 52.0)
    self.assertAlmostEqual(by["swap_fio_read_lat_p999"].value, 120.0)

  def testZeroIoDirectionSkipped(self):
    # write has io_bytes==0 -> no write samples emitted.
    samples = _BM._ParseFioJson(
        self._fio_json(write_io=0), "swap_fio", "lbl", {}
    )
    self.assertFalse(
        any(s.metric.startswith("swap_fio_write") for s in samples)
    )

  def testMetadataPropagated(self):
    samples = _BM._ParseFioJson(self._fio_json(), "j", "l", {"cloud": "aws"})
    self.assertEqual(samples[0].metadata["cloud"], "aws")
    self.assertEqual(samples[0].metadata["fio_job"], "j")
    self.assertEqual(samples[0].metadata["direction"], "read")

  def testMalformedJsonReturnsEmpty(self):
    self.assertEqual(_BM._ParseFioJson("not json{{", "j", "l", {}), [])

  def testEmptyJobsReturnsEmpty(self):
    self.assertEqual(_BM._ParseFioJson('{"jobs": []}', "j", "l", {}), [])


class ParseMemtierJsonTest(pkb_common_test_case.PkbCommonTestCase):
  """_ParseMemtierJson: dual-schema (modern + legacy) memtier parsing."""

  def _patch_podexec(self, raw):
    """Make _PodExec (which cats the json file) return ``raw``."""
    p = mock.patch.object(_BM, "_PodExec", return_value=(raw, ""))
    self.addCleanup(p.stop)
    return p.start()

  def testModernSchema(self):
    """Source-built memtier: flat 'Average Latency' + 'Percentile Latencies'."""
    raw = json.dumps({
        "ALL STATS": {
            "Totals": {
                "Ops/sec": 172030.18,
                "Average Latency": 1.16236,
                "Percentile Latencies": {
                    "p50.00": 1.015,
                    "p90.00": 1.5,
                    "p99.00": 2.287,
                    "p99.90": 28.799,
                },
            }
        }
    })
    self._patch_podexec(raw)
    by = _by_metric(_BM._ParseMemtierJson("/tmp/x.json", "pod", {}))
    self.assertAlmostEqual(by["redis_total_ops_per_sec"].value, 172030.18)
    self.assertAlmostEqual(by["redis_total_lat_avg_ms"].value, 1.16236)
    self.assertAlmostEqual(by["redis_total_lat_p50_ms"].value, 1.015)
    self.assertAlmostEqual(by["redis_total_lat_p90_ms"].value, 1.5)
    self.assertAlmostEqual(by["redis_total_lat_p99_ms"].value, 2.287)
    self.assertAlmostEqual(by["redis_total_lat_p999_ms"].value, 28.799)

  def testLegacySchema(self):
    """Older memtier: nested 'Latency' sub-dict with verbose percentile keys."""
    raw = json.dumps({
        "ALL STATS": {
            "Sets": {
                "Ops/sec": 90000.0,
                "Latency": {
                    "Average Latency": 0.9,
                    "50th Percentile Latency": 0.8,
                    "90th Percentile Latency": 1.1,
                    "99th Percentile Latency": 1.9,
                    "99.9th Percentile Latency": 9.0,
                },
            }
        }
    })
    self._patch_podexec(raw)
    by = _by_metric(_BM._ParseMemtierJson("/tmp/x.json", "pod", {}))
    self.assertAlmostEqual(by["redis_set_ops_per_sec"].value, 90000.0)
    self.assertAlmostEqual(by["redis_set_lat_avg_ms"].value, 0.9)
    self.assertAlmostEqual(by["redis_set_lat_p50_ms"].value, 0.8)
    self.assertAlmostEqual(by["redis_set_lat_p99_ms"].value, 1.9)
    self.assertAlmostEqual(by["redis_set_lat_p999_ms"].value, 9.0)

  def testLatencyScalarDoesNotCrash(self):
    """Regression: modern memtier emits a scalar 'Latency' (float).

    The old code did section['Latency'].get(...) -> AttributeError on a float.
    Parser must treat a non-dict Latency as absent and fall back to percentiles.
    """
    raw = json.dumps({
        "ALL STATS": {
            "Gets": {
                "Ops/sec": 5.0,
                "Latency": 1.23,  # scalar, not a dict
                "Average Latency": 1.23,
                "Percentile Latencies": {"p50.00": 1.0, "p99.00": 2.0},
            }
        }
    })
    self._patch_podexec(raw)
    by = _by_metric(_BM._ParseMemtierJson("/tmp/x.json", "pod", {}))
    self.assertAlmostEqual(by["redis_get_lat_p50_ms"].value, 1.0)
    self.assertAlmostEqual(by["redis_get_lat_p99_ms"].value, 2.0)
    # Missing p90 in the dict -> defaults to 0, must not raise.
    self.assertAlmostEqual(by["redis_get_lat_p90_ms"].value, 0.0)

  def testMalformedJsonReturnsEmpty(self):
    self._patch_podexec("")  # empty cat -> json.loads('') fails
    self.assertEqual(_BM._ParseMemtierJson("/tmp/x.json", "pod", {}), [])

  def testNonDictSectionSkipped(self):
    raw = json.dumps({"ALL STATS": {"Totals": "unexpected_string"}})
    self._patch_podexec(raw)
    self.assertEqual(_BM._ParseMemtierJson("/tmp/x.json", "pod", {}), [])


class DetectCloudTest(pkb_common_test_case.PkbCommonTestCase):
  """_DetectCloud: DMI-first cloud detection with metadata fallback."""

  def _patch_podexec_seq(self, *returns):
    """_PodExec returns each (stdout,'') in sequence across calls."""
    side = [(r, "") for r in returns]
    p = mock.patch.object(_BM, "_PodExec", side_effect=side)
    self.addCleanup(p.stop)
    return p.start()

  def testDmiGoogle(self):
    self._patch_podexec_seq("Google\nGoogle Compute Engine\n")
    self.assertEqual(_BM._DetectCloud("pod"), "gcp")

  def testDmiAmazon(self):
    self._patch_podexec_seq("Amazon EC2\nm6id.4xlarge\nAmazon EC2\n")
    self.assertEqual(_BM._DetectCloud("pod"), "aws")

  def testFallsBackToAwsImdsWhenDmiBlank(self):
    # DMI blank, GCP metadata blank, AWS IMDS returns an instance id.
    self._patch_podexec_seq("", "", "i-0abc123")
    self.assertEqual(_BM._DetectCloud("pod"), "aws")

  def testUnknownWhenAllBlank(self):
    self._patch_podexec_seq("", "", "")
    self.assertEqual(_BM._DetectCloud("pod"), "unknown")


class CollectCostSampleTest(pkb_common_test_case.PkbCommonTestCase):
  """_CollectCostSample: pricing, machine-type fallback, io2 storage cost."""

  def _patch_podexec(self, instance_type=""):
    # Both the GCP metadata and AWS IMDS probes return this string; an empty
    # string simulates in-pod metadata being unavailable.
    p = mock.patch.object(_BM, "_PodExec", return_value=(instance_type, ""))
    self.addCleanup(p.stop)
    return p.start()

  @flagsaver.flagsaver(swap_encryption_swap_type="instance_store")
  def testComputeCostFromMetadata(self):
    self._patch_podexec(instance_type="i4i.4xlarge")  # 1.4960/hr
    out = _BM._CollectCostSample("pod", 3600.0, {})
    self.assertLen(out, 1)
    self.assertEqual(out[0].metric, "cost_estimate_usd")
    self.assertAlmostEqual(out[0].value, 1.4960, places=3)
    self.assertEqual(out[0].metadata["instance_type"], "i4i.4xlarge")

  @flagsaver.flagsaver(
      swap_encryption_swap_type="instance_store",
      swap_encryption_benchmark_machine_type="i4i.4xlarge",
  )
  def testFallsBackToLaunchFlagWhenMetadataEmpty(self):
    """Regression: in-pod IMDS blocked by hop-limit -> metadata ''.

    Cost must still emit by falling back to the launch machine-type flag.
    """
    self._patch_podexec(instance_type="")  # metadata unavailable
    out = _BM._CollectCostSample("pod", 3600.0, {})
    self.assertLen(out, 1)
    self.assertAlmostEqual(out[0].value, 1.4960, places=3)

  @flagsaver.flagsaver(
      swap_encryption_swap_type="io2",
      swap_encryption_benchmark_machine_type="m6id.4xlarge",
      swap_encryption_io2_encrypted=True,
  )
  def testIo2AddsStorageCost(self):
    self._patch_podexec(instance_type="")  # force flag fallback
    out = _BM._CollectCostSample("pod", 3600.0, {})  # 1 hour
    self.assertLen(out, 1)
    # compute (m6id.4xlarge 0.9072) + io2 storage (1h/730 * (500*.125+16000*.065))
    expected_storage = (1.0 / 730.0) * (500 * 0.125 + 16000 * 0.065)
    self.assertAlmostEqual(
        out[0].metadata["storage_cost_usd"],
        round(expected_storage, 6),
        places=4,
    )
    self.assertAlmostEqual(out[0].value, 0.9072 + expected_storage, places=3)
    self.assertEqual(out[0].metadata["io2_encrypted"], True)

  @flagsaver.flagsaver(
      swap_encryption_swap_type="instance_store",
      swap_encryption_benchmark_machine_type="",
  )
  def testUnknownInstanceTypeSkips(self):
    self._patch_podexec(instance_type="totally-made-up.99xlarge")
    self.assertEqual(_BM._CollectCostSample("pod", 3600.0, {}), [])


if __name__ == "__main__":
  unittest.main()
