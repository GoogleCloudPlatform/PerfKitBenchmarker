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

import time
import unittest

from absl.testing import flagsaver
import mock
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks import loaded_latency_benchmark
from perfkitbenchmarker.linux_packages import numactl
from tests import pkb_common_test_case

DelayPoint = loaded_latency_benchmark.DelayPoint


class _FakeMeasure:
  """Callable that returns DelayPoints from a curve and records delays."""

  def __init__(self, bw_fn, lat_fn):
    self._bw_fn = bw_fn
    self._lat_fn = lat_fn
    self.calls = []

  def __call__(self, d: int) -> DelayPoint:
    self.calls.append(d)
    return DelayPoint(d, float(self._bw_fn(d)), float(self._lat_fn(d)))


def _FromTable(table):
  return lambda d: table[d]


class ComputeGapTest(pkb_common_test_case.PkbCommonTestCase):

  def testIdenticalPointsHaveZeroGap(self):
    p = DelayPoint(0, 1000.0, 100.0)
    self.assertEqual(loaded_latency_benchmark._ComputeGap(p, p), 0.0)

  def testBandwidthIsScaledToGbPerSecond(self):
    # 3000 MiB/s * 0.001 = 3 and 4 ns form a 3-4-5 triangle.
    p1 = DelayPoint(0, 10000.0, 100.0)
    p2 = DelayPoint(64, 7000.0, 104.0)
    self.assertAlmostEqual(loaded_latency_benchmark._ComputeGap(p1, p2), 5.0)

  def testGapIsSymmetric(self):
    p1 = DelayPoint(0, 12345.0, 250.0)
    p2 = DelayPoint(64, 2345.0, 90.0)
    self.assertAlmostEqual(
        loaded_latency_benchmark._ComputeGap(p1, p2),
        loaded_latency_benchmark._ComputeGap(p2, p1),
    )


class ProbeDelayRangeTest(pkb_common_test_case.PkbCommonTestCase):

  def testStopsAtFirstDelayBelowIdleThreshold(self):
    # Peak BW 100000 -> threshold 2% = 2000; d=512 is the first below it.
    bw = {0: 100000, 64: 90000, 128: 50000, 256: 10000, 512: 1500}
    measure = _FakeMeasure(_FromTable(bw), lambda d: 100)

    x_end, probes = loaded_latency_benchmark._ProbeDelayRange(
        measure, max_delay=20000, idle_bw_fraction=0.02
    )

    self.assertEqual(x_end, 512)
    self.assertEqual([p.delay for p in probes], [0, 64, 128, 256, 512])
    self.assertEqual(measure.calls, [0, 64, 128, 256, 512])

  def testThresholdComparisonIsStrict(self):
    # BW exactly at the threshold does not stop probing.
    bw = {0: 100000, 64: 2000, 128: 1999}
    measure = _FakeMeasure(_FromTable(bw), lambda d: 100)

    x_end, _ = loaded_latency_benchmark._ProbeDelayRange(
        measure, max_delay=20000, idle_bw_fraction=0.02
    )

    self.assertEqual(x_end, 128)

  def testFallsBackToMaxDelayWhenBandwidthNeverDrops(self):
    measure = _FakeMeasure(lambda d: 100000, lambda d: 100)

    x_end, probes = loaded_latency_benchmark._ProbeDelayRange(
        measure, max_delay=300, idle_bw_fraction=0.02
    )

    self.assertEqual(x_end, 300)
    self.assertEqual([p.delay for p in probes], [0, 64, 128, 256, 300])

  def testMaxDelayBelowFirstProbe(self):
    measure = _FakeMeasure(lambda d: 100000, lambda d: 100)

    x_end, probes = loaded_latency_benchmark._ProbeDelayRange(
        measure, max_delay=50, idle_bw_fraction=0.02
    )

    self.assertEqual(x_end, 50)
    self.assertEqual([p.delay for p in probes], [0, 50])

  def testAlwaysProbesD64ByDefault(self):
    # Even if bandwidth is already near-idle at d=64, it is still measured.
    bw = {0: 100000, 64: 10}
    measure = _FakeMeasure(_FromTable(bw), lambda d: 100)

    x_end, probes = loaded_latency_benchmark._ProbeDelayRange(
        measure, max_delay=20000, idle_bw_fraction=0.02
    )

    self.assertEqual(x_end, 64)
    self.assertIn(64, [p.delay for p in probes])

  def testCustomFirstProbe(self):
    measure = _FakeMeasure(lambda d: 100000, lambda d: 100)

    _, probes = loaded_latency_benchmark._ProbeDelayRange(
        measure, max_delay=500, idle_bw_fraction=0.02, first_probe=100
    )

    self.assertEqual([p.delay for p in probes], [0, 100, 200, 400, 500])


class AdaptiveSamplingTest(pkb_common_test_case.PkbCommonTestCase):

  def testDoesNotRemeasureSeedPoints(self):
    measure = _FakeMeasure(lambda d: 0, lambda d: 100)
    seeds = [DelayPoint(0, 0, 100), DelayPoint(64, 0, 100)]
    seeds.append(DelayPoint(1024, 0, 100))

    result = loaded_latency_benchmark._AdaptiveSampling(
        measure, 0, 1024, seeds, max_samples=30, gap_tolerance=10.0
    )

    self.assertEqual(measure.calls, [])
    self.assertEqual(result, seeds)

  def testMeasuresMissingBoundariesAndD64(self):
    measure = _FakeMeasure(lambda d: 0, lambda d: 100)

    result = loaded_latency_benchmark._AdaptiveSampling(
        measure, 0, 1024, [], max_samples=30, gap_tolerance=10.0
    )

    self.assertCountEqual(measure.calls, [0, 64, 1024])
    self.assertEqual([p.delay for p in result], [0, 64, 1024])

  def testStopsWhenAllGapsBelowTolerance(self):
    # A flat curve has zero gap everywhere so nothing is bisected.
    measure = _FakeMeasure(lambda d: 5000, lambda d: 100)
    seeds = [measure(0), measure(64), measure(4096)]
    measure.calls.clear()

    result = loaded_latency_benchmark._AdaptiveSampling(
        measure, 0, 4096, seeds, max_samples=30, gap_tolerance=10.0
    )

    self.assertEqual(measure.calls, [])
    self.assertLen(result, 3)

  def testBisectsLargestGapFirst(self):
    # Steep drop between 0 and 64, nearly flat from 64 to 1024.
    def Latency(d):
      if d <= 64:
        return 300 - 200 * d / 64
      return 100 - 10 * (d - 64) / 960

    measure = _FakeMeasure(lambda d: 0, Latency)
    seeds = [measure(0), measure(64), measure(1024)]
    measure.calls.clear()

    loaded_latency_benchmark._AdaptiveSampling(
        measure, 0, 1024, seeds, max_samples=4, gap_tolerance=20.0
    )

    self.assertEqual(measure.calls, [32])

  def testRespectsMaxSamples(self):
    measure = _FakeMeasure(lambda d: 0, lambda d: d)

    result = loaded_latency_benchmark._AdaptiveSampling(
        measure, 0, 1024, [], max_samples=6, gap_tolerance=1.0
    )

    self.assertLen(result, 6)
    self.assertLen(measure.calls, 6)

  def testResultIsSortedAndUnique(self):
    measure = _FakeMeasure(lambda d: 200000 - 10 * d, lambda d: 300 - d / 10)

    result = loaded_latency_benchmark._AdaptiveSampling(
        measure, 0, 2048, [], max_samples=15, gap_tolerance=1.0
    )

    delays = [p.delay for p in result]
    self.assertEqual(delays, sorted(set(delays)))
    self.assertEqual(len(measure.calls), len(set(measure.calls)))

  def testDoesNotSubdivideAdjacentDelays(self):
    # The only large gap is between adjacent delays 63 and 64.
    measure = _FakeMeasure(lambda d: 0, lambda d: 1000 if d < 64 else 100)
    seeds = [measure(0), measure(63), measure(64), measure(128)]
    measure.calls.clear()

    result = loaded_latency_benchmark._AdaptiveSampling(
        measure, 0, 128, seeds, max_samples=30, gap_tolerance=10.0
    )

    self.assertEqual(measure.calls, [])
    self.assertEqual([p.delay for p in result], [0, 63, 64, 128])

  def testEqualGapsAreHandled(self):
    # Linear latency gives equal gaps, exercising heap tie-breaking.
    measure = _FakeMeasure(lambda d: 0, lambda d: d)
    seeds = [measure(0), measure(64), measure(128)]
    measure.calls.clear()

    result = loaded_latency_benchmark._AdaptiveSampling(
        measure, 0, 128, seeds, max_samples=5, gap_tolerance=1.0
    )

    self.assertCountEqual(measure.calls, [32, 96])
    self.assertEqual([p.delay for p in result], [0, 32, 64, 96, 128])


class GetCoresForMultiloadTest(pkb_common_test_case.PkbCommonTestCase):

  def setUp(self):
    super().setUp()
    # 8 vCPUs, 4 physical cores (SMT siblings cpu and cpu+4), 2 NUMA nodes.
    self.vm = mock.Mock()
    self.vm.GetCpusAllowedSet.return_value = set(range(8))
    self.vm.CheckProcCpu.return_value.mappings = {
        cpu: {'physical id': '0', 'core id': str(cpu % 4)} for cpu in range(8)
    }
    self.mock_get_numa_cpus = self.enter_context(
        mock.patch.object(
            numactl,
            'GetNumaCpus',
            return_value={0: {0, 1, 4, 5}, 1: {2, 3, 6, 7}},
        )
    )

  @flagsaver.flagsaver(loaded_latency_physical_cores_only=True)
  def testPhysicalCoresOnly(self):
    cpu, node, cores = loaded_latency_benchmark._GetCoresForMultiload(self.vm)
    self.assertEqual((cpu, node, cores), (3, 1, [0, 1, 2]))

  @flagsaver.flagsaver(loaded_latency_physical_cores_only=False)
  def testAllVcpusExcludesMultichaseSibling(self):
    cpu, node, cores = loaded_latency_benchmark._GetCoresForMultiload(self.vm)
    # CPU 7 runs multichase; its SMT sibling CPU 3 is excluded.
    self.assertEqual((cpu, node, cores), (7, 1, [0, 1, 2, 4, 5, 6]))

  @flagsaver.flagsaver(
      loaded_latency_physical_cores_only=True, loaded_latency_num_cores=2
  )
  def testNumCoresLimitsSelection(self):
    cpu, node, cores = loaded_latency_benchmark._GetCoresForMultiload(self.vm)
    self.assertEqual((cpu, node, cores), (1, 0, [0]))

  @flagsaver.flagsaver(loaded_latency_num_cores=1)
  def testNumCoresTooSmallRaises(self):
    with self.assertRaises(errors.Config.InvalidValue):
      loaded_latency_benchmark._GetCoresForMultiload(self.vm)

  @flagsaver.flagsaver(
      loaded_latency_physical_cores_only=True, loaded_latency_num_cores=5
  )
  def testNumCoresExceedsPhysicalCoresRaises(self):
    with self.assertRaises(errors.Config.InvalidValue):
      loaded_latency_benchmark._GetCoresForMultiload(self.vm)

  @flagsaver.flagsaver(loaded_latency_physical_cores_only=True)
  def testMissingCoreIdTreatsEachCpuAsCore(self):
    # e.g. ARM: /proc/cpuinfo has no 'core id' / 'physical id'.
    self.vm.GetCpusAllowedSet.return_value = {0, 1, 2, 3}
    self.vm.CheckProcCpu.return_value.mappings = {}
    self.mock_get_numa_cpus.return_value = {0: {0, 1, 2, 3}}

    cpu, node, cores = loaded_latency_benchmark._GetCoresForMultiload(self.vm)

    self.assertEqual((cpu, node, cores), (3, 0, [0, 1, 2]))


class MeasureDelayPointTest(pkb_common_test_case.PkbCommonTestCase):

  _MULTILOAD_OUT = '\n'.join([
      'header',
      'Total(MiB/s)= 100.0',
      'Total(MiB/s)= 200.0',
      'Total(MiB/s)= 300.0',
  ])

  def setUp(self):
    super().setUp()
    self.enter_context(mock.patch.object(time, 'sleep'))
    self.vm = mock.Mock()
    # grep -c results: not ready, warm (2 samples), post-chase (3 samples).
    self.grep_counts = iter(['0', '2', '3'])
    self.commands = []
    self.multichase_error = None
    self.vm.RemoteCommand.side_effect = self._RemoteCommand

  def _RemoteCommand(self, cmd, **kwargs):
    del kwargs
    self.commands.append(cmd)
    if cmd.startswith('nohup'):
      return '4321\n', ''
    if cmd.startswith('grep -c'):
      return next(self.grep_counts) + '\n', ''
    if 'multichase -m' in cmd:
      if self.multichase_error:
        raise self.multichase_error
      return 'some header\n  123.4\n', ''
    if cmd.startswith('cat'):
      return self._MULTILOAD_OUT, ''
    return '', ''

  def _Measure(self):
    return loaded_latency_benchmark._MeasureDelayPoint(
        self.vm,
        delay=64,
        multiload_path='multichase/multiload',
        multichase_path='multichase/multichase',
        multiload_numactl='-C 0,1,2',
        multichase_numactl='-C 3 -m 1',
        multiload_cores=[0, 1, 2],
    )

  def testReturnsLatencyAndBandwidthOverChaseWindow(self):
    point = self._Measure()

    # Warm-up finished at sample 2, so samples 2..3 are averaged.
    self.assertEqual(point, DelayPoint(64, 250.0, 123.4))

  def testCommandSequence(self):
    self._Measure()

    self.assertTrue(self.commands[0].startswith('nohup numactl -C 0,1,2'))
    self.assertIn('-t 3 -n 0', self.commands[0])
    self.assertIn('-d 64', self.commands[0])
    chase_idx = next(
        i for i, c in enumerate(self.commands) if 'multichase -m' in c
    )
    kill_idx = next(
        i for i, c in enumerate(self.commands) if c.startswith('kill -TERM')
    )
    self.assertTrue(self.commands[chase_idx].startswith('numactl -C 3 -m 1'))
    self.assertIn('kill -TERM 4321', self.commands[kill_idx])
    self.assertLess(chase_idx, kill_idx)
    self.assertTrue(self.commands[-1].startswith('cat multiload.out'))

  def testStopsMultiloadWhenMultichaseFails(self):
    self.multichase_error = errors.VirtualMachine.RemoteCommandError('boom')

    with self.assertRaises(errors.VirtualMachine.RemoteCommandError):
      self._Measure()

    self.assertTrue(any(c.startswith('kill -TERM 4321') for c in self.commands))


if __name__ == '__main__':
  unittest.main()
