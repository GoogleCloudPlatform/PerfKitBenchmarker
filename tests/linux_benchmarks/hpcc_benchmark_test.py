# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for HPCC benchmark."""
import os
import unittest

import mock

from perfkitbenchmarker.linux_benchmarks import hpcc_benchmark
from tests import mock_flags


class HPCCTestCase(unittest.TestCase):

  def setUp(self):
    self.flags = mock_flags.PatchTestCaseFlags(self)
    self.flags.hpcc_math_library = 'openblas'
    path = os.path.join(os.path.dirname(__file__), '../data', 'hpcc-sample.txt')
    with open(path) as fp:
      self.contents = fp.read()

  def assertContainsSubDict(self, super_dict, sub_dict):
    """Asserts that every item in sub_dict is in super_dict."""
    for key, value in sub_dict.iteritems():
      self.assertEqual(super_dict[key], value)

  def testParseHpccValues(self):
    """Tests parsing the HPCC values."""
    benchmark_spec = mock.MagicMock()
    samples = hpcc_benchmark.ParseOutput(self.contents, benchmark_spec)
    self.assertEqual(46, len(samples))

    # Verify metric values and units are parsed correctly.
    actual = {metric: (value, units) for metric, value, units, _, _ in samples}

    expected = {
        'SingleDGEMM_Gflops': (25.6834, 'Gflop/s'),
        'HPL_Tflops': (0.0331844, 'Tflop/s'),
        'MPIRandomAccess_GUPs': (0.0134972, 'GUP/s'),
        'StarSTREAM_Triad': (5.92076, 'GB/s'),
        'SingleFFT_Gflops': (1.51539, 'Gflop/s'),
        'SingleSTREAM_Copy': (11.5032, 'GB/s'),
        'AvgPingPongBandwidth_GBytes': (9.01515, 'GB'),
        'MPIRandomAccess_CheckTime': (50.9459, 'seconds'),
        'MPIFFT_Gflops': (2.49383, 'Gflop/s'),
        'MPIRandomAccess_LCG_CheckTime': (53.1656, 'seconds'),
        'StarDGEMM_Gflops': (10.3432, 'Gflop/s'),
        'SingleSTREAM_Triad': (12.2433, 'GB/s'),
        'PTRANS_time': (9.24124, 'seconds'),
        'AvgPingPongLatency_usec': (0.34659, 'usec'),
        'RandomlyOrderedRingLatency_usec': (0.534474, 'usec'),
        'StarFFT_Gflops': (1.0687, 'Gflop/s'),
        'StarRandomAccess_GUPs': (0.00408809, 'GUP/s'),
        'StarRandomAccess_LCG_GUPs': (0.00408211, 'GUP/s'),
        'MPIRandomAccess_LCG_GUPs': (0.0132051, 'GUP/s'),
        'MPIRandomAccess_time': (56.6096, 'seconds'),
        'StarSTREAM_Scale': (5.16058, 'GB/s'),
        'MaxPingPongBandwidth_GBytes': (9.61445, 'GB'),
        'MPIRandomAccess_LCG_time': (60.0762, 'seconds'),
        'MinPingPongLatency_usec': (0.304646, 'usec'),
        'MPIFFT_time1': (0.603557, 'seconds'),
        'MPIFFT_time0': (9.53674e-07, 'seconds'),
        'MPIFFT_time3': (0.282359, 'seconds'),
        'MPIFFT_time2': (1.89799, 'seconds'),
        'MPIFFT_time5': (0.519769, 'seconds'),
        'MPIFFT_time4': (3.73566, 'seconds'),
        'MPIFFT_time6': (9.53674e-07, 'seconds'),
        'SingleRandomAccess_GUPs': (0.0151415, 'GUP/s'),
        'NaturallyOrderedRingBandwidth_GBytes': (1.93141, 'GB'),
        'MaxPingPongLatency_usec': (0.384119, 'usec'),
        'StarSTREAM_Add': (5.80539, 'GB/s'),
        'SingleSTREAM_Add': (12.7265, 'GB/s'),
        'SingleSTREAM_Scale': (11.6338, 'GB/s'),
        'StarSTREAM_Copy': (5.22586, 'GB/s'),
        'MPIRandomAccess_ExeUpdates': (764069508.0, 'updates'),
        'HPL_time': (1243.1, 'seconds'),
        'MPIRandomAccess_LCG_ExeUpdates': (793314388.0, 'updates'),
        'NaturallyOrderedRingLatency_usec': (0.548363, 'usec'),
        'PTRANS_GBs': (0.338561, 'GB/s'),
        'RandomlyOrderedRingBandwidth_GBytes': (2.06416, 'GB'),
        'SingleRandomAccess_LCG_GUPs': (0.0141455, 'GUP/s'),
        'MinPingPongBandwidth_GBytes': (6.85064, 'GB'),
    }
    self.assertEqual(expected, actual)

  def testParseHpccMetadata(self):
    """Tests parsing the HPCC metadata."""
    benchmark_spec = mock.MagicMock()
    samples = hpcc_benchmark.ParseOutput(self.contents, benchmark_spec)
    self.assertEqual(46, len(samples))
    results = {metric: metadata for metric, _, _, metadata, _ in samples}
    for metadata in results.itervalues():
      self.assertEqual(metadata['hpcc_math_library'], 'openblas')
      self.assertEqual(metadata['hpcc_version'], '1.5.0')

    # Spot check a few benchmark-specific metrics.
    self.assertContainsSubDict(
        results['PTRANS_time'], {
            'PTRANS_n': 19776.0,
            'PTRANS_nb': 192.0,
            'PTRANS_npcol': 2.0,
            'PTRANS_nprow': 2.0,
            'PTRANS_residual': 0.0
        })
    self.assertContainsSubDict(results['SingleRandomAccess_GUPs'],
                               {'RandomAccess_N': 268435456.0})
    self.assertContainsSubDict(
        results['MPIRandomAccess_LCG_GUPs'], {
            'MPIRandomAccess_LCG_Algorithm': 0.0,
            'MPIRandomAccess_LCG_Errors': 0.0,
            'MPIRandomAccess_LCG_ErrorsFraction': 0.0,
            'MPIRandomAccess_LCG_N': 1073741824.0,
            'MPIRandomAccess_LCG_TimeBound': 60.0
        })
    self.assertContainsSubDict(results['StarRandomAccess_LCG_GUPs'],
                               {'RandomAccess_LCG_N': 268435456.0})
    self.assertContainsSubDict(results['MPIFFT_time6'], {
        'MPIFFT_N': 134217728.0,
        'MPIFFT_Procs': 4.0,
        'MPIFFT_maxErr': 2.31089e-15
    })
    self.assertContainsSubDict(results['StarFFT_Gflops'], {'FFT_N': 67108864.0})
    self.assertContainsSubDict(results['SingleSTREAM_Copy'], {
        'STREAM_Threads': 1.0,
        'STREAM_VectorSize': 130363392.0
    })
    self.assertContainsSubDict(results['AvgPingPongLatency_usec'], {})
    self.assertContainsSubDict(results['StarRandomAccess_GUPs'],
                               {'RandomAccess_N': 268435456.0})


if __name__ == '__main__':
  unittest.main()
