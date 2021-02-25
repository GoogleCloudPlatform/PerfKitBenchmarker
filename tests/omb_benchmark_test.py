"""Tests for perfkitbenchmarker.linux_benchmarks.omb_benchmark."""

import unittest
from absl.testing import flagsaver
import mock

from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import omb_benchmark
from perfkitbenchmarker.linux_packages import omb
from tests import pkb_common_test_case


def MockBenchmarkSpec():
  benchmark_module = mock.Mock(BENCHMARK_NAME='omb')
  benchmark_config = mock.Mock(
      vm_groups={}, relational_db=mock.Mock(vm_groups={}))
  spec = benchmark_spec.BenchmarkSpec(benchmark_module, benchmark_config,
                                      'abcdefg')
  spec.vms = [mock.Mock(), mock.Mock()]
  return spec


_RUN_RESULT = result = omb.RunResult(
    name='acc_latency',
    metadata={'a': 1},
    data=[{
        'value': 10,
        'foo': 100
    }, {
        'value': 20,
        'foo': 200
    }],
    full_cmd='mpirun path/to/acc_latency',
    units='usec',
    params={'b': 2},
    mpi_vendor='intel',
    mpi_version='2019.6')

_EXPECTED_METADATA1 = {
    'foo': 100,
    'cmd': 'mpirun path/to/acc_latency',
    'metadata_a': 1,
    'param_b': 2,
    'omb_version': '5.7',
    'mpi_vendor': 'intel',
    'mpi_version': '2019.6',
}
_EXPECTED_METADATA2 = {
    'foo': 200,
    'cmd': 'mpirun path/to/acc_latency',
    'metadata_a': 1,
    'param_b': 2,
    'omb_version': '5.7',
    'mpi_vendor': 'intel',
    'mpi_version': '2019.6',
}

_EXPECTED_SAMPLES = [
    sample.Sample('acc_latency', 10.0, 'usec', _EXPECTED_METADATA1),
    sample.Sample('acc_latency', 20.0, 'usec', _EXPECTED_METADATA2),
]


class OmbBenchmarkTest(pkb_common_test_case.PkbCommonTestCase,
                       test_util.SamplesTestMixin):

  def testCreateSamples(self):
    samples = list(omb_benchmark._CreateSamples(_RUN_RESULT))

    self.assertSampleListsEqualUpToTimestamp(_EXPECTED_SAMPLES, samples)

  @flagsaver.flagsaver(omb_run_long_latency=True)
  @mock.patch.object(omb, 'RunBenchmark', return_value=_RUN_RESULT)
  def testRun(self, mock_run):
    bm_spec = MockBenchmarkSpec()
    samples = omb_benchmark.Run(bm_spec)

    self.assertSampleListsEqualUpToTimestamp(_EXPECTED_SAMPLES, samples[:2])
    self.assertLen(samples, 84)
    expected_calls = [
        mock.call(bm_spec.vms, name) for name in omb_benchmark._BENCHMARKS
    ]
    mock_run.assert_has_calls(expected_calls)


if __name__ == '__main__':
  unittest.main()
