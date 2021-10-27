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
        'latency': 10,
        'foo': 100
    }, {
        'latency': 20,
        'foo': 200
    }],
    full_cmd='mpirun path/to/acc_latency',
    units='usec',
    params={'b': 2},
    mpi_vendor='intel',
    mpi_version='2019.6',
    value_column='latency',
    number_processes=6,
    run_time=0,
    pinning=['0:0:0,1,15', '1:1:0,1,15', '2:0:2,16,17', '3:1:2,16,17'],
    perhost=1,
    mpi_env={
        'I_MPI_DEBUG': '6',
        'I_MPI_PIN_PROCESSOR_LIST': '0'
    })

_COMMON_METADATA = {
    'cmd': 'mpirun path/to/acc_latency',
    'metadata_a': 1,
    'param_b': 2,
    'omb_version': '5.7.1',
    'mpi_vendor': 'intel',
    'mpi_version': '2019.6',
    'number_processes': 6,
    'run_time': 0,
    'pinning': '0:0:0,1,15;1:1:0,1,15;2:0:2,16,17;3:1:2,16,17',
    'perhost': 1,
    'mpi_env': 'I_MPI_DEBUG=6;I_MPI_PIN_PROCESSOR_LIST=0',
}
_METADATA1 = {'foo': 100, 'latency': 10, **_COMMON_METADATA}
_METADATA2 = {'foo': 200, 'latency': 20, **_COMMON_METADATA}

_EXPECTED_SAMPLES = [
    sample.Sample('acc_latency', 10.0, 'usec', _METADATA1),
    sample.Sample('acc_latency', 20.0, 'usec', _METADATA2),
]


class OmbBenchmarkTest(pkb_common_test_case.PkbCommonTestCase,
                       test_util.SamplesTestMixin):

  def setUp(self):
    super().setUp()
    self.enter_context(mock.patch.object(omb.time, 'time', return_value=0))

  def testCreateSamples(self):
    samples = list(omb_benchmark._CreateSamples(_RUN_RESULT))

    self.assertSampleListsEqualUpToTimestamp(_EXPECTED_SAMPLES, samples)

  @flagsaver.flagsaver(omb_run_long_latency=True)
  @mock.patch.object(omb, 'RunBenchmark', return_value=[_RUN_RESULT])
  def testRun(self, mock_run):
    bm_spec = MockBenchmarkSpec()
    samples = omb_benchmark.Run(bm_spec)

    self.assertSampleListsEqualUpToTimestamp(_EXPECTED_SAMPLES, samples[:2])
    self.assertLen(samples, 84)
    expected_calls = [
        mock.call(omb.RunRequest(name, bm_spec.vms)) for name in omb.BENCHMARKS
    ]
    mock_run.assert_has_calls(expected_calls)

  @flagsaver.flagsaver(omb_message_sizes=[1024, 2048])
  @mock.patch.object(omb, 'RunBenchmark', return_value=[_RUN_RESULT])
  def testMessageSizeRequest(self, mock_run):
    bm_spec = MockBenchmarkSpec()

    omb_benchmark.Run(bm_spec)

    expected_calls = []
    for name, run_type in sorted(omb.BENCHMARKS.items()):
      if run_type.long_running:
        continue
      for size in (1024, 2048):
        expected_calls.append(
            mock.call(omb.RunRequest(name, bm_spec.vms, size)))
    mock_run.assert_has_calls(expected_calls)


if __name__ == '__main__':
  unittest.main()
