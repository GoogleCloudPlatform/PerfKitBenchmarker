"""Tests for MPI benchmark."""

from typing import List
import unittest
from unittest import mock
import uuid

from absl import flags
from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import mpi_benchmark
from perfkitbenchmarker.linux_packages import intelmpi
from perfkitbenchmarker.linux_packages import mpi
from tests import pkb_common_test_case
from tests.linux_packages import mpi_test


FLAGS = flags.FLAGS

# Histogram results from reading MPI output file
histogram1 = {'12.5': 50000, '10.0': 50000}
histogram2 = {'6.0': 50000, '50.0': 50000}
histogram_text = (
    """\
1024 12.51
1024 10.01
""" * 50000
    + """\
2048 6.00
2048 50.0
""" * 50000
)

MPI_VARS = '/opt/intel/compilers_and_libraries/linux/mpi/intel64/bin/mpivars.sh'


# All VMs have num_cpus=32
class Vm(pkb_common_test_case.TestLinuxVirtualMachine):

  def __init__(
      self, smt_enabled=True, ip='10.0.0.2', robust_remote_command_text=None
  ) -> None:
    super(Vm, self).__init__(vm_spec=pkb_common_test_case.CreateTestVmSpec())
    self.internal_ip = ip
    self.num_cpus = 32
    # pylint: disable=invalid-name
    self.IsSmtEnabled = mock.PropertyMock(return_value=smt_enabled)
    self.RemoteCommand = mock.PropertyMock(
        return_value=('Version 2019 Update 2 Build 2019.2-057', '')
    )
    self.RobustRemoteCommand = mock.PropertyMock(
        return_value=((mpi_test.ReadMpiOutput('mpi_pingpong_output.txt'), ''))
    )


def MpiRun(vms) -> List[sample.Sample]:
  benchmark_module = mock.Mock(BENCHMARK_NAME='mpi')
  benchmark_config = mock.Mock(
      vm_groups={}, relational_db=mock.Mock(vm_groups={})
  )
  spec = benchmark_spec.BenchmarkSpec(
      benchmark_module, benchmark_config, 'abcdefg'
  )
  spec.vms = vms
  return mpi_benchmark.Run(spec)


class MpiBenchmarkTestCase(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin
):
  _METRIC_ERR = 'Metric values should be equal %s != %s'
  _VALUE_ERR = 'Values should be equal %s != %s'
  _UNIT_ERR = 'Unit values should be equal %s != %s'
  # the latency dump file name uses uuid4()
  _MOCK_UUIDS = [mock.PropertyMock(hex=f'uuid_{i}') for i in range(12)]

  def setUp(self) -> None:
    super(MpiBenchmarkTestCase, self).setUp()
    FLAGS.mpi_benchmarks = ['PingPong']
    FLAGS.intelmpi_version = '2019.2-057'
    self.mock_histo = self.enter_context(
        mock.patch.object(mpi, '_GroupLatencyLines')
    )
    self.mock_histo.return_value = [histogram_text.splitlines()]
    self.enter_context(
        mock.patch.object(intelmpi, 'MpiVars', return_value=MPI_VARS)
    )

  @mock.patch.object(uuid, 'uuid4', side_effect=_MOCK_UUIDS)
  def testRun(self, mock_uuid) -> None:
    FLAGS.mpi_threads = [0]
    FLAGS.mpi_env = ['FI_PROVIDER=tcp', 'FI_LOG_LEVEL=info']
    FLAGS.mpi_genv = ['I_MPI_PIN_PROCESSOR_LIST=0', 'I_MPI_PIN=1']
    FLAGS.mpi_npmin = 2
    FLAGS.mpi_tune = True
    FLAGS.mpi_multi = True
    found = MpiRun([Vm()])
    expected = []
    for row in mpi_test.ReadJson('mpi_tests_samples.json'):
      expected.append(sample.Sample(**row))
      expected[-1].metadata['installed_mkl'] = False
    self.assertSampleListsEqualUpToTimestamp(expected, found)
    self.assertLen(expected, 8)
    self.assertEqual(2, self.mock_histo.call_count)

  @parameterized.parameters(
      {'threads': [0], 'num_vms': 1, 'expected_threads': [16]},
      {'threads': [2, 6, 18], 'num_vms': 2, 'expected_threads': [4, 12, 36]},
      {
          'threads': [0],
          'num_vms': 1,
          'expected_threads': [32],
          'smt_enabled': False,  # this forces threads=num_cpus
      },
  )
  @mock.patch.object(mpi_benchmark, '_RunTest')
  def testRunTestCommand(
      self,
      mock_run: mock.Mock,
      num_vms: int,
      expected_threads: List[int],
      threads: List[int],
      smt_enabled: bool = True,
  ) -> None:
    FLAGS.mpi_threads = threads
    MpiRun([Vm(smt_enabled) for _ in range(num_vms)])
    for total_processes, found in zip(
        expected_threads, mock_run.call_args_list
    ):
      _, found_total_processes, found_ppn, _ = found[0]
      self.assertEqual(total_processes, found_total_processes)
      self.assertEqual(0, found_ppn)
    self.assertLen(
        mock_run.call_args_list,
        len(expected_threads),
        'Missing / extra calls in {}'.format(mock_run.call_args_list),
    )
    self.mock_histo.assert_not_called()

  @mock.patch.object(mpi, 'RunMpiStats')
  def testRunMpiStatsCall(self, mock_mpistats: mock.Mock) -> None:
    tests = ['PingPong', 'AllGather']
    FLAGS.mpi_benchmarks = tests
    vms = [Vm(ip='1.2.3.4'), Vm(ip='5.6.7.8')]
    total_processes = 32
    ppn = 0
    mpi.RunMpiStats.return_value = mpi.MpiResponse('', '', '', '', [], [], {})
    mpi_benchmark._RunTest(vms, total_processes, ppn, False)
    # RunMpiStats called for each one of the --mpi_benchmarks and also for each
    # of the msglog values: len(['PingPong','AllGather']) * len([10,11]) = 4
    self.assertLen(mock_mpistats.call_args_list, 4)
    # just test the last one run which is AllGather with msglog_min=11
    mock_mpistats.assert_called_with(
        vms[0],
        mpi.MpiRequest(
            vms=vms,
            total_processes=total_processes,
            suite='IMB-MPI1',
            tests=[tests[-1]],
            ppn=ppn,
            msglog_min=11,
            msglog_max=11,
            timeout=60,
            off_cache_size=-1,
            off_cache_line_size=None,
            iterations=100000,
            include_zero_byte=False,
            compile_from_source=True,
            record_latencies=True,
            environment=['I_MPI_DEBUG=6'],
            multi=True,
        ),
    )
    self.mock_histo.assert_not_called()

  @parameterized.parameters((True, 16), (False, 32))
  def testSmtUsage(self, smt_enabled: bool, num_processes: int) -> None:
    FLAGS.mpi_threads = [0]
    data = MpiRun([Vm(smt_enabled)])
    self.assertNotEmpty(data)
    found = data[0].metadata
    self.assertEqual(num_processes, found['processes_per_host'])
    self.assertEqual(2, self.mock_histo.call_count)

  def testHistoResults(self) -> None:
    FLAGS.mpi_record_latency = True
    # Returns with this histogram MpiData with every call to the method
    data = MpiRun([Vm(False)])
    self.assertLen(data, 16)
    histogram_data = [
        item for item in data if item.metric == 'MPI_Latency_Histogram'
    ]
    self.assertLen(histogram_data, 8)
    meta1 = {
        'bytes': 1024,
        'mpi_groups': 2,
        'mpi_processes_per_group': 2,
        'histogram': histogram1,
    }
    self.assertDictContainsSubset(meta1, histogram_data[0].metadata)
    meta2 = {
        'bytes': 2048,
        'mpi_groups': 2,
        'mpi_processes_per_group': 2,
        'histogram': histogram2,
    }
    self.assertDictContainsSubset(meta2, histogram_data[1].metadata)
    self.assertEqual(4, self.mock_histo.call_count)

  @flagsaver.flagsaver(mpi_benchmarks=['Qubert', 'Broadcast', 'allTOaLL'])
  def testGetConfigBadBenchmark(self):
    # Alltoall is a valid benchmark
    with self.assertRaisesRegex(
        errors.Setup.InvalidFlagConfigurationError, '"broadcast,qubert"'
    ):
      mpi_benchmark.GetConfig({})

  @flagsaver.flagsaver(mpi_benchmarks=['Bcast'], mpi_msglog_sizes=[20])
  def testGetConfigNoErrors(self):
    # Confirms that no exception is thrown
    mpi_benchmark.GetConfig({})

  @flagsaver.flagsaver(mpi_msglog_sizes=[20])
  def testGetConfigBadMessageSizeFlags(self):
    # Need to do .parse() so that FLAGS['mpi_msglog_min'].present resolves
    FLAGS['mpi_msglog_min'].parse(10)
    with self.assertRaises(errors.Setup.InvalidFlagConfigurationError):
      mpi_benchmark.GetConfig({})

  @flagsaver.flagsaver(mpi_suites=['IMB-MT'])
  def testRunTestWithSuites(self):
    FLAGS.mpi_benchmarks = []
    # Mock response with no results as not testing that functionality
    response = mpi.MpiResponse('a', 'b', 'c', 'd', [], [], {})
    mpirun_mock = self.enter_context(
        mock.patch.object(mpi, 'RunMpiStats', return_value=response)
    )
    vm = Vm()

    mpi_benchmark._RunTest([vm], 2, 1, True)

    expected_request = mpi.MpiRequest(
        vms=[vm],
        total_processes=2,
        suite='IMB-MT',
        tests=['UniBandMT'],
        ppn=1,
        msglog_min=11,
        msglog_max=11,
        timeout=60,
        off_cache_size=-1,
        off_cache_line_size=None,
        iterations=100000,
        include_zero_byte=False,
        compile_from_source=True,
        environment=['I_MPI_DEBUG=6'],
        global_environment=[],
        record_latencies=True,
        npmin=None,
        tune=False,
        multi=True,
    )
    # Test the last one called
    mpirun_mock.assert_called_with(vm, expected_request)
    # It was called len(IMB-MT suite tests) times
    self.assertLen(mpirun_mock.call_args_list, 20)


if __name__ == '__main__':
  unittest.main()
