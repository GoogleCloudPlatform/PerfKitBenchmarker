"""Tests for perfkitbenchmarker.linux_packages.omb."""

import os
import unittest
from absl.testing import parameterized
import mock

from perfkitbenchmarker import nfs_service
from perfkitbenchmarker.linux_packages import intelmpi
from perfkitbenchmarker.linux_packages import omb


def ReadFile(file_name):
  data_dir = os.path.join(os.path.dirname(__file__), 'data', 'omb')
  with open(os.path.join(data_dir, file_name)) as reader:
    return reader.read()


class OmbTest(parameterized.TestCase, unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(
            intelmpi, 'SourceMpiVarsCommand', return_value='. mpivars.sh'))

  @parameterized.parameters(
      ('acc_latency.txt', {
          'size': 1,
          'value': 0.3
      }, 23),
      ('barrier.txt', {
          'value': 3.97
      }, 1),
      ('ibarrier.txt', {
          'comm': 20.45,
          'compute': 21.65,
          'overlap': 0.0,
          'value': 49.25
      }, 1),
      ('ibcast.txt', {
          'comm': 0.87,
          'compute': 1.29,
          'overlap': 6.28,
          'size': 1,
          'value': 2.11
      }, 21),
      ('mbw_mr.txt', {
          'messages_per_second': 6385003.8,
          'size': 1,
          'value': 6.39
      }, 23),
  )
  def testParseData(self, test_file, first_entry, number_entries):
    input_text = ReadFile(test_file)

    values = omb._ParseBenchmarkData(input_text)

    self.assertEqual(first_entry, values[0])
    self.assertLen(values, number_entries)

  @parameterized.parameters(
      ('acc_latency.txt', {
          'sync': 'MPI_Win_flush',
          'window_creation': 'MPI_Win_allocate'
      }),
      ('barrier.txt', {}),
      ('ibarrier.txt', {}),
      ('ibcast.txt', {}),
      ('mbw_mr.txt', {
          'pairs': '15',
          'window_size': '64'
      }),
  )
  def testParseMetadata(self, test_file, expected_metadata):
    input_text = ReadFile(test_file)

    metadata = omb._ParseBenchmarkMetadata(input_text)

    self.assertEqual(expected_metadata, metadata)

  def testRunBenchmarkNormal(self):
    # all calls done with vm.RemoteCommand
    benchmark = 'barrier'
    benchmark_path = f'path/to/osu_{benchmark}'
    textoutput = 'textoutput'
    ls_cmd = f'ls {omb._RUN_DIR}/*/osu_{benchmark}'
    expected_full_cmd = ('. mpivars.sh; mpirun -perhost 1 -n 2 '
                         f'-hosts 10.0.0.1,10.0.0.2 {benchmark_path}')
    vm = mock.Mock(internal_ip='10.0.0.1')
    vm.RemoteCommand.side_effect = [(benchmark_path, ''), (textoutput, '')]

    txt, full_cmd = omb._RunBenchmark(
        vm,
        benchmark,
        number_processes=2,
        hosts=[vm, mock.Mock(internal_ip='10.0.0.2')])

    self.assertEqual(textoutput, txt)
    self.assertEqual(expected_full_cmd, full_cmd)
    vm.RemoteCommand.assert_has_calls(
        [mock.call(cmd) for cmd in (ls_cmd, expected_full_cmd)])

  def testRunBenchmarkRobustCommand(self):
    # calls done with vm.RemoteCommand and vm.RobustRemoteCommand due to
    # the benchmark being a long running one
    benchmark = 'get_acc_latency'
    benchmark_path = f'path/to/osu_{benchmark}'
    textoutput = 'textoutput'
    ls_cmd = f'ls {omb._RUN_DIR}/*/osu_{benchmark}'
    expected_full_cmd = ('. mpivars.sh; mpirun -perhost 1 -n 2 '
                         f'-hosts 10.0.0.1,10.0.0.2 {benchmark_path} -t 1')
    vm = mock.Mock(internal_ip='10.0.0.1')
    vm.RemoteCommand.side_effect = [(benchmark_path, '')]
    vm.RobustRemoteCommand.side_effect = [(textoutput, '')]

    txt, full_cmd = omb._RunBenchmark(
        vm,
        benchmark,
        number_processes=2,
        hosts=[vm, mock.Mock(internal_ip='10.0.0.2')],
        options={'-t': 1})

    self.assertEqual(textoutput, txt)
    self.assertEqual(expected_full_cmd, full_cmd)
    vm.RemoteCommand.assert_called_with(ls_cmd)
    vm.RobustRemoteCommand.assert_called_with(expected_full_cmd)

  def testPrepareWorkers(self):
    # to export /opt/intel
    mock_nfs_opt_intel = self.enter_context(
        mock.patch.object(intelmpi, 'NfsExportIntelDirectory'))
    # to export /usr/.../osu-microbenchmarks
    mock_nfs_osu = self.enter_context(
        mock.patch.object(nfs_service, 'NfsExportAndMount'))
    vm = mock.Mock(internal_ip='10.0.0.1')
    vm.RemoteCommand.side_effect = [('path/to/startup/osu_hello', ''),
                                    ('Hello World', '')]
    vms = [vm, mock.Mock(internal_ip='10.0.0.2')]

    omb.PrepareWorkers(vms)

    mock_nfs_opt_intel.assert_called_with(vms)
    mock_nfs_osu.assert_called_with(
        vms, '/usr/local/libexec/osu-micro-benchmarks/mpi')
    vm.RemoteCommand.assert_called_with(
        '. mpivars.sh; mpirun -perhost 1 -n 2 '
        '-hosts 10.0.0.1,10.0.0.2 path/to/startup/osu_hello')


if __name__ == '__main__':
  unittest.main()
