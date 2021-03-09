"""Tests for perfkitbenchmarker.linux_packages.omb."""

import inspect
import os
import re
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import mock

from perfkitbenchmarker import nfs_service
from perfkitbenchmarker.linux_packages import intelmpi
from perfkitbenchmarker.linux_packages import omb


def ReadFile(file_name):
  data_dir = os.path.join(os.path.dirname(__file__), 'data', 'omb')
  with open(os.path.join(data_dir, file_name)) as reader:
    return reader.read()


def MakeDataRows(columns, values):
  ret = []
  for row in values:
    ret.append(dict(zip(columns, row)))
  return ret


def Latency(*values):
  return MakeDataRows(
      ('size', 'latency', 'min_latency', 'max_latency', 'iterations'), values)


def LatencyFullNoSize(*values):
  return MakeDataRows(('latency', 'min_latency', 'max_latency', 'iterations'),
                      values)


def LatencySizeOnly(*values):
  return MakeDataRows(('size', 'latency'), values)


def Compute(*values):
  return MakeDataRows(('size', 'overall', 'compute', 'collection_init',
                       'mpi_test', 'mpi_wait', 'pure_comm', 'overlap'), values)


def ComputeNoSize(*values):
  return MakeDataRows(('overall', 'compute', 'collection_init', 'mpi_test',
                       'mpi_wait', 'pure_comm', 'overlap'), values)


def Bandwidth(*values):
  return MakeDataRows(('size', 'bandwidth', 'messages_per_second'), values)


def BandwidthSizeOnly(*values):
  return MakeDataRows(('size', 'bandwidth'), values)


def MockVm(internal_ip='10.0.0.1', mpi_proceses_per_host=4):
  vm = mock.Mock(internal_ip=internal_ip)
  vm.NumCpusForBenchmark.return_value = mpi_proceses_per_host
  return vm


class OmbTest(parameterized.TestCase, absltest.TestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(
        mock.patch.object(
            intelmpi, 'SourceMpiVarsCommand', return_value='. mpivars.sh'))
    self.enter_context(
        mock.patch.object(intelmpi, 'MpirunMpiVersion', return_value='2019.6'))
    self.enter_context(mock.patch.object(omb.time, 'time', return_value=0))

  @parameterized.parameters(
      ('acc_latency', LatencySizeOnly([4, 90.84], [8, 91.08])),
      ('allgather',
       Latency([4, 62.40, 61.78, 63.01, 1000], [8, 60.89, 60.19, 61.59, 1000])),
      ('allgatherv',
       Latency([4, 61.51, 61.24, 61.79, 1000], [8, 64.19, 61.97, 66.41, 1000])),
      ('allreduce',
       Latency([4, 50.75, 50.44, 51.05, 1000], [8, 49.34, 49.14, 49.53, 1000])),
      ('alltoall',
       Latency([4, 55.00, 54.37, 55.63, 1000], [8, 53.41, 53.01, 53.81, 1000])),
      ('alltoallv',
       Latency([4, 70.98, 70.97, 70.98, 1000], [8, 70.80, 70.11, 71.50, 1000])),
      ('barrier', LatencyFullNoSize([98.91, 98.90, 98.91, 1000])),
      ('bcast', Latency([4, 7.38, 4.78, 9.97, 1000],
                        [8, 6.93, 4.77, 9.08, 1000])),
      ('bibw', BandwidthSizeOnly([4, 2.37], [8, 4.66])),
      ('bw', BandwidthSizeOnly([4, 1.02], [8, 1.98])),
      ('cas_latency', LatencySizeOnly([8, 100.65])),
      ('fop_latency', LatencySizeOnly([8, 110.70])),
      ('gather',
       Latency([4, 8.33, 4.94, 11.72, 1000], [8, 7.39, 4.77, 10.01, 1000])),
      ('gatherv',
       Latency([4, 102.58, 100.53, 104.63, 1000],
               [8, 103.69, 102.52, 104.87, 1000])),
      ('get_acc_latency', LatencySizeOnly([4, 309.93], [8, 310.21])),
      ('get_bw', BandwidthSizeOnly([4, 0.70], [8, 1.34])),
      ('get_latency', Latency([4, 117.79], [8, 120.48])),
      ('iallgather',
       Compute([4, 75.82, 51.84, 5.56, 0.00, 18.27, 49.52, 51.56],
               [8, 77.66, 54.44, 5.98, 0.00, 17.07, 52.11, 55.44])),
      ('iallgatherv',
       Compute([4, 86.42, 60.09, 5.52, 0.00, 20.67, 57.95, 54.57],
               [8, 86.60, 60.58, 5.52, 0.00, 20.36, 58.34, 55.40])),
      ('iallreduce',
       Compute([4, 79.51, 55.66, 5.65, 0.00, 18.05, 53.25, 55.21],
               [8, 79.90, 55.10, 5.54, 0.00, 19.12, 52.61, 52.86])),
      ('ialltoall',
       Compute([4, 96.74, 67.41, 10.19, 0.00, 18.99, 64.40, 54.44],
               [8, 94.75, 64.75, 10.28, 0.00, 19.55, 61.73, 51.41])),
      ('ialltoallv',
       Compute([4, 92.37, 62.60, 10.27, 0.00, 19.36, 59.81, 50.22],
               [8, 91.26, 61.82, 10.27, 0.00, 19.00, 58.79, 49.92])),
      ('ialltoallw',
       Compute([4, 116.56, 80.96, 10.59, 0.00, 24.86, 78.05, 54.39],
               [8, 114.01, 77.95, 10.54, 0.00, 25.37, 75.29, 52.10])),
      ('ibarrier',
       ComputeNoSize([112.28, 91.34, 4.06, 0.00, 16.71, 88.42, 76.32])),
      ('ibcast',
       Compute([4, 17.76, 9.67, 2.82, 0.00, 5.14, 8.53, 5.13],
               [8, 18.88, 10.81, 2.78, 0.00, 5.15, 9.68, 16.56])),
      ('igather',
       Compute([4, 21.13, 11.91, 3.17, 0.00, 5.89, 10.69, 13.70],
               [8, 21.21, 12.09, 3.13, 0.00, 5.83, 11.17, 18.31])),
      ('igatherv',
       Compute([4, 19.88, 11.44, 2.86, 0.00, 5.43, 10.27, 17.88],
               [8, 18.80, 10.12, 2.74, 0.00, 5.82, 8.97, 3.20])),
      ('ireduce',
       Compute([4, 18.06, 10.12, 3.00, 0.00, 4.81, 9.09, 12.65],
               [8, 18.78, 10.59, 3.03, 0.00, 5.03, 9.51, 13.91])),
      ('iscatter',
       Compute([4, 20.56, 11.41, 2.75, 0.00, 6.26, 10.05, 9.00],
               [8, 18.76, 10.71, 2.77, 0.00, 5.13, 9.50, 15.29])),
      ('iscatterv',
       Compute([4, 18.53, 10.12, 2.85, 0.00, 5.43, 8.94, 5.92],
               [8, 17.51, 10.09, 2.72, 0.00, 4.56, 8.86, 16.13])),
      ('latency', LatencySizeOnly([4, 57.29], [8, 56.51])),
      ('latency_mp', LatencySizeOnly([4, 47.96], [8, 47.95])),
      ('latency_mt', LatencySizeOnly([4, 2768.29], [8, 2950.69])),
      ('mbw_mr', Bandwidth([4, 0.95, 238253.89], [8, 1.78, 222293.85])),
      ('multi_lat', LatencySizeOnly([4, 50.58], [8, 50.00])),
      ('put_bibw', BandwidthSizeOnly([4, 1.95], [8, 3.82])),
      ('put_bw', BandwidthSizeOnly([4, 2.73], [8, 5.31])),
      ('put_latency', LatencySizeOnly([4, 3.84], [8, 3.81])),
      ('reduce',
       Latency([4, 8.93, 5.10, 12.77, 1000], [8, 8.40, 4.99, 11.80, 1000])),
      ('reduce_scatter',
       Latency([4, 86.93, 86.81, 87.06, 1000], [8, 49.81, 49.59, 50.03, 1000])),
      ('scatter',
       Latency([4, 8.36, 5.04, 11.69, 1000], [8, 7.54, 4.75, 10.33, 1000])),
      ('scatterv',
       Latency([4, 11.89, 5.07, 18.71, 1000], [8, 8.23, 4.86, 11.60, 1000])),
  )
  def testParseData(self, test_name, expected_values):
    input_text = ReadFile(f'{test_name}.txt')

    values = omb._ParseBenchmarkData(test_name, input_text)

    self.assertEqual(expected_values, values)

  @parameterized.parameters(
      ('acc_latency.txt', {
          'sync': 'MPI_Win_flush',
          'window_creation': 'MPI_Win_allocate'
      }),
      ('barrier.txt', {}),
      ('ibarrier.txt', {}),
      ('ibcast.txt', {}),
      ('mbw_mr.txt', {
          'pairs': '1',
          'window_size': '64'
      }),
  )
  def testParseMetadata(self, test_file, expected_metadata):
    input_text = ReadFile(test_file)

    metadata = omb._ParseBenchmarkMetadata(input_text)

    self.assertEqual(expected_metadata, metadata)

  @flagsaver.flagsaver(omb_mpi_debug=0)
  def testRunBenchmarkNormal(self):
    # all calls done with vm.RemoteCommand
    benchmark = 'barrier'
    benchmark_path = f'path/to/osu_{benchmark}'
    textoutput = 'textoutput'
    ls_cmd = f'ls {omb._RUN_DIR}/*/osu_{benchmark}'
    expected_full_cmd = ('. mpivars.sh; mpirun -perhost 1 -n 2 '
                         f'-hosts 10.0.0.1,10.0.0.2 {benchmark_path} '
                         '-t 1 --full')
    vm = mock.Mock(internal_ip='10.0.0.1')
    vm.RemoteCommand.side_effect = [(benchmark_path, '')]
    vm.RobustRemoteCommand.side_effect = [(textoutput, '')]

    txt, full_cmd = omb._RunBenchmark(
        vm,
        benchmark,
        number_processes=2,
        hosts=[vm, mock.Mock(internal_ip='10.0.0.2')],
        options={'-t': 1},
        perhost=1)

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
    mpi_dir = '/usr/local/libexec/osu-micro-benchmarks/mpi'
    vm = mock.Mock(internal_ip='10.0.0.1')
    vm.RemoteCommand.side_effect = [(f'{mpi_dir}/startup/osu_hello', '')]
    vm.RobustRemoteCommand.side_effect = [('Hello World', '')]
    vms = [vm, mock.Mock(internal_ip='10.0.0.2')]

    omb.PrepareWorkers(vms)

    mock_nfs_opt_intel.assert_called_with(vms)
    mock_nfs_osu.assert_called_with(vms, mpi_dir)
    vm.RemoteCommand.assert_called_with(f'ls {mpi_dir}/*/osu_hello')
    vm.RobustRemoteCommand.assert_called_with(
        '. mpivars.sh; I_MPI_DEBUG=5 mpirun -perhost 1 -n 2 '
        f'-hosts 10.0.0.1,10.0.0.2 {mpi_dir}/startup/osu_hello')

  @flagsaver.flagsaver(omb_iterations=10, omb_mpi_debug=0)
  def testRunResult(self):
    test_output = inspect.cleandoc("""
    [0] MPI startup(): Rank    Pid      Node name       Pin cpu
    [0] MPI startup(): 0       17442    pkb-a0b71860-0  {0,1}
    [0] MPI startup(): 1       3735     pkb-a0b71860-1  {0,
                                          1}
    # OSU MPI Multiple Bandwidth / Message Rate Test v5.7
    # [ pairs: 15 ] [ window size: 64 ]
    # Size                  MB/s        Messages/s
    1                       6.39        6385003.80
    """)
    vm = MockVm()
    mpitest_path = 'path/to/startup/osu_mbw_mr'
    vm.RemoteCommand.side_effect = [(mpitest_path, ''), (mpitest_path, '')]
    vm.RobustRemoteCommand.side_effect = [(test_output, ''), (test_output, '')]
    vms = [vm, mock.Mock(internal_ip='10.0.0.2')]
    results = list(omb.RunBenchmark(omb.RunRequest('mbw_mr', vms)))

    expected_result = omb.RunResult(
        name='mbw_mr',
        metadata={
            'pairs': '15',
            'window_size': '64'
        },
        data=[{
            'size': 1,
            'bandwidth': 6.39,
            'messages_per_second': 6385003.8
        }],
        full_cmd=('. mpivars.sh; mpirun -perhost 1 -n 2 '
                  f'-hosts 10.0.0.1,10.0.0.2 {mpitest_path} --iterations 10'),
        units='MB/s',
        params={'--iterations': 10},
        mpi_vendor='intel',
        mpi_version='2019.6',
        value_column='bandwidth',
        number_processes=2,
        run_time=0,
        pinning={
            0: 'pkb-a0b71860-0:0,1',
            1: 'pkb-a0b71860-1:0,1'
        },
        perhost=1)
    self.assertEqual(expected_result, results[0])
    self.assertLen(results, 2)
    # Called twice, the second time with 4*2=8 processes
    self.assertEqual(8, results[1].number_processes)

  @parameterized.parameters(
      {
          'lines': ('a', 'b1', 'b2', 'c'),
          'regex_text': 'b',
          'expected': ['b2', 'c']
      }, {
          'lines': ('a', 'b', 'c'),
          'regex_text': 'd',
          'expected': []
      })
  def testLinesAfterMarker(self, lines, regex_text, expected):
    line_re = re.compile(regex_text)
    input_text = '\n'.join(lines)

    self.assertEqual(expected, omb._LinesAfterMarker(line_re, input_text))

  def testParseMpiPinningInfo(self):
    txt = """
    MPI startup(): libfabric provider: tcp;ofi_rxm
    MPI startup(): Rank    Pid      Node name       Pin cpu
    MPI startup(): 0       17077    pkb-a0b71860-0  {0,1,15}
    MPI startup(): 1       3475     pkb-a0b71860-1  {0,
                                       1,15}
    MPI startup(): 2       17078    pkb-a0b71860-0  {2,16,17}
    MPI startup(): 3       3476     pkb-a0b71860-1  {2,16,17}
    """

    pinning = omb._ParseMpiPinningInfo(txt)

    expected_pinning = {
        0: 'pkb-a0b71860-0:0,1,15',
        1: 'pkb-a0b71860-1:0,1,15',
        2: 'pkb-a0b71860-0:2,16,17',
        3: 'pkb-a0b71860-1:2,16,17'
    }
    self.assertEqual(expected_pinning, pinning)

  @flagsaver.flagsaver(omb_perhost=2)
  @mock.patch.object(omb, '_PathToBenchmark', return_value='/igather')
  def testPerhost(self, mock_benchmark_path):
    del mock_benchmark_path
    vm = MockVm()
    vm.RobustRemoteCommand.return_value = ('', '')
    results = list(
        omb.RunBenchmark(omb.RunRequest('igather', [vm], 1024)))
    self.assertIn('-perhost 2', results[0].full_cmd)
    self.assertIn('-m 1024:1024', results[1].full_cmd)


if __name__ == '__main__':
  absltest.main()
