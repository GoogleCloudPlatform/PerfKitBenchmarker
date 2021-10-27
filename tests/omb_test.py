"""Tests for perfkitbenchmarker.linux_packages.omb."""

import inspect
import json
import os
import re

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import nfs_service
from perfkitbenchmarker.linux_packages import intelmpi
from perfkitbenchmarker.linux_packages import omb
from perfkitbenchmarker.linux_packages import openmpi


def ReadFile(file_name):
  data_dir = os.path.join(os.path.dirname(__file__), 'data', 'omb')
  with open(os.path.join(data_dir, file_name)) as reader:
    return reader.read()


def MockVm(internal_ip='10.0.0.1', mpi_proceses_per_host=4):
  vm = mock.Mock(internal_ip=internal_ip)
  vm.NumCpusForBenchmark.return_value = mpi_proceses_per_host
  return vm


class OmbCommonTest(parameterized.TestCase, absltest.TestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(mock.patch.object(omb.time, 'time', return_value=0))

  @parameterized.parameters(
      ('acc_latency',), ('allgather',), ('allgatherv',), ('allreduce',),
      ('alltoall',), ('alltoallv',), ('barrier',), ('bcast',), ('bibw',),
      ('bw',), ('cas_latency',), ('fop_latency',), ('gather',), ('gatherv',),
      ('get_acc_latency',), ('get_bw',), ('get_latency',), ('iallgather',),
      ('iallgatherv',), ('iallreduce',), ('ialltoall',), ('ialltoallv',),
      ('ialltoallw',), ('ibarrier',), ('ibcast',), ('igather',), ('igatherv',),
      ('ireduce',), ('iscatter',), ('iscatterv',), ('latency',),
      ('latency_mp',), ('latency_mt',), ('mbw_mr',), ('multi_lat',),
      ('put_bibw',), ('put_bw',), ('put_latency',), ('reduce',),
      ('reduce_scatter',), ('scatter',), ('scatterv',))
  def testParseData(self, test_name):
    input_text = ReadFile(f'{test_name}.txt')

    values = omb._ParseBenchmarkData(test_name, input_text)

    self.assertEqual(json.loads(ReadFile(f'{test_name}.json')), values)

  @parameterized.parameters(
      ('acc_latency.txt', {
          'sync': 'MPI_Win_flush',
          'window_creation': 'MPI_Win_allocate'
      }),
      ('barrier.txt', {}),
      ('ibarrier.txt', {}),
      ('ibcast.txt', {}),
      ('mbw_mr.txt', {
          'pairs': '4',
          'window_size': '64'
      }),
  )
  def testParseMetadata(self, test_file, expected_metadata):
    input_text = ReadFile(test_file)

    metadata = omb._ParseBenchmarkMetadata(input_text)

    self.assertEqual(expected_metadata, metadata)

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
    txt = inspect.cleandoc("""
    [0] MPI startup(): libfabric provider: tcp;ofi_rxm
    [0] MPI startup(): Rank    Pid      Node name       Pin cpu
    [0] MPI startup(): 0       17077    pkb-a0b71860-0  {0,1,15}
    [0] MPI startup(): 1       3475     pkb-a0b71860-1  {0,
                                           1,15}
    [0] MPI startup(): 2       17078    pkb-a0b71860-0  {2,16,17}
    [0] MPI startup(): 3       3476     pkb-a0b71860-1  {2,16,17}
    """)

    pinning = omb.ParseMpiPinning(txt.splitlines())

    expected_pinning = [
        '0:0:0,1,15', '1:1:0,1,15', '2:0:2,16,17', '3:1:2,16,17'
    ]
    self.assertEqual(expected_pinning, pinning)


class OmbIntelMpiTest(parameterized.TestCase, absltest.TestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(mpi_vendor='intel'))
    self.enter_context(
        mock.patch.object(
            intelmpi, 'SourceMpiVarsCommand', return_value='. mpivars.sh'))
    self.enter_context(
        mock.patch.object(intelmpi, 'MpirunMpiVersion', return_value='2019.6'))
    self.enter_context(mock.patch.object(omb.time, 'time', return_value=0))

  @flagsaver.flagsaver(
      omb_mpi_env=['IMPI_DEBUG=5'],
      omb_mpi_genv=['I_MPI_PIN_PROCESSOR_LIST=0', 'I_MPI_PIN=1'])
  def testRunBenchmarkNormal(self):
    # all calls done with vm.RemoteCommand
    benchmark = 'barrier'
    benchmark_path = f'path/to/osu_{benchmark}'
    textoutput = 'textoutput'
    ls_cmd = f'ls {omb._RUN_DIR}/*/osu_{benchmark}'
    expected_full_cmd = ('. mpivars.sh; IMPI_DEBUG=5 mpirun '
                         '-genv I_MPI_PIN=1 '
                         '-genv I_MPI_PIN_PROCESSOR_LIST=0 -perhost 1 -n 2 '
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
        '. mpivars.sh; mpirun -perhost 1 -n 2 '
        f'-hosts 10.0.0.1,10.0.0.2 {mpi_dir}/startup/osu_hello')

  @flagsaver.flagsaver(
      omb_iterations=10,
      omb_mpi_env=['IMPI_DEBUG=5'],
      omb_mpi_genv=['I_MPI_PIN_PROCESSOR_LIST=0', 'I_MPI_PIN=1'])
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
        full_cmd=('. mpivars.sh; IMPI_DEBUG=5 mpirun '
                  '-genv I_MPI_PIN=1 '
                  '-genv I_MPI_PIN_PROCESSOR_LIST=0 -perhost 1 -n 2 '
                  f'-hosts 10.0.0.1,10.0.0.2 {mpitest_path} --iterations 10'),
        units='MB/s',
        params={'--iterations': 10},
        mpi_vendor='intel',
        mpi_version='2019.6',
        value_column='bandwidth',
        number_processes=2,
        run_time=0,
        pinning=['0:0:0,1', '1:1:0,1'],
        perhost=1,
        mpi_env={
            'I_MPI_PIN_PROCESSOR_LIST': '0',
            'I_MPI_PIN': '1',
            'IMPI_DEBUG': '5'
        })
    self.assertEqual(expected_result, results[0])
    self.assertLen(results, 2)
    # Called twice, the second time with 4*2=8 processes
    self.assertEqual(8, results[1].number_processes)

  @flagsaver.flagsaver(omb_perhost=2)
  @mock.patch.object(omb, '_PathToBenchmark', return_value='/igather')
  def testPerhost(self, mock_benchmark_path):
    del mock_benchmark_path
    vm = MockVm()
    vm.RobustRemoteCommand.return_value = ('', '')
    results = list(omb.RunBenchmark(omb.RunRequest('igather', [vm], 1024)))
    self.assertIn('-perhost 2', results[0].full_cmd)
    self.assertIn('-m 1024:1024', results[1].full_cmd)


class OmbOpenMpiTest(parameterized.TestCase, absltest.TestCase):

  def setUp(self):
    super().setUp()
    self.enter_context(flagsaver.flagsaver(mpi_vendor='openmpi'))
    self.enter_context(
        mock.patch.object(openmpi, 'GetMpiVersion', return_value='3.1.2'))
    self.enter_context(mock.patch.object(omb.time, 'time', return_value=0))

  def testRunBenchmarkNormal(self):
    # all calls done with vm.RemoteCommand
    benchmark = 'barrier'
    benchmark_path = f'path/to/osu_{benchmark}'
    textoutput = 'textoutput'
    ls_cmd = f'ls {omb._RUN_DIR}/*/osu_{benchmark}'
    expected_full_cmd = ('mpirun -report-bindings -display-map '
                         '-n 2 -npernode 1 --use-hwthread-cpus '
                         '-host 10.0.0.1:slots=2,10.0.0.2:slots=2 '
                         f'{benchmark_path} -t 1 --full')
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
    # to export /usr/.../osu-microbenchmarks
    mock_nfs_osu = self.enter_context(
        mock.patch.object(nfs_service, 'NfsExportAndMount'))
    mpi_dir = '/usr/local/libexec/osu-micro-benchmarks/mpi'
    vm = mock.Mock(internal_ip='10.0.0.1')
    vm.RemoteCommand.side_effect = [(f'{mpi_dir}/startup/osu_hello', '')]
    vm.RobustRemoteCommand.side_effect = [('Hello World', '')]
    vms = [vm, mock.Mock(internal_ip='10.0.0.2')]

    omb.PrepareWorkers(vms)

    mock_nfs_osu.assert_called_with(vms, mpi_dir)
    vm.Install.assert_called_with('openmpi')
    vm.RemoteCommand.assert_called_with(f'ls {mpi_dir}/*/osu_hello')
    vm.RobustRemoteCommand.assert_called_with(
        'mpirun -report-bindings -display-map -n 2 -npernode 1 '
        '--use-hwthread-cpus -host 10.0.0.1:slots=2,10.0.0.2:slots=2 '
        f'{mpi_dir}/startup/osu_hello')

  @flagsaver.flagsaver(
      omb_iterations=10,
      # For OpenMPI, env and genv are treated the same.
      omb_mpi_env=['OMPI_MCA_btl=self,tcp'],
      omb_mpi_genv=['OMPI_MCA_hwloc_base_binding_policy=core'])
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
        full_cmd=('OMPI_MCA_btl=self,tcp '
                  'OMPI_MCA_hwloc_base_binding_policy=core '
                  'mpirun -x OMPI_MCA_btl '
                  '-x OMPI_MCA_hwloc_base_binding_policy '
                  '-report-bindings -display-map -n 2 -npernode 1 '
                  '--use-hwthread-cpus -host 10.0.0.1:slots=2,10.0.0.2:slots=2 '
                  f'{mpitest_path} --iterations 10'),
        units='MB/s',
        params={'--iterations': 10},
        mpi_vendor='openmpi',
        mpi_version='3.1.2',
        value_column='bandwidth',
        number_processes=2,
        run_time=0,
        pinning=['0:0:0,1', '1:1:0,1'],
        perhost=1,
        mpi_env={
            'OMPI_MCA_btl': 'self,tcp',
            'OMPI_MCA_hwloc_base_binding_policy': 'core',
        })
    self.assertEqual(expected_result, results[0])
    self.assertLen(results, 2)
    # Called twice, the second time with 4*2=8 processes
    self.assertEqual(8, results[1].number_processes)

  @flagsaver.flagsaver(omb_perhost=2)
  @mock.patch.object(omb, '_PathToBenchmark', return_value='/igather')
  def testPerhost(self, mock_benchmark_path):
    del mock_benchmark_path
    vm = MockVm()
    vm.RobustRemoteCommand.return_value = ('', '')
    results = list(omb.RunBenchmark(omb.RunRequest('igather', [vm], 1024)))
    self.assertIn('-npernode 2', results[0].full_cmd)
    self.assertIn('-m 1024:1024', results[1].full_cmd)


if __name__ == '__main__':
  absltest.main()
