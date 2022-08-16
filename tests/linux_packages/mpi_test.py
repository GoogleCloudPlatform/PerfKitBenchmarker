"""Tests for MPI benchmark."""

import json
import os
from typing import Any, Dict, List, Union
import unittest
from unittest import mock
import uuid
from absl import flags
from absl.testing import parameterized
from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_packages import intelmpi
from perfkitbenchmarker.linux_packages import mpi
from perfkitbenchmarker.linux_packages import omb
from tests import pkb_common_test_case

FLAGS = flags.FLAGS

_TEST_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'mpi')
MPI_VARS = '/opt/intel/compilers_and_libraries/linux/mpi/intel64/bin/mpivars.sh'
# all "mpirun" commands start with this
RUN_PREFIX = f'. {MPI_VARS};'


def FilePath(file_name: str) -> str:
  return os.path.join(_TEST_DIR, file_name)


def ReadMpiOutput(file_name: str) -> str:
  with open(FilePath(file_name)) as reader:
    return reader.read()


def ReadJson(file_name: str) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
  with open(FilePath(file_name)) as reader:
    return json.load(reader)


def _CreateMpiDataFromDict(data: Dict[str, Any]) -> mpi.MpiData:
  if 'error' in data:
    return mpi.MpiData(is_error=True, bytes=data['bytes'])
  else:
    number_bytes = data.pop('bytes', None)
    repetitions = data.pop('repetitions', None)
    return mpi.MpiData(
        bytes=number_bytes, repetitions=repetitions, data=data['data'])


def _CreateMpiResultsFromDict(result_json: Dict[str, Any]) -> mpi.MpiResult:
  mpi_datas = [
      _CreateMpiDataFromDict(mpi_data) for mpi_data in result_json['data']
  ]
  result_json['data'] = mpi_datas
  if result_json.get('group_layout'):
    # Convert json-serialized group number from string to an int
    result_json['group_layout'] = {
        int(key): value for key, value in result_json['group_layout'].items()
    }
  return mpi.MpiResult(**result_json)


def _CreateMpiResponseFromDict(
    data: List[Dict[str, Any]]) -> List[mpi.MpiResult]:
  return [_CreateMpiResultsFromDict(result) for result in data]


def ReadParsedOutput(file_name: str) -> List[mpi.MpiResult]:
  return _CreateMpiResponseFromDict(ReadJson(file_name))


def _MockVm(ip: str) -> mock.Mock:
  vm = mock.Mock(internal_ip=ip)
  vm.NumCpusForBenchmark.return_value = 8
  return vm


class MpiTestCase(pkb_common_test_case.PkbCommonTestCase):
  MPI_VERSION = '2019.2-057'
  # Lines from the -dumpfile latency file.  Format is (bytes, latency usec).
  LATENCY_DATA_FILE = (
      '0 1.123',
      '0 2.9999',
      '0 42.3',
      '1024 2.0',
      '1024 3.0',
      '1024 3.0',
  )

  # The latency_data_file summarized in a Dict.
  LATENCY_DATA: Dict[int, Dict[float, int]] = {
      0: {
          1.12: 1,
          3.0: 1,
          42.0: 1
      },
      1024: {
          2.0: 1,
          3.0: 2
      }
  }

  def setUp(self):
    super(MpiTestCase, self).setUp()
    FLAGS.intelmpi_version = self.MPI_VERSION
    self.enter_context(
        mock.patch.object(intelmpi, 'MpiVars', return_value=MPI_VARS))

  @parameterized.parameters(
      # mpirun -n 120 -hosts a,b,c,d -ppn 1 mpi-benchmarks/....
      ('mpi_allgather_output.txt', 'mpi_allgather_parsed.json'),
      ('mpi_barrier_output.txt', 'mpi_barrier_parsed.json'),
      ('mpi_pingpong_output.txt', 'mpi_pingpong_parsed.json'),
      ('mpi_reduce_output.txt', 'mpi_reduce_parsed.json'),
      ('mpi_latencies_output.txt', 'mpi_latencies_parsed.json'),
      ('mpi_one_put_all_output.txt', 'mpi_one_put_all_parsed.json'),
  )
  def testParseMpiOutput(self, mpi_output_file: str,
                         mpi_parsed_file: str) -> None:
    found = list(
        mpi.MpiResultParser(ReadMpiOutput(mpi_output_file).splitlines()))
    expected = ReadParsedOutput(mpi_parsed_file)
    self.assertEqual(expected, found)

  def testVerifyInstall(self) -> None:
    vms = [_MockVm(ip) for ip in ('a', 'b')]
    vms[0].RobustRemoteCommand.return_value = '', ''
    mpi.VerifyInstall(vms)
    mpirun_cmd = ('mpirun -n 8 -hosts a,b -ppn 8 mpi-benchmarks/IMB-MPI1 '
                  '-msglog 10:11 -multi 0 -time 20 -off_cache -1 -iter 100 '
                  '-iter_policy off -zero_size off -show_tail yes PingPong')
    vms[0].RobustRemoteCommand.assert_called_with(RUN_PREFIX + ' ' + mpirun_cmd)

  def _CreateMpiRequest(self,
                        record_latencies: bool,
                        iterations: int = 100000) -> mpi.MpiRequest:
    return mpi.MpiRequest(
        vms=[_MockVm('a'), _MockVm('b')],
        total_processes=10,
        ppn=0,
        suite='IMB-MPI1',
        tests=['PingPong'],
        msglog_min=10,
        msglog_max=11,
        timeout=20,
        off_cache_size=-1,
        off_cache_line_size=None,
        iterations=iterations,
        include_zero_byte=False,
        compile_from_source=True,
        record_latencies=record_latencies,
        multi=True)

  def testRunMpiStats(self) -> None:
    vm = _MockVm('a')
    vm.RobustRemoteCommand.return_value = ReadMpiOutput(
        'mpi_pingpong_output.txt'), ''
    request = self._CreateMpiRequest(False)
    response = mpi.RunMpiStats(vm, request)
    self.assertEqual(RUN_PREFIX + ' mpirun -n 10 -hosts a,b', response.mpi_run)
    self.assertEqual('intel', response.vendor)
    self.assertEqual('2019.2-057', response.version)
    # fully tested in testParseFiles
    self.assertLen(response.results, 1)
    expected_args = ('mpi-benchmarks/IMB-MPI1 -msglog 10:11 -multi 0 -time 20 '
                     '-off_cache -1 -iter 100000 -iter_policy off '
                     '-zero_size off -show_tail yes -map 5x2 PingPong')
    self.assertEqual(expected_args, response.args)

  @mock.patch.object(mpi, '_GroupLatencyLines')
  @mock.patch.object(uuid, 'uuid4', side_effect=[mock.PropertyMock(hex='abc')])
  def testRunMpiStatsLatencyFile(self, mock_uuid: mock.Mock,
                                 mock_create_histo: mock.Mock) -> None:
    mock_create_histo.return_value = [[
        '1024 10.0', '1024 11.0', '2048 11.10', '2048 11.11'
    ]]
    vm = _MockVm('a')
    vm.RobustRemoteCommand.return_value = (
        ReadMpiOutput('mpi_barrier_output.txt'), '')
    request = self._CreateMpiRequest(True, 2)
    response = mpi.RunMpiStats(vm, request)
    # has the -show_tail and -dumpfile flags set
    expected_args_re = (r'.*-zero_size off -show_tail yes '
                        r'-dumpfile /tmp/latency\S+ -map 5x2 PingPong$')
    self.assertRegex(response.args, expected_args_re)
    mock_create_histo.assert_called_with(vm, '/tmp/latency-PingPong-abc.txt', 2)

  @mock.patch('builtins.open',
              mock.mock_open(read_data='\n'.join(LATENCY_DATA_FILE)))
  def testGroupLatencyLines(self):
    vm = mock.Mock()
    vm.TryRemoteCommand.return_value = True
    expected_group1 = ['0 1.123', '0 2.9999', '0 42.3']
    expected_group2 = ['1024 2.0', '1024 3.0', '1024 3.0']
    lines = mpi._GroupLatencyLines(vm, '/tmp/remote.txt', 3)
    self.assertEqual([expected_group1, expected_group2], lines)
    vm.TryRemoteCommand.assert_called_with('test -f /tmp/remote.txt')

  def testGroupLatencyLinesMissingFile(self):
    # method returns an empty list if check for remote latency file fails
    vm = mock.Mock()
    vm.TryRemoteCommand.return_value = False
    lines = mpi._GroupLatencyLines(vm, '/tmp/remote.txt', 3)
    self.assertEmpty(lines)

  def testCreateMpiDataForHistogram(self) -> None:
    FLAGS.run_uri = '12345678'
    grouped_lines = [['1024 10.0', '1024 11.0', '2048 11.10', '2048 11.11']]
    mpi_data1 = mpi.MpiData(
        bytes=1024, repetitions=2, data={'p50': 10.5}, is_error=False)
    mpi_data2 = mpi.MpiData(
        bytes=2048, repetitions=2, data={'p50': 11.0}, is_error=False)
    parsed_results = [
        mpi.MpiResult(benchmark='PingPong', data=[mpi_data1, mpi_data2])
    ]
    self.assertIsNone(parsed_results[0].data[0].histogram)
    self.assertIsNone(parsed_results[0].data[1].histogram)
    mpi._CreateMpiDataForHistogram(grouped_lines, parsed_results)
    # number of results did not change -- added "histogram=" entry to it
    self.assertLen(parsed_results, 1)
    self.assertEqual({10.0: 1, 11.0: 1}, parsed_results[0].data[0].histogram)
    self.assertEqual({11.1: 2}, parsed_results[0].data[1].histogram)

  def testCreateMpiDataForHistogramNoParsedResults(self) -> None:
    # No parsed results -> no histograms are parsed
    FLAGS.run_uri = '12345678'
    grouped_lines = [['1024 10.0', '1024 11.0', '2048 11.10', '2048 11.11']]
    parsed_results = []
    self.assertLen(parsed_results, 0)
    mpi._CreateMpiDataForHistogram(grouped_lines, parsed_results)
    self.assertLen(parsed_results, 0)

  def testRunMpiStatsWithException(self) -> None:
    request = self._CreateMpiRequest(False)
    vm = request.vms[0]
    vm.RobustRemoteCommand.side_effect = [
        errors.VirtualMachine.RemoteCommandError
    ]
    with self.assertRaises(errors.VirtualMachine.RemoteCommandError):
      mpi.RunMpiStats(vm, request)
    # pytyping thinks that vm.RemoteCommand is a Callable but it is a Mock
    last_command = vm.RemoteCommand.call_args[0][0]  # pytype: disable=attribute-error
    self.assertRegex(last_command, 'tail.*/var/log/')
    vm.RemoteCommand.assert_called_once()  # pytype: disable=attribute-error

  def testParseMpiPinning(self):
    lines = ReadMpiOutput('mpi_debug_output.txt').splitlines()
    # nodes 0 and 1 had the same MPI pinning groups of 0,1,2,3 CPUids
    expected_pinning = ['0:0:0,1,2,3', '1:1:0,1,2,3']

    self.assertEqual(expected_pinning, omb.ParseMpiPinning(lines))

  def testParseMpiEnv(self):
    lines = ReadMpiOutput('mpi_debug_output.txt').splitlines()
    expected_mpi_env = {
        'I_MPI_DEBUG': '5',
        'I_MPI_HYDRA_TOPOLIB': 'hwloc',
        'I_MPI_INTERNAL_MEM_POLICY': 'default',
        'I_MPI_MPIRUN': 'mpirun',
        'I_MPI_ROOT': '/opt/intel/compilers_and_libraries_2020.0.166/linux/mpi'
    }

    self.assertEqual(expected_mpi_env, mpi.ParseMpiEnv(lines))


if __name__ == '__main__':
  unittest.main()
