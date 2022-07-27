# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for apachebench_benchmark."""

import collections
import os
import unittest

from absl import flags
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker.linux_benchmarks import apachebench_benchmark
from perfkitbenchmarker.linux_packages import apache2_server
from tests import pkb_common_test_case

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()


class RunTest(pkb_common_test_case.PkbCommonTestCase):

  def populateTestString(self, filename):
    path = os.path.join(os.path.dirname(__file__), '..', 'data', filename)
    output = ''

    with open(path) as fp:
      output = fp.read()

    return output

  def setUp(self):
    super().setUp()

    self.apachebench_output = self.populateTestString('apachebench_output.txt')
    self.apachebench_raw_request_times = self.populateTestString(
        'apachebench_raw_request_times.tsv')

    self.run_config = apachebench_benchmark.ApacheBenchRunConfig(
        num_requests=1,
        concurrency=1,
        keep_alive=True,
        http_method='GET',
        socket_timeout=30,
        timelimit=None,
        server_content_size=1,
        client_vms=1
    )
    self.ip_config = apachebench_benchmark.ApacheBenchIpConfig(
        'internal-ip',
        'internal_ip',
        'internal_results.txt',
        'internal_ip_raw_request_times.tsv')

    client = mock.MagicMock(
        hostname='pkb-mock-0',
        id='0123456789876543210')
    server = mock.MagicMock(
        hostname='pkb-mock-1',
        id='9876543210123456789',
        internal_ip='30.128.0.2')
    self.vm_spec = mock.MagicMock(spec=benchmark_spec.BenchmarkSpec)
    setattr(self.vm_spec, 'vm_groups', {'client': [client], 'server': [server]})

    self.expected_histogram = collections.OrderedDict()
    self.expected_histogram[4] = 1
    self.expected_histogram[5] = 3
    self.expected_histogram[6] = 1
    self.expected_histogram[7] = 3

  def testApacheBenchRunConfigGetCommand(self):
    server = self.vm_spec.vm_groups['server'][0]
    cmd = self.run_config.GetCommand(self.ip_config, server)

    self.assertIn('-k', cmd)
    self.assertNotIn('-t', cmd)
    self.assertIn(f'-n {self.run_config.num_requests}', cmd)
    self.assertIn(f'-c {self.run_config.concurrency}', cmd)
    self.assertIn(f'-m {self.run_config.http_method}', cmd)
    self.assertIn(f'-s {self.run_config.socket_timeout}', cmd)
    self.assertIn(f'-g {self.ip_config.raw_request_data_path}', cmd)
    self.assertIn(f'http://{server.internal_ip}:80/', cmd)
    self.assertIn(f'1> {self.ip_config.output_path}', cmd)

  @mock.patch.multiple(
      apache2_server, SetupServer=mock.DEFAULT, StartServer=mock.DEFAULT)
  def testPrepare(self, SetupServer, StartServer):  # pylint: disable=invalid-name
    client = self.vm_spec.vm_groups['client'][0]
    server = self.vm_spec.vm_groups['server'][0]
    FLAGS.apachebench_server_content_size = 1

    apachebench_benchmark.Prepare(self.vm_spec)

    client.Install.assert_called_once_with('apache2_utils')
    server.Install.assert_called_once_with('apache2_server')
    server.AllowPort.assert_called_once_with(apachebench_benchmark._PORT)
    SetupServer.assert_called_once_with(server, 1)
    StartServer.assert_called_once_with(server)

  @mock.patch.object(apache2_server, 'GetApacheCPUSeconds', autospec=True)
  @mock.patch.object(
      apachebench_benchmark, '_ParseHistogramFromFile', autospec=True)
  def testApacheBench_Run(self, _ParseHistogramFromFile, GetApacheCPUSeconds):  # pylint: disable=invalid-name
    server = self.vm_spec.vm_groups['server'][0]
    clients = self.vm_spec.vm_groups['client']
    for client in clients:
      client.RemoteCommand.side_effect = [
          None,
          (self.apachebench_output, ''),
          (self.apachebench_raw_request_times, '')
      ]

    GetApacheCPUSeconds.return_value = 1.0
    _ParseHistogramFromFile.return_value = self.expected_histogram

    result = apachebench_benchmark._Run(clients, server, self.run_config,
                                        self.ip_config)

    expected_attrs = {
        'complete_requests': 1,
        'failed_requests': 0,
        'requests_per_second': 1.0,
        'requests_per_second_unit': '#/sec',
        'time_per_request': 1.0,
        'time_per_request_unit': 'ms',
        'time_per_request_concurrent': 1.0,
        'time_per_request_concurrent_unit': 'ms',
        'transfer_rate': 1.0,
        'transfer_rate_unit': 'Kbytes/sec',
        'time_taken_for_tests': 1.0,
        'time_taken_for_tests_unit': 'seconds',
        'total_transferred': 1,
        'total_transferred_unit': 'bytes',
        'html_transferred': 1,
        'html_transferred_unit': 'bytes',
        'histogram': self.expected_histogram,
        'cpu_seconds': 1.0
    }

    for metric, expected in expected_attrs.items():
      self.assertEqual(getattr(result, metric), expected)

  def testApacheBenchGetMetadata(self):
    clients = self.vm_spec.vm_groups['client']
    for client in clients:
      client.RemoteCommand.return_value = (self.apachebench_output, '')

    FLAGS.apachebench_run_mode = apachebench_benchmark.ApacheBenchRunMode.MAX_THROUGHPUT

    result = apachebench_benchmark.ApacheBenchResults(
        complete_requests=1,
        failed_requests=0,
        requests_per_second=1.0,
        requests_per_second_unit='#/sec',
        time_per_request=1.0,
        time_per_request_unit='ms',
        time_per_request_concurrent=1.0,
        time_per_request_concurrent_unit='ms',
        transfer_rate=1.0,
        transfer_rate_unit='Kbytes/sec',
        time_taken_for_tests=1.0,
        time_taken_for_tests_unit='seconds',
        total_transferred=1,
        total_transferred_unit='bytes',
        html_transferred=1,
        html_transferred_unit='bytes',
        histogram=self.expected_histogram,
        raw_results=['1'],
        cpu_seconds=1.0
    )
    metadata = apachebench_benchmark.GetMetadata(result, self.run_config,
                                                 self.ip_config)

    expected_metadata = {
        'apachebench_requests': 1,
        'apachebench_concurrency_level': 1,
        'apachebench_keep_alive': True,
        'apachebench_http_method': 'GET',
        'apachebench_socket_timeout': 30,
        'apachebench_timelimit': None,
        'apachebench_server_content_size': 1,
        'apachebench_ip_type': 'internal-ip',
        'apachebench_client_vms': 1,
        'apachebench_complete_requests': 1,
        'apachebench_time_taken_for_tests': 1.0,
        'apachebench_run_mode':
            apachebench_benchmark.ApacheBenchRunMode.MAX_THROUGHPUT
    }

    self.assertDictEqual(metadata, expected_metadata)

  def testParseHistogramFromFile(self):
    client = self.vm_spec.vm_groups['client'][0]
    client.RemoteCommand.return_value = (self.apachebench_raw_request_times, '')
    result = apachebench_benchmark._ParseHistogramFromFile(client, 'path')

    self.assertDictEqual(result, self.expected_histogram)


if __name__ == '__main__':
  unittest.main()
