"""Tests for cloudharmony_iperf_benchmark."""
import os

import unittest
from absl import flags
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import cloud_harmony_util
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import cloudharmony_iperf_benchmark
from tests import pkb_common_test_case


FLAGS = flags.FLAGS


class CloudharmonyIperfBenchmarkTestCase(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin):

  def setUp(self):
    super(CloudharmonyIperfBenchmarkTestCase, self).setUp()
    self.path = os.path.join(os.path.dirname(__file__),
                             '../data/cloudharmony_iperf.csv')
    with open(self.path, 'r') as fp:
      self.iperf_csv_text = fp.read()
      self.cloud_harmony_metadata = cloud_harmony_util.ParseCsvResultsFromString(
          self.iperf_csv_text)

  def testRun(self):
    FLAGS.run_uri = '12345678'
    FLAGS.zone = ['us-central1-a']
    remote_command = mock.PropertyMock(return_value=(self.iperf_csv_text, ''))
    client = mock.Mock(RemoteCommand=remote_command,
                       machine_type='n2-standard-2')
    server = mock.Mock(internal_ip='10.0.0.1',
                       machine_type='n2-standard-4')
    benchmark_module = mock.Mock(BENCHMARK_NAME='cloud_harmony_iperf')
    benchmark_config = mock.Mock(vm_groups={},
                                 relational_db=mock.Mock(vm_groups={}))
    spec = benchmark_spec.BenchmarkSpec(benchmark_module, benchmark_config,
                                        'abcdefg')
    spec.vm_groups = {'client': [client], 'server': [server]}
    results = cloudharmony_iperf_benchmark._Run(spec)
    client.RobustRemoteCommand.assert_called_with(
        'iperf/run.sh  '
        f'--iperf_server 10.0.0.1:{cloudharmony_iperf_benchmark._PORT} '
        '--meta_compute_service Google Compute Engine '
        '--meta_compute_service_id google:compute '
        '--meta_instance_id n2-standard-2 '
        '--meta_provider Google Cloud Platform '
        '--meta_provider_id google '
        '--meta_region us-central1 '
        '--meta_zone us-central1-a '
        '--meta_test_id 12345678 '
        '--iperf_server_instance_id n2-standard-4 '
        '--iperf_server_region us-central1-a '
        '--iperf_time 10 '
        '--iperf_warmup 0 '
        '--iperf_test TCP '
        '--iperf_parallel 1 '
        '--tcp_bw_file /tmp/tcpbw '
        '--wkhtml_xvfb '
        '--verbose')
    self.assertLen(results, 2)

    expected_gartner_metadata = [{
        'bandwidth_direction': 'up',
        'bandwidth_max': 9894.384,
        'bandwidth_mean': 9735.5,
        'bandwidth_median': 9888.76,
        'bandwidth_min': 8349.13,
        'bandwidth_p10': 8349.13,
        'bandwidth_p25': 9888.001,
        'bandwidth_p75': 9889.2,
        'bandwidth_p90': 9889.8,
        'bandwidth_stdev': 487,
        'benchmark_version': 1.0,
        'collectd_rrd': '',
        'cpu_client': 34.346987,
        'cpu_server': 2.880331,
        'iperf_bandwidth': '',
        'iperf_cmd': 'iperf3 -c 10.240.0.14 -J -i 1',
        'iperf_concurrency': 1,
        'iperf_interval': 1,
        'iperf_len': '',
        'iperf_mss': '',
        'iperf_nodelay': '',
        'iperf_num': '',
        'iperf_parallel': 1,
        'iperf_reverse': '',
        'iperf_server': '10.240.0.14',
        'iperf_server_instance_id': 'n1-standard-2',
        'iperf_server_os': 'Ubuntu 18.04.5 LTS',
        'iperf_server_provider': '',
        'iperf_server_provider_id': 'google',
        'iperf_server_region': 'us-central1',
        'iperf_server_service': '',
        'iperf_server_service_id': 'google:compute',
        'iperf_time': 10,
        'iperf_tos': '',
        'iperf_udp': '',
        'iperf_version': '3.1.3',
        'iperf_warmup': 0,
        'iperf_window': '',
        'iperf_zerocopy': '',
        'iteration': 1,
        'jitter_max': '',
        'jitter_mean': '',
        'jitter_median': '',
        'jitter_min': '',
        'jitter_p10': '',
        'jitter_p25': '',
        'jitter_p75': '',
        'jitter_p90': '',
        'jitter_stdev': '',
        'loss_max': '',
        'loss_mean': '',
        'loss_median': '',
        'loss_min': '',
        'loss_p10': '',
        'loss_p25': '',
        'loss_p75': '',
        'loss_p90': '',
        'loss_stdev': '',
        'meta_compute_service': '',
        'meta_compute_service_id': 'google:compute',
        'meta_cpu': 'Intel Xeon 2.30GHz',
        'meta_cpu_cache': '46080 KB',
        'meta_cpu_cores': 2,
        'meta_cpu_speed': 2300.0,
        'meta_hostname': 'pkb-3b246db4-0',
        'meta_instance_id': 'n1-standard-2',
        'meta_memory': '7 GB',
        'meta_memory_gb': 7,
        'meta_memory_mb': 7457,
        'meta_os': 'Ubuntu 18.04.5 LTS',
        'meta_provider': '',
        'meta_provider_id': 'google',
        'meta_region': 'us-central1',
        'meta_resource_id': '',
        'meta_run_group_id': '',
        'meta_run_id': '',
        'meta_test_id': '3b246db4',
        'report_pdf': '',
        'report_zip': '',
        'same_instance_id': 1,
        'same_os': 1,
        'same_provider': 1,
        'same_region': 1,
        'same_service': 1,
        'test_started': '2020-10-30 00:33:51',
        'test_stopped': '2020-10-30 00:34:01',
        'transfer': 11605.6
    }, {
        'bandwidth_direction': 'down',
        'bandwidth_max': 9910.723,
        'bandwidth_mean': 9730.44,
        'bandwidth_median': 9866.29,
        'bandwidth_min': 8349.13,
        'bandwidth_p10': 8349.13,
        'bandwidth_p25': 9888.001,
        'bandwidth_p75': 9889.76,
        'bandwidth_p90': 9889.8,
        'bandwidth_stdev': 487,
        'benchmark_version': 1.0,
        'collectd_rrd': '',
        'cpu_client': 34.346987,
        'cpu_server': 2.880331,
        'iperf_bandwidth': '',
        'iperf_cmd': 'iperf3 -c 10.240.0.14 -J -i 1',
        'iperf_concurrency': 1,
        'iperf_interval': 1,
        'iperf_len': '',
        'iperf_mss': '',
        'iperf_nodelay': '',
        'iperf_num': '',
        'iperf_parallel': 1,
        'iperf_reverse': '',
        'iperf_server': '10.240.0.14',
        'iperf_server_instance_id': 'n1-standard-2',
        'iperf_server_os': 'Ubuntu 18.04.5 LTS',
        'iperf_server_provider': '',
        'iperf_server_provider_id': 'google',
        'iperf_server_region': 'us-central1',
        'iperf_server_service': '',
        'iperf_server_service_id': 'google:compute',
        'iperf_time': 10,
        'iperf_tos': '',
        'iperf_udp': '',
        'iperf_version': '3.1.3',
        'iperf_warmup': 0,
        'iperf_window': '',
        'iperf_zerocopy': '',
        'iteration': 1,
        'jitter_max': '',
        'jitter_mean': '',
        'jitter_median': '',
        'jitter_min': '',
        'jitter_p10': '',
        'jitter_p25': '',
        'jitter_p75': '',
        'jitter_p90': '',
        'jitter_stdev': '',
        'loss_max': '',
        'loss_mean': '',
        'loss_median': '',
        'loss_min': '',
        'loss_p10': '',
        'loss_p25': '',
        'loss_p75': '',
        'loss_p90': '',
        'loss_stdev': '',
        'meta_compute_service': '',
        'meta_compute_service_id': 'google:compute',
        'meta_cpu': 'Intel Xeon 2.30GHz',
        'meta_cpu_cache': '46080 KB',
        'meta_cpu_cores': 2,
        'meta_cpu_speed': 2300.0,
        'meta_hostname': 'pkb-4c137ef3-0',
        'meta_instance_id': 'n1-standard-2',
        'meta_memory': '7 GB',
        'meta_memory_gb': 7,
        'meta_memory_mb': 7457,
        'meta_os': 'Ubuntu 18.04.5 LTS',
        'meta_provider': '',
        'meta_provider_id': 'google',
        'meta_region': 'us-central1',
        'meta_resource_id': '',
        'meta_run_group_id': '',
        'meta_run_id': '',
        'meta_test_id': '4c137ef3',
        'report_pdf': '',
        'report_zip': '',
        'same_instance_id': 1,
        'same_os': 1,
        'same_provider': 1,
        'same_region': 1,
        'same_service': 1,
        'test_started': '2021-02-25 4:20:12',
        'test_stopped': '2021-02-25 4:22:01',
        'transfer': 11605.5
    }]

    self.assertListEqual(expected_gartner_metadata, results)


if __name__ == '__main__':
  unittest.main()
