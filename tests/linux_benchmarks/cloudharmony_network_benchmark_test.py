"""Tests for cloudharmony_network_benchmark."""
import os
import unittest

from absl import flags
import mock
from perfkitbenchmarker import benchmark_spec
from perfkitbenchmarker import cloud_harmony_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import cloudharmony_network_benchmark
from tests import pkb_common_test_case

FLAGS = flags.FLAGS


class CloudharmonyNetworkBenchmarkTestCase(
    pkb_common_test_case.PkbCommonTestCase, test_util.SamplesTestMixin):

  def setUp(self):
    super(CloudharmonyNetworkBenchmarkTestCase, self).setUp()
    self.path = os.path.join(os.path.dirname(__file__),
                             '../data/cloudharmony_network.csv')
    with open(self.path, 'r') as fp:
      self.network_csv_text = fp.read()
      self.cloud_harmony_metadata = cloud_harmony_util.ParseCsvResultsFromString(
          self.network_csv_text)

  def testParseOutput(self):
    results = cloudharmony_network_benchmark.ParseOutput(
        self.cloud_harmony_metadata)
    self.assertLen(results, 2)

    expected_gartner_metadata = [{
        'benchmark_version': 1.0,
        'collectd_rrd': '',
        'dns_recursive': '',
        'dns_servers': '',
        'iteration': 1,
        'meta_compute_service': 'Google Compute Engine',
        'meta_compute_service_id': 'google:compute',
        'meta_cpu': 'Intel Xeon CPU',
        'meta_cpu_cores': 4,
        'meta_geo_region': '',
        'meta_hostname': 'pkb-1348d630-0',
        'meta_instance_id': 'n2-standard-4',
        'meta_location': '',
        'meta_location_country': '',
        'meta_location_state': '',
        'meta_memory': '16 GB',
        'meta_memory_gb': 16,
        'meta_memory_mb': 16012,
        'meta_os_info': 'Ubuntu 18.04.3 LTS',
        'meta_provider': 'Google Cloud Platform',
        'meta_provider_id': 'google',
        'meta_region': 'europe-west4',
        'meta_resource_id': '',
        'meta_run_id': '',
        'meta_test_id': '',
        'metric': 0.164,
        'metric_10': 0.187,
        'metric_25': 0.174,
        'metric_75': 0.159,
        'metric_90': 0.154,
        'metric_fastest': 0.145,
        'metric_max': 1.05,
        'metric_mean': 0.1767,
        'metric_min': 0.145,
        'metric_rstdev': 54.561,
        'metric_slowest': 1.05,
        'metric_stdev': 0.0895,
        'metric_sum': 17.674,
        'metric_sum_squares': 3.9164,
        'metric_timed': '',
        'metric_unit': 'ms',
        'metric_unit_long': 'milliseconds',
        'metrics':
            '1.05|0.178|0.172|0.164|0.164|0.179|0.18|0.184|0.161|0.177|0.169|'
            '0.167|0.203|0.153|0.164|0.157|0.18|0.159|0.163|0.155|0.161|0.167|'
            '0.157|0.159|0.156|0.174|0.163|0.166|0.187|0.18|0.174|0.174|0.191|'
            '0.165|0.154|0.156|0.159|0.161|0.154|0.164|0.168|0.16|0.167|0.16|'
            '0.169|0.172|0.195|0.185|0.176|0.176|0.171|0.162|0.171|0.151|0.154|'
            '0.223|0.161|0.231|0.213|0.165|0.169|0.212|0.154|0.172|0.159|0.156|'
            '0.158|0.164|0.164|0.172|0.156|0.174|0.168|0.169|0.149|0.152|0.16|'
            '0.159|0.151|0.178|0.159|0.158|0.158|0.163|0.165|0.16|0.187|0.182|'
            '0.15|0.145|0.177|0.153|0.156|0.16|0.159|0.162|0.161|0.164|0.168|'
            '0.16',
        'samples': 100,
        'status': 'success',
        'tcp_file': '',
        'test': 'latency',
        'test_endpoint': '10.240.0.86',
        'test_geo_region': '',
        'test_instance_id': 'n2-standard-4',
        'test_ip': '10.240.0.86',
        'test_location': '',
        'test_location_country': '',
        'test_location_state': '',
        'test_private_endpoint': '',
        'test_private_network_type': '',
        'test_provider': 'Google Cloud Platform',
        'test_provider_id': 'google',
        'test_region': 'europe-west4',
        'test_service': 'Google Compute Engine',
        'test_service_id': 'google:compute',
        'test_service_type': 'compute',
        'test_started': '2020-09-28 23:29:09',
        'test_stopped': '2020-09-28 23:29:29',
        'tests_failed': 0,
        'tests_success': 100,
        'throughput_custom_cmd': '',
        'throughput_https': '',
        'throughput_keepalive': '',
        'throughput_size': '',
        'throughput_small_file': '',
        'throughput_threads': '',
        'throughput_time': '',
        'throughput_transfer': '',
        'timeout': 3,
        'traceroute': '',
        'server_type': 'nginx',
    }, {
        'benchmark_version': 1.0,
        'collectd_rrd': '',
        'dns_recursive': '',
        'dns_servers': '',
        'iteration': 1,
        'meta_compute_service': 'Google Compute Engine',
        'meta_compute_service_id': 'google:compute',
        'meta_cpu': 'Intel Xeon CPU',
        'meta_cpu_cores': 4,
        'meta_geo_region': '',
        'meta_hostname': 'pkb-4c137ef3-0',
        'meta_instance_id': 'n2-standard-8',
        'meta_location': '',
        'meta_location_country': '',
        'meta_location_state': '',
        'meta_memory': '16 GB',
        'meta_memory_gb': 16,
        'meta_memory_mb': 16012,
        'meta_os_info': 'Ubuntu 18.04.3 LTS',
        'meta_provider': 'Google Cloud Platform',
        'meta_provider_id': 'google',
        'meta_region': 'europe-west4',
        'meta_resource_id': '',
        'meta_run_id': '',
        'meta_test_id': '',
        'metric': 0.164,
        'metric_10': 0.187,
        'metric_25': 0.174,
        'metric_75': 0.159,
        'metric_90': 0.154,
        'metric_fastest': 0.145,
        'metric_max': 1.05,
        'metric_mean': 0.1767,
        'metric_min': 0.145,
        'metric_rstdev': 54.561,
        'metric_slowest': 1.05,
        'metric_stdev': 0.0895,
        'metric_sum': 17.674,
        'metric_sum_squares': 3.9164,
        'metric_timed': '',
        'metric_unit': 'ms',
        'metric_unit_long': 'milliseconds',
        'metrics':
            '1.05|0.178|0.172|0.164|0.164|0.179|0.18|0.184|0.161|0.177|0.169|'
            '0.167|0.203|0.153|0.164|0.157|0.18|0.159|0.163|0.155|0.161|0.167|'
            '0.157|0.159|0.156|0.174|0.163|0.166|0.187|0.18|0.174|0.174|0.191|'
            '0.165|0.154|0.156|0.159|0.161|0.154|0.164|0.168|0.16|0.167|0.16|'
            '0.169|0.172|0.195|0.185|0.176|0.176|0.171|0.162|0.171|0.151|0.154|'
            '0.223|0.161|0.231|0.213|0.165|0.169|0.212|0.154|0.172|0.159|0.156|'
            '0.158|0.164|0.164|0.172|0.156|0.174|0.168|0.169|0.149|0.152|0.16|'
            '0.159|0.151|0.178|0.159|0.158|0.158|0.163|0.165|0.16|0.187|0.182|'
            '0.15|0.145|0.177|0.153|0.156|0.16|0.159|0.162|0.161|0.164|0.168|'
            '0.16',
        'samples': 100,
        'status': 'success',
        'tcp_file': '',
        'test': 'latency',
        'test_endpoint': '10.240.0.87',
        'test_geo_region': '',
        'test_instance_id': 'n2-standard-8',
        'test_ip': '10.240.0.87',
        'test_location': '',
        'test_location_country': '',
        'test_location_state': '',
        'test_private_endpoint': '',
        'test_private_network_type': '',
        'test_provider': 'Google Cloud Platform',
        'test_provider_id': 'google',
        'test_region': 'europe-west4',
        'test_service': 'Google Compute Engine',
        'test_service_id': 'google:compute',
        'test_service_type': 'compute',
        'test_started': '2021-02-25 4:20:12',
        'test_stopped': '2021-02-25 4:22:01',
        'tests_failed': 0,
        'tests_success': 100,
        'throughput_custom_cmd': '',
        'throughput_https': '',
        'throughput_keepalive': '',
        'throughput_size': '',
        'throughput_small_file': '',
        'throughput_threads': '',
        'throughput_time': '',
        'throughput_transfer': '',
        'timeout': 3,
        'traceroute': '',
        'server_type': 'nginx',
    }]
    expected_gartner_samples = []
    for result in expected_gartner_metadata:
      expected_gartner_samples.append(
          sample.Sample(
              metric='cloudharmony_output', value='', unit='',
              metadata=result))

    self.assertEqual(len(expected_gartner_samples), len(results))
    for index in range(len(results)):
      self.assertSamplesEqualUpToTimestamp(expected_gartner_samples[index],
                                           results[index])

  def testRun(self):
    FLAGS.run_uri = '12345678'
    FLAGS.zone = ['us-east1-a']
    remote_command = mock.PropertyMock(return_value=(self.network_csv_text, ''))
    client = mock.Mock(RemoteCommand=remote_command,
                       machine_type='n2-standard-2',
                       zone='us-east1-a',
                       os_info='Ubuntu 18.04.5 LTS')
    server1 = mock.Mock(internal_ip='10.0.0.1', machine_type='n2-standard-4',
                        zone='us-west1-a')
    server2 = mock.Mock(internal_ip='10.0.0.2', machine_type='n2-standard-4',
                        zone='us-west1-a')
    benchmark_module = mock.Mock(BENCHMARK_NAME='cloud_harmony_network')
    benchmark_config = mock.Mock(
        vm_groups={},
        relational_db=mock.Mock(vm_groups={})
    )
    spec = benchmark_spec.BenchmarkSpec(benchmark_module, benchmark_config,
                                        'abcdefg')
    spec.vm_groups = {
        'client': [client],
        'server': [server1, server2]
    }
    cloudharmony_network_benchmark.Run(spec)
    client.RobustRemoteCommand.assert_called_with(
        'sudo /opt/pkb/network/run.sh --test_endpoint=10.0.0.1 '
        '--test_endpoint=10.0.0.2 --meta_compute_service Google Compute Engine '
        '--meta_compute_service_id google:compute '
        '--meta_instance_id n2-standard-2 '
        '--meta_provider Google Cloud Platform --meta_provider_id google '
        '--meta_region us-east1 --meta_zone us-east1-a --meta_test_id 12345678 '
        '--test_service_type compute --test latency --throughput_size 5.0 '
        '--throughput_threads 2 --throughput_samples 5 --tcp_samples 10 '
        '--test_instance_id n2-standard-4 --test_region us-west1 '
        '--output=/tmp/pkb --verbose')


if __name__ == '__main__':
  unittest.main()
