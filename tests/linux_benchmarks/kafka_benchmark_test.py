# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for kafka_benchmark cluster preparation, execution, and output parsing."""

import time
import unittest
from unittest import mock

from absl.testing import flagsaver
from absl.testing import parameterized
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker.linux_benchmarks import kafka_benchmark
from tests import pkb_common_test_case


class KafkaBenchmarkTestCaseBase(pkb_common_test_case.PkbCommonTestCase):
  """Base test case with common setup for Kafka benchmark tests."""

  def setUp(self):
    super().setUp()
    self.broker_vm = mock.MagicMock(spec=virtual_machine.BaseVirtualMachine)
    self.broker_vm.internal_ip = '10.0.0.1'
    self.broker_vm.NumCpusForBenchmark.return_value = 16

    self.producer_vm = mock.MagicMock(spec=virtual_machine.BaseVirtualMachine)
    self.producer_vm.internal_ip = '10.0.0.2'
    self.producer_vm.NumCpusForBenchmark.return_value = 16

    self.consumer_vm = mock.MagicMock(spec=virtual_machine.BaseVirtualMachine)
    self.consumer_vm.internal_ip = '10.0.0.3'
    self.consumer_vm.NumCpusForBenchmark.return_value = 16

    self.controller_vm = mock.MagicMock(spec=virtual_machine.BaseVirtualMachine)
    self.controller_vm.internal_ip = '10.0.0.4'
    self.controller_vm.NumCpusForBenchmark.return_value = 16

    self.benchmark_spec = mock.MagicMock(spec=bm_spec.BenchmarkSpec)
    self.benchmark_spec.vms = [
        self.broker_vm,
        self.producer_vm,
        self.consumer_vm,
        self.controller_vm,
    ]
    self.benchmark_spec.vm_groups = {
        'broker': [self.broker_vm],
        'producer': [self.producer_vm],
        'consumer': [self.consumer_vm],
        'controller': [self.controller_vm],
    }


class KafkaBenchmarkConfigTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for GetConfig."""

  def testGetConfigDefault(self):
    config = kafka_benchmark.GetConfig({})
    self.assertIn('vm_groups', config)
    for group in ('broker', 'producer', 'consumer', 'controller'):
      self.assertIn(group, config['vm_groups'])

  def testGetConfigOverride(self):
    override = {'description': 'Custom Kafka Description'}
    config = kafka_benchmark.GetConfig(override)
    self.assertEqual(config['description'], 'Custom Kafka Description')


class KafkaBenchmarkInstallTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _InstallKafka."""

  def testInstallKafka(self):
    vm = mock.MagicMock(spec=virtual_machine.BaseVirtualMachine)
    kafka_benchmark._InstallKafka(vm)
    vm.InstallPackages.assert_called_once_with('openjdk-17-jdk')
    vm.Install.assert_has_calls([mock.call('build_tools'), mock.call('pip')])
    fallback_url = (
        f'https://archive.apache.org/dist/kafka/'
        f'{kafka_benchmark.KAFKA_VERSION.split("-")[1]}/'
        f'{kafka_benchmark.KAFKA_TAR}'
    )
    vm.RemoteCommand.assert_has_calls([
        mock.call(
            f'wget {kafka_benchmark.KAFKA_URL} || wget {fallback_url}'
        ),
        mock.call(f'tar -xzf {kafka_benchmark.KAFKA_TAR}'),
    ])


class KafkaBenchmarkPrepareTest(KafkaBenchmarkTestCaseBase):
  """Tests for Prepare."""

  def testPrepareSuccess(self):
    self.controller_vm.RemoteCommand.return_value = ('test-uuid-1234\n', '')
    self.broker_vm.RemoteCommand.return_value = ('', '')

    with mock.patch.object(
        kafka_benchmark, '_InstallKafka'
    ) as mock_install, mock.patch.object(time, 'sleep') as mock_sleep:
      kafka_benchmark.Prepare(self.benchmark_spec)
      mock_install.assert_has_calls(
          [mock.call(vm) for vm in self.benchmark_spec.vms], any_order=True
      )
      self.assertLen(mock_install.call_args_list, len(self.benchmark_spec.vms))
      mock_sleep.assert_has_calls([mock.call(5), mock.call(10)])

    # Verify controller setup commands
    self.controller_vm.RemoteCommand.assert_any_call(
        f'cd {kafka_benchmark.KAFKA_DIR} && bin/kafka-storage.sh random-uuid'
    )
    self.controller_vm.RemoteCommand.assert_any_call(
        f'cd {kafka_benchmark.KAFKA_DIR} && bin/kafka-storage.sh format'
        ' -t test-uuid-1234 -c config/controller.properties'
    )
    self.controller_vm.RemoteCommand.assert_any_call(
        f'cd {kafka_benchmark.KAFKA_DIR} && ulimit -n 100000 &&'
        ' bin/kafka-server-start.sh -daemon config/controller.properties'
    )

    # Verify broker setup commands
    self.broker_vm.RemoteCommand.assert_any_call(
        f'cd {kafka_benchmark.KAFKA_DIR} && bin/kafka-storage.sh format'
        ' -t test-uuid-1234 -c config/broker.properties'
    )
    self.broker_vm.RemoteCommand.assert_any_call(
        f'cd {kafka_benchmark.KAFKA_DIR} && ulimit -n 100000 &&'
        ' bin/kafka-server-start.sh -daemon config/broker.properties'
    )

  @flagsaver.flagsaver(kafka_replication_factor=3)
  def testPrepareWithCustomReplicationFactor(self):
    self.controller_vm.RemoteCommand.return_value = ('test-uuid-1234\n', '')
    self.broker_vm.RemoteCommand.return_value = ('', '')

    with mock.patch.object(
        kafka_benchmark, '_InstallKafka'
    ), mock.patch.object(time, 'sleep'):
      kafka_benchmark.Prepare(self.benchmark_spec)

    # Verify broker properties configuration includes replication factor 3
    broker_properties_write_call = [
        call_args[0][0]
        for call_args in self.broker_vm.RemoteCommand.call_args_list
        if 'config/broker.properties' in call_args[0][0]
        and 'cat <<EOF' in call_args[0][0]
    ]
    self.assertLen(broker_properties_write_call, 1)
    props = broker_properties_write_call[0]
    self.assertIn('offsets.topic.replication.factor=3', props)
    self.assertIn('transaction.state.log.replication.factor=3', props)

  @flagsaver.flagsaver(kafka_file_delete_delay_ms=5000)
  def testPrepareWithCustomFileDeleteDelay(self):
    self.controller_vm.RemoteCommand.return_value = ('test-uuid-1234\n', '')
    self.broker_vm.RemoteCommand.return_value = ('', '')

    with mock.patch.object(
        kafka_benchmark, '_InstallKafka'
    ), mock.patch.object(time, 'sleep'):
      kafka_benchmark.Prepare(self.benchmark_spec)

    broker_properties_write_call = [
        call_args[0][0]
        for call_args in self.broker_vm.RemoteCommand.call_args_list
        if 'config/broker.properties' in call_args[0][0]
        and 'cat <<EOF' in call_args[0][0]
    ]
    self.assertLen(broker_properties_write_call, 1)
    props = broker_properties_write_call[0]
    self.assertIn('file.delete.delay.ms=5000', props)
    self.assertIn('log.segment.delete.delay.ms=5000', props)


class KafkaBenchmarkTopicAndCommandsTest(KafkaBenchmarkTestCaseBase):
  """Tests for _CreateTopic, _RunProducer, and _RunConsumer."""

  @mock.patch.object(time, 'sleep')
  def testCreateTopicDefault(self, mock_sleep):
    kafka_benchmark._CreateTopic(self.broker_vm, '10.0.0.1:9092', 'test-topic')
    self.broker_vm.RemoteCommand.assert_called_once_with(
        f'cd {kafka_benchmark.KAFKA_DIR} && bin/kafka-topics.sh --create'
        ' --topic test-topic --bootstrap-server 10.0.0.1:9092 --partitions=256'
        ' --replication-factor=1 --config min.insync.replicas=1'
        ' --if-not-exists'
    )
    mock_sleep.assert_called_once_with(5)

  @mock.patch.object(time, 'sleep')
  @flagsaver.flagsaver(kafka_num_partitions=64, kafka_replication_factor=3)
  def testCreateTopicCustomFlags(self, mock_sleep):
    kafka_benchmark._CreateTopic(self.broker_vm, '10.0.0.1:9092', 'test-topic')
    self.broker_vm.RemoteCommand.assert_called_once_with(
        f'cd {kafka_benchmark.KAFKA_DIR} && bin/kafka-topics.sh --create'
        ' --topic test-topic --bootstrap-server 10.0.0.1:9092 --partitions=64'
        ' --replication-factor=3 --config min.insync.replicas=3'
        ' --if-not-exists'
    )
    mock_sleep.assert_called_once_with(5)

  def testDeleteTopic(self):
    self.broker_vm.RemoteCommand.side_effect = [
        ('', ''),
        ('other-topic\n', ''),
    ]
    kafka_benchmark._DeleteTopic(self.broker_vm, '10.0.0.1:9092', 'test-topic')
    self.assertLen(self.broker_vm.RemoteCommand.call_args_list, 2)
    self.broker_vm.RemoteCommand.assert_has_calls([
        mock.call(
            f'cd {kafka_benchmark.KAFKA_DIR} && bin/kafka-topics.sh --delete'
            ' --topic test-topic --bootstrap-server 10.0.0.1:9092',
            ignore_failure=True,
        ),
        mock.call(
            f'cd {kafka_benchmark.KAFKA_DIR} && bin/kafka-topics.sh --list'
            ' --bootstrap-server 10.0.0.1:9092',
            ignore_failure=True,
        ),
    ])

  @mock.patch.object(time, 'sleep')
  def testDeleteTopicTimeout(self, mock_sleep):
    self.broker_vm.RemoteCommand.side_effect = [('', '')] + [
        ('test-topic\n', '')
    ] * 10

    with mock.patch.object(kafka_benchmark.logging, 'warning') as mock_warning:
      kafka_benchmark._DeleteTopic(
          self.broker_vm, '10.0.0.1:9092', 'test-topic'
      )

      mock_warning.assert_called_once_with(
          'Topic %s not deleted.', 'test-topic'
      )
      self.assertEqual(mock_sleep.call_count, 10)
      mock_sleep.assert_called_with(5)

  def testRunProducerDefault(self):
    self.producer_vm.RemoteCommand.side_effect = [
        ('', ''),
        ('', ''),
        ('producer-stdout', ''),
    ]
    result = kafka_benchmark._RunProducer(
        self.producer_vm, '10.0.0.1:9092', 'test-topic', 4, 1000
    )
    self.assertEqual(result, 'producer-stdout')
    self.assertLen(self.producer_vm.RemoteCommand.call_args_list, 3)

    # Check 1st call (properties)
    props_call = self.producer_vm.RemoteCommand.call_args_list[0][0][0]
    self.assertIn('batch.size=131072', props_call)
    self.assertIn('acks=all', props_call)
    self.assertIn('compression.type=zstd', props_call)

    # Check 2nd call (execution loop)
    exec_call = self.producer_vm.RemoteCommand.call_args_list[1][0][0]
    self.assertIn('for i in $(seq 1 4); do', exec_call)
    self.assertIn('bin/kafka-producer-perf-test.sh', exec_call)
    self.assertIn('--topic=test-topic', exec_call)
    self.assertIn('--num-records=1000', exec_call)
    self.assertIn('--record-size=1024', exec_call)
    self.assertIn('--bootstrap-server 10.0.0.1:9092', exec_call)
    self.assertIn(
        (
            f'--reporting-interval='
            f'{kafka_benchmark._KAFKA_REPORTING_INTERVAL.value}'
        ),
        exec_call,
    )

    # Check 3rd call (cat logs)
    cat_call = self.producer_vm.RemoteCommand.call_args_list[2][0][0]
    self.assertIn('cat /tmp/producer_*.log', cat_call)

  @flagsaver.flagsaver(kafka_producer_batch_size=65536, kafka_record_size=512)
  def testRunProducerCustomFlags(self):
    self.producer_vm.RemoteCommand.side_effect = [
        ('', ''),
        ('', ''),
        ('output', ''),
    ]
    kafka_benchmark._RunProducer(
        self.producer_vm, '10.0.0.1:9092', 'test-topic', 2, 500
    )
    props_call = self.producer_vm.RemoteCommand.call_args_list[0][0][0]
    exec_call = self.producer_vm.RemoteCommand.call_args_list[1][0][0]
    self.assertIn('batch.size=65536', props_call)
    self.assertIn('--record-size=512', exec_call)

  def testRunConsumerDefault(self):
    self.consumer_vm.RemoteCommand.side_effect = [
        ('', ''),
        ('', ''),
        ('consumer-stdout', ''),
    ]
    result = kafka_benchmark._RunConsumer(
        self.consumer_vm, '10.0.0.1:9092', 'test-topic', 8, 5000
    )
    self.assertEqual(result, 'consumer-stdout')
    self.assertLen(self.consumer_vm.RemoteCommand.call_args_list, 3)

    # Check 1st call (properties)
    props_call = self.consumer_vm.RemoteCommand.call_args_list[0][0][0]
    self.assertIn('auto.offset.reset=earliest', props_call)

    # Check 2nd call (execution loop)
    exec_call = self.consumer_vm.RemoteCommand.call_args_list[1][0][0]
    self.assertIn('for i in $(seq 1 8); do', exec_call)
    self.assertIn('bin/kafka-consumer-perf-test.sh', exec_call)
    self.assertIn('--topic=test-topic', exec_call)
    self.assertIn('--bootstrap-server 10.0.0.1:9092', exec_call)
    self.assertIn('--group pkb-group-t8', exec_call)
    self.assertIn('--num-records=5000', exec_call)
    self.assertIn(
        f'--fetch-size={kafka_benchmark._KAFKA_CONSUMER_FETCH_SIZE.value}',
        exec_call,
    )
    self.assertIn(
        f'--timeout {kafka_benchmark._KAFKA_CONSUMER_TIMEOUT_MS.value}',
        exec_call,
    )
    self.assertIn(
        (
            f'--reporting-interval='
            f'{kafka_benchmark._KAFKA_REPORTING_INTERVAL.value}'
        ),
        exec_call,
    )

    # Check 3rd call (cat logs)
    cat_call = self.consumer_vm.RemoteCommand.call_args_list[2][0][0]
    self.assertIn('cat /tmp/consumer_*.log', cat_call)

  @flagsaver.flagsaver(kafka_consumer_fetch_size=1048576)
  def testRunConsumerCustomFlags(self):
    self.consumer_vm.RemoteCommand.side_effect = [
        ('', ''),
        ('', ''),
        ('output', ''),
    ]
    kafka_benchmark._RunConsumer(
        self.consumer_vm, '10.0.0.1:9092', 'test-topic', 2, 500
    )
    exec_call = self.consumer_vm.RemoteCommand.call_args_list[1][0][0]
    self.assertIn('--fetch-size=1048576', exec_call)

  @flagsaver.flagsaver(kafka_consumer_timeout_ms=50000)
  def testRunConsumerCustomTimeout(self):
    self.consumer_vm.RemoteCommand.side_effect = [
        ('', ''),
        ('', ''),
        ('output', ''),
    ]
    kafka_benchmark._RunConsumer(
        self.consumer_vm, '10.0.0.1:9092', 'test-topic', 2, 500
    )
    exec_call = self.consumer_vm.RemoteCommand.call_args_list[1][0][0]

    self.assertIn('--timeout 50000', exec_call)


class KafkaBenchmarkResultParserTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _ParseProducerResults and _ParseConsumerResults."""

  @parameterized.named_parameters(
      (
          'RecordsPerSec',
          0,
          'Producer Throughput (Records/sec)',
          5000.0,
          'records/sec',
      ),
      (
          'MBPerSec',
          1,
          'Producer Throughput (MB/sec)',
          5.0,
          'MB/sec',
      ),
      (
          'AvgLatency',
          2,
          'Producer Avg Latency',
          12.5,
          'ms',
      ),
      (
          'P95Ingress',
          3,
          'Producer P95 Maximum sustained ingress scale',
          5.0,
          'MB/s',
      ),
  )
  def testParseProducerResultsSingleThread(
      self, index, expected_metric, expected_value, expected_unit
  ):
    stdout = '5000.0 records/sec (5.0 MB/sec), 12.5 ms avg latency\n'
    metadata = {'test_key': 'test_val'}

    results = kafka_benchmark._ParseProducerResults(stdout, metadata)

    self.assertLen(results, 4)
    sample_item = results[index]
    self.assertEqual(sample_item.metric, expected_metric)
    self.assertEqual(sample_item.value, expected_value)
    self.assertEqual(sample_item.unit, expected_unit)
    self.assertEqual(sample_item.metadata, metadata)

  def testParseProducerResultsMultiThread(self):
    stdout = (
        '5000.0 records/sec (5.0 MB/sec), 10.0 ms avg latency\n'
        '3000.0 records/sec (3.0 MB/sec), 20.0 ms avg latency\n'
    )

    results = kafka_benchmark._ParseProducerResults(stdout, {})

    self.assertLen(results, 4)
    self.assertEqual(results[0].value, 8000.0)
    self.assertEqual(results[1].value, 8.0)
    self.assertEqual(results[2].value, 15.0)
    self.assertEqual(results[3].value, 8.0)

  def testParseProducerResultsEmptyOrInvalid(self):
    self.assertEqual(kafka_benchmark._ParseProducerResults('', {}), [])
    self.assertEqual(
        kafka_benchmark._ParseProducerResults('No stats line', {}), []
    )

  def testParseProducerResultsWithNoise(self):
    stdout = (
        '[INFO] Starting test...\n'
        '2500.5 records/sec (2.5 MB/sec), 10.0 ms avg latency\n'
        '[INFO] Done.\n'
    )

    results = kafka_benchmark._ParseProducerResults(stdout, {})

    self.assertLen(results, 4)
    self.assertEqual(results[0].value, 2500.5)
    self.assertEqual(results[1].value, 2.5)
    self.assertEqual(results[2].value, 10.0)
    self.assertEqual(results[3].value, 2.5)

  @parameterized.named_parameters(
      (
          'RecordsPerSec',
          0,
          'Producer Throughput (Records/sec)',
          10.0,
          'records/sec',
      ),
      (
          'MBPerSec',
          1,
          'Producer Throughput (MB/sec)',
          0.01,
          'MB/sec',
      ),
      (
          'AvgLatency',
          2,
          'Producer Avg Latency',
          1.9,
          'ms',
      ),
      (
          'MaxLatency',
          3,
          'Producer Max Latency',
          172.0,
          'ms',
      ),
      (
          'P95Latency',
          4,
          'Producer p95 Latency',
          3.0,
          'ms',
      ),
      (
          'P99Latency',
          5,
          'Producer p99 Latency',
          5.0,
          'ms',
      ),
      (
          'P999Latency',
          6,
          'Producer p99.9 Latency',
          172.0,
          'ms',
      ),
      (
          'P95Ingress',
          7,
          'Producer P95 Maximum sustained ingress scale',
          1.47,
          'MB/s',
      ),
  )
  def testParseProducerResultsWithPercentiles(
      self, index, expected_metric, expected_value, expected_unit
  ):
    stdout = (
        '5005 records sent, 1000.6 records/sec (0.98 MB/sec), 3.7 ms avg'
        ' latency, 41.0 ms max latency.\n3003 records sent, 1500.0 records/sec'
        ' (1.47 MB/sec), 3.0 ms avg latency, 20.0 ms max latency.\n10000'
        ' records sent, 10.0 records/sec (0.01 MB/sec), 1.9 ms avg latency,'
        ' 172.0 ms max latency, 2 ms 50th, 3 ms 95th, 5 ms 99th, 172 ms'
        ' 99.9th.\n'
    )
    metadata = {'test_key': 'test_val'}

    results = kafka_benchmark._ParseProducerResults(stdout, metadata)

    self.assertLen(results, 8)
    sample_item = results[index]
    self.assertEqual(sample_item.metric, expected_metric)
    self.assertEqual(sample_item.value, expected_value)
    self.assertEqual(sample_item.unit, expected_unit)
    self.assertEqual(sample_item.metadata, metadata)

  def testParseProducerResultsMultiThreadWithPercentiles(self):
    stdout = (
        '1000 records sent, 1000.0 records/sec (1.0 MB/sec), 3.0 ms avg'
        ' latency, 10.0 ms max latency.\n1000 records sent, 1200.0 records/sec'
        ' (1.2 MB/sec), 3.0 ms avg latency, 10.0 ms max latency.\n1000 records'
        ' sent, 1000.0 records/sec (1.0 MB/sec), 3.0 ms avg latency, 10.0 ms'
        ' max latency, 2 ms 50th, 3 ms 95th, 4 ms 99th, 5 ms 99.9th.\n1000'
        ' records sent, 800.0 records/sec (0.8 MB/sec), 3.0 ms avg latency,'
        ' 10.0 ms max latency.\n1000 records sent, 900.0 records/sec (0.9'
        ' MB/sec), 3.0 ms avg latency, 10.0 ms max latency.\n1000 records sent,'
        ' 700.0 records/sec (0.7 MB/sec), 3.0 ms avg latency, 10.0 ms max'
        ' latency.\n1000 records sent, 800.0 records/sec (0.8 MB/sec), 3.0 ms'
        ' avg latency, 10.0 ms max latency, 2 ms 50th, 4 ms 95th, 5 ms 99th, 6'
        ' ms 99.9th.\n'
    )

    results = kafka_benchmark._ParseProducerResults(stdout, {})

    self.assertLen(results, 8)
    self.assertEqual(results[0].value, 1800.0)
    self.assertEqual(results[1].value, 1.8)
    self.assertEqual(results[2].value, 3.0)
    self.assertEqual(results[3].value, 10.0)
    self.assertEqual(results[4].value, 3.5)
    self.assertEqual(results[5].value, 4.5)
    self.assertEqual(results[6].value, 5.5)
    self.assertEqual(results[7].value, 2.1)

  def testParseProducerResultsSyntheticLog(self):
    stdout = """1000 records sent, 50.0 records/sec (5.0 MB/sec), 3.0 ms avg latency, 10.0 ms max latency.
2000 records sent, 70.0 records/sec (7.0 MB/sec), 3.0 ms avg latency, 10.0 ms max latency.
3000 records sent, 10.0 records/sec (1.0 MB/sec), 1.9 ms avg latency, 172.0 ms max latency, 2 ms 50th, 3 ms 95th, 5 ms 99th, 172 ms 99.9th.
1000 records sent, 60.0 records/sec (6.0 MB/sec), 3.0 ms avg latency, 10.0 ms max latency.
2000 records sent, 80.0 records/sec (8.0 MB/sec), 3.0 ms avg latency, 10.0 ms max latency.
3000 records sent, 12.0 records/sec (1.2 MB/sec), 2.5 ms avg latency, 180.0 ms max latency, 2 ms 50th, 4 ms 95th, 6 ms 99th, 180 ms 99.9th.
1000 records sent, 70.0 records/sec (7.0 MB/sec), 3.0 ms avg latency, 10.0 ms max latency.
2000 records sent, 90.0 records/sec (9.0 MB/sec), 3.0 ms avg latency, 10.0 ms max latency.
3000 records sent, 15.0 records/sec (1.5 MB/sec), 2.1 ms avg latency, 169.0 ms max latency, 2 ms 50th, 5 ms 95th, 7 ms 99th, 169 ms 99.9th.
"""

    results = kafka_benchmark._ParseProducerResults(stdout, {})

    self.assertLen(results, 8)

    # Records/sec sum: 10.0 + 12.0 + 15.0 = 37.0
    self.assertAlmostEqual(results[0].value, 37.0, places=3)
    # MB/sec sum: 1.0 + 1.2 + 1.5 = 3.7
    self.assertAlmostEqual(results[1].value, 3.7, places=2)
    # Avg latency average: (1.9 + 2.5 + 2.1) / 3 = 2.166666... -> 2.167
    self.assertAlmostEqual(results[2].value, 2.1666666666666665, places=4)
    # Max latency maximum: max([172.0, 180.0, 169.0]) = 180.0
    self.assertAlmostEqual(results[3].value, 180.0, places=2)
    self.assertEqual(results[3].unit, 'ms')
    # p95 latency average: (3 + 4 + 5) / 3 = 4.0
    self.assertEqual(results[4].value, 4.0)
    # p99 latency average: (5 + 6 + 7) / 3 = 6.0
    self.assertEqual(results[5].value, 6.0)
    # p99.9 latency average: (172 + 180 + 169) / 3 = 173.6666... -> 173.67
    self.assertAlmostEqual(results[6].value, 173.66666666666666, places=2)
    # P95 maximum sustained ingress scale: 24.0 (95th %-ile of [18.0, 24.0])
    self.assertAlmostEqual(results[7].value, 24.0, places=2)
    self.assertEqual(results[7].unit, 'MB/s')

  def testParseProducerResultsTrailingMetricsWithPercentiles(self):
    stdout = (
        '1000 records sent, 1000.0 records/sec (1.0 MB/sec), 3.0 ms avg'
        ' latency, 10.0 ms max latency.\n1000 records sent, 1000.0 records/sec'
        ' (1.0 MB/sec), 3.0 ms avg latency, 10.0 ms max latency, 2 ms 50th, 3'
        ' ms 95th, 4 ms 99th, 5 ms 99.9th.\n1000 records sent, 1200.0'
        ' records/sec (1.2 MB/sec), 3.0 ms avg latency, 10.0 ms max latency.\n'
    )

    results = kafka_benchmark._ParseProducerResults(stdout, {})

    self.assertLen(results, 8)
    self.assertEqual(results[0].value, 1000.0)
    self.assertEqual(results[1].value, 1.0)
    self.assertEqual(results[2].value, 3.0)
    self.assertEqual(results[3].value, 10.0)
    self.assertEqual(results[4].value, 3.0)
    self.assertEqual(results[5].value, 4.0)
    self.assertEqual(results[6].value, 5.0)
    self.assertEqual(results[7].value, 2.2)

  def testParseProducerResultsWithoutPercentilesIgnoresIntermediateProgress(
      self,
  ):
    stdout = (
        '1000 records sent, 200.0 records/sec (2.0 MB/sec), 3.0 ms avg latency,'
        ' 10.0 ms max latency.\n'
        '2000 records sent, 400.0 records/sec (4.0 MB/sec), 3.0 ms avg latency,'
        ' 10.0 ms max latency.\n'
        '3000 records sent, 300.0 records/sec (3.0 MB/sec), 3.0 ms avg latency,'
        ' 10.0 ms max latency.\n'
    )
    metadata = {'kafka_num_records': 3000}
    results = kafka_benchmark._ParseProducerResults(stdout, metadata)
    self.assertLen(results, 5)
    self.assertEqual(results[0].value, 300.0)
    self.assertEqual(results[1].value, 3.0)
    self.assertEqual(
        results[4].metric, 'Producer P95 Maximum sustained ingress scale'
    )
    self.assertEqual(results[4].value, 4.0)

  def testParseProducerResultsWithInvalidNumRecords(self):
    stdout = (
        '1000 records sent, 200.0 records/sec (2.0 MB/sec), 3.0 ms avg latency,'
        ' 10.0 ms max latency.\n'
        '2000 records sent, 400.0 records/sec (4.0 MB/sec), 3.0 ms avg latency,'
        ' 10.0 ms max latency.\n'
    )
    metadata = {'kafka_num_records': 'invalid'}
    results = kafka_benchmark._ParseProducerResults(stdout, metadata)
    self.assertLen(results, 1)
    self.assertEqual(
        results[0].metric, 'Producer P95 Maximum sustained ingress scale'
    )
    self.assertEqual(results[0].value, 4.0)

  def testParseProducerResultsMetadataCopy(self):
    metadata = {'key': 'initial'}

    results = kafka_benchmark._ParseProducerResults(
        '100.0 records/sec (1.0 MB/sec), 5.0 ms avg latency', metadata
    )

    metadata['key'] = 'modified'
    self.assertEqual(results[0].metadata['key'], 'initial')

  @parameterized.named_parameters(
      (
          'MBPerSec',
          0,
          'Consumer Throughput (MB/sec)',
          10.5,
          'MB/sec',
      ),
      (
          'RecordsPerSec',
          1,
          'Consumer Throughput (Records/sec)',
          1000.0,
          'records/sec',
      ),
  )
  def testParseConsumerResultsSingleThread(
      self, index, expected_metric, expected_value, expected_unit
  ):
    stdout = (
        '2026-07-01 00:00:00, 2026-07-01 00:00:01, 10485760,'
        ' 10.5, 1000, 1000.0, 0, 10, 0, 0\n'
    )
    metadata = {'test_key': 'test_val'}

    results = kafka_benchmark._ParseConsumerResults(stdout, metadata)

    self.assertLen(results, 2)
    sample_item = results[index]
    self.assertEqual(sample_item.metric, expected_metric)
    self.assertEqual(sample_item.value, expected_value)
    self.assertEqual(sample_item.unit, expected_unit)
    self.assertEqual(sample_item.metadata, metadata)

  def testParseConsumerResultsMultiThread(self):
    stdout = (
        's1, e1, d1, 10.0, c1, 1000.0, rest1\n'
        's2, e2, d2, 20.0, c2, 2000.0, rest2\n'
    )
    results = kafka_benchmark._ParseConsumerResults(stdout, {})
    self.assertLen(results, 2)
    self.assertEqual(results[0].value, 30.0)
    self.assertEqual(results[1].value, 3000.0)

  def testParseConsumerResultsShortOrEmpty(self):
    self.assertEqual(kafka_benchmark._ParseConsumerResults('', {}), [])
    self.assertEqual(
        kafka_benchmark._ParseConsumerResults('col0, col1, col2\n', {}), []
    )

  def testParseConsumerResultsWithInvalidFloats(self):
    stdout = (
        'start, end, size, MB.sec, count, nrecords, more\n'
        's1, e1, d1, 15.5, c1, 1550.0, rest\n'
    )
    results = kafka_benchmark._ParseConsumerResults(stdout, {})
    self.assertLen(results, 2)
    self.assertEqual(results[0].value, 15.5)
    self.assertEqual(results[1].value, 1550.0)

  def testParseConsumerResultsMetadataCopy(self):
    metadata = {'key': 'initial'}
    results = kafka_benchmark._ParseConsumerResults(
        's1, e1, d1, 5.0, c1, 500.0', metadata
    )
    metadata['key'] = 'modified'
    self.assertEqual(results[0].metadata['key'], 'initial')


class KafkaBenchmarkRunTest(KafkaBenchmarkTestCaseBase):
  """Tests for _RunSingleTrial and Run."""

  def testRunSingleTrial(self):
    s1 = sample.Sample('Prod Metric', 1, 'unit')
    s2 = sample.Sample('Cons Metric', 2, 'unit')
    self.broker_vm.RemoteCommand.side_effect = [
        ('300_000_000\n', ''),  # 1. initial _GetBrokerFreeDiskSpaceKb
    ]

    with mock.patch.object(
        kafka_benchmark, '_CreateTopic'
    ) as mock_topic, mock.patch.object(
        kafka_benchmark, '_DeleteTopic'
    ) as mock_delete, mock.patch.object(
        kafka_benchmark, '_RunProducer', return_value='prod_out'
    ) as mock_prod, mock.patch.object(
        kafka_benchmark, '_RunConsumer', return_value='cons_out'
    ) as mock_cons, mock.patch.object(
        kafka_benchmark, '_ParseProducerResults', return_value=[s1]
    ) as mock_parse_prod, mock.patch.object(
        kafka_benchmark, '_ParseConsumerResults', return_value=[s2]
    ) as mock_parse_cons, mock.patch.object(
        kafka_benchmark, '_WaitForBrokerDiskRecovery'
    ) as mock_wait:
      results = kafka_benchmark._RunSingleTrial(self.benchmark_spec, 8, 50_000)

      expected_topic = 'kafka-benchmark-test-threads-8-records-50000'
      mock_topic.assert_called_once_with(
          self.broker_vm, '10.0.0.1:9092', expected_topic
      )
      mock_delete.assert_called_once_with(
          self.broker_vm, '10.0.0.1:9092', expected_topic
      )
      mock_prod.assert_called_once_with(
          self.producer_vm, '10.0.0.1:9092', expected_topic, 8, 50_000
      )
      mock_cons.assert_called_once_with(
          self.consumer_vm, '10.0.0.1:9092', expected_topic, 8, 50_000
      )
      # 300_000_000 KB = 286.102294921875 GB. Floating point eq check.
      mock_wait.assert_called_once()
      self.assertEqual(mock_wait.call_args[0][0], self.broker_vm)
      self.assertAlmostEqual(mock_wait.call_args[0][1], 286.10229492)

      expected_metadata = {
          'kafka_num_records': 50_000,
          'kafka_record_size': 1024,
          'kafka_producer_batch_size': 131_072,
          'kafka_consumer_fetch_size': (
              kafka_benchmark._KAFKA_CONSUMER_FETCH_SIZE.value
          ),
          'kafka_num_threads': 8,
      }
      mock_parse_prod.assert_called_once_with('prod_out', expected_metadata)
      mock_parse_cons.assert_called_once_with('cons_out', expected_metadata)
      self.assertEqual(results, [s1, s2])

  @parameterized.named_parameters(
      ('prod_zero', 0, 100),
      ('cons_neg', 100, -50),
      ('both_zero', 0, 0),
  )
  def testIsThroughputImprovedPrevZeroOrNegative(self, prev_prod, prev_cons):
    curr_samples = [
        sample.Sample('Producer Throughput (MB/sec)', 100, 'MB/sec'),
        sample.Sample('Consumer Throughput (MB/sec)', 100, 'MB/sec'),
    ]
    if prev_prod == 0 and prev_cons == 0:
      # Uses empty list to yield 0.0 for both prev_prod and prev_cons
      prev_samples = []
    else:
      prev_samples = [
          sample.Sample('Producer Throughput (MB/sec)', prev_prod, 'MB/sec'),
          sample.Sample('Consumer Throughput (MB/sec)', prev_cons, 'MB/sec'),
      ]

    self.assertTrue(
        kafka_benchmark._IsThroughputImproved(curr_samples, prev_samples)
    )

  @parameterized.named_parameters(
      ('producer_improves', 120, 104),
      ('consumer_improves', 104, 120),
  )
  def testIsThroughputImprovedPartialStall(self, curr_prod, curr_cons):
    # Tests that if one metric completely stalls, but another strictly improves
    # without any declining, improvement is evaluated as True.
    # Catching mutants replacing 'and' with 'or' for throughput_stalled checks.
    prev_samples = [
        sample.Sample('Producer Throughput (MB/sec)', 100, 'MB/sec'),
        sample.Sample('Consumer Throughput (MB/sec)', 100, 'MB/sec'),
    ]
    curr_samples = [
        sample.Sample('Producer Throughput (MB/sec)', curr_prod, 'MB/sec'),
        sample.Sample('Consumer Throughput (MB/sec)', curr_cons, 'MB/sec'),
    ]

    self.assertTrue(
        kafka_benchmark._IsThroughputImproved(curr_samples, prev_samples)
    )

  @parameterized.named_parameters(
      ('producer_declines', 99, 150),
      ('consumer_declines', 150, 99),
  )
  def testIsThroughputImprovedThroughputDeclined(self, curr_prod, curr_cons):
    prev_samples = [
        sample.Sample('Producer Throughput (MB/sec)', 100, 'MB/sec'),
        sample.Sample('Consumer Throughput (MB/sec)', 100, 'MB/sec'),
    ]
    curr_samples = [
        sample.Sample('Producer Throughput (MB/sec)', curr_prod, 'MB/sec'),
        sample.Sample('Consumer Throughput (MB/sec)', curr_cons, 'MB/sec'),
    ]

    self.assertFalse(
        kafka_benchmark._IsThroughputImproved(curr_samples, prev_samples)
    )

  @mock.patch.object(time, 'sleep')
  def testWaitForBrokerDiskRecovery(self, mock_sleep):
    self.broker_vm.RemoteCommand.side_effect = [
        ('200_000_000\n', ''),  # first check: 200 GB < 270 GB target
        ('250_000_000\n', ''),  # second check: 250 GB < 270 GB target
        (
            '280_000_000\n',
            '',
        ),  # third check: 280 GB >= 270 GB target (recovers)
    ]

    kafka_benchmark._WaitForBrokerDiskRecovery(self.broker_vm, 286.10)

    self.assertLen(self.broker_vm.RemoteCommand.call_args_list, 3)
    self.assertEqual(mock_sleep.call_count, 2)

  @mock.patch.object(time, 'sleep')
  def testWaitForBrokerDiskRecoveryBorderlineThreshold(self, mock_sleep):
    # 90.5 GB is exactly between 90.0 (0.90 * 100) and 91.0
    self.broker_vm.RemoteCommand.return_value = ('94_896_128\n', '')

    kafka_benchmark._WaitForBrokerDiskRecovery(self.broker_vm, 100.0)

    self.assertLen(self.broker_vm.RemoteCommand.call_args_list, 1)
    mock_sleep.assert_not_called()

  @mock.patch.object(time, 'sleep')
  def testWaitForBrokerDiskRecoveryStrictThreshold(self, mock_sleep):
    # 89.5 GB is less than the expected 90.0 GB threshold (0.90 * 100).
    # It must sleep and wait for the second attempt. 90.1 GB easily passes.
    # 89.5 GB = 89.5 * 1024 * 1024 KB = 93847552 KB
    # 90.1 GB = 90.1 * 1024 * 1024 KB = 94476697 KB (rounded down)
    self.broker_vm.RemoteCommand.side_effect = [
        ('93847552\n', ''),
        ('94476697\n', ''),
    ]

    kafka_benchmark._WaitForBrokerDiskRecovery(self.broker_vm, 100.0)

    self.assertLen(self.broker_vm.RemoteCommand.call_args_list, 2)
    mock_sleep.assert_called_once_with(15)

  def testGetBrokerFreeDiskSpaceGbError(self):
    self.broker_vm.RemoteCommand.return_value = ('invalid_float\n', '')

    free_gb = kafka_benchmark._GetBrokerFreeDiskSpaceGb(self.broker_vm)

    self.broker_vm.RemoteCommand.assert_called_once_with(
        "df -k / | tail -1 | awk '{print $4}'", ignore_failure=True
    )
    self.assertEqual(free_gb, 0.0)

  def testWaitForBrokerDiskRecoveryInitialZeroOrNegative(self):
    kafka_benchmark._WaitForBrokerDiskRecovery(self.broker_vm, 0.0)

    self.broker_vm.RemoteCommand.assert_not_called()

  @mock.patch.object(time, 'time')
  @mock.patch.object(time, 'sleep')
  def testWaitForBrokerDiskRecoveryTimeout(self, mock_sleep, mock_time):
    # min_wait_seconds=300. Start time is 100.0, deadline is 400.0.
    # Attempt 1 -> time 100.0 + 15 <= 400.0 (sleep)
    # Attempt 2 -> time 500.0 + 15 >= 400.0 (timeout)
    mock_time.side_effect = [100.0, 100.0, 500.0, 501.0]
    self.broker_vm.RemoteCommand.return_value = ('100_000_000\n', '')

    with mock.patch.object(kafka_benchmark.logging, 'warning') as mock_warning:
      kafka_benchmark._WaitForBrokerDiskRecovery(
          self.broker_vm, 286.10, min_wait_seconds=300
      )
      mock_warning.assert_called_once_with(
          'Broker disk space did not fully recover within %s seconds.', 300
      )

    self.assertLen(self.broker_vm.RemoteCommand.call_args_list, 2)
    mock_sleep.assert_called_once_with(15)

  @mock.patch.object(time, 'time')
  @mock.patch.object(time, 'sleep')
  @flagsaver.flagsaver(kafka_file_delete_delay_ms=100000)
  def testWaitForBrokerDiskRecoveryTimeoutWithLargeDelay(
      self, mock_sleep, mock_time
  ):
    # max_wait_seconds = int(100000 / 1000) * 4 = 400.
    # Start time is 100.0, deadline is 500.0.
    # Attempt 1 -> time 100.0 + 15 <= 500.0 (sleep)
    # Attempt 2 -> time 484.5 + 15 = 499.5 <= 500.0 (sleep).
    # Attempt 3 -> time 550.0 + 15 > 500.0 (timeout)
    mock_time.side_effect = [100.0, 100.0, 484.5, 550.0, 551.0]
    self.broker_vm.RemoteCommand.return_value = ('100_000_000\n', '')

    with mock.patch.object(kafka_benchmark.logging, 'warning') as mock_warning:
      kafka_benchmark._WaitForBrokerDiskRecovery(
          self.broker_vm, 286.10, min_wait_seconds=300
      )
      mock_warning.assert_called_once_with(
          'Broker disk space did not fully recover within %s seconds.', 400
      )

    self.assertLen(self.broker_vm.RemoteCommand.call_args_list, 3)
    self.assertLen(mock_sleep.call_args_list, 2)

  def testRunSweepAllThreads(self):
    def _mock_single(spec, num_threads, num_records):
      del spec, num_records
      return [
          sample.Sample(
              'Producer Throughput (MB/sec)', num_threads * 100.0, 'MB/sec'
          ),
          sample.Sample(
              'Consumer Throughput (MB/sec)', num_threads * 90.0, 'MB/sec'
          ),
      ]

    with mock.patch.object(
        kafka_benchmark, '_RunSingleTrial', side_effect=_mock_single
    ) as mock_single, mock.patch.object(time, 'sleep'):
      results = kafka_benchmark.Run(self.benchmark_spec)
      self.assertLen(mock_single.call_args_list, 9)
      expected_calls = [
          mock.call(self.benchmark_spec, 2**i, 10_000_000) for i in range(9)
      ]
      mock_single.assert_has_calls(expected_calls)
      self.assertLen(results, 2)
      self.assertEqual(results[0].value, 25600.0)

  def testRunSweepEarlyStopping(self):
    trial_data = {
        1: (100.0, 90.0),
        2: (200.0, 180.0),
        4: (201.0, 180.9),
        3: (202.0, 182.0),
    }

    def _mock_single(spec, num_threads, num_records):
      del spec, num_records
      prod_val, cons_val = trial_data[num_threads]
      return [
          sample.Sample('Producer Throughput (MB/sec)', prod_val, 'MB/sec'),
          sample.Sample('Consumer Throughput (MB/sec)', cons_val, 'MB/sec'),
      ]

    with mock.patch.object(
        kafka_benchmark, '_RunSingleTrial', side_effect=_mock_single
    ) as mock_single, mock.patch.object(time, 'sleep'):
      results = kafka_benchmark.Run(self.benchmark_spec)
      self.assertLen(mock_single.call_args_list, 4)
      expected_calls = [
          mock.call(self.benchmark_spec, t, 10_000_000) for t in [1, 2, 4, 3]
      ]
      mock_single.assert_has_calls(expected_calls)
      self.assertLen(results, 2)
      self.assertEqual(results[0].value, 200.0)

  @flagsaver.flagsaver(kafka_num_threads=['8'], kafka_num_records=12345)
  def testRunWithCustomFlags(self):
    mock_sample = sample.Sample(
        'Producer Throughput (MB/sec)', 100.0, 'MB/sec'
    )
    with mock.patch.object(
        kafka_benchmark, '_RunSingleTrial', return_value=[mock_sample]
    ) as mock_single, mock.patch.object(time, 'sleep'):
      results = kafka_benchmark.Run(self.benchmark_spec)
      mock_single.assert_called_once_with(self.benchmark_spec, 8, 12345)
      self.assertEqual(results, [mock_sample])

  @flagsaver.flagsaver(
      kafka_num_threads=['8', '2', '4'], kafka_num_records=12345
  )
  def testRunWithCustomThreadList(self):
    def _mock_single(spec, num_threads, num_records):
      del spec, num_records
      return [
          sample.Sample(
              'Producer Throughput (MB/sec)', num_threads * 100.0, 'MB/sec'
          ),
          sample.Sample(
              'Consumer Throughput (MB/sec)', num_threads * 90.0, 'MB/sec'
          ),
      ]

    with mock.patch.object(
        kafka_benchmark, '_RunSingleTrial', side_effect=_mock_single
    ) as mock_single, mock.patch.object(time, 'sleep'):
      results = kafka_benchmark.Run(self.benchmark_spec)
      self.assertLen(mock_single.call_args_list, 3)
      expected_calls = [
          mock.call(self.benchmark_spec, t, 12345) for t in [2, 4, 8]
      ]
      mock_single.assert_has_calls(expected_calls)
      self.assertLen(results, 2)
      self.assertEqual(results[0].value, 800.0)

  def testRunBinarySearchRefinement(self):
    # Coarse search: 1 -> 2 -> 4 -> 8
    # 1: 100
    # 2: 200 (pass, 100% inc)
    # 4: 400 (pass, 100% inc)
    # 8: 400 (fail, 0% inc) => Coarse loop breaks. last_pass=4, last_fail=8
    # Binary search over [4, 7]:
    # mid = (4 + 7 + 1) // 2 = 6
    # 6: 500 (pass over 4 which was 400, 25% inc > 5%) -> low=6, best=6
    # Next loop over [6, 7]:
    # mid = (6 + 7 + 1) // 2 = 7
    # 7: 504 (fail over 6 which was 500, < 5% inc) -> high=6
    # Loop terminates (low=6, high=6), returns 6.
    trial_data = {
        1: (100.0, 90.0),
        2: (200.0, 180.0),
        4: (400.0, 360.0),
        8: (400.0, 360.0),
        6: (500.0, 450.0),
        7: (504.0, 453.0),
    }

    def _mock_single(spec, num_threads, num_records):
      del spec, num_records
      prod_val, cons_val = trial_data[num_threads]
      return [
          sample.Sample('Producer Throughput (MB/sec)', prod_val, 'MB/sec'),
          sample.Sample('Consumer Throughput (MB/sec)', cons_val, 'MB/sec'),
      ]

    with mock.patch.object(
        kafka_benchmark, '_RunSingleTrial', side_effect=_mock_single
    ) as mock_single, mock.patch.object(time, 'sleep'):
      results = kafka_benchmark.Run(self.benchmark_spec)

      expected_calls = [
          mock.call(self.benchmark_spec, 1, 10_000_000),
          mock.call(self.benchmark_spec, 2, 10_000_000),
          mock.call(self.benchmark_spec, 4, 10_000_000),
          mock.call(self.benchmark_spec, 8, 10_000_000),
          # Binary search
          mock.call(self.benchmark_spec, 6, 10_000_000),
          mock.call(self.benchmark_spec, 7, 10_000_000),
      ]
      self.assertEqual(mock_single.call_args_list, expected_calls)
      self.assertLen(results, 2)
      self.assertEqual(results[0].value, 500.0)

  def testRunBinarySearchRefinementMidMinusOneCrucial(self):
    # Coarse search: 1 -> 2 -> 4 -> 8
    # 4: 400 (last pass, 100% inc)
    # 8: 400 (fail, 0% inc) => low=4, high=7
    # Binary search over [4, 7]:
    # mid = (4 + 7 + 1) // 2 = 6
    # 6: 410 (fails, < 5% inc over 400) -> high=5
    # Next loop over [4, 5]:
    # mid = (4 + 5 + 1) // 2 = 5
    # 5: 450 (passes, > 5% inc over 400) -> low=5, best=5
    # Loop terminates (low=5, high=5), returns 5.

    trial_data = {
        1: (100.0, 90.0),
        2: (200.0, 180.0),
        4: (400.0, 360.0),
        8: (400.0, 360.0),
        6: (410.0, 369.0),
        5: (450.0, 405.0),
    }

    def _mock_single(spec, num_threads, num_records):
      del spec, num_records
      prod_val, cons_val = trial_data[num_threads]
      return [
          sample.Sample('Producer Throughput (MB/sec)', prod_val, 'MB/sec'),
          sample.Sample('Consumer Throughput (MB/sec)', cons_val, 'MB/sec'),
      ]

    with mock.patch.object(
        kafka_benchmark, '_RunSingleTrial', side_effect=_mock_single
    ) as mock_single, mock.patch.object(time, 'sleep'):
      results = kafka_benchmark.Run(self.benchmark_spec)

      expected_calls = [
          mock.call(self.benchmark_spec, 1, 10_000_000),
          mock.call(self.benchmark_spec, 2, 10_000_000),
          mock.call(self.benchmark_spec, 4, 10_000_000),
          mock.call(self.benchmark_spec, 8, 10_000_000),
          # Binary search
          mock.call(self.benchmark_spec, 6, 10_000_000),
          mock.call(self.benchmark_spec, 5, 10_000_000),
      ]
      self.assertEqual(mock_single.call_args_list, expected_calls)
      self.assertLen(results, 2)
      self.assertEqual(results[0].value, 450.0)


class KafkaBenchmarkCleanupTest(KafkaBenchmarkTestCaseBase):
  """Tests for Cleanup."""

  def testCleanup(self):
    kafka_benchmark.Cleanup(self.benchmark_spec)
    expected_calls = [
        mock.call(
            f'cd {kafka_benchmark.KAFKA_DIR} && bin/kafka-server-stop.sh',
            ignore_failure=True,
        ),
        mock.call('rm -rf /tmp/kraft-*-logs', ignore_failure=True),
    ]
    self.broker_vm.RemoteCommand.assert_has_calls(expected_calls)
    self.controller_vm.RemoteCommand.assert_has_calls(expected_calls)
    self.assertLen(self.broker_vm.RemoteCommand.call_args_list, 2)
    self.assertLen(self.controller_vm.RemoteCommand.call_args_list, 2)


if __name__ == '__main__':
  unittest.main()
