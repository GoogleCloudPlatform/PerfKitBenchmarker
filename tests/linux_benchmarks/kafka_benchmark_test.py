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


class KafkaBenchmarkTopicAndCommandsTest(KafkaBenchmarkTestCaseBase):
  """Tests for _CreateTopic, _RunProducer, and _RunConsumer."""

  def testCreateTopicDefault(self):
    kafka_benchmark._CreateTopic(self.broker_vm, '10.0.0.1:9092', 'test-topic')
    self.broker_vm.RemoteCommand.assert_called_once_with(
        f'cd {kafka_benchmark.KAFKA_DIR} && bin/kafka-topics.sh --create'
        ' --topic test-topic --bootstrap-server 10.0.0.1:9092 --partitions=256'
        ' --replication-factor=1 --config min.insync.replicas=1'
        ' --if-not-exists'
    )

  @flagsaver.flagsaver(kafka_num_partitions=64, kafka_replication_factor=3)
  def testCreateTopicCustomFlags(self):
    kafka_benchmark._CreateTopic(self.broker_vm, '10.0.0.1:9092', 'test-topic')
    self.broker_vm.RemoteCommand.assert_called_once_with(
        f'cd {kafka_benchmark.KAFKA_DIR} && bin/kafka-topics.sh --create'
        ' --topic test-topic --bootstrap-server 10.0.0.1:9092 --partitions=64'
        ' --replication-factor=3 --config min.insync.replicas=3'
        ' --if-not-exists'
    )

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
    self.assertIn('--fetch-size=5242880', exec_call)
    self.assertIn(
        f'--timeout {kafka_benchmark.KAFKA_CONSUMER_TIMEOUT_MS}', exec_call
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


class KafkaBenchmarkResultParserTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for _ParseProducerResults and _ParseConsumerResults."""

  def testParseProducerResultsSingleThread(self):
    stdout = '5000.0 records/sec (5.0 MB/sec), 12.5 ms avg latency\n'
    metadata = {'test_key': 'test_val'}
    results = kafka_benchmark._ParseProducerResults(stdout, metadata)
    self.assertLen(results, 3)

    r_sec = results[0]
    self.assertEqual(r_sec.metric, 'Producer Throughput (Records/sec)')
    self.assertEqual(r_sec.value, 5000.0)
    self.assertEqual(r_sec.unit, 'records/sec')
    self.assertEqual(r_sec.metadata, metadata)

    mb_sec = results[1]
    self.assertEqual(mb_sec.metric, 'Producer Throughput (MB/sec)')
    self.assertEqual(mb_sec.value, 5.0)
    self.assertEqual(mb_sec.unit, 'MB/sec')

    lat_avg = results[2]
    self.assertEqual(lat_avg.metric, 'Producer Avg Latency')
    self.assertEqual(lat_avg.value, 12.5)
    self.assertEqual(lat_avg.unit, 'ms')

  def testParseProducerResultsMultiThread(self):
    stdout = (
        '5000.0 records/sec (5.0 MB/sec), 10.0 ms avg latency\n'
        '3000.0 records/sec (3.0 MB/sec), 20.0 ms avg latency\n'
    )
    results = kafka_benchmark._ParseProducerResults(stdout, {})
    self.assertLen(results, 3)
    self.assertEqual(results[0].value, 8000.0)
    self.assertEqual(results[1].value, 8.0)
    self.assertEqual(results[2].value, 15.0)

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
    self.assertLen(results, 3)
    self.assertEqual(results[0].value, 2500.5)
    self.assertEqual(results[1].value, 2.5)
    self.assertEqual(results[2].value, 10.0)

  def testParseProducerResultsMetadataCopy(self):
    metadata = {'key': 'initial'}
    results = kafka_benchmark._ParseProducerResults(
        '100.0 records/sec (1.0 MB/sec), 5.0 ms avg latency', metadata
    )
    metadata['key'] = 'modified'
    self.assertEqual(results[0].metadata['key'], 'initial')

  def testParseConsumerResultsSingleThread(self):
    stdout = (
        '2026-07-01 00:00:00, 2026-07-01 00:00:01, 10485760,'
        ' 10.5, 1000, 1000.0, 0, 10, 0, 0\n'
    )
    metadata = {'test_key': 'test_val'}
    results = kafka_benchmark._ParseConsumerResults(stdout, metadata)
    self.assertLen(results, 2)

    mb_sec = results[0]
    self.assertEqual(mb_sec.metric, 'Consumer Throughput (MB/sec)')
    self.assertEqual(mb_sec.value, 10.5)
    self.assertEqual(mb_sec.unit, 'MB/sec')
    self.assertEqual(mb_sec.metadata, metadata)

    r_sec = results[1]
    self.assertEqual(r_sec.metric, 'Consumer Throughput (Records/sec)')
    self.assertEqual(r_sec.value, 1000.0)
    self.assertEqual(r_sec.unit, 'records/sec')

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

    with mock.patch.object(
        kafka_benchmark, '_CreateTopic'
    ) as mock_topic, mock.patch.object(
        kafka_benchmark, '_RunProducer', return_value='prod_out'
    ) as mock_prod, mock.patch.object(
        kafka_benchmark, '_RunConsumer', return_value='cons_out'
    ) as mock_cons, mock.patch.object(
        kafka_benchmark, '_ParseProducerResults', return_value=[s1]
    ) as mock_parse_prod, mock.patch.object(
        kafka_benchmark, '_ParseConsumerResults', return_value=[s2]
    ) as mock_parse_cons:

      results = kafka_benchmark._RunSingleTrial(self.benchmark_spec, 8, 50000)

      expected_topic = 'kafka-benchmark-test-threads-8-records-50000'
      mock_topic.assert_called_once_with(
          self.broker_vm, '10.0.0.1:9092', expected_topic
      )
      mock_prod.assert_called_once_with(
          self.producer_vm, '10.0.0.1:9092', expected_topic, 8, 50000
      )
      mock_cons.assert_called_once_with(
          self.consumer_vm, '10.0.0.1:9092', expected_topic, 8, 50000
      )

      expected_metadata = {
          'kafka_num_records': 50000,
          'kafka_record_size': 1024,
          'kafka_producer_batch_size': 131072,
          'kafka_consumer_fetch_size': 5242880,
          'kafka_num_threads': 8,
      }
      mock_parse_prod.assert_called_once_with('prod_out', expected_metadata)
      mock_parse_cons.assert_called_once_with('cons_out', expected_metadata)
      self.assertEqual(results, [s1, s2])

  def testRunDefaultThreadsAndRecords(self):
    self.producer_vm.NumCpusForBenchmark.return_value = 32
    self.consumer_vm.NumCpusForBenchmark.return_value = 64
    with mock.patch.object(
        kafka_benchmark, '_RunSingleTrial', return_value=['sample1']
    ) as mock_single:
      results = kafka_benchmark.Run(self.benchmark_spec)
      mock_single.assert_called_once_with(
          self.benchmark_spec, 16, 9_000_000
      )
      self.assertEqual(results, ['sample1'])

  def testRunProducerCpuBottleneck(self):
    self.producer_vm.NumCpusForBenchmark.return_value = 4
    self.consumer_vm.NumCpusForBenchmark.return_value = 32
    with mock.patch.object(kafka_benchmark, '_RunSingleTrial') as mock_single:
      kafka_benchmark.Run(self.benchmark_spec)
      mock_single.assert_called_once_with(self.benchmark_spec, 4, 9_000_000)

  def testRunConsumerCpuBottleneck(self):
    self.producer_vm.NumCpusForBenchmark.return_value = 32
    self.consumer_vm.NumCpusForBenchmark.return_value = 2
    with mock.patch.object(kafka_benchmark, '_RunSingleTrial') as mock_single:
      kafka_benchmark.Run(self.benchmark_spec)
      mock_single.assert_called_once_with(self.benchmark_spec, 2, 9_000_000)

  @flagsaver.flagsaver(kafka_num_threads=8, kafka_num_records=12345)
  def testRunWithCustomFlags(self):
    self.producer_vm.NumCpusForBenchmark.return_value = 32
    self.consumer_vm.NumCpusForBenchmark.return_value = 32
    with mock.patch.object(kafka_benchmark, '_RunSingleTrial') as mock_single:
      kafka_benchmark.Run(self.benchmark_spec)
      mock_single.assert_called_once_with(self.benchmark_spec, 8, 12345)


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
