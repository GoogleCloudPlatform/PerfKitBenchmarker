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
"""Runs Apache Kafka benchmarks."""

import concurrent.futures
import itertools
import logging
import re
import time
from typing import Any, Mapping

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample
from perfkitbenchmarker import virtual_machine
from perfkitbenchmarker import vm_util


class DiskNotFullyRecoveredError(Exception):
  """Exception raised when broker disk space is not fully recovered."""


BENCHMARK_NAME = 'kafka'
BENCHMARK_CONFIG = """
kafka:
  description: Runs Apache Kafka benchmarks
  vm_groups:
    broker:
      vm_spec: *default_dual_core
    producer:
      vm_spec: *default_dual_core
    consumer:
      vm_spec: *default_dual_core
    controller:
      vm_spec: *default_dual_core
"""

BENCHMARK_DATA = {}
KAFKA_VERSION = '2.13-4.2.0'
KAFKA_TAR = f'kafka_{KAFKA_VERSION}.tgz'
KAFKA_URL = (
    f'https://downloads.apache.org/kafka/{KAFKA_VERSION.split("-")[1]}/{KAFKA_TAR}'
)

KAFKA_DIR = f'kafka_{KAFKA_VERSION}'
KAFKA_TOPIC_NAME = 'kafka-benchmark-test'
KAFKA_BROKER_PORT = 9092

FLAGS = flags.FLAGS
_KAFKA_NUM_PARTITIONS = flags.DEFINE_integer(
    'kafka_num_partitions',
    256,
    'Number of partitions for the topic.',
)
_KAFKA_REPLICATION_FACTOR = flags.DEFINE_integer(
    'kafka_replication_factor',
    1,
    'Replication factor for the topic.',
)
_KAFKA_NUM_RECORDS = flags.DEFINE_integer(
    'kafka_num_records',
    10_000_000,  # 10 million
    'Number of records to produce/consume per thread.',
)
_KAFKA_RECORD_SIZE = flags.DEFINE_integer(
    'kafka_record_size', 1024, 'Size of each record in bytes.'
)
_KAFKA_PRODUCER_BATCH_SIZE = flags.DEFINE_integer(
    'kafka_producer_batch_size',
    1024 * 128 * 1,  # 128 KB
    'Batch size of the producer in bytes, default is 128 KB.',
)
_KAFKA_CONSUMER_FETCH_SIZE = flags.DEFINE_integer(
    'kafka_consumer_fetch_size',
    1024 * 1024 * 50,  # 50 MB
    'Fetch size of the consumer in bytes, default is 5 MB.',
)
_KAFKA_NUM_THREADS = flags.DEFINE_list(
    'kafka_num_threads',
    None,
    'List of thread counts to use for the benchmark (e.g. 8,16,32). If None '
    'or empty, sweeps through powers of 2 from 1 up to 256 with early '
    'stopping.',
)
_KAFKA_REPORTING_INTERVAL = flags.DEFINE_integer(
    'kafka_reporting_interval',
    20_000,
    'Interval in milliseconds at which the performance test reports progress.',
)
_KAFKA_CONSUMER_TIMEOUT_MS = flags.DEFINE_integer(
    'kafka_consumer_timeout_ms',
    10_000,
    'Timeout in milliseconds for the consumer performance test.',
)
_KAFKA_FILE_DELETE_DELAY_MS = flags.DEFINE_integer(
    'kafka_file_delete_delay_ms',
    60_000,
    'Delay in milliseconds before deleting file segments and log segments on'
    ' the broker once marked for deletion after a topic drop.',
)

_SUMMARY_PERCENTILE_REGEX = re.compile(r'\b\d+(?:\.\d+)?th\b')


def _InstallKafka(vm):
  """Installs Kafka on a VM.

  Args:
    vm: The VM to install Kafka on.
  """
  vm.InstallPackages('openjdk-17-jdk')
  vm.Install('build_tools')
  vm.Install('pip')

  fallback_url = (
      f'https://archive.apache.org/dist/kafka/{KAFKA_VERSION.split("-")[1]}/{KAFKA_TAR}'
  )
  vm.RemoteCommand(f'wget {KAFKA_URL} || wget {fallback_url}')
  vm.RemoteCommand(f'tar -xzf {KAFKA_TAR}')


def GetConfig(user_config: Mapping[Any, Any]) -> Mapping[Any, Any]:
  """Merge BENCHMARK_CONFIG with user_config to create benchmark_spec.

  Args:
    user_config: user-defined configs (through FLAGS.benchmark_config_file or
      FLAGS.config_override).

  Returns:
    The resulting configs that come from merging user-defined configs with
    BENCHMARK_CONFIG.
  """
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Prepares VM to run Kafka.

  Args:
    benchmark_spec: The benchmark specification.
  """
  vms = benchmark_spec.vms
  with concurrent.futures.ThreadPoolExecutor(max_workers=len(vms)) as executor:
    executor.map(_InstallKafka, vms)

  broker_vm = benchmark_spec.vm_groups['broker'][0]
  controller_vm = benchmark_spec.vm_groups['controller'][0]

  stdout, _ = controller_vm.RemoteCommand(
      f'cd {KAFKA_DIR} && bin/kafka-storage.sh random-uuid'
  )
  cluster_id = stdout.strip()

  controller_config = (
      'process.roles=controller\n'
      'node.id=1\n'
      f'controller.quorum.voters=1@{controller_vm.internal_ip}:9093\n'
      'listeners=CONTROLLER://0.0.0.0:9093\n'
      'controller.listener.names=CONTROLLER\n'
      'listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT\n'
      'log.dirs=/tmp/kraft-controller-logs\n'
  )
  controller_vm.RemoteCommand(
      f'cat <<EOF > {KAFKA_DIR}/config/controller.properties\n'
      f'{controller_config}EOF'
  )
  controller_vm.RemoteCommand(
      f'cd {KAFKA_DIR} && bin/kafka-storage.sh format -t {cluster_id} -c'
      ' config/controller.properties'
  )
  controller_vm.RemoteCommand(
      f'cd {KAFKA_DIR} && ulimit -n 100000 && bin/kafka-server-start.sh -daemon'
      ' config/controller.properties'
  )
  time.sleep(5)

  broker_config = (
      'process.roles=broker\n'
      'node.id=2\n'
      f'controller.quorum.voters=1@{controller_vm.internal_ip}:9093\n'
      f'listeners=PLAINTEXT://0.0.0.0:{KAFKA_BROKER_PORT}\n'
      f'advertised.listeners=PLAINTEXT://{broker_vm.internal_ip}:{KAFKA_BROKER_PORT}\n'
      'controller.listener.names=CONTROLLER\n'
      'inter.broker.listener.name=PLAINTEXT\n'
      'listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT\n'
      'log.dirs=/tmp/kraft-broker-logs\n'
      f'file.delete.delay.ms={_KAFKA_FILE_DELETE_DELAY_MS.value}\n'
      f'log.segment.delete.delay.ms={_KAFKA_FILE_DELETE_DELAY_MS.value}\n'
      f'offsets.topic.replication.factor={_KAFKA_REPLICATION_FACTOR.value}\n'
      f'transaction.state.log.replication.factor={_KAFKA_REPLICATION_FACTOR.value}\n'
      'transaction.state.log.min.isr=1\n'
  )
  broker_vm.RemoteCommand(
      f'cat <<EOF > {KAFKA_DIR}/config/broker.properties\n'
      f'{broker_config}EOF'
  )
  broker_vm.RemoteCommand(
      f'cd {KAFKA_DIR} && bin/kafka-storage.sh format -t {cluster_id} -c'
      ' config/broker.properties'
  )
  broker_vm.RemoteCommand(
      f'cd {KAFKA_DIR} && ulimit -n 100000 && bin/kafka-server-start.sh -daemon'
      ' config/broker.properties'
  )
  time.sleep(10)


def _CreateTopic(
    broker_vm: virtual_machine.BaseVirtualMachine,
    bootstrap_server: str,
    topic_name: str,
) -> None:
  """Creates a Kafka topic for the trial.

  Args:
    broker_vm: The broker VM.
    bootstrap_server: The Kafka bootstrap server address.
    topic_name: The name of the topic to create.
  """
  broker_vm.RemoteCommand(
      f'cd {KAFKA_DIR} && bin/kafka-topics.sh --create --topic'
      f' {topic_name} --bootstrap-server {bootstrap_server}'
      f' --partitions={_KAFKA_NUM_PARTITIONS.value}'
      f' --replication-factor={_KAFKA_REPLICATION_FACTOR.value}'
      f' --config min.insync.replicas={_KAFKA_REPLICATION_FACTOR.value}'
      ' --if-not-exists'
  )
  # Allow 5 seconds for partition log creation and leader elections to settle
  # across all partitions before starting high-throughput producer clients.
  time.sleep(5)


def _DeleteTopic(
    broker_vm: virtual_machine.BaseVirtualMachine,
    bootstrap_server: str,
    topic_name: str,
) -> None:
  """Deletes a Kafka topic synchronously right after a trial completes.

  Args:
    broker_vm: The broker VM.
    bootstrap_server: The Kafka bootstrap server address.
    topic_name: The name of the topic to delete.
  """
  broker_vm.RemoteCommand(
      f'cd {KAFKA_DIR} && bin/kafka-topics.sh --delete --topic {topic_name}'
      f' --bootstrap-server {bootstrap_server}',
      ignore_failure=True,
  )
  for _ in range(10):
    stdout, _ = broker_vm.RemoteCommand(
        f'cd {KAFKA_DIR} && bin/kafka-topics.sh --list --bootstrap-server'
        f' {bootstrap_server}',
        ignore_failure=True,
    )
    if topic_name not in stdout.split():
      logging.info('Topic %s deleted successfully.', topic_name)
      return
    time.sleep(5)
  logging.warning('Topic %s not deleted.', topic_name)


def _RunProducer(
    producer_vm: virtual_machine.BaseVirtualMachine,
    bootstrap_server: str,
    topic_name: str,
    num_threads: int,
    num_records: int,
) -> str:
  """Runs the Kafka producer performance test across multiple threads.

  Args:
    producer_vm: The producer VM.
    bootstrap_server: The Kafka bootstrap server address.
    topic_name: The topic to produce records to.
    num_threads: The number of concurrent producer threads.
    num_records: The number of records to produce.

  Returns:
    The concatenated standard output from all producer threads.
  """
  producer_vm.RemoteCommand(
      f'echo "batch.size={_KAFKA_PRODUCER_BATCH_SIZE.value}" >'
      ' /tmp/producer.properties && echo "acks=all" >>'
      ' /tmp/producer.properties && echo "compression.type=zstd" >>'
      ' /tmp/producer.properties'
  )
  cmd = (
      f'cd {KAFKA_DIR} && rm -f /tmp/producer_*.log && '
      f'for i in $(seq 1 {num_threads}); do '
      'bin/kafka-producer-perf-test.sh '
      f'--topic={topic_name} '
      f'--num-records={num_records} '
      f'--record-size={_KAFKA_RECORD_SIZE.value} '
      '--throughput=-1 '
      f'--bootstrap-server {bootstrap_server} '
      f'--reporting-interval={_KAFKA_REPORTING_INTERVAL.value} '
      '--command-config /tmp/producer.properties '
      '> /tmp/producer_$i.log 2>&1 & '
      'done && wait'
  )
  producer_vm.RemoteCommand(cmd)
  stdout, _ = producer_vm.RemoteCommand(
      'cat /tmp/producer_*.log && rm -f /tmp/producer_*.log'
      ' /tmp/producer.properties'
  )
  return stdout


def _RunConsumer(
    consumer_vm: virtual_machine.BaseVirtualMachine,
    bootstrap_server: str,
    topic_name: str,
    num_threads: int,
    num_records: int,
) -> str:
  """Runs the Kafka consumer performance test across multiple threads.

  Args:
    consumer_vm: The consumer VM.
    bootstrap_server: The Kafka bootstrap server address.
    topic_name: The topic to consume records from.
    num_threads: The number of concurrent consumer threads.
    num_records: The number of records to consume.

  Returns:
    The concatenated standard output from all consumer threads.
  """
  consumer_vm.RemoteCommand(
      'echo "auto.offset.reset=earliest" > /tmp/consumer.properties'
  )
  cmd = (
      f'cd {KAFKA_DIR} && rm -f /tmp/consumer_*.log && '
      f'for i in $(seq 1 {num_threads}); do '
      'bin/kafka-consumer-perf-test.sh '
      f'--topic={topic_name} '
      f'--bootstrap-server {bootstrap_server} '
      f'--group pkb-group-t{num_threads} '
      f'--num-records={num_records} '
      f'--fetch-size={_KAFKA_CONSUMER_FETCH_SIZE.value} '
      f'--timeout {_KAFKA_CONSUMER_TIMEOUT_MS.value} '
      f'--reporting-interval={_KAFKA_REPORTING_INTERVAL.value} '
      '--command-config /tmp/consumer.properties '
      '> /tmp/consumer_$i.log 2>&1 & '
      'done && wait'
  )
  consumer_vm.RemoteCommand(cmd)
  stdout, _ = consumer_vm.RemoteCommand(
      'cat /tmp/consumer_*.log && rm -f /tmp/consumer_*.log'
      ' /tmp/consumer.properties'
  )
  return stdout


def _ExtractProducerSummaryAndProgressLines(
    lines: list[str],
    metadata: Mapping[str, Any] | None = None,
) -> tuple[list[str], list[list[float]]]:
  """Extracts summary output lines and periodic progress throughputs.

  Args:
    lines: The lines from running the producer test.
    metadata: Optional metadata dict containing test parameters like
      'kafka_num_records'.

  Returns:
    A tuple of summary lines and periodic progress throughputs.
  """
  has_percentiles = any(
      _SUMMARY_PERCENTILE_REGEX.search(line) for line in lines
  )
  summary_lines = []
  threads_progress_mb = []
  current_thread_mb = []

  num_records = None
  if metadata and 'kafka_num_records' in metadata:
    try:
      num_records = int(metadata['kafka_num_records'])
    except (ValueError, TypeError):
      num_records = None

  for line in lines:
    records_sent_match = re.match(r'^(\d+)\s+records sent,', line)
    is_summary = False

    if _SUMMARY_PERCENTILE_REGEX.search(line):
      is_summary = True
    elif records_sent_match:
      if (
          num_records is not None
          and int(records_sent_match.group(1)) == num_records
          and not has_percentiles
      ):
        is_summary = True
    elif re.search(r'[\d.]+\s+records/sec\s+\([\d.]+\s+MB/sec\)', line):
      is_summary = True

    if is_summary:
      summary_lines.append(line)
      if current_thread_mb:
        threads_progress_mb.append(current_thread_mb)
        current_thread_mb = []
    elif 'records sent' in line:
      throughput_mb_match = re.search(r'\(([\d.]+)\s+MB/sec\)', line)
      if throughput_mb_match:
        current_thread_mb.append(float(throughput_mb_match.group(1)))

  if current_thread_mb:
    threads_progress_mb.append(current_thread_mb)

  return summary_lines, threads_progress_mb


def _ExtractProducerSummaryMetrics(
    summary_lines: list[str],
) -> dict[str, list[float]]:
  """Extracts numeric metrics from producer summary lines.

  Args:
    summary_lines: The summary lines from running the producer test.

  Returns:
    A dictionary of metrics with keys 'rps', 'mb', 'avg_lat', 'max_lat',
    'p95', 'p99', and 'p99_9'.
  """
  metrics: dict[str, list[float]] = {
      'rps': [],
      'mb': [],
      'avg_lat': [],
      'max_lat': [],
      'p95': [],
      'p99': [],
      'p99_9': [],
  }
  for line in summary_lines:
    throughput_rps_match = re.search(r'([\d.]+)\s+records/sec', line)
    throughput_mb_match = re.search(r'\(([\d.]+)\s+MB/sec\)', line)
    avg_latency_match = re.search(r'([\d.]+)\s+ms\s+avg\s+latency', line)
    max_latency_match = re.search(r'([\d.]+)\s+ms\s+max\s+latency', line)
    p95_match = re.search(r'([\d.]+)\s+ms\s+95th', line)
    p99_match = re.search(r'([\d.]+)\s+ms\s+99th', line)
    p99_9_match = re.search(r'([\d.]+)\s+ms\s+99\.9th', line)

    if throughput_rps_match:
      metrics['rps'].append(float(throughput_rps_match.group(1)))
    if throughput_mb_match:
      metrics['mb'].append(float(throughput_mb_match.group(1)))
    if avg_latency_match:
      metrics['avg_lat'].append(float(avg_latency_match.group(1)))
    if max_latency_match:
      metrics['max_lat'].append(float(max_latency_match.group(1)))
    if p95_match:
      metrics['p95'].append(float(p95_match.group(1)))
    if p99_match:
      metrics['p99'].append(float(p99_match.group(1)))
    if p99_9_match:
      metrics['p99_9'].append(float(p99_9_match.group(1)))

  return metrics


def _CreateProducerThroughputSamples(
    metrics: dict[str, list[float]],
    metadata: dict[str, Any],
) -> list[sample.Sample]:
  """Creates producer throughput samples from extracted metrics.

  Args:
    metrics: The metrics dictionary to attach to each sample.
    metadata: The metadata dictionary to attach to each sample.

  Returns:
    A list of sample.Sample objects for producer throughput samples.
  """
  results = []
  thread_rps = metrics['rps']
  thread_mb = metrics['mb']

  if thread_rps:
    results.append(
        sample.Sample(
            'Producer Throughput (Records/sec)',
            sum(thread_rps),
            'records/sec',
            metadata.copy(),
        )
    )
  if thread_mb:
    results.append(
        sample.Sample(
            'Producer Throughput (MB/sec)',
            sum(thread_mb),
            'MB/sec',
            metadata.copy(),
        )
    )
  return results


def _CreateProducerLatencySamples(
    metrics: dict[str, list[float]],
    metadata: dict[str, Any],
) -> list[sample.Sample]:
  """Creates producer latency and percentile samples from extracted metrics.

  Args:
    metrics: The metrics dictionary to attach to each sample.
    metadata: The metadata dictionary to attach to each sample.

  Returns:
    A list of sample.Sample objects for producer latency and percentile samples.
  """
  results = []
  thread_avg_lat = metrics['avg_lat']
  thread_max_lat = metrics['max_lat']
  thread_p95 = metrics['p95']
  thread_p99 = metrics['p99']
  thread_p99_9 = metrics['p99_9']

  if thread_avg_lat:
    results.append(
        sample.Sample(
            'Producer Avg Latency',
            sum(thread_avg_lat) / len(thread_avg_lat),
            'ms',
            metadata.copy(),
        )
    )
  if thread_max_lat:
    results.append(
        sample.Sample(
            'Producer Max Latency',
            max(thread_max_lat),
            'ms',
            metadata.copy(),
        )
    )
  if thread_p95:
    results.append(
        sample.Sample(
            'Producer p95 Latency',
            sum(thread_p95) / len(thread_p95),
            'ms',
            metadata.copy(),
        )
    )
  if thread_p99:
    results.append(
        sample.Sample(
            'Producer p99 Latency',
            sum(thread_p99) / len(thread_p99),
            'ms',
            metadata.copy(),
        )
    )
  if thread_p99_9:
    results.append(
        sample.Sample(
            'Producer p99.9 Latency',
            sum(thread_p99_9) / len(thread_p99_9),
            'ms',
            metadata.copy(),
        )
    )
  return results


def _CreateProducerIngressSample(
    metrics: dict[str, list[float]],
    threads_progress_mb: list[list[float]],
    metadata: dict[str, Any],
) -> list[sample.Sample]:
  """Creates producer P95 maximum sustained ingress scale sample.

  Args:
    metrics: The metrics dictionary to attach to each sample.
    threads_progress_mb: The progress throughputs from each thread.
    metadata: The metadata dictionary to attach to each sample.

  Returns:
    A list of sample.Sample objects for producer P95 maximum sustained ingress
    scale.
  """
  results = []
  thread_mb = metrics['mb']
  aggregated_throughputs = []

  if threads_progress_mb:
    aggregated_throughputs = [
        sum(interval)
        for interval in itertools.zip_longest(
            *threads_progress_mb, fillvalue=0.0
        )
    ]

  p95_ingress = 0.0
  if aggregated_throughputs:
    p95_ingress = sample.PercentileCalculator(
        aggregated_throughputs, [95]
    )['p95']
  elif thread_mb:
    p95_ingress = sum(thread_mb)

  if p95_ingress > 0.0 or thread_mb:
    results.append(
        sample.Sample(
            'Producer P95 Maximum sustained ingress scale',
            p95_ingress,
            'MB/s',
            metadata.copy(),
        )
    )
  return results


def _ParseProducerResults(
    producer_stdout: str, metadata: dict[str, Any]
) -> list[sample.Sample]:
  """Parses the producer output and returns a list of samples.

  Args:
    producer_stdout: The stdout from running the producer test.
    metadata: The metadata dictionary to attach to each sample.

  Returns:
    A list of sample.Sample objects for producer throughput and latency.
  """
  lines = [
      line.strip()
      for line in producer_stdout.strip().split('\n')
      if line.strip()
  ]
  summary_lines, threads_progress_mb = _ExtractProducerSummaryAndProgressLines(
      lines, metadata
  )
  metrics = _ExtractProducerSummaryMetrics(summary_lines)
  results = _CreateProducerThroughputSamples(metrics, metadata)
  results.extend(_CreateProducerLatencySamples(metrics, metadata))
  results.extend(
      _CreateProducerIngressSample(metrics, threads_progress_mb, metadata)
  )
  return results


def _ParseConsumerResults(
    consumer_stdout: str, metadata: dict[str, Any]
) -> list[sample.Sample]:
  """Parses the consumer output and returns a list of samples.

  Args:
    consumer_stdout: The stdout from running the consumer test.
    metadata: The metadata dictionary to attach to each sample.

  Returns:
    A list of sample.Sample objects for consumer throughput.
  """
  results = []
  mb_per_sec_values = []
  records_per_sec_values = []
  for line in consumer_stdout.strip().split('\n'):
    columns = [col.strip() for col in line.split(',')]
    if len(columns) >= 6:
      try:
        mb_per_sec_values.append(float(columns[3]))
        records_per_sec_values.append(float(columns[5]))
      except ValueError:
        continue

  if mb_per_sec_values:
    results.append(
        sample.Sample(
            'Consumer Throughput (MB/sec)',
            sum(mb_per_sec_values),
            'MB/sec',
            metadata.copy(),
        )
    )
    results.append(
        sample.Sample(
            'Consumer Throughput (Records/sec)',
            sum(records_per_sec_values),
            'records/sec',
            metadata.copy(),
        )
    )
  return results


def _GetBrokerFreeDiskSpaceGb(
    broker_vm: virtual_machine.BaseVirtualMachine,
) -> float:
  """Returns the free disk space on the broker VM root filesystem in GB.

  Args:
    broker_vm: The broker VM instance to query.

  Returns:
    The available disk space across the root filesystem in gigabytes, or 0.0
    if the query fails.
  """
  stdout, _ = broker_vm.RemoteCommand(
      "df -k / | tail -1 | awk '{print $4}'", ignore_failure=True
  )
  try:
    return float(stdout.strip()) / (1024 * 1024)
  except (ValueError, TypeError):
    return 0.0


def _WaitForBrokerDiskRecovery(
    broker_vm: virtual_machine.BaseVirtualMachine,
    initial_free_gb: float,
    recovery_threshold: float = 0.90,
    min_wait_seconds: int = 300,
    delay_multiplier: int = 4,
) -> None:
  """Waits until free disk space on broker recovers near original capacity.

  Args:
    broker_vm: The broker VM instance to monitor during cleanup.
    initial_free_gb: The initial available disk space in gigabytes captured
      before the trial run began.
    recovery_threshold: The target fraction of initial free space (default
      0.90).
    min_wait_seconds: The minimum waiting timeout in seconds (default 300).
    delay_multiplier: Multiplier for file delete delay ms calculation
      (default 4).
  """
  if initial_free_gb <= 0:
    return

  target_free_gb = initial_free_gb * recovery_threshold
  max_wait_seconds = max(
      min_wait_seconds,
      int(_KAFKA_FILE_DELETE_DELAY_MS.value / 1000) * delay_multiplier,
  )

  @vm_util.Retry(
      retryable_exceptions=(DiskNotFullyRecoveredError,),
      poll_interval=15,
      timeout=max_wait_seconds,
      fuzz=0,
  )
  def _Wait() -> None:
    curr_free_gb = _GetBrokerFreeDiskSpaceGb(broker_vm)
    if curr_free_gb >= target_free_gb:
      logging.info(
          (
              'Broker disk space recovered: current free=%.1f GB,'
              ' target=%.1f GB (initial=%.1f GB)'
          ),
          curr_free_gb,
          target_free_gb,
          initial_free_gb,
      )
      return
    logging.info(
        (
            'Waiting for broker disk recovery: current free=%.1f GB,'
            ' target=%.1f GB (initial=%.1f GB)'
        ),
        curr_free_gb,
        target_free_gb,
        initial_free_gb,
    )
    raise DiskNotFullyRecoveredError()

  try:
    _Wait()
  except vm_util.TimeoutExceededRetryError:
    logging.warning(
        'Broker disk space did not fully recover within %s seconds.',
        max_wait_seconds,
    )


def _RunSingleTrial(
    benchmark_spec: bm_spec.BenchmarkSpec,
    num_threads: int,
    num_records: int,
) -> list[sample.Sample]:
  """Runs a single trial with the specified number of concurrent threads.

  Args:
    benchmark_spec: The benchmark specification.
    num_threads: The number of threads to use for the benchmark.
    num_records: The number of records to produce/consume.

  Returns:
    A list of samples.
  """
  broker_vm = benchmark_spec.vm_groups['broker'][0]
  producer_vm = benchmark_spec.vm_groups['producer'][0]
  consumer_vm = benchmark_spec.vm_groups['consumer'][0]
  bootstrap_server = f'{broker_vm.internal_ip}:{KAFKA_BROKER_PORT}'

  initial_free_gb = _GetBrokerFreeDiskSpaceGb(broker_vm)
  trial_topic_name = (
      f'{KAFKA_TOPIC_NAME}-threads-{num_threads}-records-{num_records}'
  )
  _CreateTopic(broker_vm, bootstrap_server, trial_topic_name)

  try:
    # Execute producer and consumer workloads concurrently against the broker.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
      producer_future = executor.submit(
          _RunProducer,
          producer_vm,
          bootstrap_server,
          trial_topic_name,
          num_threads,
          num_records,
      )
      consumer_future = executor.submit(
          _RunConsumer,
          consumer_vm,
          bootstrap_server,
          trial_topic_name,
          num_threads,
          num_records,
      )
      producer_stdout = producer_future.result()
      consumer_stdout = consumer_future.result()

    metadata = {
        'kafka_num_records': int(num_records),
        'kafka_record_size': int(_KAFKA_RECORD_SIZE.value),
        'kafka_producer_batch_size': int(_KAFKA_PRODUCER_BATCH_SIZE.value),
        'kafka_consumer_fetch_size': int(_KAFKA_CONSUMER_FETCH_SIZE.value),
        'kafka_num_threads': num_threads,
    }

    trial_results = []
    trial_results.extend(_ParseProducerResults(producer_stdout, metadata))
    trial_results.extend(_ParseConsumerResults(consumer_stdout, metadata))
    return trial_results
  finally:
    _DeleteTopic(broker_vm, bootstrap_server, trial_topic_name)
    _WaitForBrokerDiskRecovery(broker_vm, initial_free_gb)


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs Kafka benchmarks across a sweep of thread counts with early stopping.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of samples corresponding to the winning (optimal) thread count.
  """
  if _KAFKA_NUM_THREADS.value:
    thread_counts = [int(t) for t in _KAFKA_NUM_THREADS.value]
  else:
    thread_counts = [2**i for i in range(9)]

  num_records = int(_KAFKA_NUM_RECORDS.value)
  winning_samples: list[sample.Sample] = []
  prev_producer_mb = 0.0
  prev_consumer_mb = 0.0

  throughput_margin = 0.05

  for num_threads in thread_counts:
    trial_samples = _RunSingleTrial(benchmark_spec, num_threads, num_records)
    if not winning_samples:
      winning_samples = trial_samples

    curr_producer_mb = 0.0
    curr_consumer_mb = 0.0

    for s in trial_samples:
      if getattr(s, 'metric', '') == 'Producer Throughput (MB/sec)':
        curr_producer_mb = getattr(s, 'value', 0.0)
      elif getattr(s, 'metric', '') == 'Consumer Throughput (MB/sec)':
        curr_consumer_mb = getattr(s, 'value', 0.0)

    if prev_producer_mb > 0 and prev_consumer_mb > 0:
      prod_improvement = (
          curr_producer_mb - prev_producer_mb
      ) / prev_producer_mb
      cons_improvement = (
          curr_consumer_mb - prev_consumer_mb
      ) / prev_consumer_mb

      throughput_stalled = (
          prod_improvement <= throughput_margin
          and cons_improvement <= throughput_margin
      )
      throughput_declined = prod_improvement < 0.0 or cons_improvement < 0.0

      if throughput_stalled or throughput_declined:
        break

      winning_samples = trial_samples

    prev_producer_mb = curr_producer_mb
    prev_consumer_mb = curr_consumer_mb

  return winning_samples


def Cleanup(benchmark_spec: bm_spec.BenchmarkSpec) -> None:
  """Cleans up the benchmark.

  Args:
    benchmark_spec: The benchmark specification.
  """
  broker_vm = benchmark_spec.vm_groups['broker'][0]
  controller_vm = benchmark_spec.vm_groups['controller'][0]

  for vm in (broker_vm, controller_vm):
    vm.RemoteCommand(
        f'cd {KAFKA_DIR} && bin/kafka-server-stop.sh', ignore_failure=True
    )
    vm.RemoteCommand(
        'rm -rf /tmp/kraft-*-logs', ignore_failure=True
    )
