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
import time
from typing import Any, Mapping

from absl import flags
from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

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
KAFKA_CONSUMER_TIMEOUT_MS = 120_000

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


def _InstallKafka(vm):
  """Installs Kafka on a VM.

  Args:
    vm: The VM to install Kafka on.
  """
  vm.InstallPackages('openjdk-17-jdk')
  vm.Install('build_tools')
  vm.Install('pip')

  fallback_url = f'https://archive.apache.org/dist/kafka/4.2.0/{KAFKA_TAR}'
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

  kafka_cluster_id, _ = controller_vm.RemoteCommand(
      f'cd {KAFKA_DIR} && bin/kafka-storage.sh random-uuid'
  )
  cluster_id = kafka_cluster_id.strip()

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


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> list[sample.Sample]:
  """Runs Kafka benchmarks.

  Args:
    benchmark_spec: The benchmark specification.

  Returns:
    A list of samples.
  """
  return []


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
