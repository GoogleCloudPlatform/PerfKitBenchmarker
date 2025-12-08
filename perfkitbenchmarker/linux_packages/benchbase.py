# Copyright 2025 Google LLC. All rights reserved.
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

"""Module containing Benchbase installation and cleanup functions."""

import json
import logging
import os
from typing import Any

from absl import flags
import jinja2
from perfkitbenchmarker import data as pkb_data
from perfkitbenchmarker import errors
from perfkitbenchmarker import sample
from perfkitbenchmarker import sql_engine_utils
from perfkitbenchmarker import virtual_machine


_OPENJDK_23_URL: str = (
    'https://download.oracle.com/java/23/archive/jdk-23.0.2_linux-x64_bin.tar.gz'
)
_OPENJDK_23_TAR_FILE: str = 'jdk-23.0.2_linux-x64_bin.tar.gz'
_JDK_BIN_PATH: str = '/opt/jdk-23.0.2/bin'
BENCHBASE_DIR: str = '~/benchbase'
CONFIG_FILE_NAME: str = 'pkb_benchbase_config.xml'
CONFIG_FILE_PATH: str = os.path.join(BENCHBASE_DIR, CONFIG_FILE_NAME)

_SECONDS_IN_MINUTE: int = 60
_CONFIG_TEMPLATE_FILE_NAME: str = 'benchbase_conf.j2'

FLAGS = flags.FLAGS

# To run DSQL, pass
# https://github.com/amazon-contributing/aurora-dsql-benchbase-benchmarking.git
_BENCHBASE_REPO_URL = flags.DEFINE_string(
    'benchbase_repo_url',
    default='https://github.com/cmu-db/benchbase.git',
    help='The URL of the Benchbase repository.',
)

_BENCHBASE_THREAD_COUNT = flags.DEFINE_integer(
    'benchbase_thread_count',
    default=200,
    help='Number of threads for Benchbase. Tune to hit ~65% CPU on Spanner.',
)
_BENCHBASE_RATE = flags.DEFINE_string(
    'benchbase_rate',
    default='1000',
    help=(
        'Target QPS for BenchBase. Use "unlimited" for no rate limit, or an'
        ' integer value for a specific QPS cap.'
    ),
)
_BENCHBASE_TXN_WEIGHTS = flags.DEFINE_list(
    'benchbase_txn_weights',
    default=['45', '43', '4', '4', '4'],
    help=(
        'Transaction weights for TPC-C mix (NewOrder, Payment, OrderStatus,'
        ' Delivery, StockLevel).'
    ),
)
_BENCHBASE_WAREHOUSES = flags.DEFINE_integer(
    'benchbase_warehouses',
    default=10000,
    help=(
        'Number of warehouses to load, scales the data size (e.g., 10000 ~'
        ' 1TB).'
    ),
)
_BENCHBASE_WARMUP_DURATION = flags.DEFINE_integer(
    'benchbase_warmup_duration',
    default=30,
    help='Warmup duration for the run phase, in minutes.',
)
_BENCHBASE_WORKLOAD_DURATION = flags.DEFINE_integer(
    'benchbase_workload_duration',
    default=30,
    help='Main workload execution duration, in minutes.',
)
_BENCHBASE_COOLDOWN_DURATION = flags.DEFINE_integer(
    'benchbase_cooldown_duration',
    default=60,
    help='Cooldown duration after the run phase, in minutes.',
)
_BENCHBASE_ISOLATION = flags.DEFINE_enum(
    'benchbase_isolation',
    default='TRANSACTION_REPEATABLE_READ',
    enum_values=[
        'TRANSACTION_REPEATABLE_READ',
        'TRANSACTION_SERIALIZABLE',
    ],
    help='Transaction isolation level.',
)
_BENCHBASE_USE_FOREIGN_KEY = flags.DEFINE_bool(
    'benchbase_use_foreign_key',
    default=False,
    help='Use foreign key for Spanner.',
)


def Install(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Clones the appropriate Benchbase git repository onto the VM.

  Args:
    vm: The virtual machine to install Benchbase on.

  """
  vm.Install('maven')
  _InstallJDK23(vm)
  vm.RemoteCommand(f'sudo rm -rf {BENCHBASE_DIR}')
  git_repo = _BENCHBASE_REPO_URL.value
  vm.RemoteCommand(f'git clone {git_repo} {BENCHBASE_DIR}')

  if FLAGS.db_engine == sql_engine_utils.SPANNER_POSTGRES:
    logging.info(
        'benchbase_use_foreign_key: %s', _BENCHBASE_USE_FOREIGN_KEY.value
    )
    if _BENCHBASE_USE_FOREIGN_KEY.value:
      diff_file = 'spanner_pg_tpcc_with_fk.diff'
      data_file = 'benchbase/spanner_pg_tpcc_with_fk.diff'
    else:
      diff_file = 'spanner_pg_tpcc_no_fk.diff'
      data_file = 'benchbase/spanner_pg_tpcc_no_fk.diff'
    vm.PushDataFile(data_file, os.path.join(BENCHBASE_DIR, diff_file))
    logging.info('Applying benchbase diff file: %s', diff_file)
    vm.RemoteCommand(
        f'cd {BENCHBASE_DIR} && git apply'
        f' {diff_file}'
    )
    print(f'BenchBase installation complete in {BENCHBASE_DIR}')


def _GetJdbcUrl() -> str:
  """Constructs the JDBC URL based on the database engine.

  Returns:
    The JDBC URL string.

  Raises:
    errors.Config.InvalidValue: If the db_engine is not supported.
  """
  db_engine: str = FLAGS.db_engine
  if db_engine == sql_engine_utils.SPANNER_POSTGRES:
    return 'jdbc:postgresql://localhost:5432/benchbase?sslmode=disable&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true&amp;options=-c%20spanner.support_drop_cascade=true'
  elif db_engine == sql_engine_utils.AURORA_DSQL_POSTGRES:
    return 'jdbc:postgresql://localhost:5432/postgres?sslmode=require&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true'
  raise errors.Config.InvalidValue(f'Unsupported db_engine: {db_engine}')


def CreateConfigFile(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Creates the BenchBase XML configuration file on the client VM using Jinja2.

  Args:
    vm: The client virtual machine to create the file on.
  """
  db_engine: str = FLAGS.db_engine

  context: dict[str, int | str] = {
      'driver_class': 'org.postgresql.Driver',
      'jdbc_url': _GetJdbcUrl(),
      'isolation': _BENCHBASE_ISOLATION.value,
      'scalefactor': _BENCHBASE_WAREHOUSES.value,
      'terminals': _BENCHBASE_THREAD_COUNT.value,
      'workload_duration_seconds': (
          _BENCHBASE_WORKLOAD_DURATION.value * _SECONDS_IN_MINUTE
      ),
      'weights': ','.join(_BENCHBASE_TXN_WEIGHTS.value),
  }

  if db_engine == sql_engine_utils.AURORA_DSQL_POSTGRES:
    context['db_type'] = 'AURORADSQL'
    # Following guide here to use automatic username and password generation:
    # https://github.com/amazon-contributing/aurora-dsql-benchbase-benchmarking/wiki#loading-data-and-running-tpc-c-against-an-aurora-dsql-cluster
    context['username_element'] = '<username>admin</username>'
    context['password_element'] = '<password></password>'
  else:  # spanner by default
    context['db_type'] = 'POSTGRES'
    context['username_element'] = '<username>admin</username>'
    context['password_element'] = '<password>password</password>'

  rate: str = _BENCHBASE_RATE.value
  if rate.isdigit():
    context['rate_element'] = f'<rate>{rate}</rate>'
  else:  # unlimited
    context['rate_element'] = '<rate>unlimited</rate>'
  try:
    template_file_path: str = pkb_data.ResourcePath(_CONFIG_TEMPLATE_FILE_NAME)
    logging.info('template_file_path: %s', template_file_path)
    vm.RenderTemplate(
        template_path=template_file_path,
        context=context,
        remote_path=CONFIG_FILE_PATH,
    )
  except jinja2.TemplateNotFound:
    logging.error('Template file not found: %s', _CONFIG_TEMPLATE_FILE_NAME)
    return
  except jinja2.TemplateError as e:
    logging.exception('Error rendering template: %s', e)
    return


def ParseResults(
    vm: virtual_machine.BaseVirtualMachine, metadata: dict[str, Any]
) -> list[sample.Sample]:
  """Parses the latest benchbase result file and returns metrics.

  Args:
    vm: The virtual machine to parse results from.
    metadata: The metadata to attach to the samples.

  Returns:
    A list of sample.Sample objects.

  Raises:
    errors.Benchmarks.RunError: If the result file is not found or cannot be
      parsed.
  """
  stdout, _ = vm.RemoteCommand(
      f'ls -t {BENCHBASE_DIR}/results/tpcc*summary.json | head -n 1'
  )
  result_file = stdout.strip()
  if not result_file:
    raise errors.Benchmarks.RunError('Benchbase result file not found.')
  stdout, _ = vm.RemoteCommand(f'cat {result_file}')
  try:
    results = json.loads(stdout)
  except json.JSONDecodeError as e:
    raise errors.Benchmarks.RunError(
        f'Error parsing benchbase result file {result_file}: {e}'
    ) from e
  samples: list[sample.Sample] = []
  latency_metrics = results.get('Latency Distribution', {})
  if not latency_metrics:
    raise errors.Benchmarks.RunError(
        'Latency Distribution not found in benchbase result file.'
    )
  for key, value in latency_metrics.items():
    metric_name = (
        key.replace('(microseconds)', '').strip().replace(' ', '_').lower()
    )
    samples.append(sample.Sample(metric_name, value / 1000, 'ms', metadata))

  if 'Throughput (requests/second)' in results:
    throughput = results['Throughput (requests/second)']
    samples.append(
        sample.Sample(
            'tps',
            throughput,
            'tps',
            metadata,
        )
    )
    tpmc = throughput * int(_BENCHBASE_TXN_WEIGHTS.value[0]) / 100.0 * 60.0
    samples.append(sample.Sample('tpmc', tpmc, 'tpm', metadata))
  else:
    raise errors.Benchmarks.RunError(
        'Throughput (requests/second) not found in benchbase result file.'
    )
  return samples


def OverrideEndpoint(
    vm: virtual_machine.BaseVirtualMachine, endpoint: str
) -> None:
  """Overrides the endpoint in the Benchbase XML configuration file on the client VM.

  Args:
    vm: The client virtual machine to create the file on.
    endpoint: The endpoint of the database.
  """
  vm.RemoteCommand(f"sed -i 's/localhost/{endpoint}/g' {CONFIG_FILE_PATH}")


def Uninstall(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Uninstalls the BenchBase package on the VM by removing the directory.

  Args:
    vm: The virtual machine to uninstall BenchBase from.
  """
  vm.RemoteCommand(f'sudo rm -rf {BENCHBASE_DIR}')


# JDK 23 is not available when installing via package manager so we need to
# install it from the tar.
def _InstallJDK23(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Downloads and installs OpenJDK 23 on the VM.

  Args:
    vm: The virtual machine to install JDK 23 on.

  Raises:
    errors.Setup.InvalidSetupError: If the Java version is not 23.0.2 after
      installation.
  """
  vm.RemoteCommand(f'wget {_OPENJDK_23_URL} -O {_OPENJDK_23_TAR_FILE}')
  vm.RemoteCommand(f'sudo tar -xvf {_OPENJDK_23_TAR_FILE} -C /opt')
  priority: int = 2000

  commands: list[str] = [
      'java',
      'javac',
      'jar',
      'javadoc',
      'javap',
      'jps',
      'jconsole',
      'keytool',
  ]

  # Use update-alternatives to set JDK 23 as the default java version.
  for cmd in commands:
    # Install the alternative
    vm.RemoteCommand(
        'sudo update-alternatives --install'
        f' /usr/bin/{cmd} {cmd} {_JDK_BIN_PATH}/{cmd} {priority}'
    )
    # Explicitly set it as the default
    vm.RemoteCommand(
        f'sudo update-alternatives --set {cmd} {_JDK_BIN_PATH}/{cmd}'
    )
  _, stderr = vm.RemoteCommand('java -version')
  if not stderr.startswith('java version "23.0.2"'):
    raise errors.Setup.InvalidSetupError(
        f'Java version is not 23.0.2: {stderr}, jdk 23 is required for'
        ' Benchbase.'
    )
  # Override the JAVA_HOME in maven shell to use JDK 23.
  vm.RemoteCommand(
      'sudo sed -i "/^export JAVA_HOME=/d" /etc/profile.d/maven.sh'
  )
  vm.RemoteCommand(
      'echo "export JAVA_HOME=/opt/jdk-23.0.2" | sudo tee -a'
      ' /etc/profile.d/maven.sh'
  )
