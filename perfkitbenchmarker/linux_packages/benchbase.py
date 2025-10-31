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
# TODO(shuninglin): Add result parsing functions

import logging
import os

from absl import flags
import jinja2
from perfkitbenchmarker import data as pkb_data
from perfkitbenchmarker import errors
from perfkitbenchmarker import virtual_machine


_OPENJDK_23_URL: str = (
    'https://download.oracle.com/java/23/archive/jdk-23.0.2_linux-x64_bin.tar.gz'
)
_OPENJDK_23_TAR_FILE: str = 'jdk-23.0.2_linux-x64_bin.tar.gz'
_JDK_BIN_PATH: str = '/opt/jdk-23.0.2/bin'
_BENCHBASE_DIR: str = '~/benchbase'
_CONFIG_FILE_NAME: str = 'pkb_benchbase_config.xml'
_CONFIG_FILE_PATH: str = os.path.join(_BENCHBASE_DIR, _CONFIG_FILE_NAME)

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

_BENCHBASE_DB_ENGINE = flags.DEFINE_enum(
    'benchbase_db_engine',
    default='spanner',
    enum_values=['spanner', 'aurora_dsql'],
    help='The database engine to benchmark with Benchbase. ',
)


_BENCHBASE_THREAD_COUNT = flags.DEFINE_integer(
    'benchbase_thread_count',
    default=200,
    help='Number of threads for Benchbase. Tune to hit ~65% CPU on Spanner.',
)
_BENCHBASE_RATE = flags.DEFINE_string(
    'unlimited',
    default='420',
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


def Install(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Clones the appropriate Benchbase git repository onto the VM.

  Args:
    vm: The virtual machine to install Benchbase on.

  Raises:
    errors.Setup.InvalidFlagConfigurationError: If db_engine is not provided
      or is an unsupported value.
  """
  _InstallJDK23(vm)
  vm.RemoteCommand(f'sudo rm -rf {_BENCHBASE_DIR}')
  git_repo = _BENCHBASE_REPO_URL.value
  vm.RemoteCommand(f'git clone {git_repo} {_BENCHBASE_DIR}')
  # TODO(shuninglin): Applying diff to be compatible with spanner/dsql
  print(f'BenchBase installation complete in {_BENCHBASE_DIR}')


def _GetJdbcUrl() -> str:
  """Constructs the JDBC URL based on the database engine.

  Returns:
    The JDBC URL string.

  Raises:
    errors.Config.InvalidValue: If the db_engine is not supported.
  """
  db_engine: str = _BENCHBASE_DB_ENGINE.value
  if db_engine == 'spanner':
    return 'jdbc:postgresql://localhost:5432/benchbase?sslmode=disable&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true&amp;options=-c%20spanner.support_drop_cascade=true'
  elif db_engine == 'aurora_dsql':
    return 'jdbc:postgresql://localhost:5432/postgres?sslmode=require&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true'
  raise errors.Config.InvalidValue(f'Unsupported db_engine: {db_engine}')


def CreateConfigFile(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Creates the BenchBase XML configuration file on the client VM using Jinja2.

  Args:
    vm: The client virtual machine to create the file on.
  """
  db_engine: str = _BENCHBASE_DB_ENGINE.value

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

  if db_engine == 'aurora_dsql':
    context['db_type'] = 'AURORADSQL'
    # For DSQL we use automatic username and password generation so comment out
    # the username and password elements.
    context['username_element'] = '<!--<username>admin</username>-->'
    context['password_element'] = '<!--<password>password</password>-->'
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
        remote_path=_CONFIG_FILE_PATH,
    )
  except jinja2.TemplateNotFound:
    logging.error('Template file not found: %s', _CONFIG_TEMPLATE_FILE_NAME)
    return
  except jinja2.TemplateError as e:
    logging.exception('Error rendering template: %s', e)
    return


def Uninstall(vm: virtual_machine.BaseVirtualMachine) -> None:
  """Uninstalls the BenchBase package on the VM by removing the directory.

  Args:
    vm: The virtual machine to uninstall BenchBase from.
  """
  vm.RemoteCommand(f'sudo rm -rf {_BENCHBASE_DIR}')


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
