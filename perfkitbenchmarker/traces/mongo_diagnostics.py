# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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
"""Collects Mongo diagnostic files for analysis.

No samples will be published.
"""


import logging
import os
import uuid
from absl import flags
from perfkitbenchmarker import events
from perfkitbenchmarker import stages
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.traces import base_collector

flags.DEFINE_boolean(
    'mongo_diagnostics',
    False,
    'Run sar (https://linux.die.net/man/1/sar) '
    'on each VM to collect system performance metrics during '
    'each benchmark run, and then download the full archive for analysis.',
)
FLAGS = flags.FLAGS


class _MongoDiagnosticsCollector(base_collector.BaseCollector):
  """sar archive collector for manual analysis.

  Installs sysstat and runs sar on a collection of VMs.
  """

  def _CollectorName(self):
    return 'mongo_diagnostics'

  def _InstallCollector(self, vm):
    pass

  def _CollectorRunCommand(self, vm, collector_file):
    # this starts sar in the background and returns the pid
    cmd = ('echo "nothing to run" &>{output} & echo $!').format(
        output=collector_file,
    )
    return cmd

  def _CollectorPostProcess(self, vm):
    prefix = '{0}-{1}-{2}-'.format(
        vm.name, str(uuid.uuid4())[:8], self._CollectorName()
    )

    def _RunMongoshCommand(vm, command: str) -> tuple[str, str]:
      """Runs a mongosh command on the VM."""
      return vm.RemoteCommand(f'mongosh --eval "{command}" --verbose')

    should_run_fsync_unlock = True
    try:
      # Step PREPARE: run fsyncLock to flush and allow safe copies
      # refer:
      # https://www.mongodb.com/docs/manual/reference/method/db.fsyncLock/
      try:
        _RunMongoshCommand(vm, 'db.fsyncLock()')
      except Exception:  # pylint: disable=broad-except
        should_run_fsync_unlock = False
        logging.exception(
            'Failed running db.fsyncLock() on %s. This is expected on client'
            ' VMs, and on server VMs if Mongo has crashed. We still want to'
            ' collect the diagnostic data for analysis.',
            vm.name,
        )

      # Step 1: download the Mongo FTDC dir
      # refer:
      # https://alexbevi.com/blog/2020/01/26/what-is-mongodb-ftdc-aka-diagnostic-dot-data
      data_folder = '/scratch/mongodb-data'
      diag_data_folder_name = 'diagnostic.data'
      diag_data_folder_path = f'{data_folder}/{diag_data_folder_name}'
      # need read access
      vm.RemoteCommand(f'sudo chmod -R 755 {data_folder}')
      vm.PullFile(
          f'{self.output_directory}/{prefix}{diag_data_folder_name}',
          diag_data_folder_path,
      )

      # Step 2: capture mongo.conf in log output, no need to download
      vm.RemoteCommand('cat /etc/mongod.conf')

      # Step 3: download mongodb.log for analysis
      log_file_name = 'mongod.log'
      log_file_path = f'/var/log/mongodb/{log_file_name}'
      vm.RemoteCommand(f'sudo chmod 755 {log_file_path}')
      vm.PullFile(
          f'{self.output_directory}/{prefix}{log_file_name}',
          log_file_path,
      )

      # Step CLEANUP: unlock fsyncLock
      if should_run_fsync_unlock:
        _RunMongoshCommand(vm, 'db.fsyncUnlock()')

    except Exception:  # pylint: disable=broad-except
      logging.exception(
          'Failed fetching Mongo diagnostics from %s. This is expected on'
          ' client VMs.',
          vm.name,
      )


def Register(parsed_flags):
  """Registers the sar collector if FLAGS.sar is set."""
  if not parsed_flags.mongo_diagnostics:
    return

  output_directory = vm_util.GetTempDir()

  logging.debug(
      'Registering mongo_diagnostics collector output to %s.',
      output_directory,
  )

  if not os.path.isdir(output_directory):
    os.makedirs(output_directory)
  collector = _MongoDiagnosticsCollector(
      output_directory=output_directory
  )
  events.before_phase.connect(collector.Start, stages.RUN, weak=False)
  events.after_phase.connect(collector.Stop, stages.RUN, weak=False)
