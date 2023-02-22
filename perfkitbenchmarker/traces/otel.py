# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License);
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
"""Runs Opentelemetry Operations Collector on VMs."""

import collections
import json
import logging
import os

from absl import flags
from perfkitbenchmarker import background_tasks
from perfkitbenchmarker import data
from perfkitbenchmarker import events
from perfkitbenchmarker import sample
from perfkitbenchmarker import stages
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.traces import base_collector
import six

flags.DEFINE_boolean('otel', False, 'Run otel on VMs.')
flags.DEFINE_integer('otel_interval_secs', 60,
                     'Interval of the metrics to collect.')
flags.DEFINE_string('otel_config_file', './otel/config.yaml',
                    'Path of the configuration file for Open-Telemetry.')
flags.DEFINE_string(
    'otel_output_directory', None, 'Output directory for otel output. '
    'Only applicable when --otel is specified. '
    'Default: run temporary directory.')

GIT_REPO = 'https://github.com/GoogleCloudPlatform/opentelemetry-operations-collector.git'
GIT_TAG = '08f2752ed36759c4139e8278559e15270e26e140'
OTEL_DIR = 'otel'
FLAGS = flags.FLAGS


class _OTELCollector(base_collector.BaseCollector):
  """otel collector.

  Installs otel and runs it on the VMs.
  """

  def _CollectorName(self):
    """See base class."""
    return 'otel'

  def _InstallCollector(self, vm):
    """See base class."""

    vm.Install('build_tools')
    vm.Install('go_lang')
    # Install collector runs before run phase
    # Remove OTEL folder to support running run phase multiple times.
    vm.RemoteCommand(f'sudo rm -rf {OTEL_DIR}')
    vm.RemoteCommand(f'git clone {GIT_REPO} {OTEL_DIR}')
    vm.RobustRemoteCommand(f'cd {OTEL_DIR} && git checkout {GIT_TAG} && '
                           'export PATH=$PATH:/usr/local/go/bin && make build')

  def _CollectorRunCommand(self, vm, collector_file):
    """See base class."""

    otel_binary, _ = vm.RemoteCommand(
        f'find {OTEL_DIR}/bin -name "google-cloud-metrics-agent*"')

    vm.RenderTemplate(
        data.ResourcePath(FLAGS.otel_config_file),
        f'./{OTEL_DIR}/config.yaml',
        context={
            'INTERVAL': str(self.interval),
            'OUTPUT_FILE': str(collector_file)
        })

    # Create collector file to avoid running into permission issue. Otherwise,
    # the file is created by the process running otel binary which will
    # cause permission denied error when trying to copy file back to runner VM.
    vm.RemoteCommand(f'sudo touch {collector_file}')
    return (f'sudo ./{otel_binary.strip()} --config=./{OTEL_DIR}/config.yaml'
            f'> ./{OTEL_DIR}/otel.log 2>&1 & echo $!')

  def Analyze(self, unused_sender, benchmark_spec, samples):
    """Parse otel metric file and record samples.

    Args:
      benchmark_spec: benchmark_spec of this run.
      samples: samples to add stats to.
    """

    logging.debug('Parsing otel collector data.')

    def _Analyze(role, file):
      parsed_metrics = collections.defaultdict(
          lambda: collections.defaultdict(list))

      with open(
          os.path.join(self.output_directory, os.path.basename(file)),
          'r') as file_contents:

        for data_element in file_contents:
          data_element = json.loads(data_element)
          for resource_metric in data_element['resourceMetrics']:
            for metric in resource_metric['scopeMetrics'][0]['metrics']:
              for data_point in (metric.get('sum', {}) or
                                 metric.get('gauge', {})).get('dataPoints', []):
                name_string = [metric['name']] + [
                    attribute['value']['stringValue'].strip()
                    for attribute in data_point.get('attributes', [])
                ]
                # Filter out all falsy values
                name = ('_').join(filter(None, name_string))
                parsed_metrics[name]['values'].append(
                    str(data_point.get('asInt') or data_point.get('asDouble')))
                parsed_metrics[name]['timestamps'].append(
                    str(data_point['timeUnixNano']))
                parsed_metrics[name]['unit'] = metric['unit']
                parsed_metrics[name]['vm_role'] = role

      for key, value in parsed_metrics.items():
        samples.append(
            sample.Sample(
                metric=key, value=-1, unit=value['unit'], metadata=value))

    background_tasks.RunThreaded(
        _Analyze, [((k, w), {}) for k, w in six.iteritems(self._role_mapping)]
    )


def Register(parsed_flags):
  """Registers the otel collector if FLAGS.otel is set."""

  if not parsed_flags.otel:
    return
  logging.debug('Registering otel collector.')

  output_directory = (
      parsed_flags.otel_output_directory
      if parsed_flags['otel_output_directory'].value else vm_util.GetTempDir())

  collector = _OTELCollector(
      interval=parsed_flags.otel_interval_secs,
      output_directory=output_directory)
  events.before_phase.connect(collector.Start, stages.RUN, weak=False)
  events.after_phase.connect(collector.Stop, stages.RUN, weak=False)
  events.benchmark_samples_created.connect(collector.Analyze, weak=False)
