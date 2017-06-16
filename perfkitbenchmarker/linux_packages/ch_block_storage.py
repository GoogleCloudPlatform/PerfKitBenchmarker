# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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

"""Contains cloudharmony block storage benchmark installation functions."""

import json
import os
from perfkitbenchmarker import flags
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import fio
from perfkitbenchmarker.linux_packages import INSTALL_DIR


flags.DEFINE_list(
    'ch_params', [],
    'A list of comma seperated "key=value" parameters passed into '
    'cloud harmony benchmarks.')

BENCHMARK = 'block-storage'
INSTALL_PATH = os.path.join(INSTALL_DIR, BENCHMARK)
STEADY_STATE_MEASUREMENT_WINDOW = '-ssmw'


def _Install(vm):
  vm.InstallPackages('fio')  # CloudHarmony doesn't work well with v2.7 fio
  for deps in ['php', 'build_tools']:
    vm.Install(deps)
  vm.RemoteCommand(
      ('git clone https://github.com/cloudharmony/{benchmark}.git '
       '{dir}').format(benchmark=BENCHMARK, dir=INSTALL_PATH))


def YumInstall(vm):
  _Install(vm)


def AptInstall(vm):
  _Install(vm)


def _ParseFioJson(fio_json):
  """Parse fio json output.

  Args:
    fio_json: string. Json output from fio comomand.

  Returns:
    A list of sample.Sample object.
  """
  samples = []
  for job in json.loads(fio_json)['jobs']:
    cmd = job['fio_command']
    # Get rid of ./fio.
    cmd = ' '.join(cmd.split()[1:])
    additional_metadata = {'cmd': cmd}
    # Remove ssmw suffix from job name.
    try:
      job['jobname'] = regex_util.Substitute(
          STEADY_STATE_MEASUREMENT_WINDOW, '', job['jobname'])
      additional_metadata['steady_state'] = True
    except regex_util.NoMatchError:
      additional_metadata['steady_state'] = False

    # Mock fio_json to reuse fio parser.
    mock_json = {'jobs': [job]}
    new_samples = fio.ParseResults(
        fio.FioParametersToJob(cmd).__str__(), mock_json)
    for s in new_samples:
      s.metadata.update(additional_metadata)
    samples += new_samples
  return samples


def _ParseCHResultsJson(results_json):
  """Parse json output from CloudHarmony block storage benchmark.

  Args:
    results_json: JSON formatted results for test. Each test provides a
      key/value pair.

  Returns:
     A list of sample.Sample object.
  """
  metadata = {}
  _ExtractMetadata(metadata)
  return [sample.Sample(metric, val, '', metadata)
          for metric, val in json.loads(results_json).items()]


def ParseOutput(results_json, fio_json_list):
  """Parse json output from CloudHarmony block storage benchmark.

  Args:
    results_json: string. Json output reported by CloudHarmony.
    fio_json_list: list of string. Json output strings from fio command.
  Returns:
     A list of sample.Sample object.
  """
  return _ParseCHResultsJson(results_json) + [
      s for fio_json in fio_json_list for s in _ParseFioJson(fio_json)]


def _ExtractMetadata(metadata):
  """Extract run metadata from flags."""
  metadata.update(dict([p.split('=') for p in flags.FLAGS.ch_params]))
