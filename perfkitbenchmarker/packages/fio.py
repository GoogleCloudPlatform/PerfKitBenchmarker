# Copyright 2014 Google Inc. All rights reserved.
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

"""Module containing fio installation, cleanup, parsing functions."""
import ConfigParser
import io

from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FIO_DIR = '%s/fio' % vm_util.VM_TMP_DIR
GIT_REPO = 'http://git.kernel.dk/fio.git'
GIT_TAG = 'fio-2.1.14'
FIO_PATH = FIO_DIR + '/fio'
FIO_CMD_PREFIX = '%s --output-format=json' % FIO_PATH
SECTION_REGEX = r'\[(\w+)\]\n([\w\d\n=*$/]+)'
PARAMETER_REGEX = r'(\w+)=([/\w\d$*]+)\n'
GLOBAL = 'global'
CMD_SECTION_REGEX = r'--name=(\w+)\s+'
JOB_SECTION_REPL_REGEX = r'[\1]\n'
CMD_PARAMETER_REGEX = r'--(\w+=[/\w\d]+)\n'
CMD_PARAMETER_REPL_REGEX = r'\1\n'
CMD_STONEWALL_PARAMETER = '--stonewall'
JOB_STONEWALL_PARAMETER = 'stonewall'


def _Install(vm):
  """Installs the fio package on the VM."""
  vm.Install('build_tools')
  vm.RemoteCommand('git clone {0} {1}'.format(GIT_REPO, FIO_DIR))
  vm.RemoteCommand('cd {0} && git checkout {1}'.format(FIO_DIR, GIT_TAG))
  vm.RemoteCommand('cd {0} && ./configure && make'.format(FIO_DIR))


def YumInstall(vm):
  """Installs the fio package on the VM."""
  vm.InstallPackages('libaio-devel libaio bc')
  _Install(vm)


def AptInstall(vm):
  """Installs the fio package on the VM."""
  vm.InstallPackages('libaio-dev libaio1 bc')
  _Install(vm)


def ParseJobFile(job_file):
  """Parse fio job file as dictionaries of sample metadata.

  Args:
    job_file: The contents of fio job file.

  Returns:
    A dictionary of dictionaries of sample metadata, using test name as keys,
        dictionaries of sample metadata as value.
  """
  config = ConfigParser.RawConfigParser(allow_no_value=True)
  config.readfp(io.BytesIO(job_file))
  global_metadata = {}
  if GLOBAL in config.sections():
    global_metadata = dict(config.items(GLOBAL))
  section_metadata = {}
  for section in config.sections():
    if section != GLOBAL:
      metadata = {}
      metadata.update(dict(config.items(section)))
      metadata.update(global_metadata)
      if JOB_STONEWALL_PARAMETER in metadata:
        del metadata[JOB_STONEWALL_PARAMETER]
      section_metadata[section] = metadata
  return section_metadata


def FioParametersToJob(fio_parameters):
  """Translate fio parameters into a job config file.

  Sample fio parameters:
  --filesize=10g --directory=/scratch0
  --name=sequential_write --overwrite=0 --rw=write

  Output:
  [global]
  filesize=10g
  directory=/scratch0
  [sequential_write]
  overwrite=0
  rw=write

  Args:
    fio_parameter: string. Fio parameters in string format.

  Returns:
    A string representing a fio job config file.
  """
  fio_parameters = fio_parameters.replace(' ', '\n')
  fio_parameters = regex_util.Substitute(
      CMD_SECTION_REGEX, JOB_SECTION_REPL_REGEX, fio_parameters)
  fio_parameters = '[%s]\n%s' % (GLOBAL, fio_parameters)
  fio_parameters = regex_util.Substitute(
      CMD_PARAMETER_REGEX, CMD_PARAMETER_REPL_REGEX, fio_parameters)
  return fio_parameters.replace(CMD_STONEWALL_PARAMETER,
                                JOB_STONEWALL_PARAMETER)


def ParseResults(job_file, fio_json_result):
  """Parse fio json output into samples.

  Args:
    job_file: The contents of the fio job file.
    fio_json_result: Fio results in json format.

  Returns:
    A list of sample.Sample objects.
  """
  samples = []
  parameter_metadata = ParseJobFile(job_file)
  io_modes = ['read', 'write', 'trim']
  for job in fio_json_result['jobs']:
    job_name = job['jobname']
    for mode in io_modes:
      if job[mode]['io_bytes']:
        metric_name = '%s:%s' % (job_name, mode)
        parameters = parameter_metadata[job_name]
        bw_metadata = {
            'bw_min': job[mode]['bw_min'],
            'bw_max': job[mode]['bw_max'],
            'bw_dev': job[mode]['bw_dev'],
            'bw_agg': job[mode]['bw_agg'],
            'bw_mean': job[mode]['bw_mean']}
        bw_metadata.update(parameters)
        samples.append(
            sample.Sample('%s:bandwidth' % metric_name,
                          job[mode]['bw'],
                          'KB/s', bw_metadata))
        lat_metadata = {
            'min': job[mode]['clat']['min'],
            'max': job[mode]['clat']['max'],
            'stddev': job[mode]['clat']['stddev']}
        lat_metadata.update(parameters)
        samples.append(
            sample.Sample('%s:latency' % metric_name,
                          job[mode]['clat']['mean'],
                          'usec', lat_metadata))
        samples.append(
            sample.Sample('%s:iops' % metric_name,
                          job[mode]['iops'], '', parameters))
  return samples


def DeleteParameterFromJobFile(job_file, parameter):
  """Delete all occurance of parameter from job_file.

  Args:
    job_file: The contents of the fio job file.
    parameter: The parameter to be deleted in job file.

  Returns:
    A string representing a fio job file after removing parameter.
  """
  try:
    return regex_util.Substitute(r'%s=[\w\d_/]+\n' % parameter, '', job_file)
  except regex_util.NoMatchError:
    return job_file
