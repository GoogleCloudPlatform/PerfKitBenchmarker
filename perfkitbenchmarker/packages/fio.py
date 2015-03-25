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
from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util

FIO_DIR = '%s/fio' % vm_util.VM_TMP_DIR
GIT_REPO = 'git://git.kernel.dk/fio.git'
GIT_TAG = 'fio-2.1.14'
FIO_PATH = FIO_DIR + '/fio'
FIO_CMD_PREFIX = '%s --output-format=json ' % FIO_PATH
SECTION_REGEX = r'\[(\w+)\]\n([\w\d\n=*$/]+)'
PARAMETER_REGEX = r'(\w+)=([/\w\d$*]+)\n'
GLOBAL = 'global'
CMD_SECTION_REGEX = r'--name=(\w+)\s+'
JOB_SECTION_REGEX = r'[\1]\n'


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


def ExtractFioParameters(fio_parameter):
  """Extract fio parameters from raw string.

  Sample parameter_string:
  overwrite=0
  rw=write
  blocksize=512k
  size=10*10*1000*$mb_memory
  iodepth=64
  direct=1
  end_fsync=1

  Args:
    fio_parameter: string. Parameters in string format.
  Returns:
    A dictionary of parameters.
  """
  parameters = regex_util.ExtractAllMatches(
      PARAMETER_REGEX, fio_parameter)
  param_dict = {}
  for parameter in parameters:
    param_dict[parameter[0]] = parameter[1]
  return param_dict


def ParseJobFile(job_file):
  """Parse fio job file as dictionaries of sample metadata.

  Args:
    job_file: The contents of fio job file.

  Returns:
    A dictionary of dictionaries of sample metadata, using test name as keys,
        dictionaries of sample metadata as value.
  """
  parameter_metadata = {}
  global_metadata = {}
  section_match = regex_util.ExtractAllMatches(SECTION_REGEX, job_file)
  for section in section_match:
    if section[0] == GLOBAL:
      global_metadata = ExtractFioParameters(section[1])
      break
  for section in section_match:
    section_name = section[0]
    if section_name == GLOBAL:
      continue
    parameter_metadata[section_name] = {}
    parameter_metadata[section_name].update(global_metadata)
    parameter_metadata[section_name].update(ExtractFioParameters(section[1]))

  return parameter_metadata


def FioParametersToJob(fio_parameters):
  """Translate fio parameters into a job file in raw string.

  Sample fio parameters:
  --filesize=10g --directory=/scratch0
  --ioengine=libaio --filename=fio_test_file --invalidate=1
  --randrepeat=0 --direct=0 --size=3790088k --iodepth=8
  --name=sequential_write --overwrite=0 --rw=write --end_fsync=1

  Args:
    fio_parameter: string. Fio parameters in string format.

  Returns:
    A raw string representing a job file can be read by fio binary.
  """
  fio_parameters = fio_parameters.replace(' ', '\n')
  fio_parameters = regex_util.Substitute(
      CMD_SECTION_REGEX, JOB_SECTION_REGEX, fio_parameters)
  fio_parameters = '[%s]\n%s' % (GLOBAL, fio_parameters)
  return fio_parameters.replace('--', '')


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
            'bw_agg': job[mode]['bw_agg']}
        bw_metadata.update(parameters)
        samples.append(
            sample.Sample('%s:bandwidth' % metric_name,
                          job[mode]['bw_mean'],
                          'KB/s', bw_metadata))
        lat_metadata = {
            'min': job[mode]['lat']['min'],
            'max': job[mode]['lat']['max'],
            'stddev': job[mode]['lat']['stddev']}
        lat_metadata.update(parameters)
        samples.append(
            sample.Sample('%s:latency' % metric_name,
                          job[mode]['lat']['mean'],
                          'usec', lat_metadata))
  return samples
