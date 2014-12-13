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

"""Utilities for fio tool."""

from perfkitbenchmarker import regex_util

REQUIRED_PACKAGES = 'bc fio libaio-dev make gcc'
FIO_TAR = 'fio-2.1.4.tar.gz'
FIO_FOLDER = 'fio-2.1.4'
FIO_PACKAGE_URL = 'http://brick.kernel.dk/snaps/'
SECTION_REGEX = r'\[(\w+)\]\n([\w\d\n=*$/]+)'
PARAMETER_REGEX = r'(\w+)=([/\w\d$*]+)\n'
GLOBAL = 'global'


def PrepareFio(vm):
  """Install fio on a single vm.

  Args:
    vm: The VM upon which fio will be installed.
  """
  vm.InstallPackage(REQUIRED_PACKAGES)
  vm.RemoteCommand('wget %s%s' % (FIO_PACKAGE_URL, FIO_TAR))
  vm.RemoteCommand('tar xvf %s' % FIO_TAR)
  vm.RemoteCommand('cd %s; ./configure; make' % FIO_FOLDER)


def ExtractFioParameters(parameter_string):
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
    parameter_string: Raw string containing parameters.
  Returns:
    A dictionary of parameters, which uses parameter name as key and parameter
       value as value.
  """
  parameters = regex_util.ExtractAllMatches(
      PARAMETER_REGEX, parameter_string)
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


def ParseResults(job_file, fio_json_result):
  """Parse fio json output into samples.

  Args:
    job_file: The contents of the fio job file.
    fio_json_result: Fio results in json format.

  Returns:
    A list of samples in the form of 3 or 4 tuples. The tuples contain
        the sample metric (string), value (float), and unit (string).
        If a 4th element is included, it is a dictionary of sample
        metadata.
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
        samples.append(['%s:bandwidth' % metric_name,
                        job[mode]['bw_mean'],
                        'KB/s', bw_metadata])
        lat_metadata = {
            'min': job[mode]['lat']['min'],
            'max': job[mode]['lat']['max'],
            'stddev': job[mode]['lat']['stddev']}
        lat_metadata.update(parameters)
        samples.append(['%s:latency' % metric_name,
                        job[mode]['lat']['mean'],
                        'usec', lat_metadata])
  return samples
