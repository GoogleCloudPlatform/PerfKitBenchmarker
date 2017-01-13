# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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
import time

from perfkitbenchmarker import regex_util
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import INSTALL_DIR

FIO_DIR = '%s/fio' % INSTALL_DIR
GIT_REPO = 'http://git.kernel.dk/fio.git'
GIT_TAG = 'fio-2.7'
FIO_PATH = FIO_DIR + '/fio'
FIO_CMD_PREFIX = '%s --output-format=json' % FIO_PATH
SECTION_REGEX = r'\[(\w+)\]\n([\w\d\n=*$/]+)'
PARAMETER_REGEX = r'(\w+)=([/\w\d$*]+)\n'
GLOBAL = 'global'
CMD_SECTION_REGEX = r'--name=([\S]+)\s+'
JOB_SECTION_REPL_REGEX = r'[\1]\n'
CMD_PARAMETER_REGEX = r'--([\S]+)\s*'
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


def ParseResults(job_file, fio_json_result, base_metadata=None):
  """Parse fio json output into samples.

  Args:
    job_file: The contents of the fio job file.
    fio_json_result: Fio results in json format.
    base_metadata: Extra metadata to annotate the samples with.

  Returns:
    A list of sample.Sample objects.
  """
  samples = []
  # The samples should all have the same timestamp because they
  # come from the same fio run.
  timestamp = time.time()
  parameter_metadata = ParseJobFile(job_file)
  io_modes = ['read', 'write', 'trim']
  for job in fio_json_result['jobs']:
    job_name = job['jobname']
    for mode in io_modes:
      if job[mode]['io_bytes']:
        metric_name = '%s:%s' % (job_name, mode)
        parameters = parameter_metadata[job_name]
        if base_metadata:
          parameters.update(base_metadata)
        parameters['fio_job'] = job_name
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

        # There is one sample whose metric is '<metric_name>:latency'
        # with all of the latency statistics in its metadata, and then
        # a bunch of samples whose metrics are
        # '<metric_name>:latency:min' through
        # '<metric_name>:latency:p99.99' that hold the individual
        # latency numbers as values. This is for historical reasons.
        clat_section = job[mode]['clat']
        percentiles = clat_section['percentile']
        lat_statistics = [
            ('min', clat_section['min']),
            ('max', clat_section['max']),
            ('mean', clat_section['mean']),
            ('stddev', clat_section['stddev']),
            ('p1', percentiles['1.000000']),
            ('p5', percentiles['5.000000']),
            ('p10', percentiles['10.000000']),
            ('p20', percentiles['20.000000']),
            ('p30', percentiles['30.000000']),
            ('p40', percentiles['40.000000']),
            ('p50', percentiles['50.000000']),
            ('p60', percentiles['60.000000']),
            ('p70', percentiles['70.000000']),
            ('p80', percentiles['80.000000']),
            ('p90', percentiles['90.000000']),
            ('p95', percentiles['95.000000']),
            ('p99', percentiles['99.000000']),
            ('p99.5', percentiles['99.500000']),
            ('p99.9', percentiles['99.900000']),
            ('p99.95', percentiles['99.950000']),
            ('p99.99', percentiles['99.990000'])]

        lat_metadata = parameters.copy()
        for name, val in lat_statistics:
          lat_metadata[name] = val
        samples.append(
            sample.Sample('%s:latency' % metric_name,
                          job[mode]['clat']['mean'],
                          'usec', lat_metadata, timestamp))

        for stat_name, stat_val in lat_statistics:
          samples.append(
              sample.Sample('%s:latency:%s' % (metric_name, stat_name),
                            stat_val, 'usec', parameters, timestamp))

        samples.append(
            sample.Sample('%s:iops' % metric_name,
                          job[mode]['iops'], '', parameters, timestamp))
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
