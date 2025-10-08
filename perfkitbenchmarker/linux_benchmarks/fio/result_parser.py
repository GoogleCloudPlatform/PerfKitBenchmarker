# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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

"""Module containing functions for parsing fio results."""
import configparser
import io
import time

from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_benchmarks.fio import constants as fio_constants
from perfkitbenchmarker.linux_benchmarks.fio import flags as fio_flags


_DATA_DIRECTION = {0: 'read', 1: 'write', 2: 'trim'}
_GLOBAL = 'global'
_JOB_STONEWALL_PARAMETER = 'stonewall'


def ParseJobFile(job_file, merge=False):
  """Parse fio job file as dictionaries of sample metadata.

  Args:
    job_file: The contents of fio job file.
    merge: whether the job files need to be merged later.

  Returns:
    A dictionary of dictionaries of sample metadata, using test name as keys,
        dictionaries of sample metadata as value.
  """
  config = configparser.RawConfigParser(allow_no_value=True)
  config.read_file(io.StringIO(job_file))
  global_metadata = {}
  if _GLOBAL in config.sections():
    global_metadata = dict(config.items(_GLOBAL))
  section_metadata = {}
  require_merge = merge
  for section in config.sections():
    if section == _GLOBAL:
      continue
    metadata = dict(config.items(section))
    if _JOB_STONEWALL_PARAMETER in metadata:
      del metadata[_JOB_STONEWALL_PARAMETER]
    if require_merge:
      section, index = section.rsplit('.', 1)[0], section.rsplit('.', 1)[1]
      updated_metadata = {
          f'{key}.{index}': value for key, value in metadata.items()}
      metadata = updated_metadata
    metadata.update(global_metadata)
    if section in section_metadata:
      section_metadata[section].update(metadata)
    else:
      section_metadata[section] = metadata
  return section_metadata


def ParseResults(
    job_file,
    fio_json_result,
    base_metadata=None,
    skip_latency_individual_stats=False,
    latency_measure='clat',
):
  """Parse fio json output into samples.

  Args:
    job_file: The contents of the fio job file.
    fio_json_result: Fio results in json format.
    base_metadata: Extra metadata to annotate the samples with.
    skip_latency_individual_stats: Bool. If true, skips pulling latency stats
      that are not aggregate.
    latency_measure: The latency measurement to use (e.g., 'clat', 'slat',
      'lat').

  Returns:
    A list of sample.Sample objects.
  """
  samples = []
  # The samples should all have the same timestamp because they
  # come from the same fio run.
  # - If we are running multiple jobs in parallel and using group_reporting,
  # fio will merge all the jobs output. In that case, we drop the index
  # number from the job name. For example:
  # rand_4k_read_100%-io-depth-1-num-jobs-1.0 and
  # rand_4k_read_100%-io-depth-1-num-jobs-1.1 will be merged into
  # rand_4k_read_100%-io-depth-1-num-jobs-1
  # - If we are running jobs in parallel and group_reporting is false,
  # then Fio won't merge results.
  timestamp = time.time()
  merge_metadata = fio_flags.FIO_RUN_PARALLEL_JOBS_ON_DISKS.value and int(
      fio_json_result['global options']['group_reporting']
  ) and fio_flags.FIO_SEPARATE_JOBS_FOR_DISKS.value
  parameter_metadata = (
      ParseJobFile(job_file, merge_metadata) if job_file else dict()
  )
  io_modes = list(_DATA_DIRECTION.values())
  for job in fio_json_result['jobs']:
    if not merge_metadata:
      job_name = job['jobname']
    else:
      job_name = job['jobname'].split('.')[0]
    parameters = {'fio_job': job_name, 'latency_measure': latency_measure}
    if parameter_metadata:
      parameters.update(parameter_metadata[job_name])
    if base_metadata:
      parameters.update(base_metadata)
    for mode in io_modes:
      if job[mode]['io_bytes']:
        metric_name = '%s:%s' % (job_name, mode)
        bw_metadata = {
            'bw_min': job[mode]['bw_min'],
            'bw_max': job[mode]['bw_max'],
            'bw_dev': job[mode]['bw_dev'],
            'bw_agg': job[mode]['bw_agg'],
            'bw_mean': job[mode]['bw_mean'],
        }
        bw_metadata.update(parameters)
        samples.append(
            sample.Sample(
                '%s:bandwidth' % metric_name,
                job[mode]['bw'],
                'KB/s',
                bw_metadata,
            )
        )
        lat_key = (
            latency_measure
            if latency_measure in job[mode]
            else f'{latency_measure}_ns'
        )
        lat_section = job[mode][lat_key]

        def _ConvertLat(value):
          if lat_key == f'{latency_measure}_ns':  # pylint: disable=cell-var-from-loop
            # convert from nsec to usec
            return value / 1000
          else:
            return value

        lat_statistics = [
            ('min', _ConvertLat(lat_section['min'])),
            ('max', _ConvertLat(lat_section['max'])),
            ('mean', _ConvertLat(lat_section['mean'])),
            ('stddev', _ConvertLat(lat_section['stddev'])),
        ]
        if 'percentile' in lat_section and not skip_latency_individual_stats:
          percentiles = lat_section['percentile']
          percentile_map = FormatPercentileMap(percentiles)
          for key, value in percentile_map.items():
            lat_statistics.append((f'p{key}', _ConvertLat(value)))

        lat_metadata = parameters.copy()
        for name, val in lat_statistics:
          lat_metadata[name] = val
        samples.append(
            sample.Sample(
                '%s:latency' % metric_name,
                _ConvertLat(job[mode][lat_key]['mean']),
                'usec',
                lat_metadata,
                timestamp,
            )
        )

        for stat_name, stat_val in lat_statistics:
          samples.append(
              sample.Sample(
                  '%s:latency:%s' % (metric_name, stat_name),
                  stat_val,
                  'usec',
                  parameters,
                  timestamp,
              )
          )

        samples.append(
            sample.Sample(
                '%s:iops' % metric_name,
                job[mode]['iops'],
                '',
                parameters,
                timestamp,
            )
        )
  for s in samples:
    s.metadata['fio_version'] = fio_constants.GIT_TAG
  return samples


def FormatPercentileMap(percentile_map):
  formatted_map = {}
  for key, value in percentile_map.items():
    formatted_map['%g'%(float(key))] = value
  return formatted_map
