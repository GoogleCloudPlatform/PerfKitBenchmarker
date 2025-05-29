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
"""Parser for fio scenarios."""

import dataclasses

from perfkitbenchmarker import errors
from perfkitbenchmarker.linux_benchmarks.fio import constants


@dataclasses.dataclass
class FioParameters:
  access_pattern: str
  blocksize: str
  operation_type: str
  rwkind: str
  working_set_size: str
  job_name: str
  extra_params: dict[str, str]
  numjobs: int | None = None
  iodepth: int | None = None


class FioScenarioParser:
  """A parser for fio parameters.

  Valid FIO scenario : rand_4k_read_100%_iodepth-20_numjobs-1
  """
  fio_parameters: FioParameters

  def ValidateScenarioString(self, scenario_string):
    """Validates the scenario string."""
    fields = scenario_string.split('_')
    if len(fields) < 4:
      raise errors.Setup.InvalidFlagConfigurationError(
          f'Unexpected Scenario string format: {scenario_string}'
      )
    (access_pattern, _, operation, _) = fields[0:4]

    if access_pattern not in constants.ALL_ACCESS_PATTERNS:
      raise errors.Setup.InvalidFlagConfigurationError(
          f'Unexpected access pattern {access_pattern} '
          f'in scenario {scenario_string}'
      )

    if operation not in constants.ALL_OPERATIONS:
      raise errors.Setup.InvalidFlagConfigurationError(
          f'Unexpected operation {operation}in scenario {scenario_string}'
      )
    access_op = (access_pattern, operation)
    rwkind = constants.MAP_ACCESS_OP_TO_RWKIND.get(access_op, None)
    if not rwkind:
      raise errors.Setup.InvalidFlagConfigurationError(
          f'{access_pattern} and {operation} could not be mapped '
          'to a rwkind fio parameter from '
          f'scenario {scenario_string}'
      )

  def GetFioParameters(self, scenario_string, benchmark_params):
    """Parses the scenario string and returns the fio parameters.

    Args:
      scenario_string: The scenario string to parse.
      benchmark_params: Parameters set by the benchmark.

    Returns:
      A tuple containing the rwkind, blocksize, workingset, and extra
      parameters.
    """
    self.ValidateScenarioString(scenario_string)
    fields = scenario_string.split('_')
    (access_pattern, blocksize_str, operation, workingset_str) = fields[0:4]
    access_op = (access_pattern, operation)
    rwkind = constants.MAP_ACCESS_OP_TO_RWKIND.get(access_op, None)
    # The first four fields are well defined - after that, we use
    # key value pairs to encode any extra fields we need
    # The format is key-value for any additional fields appended
    # e.g. rand_16k_readwrite_5TB_rwmixread-65
    #          random access pattern
    #          16k block size
    #          readwrite operation
    #          5 TB working set
    #          rwmixread of 65. (so 65% reads and 35% writes)
    extra_params = {}
    for extra_fields in fields[4:]:
      key_value = extra_fields.split('-')
      key = key_value[0]
      value = key_value[1]
      if key not in constants.FIO_KNOWN_FIELDS_IN_JINJA:
        raise errors.Setup.InvalidFlagConfigurationError(
            'Unrecognized FIO parameter {} out of scenario {}'.format(
                key, scenario_string
            )
        )
      if key in benchmark_params:
        raise errors.Setup.InvalidFlagConfigurationError(
            'Key {} passed as fio parameter in {} and benchmark parameter.'
            ' Please use only one value for the key'.format(
                key, scenario_string
            )
        )
      extra_params[key] = value
    extra_params.update(benchmark_params)
    iodepth = int(extra_params['iodepth'])
    numjobs = int(extra_params['numjobs'])
    name = self.GenerateJobName(scenario_string, iodepth, numjobs)
    return FioParameters(
        access_pattern,
        blocksize_str,
        operation,
        rwkind,
        workingset_str,
        name,
        extra_params,
        iodepth=iodepth,
        numjobs=numjobs,
    )

  def GetScenarioFromScenarioStringAndParams(
      self, scenario_string, benchmark_params
  ):
    """Generate Scenario from scenario string and benchmark parameters."""
    fio_parameters = self.GetFioParameters(scenario_string, benchmark_params)
    # required fields of JOB_FILE_TEMPLATE
    result = {
        'rwkind': fio_parameters.rwkind,
        'size': fio_parameters.working_set_size,
        'blocksize': fio_parameters.blocksize,
        'iodepth': fio_parameters.iodepth,
        'numjobs': fio_parameters.numjobs,
        'name': fio_parameters.job_name,
    }

    # The first four fields are well defined - after that, we use
    # key value pairs to encode any extra fields we need
    # The format is key-value for any additional fields appended
    # e.g. rand_16k_readwrite_5TB_rwmixread-65
    #          random access pattern
    #          16k block size
    #          readwrite operation
    #          5 TB working set
    #          rwmixread of 65. (so 65% reads and 35% writes)
    result.update(fio_parameters.extra_params)
    return result

  def GenerateJobName(self, scenario_string, iodepth, numjobs):
    return (
        scenario_string.replace(',', '__')
        .replace(f'_iodepth-{iodepth}', '')
        .replace(f'_numjobs-{numjobs}', '')
    )
