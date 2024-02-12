# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""This module contains function to perform alias on the flags."""

import logging
import re
from typing import Any, Dict, List, Optional

# Added 7th Nov 2022
DISK_FLAGS_TO_TRANSLATE = {
    'scratch_disk_type': 'data_disk_type',
    'scratch_disk_iops': 'aws_provisioned_iops',
    'scratch_disk_throughput': 'aws_provisioned_throughput',
    'scratch_disk_size': 'data_disk_size',
}

# Added 10th Nov 2022
RELATIONAL_DB_FLAGS_TO_TRANSLATE = {
    'managed_db_engine': 'db_engine',
    'managed_db_engine_version': 'db_engine_version',
    'managed_db_database_name': 'database_name',
    'managed_db_database_username': 'database_username',
    'managed_db_database_password': 'database_password',
    'managed_db_high_availability': 'db_high_availability',
    'managed_db_high_availability_type': 'db_high_availability_type',
    'managed_db_backup_enabled': 'db_backup_enabled',
    'managed_db_backup_start_time': 'db_backup_start_time',
    'managed_db_zone': 'db_zone',
    'managed_db_machine_type': 'db_machine_type',
    'managed_db_cpus': 'db_cpus',
    'managed_db_memory': 'db_memory',
    'managed_db_disk_size': 'db_disk_size',
    'managed_db_disk_type': 'db_disk_type',
    'managed_db_disk_iops': 'db_disk_iops',
}

LIST_TO_MULTISTRING_TRANSLATIONS = {'zones': 'zone', 'extra_zones': 'zone'}

SYSBENCH_TRANSLATIONS = {'sysbench_thread_counts': 'sysbench_run_threads'}

ALL_TRANSLATIONS = [
    DISK_FLAGS_TO_TRANSLATE,
    RELATIONAL_DB_FLAGS_TO_TRANSLATE,
    LIST_TO_MULTISTRING_TRANSLATIONS,
    SYSBENCH_TRANSLATIONS,
]

# Make sure the regex only matches the argument instead of value
# Arguments can come with either - or --
# To specify a parameter both pattern below would work
# --arg=Value
# --arg Value
# --noarg
# To avoid matching value with arg, match the start and end of the string
PRIOR_ALIAS_REGEX = '(^-?-(?:no)?){0}(=.*|$)'


def _FlattenTranslationsDicts(dicts: List[Dict[str, str]]) -> Dict[str, str]:
  result = {}
  for translation_dict in dicts:
    result.update(translation_dict)
  return result


def _GetMultiStringFromList(flag: str, args: str) -> List[str]:
  """Gets multi string args from a comma-delineated string."""
  items = args.strip('=').split(',')
  return [f'--{flag}={item}' for item in items]


# pylint: disable=dangerous-default-value
def AliasFlagsFromArgs(
    argv: List[str], alias_dict: List[Dict[str, str]] = ALL_TRANSLATIONS
) -> List[str]:
  """Alias flags to support backwards compatibility."""
  original_to_translation = _FlattenTranslationsDicts(alias_dict)
  new_argv = []
  for arg in argv:
    result = [arg]
    for original in original_to_translation:
      regex = PRIOR_ALIAS_REGEX.format(original)
      m = re.match(regex, arg)
      if not m:
        continue
      translation = original_to_translation[original]
      if original in LIST_TO_MULTISTRING_TRANSLATIONS:
        result = _GetMultiStringFromList(translation, m.group(2))
      else:
        result[0] = re.sub(regex, r'\1{0}\2'.format(translation), arg)
      logging.warning(
          (
              'The flag %s is deprecated and will be removed in the future.'
              ' Translating to %s for now.'
          ),
          original,
          translation,
      )
    new_argv.extend(result)
  return new_argv


# pylint: disable=dangerous-default-value
def AliasFlagsFromYaml(
    config: Optional[Dict[str, Any]],
    alias_dict: List[Dict[str, str]] = ALL_TRANSLATIONS,
) -> Optional[Dict[str, Any]]:
  """Alias flags to support backwards compatibility."""
  if not config:
    return config
  original_to_translation = _FlattenTranslationsDicts(alias_dict)
  new_dict = {}
  for original, value in config.items():
    if original in original_to_translation:
      translation = original_to_translation[original]
      if original in LIST_TO_MULTISTRING_TRANSLATIONS:
        current_list = new_dict.get(translation, [])
        # Flag can either be a list or a single str, both work
        if isinstance(value, str):
          value = [value]
        new_dict[translation] = current_list + value
      else:
        new_dict[translation] = value
      logging.warning(
          (
              'The config flag %s is deprecated and will be '
              'removed in the future. '
              'Translating to %s for now.'
          ),
          original,
          translation,
      )
    else:
      new_dict[original] = value
  return new_dict
