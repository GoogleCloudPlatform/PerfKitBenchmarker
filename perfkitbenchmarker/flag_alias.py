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
    'scratch_disk_size': 'data_disk_size'
}

# Added 10th Nov 2022
RELATIONAL_DB_FLAGS_TO_TRANSLATE = {
    'managed_db_engine': 'db_engine'
}

ALL_TRANSLATIONS = [DISK_FLAGS_TO_TRANSLATE, RELATIONAL_DB_FLAGS_TO_TRANSLATE]

# Make sure the regex only matches the argument instead of value
# Arguments can come with either - or --
# To specify a parameter both pattern below would work
# --arg=Value
# --arg Value
# --noarg
# To avoid matching value with arg, match the start and end of the string
PRIOR_ALIAS_REGEX = '(^-?-(?:no)?){0}(=.*|$)'


# pylint: disable=dangerous-default-value
def AliasFlagsFromArgs(
    argv: List[str],
    alias_dict: List[Dict[str, str]] = ALL_TRANSLATIONS) -> List[str]:
  """Alias flags to support backwards compatibility."""
  new_argv = []
  for arg in argv:
    for alias in alias_dict:
      for key in alias:
        regex = PRIOR_ALIAS_REGEX.format(key)
        if re.match(regex, arg):
          arg = re.sub(regex, r'\1{0}\2'.format(alias[key]), arg)
          logging.warning(
              'The flag %s is deprecated and will be removed in the future. '
              'Translating to %s for now.', key, alias[key])
    new_argv.append(arg)
  return new_argv


# pylint: disable=dangerous-default-value
def AliasFlagsFromYaml(
    config: Optional[Dict[str, Any]],
    alias_dict: List[Dict[str, str]] = ALL_TRANSLATIONS
) -> Optional[Dict[str, Any]]:
  """Alias flags to support backwards compatibility."""
  if not config:
    return config
  new_dict = {}
  for original_flag in config:
    key = original_flag
    value = config[original_flag]
    for alias in alias_dict:
      for flag in alias:
        if flag == original_flag:
          key = alias[flag]
          logging.warning(
              'The config flag %s is deprecated and will be '
              'removed in the future. '
              'Translating to %s for now.', flag, alias[flag])
    new_dict[key] = value
  return new_dict
