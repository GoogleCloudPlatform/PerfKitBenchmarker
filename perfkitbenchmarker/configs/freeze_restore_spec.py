# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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
"""Benchmark-configurable options for freezing and restoring resources."""

from typing import Dict, Optional

from absl import flags
from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec

FLAGS = flags.FLAGS


class FreezeRestoreSpec(spec.BaseSpec):
  """Configurable freeze/restore options for resources.

  Attributes:
    enable_freeze_restore: Designates the current resource to use
      freeze/restore functionality if --freeze/--restore is specified on the
      command line. This is a no-op if the resource does not have
      _Freeze/_Restore implemented.
    delete_on_freeze_error: If true, the resource deletes itself if there are
      issues during a freeze.
    create_on_restore_error: If true, the resource creates itself if there are
      issues during a restore.
  """

  enable_freeze_restore: bool
  delete_on_freeze_error: bool
  create_on_restore_error: bool

  def __init__(self,
               component_full_name: str,
               flag_values: Optional[Dict[str, flags.FlagValues]] = None,
               **kwargs):
    super().__init__(component_full_name, flag_values=flag_values, **kwargs)

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    """Gets decoder classes and constructor args for each configurable option.

    Returns:
      dict. Maps option name string to a (ConfigOptionDecoder class, dict) pair.
      The pair specifies a decoder class and its __init__() keyword arguments
      to construct in order to decode the named option.
    """
    result = super()._GetOptionDecoderConstructions()
    result.update({
        'enable_freeze_restore': (option_decoders.BooleanDecoder, {
            'default': False,
            'none_ok': True
        }),
        'delete_on_freeze_error': (option_decoders.BooleanDecoder, {
            'default': False,
            'none_ok': True
        }),
        'create_on_restore_error': (option_decoders.BooleanDecoder, {
            'default': False,
            'none_ok': True
        }),
    })
    return result
