# Copyright 2016 PerfKitBenchmarker Authors. All rights reserved.
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
"""Variables and classes related to the different stages of a PKB run."""

import itertools

from perfkitbenchmarker import flags


PROVISION = 'provision'
PREPARE = 'prepare'
RUN = 'run'
CLEANUP = 'cleanup'
TEARDOWN = 'teardown'

STAGES = [PROVISION, PREPARE, RUN, CLEANUP, TEARDOWN]

_NEXT_STAGE = {PROVISION: PREPARE, PREPARE: RUN, RUN: CLEANUP,
               CLEANUP: TEARDOWN}
_ALL = 'all'
_VALID_FLAG_VALUES = PROVISION, PREPARE, RUN, CLEANUP, TEARDOWN, _ALL


_SYNTACTIC_HELP = (
    "A complete benchmark execution consists of {0} stages: {1}. Possible flag "
    "values include an individual stage, a comma-separated list of stages, or "
    "'all'. If a list of stages is provided, they must be in order without "
    "skipping any stage.".format(len(STAGES), ', '.join(STAGES)))


class RunStageParser(flags.ListParser):
  """Parse a string containing PKB run stages.

  See _SYNTACTIC_HELP for more information.
  """

  def __init__(self, *args, **kwargs):
    super(RunStageParser, self).__init__(*args, **kwargs)
    self.syntactic_help = _SYNTACTIC_HELP

  def parse(self, argument):
    """Parses a list of stages.

    Args:
      argument: string or list of strings.

    Returns:
      list of strings whose elements are chosen from STAGES.

    Raises:
      ValueError: If argument does not conform to the guidelines explained in
          syntactic_help.
    """
    stage_list = super(RunStageParser, self).parse(argument)

    if not stage_list:
      raise ValueError('Unable to parse {0}. Stage list cannot be '
                       'empty.'.format(repr(argument)))

    invalid_items = set(stage_list).difference(_VALID_FLAG_VALUES)
    if invalid_items:
      raise ValueError(
          'Unable to parse {0}. Unrecognized stages were found: {1}'.format(
              repr(argument), ', '.join(sorted(invalid_items))))

    if _ALL in stage_list:
      if len(stage_list) > 1:
        raise ValueError(
            "Unable to parse {0}. If 'all' stages are specified, individual "
            "stages cannot also be specified.".format(repr(argument)))
      return list(STAGES)

    previous_stage = stage_list[0]
    for stage in itertools.islice(stage_list, 1, None):
      expected_stage = _NEXT_STAGE.get(previous_stage)
      if not expected_stage:
        raise ValueError("Unable to parse {0}. '{1}' should be the last "
                         "stage.".format(repr(argument), previous_stage))
      if stage != expected_stage:
        raise ValueError(
            "Unable to parse {0}. The stage after '{1}' should be '{2}', not "
            "'{3}'.".format(repr(argument), previous_stage, expected_stage,
                            stage))
      previous_stage = stage

    return stage_list


flags.DEFINE(
    RunStageParser(), 'run_stage', STAGES,
    "The stage or stages of perfkitbenchmarker to run.",
    flags.FLAGS, flags.ListSerializer(','))
