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

from absl import flags


PROVISION = 'provision'
PREPARE_SYSTEM = 'prepare_system'
INSTALL_PACKAGES = 'install_packages'
START_SERVICES = 'start_services'
PREPARE = 'prepare'
RUN = 'run'
CLEANUP = 'cleanup'
TEARDOWN = 'teardown'

# The stages run in this order:
#   provision -> prepare -> run -> cleanup -> teardown
# However, you can replace the prepare stage with three stages:
#   install_packages -> prepare_system -> start_services
# You can skip any of the three, but the ones you run must be in that order.
# It is not valid to do prepare *and* the above three, these are distinct paths.

STAGES = [PROVISION, PREPARE, RUN, CLEANUP, TEARDOWN]

_NEXT_STAGE = {
    PROVISION: [
        PREPARE,
        INSTALL_PACKAGES,
        PREPARE_SYSTEM,
        START_SERVICES,
        TEARDOWN,
    ],
    PREPARE: [RUN, CLEANUP],
    INSTALL_PACKAGES: [PREPARE_SYSTEM, START_SERVICES, RUN, CLEANUP],
    PREPARE_SYSTEM: [START_SERVICES, RUN, CLEANUP],
    START_SERVICES: [RUN, CLEANUP],
    RUN: [CLEANUP],
    CLEANUP: [TEARDOWN],
}
_ALL = 'all'
_VALID_FLAG_VALUES = (
    PROVISION,
    PREPARE,
    RUN,
    CLEANUP,
    TEARDOWN,
    PREPARE_SYSTEM,
    INSTALL_PACKAGES,
    START_SERVICES,
    _ALL,
)


_SYNTACTIC_HELP = (
    'A complete benchmark execution consists of {} stages: {}. Possible flag '
    'values include an individual stage, a comma-separated list of stages, or '
    "'all'. If a list of stages is provided, they must be in order without "
    'skipping any stage.'.format(len(STAGES), ', '.join(STAGES))
)


class RunStageParser(flags.ListParser):
  """Parse a string containing PKB run stages.

  See _SYNTACTIC_HELP for more information.
  """

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
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
    stage_list = super().parse(argument)

    if not stage_list:
      raise ValueError(
          'Unable to parse {}. Stage list cannot be empty.'.format(
              repr(argument)
          )
      )

    invalid_items = set(stage_list).difference(_VALID_FLAG_VALUES)
    if invalid_items:
      raise ValueError(
          'Unable to parse {}. Unrecognized stages were found: {}'.format(
              repr(argument), ', '.join(sorted(invalid_items))
          )
      )

    if _ALL in stage_list:
      if len(stage_list) > 1:
        raise ValueError(
            "Unable to parse {}. If 'all' stages are specified, individual "
            'stages cannot also be specified.'.format(repr(argument))
        )
      return list(STAGES)

    previous_stage = stage_list[0]
    for stage in itertools.islice(stage_list, 1, None):
      expected_stages = _NEXT_STAGE.get(previous_stage)
      if not expected_stages:
        raise ValueError(
            "Unable to parse {}. '{}' should be the last stage.".format(
                repr(argument), previous_stage
            )
        )
      if stage not in expected_stages:
        raise ValueError(
            "Unable to parse {}. The stage after '{}' should be one of '{}',"
            " not '{}'.".format(
                repr(argument), previous_stage, expected_stages, stage
            )
        )
      previous_stage = stage

    if PREPARE in stage_list and (
        PREPARE_SYSTEM in stage_list
        or INSTALL_PACKAGES in stage_list
        or START_SERVICES in stage_list
    ):
      raise ValueError(
          "Unable to parse {}. '{}' should not be run with '{}' or '{}' or"
          " '{}'".format(
              repr(argument),
              PREPARE,
              PREPARE_SYSTEM,
              INSTALL_PACKAGES,
              START_SERVICES,
          )
      )

    return stage_list


flags.DEFINE(
    RunStageParser(),
    'run_stage',
    STAGES,
    'The stage or stages of perfkitbenchmarker to run.',
    flags.FLAGS,
    flags.ListSerializer(','),
)
