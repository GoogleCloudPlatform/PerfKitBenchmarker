# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Common base class for PKB unittests."""

import subprocess

from absl.testing import absltest
from absl.testing import flagsaver

import mock

from perfkitbenchmarker import flags
from perfkitbenchmarker import pkb  # pylint:disable=unused-import
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()


class PkbCommonTestCase(absltest.TestCase):
  """Test case class for PKB.

  Contains common functions shared by PKB test cases.
  """

  def setUp(self):
    super(PkbCommonTestCase, self).setUp()
    saved_flag_values = flagsaver.save_flag_values()
    self.addCleanup(flagsaver.restore_flag_values, saved_flag_values)

  # TODO(user): Extend MockIssueCommand to support multiple calls to
  # vm_util.IssueCommand
  def MockIssueCommand(self, stdout, stderr, retcode):
    """Mocks function calls inside vm_util.IssueCommand.

    Mocks subproccess.Popen and _ReadIssueCommandOutput in IssueCommand.
    This allows the logic of IssueCommand to run and returns the given
    stdout, stderr when IssueCommand is called.

    Args:
      stdout: String. Output from standard output
      stderr: String. Output from standard error
      retcode: Int. Return code from running the command.
    """

    p = mock.patch(
        'subprocess.Popen', spec=subprocess.Popen)
    cmd_output = mock.patch.object(vm_util, '_ReadIssueCommandOutput')

    self.addCleanup(p.stop)
    self.addCleanup(cmd_output.stop)

    p.start().return_value.returncode = retcode
    cmd_output.start().side_effect = [(stdout, stderr)]
