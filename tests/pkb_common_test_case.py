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

import unittest

from absl.testing import flagsaver

from perfkitbenchmarker import flags
from perfkitbenchmarker import pkb  # pylint:disable=unused-import

FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()


class PkbCommonTestCase(unittest.TestCase):

  def setUp(self):
    saved_flag_values = flagsaver.save_flag_values()
    self.addCleanup(
        flagsaver.restore_flag_values, saved_flag_values)
