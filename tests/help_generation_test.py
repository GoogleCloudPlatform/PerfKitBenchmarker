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

"""Test that we can generate help for PKB."""

import os
import unittest

from perfkitbenchmarker import flags


class HelpTest(unittest.TestCase):
  def testHelp(self):
    # Test that help generation finishes without errors
    flags.GLOBAL_FLAGS.GetHelp()


class HelpXMLTest(unittest.TestCase):
  def testHelpXML(self):
    with open(os.devnull, 'w') as out:
      flags.GLOBAL_FLAGS.WriteHelpInXMLFormat(outfile=out)
