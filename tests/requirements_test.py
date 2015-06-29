# Copyright 2015 Google Inc. All rights reserved.
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

"""Verifies that test requirements are satisfied."""

import os
import pkg_resources
import psutil
import sys
import unittest

class CheckResourcesTestCase(unittest.TestCase):
  def testRequirements(self):
    testreq_file = os.path.join(
        os.path.dirname(__file__), os.pardir, 'test-requirements.txt')
    expected_modules = set(['psutil'])
    checked_modules = set()
    with open(testreq_file) as requirements:
      for line in requirements:
        try:
          req = pkg_resources.Requirement.parse(line)
          # There's no obvious mapping from project name as used in
          # requirements to importable package names. For now, just
          # manually import packages to check above.
          module = sys.modules.get(req.project_name)
          if not module:
            continue
          # Version strings aren't standardized, may need more variants.
          actual_version = getattr(module, '__version__', None)
          if actual_version and actual_version not in req:
            raise Exception('Module mismatch: Expected %s, got %s.' % (
               req, actual_version))
          checked_modules.add(req.project_name)
        except ValueError:
          # Empty or unparseable line
          pass
    missing_modules = expected_modules - checked_modules
    if missing_modules:
      raise Exception('Checks not done for modules: %s' % (
        missing_modules))
