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
"""Tests for perfkitbenchmarker.requirements."""

import contextlib
import StringIO
import unittest

import mock

from perfkitbenchmarker import errors
from perfkitbenchmarker import requirements


_PATH = 'path'


class _MockOpenRequirementsFile(object):

  def __init__(self, file_content):
    self._io = StringIO.StringIO(file_content)

  def __enter__(self):
    return self._io

  def __exit__(self, *unused_args, **unused_kwargs):
    pass


class CheckRequirementsTestCase(unittest.TestCase):

  @contextlib.contextmanager
  def _MockOpen(self, file_content):
    mocked_open = _MockOpenRequirementsFile(file_content)
    with mock.patch(requirements.__name__ + '.open', return_value=mocked_open):
      yield

  def testFulfilledRequirement(self):
    requirements_content = """
    # Comment line, blank line, and a fulfilled requirement.

    python-gflags>=2.0
    """
    with self._MockOpen(requirements_content):
      requirements._CheckRequirements(_PATH)

  def testMissingPackage(self):
    requirements_content = """
    # This is not a real package.
    perfkitbenchmarker-fake-package>=1.2
    """
    with self._MockOpen(requirements_content):
      with self.assertRaises(errors.Setup.PythonPackageRequirementUnfulfilled):
        requirements._CheckRequirements(_PATH)

  def testInstalledVersionLowerThanRequirement(self):
    requirements_content = """
    # The version of the installed python-gflags will be less than 42.
    python-gflags>=42
    """
    with self._MockOpen(requirements_content):
      with self.assertRaises(errors.Setup.PythonPackageRequirementUnfulfilled):
        requirements._CheckRequirements(_PATH)

  def testInstalledVersionGreaterThanRequirement(self):
    requirements_content = """
    # The version of the installed python-gflags will be greater than 0.5.
    python-gflags==0.5
    """
    with self._MockOpen(requirements_content):
      with self.assertRaises(errors.Setup.PythonPackageRequirementUnfulfilled):
        requirements._CheckRequirements(_PATH)


class CheckBasicRequirementsTestCase(unittest.TestCase):

  def testAllRequirementsFulfilled(self):
    requirements.CheckBasicRequirements()


class CheckProviderRequirementsTestCase(unittest.TestCase):

  def testNoRequirementsFile(self):
    # If a provider does not have a requirements file, then there can be no
    # unfulfilled requirement.
    requirements.CheckProviderRequirements('fakeprovider')

  def testUnfulfilledRequirements(self):
    # AWS does have a requirements file, but it contains packages that are not
    # installed as part of the test environment.
    with self.assertRaises(errors.Setup.PythonPackageRequirementUnfulfilled):
      requirements.CheckProviderRequirements('aws')


if __name__ == '__main__':
  unittest.main()
