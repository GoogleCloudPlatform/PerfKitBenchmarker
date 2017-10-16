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

from collections import deque
import contextlib
import StringIO
import unittest

import mock
import pkg_resources

from perfkitbenchmarker import errors
from perfkitbenchmarker import requirements


_PATH = 'dir/file'


class _MockOpenRequirementsFile(object):

  def __init__(self, *args):
    self._io = deque(StringIO.StringIO(a) for a in args)

  def __enter__(self):
    return self._io.popleft()

  def __exit__(self, *unused_args, **unused_kwargs):
    pass


class CheckRequirementsTestCase(unittest.TestCase):

  @contextlib.contextmanager
  def _MockOpen(self, *args):
    mocked_file = _MockOpenRequirementsFile(*args)
    with mock.patch.object(requirements, 'open', return_value=mocked_file) as m:
      yield m

  def testFulfilledRequirement(self):
    requirements_content = """
    # Comment line, blank line, and a fulfilled requirement.

    absl-py
    """
    with self._MockOpen(requirements_content) as mocked_open:
      requirements._CheckRequirements(_PATH)
    mocked_open.assert_called_once_with('dir/file', 'rb')

  def testMissingPackage(self):
    requirements_content = """
    # This is not a real package.
    perfkitbenchmarker-fake-package>=1.2
    """
    with self._MockOpen(requirements_content) as mocked_open:
      with self.assertRaises(errors.Setup.PythonPackageRequirementUnfulfilled):
        requirements._CheckRequirements(_PATH)
    mocked_open.assert_called_once_with('dir/file', 'rb')

  def testInstalledVersionLowerThanRequirement(self):
    requirements_content = """
    # The version of the installed absl-py will be less than 42.
    absl-py>=42
    """
    with self._MockOpen(requirements_content) as mocked_open:
      with self.assertRaises(errors.Setup.PythonPackageRequirementUnfulfilled):
        requirements._CheckRequirements(_PATH)
    mocked_open.assert_called_once_with('dir/file', 'rb')

  def testInstalledVersionGreaterThanRequirement(self):
    requirements_content = """
    # The version of the installed absl-py will be greater than 0.0.1.
    absl-py==0.0.1
    """
    with self._MockOpen(requirements_content) as mocked_open:
      with self.assertRaises(errors.Setup.PythonPackageRequirementUnfulfilled):
        requirements._CheckRequirements(_PATH)
    mocked_open.assert_called_once_with('dir/file', 'rb')

  def testIncludedFiles(self):
    top_file = """
    package-0
    -rsubfile0
    package-1>=2.0
    -rsubfile1
    package-2
    """
    subfile0 = """
    package-3
    -rsubdir/subfile2
    package-4
    -r../subfile3
    package-5
    """
    subfile1 = """
    package-6
    """
    with self._MockOpen(top_file, subfile0, '', '', subfile1) as mocked_open:
      with mock.patch.object(pkg_resources, 'require') as mocked_require:
        requirements._CheckRequirements(_PATH)
    mocked_open.assert_has_calls((
        mock.call('dir/file', 'rb'), mock.call('dir/subfile0', 'rb'),
        mock.call('dir/subdir/subfile2', 'rb'),
        mock.call('dir/../subfile3', 'rb'), mock.call('dir/subfile1', 'rb')))
    mocked_require.assert_has_calls(map(mock.call, (
        'package-0', 'package-3', 'package-4', 'package-5',
        'package-1>=2.0', 'package-6', 'package-2')))


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
