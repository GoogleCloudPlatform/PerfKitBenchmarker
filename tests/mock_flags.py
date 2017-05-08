# Copyright 2015 PerfKitBenchmarker Authors. All rights reserved.
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
"""Class for mocking a FlagValues object."""

import contextlib

from perfkitbenchmarker import flag_util
from perfkitbenchmarker import flags


FLAGS = flags.FLAGS


class _MockFlag(object):
  """Mock version of a Flag object.

  Attributes:
    present: boolean. In the original Flag object, indicates whether the flag
        was provided a value by the user (in the case of PKB, either via the
        command-line flags or via the benchmark-specific YAML config flag
        overrides).
    validators: Empty tuple. FlagValues.__setattr__ invokes a real Flag's
        validators to validate the new value. When the FlagValues internal
        flag dict is substituted with a _MockFlagDict, this attribute must be
        present for FlagValues.__setattr__ to work, but because it is empty, no
        actual validation occurs.
    value: In the original Flag object, it is either the flag's default value or
        the user-provided value.
  """

  def __init__(self):
    self.present = False
    self.validators = ()
    self.value = None

  def parse(self, argument):
    self.present = True
    self.value = argument


class _MockFlagDict(dict):
  """Mock version of the internal dictionary of a FlagValues object.

  Typically, the internal dictionary of a FlagValues object maps each flag name
  string to a Flag object. This mock maps each flag name to a _MockFlag object
  that is created upon first access.

  Because FlagValues and MockFlags override __getattr__ to search their flag
  dictionaries, care is taken not to create a _MockFlag when searching for
  special attributes like the __copy__ method.
  """

  def __getitem__(self, key):
    if (not super(_MockFlagDict, self).__contains__(key) and
        not key.startswith('__')):
      self[key] = _MockFlag()
    return super(_MockFlagDict, self).__getitem__(key)

  def __contains__(self, item):
    if item.startswith('__'):
      return super(_MockFlagDict, self).__contains__(item)
    return True


class MockFlags(object):
  """Class for mocking a FlagValues object.

  Supports setting a flag value via __setattr__, getting a flag value via
  __getattr__, and getting a _MockFlag via __getitem__.
  """

  def __init__(self):
    super(MockFlags, self).__setattr__('_dict', _MockFlagDict())

  def FlagDict(self):
    return self._dict

  def __setattr__(self, key, value):
    self.FlagDict()[key].value = value

  def __getattr__(self, key):
    flag = self.FlagDict().get(key)
    return flag.value if flag else super(MockFlags, self).__getattr__(key)

  def __getitem__(self, key):
    return self.FlagDict()[key]

  def __contains__(self, item):
    return item in self.FlagDict()


@contextlib.contextmanager
def PatchFlags(mock_flags=None):
  """Patches read and write access to perfkitbenchmarker.flags.FLAGS.

  By patching the underlying FlagValues instance, this method affects all
  modules that have read FLAGS from perfkitbenchmarker.flags. For example, a
  module my_module.py may have the code
      from perfkitbenchmarker import flags
      FLAGS = flags.FLAGS
      ...
      def Func():
        my_flag = FLAGS['cloud']
        my_value = FLAGS.cloud
        FLAGS.cloud = my_override_value
  Within the effect of the PatchFlags contextmanager, calling my_module.Func()
  will cause my_flag and my_value to be initialized from mock_flags rather than
  an actual FlagValues instance. Similarly, mock_flags.cloud will be set with
  my_override_value.

  Args:
    mock_flags: None or MockFlags. If provided, the source of mocked flag
        values. If not provided, a new MockFlags object will be used.

  Yields:
    MockFlags. Either mock_flags or the newly created MockFlags value.
  """
  mock_flags = mock_flags or MockFlags()
  with flag_util.FlagDictSubstitution(FLAGS, mock_flags.FlagDict):
    yield mock_flags


def PatchTestCaseFlags(testcase):
  """Patches access to perfkitbenchmarker.flags.FLAGS for a TestCase.

  Similar to PatchFlags, but only needs to be called once during a test method
  or its setUp method, and remains in effect for the rest of the test method.

  Args:
    testcase: unittest.TestCase. The current test. A cleanup method is
        registered to undo the patch after this test completes.

  Returns:
    MockFlags. The mocked FlagValues object.
  """
  mock_flags = MockFlags()
  substitution = flag_util.FlagDictSubstitution(FLAGS, mock_flags.FlagDict)
  substitution.__enter__()
  testcase.addCleanup(substitution.__exit__)
  return mock_flags
