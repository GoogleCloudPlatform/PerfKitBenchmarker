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

import mock

from perfkitbenchmarker import flag_util
from perfkitbenchmarker import flags


FLAGS = flags.FLAGS


class MockFlags(object):
  """Class for mocking a FlagValues object.

  Supports setting flag values via __setattr__, getting flag values via
  __getattr__, and getting mock Flag-like objects via __getitem__, where the
  Flag-like object supports the 'present' and 'value' attributes.

  Attempting to get a Flag that does not exist will generate a new MagicMock
  with the 'present' attribute initialized to False and the 'value' attribute
  initialized to None.
  """

  def __init__(self):
    super(MockFlags, self).__setattr__('_dict', {})

  def __setattr__(self, key, value):
    mock_flag = self[key]
    mock_flag.present = True
    mock_flag.value = value

  def __getattr__(self, key):
    return self[key].value

  def __getitem__(self, key):
    if key.startswith('__'):
      return super(MockFlags, self).__getitem__(key)
    if key not in self._dict:
      mock_flag = mock.MagicMock()
      mock_flag.present = False
      mock_flag.value = None
      self._dict[key] = mock_flag
    return self._dict[key]

  def __contains__(self, item):
    if item.startswith('__'):
      return super(MockFlags, self).__contains__(item)
    return True


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
  with flag_util.FlagDictSubstitution(FLAGS, lambda: mock_flags):
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
  substitution = flag_util.FlagDictSubstitution(FLAGS, lambda: mock_flags)
  substitution.__enter__()
  testcase.addCleanup(substitution.__exit__)
  return mock_flags
