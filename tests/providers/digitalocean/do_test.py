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
"""Tests for perfkitbenchmarker.providers.digitalocean"""

import unittest
import mock

from perfkitbenchmarker import vm_util
from perfkitbenchmarker.providers.digitalocean import util


class TestDoctlAndParse(unittest.TestCase):
  def testCommandSucceeds(self):
    with mock.patch(vm_util.__name__ + '.IssueCommand',
                    return_value=('{"a": 1, "b": 2}', '', 0)):
      response, retval = util.DoctlAndParse(['foo', 'bar', 'baz'])

      self.assertEqual(response, {'a': 1, 'b': 2})
      self.assertEqual(retval, 0)

  def testCommandFailsWithNull(self):
    with mock.patch(vm_util.__name__ + '.IssueCommand',
                    return_value=(
                        'null{"errors": [{"detail": "foo"}]}', '', 1)):
      response, retval = util.DoctlAndParse(['foo', 'bar', 'baz'])

      self.assertEqual(response, {'errors': [{'detail': 'foo'}]})
      self.assertEqual(retval, 1)

  def testCommandFailsWithoutNull(self):
    with mock.patch(vm_util.__name__ + '.IssueCommand',
                    return_value=('{"errors": [{"detail": "foo"}]}', '', 1)):
      response, retval = util.DoctlAndParse(['foo', 'bar', 'baz'])

      self.assertEqual(response, {'errors': [{'detail': 'foo'}]})
      self.assertEqual(retval, 1)

  def testCommandSucceedsNoOutput(self):
    with mock.patch(vm_util.__name__ + '.IssueCommand',
                    return_value=('', '', 0)):
      response, retval = util.DoctlAndParse(['foo', 'bar', 'baz'])

      self.assertEqual(response, None)
      self.assertEqual(retval, 0)


if __name__ == '__main__':
  unittest.main()
