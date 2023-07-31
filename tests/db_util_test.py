# Copyright 2023 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker db_util."""

import string
import unittest

from perfkitbenchmarker import db_util


class DbUtilTest(unittest.TestCase):
  """implements unit tests for db_util.py."""

  def testGenerateRandomDbPassword(self):
    """validates GenerateRandomDbPassword as per upstream requirements.

    The source doc
    https://learn.microsoft.com/en-us/sql/relational-databases/security/password-policy?view=sql-server-ver15.
    requires 3 out 4 of [lower, upper, digits, symbols].  This implementation
    generates [lower, upper, digits] only.
    """
    # Try 1000 random passwords.
    for _ in range(1000):
      password = db_util.GenerateRandomDbPassword()
      self.assertGreaterEqual(
          len(password),
          8,
          f'password {password} is too short',
      )
      self.assertTrue(
          set(password) & set(string.ascii_uppercase),
          f'password {password} must have at least one uppercase letter',
      )
      self.assertTrue(
          set(password) & set(string.ascii_lowercase),
          f'password {password} must have at least one lowercase letter',
      )
      self.assertTrue(
          set(password) & set(string.digits),
          f'password {password} must have at least one digit',
      )


if __name__ == '__main__':
  unittest.main()
