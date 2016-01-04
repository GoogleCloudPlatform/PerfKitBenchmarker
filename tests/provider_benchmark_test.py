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

"""Tests for perfkitbenchmarker.providers"""

import unittest

from perfkitbenchmarker import providers
from perfkitbenchmarker import provider_info


class ProviderBenchmarkChecks(unittest.TestCase):

  def testPingSupported(self):
    for cloud in providers.VALID_CLOUDS:
      providers.LoadProvider(cloud.lower())
      ThisProviderInfoClass = provider_info.GetProviderInfoClass(cloud)
      self.assertTrue(ThisProviderInfoClass.IsBenchmarkSupported('iperf'),
                      'provider {0} does not support iperf'.format(
                          cloud))

  def testMYSQLSupport(self):
    for cloud in providers.VALID_CLOUDS:
      providers.LoadProvider(cloud.lower())
      ThisProviderInfoClass = provider_info.GetProviderInfoClass(cloud)
      if (cloud == providers.AWS or cloud == providers.GCP):
        self.assertTrue(ThisProviderInfoClass.IsBenchmarkSupported(
            'mysql_service'))
      else:
        self.assertFalse(ThisProviderInfoClass.IsBenchmarkSupported(
            'mysql_service'),
            'Cloud {0} is not supposed to support mysql_service {1}'
            .format(cloud, ThisProviderInfoClass.__name__))
