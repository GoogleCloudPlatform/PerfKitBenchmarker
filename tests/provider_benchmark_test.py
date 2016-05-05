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

import mock

from perfkitbenchmarker import providers
from perfkitbenchmarker import provider_info


class ProviderBenchmarkChecks(unittest.TestCase):

  def setUp(self):
    p = mock.patch.object(providers, '_imported_providers', new=set())
    p.start()
    self.addCleanup(p.stop)

  def _VerifyProviderBenchmarkSupport(self, cloud, benchmark, support_expected):
    providers.LoadProvider(cloud)
    provider_info_class = provider_info.GetProviderInfoClass(cloud)
    supported = provider_info_class.IsBenchmarkSupported(benchmark)
    fmt_args = ('', ' not') if support_expected else (' not', '')
    self.assertEqual(supported, support_expected, (
        'Expected provider {provider} {0}to support benchmark {benchmark}, but '
        'it did{1}.'.format(*fmt_args, provider=cloud, benchmark=benchmark)))

  def testIperfSupport(self):
    expected = {providers.GCP: True, providers.DIGITALOCEAN: True}
    for cloud, support_expected in expected.iteritems():
      self._VerifyProviderBenchmarkSupport(cloud, 'iperf', support_expected)

  def testMYSQLSupport(self):
    expected = {providers.GCP: True, providers.DIGITALOCEAN: False}
    for cloud, support_expected in expected.iteritems():
      self._VerifyProviderBenchmarkSupport(cloud, 'mysql_service',
                                           support_expected)
