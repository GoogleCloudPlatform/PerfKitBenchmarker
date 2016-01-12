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
"""Tests for perfkitbenchmarker.configs.spec."""

import unittest

import mock

from perfkitbenchmarker.configs import option_decoders
from perfkitbenchmarker.configs import spec


_COMPONENT = 'test_component'


class _TestFixedDecodeOrderDecoder(option_decoders.IntDecoder):
  """Decoder used as part of a test that spec option decode order is fixed."""

  def Decode(self, callback_function, *args, **kwargs):
    callback_function(self.option)


class _TestFixedDecodeOrderSpec(spec.BaseSpec):
  """Spec used as part of a test that spec option decode order is fixed."""

  @classmethod
  def _GetOptionDecoderConstructions(cls):
    return {str(i): (_TestFixedDecodeOrderDecoder, {}) for i in range(100)}


class BaseSpecFixedDecodeOrderTestCase(unittest.TestCase):

  def setUp(self):
    super(BaseSpecFixedDecodeOrderTestCase, self).setUp()
    self.config_option_names = [str(i) for i in range(100)]

  def testSuccessfulDecode(self):
    expected_decode_call_order = sorted(self.config_option_names)
    observed_decode_call_order = []
    callback = observed_decode_call_order.append
    config = {config_option_name: callback
              for config_option_name in self.config_option_names}
    _TestFixedDecodeOrderSpec(_COMPONENT, **config)
    self.assertEqual(observed_decode_call_order, expected_decode_call_order)

  def testFailedDecode(self):
    callback = mock.MagicMock(side_effect=ValueError)
    config = {config_option_name: callback
              for config_option_name in self.config_option_names}
    # Only the first OptionDecoder's Decode method should be called.
    with self.assertRaises(ValueError):
      _TestFixedDecodeOrderSpec(_COMPONENT, **config)
    self.assertEqual(len(callback.mock_calls), 1)
    callback.assert_called_once_with('0')


if __name__ == '__main__':
  unittest.main()
