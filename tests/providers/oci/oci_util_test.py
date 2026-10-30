# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.oci.util."""

# pylint: disable=protected-access

import json
import unittest

from absl.testing import flagsaver
from absl.testing import parameterized
import mock
from perfkitbenchmarker import context
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.oci import util
from tests import pkb_common_test_case

_AVAILABILITY_DOMAINS = json.dumps({
    'data': [
        {'name': 'Uocm:US-ASHBURN-AD-1'},
        {'name': 'Uocm:US-ASHBURN-AD-2'},
        {'name': 'Uocm:US-ASHBURN-AD-3'},
    ]
})


class OciUtilTest(pkb_common_test_case.PkbCommonTestCase):
  """Tests for the OCI CLI helpers in util."""

  @parameterized.named_parameters(
      ('region', 'us-ashburn-1', 'us-ashburn-1', None),
      ('zone_with_ad', 'us-ashburn-1-ad-2', 'us-ashburn-1', 2),
      ('single_ad_region', 'ap-mumbai-1-ad-1', 'ap-mumbai-1', 1),
  )
  def test_parse_zone(self, zone, expected_region, expected_ad_index):
    """Tests parse zone."""
    self.assertEqual(expected_region, util.GetRegionFromZone(zone))
    self.assertEqual(
        expected_ad_index, util.GetAvailabilityDomainIndexFromZone(zone)
    )

  @flagsaver.flagsaver(oci_profile='MYPROFILE')
  def test_oci_command(self):
    """Tests oci command."""
    self.assertEqual(
        [
            'oci',
            'network',
            'vcn',
            'get',
            '--region',
            'us-ashburn-1',
            '--profile',
            'MYPROFILE',
        ],
        util.OciCommand(['network', 'vcn', 'get'], 'us-ashburn-1'),
    )

  def test_oci_command_without_region(self):
    """Tests oci command without region."""
    self.assertEqual(
        ['oci', 'iam', 'region', 'list', '--profile', 'DEFAULT'],
        util.OciCommand(['iam', 'region', 'list']),
    )

  def test_get_compartment_args_unset(self):
    """Tests get compartment args unset."""
    self.assertEqual([], util.GetCompartmentArgs())

  @flagsaver.flagsaver(oci_compartment_id='ocid1.compartment.oc1..test')
  def test_get_compartment_args_set(self):
    """Tests get compartment args set."""
    self.assertEqual(
        ['--compartment-id', 'ocid1.compartment.oc1..test'],
        util.GetCompartmentArgs(),
    )

  def test_issue_oci_command_parses_json(self):
    """Tests issue oci command parses json."""
    self.MockIssueCommand({'vcn get': [('{"data": {"id": "vcn1"}}', '', 0)]})
    response, _, retcode = util.IssueOciCommand(['network', 'vcn', 'get'])
    self.assertEqual({'data': {'id': 'vcn1'}}, response)
    self.assertEqual(0, retcode)

  def test_issue_oci_command_no_output(self):
    """Tests issue oci command no output."""
    self.MockIssueCommand({'vcn delete': [('', '', 0)]})
    response, _, _ = util.IssueOciCommand(['network', 'vcn', 'delete'])
    self.assertIsNone(response)

  def test_issue_oci_command_failure_with_non_json_output(self):
    """Tests issue oci command failure with non json output."""
    self.MockIssueCommand({'vcn get': [('ServiceError', 'NotFound', 1)]})
    response, stderr, retcode = util.IssueOciCommand(
        ['network', 'vcn', 'get'], raise_on_failure=False
    )
    self.assertIsNone(response)
    self.assertEqual('NotFound', stderr)
    self.assertEqual(1, retcode)

  @parameterized.named_parameters(
      (
          'available',
          ('{"data": {"lifecycle-state": "AVAILABLE"}}', '', 0),
          True,
      ),
      ('running', ('{"data": {"lifecycle-state": "RUNNING"}}', '', 0), True),
      (
          'terminating',
          ('{"data": {"lifecycle-state": "TERMINATING"}}', '', 0),
          False,
      ),
      (
          'terminated',
          ('{"data": {"lifecycle-state": "TERMINATED"}}', '', 0),
          False,
      ),
      ('not_found', ('', 'NotAuthorizedOrNotFound', 1), False),
  )
  def test_resource_exists(self, response, expected):
    """Tests resource exists."""
    self.MockIssueCommand({'vcn get': [response]})
    self.assertEqual(
        expected,
        util.ResourceExists(['network', 'vcn', 'get'], 'us-ashburn-1'),
    )

  def test_get_availability_domain_defaults_to_first(self):
    """Tests get availability domain defaults to first."""
    self.MockIssueCommand(
        {'availability-domain list': [(_AVAILABILITY_DOMAINS, '', 0)]}
    )
    self.assertEqual(
        'Uocm:US-ASHBURN-AD-1', util.GetAvailabilityDomain('us-ashburn-1')
    )

  def test_get_availability_domain_from_zone_suffix(self):
    """Tests get availability domain from zone suffix."""
    mock_cmd = self.MockIssueCommand(
        {'availability-domain list': [(_AVAILABILITY_DOMAINS, '', 0)]}
    )
    self.assertEqual(
        'Uocm:US-ASHBURN-AD-3',
        util.GetAvailabilityDomain('us-ashburn-1-ad-3'),
    )
    self.assertIn('--region us-ashburn-1 ', mock_cmd.all_commands)

  def test_get_availability_domain_invalid_suffix(self):
    """Tests get availability domain invalid suffix."""
    self.MockIssueCommand(
        {'availability-domain list': [(_AVAILABILITY_DOMAINS, '', 0)]}
    )
    with self.assertRaises(errors.Config.InvalidValue):
      util.GetAvailabilityDomain('us-ashburn-1-ad-4')

  @flagsaver.flagsaver(oci_availability_domain='Uocm:US-ASHBURN-AD-2')
  def test_get_availability_domain_flag_overrides_zone(self):
    """Tests get availability domain flag overrides zone."""
    mock_cmd = self.MockIssueCommand({})
    self.assertEqual(
        'Uocm:US-ASHBURN-AD-2',
        util.GetAvailabilityDomain('us-ashburn-1-ad-3'),
    )
    mock_cmd.func_to_mock.assert_not_called()

  @flagsaver.flagsaver(oci_compartment_id='ocid1.compartment.oc1..test')
  def test_get_image_id(self):
    """Tests get image id."""
    mock_cmd = self.MockIssueCommand({
        'image list': [(
            json.dumps({'data': [{'id': 'ocid1.image.new'}, {'id': 'old'}]}),
            '',
            0,
        )]
    })
    self.assertEqual(
        'ocid1.image.new',
        util.GetImageId(
            'Oracle Linux', '9', 'VM.Standard.E5.Flex', 'us-ashburn-1'
        ),
    )
    cmd = mock_cmd.func_to_mock.call_args[0][0]
    self.assertEqual('Oracle Linux', cmd[cmd.index('--operating-system') + 1])
    self.assertEqual('9', cmd[cmd.index('--operating-system-version') + 1])
    self.assertEqual('VM.Standard.E5.Flex', cmd[cmd.index('--shape') + 1])
    self.assertEqual('DESC', cmd[cmd.index('--sort-order') + 1])
    self.assertEqual(
        'ocid1.compartment.oc1..test', cmd[cmd.index('--compartment-id') + 1]
    )

  def test_get_image_id_no_images(self):
    """Tests get image id no images."""
    self.MockIssueCommand({'image list': [('{"data": []}', '', 0)]})
    with self.assertRaises(errors.Config.InvalidValue):
      util.GetImageId(
          'Oracle Linux', '9', 'VM.Standard.A1.Flex', 'us-ashburn-1'
      )

  def test_get_image_id_from_name(self):
    """Tests get image id from name."""
    mock_cmd = self.MockIssueCommand(
        {'image list': [('{"data": [{"id": "ocid1.image.custom"}]}', '', 0)]}
    )
    self.assertEqual(
        'ocid1.image.custom',
        util.GetImageIdFromName(
            'my-image', 'VM.Standard.E5.Flex', 'us-ashburn-1'
        ),
    )
    cmd = mock_cmd.func_to_mock.call_args[0][0]
    self.assertEqual('my-image', cmd[cmd.index('--display-name') + 1])

  def test_get_image_id_from_name_not_found(self):
    """Tests get image id from name not found."""
    self.MockIssueCommand({'image list': [('{"data": []}', '', 0)]})
    with self.assertRaises(errors.Config.InvalidValue):
      util.GetImageIdFromName('missing', 'VM.Standard.E5.Flex', 'us-ashburn-1')

  def test_make_formatted_default_tags_without_benchmark_spec(self):
    """Tests make formatted default tags without benchmark spec."""
    self.assertEqual('{}', util.MakeFormattedDefaultTags())

  def test_make_formatted_default_tags(self):
    """Tests make formatted default tags."""
    benchmark_spec = mock.Mock()
    benchmark_spec.GetResourceTags.return_value = {
        'owner': 'perfkit',
        'benchmark': 'iperf',
    }
    context.SetThreadBenchmarkSpec(benchmark_spec)
    self.assertEqual(
        {'owner': 'perfkit', 'benchmark': 'iperf'},
        json.loads(util.MakeFormattedDefaultTags(timeout_minutes=10)),
    )
    benchmark_spec.GetResourceTags.assert_called_once_with(10)

  @parameterized.named_parameters(
      ('simple', 'vcn', 'abc123', 'vcnabc123'),
      ('strips_symbols', 'sub', 'ab-c_1.2', 'subabc12'),
      ('truncates', 'vcn', 'abcdefghijklmnop', 'vcnabcdefghijkl'),
      ('lowercases', 'vcn', 'ABC', 'vcnabc'),
  )
  def test_make_dns_label(self, prefix, name, expected):
    """Tests make dns label."""
    self.assertEqual(expected, util.MakeDnsLabel(prefix, name))


if __name__ == '__main__':
  unittest.main()
