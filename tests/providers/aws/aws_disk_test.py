# Copyright 2018 PerfKitBenchmarker Authors. All rights reserved.
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
"""Tests for perfkitbenchmarker.providers.aws.aws_disk."""

import unittest
from absl import flags
from perfkitbenchmarker import errors
from perfkitbenchmarker.providers.aws import aws_disk
from tests import pkb_common_test_case

FLAGS = flags.FLAGS
_COMPONENT = 'test_component'


class AwsDiskSpecTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testDefaults(self):
    spec = aws_disk.AwsDiskSpec(_COMPONENT)
    self.assertIsNone(spec.device_path)
    self.assertIsNone(spec.disk_number)
    self.assertIsNone(spec.disk_size)
    self.assertIsNone(spec.disk_type)
    self.assertIsNone(spec.provisioned_iops)
    self.assertIsNone(spec.provisioned_throughput)
    self.assertIsNone(spec.mount_point)
    self.assertEqual(spec.num_striped_disks, 1)

  def testProvidedValid(self):
    spec = aws_disk.AwsDiskSpec(
        _COMPONENT,
        device_path='test_device_path',
        disk_number=1,
        disk_size=75,
        disk_type='test_disk_type',
        provisioned_iops=1000,
        provisioned_throughput=100,
        mount_point='/mountpoint',
        num_striped_disks=2,
    )
    self.assertEqual(spec.device_path, 'test_device_path')
    self.assertEqual(spec.disk_number, 1)
    self.assertEqual(spec.disk_size, 75)
    self.assertEqual(spec.disk_type, 'test_disk_type')
    self.assertEqual(spec.provisioned_iops, 1000)
    self.assertEqual(spec.provisioned_throughput, 100)
    self.assertEqual(spec.mount_point, '/mountpoint')
    self.assertEqual(spec.num_striped_disks, 2)

  def testProvidedNone(self):
    spec = aws_disk.AwsDiskSpec(
        _COMPONENT, provisioned_iops=None, provisioned_throughput=None
    )
    self.assertIsNone(spec.provisioned_iops)
    self.assertIsNone(spec.provisioned_throughput)

  def testInvalidOptionTypes(self):
    with self.assertRaises(errors.Config.InvalidValue):
      aws_disk.AwsDiskSpec(_COMPONENT, provisioned_iops='ten')

  def testNonPresentFlagsDoNotOverrideConfigs(self):
    FLAGS.provisioned_iops = 2000
    FLAGS.provisioned_throughput = 200
    FLAGS.data_disk_size = 100
    spec = aws_disk.AwsDiskSpec(
        _COMPONENT,
        FLAGS,
        disk_size=75,
        provisioned_iops=1000,
        provisioned_throughput=150,
    )
    self.assertEqual(spec.disk_size, 75)
    self.assertEqual(spec.provisioned_iops, 1000)
    self.assertEqual(spec.provisioned_throughput, 150)

  def testPresentFlagsOverrideConfigs(self):
    FLAGS['provisioned_iops'].parse(2000)
    FLAGS['provisioned_throughput'].parse(200)
    FLAGS['data_disk_size'].parse(100)
    spec = aws_disk.AwsDiskSpec(
        _COMPONENT,
        FLAGS,
        disk_size=75,
        provisioned_iops=1000,
        provisioned_throughput=150,
    )
    self.assertEqual(spec.disk_size, 100)
    self.assertEqual(spec.provisioned_iops, 2000)
    self.assertEqual(spec.provisioned_throughput, 200)


class AwsDiskTestCase(pkb_common_test_case.PkbCommonTestCase):

  def testGeneratedDeviceLetters(self):
    allowed_device_letters = [
        'e',
        'f',
        'g',
        'h',
        'i',
        'j',
        'k',
        'l',
        'm',
        'n',
        'o',
        'p',
        'q',
        'r',
        's',
        't',
        'u',
        'v',
        'w',
        'x',
        'y',
        'z',
        'ab',
        'ac',
        'ad',
        'ae',
        'af',
        'ag',
        'ah',
        'ai',
        'aj',
        'ak',
        'al',
        'am',
        'an',
        'ao',
        'ap',
        'aq',
        'ar',
        'as',
        'at',
        'au',
        'av',
        'aw',
        'ax',
        'ay',
        'az',
        'ba',
        'bb',
        'bc',
        'bd',
        'be',
        'bf',
        'bg',
        'bh',
        'bi',
        'bj',
        'bk',
        'bl',
        'bm',
        'bn',
        'bo',
        'bp',
        'bq',
        'br',
        'bs',
        'bt',
        'bu',
        'bv',
        'bw',
        'bx',
        'by',
        'bz',
        'ca',
        'cb',
        'cc',
        'cd',
        'ce',
        'cf',
        'cg',
        'ch',
        'ci',
        'cj',
        'ck',
        'cl',
        'cm',
        'cn',
        'co',
        'cp',
        'cq',
        'cr',
        'cs',
        'ct',
        'cu',
        'cv',
        'cw',
        'cx',
        'cy',
        'cz',
        'da',
        'db',
        'dc',
        'dd',
        'de',
        'df',
        'dg',
        'dh',
        'di',
        'dj',
        'dk',
        'dl',
        'dm',
        'dn',
        'do',
        'dp',
        'dq',
        'dr',
        'ds',
        'dt',
        'du',
        'dv',
        'dw',
        'dx',
    ]
    allowed_device_letters.sort()
    aws_disk.AwsDisk.GenerateDeviceLetter('abcde')
    generated_letters = list(
        aws_disk.AwsDisk.available_device_letters_by_vm['abcde']
    )
    self.assertCountEqual(
        generated_letters,
        allowed_device_letters,
    )


if __name__ == '__main__':
  unittest.main()
