# Copyright 2014 Google Inc. All rights reserved.
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
"""Tests for perfkitbenchmarker.data."""

import os
import shutil
import tempfile
import unittest

from perfkitbenchmarker import data


class FileResourceLoaderTestCase(unittest.TestCase):

  def setUp(self):
    self.temp_dir = tempfile.mkdtemp(prefix='pkb-test-')
    self.instance = data.FileResourceLoader(self.temp_dir)

  def tearDown(self):
    shutil.rmtree(self.temp_dir)

  def _Create(self, file_name, content=''):
    file_path = os.path.join(self.temp_dir, file_name)
    with open(file_path, 'w') as fp:
      fp.write(content)
    return file_path

  def testResourcePath_NonExistantResource(self):
    self.assertListEqual([], os.listdir(self.temp_dir))
    self.assertRaises(data.ResourceNotFound,
                      self.instance.ResourcePath,
                      'fake.txt')

  def testResourcePath_ExtantResource(self):
    file_name = 'test.txt'
    file_path = self._Create(file_name)
    self.assertEqual(file_path, self.instance.ResourcePath(file_name))

  def testResourceExists_NonExistantResource(self):
    self.assertFalse(self.instance.ResourceExists('fake.txt'))

  def testResourceExists_ExtantResource(self):
    file_name = 'test.txt'
    self._Create(file_name)
    self.assertTrue(self.instance.ResourceExists(file_name))


class PackageResourceLoaderTestCase(unittest.TestCase):

  def setUp(self):
    self.instance = data.PackageResourceLoader(data.__name__)

  def testResourcePath_NonExistantResource(self):
    self.assertRaises(data.ResourceNotFound,
                      self.instance.ResourcePath,
                      'fake.txt')

  def testResourcePath_ExtantResource(self):
    file_name = '__init__.py'
    path = self.instance.ResourcePath(file_name)
    self.assertEqual(file_name, os.path.basename(path))
    self.assertTrue(os.path.exists(path))

  def testResourceExists_NonExistantResource(self):
    self.assertFalse(self.instance.ResourceExists('fake.txt'))

  def testResourceExists_ExtantResource(self):
    file_name = '__init__.py'
    self.assertTrue(self.instance.ResourceExists(file_name))
