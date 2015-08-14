# Copyright 2015 Google Inc. All rights reserved.
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

import logging
import time

from perfkitbenchmarker import flags
from perfkitbenchmarker import disk
from perfkitbenchmarker.centurylink import util

FLAGS = flags.FLAGS

class CenturylinkDisk(disk.BaseDisk):

  def __init__(self, disk_spec, api_key, api_pass, server_name, disk_size):
    super(CenturylinkDisk, self).__init__(disk_spec)
    self.api_key = api_key
    self.api_pass = api_pass
    self.server_name = server_name
    self.disk_size = disk_size

  def _Create(self):

    create_cmd = "clc --v1-api-key %s --v1-api-passwd %s servers add-disk \
    --server %s \
    --size %d" % (self.api_key, self.api_pass, self.server_name, self.disk_size)

    vm_util.IssueCommand(create_cmd)

  def _Exists(self):
    pass

  def Attach(self, vm):
    pass

  def Detach(self):
    pass
