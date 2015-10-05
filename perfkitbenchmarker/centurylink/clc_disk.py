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

import clc
from perfkitbenchmarker import flags
from perfkitbenchmarker import disk

FLAGS = flags.FLAGS


class CenturylinkDisk(disk.BaseDisk):

    def __init__(self, disk_spec):
        super(CenturylinkDisk, self).__init__(disk_spec)

    def _Create(self):

        print "*" * 25 + " Creating ScratchDisk - _Create" + "*" * 25
        clc.v2.Server(self.server_name).Disks().Add(size=1, path=None, type="raw").WaitUntilComplete()

    def _Exists(self):
        pass

    def Attach(self, vm):
        pass

    def Detach(self):
        pass
