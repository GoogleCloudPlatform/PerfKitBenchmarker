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
"""Module containing classes related to DigitalOcean VM networking."""

from perfkitbenchmarker import network


class DigitalOceanFirewall(network.BaseFirewall):
  """A dummy firewall for DigitalOcean, this does nothing."""
  pass


class DigitalOceanNetwork(network.BaseNetwork):
  """Object representing a DigitalOcean Network."""

  def Create(self):
    """Creates the actual network."""
    pass

  def Delete(self):
    """Deletes the actual network."""
    pass
