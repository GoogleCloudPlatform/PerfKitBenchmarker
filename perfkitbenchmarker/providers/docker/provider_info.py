# Copyright 2019 PerfKitBenchmarker Authors. All rights reserved.
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

"""Provider info for Docker."""

from perfkitbenchmarker import provider_info
from perfkitbenchmarker import providers


class DockerProviderInfo(provider_info.BaseProviderInfo):
  SUPPORTED_BENCHMARKS = []

  UNSUPPORTED_BENCHMARKS = ['sysbench']

  CLOUD = providers.DOCKER

  @classmethod
  def IsBenchmarkSupported(cls, benchmark):
    if benchmark in cls.UNSUPPORTED_BENCHMARKS:
      return False
    else:
      return True
