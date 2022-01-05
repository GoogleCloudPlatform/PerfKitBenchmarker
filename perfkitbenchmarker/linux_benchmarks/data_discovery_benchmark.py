# Copyright 2022 PerfKitBenchmarker Authors. All rights reserved.
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
"""This benchmark reports the duration of data discovery in a data lake."""

from typing import Any, Dict, List

from perfkitbenchmarker import benchmark_spec as bm_spec
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'data_discovery'
BENCHMARK_CONFIG = """
data_discovery:
  description: data_discovery benchmark
  data_discovery_service:
    cloud: AWS
    service_type: glue
"""


def GetConfig(user_config: Dict[Any, Any]) -> Dict[Any, Any]:
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(_: bm_spec.BenchmarkSpec) -> None:
  pass


def Run(benchmark_spec: bm_spec.BenchmarkSpec) -> List[sample.Sample]:
  """Runs the data discovery benchmark.

  This benchmark measures the time it took for the data discovery service to
  discover the data at the location given in the --data_discovery_path flag and
  reports it as a sample.

  Args:
    benchmark_spec: Spec needed to run the data discovery benchmark.

  Returns:
    A list of samples, comprised of the data discovery duration in seconds.
  """
  discovery_duration = benchmark_spec.data_discovery_service.DiscoverData()
  return [
      sample.Sample('data_discovery_duration', discovery_duration, 'seconds',
                    benchmark_spec.data_discovery_service.GetMetadata())]


def Cleanup(_: bm_spec.BenchmarkSpec) -> None:
  pass
