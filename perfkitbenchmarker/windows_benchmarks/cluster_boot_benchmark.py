# Copyright 2014 PerfKitBenchmarker Authors. All rights reserved.
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

"""Records the time required to boot a cluster of VMs.

This is a copy of the Linux cluster_boot benchmark. In the future, we need to
figure out a way for benchmarks which work on both Windows and Linux to share
code.
"""

from perfkitbenchmarker.linux_benchmarks import cluster_boot_benchmark

BENCHMARK_NAME = cluster_boot_benchmark.BENCHMARK_NAME
BENCHMARK_CONFIG = cluster_boot_benchmark.BENCHMARK_CONFIG
GetConfig = cluster_boot_benchmark.GetConfig
Prepare = cluster_boot_benchmark.Prepare
Run = cluster_boot_benchmark.Run
Cleanup = cluster_boot_benchmark.Cleanup
