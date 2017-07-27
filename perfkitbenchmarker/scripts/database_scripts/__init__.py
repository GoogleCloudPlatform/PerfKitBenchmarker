# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
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
"""Contains tools that deliver information about the state of the database.

Currently, these tools are made to be used with mysql_service benchmark but may
be expanded in the future to support other benchmarks.

Launch_mysql_service: Wrapper script to drive multiple runs of mysql_service.
Launch_driver: Driver script that configures mysql_service runs.
plot_sysbench_results: Creates per second performance graphs from stderr.
plot_scatter_points: Creates a gnuplot info file for plot_sysbench_results.
"""
