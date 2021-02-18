# Copyright 2020 PerfKitBenchmarker Authors. All rights reserved.
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
"""Flags for the GCS provider interface."""

from absl import flags

GCS_CLIENT_PYTHON = 'python'
GCS_CLIENT_BOTO = 'boto'

flags.DEFINE_enum('gcs_client', GCS_CLIENT_BOTO,
                  [GCS_CLIENT_PYTHON, GCS_CLIENT_BOTO],
                  'The GCS client library to use (default boto).')
