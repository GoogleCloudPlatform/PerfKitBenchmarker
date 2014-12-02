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
"""Contains all benchmark imports and a list of benchmarks."""

import pkgutil


def _LoadModules():
  result = []
  for importer, modname, ispkg in pkgutil.iter_modules(__path__):
    result.append(importer.find_module(modname).load_module(modname))
  return result


BENCHMARKS = _LoadModules()
