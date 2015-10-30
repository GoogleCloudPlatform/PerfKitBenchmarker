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

"""Classes for objects that can be shared between PKB processes."""

import functools
import multiprocessing.managers
import signal


_MANAGER = multiprocessing.managers.SyncManager()
_MANAGER.start(initializer=functools.partial(signal.signal, signal.SIGINT,
                                             signal.SIG_IGN))


Namespace = _MANAGER.Namespace
