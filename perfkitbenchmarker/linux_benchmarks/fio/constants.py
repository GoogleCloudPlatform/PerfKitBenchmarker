# Copyright 2025 PerfKitBenchmarker Authors. All rights reserved.
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
"""CONSTANTS for fio benchmark."""
BENCHMARK_NAME = 'fio'

SECONDS_PER_MINUTE = 60

# known rwkind fio parameters
RWKIND_SEQUENTIAL_READ = 'read'
RWKIND_SEQUENTIAL_WRITE = 'write'
RWKIND_RANDOM_READ = 'randread'
RWKIND_RANDOM_WRITE = 'randwrite'
RWKIND_SEQUENTIAL_READ_WRITE = 'rw'  # 'readwrite' is also valid
RWKIND_RANDOM_READ_WRITE = 'randrw'
RWKIND_SEQUENTIAL_TRIM = 'trim'

# define fragments from scenario_strings
OPERATION_READ = 'read'
OPERATION_WRITE = 'write'
OPERATION_TRIM = 'trim'
OPERATION_READWRITE = 'readwrite'  # mixed read and writes
ALL_OPERATIONS = frozenset(
    [OPERATION_READ, OPERATION_WRITE, OPERATION_TRIM, OPERATION_READWRITE]
)

ACCESS_PATTERN_SEQUENTIAL = 'seq'
ACCESS_PATTERN_RANDOM = 'rand'
ALL_ACCESS_PATTERNS = frozenset(
    [ACCESS_PATTERN_SEQUENTIAL, ACCESS_PATTERN_RANDOM]
)
ALL_OPERATIONS = frozenset(
    [OPERATION_READ, OPERATION_WRITE, OPERATION_TRIM, OPERATION_READWRITE]
)
MAP_ACCESS_OP_TO_RWKIND = {
    (ACCESS_PATTERN_SEQUENTIAL, OPERATION_READ): RWKIND_SEQUENTIAL_READ,
    (ACCESS_PATTERN_SEQUENTIAL, OPERATION_WRITE): RWKIND_SEQUENTIAL_WRITE,
    (ACCESS_PATTERN_RANDOM, OPERATION_READ): RWKIND_RANDOM_READ,
    (ACCESS_PATTERN_RANDOM, OPERATION_WRITE): RWKIND_RANDOM_WRITE,
    (
        ACCESS_PATTERN_SEQUENTIAL,
        OPERATION_READWRITE,
    ): RWKIND_SEQUENTIAL_READ_WRITE,
    (ACCESS_PATTERN_RANDOM, OPERATION_READWRITE): RWKIND_RANDOM_READ_WRITE,
    (ACCESS_PATTERN_SEQUENTIAL, RWKIND_SEQUENTIAL_TRIM): RWKIND_SEQUENTIAL_TRIM,
}
FIO_KNOWN_FIELDS_IN_JINJA = [
    'rwmixread',
    'rwmixwrite',
    'fsync',
    'iodepth',  # overrides --fio_io_depths
    'numjobs',  # overrides --fio_num_jobs
]
# Modes for --fio_target_mode
AGAINST_FILE_WITH_FILL_MODE = 'against_file_with_fill'
AGAINST_FILE_WITHOUT_FILL_MODE = 'against_file_without_fill'
AGAINST_DEVICE_WITH_FILL_MODE = 'against_device_with_fill'
AGAINST_DEVICE_WITHOUT_FILL_MODE = 'against_device_without_fill'
AGAINST_DEVICE_MODES = frozenset({
    AGAINST_DEVICE_WITH_FILL_MODE,
    AGAINST_DEVICE_WITHOUT_FILL_MODE,
})
FILL_TARGET_MODES = frozenset(
    {AGAINST_DEVICE_WITH_FILL_MODE, AGAINST_FILE_WITH_FILL_MODE}
)
DEFAULT_TEMP_FILE_NAME = 'fio-temp-file'
FIO_DIR = '/opt/pkb/fio'
GIT_REPO = 'https://github.com/axboe/fio.git'
GIT_TAG = 'fio-3.39'
FIO_PATH = FIO_DIR + '/fio'
MOUNT_POINT = '/scratch'
