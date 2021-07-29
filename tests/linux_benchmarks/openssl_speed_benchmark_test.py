# Copyright 2021 PerfKitBenchmarker Authors. All rights reserved.
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

"""Tests for openssl_speed_benchmark."""

import unittest
from absl import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker import test_util
from perfkitbenchmarker.linux_benchmarks import openssl_speed_benchmark
from tests import pkb_common_test_case

flags.FLAGS.mark_as_parsed()


class TestParseOpenSSLOutput(pkb_common_test_case.PkbCommonTestCase,
                             test_util.SamplesTestMixin):

  def testParseOpenSSLOutput(self):
    raw_results = """
    OpenSSL 1.1.1f  31 Mar 2020
built on: Wed Apr 28 00:37:28 2021 UTC
options:bn(64,64) rc4(16x,int) des(int) aes(partial) blowfish(ptr)
compiler: gcc -fPIC -pthread -m64 -Wa,--noexecstack -Wall -Wa,--noexecstack -g -O2 -fdebug-prefix-map=/build/openssl-olnknv/openssl-1.1.1f=. -fstack-protector-strong -Wformat -Werror=format-security -DOPENSSL_TLS_SECURITY_LEVEL=2 -DOPENSSL_USE_NODELETE -DL_ENDIAN -DOPENSSL_PIC -DOPENSSL_CPUID_OBJ -DOPENSSL_IA32_SSE2 -DOPENSSL_BN_ASM_MONT -DOPENSSL_BN_ASM_MONT5 -DOPENSSL_BN_ASM_GF2m -DSHA1_ASM -DSHA256_ASM -DSHA512_ASM -DKECCAK1600_ASM -DRC4_ASM -DMD5_ASM -DAESNI_ASM -DVPAES_ASM -DGHASH_ASM -DECP_NISTZ256_ASM -DX25519_ASM -DPOLY1305_ASM -DNDEBUG -Wdate-time -D_FORTIFY_SOURCE=2
evp            7928414.04k 24973982.48k 64151343.18k 100449384.11k 117555948.61k 118711951.91k
    """
    results = openssl_speed_benchmark.ParseOpenSSLOutput(
        raw_results, 'fake_version', 10)
    print(results)
    self.assertSampleListsEqualUpToTimestamp(results, [
        sample.Sample(metric='Throughput', value=7928414.04, unit='k',
                      metadata={'duration': 60, 'algorithm': 'aes-256-ctr',
                                'parallelism': 10, 'version': 'fake_version',
                                'blocksize': 16}, timestamp=1627597373.5422955),
        sample.Sample(metric='Throughput', value=24973982.48, unit='k',
                      metadata={'duration': 60, 'algorithm': 'aes-256-ctr',
                                'parallelism': 10, 'version': 'fake_version',
                                'blocksize': 64}, timestamp=1627597909.9372606),
        sample.Sample(metric='Throughput', value=64151343.18, unit='k',
                      metadata={'duration': 60, 'algorithm': 'aes-256-ctr',
                                'parallelism': 10, 'version': 'fake_version',
                                'blocksize': 256},
                      timestamp=1627597909.9372814),
        sample.Sample(metric='Throughput', value=100449384.11, unit='k',
                      metadata={'duration': 60, 'algorithm': 'aes-256-ctr',
                                'parallelism': 10, 'version': 'fake_version',
                                'blocksize': 1024},
                      timestamp=1627597909.9372985),
        sample.Sample(metric='Throughput', value=117555948.61, unit='k',
                      metadata={'duration': 60, 'algorithm': 'aes-256-ctr',
                                'parallelism': 10, 'version': 'fake_version',
                                'blocksize': 8192},
                      timestamp=1627597909.937315),
        sample.Sample(metric='Throughput', value=118711951.91, unit='k',
                      metadata={'duration': 60, 'algorithm': 'aes-256-ctr',
                                'parallelism': 10, 'version': 'fake_version',
                                'blocksize': 16384},
                      timestamp=1627597909.9373317)
    ])


if __name__ == '__main__':
  unittest.main()
