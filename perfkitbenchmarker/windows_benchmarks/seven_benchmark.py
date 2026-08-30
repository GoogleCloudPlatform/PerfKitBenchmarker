# Copyright 2026 PerfKitBenchmarker Authors. All Rights Reserved.
"""7-Zip LZMA benchmark for Windows."""

from absl import flags
from perfkitbenchmarker import configs
from perfkitbenchmarker import vm_util

FLAGS = flags.FLAGS

BENCHMARK_NAME = 'seven_benchmark'
BENCHMARK_CONFIG = """
seven_benchmark:
  description: Runs the 7-Zip LZMA benchmark on Windows.
  vm_groups:
    default:
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          zone: us-central1-b
          image: windows-2022
        AWS:
          machine_type: m5.xlarge
          zone: us-east-1
          image: windows-2022
        Azure:
          machine_type: Standard_D4s_v5
          zone: eastus
          image: windows-2022
      vm_count: 1
"""


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def Prepare(benchmark_spec):
  del benchmark_spec
  return []


def Run(benchmark_spec):
  del benchmark_spec
  return []


def Cleanup(benchmark_spec):
  del benchmark_spec
  return []
