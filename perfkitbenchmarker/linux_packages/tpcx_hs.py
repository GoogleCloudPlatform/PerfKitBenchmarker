# Copyright 2026 PerfKitBenchmarker Authors. All rights reserved.
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
"""Module containing TPCx-HS kit installation functions and helpers.

TPCx-HS (TPC Express Benchmark HS) is a Hadoop sort benchmark derived from
TeraGen/TeraSort/TeraValidate. See https://www.tpc.org/tpcx-hs/.

The kit is distributed by the TPC behind an EULA click-through and cannot be
downloaded automatically. Before running benchmarks that depend on this package
you must download the kit zip from
https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp
and either:

* place it in a PKB data directory (e.g. perfkitbenchmarker/data/ or a path
  passed via --data_search_paths), or
* upload it to the preprovisioned data bucket of your cloud provider (see
  --gcp_preprovisioned_data_bucket and friends) under the folder given by
  --tpcx_hs_kit_bucket_dir. If the zip is not the stock v2.0.3 kit its sha256
  is unknown and --preprovision_ignore_checksum is required.

The kit zip name can be overridden with --tpcx_hs_kit_file.

The bucket folder defaults to tpcx_hs (like PACKAGE_NAME), but is
configurable with --tpcx_hs_kit_bucket_dir if the kit is staged in a shared
folder with other TPC kits.
"""

import logging
import posixpath

from absl import flags
from perfkitbenchmarker import data
from perfkitbenchmarker import errors
from perfkitbenchmarker import linux_packages

FLAGS = flags.FLAGS

PACKAGE_NAME = 'tpcx_hs'

KIT_FILE = flags.DEFINE_string(
    'tpcx_hs_kit_file',
    'TPCX-HS-Tool.zip',
    'File name of the TPCx-HS kit zip, as found in a PKB data directory or in '
    'the preprovisioned data bucket under --tpcx_hs_kit_bucket_dir.',
)
KIT_BUCKET_DIR = flags.DEFINE_string(
    'tpcx_hs_kit_bucket_dir',
    'tpcx_hs',
    'Folder of the preprovisioned data bucket holding --tpcx_hs_kit_file, i.e. '
    'the kit is downloaded from <bucket>/<tpcx_hs_kit_bucket_dir>/<kit_file>.',
)

# sha256 of the unmodified TPCx-HS v2.0.3 kit zip as published by the TPC
# (TPCx-HS-Runtime-Suite + TPCx-HS-SRC + specification + EULA).
PREPROVISIONED_DATA: dict[str, str] = {
    'TPCX-HS-Tool.zip': (
        '9e2867244dc0b143e666291f6b4379840d58821634246dd87960b132f08f403b'
    ),
}

TPCX_HS_DIR = posixpath.join(linux_packages.INSTALL_DIR, 'tpcx_hs')

# File name pattern of the MapReduce jar shipped in the TPCx-HS v2 kit
# (TPCx-HS-Runtime-Suite/). It targets Hadoop 2/3 (YARN). The kit also ships a
# Spark jar, but it was built for Spark 1.6 and is not supported here.
JAR_PATTERN = 'TPCx-HS-master_MR2*.jar'

# Program names used by the kit for each phase. They are passed as the first
# argument to `hadoop jar` (the jar has a program driver, like
# hadoop-mapreduce-examples.jar).
HSGEN = 'HSGen'
HSSORT = 'HSSort'
HSVALIDATE = 'HSValidate'
STAGES = (HSGEN, HSSORT, HSVALIDATE)

# Each TPCx-HS record is 100 bytes and 1 scale factor unit is 1 TB, i.e. 10^10
# records.
RECORD_SIZE_BYTES = 100
RECORDS_PER_SCALE_FACTOR = 10**10
# Scale factors allowed by the TPCx-HS specification (in TB).
VALID_SCALE_FACTORS = (1, 3, 10, 30, 100, 300, 1000, 3000, 10000)


def _FetchKit(vm, kit_file: str) -> None:
  """Places the kit zip in TPCX_HS_DIR on the VM.

  Looks for the kit in a local PKB data directory first and pushes it if found.
  Otherwise downloads it from <preprovisioned bucket>/<kit bucket dir>/<file>
  and verifies its sha256 (unless --preprovision_ignore_checksum). This
  mirrors vm.InstallPreprovisionedPackageData, which cannot be used directly
  because it hardcodes the bucket folder to the package name.

  Args:
    vm: VM to install the kit on.
    kit_file: Kit zip file name.

  Raises:
    errors.Setup.BadPreprovisionedDataError: If the kit is neither available
      locally nor in the preprovisioned bucket, or its checksum is unknown or
      does not match.
  """
  if data.ResourceExists(kit_file):
    vm.PushFile(data.ResourcePath(kit_file), TPCX_HS_DIR)
    return
  bucket_dir = KIT_BUCKET_DIR.value
  sha256 = PREPROVISIONED_DATA.get(kit_file)
  if not sha256 and not FLAGS.preprovision_ignore_checksum:
    raise errors.Setup.BadPreprovisionedDataError(
        f'No sha256 known for TPCx-HS kit {kit_file!r}. Only the stock kit '
        f'{sorted(PREPROVISIONED_DATA)} can be verified; pass '
        '--preprovision_ignore_checksum to use another file.'
    )
  if not vm.ShouldDownloadPreprovisionedData(bucket_dir, kit_file):
    raise errors.Setup.BadPreprovisionedDataError(
        f'TPCx-HS kit {kit_file!r} was not found in a PKB data directory nor '
        f'under {bucket_dir}/ in the preprovisioned data bucket. Download it '
        'from https://www.tpc.org/tpcx-hs/ and stage it; see '
        'linux_packages/tpcx_hs.py.'
    )
  vm.DownloadPreprovisionedData(TPCX_HS_DIR, bucket_dir, kit_file)
  if not FLAGS.preprovision_ignore_checksum:
    vm.CheckPreprovisionedData(TPCX_HS_DIR, bucket_dir, kit_file, sha256)


def _Install(vm) -> None:
  """Installs the TPCx-HS kit on the VM."""
  vm.Install('unzip')
  kit_file = KIT_FILE.value
  vm.RemoteCommand(f'mkdir -p {TPCX_HS_DIR}')
  _FetchKit(vm, kit_file)
  kit_path = posixpath.join(TPCX_HS_DIR, kit_file)
  if kit_file.endswith('.zip'):
    vm.RemoteCommand(f'unzip -o -q {kit_path} -d {TPCX_HS_DIR}')
  else:
    vm.RemoteCommand(f'tar -xzf {kit_path} -C {TPCX_HS_DIR}')
  vm.RemoteCommand(
      f'find {TPCX_HS_DIR} -name "*.sh" -exec chmod +x {{}} +',
      ignore_failure=True,
  )


def AptInstall(vm) -> None:
  _Install(vm)


def YumInstall(vm) -> None:
  _Install(vm)


def Uninstall(vm) -> None:
  vm.RemoteCommand(f'rm -rf {TPCX_HS_DIR}', ignore_failure=True)


def GetJarPath(vm) -> str:
  """Returns the absolute path of the kit's MapReduce jar on the VM.

  Args:
    vm: VM where the tpcx_hs package has been installed.

  Returns:
    Absolute path to the jar on the VM.

  Raises:
    errors.Setup.InvalidSetupError: if no jar matching JAR_PATTERN is found.
  """
  stdout, _ = vm.RemoteCommand(
      f'find {TPCX_HS_DIR} -name "{JAR_PATTERN}" | sort | head -n 1'
  )
  jar = stdout.strip()
  if not jar:
    raise errors.Setup.InvalidSetupError(
        f'Could not find a jar matching {JAR_PATTERN!r} under {TPCX_HS_DIR}. '
        'Check that --tpcx_hs_kit_file points to a TPCx-HS v2 kit.'
    )
  return jar


def ScaleFactorToRecords(scale_factor: int) -> int:
  """Converts a TPCx-HS scale factor (TB) to a number of 100-byte records."""
  return scale_factor * RECORDS_PER_SCALE_FACTOR


def RecordsToScaleFactor(num_records: int) -> float:
  """Converts a number of 100-byte records to an (possibly fractional) SF."""
  return num_records / RECORDS_PER_SCALE_FACTOR


def ComputeHsph(scale_factor: float, total_seconds: float) -> float:
  """Computes the TPCx-HS metric HSph@SF = SF / (T / 3600)."""
  if total_seconds <= 0:
    logging.warning(
        'total_seconds must be positive to compute HSph, got %s; reporting 0.0',
        total_seconds,
    )
    return 0.0
  return scale_factor / (total_seconds / 3600.0)
