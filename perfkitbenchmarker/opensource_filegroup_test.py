"""Tests the opensource_filegroup for PerfKitBenchmarker.

In particular, this test checks that the filegroup contains everything that
should be exported to GitHub except those items that are explicitly internal.

When adding new top-level files in third_party/py/perfkitbenchmarker, they need
to be explicitly added to the opensource_filegroup so that they are exported to
GitHub. This test ensures that we add all new files, or explicitly mark them as
internal files.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from absl import flags  # NOQA
from absl import app    # NOQA
from google3.pyglib import logging
from google3.pyglib import resources
from google3.testing.pybase import googletest


flags.DEFINE_list('root_files', None, 'Files in '
                  'third_party/py/perfkitbenchmarker. Filled in by the py_test '
                  'BUILD rule.')
FLAGS = flags.FLAGS

# Top-level path of PerfKitBenchmarker.
_ROOT = 'google3/third_party/py/perfkitbenchmarker'

# Files and directories that are not exported to GitHub.
_INTERNAL_CODE = set([
    'BUILD',
    'METADATA',
    'OWNERS',
    # TODO(deitz): Refactor to drop __main__.py.
    '__main__.py',
    'perfkitbenchmarker.blueprint',
    # TODO(deitz): Internal tests of PKB should be moved elsewhere.
    'perfkitbenchmarker_import_test.py',
    'perfkitbenchmarker_main_test.py',
    'perfkitbenchmarker_test.py',
])


class OpensourceFilegroupTest(googletest.TestCase):

  def testOpensourceFilegroupContainsAllFiles(self):
    for root_file in FLAGS.root_files:
      if root_file in _INTERNAL_CODE:
        logging.info('Checking for omission of %s...', root_file)
        try:
          resources.GetResource(os.path.join(_ROOT, root_file))
        except IOError:
          pass
        else:
          self.fail('Found file %s that should have been omitted.' % root_file)
      else:
        logging.info('Checking for existence of %s...', root_file)
        resources.GetResource(os.path.join(_ROOT, root_file))


if __name__ == '__main__':
  googletest.main()
