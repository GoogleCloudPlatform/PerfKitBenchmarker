import unittest

from absl import flags
from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import locust


class LocustTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.maxDiff = None

  def test_convert_locust_to_samples(self):
    flags.FLAGS["locust_path"].value = "path/to/locust.py"
    flags.FLAGS.mark_as_parsed()
    metadata = {"locustfile_path": "path/to/locust.py"}

    locust_output = """Timestamp,Type,Name,field1,field2
        12345678,ignored,also ignored,1,10
        12345679,ignored,also ignored,2,20
        12345680,ignored,also ignored,N/A,30"""

    samples = locust._ConvertLocustResultsToSamples(locust_output)
    self.assertCountEqual(
        [
            sample.Sample("locust/field1", 1, "", metadata, 12345678),
            sample.Sample("locust/field2", 10, "", metadata, 12345678),
            sample.Sample("locust/field1", 2, "", metadata, 12345679),
            sample.Sample("locust/field2", 20, "", metadata, 12345679),
            sample.Sample("locust/field2", 30, "", metadata, 12345680),
        ],
        samples,
    )


if __name__ == "__main__":
  unittest.main()
