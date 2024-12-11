from perfkitbenchmarker import sample
from perfkitbenchmarker.linux_packages import locust
from google3.testing.pybase import googletest


class LocustTest(googletest.TestCase):

  def test_convert_locust_to_samples(self):
    locust_output = """Timestamp,Type,Name,field1,field2
        12345678,ignored,also ignored,1,10
        12345679,ignored,also ignored,2,20
        12345680,ignored,also ignored,N/A,30"""

    samples = locust._ConvertLocustResultsToSamples(locust_output)
    self.assertCountEqual(
        [
            sample.Sample("locust/field1", 1, "", {}, 12345678),
            sample.Sample("locust/field2", 10, "", {}, 12345678),
            sample.Sample("locust/field1", 2, "", {}, 12345679),
            sample.Sample("locust/field2", 20, "", {}, 12345679),
            sample.Sample("locust/field2", 30, "", {}, 12345680),
        ],
        samples,
    )


if __name__ == "__main__":
  googletest.main()
