import unittest
import os
from perfkitbenchmarker.linux_benchmarks.geekbench_benchmark import ParseResults


class TestParseResultsLinux(unittest.TestCase):
    '''
    Unit tests for the ParseResults function, which parses Geekbench benchmark output and 
    converts it into a structured list of performance metrics.
    '''

    def setUp(self):
        """
        Loads sample Geekbench data from a text file to be used in each test.

        The file 'geekbench_linux.txt' is stored under the 'data/geekbench' directory 
        and contains raw Geekbench benchmark results to be parsed.
        """

        # Load content from the Linux geekbench result file
        data_file_path = os.path.join(
            os.path.dirname(__file__), 
            '..', 'data', 'geekbench', 'geekbench_linux.txt'
        )

        # Load content from the specified file path
        with open(data_file_path, "r") as f:
            self.sample_output = f.read()

    def test_parse_results(self):
        """
        Tests the ParseResults function to ensure it correctly parses specific metrics from
        the Geekbench output.
        """
        # Run ParseResults function
        samples = ParseResults(self.sample_output)

        # Example checks for specific metrics
        single_core_score = next(s for s in samples if s['metric_name'] == "Single-Core Score")
        self.assertEqual(single_core_score['metric_value'], 1803)

        single_core_file_compression = next(s for s in samples if s['metric_name'] == "Single-Core File Compression")
        self.assertEqual(single_core_file_compression['metric_value'], 257.5)
        self.assertEqual(single_core_file_compression['metric_unit'], "MB/sec")
        self.assertEqual(single_core_file_compression['metric_metadata']['score'], 1793)

        multi_core_score = next(s for s in samples if s['metric_name'] == "Multi-Core Score")
        self.assertEqual(multi_core_score['metric_value'], 5678)

        multi_core_file_compression = next(s for s in samples if s['metric_name'] == "Multi-Core File Compression")
        self.assertEqual(multi_core_file_compression['metric_value'], 595.5)
        self.assertEqual(multi_core_file_compression['metric_unit'], "MB/sec")
        self.assertEqual(multi_core_file_compression['metric_metadata']['score'], 4147)

        opencl_score = next(s for s in samples if s['metric_name'] == "OpenCL Score")
        self.assertEqual(opencl_score['metric_value'], 358153)

        opencl_background_blur = next(s for s in samples if s['metric_name'] == "OpenCL Background Blur")
        self.assertEqual(opencl_background_blur['metric_value'], 538.9)
        self.assertEqual(opencl_background_blur['metric_unit'], "images/sec")
        self.assertEqual(opencl_background_blur['metric_metadata']['score'], 130203)

    def test_print_parse_results(self):
        """
        Prints all parsed samples for manual verification.

        This method outputs each parsed sample in a structured format to the console, 
        allowing manual inspection of metric names, values, units, and metadata.
        """
        
        # Run ParseResults function
        samples = ParseResults(self.sample_output)

        # Print all parsed data for manual verification
        for sample in samples:
            print({
                "metric_name": sample["metric_name"],
                "metric_value": sample["metric_value"],
                "metric_unit": sample["metric_unit"],
                "metric_metadata": sample["metric_metadata"]
            })


if __name__ == '__main__':
    unittest.main()
