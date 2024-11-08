import unittest
import os
from perfkitbenchmarker.windows_benchmarks.geekbench_benchmark import ParseResults


class TestParseResults(unittest.TestCase):
    '''
    Unit tests for the ParseResults function, which parses Geekbench benchmark output and 
    converts it into a structured list of performance metrics.
    '''
    def setUp(self):
        """
        Loads sample Geekbench data from a text file to be used in each test.

        The file 'geekbench_windows.txt' is stored under the 'data/geekbench' directory 
        and contains raw Geekbench benchmark results to be parsed.
        """

        # Load content from geekbench_windows.txt file
        data_file_path = os.path.join(
            os.path.dirname(__file__), 
            '..', 'perfkitbenchmarker', 'data', 'geekbench', 'geekbench_windows.txt'
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

        # Example checks for a specific metric
        single_core_score = next(s for s in samples if s['metric_name'] == "Single-Core Score")
        self.assertEqual(single_core_score['metric_value'], 1795)

        single_core_file_compression = next(s for s in samples if s['metric_name'] == "Single-Core File Compression")
        self.assertEqual(single_core_file_compression['metric_value'], 269.3)
        self.assertEqual(single_core_file_compression['metric_unit'], "MB/sec")
        self.assertEqual(single_core_file_compression['metric_metadata']['score'], 1875)

        multi_core_score = next(s for s in samples if s['metric_name'] == "Multi-Core Score")
        self.assertEqual(multi_core_score['metric_value'], 6627)

        opencl_score = next(s for s in samples if s['metric_name'] == "OpenCL Score")
        self.assertEqual(opencl_score['metric_value'], 88265)
    
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


