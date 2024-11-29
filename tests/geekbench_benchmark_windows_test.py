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
            'data', 'geekbench', 'geekbench_windows.txt'
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
        single_core_score = next(s for s in samples if s.metric == "Single-Core Score")
        self.assertEqual(single_core_score.value, 1795)
        self.assertEqual(single_core_score.unit, "points")
        self.assertEqual(single_core_score.metadata["category"], "Single-Core")

        single_core_file_compression = next(s for s in samples if s.metric == "Single-Core File Compression")
        self.assertEqual(single_core_file_compression.value, 269.3)
        self.assertEqual(single_core_file_compression.unit, "MB/sec")
        self.assertEqual(single_core_file_compression.metadata["score"], 1875)
        self.assertEqual(single_core_file_compression.metadata["category"], "Single-Core")
        self.assertEqual(single_core_file_compression.metadata["test"], "File Compression")

        multi_core_score = next(s for s in samples if s.metric == "Multi-Core Score")
        self.assertEqual(multi_core_score.value, 6627)
        self.assertEqual(multi_core_score.unit, "points")
        self.assertEqual(multi_core_score.metadata["category"], "Multi-Core")

        opencl_score = next(s for s in samples if s.metric == "OpenCL Score")
        self.assertEqual(opencl_score.value, 88265)
        self.assertEqual(opencl_score.unit, "points")
        self.assertEqual(opencl_score.metadata["category"], "OpenCL")
    
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
            print(sample.asdict())  # Convert the Sample object to a dictionary for easier inspection


if __name__ == '__main__':
    unittest.main()


