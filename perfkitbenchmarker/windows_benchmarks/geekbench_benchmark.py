# Define the name of the benchmark as a string constant.
BENCHMARK_NAME = 'geekbench'

# Define the configuration for the benchmark.
# This includes VM groups and any flags specific to this benchmark.
BENCHMARK_CONFIG = """
geekbench:
  description: >
    Runs Geekbench 6 to evaluate system performance across CPU and GPU on
    Linux or Windows platforms.
  vm_groups:
    default:
      vm_spec: *default_single_core
"""

# Import necessary modules from PKB
from perfkitbenchmarker import configs
from perfkitbenchmarker import sample

def GetConfig(user_config):
    """
    Returns the configuration for the benchmark.
    """
    return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)

def Prepare(benchmark_spec):
    """
    Sets up the environment on the VM for the benchmark.
    """
    pass 

def Run(benchmark_spec):
    """
    Runs Geekbench on the VM and returns performance samples.
    """
    return []

def Cleanup(benchmark_spec):
    """
    Cleans up the environment on the VM after the benchmark.
    """
    pass 

def ParseResults(geekbench_output: str):
    """
    Parses Geekbench benchmark results to extract metrics for Single-Core, Multi-Core, 
    and OpenCL performance tests. Each metric entry in the output represents a specific 
    test result with associated metadata.

    Args:
        geekbench_output (str): Raw output from a Geekbench benchmark as a string.

    Returns:
        List[Dict]: A list of dictionaries where each dictionary represents a parsed metric 
        sample. Each dictionary has the following structure:
        
        - "metric_name" (str): The name of the metric, describing the test and 
          performance category. Examples include "Single-Core File Compression" or "Multi-Core Score".
        
        - "metric_value" (float): The numerical result or score of the specific test. This could 
          be a throughput value, such as MB/sec, or a score in points.

        - "metric_unit" (str): The unit associated with the metric value. For example, units 
          can be "MB/sec" for throughput or "points" for scores.

        - "metric_metadata" (dict): Additional metadata about the test, including:
            - "category" (str): The performance category, such as "Single-Core", "Multi-Core", or "OpenCL".
            - "test" (str, optional): The specific test name within the category, such as "File Compression" 
              or "HTML5 Browser". This key is present for detailed test metrics.
            - "score" (int, optional): The individual test score associated with the metric, where applicable.
              For instance, if a throughput value is provided, the corresponding score is also included.

    Example Output:
        [
            {
                "metric_name": "Single-Core Score",
                "metric_value": 1795,
                "metric_unit": "points",
                "metric_metadata": {"category": "Single-Core"}
            },
            {
                "metric_name": "Single-Core File Compression",
                "metric_value": 269.3,
                "metric_unit": "MB/sec",
                "metric_metadata": {
                    "category": "Single-Core",
                    "test": "File Compression",
                    "score": 1875
                }
            }
        ]
    """

    # Initialize a list to store the parsed samples
    samples = []

    # Track the current category (Single-Core, Multi-Core, or OpenCL)
    current_category = None
    current_metric_name = None
    last_score = None 

    # Split the output into lines for easier processing
    lines = geekbench_output.splitlines()

    for line in lines:
        line = line.strip() 
        # Detect category headers
        if "Single-Core Performance" in line:
            current_category = "Single-Core"
        elif "Multi-Core Performance" in line:
            current_category = "Multi-Core"
        elif "OpenCL Performance" in line:
            current_category = "OpenCL"
        
        # Detect overall score lines, ensuring current_category is not None
        elif "Score" in line and current_category:
            # Parse overall score based on the current category
            try:
                score = int(line.split()[-1])
                samples.append({
                    "metric_name": f"{current_category} Score",
                    "metric_value": score,
                    "metric_unit": "points",
                    "metric_metadata": {"category": current_category}
                })
            except ValueError:
                # Handle the case where score parsing fails
                continue

        # Detect specific test names within a category
        elif line.strip() and line.split()[0].isalpha():
            current_metric_name = line.strip()
        
        # Detect score line before throughput, storing score for metadata
        elif current_metric_name and line.strip().isdigit():
            last_score = int(line.strip())
        
        # Detect throughput values with units (e.g., 269.3 MB/sec)
        elif current_metric_name and line.strip():
            parts = line.strip().split()
            try:
                value = float(parts[0])  # First part is the numeric value
                unit = ' '.join(parts[1:]) if len(parts) > 1 else 'points'  # Remaining part is the unit

                # Add the parsed data as a sample, including the last_score in metadata
                samples.append({
                    "metric_name": f"{current_category} {current_metric_name}",
                    "metric_value": value,
                    "metric_unit": unit,
                    "metric_metadata": {
                        "category": current_category,
                        "test": current_metric_name,
                        "score": last_score  
                    }
                })

                # Reset the metric name and score after processing
                current_metric_name = None
                last_score = None
            except ValueError:
                # Handle cases where conversion to float fails
                continue

    return samples
