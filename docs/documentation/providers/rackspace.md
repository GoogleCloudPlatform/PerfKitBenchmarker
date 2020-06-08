# Rackspace Public Cloud

## Installing CLIs and credentials for Rackspace

In order to interact with the Rackspace Public Cloud, PerfKitBenchmarker makes
use of RackCLI. You can find the instructions to install and configure RackCLI here:
[https://developer.rackspace.com/docs/rack-cli/](https://developer.rackspace.com/docs/rack-cli/)

To run PerfKit Benchmarker against Rackspace is very easy. Simply make sure
Rack CLI is installed and available in your `PATH`, optionally use the flag
`--rack_path` to indicate the path to the binary.

For a Rackspace UK Public Cloud account, unless it's your default RackCLI profile then it's
recommended that you create a profile for your UK account. Once configured, use flag
`--profile` to specify which RackCLI profile to use. You can find more details here:
[https://developer.rackspace.com/docs/rack-cli/configuration/#config-file](https://developer.rackspace.com/docs/rack-cli/configuration/#config-file)

Note: Not all flavors are supported on every region. Always check first if the flavor is supported in the region.

## Running a Single Benchmark
---

### Example run on Rackspace
```bash
  python pkb.py --cloud=Rackspace --machine_type=general1-2 --benchmarks=iperf
```
