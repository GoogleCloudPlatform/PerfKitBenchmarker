# PerfSpect

PerfSpect is a system performance profiling and processing tool based on linux perf. The tool has two parts, perf collection to collect underlying PMU(Performance Monitoring Unit) counters and post processing that generates csv output of performance metrics.

## How to run PerfSpect with PKB
**Run PerfSpect on AWS**
```
./pkb.py --cloud=AWS --benchmarks=your_wl --machine_type=m5.2xlarge
--os_type=ubuntu2004 --perfspect
```
  Flags that can be passed to pkb commands are:
  - `--perfspect`={True|False} ; default is False
  - `--perfspect_tarball`={Local path to perfspect tarball} ; default is None
  - `--perfspect_url`={URL for downloading perfspect tarball} ; default is None

**Run PerfSpect on baremetal/on-prem server**

To run PerfSpect on an on-prem SUT, provide a benchmark configuration yaml file that specifies the details of SUT e.g. as below
```YAML
static_vms:
  - &worker
      os_type: ubuntu2004
      ip_address: <SUT IP Address>
      user_name: <SUT user name>
      ssh_private_key: ~/.ssh/id_rsa
      internal_ip: <SUT IP Address>

your_wl:
  vm_groups:
    vm_1:
      static_vms:
        - *worker
```

Use the PKB command line flag `--benchmark_config_file` specifying above config file. e.g.

```bash
$ python3 pkb.py --benchmarks=your_wl --benchmark_config_file=<path_to>/benchmark_config.yaml --perfspect
```

## Caveats

1. The tool can collect only the counters supported by underlying linux perf version. 
2. Current version supports Intel Icelake, Cascadelake, Skylake and Broadwell servers only.
3. PerfSpect can only be run on-prem SUT and AWS cloud baremetal instances.
