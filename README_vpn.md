PKB VPN Benchmark Usage
==================
## Basic Usage
`./pkb.py --project=<project> --benchmarks=iperf_vpn --machine_type=f1-micro --benchmark_config_file=iperf_vpn.yaml`

## Flags:
`iperf_vpn_sending_thread_count` The number of threads the iperf client uses to 
transmit to server.

`tunnel_count` Sets the number of VPN GWs to stand up for load balancing

`cidr` The CIDR range to assign the private network for the vm_group. *avoid collisions*

`use_vpn` flag to alert provider networks to create VPN GWs 

./configs/vpn_config.yaml:
```
# VPN iperf config.
iperf_vpn:
  description: Run iperf over vpn 
  flags:
    iperf_vpn_sending_thread_count: 20
    use_vpn: True
  vpn_service:
    tunnel_count: 3  # create multiple tunnels for ecmp load balance testing
  vm_groups:
    vm_1:
      cloud: GCP
      cidr: 10.0.1.0/24
      zone: us-west1-b
      #vm_spec: 
            #zone: us-central1-a
    vm_2:
      cloud: GCP
      cidr: 192.168.1.0/24
      zone: us-central1-b
      #vm_spec:
            #zone: us-west1-c
```

Setup
=========
* Setup PKB environment as described in the [PKB README](https://github.com/SMU-ATT-Center-for-Virtualization/PerfKitBenchmarker)
* Clone the repository and checkout the vpn_support branch
```
git clone https://github.com/SMU-ATT-Center-for-Virtualization/PerfKitBenchmarker.git
git checkout vpn_support
``` 

Examples
=========

* Run the benchmark
```
./pkb.py --project=<project> --benchmarks=iperf_vpn --machine_type=f1-micro --benchmark_config_file=iperf_vpn.yaml
```

* Run the benchmark and publish to BigQuery for PKE
```
./pkb.py --project=<project> --benchmarks=iperf_vpn --machine_type=n1-standard-16 --benchmark_config_file=iperf_vpn.yaml --bigquery_table=vpn_tests.test_results --bq_project=cloud-network-performance-pke
```

Known Issues
============
* You are always welcome to [open an issue](https://github.com/SMU-ATT-Center-for-Virtualization/ADSS/issues). 
Notes on how we manage issues in the sprint backlog are available [here](https://drive.google.com/open?id=1ssIJUVsmWbBQHEzcsalebrFgjx-SwD1BLRi3e53pK_k)

* In progress: Apply flags default in vpn_service.py 
overrides tunnel_count param in config.yaml. 

* In progress: Pickling errors with bechmarkspec holding 
VPN objects affects selecting run stages. 


Planned Improvements
=======================
* In progress: AWS provider and inter CSP testing
* On deck: Azure provider, Docker provider for SW VPN testing
... 
