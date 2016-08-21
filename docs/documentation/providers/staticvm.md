# Pre-provisioned Machines

## How To Run Benchmarks Without Cloud Provisioning (e.g., local workstation)
---
It is possible to run PerfKit Benchmarker without running the Cloud provisioning steps.  This is useful if you want to run on a local machine, or have a benchmark like iperf run from an external point to a Cloud VM.

In order to do this you need to make sure:
* The static (i.e. not provisioned by PerfKit Benchmarker) machine is ssh'able
* The user PerfKitBenchmarker will login as has 'sudo' access.

Next, you will want to create a YAML user config file describing how to connect to the machine as follows:

```yaml
static_vms:
  - &vm1 # Using the & character creates an anchor that we can
         # reference later by using the same name and a * character.
    ip_address: 170.200.60.23
    user_name: voellm
    ssh_private_key: /home/voellm/perfkitkeys/my_key_file.pem
    zone: Siberia
    disk_specs:
      - mount_point: /data_dir
```

* The `ip_address` is the address where you want benchmarks to run.
* `ssh_private_key` is where to find the private ssh key.
* `zone` can be anything you want.  It is used when publishing results.
* `disk_specs` is used by all benchmarks which use disk (i.e., `fio`, `bonnie++`, many others).

In the same file, configure any number of benchmarks (in this case just iperf),
and reference the static VM as follows:

```yaml
iperf:
  vm_groups:
    vm_1:
      static_vms:
        - *vm1
```

I called my file `iperf.yaml` and used it to run iperf from Siberia to a GCP VM in us-central1-f as follows:

```bash
python pkb.py --benchmarks=iperf --machine_type=f1-micro \
              --benchmark_config_file=iperf.yaml --zone=us-central1-f \
              --ip_addresses=EXTERNAL
```

* `ip_addresses=EXTERNAL` tells PerfKit Benchmarker not to use 10.X (ie Internal) machine addresses that all Cloud VMs have.  Just use the external IP address.

If a benchmark requires two machines like iperf, you can have two machines in the same YAML file as shown below.  This means you can indeed run between two machines and never provision any VMs in the Cloud.

```yaml
static_vms:
  - &vm1
    ip_address: <ip1>
    user_name: connormccoy
    ssh_private_key: /home/connormccoy/.ssh/google_compute_engine
    internal_ip: 10.240.223.37
    install_packages: false
  - &vm2
    ip_address: <ip2>
    user_name: connormccoy
    ssh_private_key: /home/connormccoy/.ssh/google_compute_engine
    internal_ip: 10.240.234.189
    ssh_port: 2222

iperf:
  vm_groups:
    vm_1:
      static_vms:
        - *vm2
    vm_2:
      static_vms:
        - *vm1
```
