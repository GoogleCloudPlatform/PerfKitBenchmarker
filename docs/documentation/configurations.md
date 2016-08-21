# Configuring PerfKit Benchmarker

## Overview
---
A configuration describes the resources necessary to run a benchmark as well as the options that should be used while running it. Benchmarks have default configurations which determine how they will run if users don't specify any options. Users can override the default configuration settings by providing a configuration file or by using command line flags. In addition, configurations provide a means to run with static (i.e. pre-provisioned) machines and to run multiple copies of the same benchmark in a single PKB invocation.

## Structure of a Configuration File
---
Configuration files are written in [YAML](http://www.yaml.org/), and are for the most part just nested dictionaries. At each level of the configuration, there are a set of keys which are allowed:

* Valid top level keys:
    * `benchmarks`: A YAML array of dictionaries mapping benchmark names to their
      configs. This also determines which benchmarks to run.
    * `*any_benchmark_name*`: If the `benchmarks` key is not specified, then
      specifying a benchmark name mapped to a config will override
      that benchmark's default configuration in the event that that
      benchmark is run.
    * Any keys not listed above are allowed, but will not affect PKB.
* Valid config keys:
    * `vm_groups`: A YAML dictionary mapping the names of VM groups to the groups
      themselves. These names can be any string.
    * `description`: A description of the benchmark.
    * `flags`: A YAML dictionary with overrides for default flag values.
* Valid VM group keys:
    * `vm_spec`: A YAML dictionary mapping names of clouds (e.g. AWS) to the
      actual VM spec.
    * `disk_spec`: A YAML dictionary mapping names of clouds to the actual
      disk spec.
    * `vm_count`: The number of VMs to create in this group. If this key isn't
      specified, it defaults to 1.
    * `disk_count`: The number of disks to attach to VMs of this group. If this key
      isn't specified, it defaults to 1.
    * `cloud`: The name of the cloud to create the group in. This is used for
      multi-cloud configurations.
    * `os_type`: The OS type of the VMs to create (see the flag of the same name for
      more information). This is used if you want to run a benchmark using VMs
      with different OS types (e.g. Debian and RHEL).
    * `static_vms`: A YAML array of Static VM specs. These VMs will be used before
      any Cloud VMs are created. The total number of VMs will still add up to
      the number specified by the `vm_count` key.
* For valid VM spec keys, see [virtual_machine.BaseVmSpec](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/perfkitbenchmarker/virtual_machine.py) and derived classes.
* For valid disk spec keys, see [disk.BaseDiskSpec](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/perfkitbenchmarker/disk.py) and derived classes.

## Specifying Configurations and Precedence Rules
---

The most basic way to specify a configuration is to use the `--benchmark_config_file` command line flag. Anything specified in the file will override the default configuration. Here is an example showing how to change the number of VMs created in the `cluster_boot` benchmark:
```
./pkb.py --benchmark_config_file=cluster_boot.yml --benchmarks=cluster_boot
```
[cluster_boot.yml]
```{yaml}
cluster_boot:
  vm_groups:
    default:
      vm_count: 10
```
A second flag, `--config_override` will directly override the config file. It can be specified multiple times. Since it overrides the config file, any settings supplied via this flag have a higher priority than those supplied via the `--benchmark_config_file` flag. Here is an example performing the same change to the default `cluster_boot` configuration as above:
```
./pkb.py --config_override="cluster_boot.vm_groups.default.vm_count=10" --benchmarks=cluster_boot
```
Finally, any other flags which the user specifies on the command line have the highest priority. For example, specifying the `--machine_type` flag will cause all VM groups to use that machine type, regardless of any other settings.

## Result Metadata
---
Result metadata has been slightly modified as part of the configuration change (unless the VM group's name is 'default', in which case there is no change).
The metadata created by the `DefaultMetadataProvider` is now prefixed by the VM group name. For example, if a VM group's name is 'workers', then all samples will contain `workers_machine_type`, `workers_cloud`, and `workers_zone` metadata. This change was made to enable benchmarks with heterogeneous VM groups.

## Examples
---
### Cross-Cloud `netperf`
```{yaml}
netperf:
  vm_groups:
    vm_1:
      cloud: AWS
    vm_2:
      cloud: GCP
```
### Multiple `iperf` runs

Run `iperf` under the default configuration, once with a single client thread, once with 8:

```{yaml}
benchmarks:
  - iperf:
      flags:
        iperf_sending_thread_count: 1
  - iperf:
      flags:
        iperf_sending_thread_count: 8
```
### `fio` with Static VMs

Testing against a mounted filesystem (under `/scratch`) for `vm1`, and against the disk directly (`/dev/sdb`) for `vm2`.

```{yaml}
my_static_vms:  # Any key is accepted here.
  - &vm1
    user_name: perfkit
    ssh_private_key: /absolute/path/to/key
    ip_address: 1.1.1.1
    disk_specs:
      - mount_point: /scratch
  - &vm2
    user_name: perfkit
    ssh_private_key: /absolute/path/to/key
    ip_address: 2.2.2.2
    disk_specs:
      - device_path: /dev/sdb

benchmarks:
  - fio: {vm_groups: {default: {static_vms: [*vm1]}},
          flags: {against_device: False}}
  - fio: {vm_groups: {default: {static_vms: [*vm2]}},
          flags: {against_device: True}}
```
### Cross region `iperf`
```{yaml}
iperf:
  vm_groups:
    vm_1:
      cloud: GCP
      vm_spec:
        GCP:
          zone: us-central1-b
    vm_2:
      cloud: GCP
      vm_spec:
        GCP:
          zone: europe-west1-d
```
### `fio` using Local SSDs
```{yaml}
fio:
  vm_groups:
    default:
      cloud: AWS
      vm_spec:
        AWS:
          machine_type: i2.2xlarge
      disk_spec:
        AWS:
          disk_type: local
          num_striped_disks: 2
```


## Configurations and Configuration Overrides
---
Each benchmark as an independent configuration which is written in YAML.
Users may override this default configuration by providing a configuration.
This allows for much more complex setups than previously possible,
including running benchmarks across clouds.

A benchmark configuration has a somewhat simple structure. It is essentially
just a series of nested dictionaries. At the top level, it contains VM groups.
VM groups are logical groups of homogeneous machines. The VM groups hold both a
`vm_spec` and a `disk_spec` which contain the parameters needed to
create members of that group. Here is an example of an expanded configuration:

```yaml
hbase_ycsb:
  vm_groups:
    loaders:
      vm_count: 4
      vm_spec:
        GCP:
          machine_type: n1-standard-1
          image: ubuntu-14-04
          zone: us-central1-c
        AWS:
          machine_type: m3.medium
          image: ami-######
          zone: us-east-1a
        # Other clouds here...
      # This specifies the cloud to use for the group. This allows for
      # benchmark configurations that span clouds.
      cloud: AWS
      # No disk_spec here since these are loaders.
    master:
      vm_count: 1
      cloud: GCP
      vm_spec:
        GCP:
          machine_type:
            cpus: 2
            memory: 10.0GiB
          image: ubuntu-14-04
          zone: us-central1-c
        # Other clouds here...
      disk_count: 1
      disk_spec:
        GCP:
          disk_size: 100
          disk_type: standard
          mount_point: /scratch
        # Other clouds here...
    workers:
      vm_count: 4
      cloud: GCP
      vm_spec:
        GCP:
          machine_type: n1-standard-4
          image: ubuntu-14-04
          zone: us-central1-c
        # Other clouds here...
      disk_count: 1
      disk_spec:
        GCP:
          disk_size: 500
          disk_type: remote_ssd
          mount_point: /scratch
        # Other clouds here...
```

For a complete list of keys for `vm_spec`s and `disk_spec`s see
[`virtual_machine.BaseVmSpec`](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/perfkitbenchmarker/virtual_machine.py)
and
[`disk.BaseDiskSpec`](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/perfkitbenchmarker/disk.py)
and their derived classes.

User configs are applied on top of the existing default config and can be
specified in two ways. The first is by supplying a config file via the
`--benchmark_config_file` flag. The second is by specifying a single setting to
override via the `--config_override` flag.

A user config file only needs to specify the settings which it is intended to
override. For example if the only thing you want to do is change the number of
VMs for the `cluster_boot` benchmark, this config is sufficient:
```yaml
cluster_boot:
  vm_groups:
    default:
      vm_count: 100
```
You can achieve the same effect by specifying the `--config_override` flag.
The value of the flag should be a path within the YAML (with keys delimited by
periods), an equals sign, and finally the new value:
```
--config_override=cluster_boot.vm_groups.default.vm_count=100
```
See the section below for how to use static (i.e. pre-provisioned) machines in your config.

## Specifying Flags in Configuration Files
---
You can now specify flags in configuration files by using the `flags` key at the
top level in a benchmark config. The expected value is a dictionary mapping
flag names to their new default values. The flags are only defaults; it's still
possible to override them via the command line. It's even possible to specify
conflicting values of the same flag in different benchmarks:

```yaml
iperf:
  flags:
    machine_type: n1-standard-2
    zone: us-central1-b
    iperf_sending_thread_count: 2

netperf:
  flags:
    machine_type: n1-standard-8
```

The new defaults will only apply to the benchmark in which they are specified.

## Proxy configuration for VM guests.
---
If the VM guests do not have direct Internet access in the cloud
environment, you can configure proxy settings through `pkb.py` flags.

To do that simple setup three flags (All urls are in notation ): The
flag values use the same `<protocol>://<server>:<port>` syntax as the
corresponding environment variables, for example
`--http_proxy=http://proxy.example.com:8080` .

Flag | Notes
-----|------
`--http_proxy`       | Needed for package manager on Guest OS and for some Perfkit packages
`--https_proxy`      | Needed for package manager or Ubuntu guest and for from github downloaded packages
`--ftp_proxy`       | Needed for some Perfkit packages
