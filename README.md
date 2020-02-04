# PerfKit Benchmarker

PerfKit Benchmarker is an open effort to define a canonical set of benchmarks to
measure and compare cloud offerings. It's designed to operate via vendor
provided command line tools. The benchmark default settings are not tuned for
any particular platform or instance type. These settings are recommended for
consistency across services. Only in the rare case where there is a common
practice like setting the buffer pool size of a database do we change any
settings.

This README is designed to give you the information you need to get running with
the benchmarker and the basics of working with the code. The
[wiki](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki) contains
more detailed information:

*   [FAQ](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/FAQ)
*   [Tech Talks](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/Tech-Talks)
*   [Governing rules](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/Governing-Rules)
*   [Community meeting decks and notes](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/Community-Meeting-Notes-Decks)
*   [Design documents](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/Design-Docs)
*   You are always welcome to
    [open an issue](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/issues),
    or to join us on #PerfKitBenchmarker on freenode to discuss issues you're
    having, pull requests, or anything else related to PerfKitBenchmarker

# Licensing

PerfKit Benchmarker provides wrappers and workload definitions around popular
benchmark tools. We made it very simple to use and automate everything we can.
It instantiates VMs on the Cloud provider of your choice, automatically installs
benchmarks, and runs the workloads without user interaction.

Due to the level of automation you will not see prompts for software installed
as part of a benchmark run. Therefore you must accept the license of each of the
benchmarks individually, and take responsibility for using them before you use
the PerfKit Benchmarker.

In its current release these are the benchmarks that are executed:

-   `aerospike`:
    [Apache v2 for the client](http://www.aerospike.com/aerospike-licensing/)
    and
    [GNU AGPL v3.0 for the server](https://github.com/aerospike/aerospike-server/blob/master/LICENSE)
-   `bonnie++`: [GPL v2](http://www.coker.com.au/bonnie++/readme.html)
-   `cassandra_ycsb`: [Apache v2](http://cassandra.apache.org/)
-   `cassandra_stress`: [Apache v2](http://cassandra.apache.org/)
-   `cloudsuite3.0`:
    [CloudSuite 3.0 license](http://cloudsuite.ch/pages/license/)
-   `cluster_boot`: MIT License
-   `coremark`: [EEMBC](https://www.eembc.org/)
-   `copy_throughput`: Apache v2
-   `fio`: [GPL v2](https://github.com/axboe/fio/blob/master/COPYING)
-   [`gpu_pcie_bandwidth`](https://developer.nvidia.com/cuda-downloads):
    [NVIDIA Software Licence Agreement](http://docs.nvidia.com/cuda/eula/index.html#nvidia-driver-license)
-   `hadoop_terasort`: [Apache v2](http://hadoop.apache.org/)
-   `hpcc`: [Original BSD license](http://icl.cs.utk.edu/hpcc/faq/#263)
-   [`hpcg`](https://github.com/hpcg-benchmark/hpcg/):
    [BSD 3-clause](https://github.com/hpcg-benchmark/hpcg/blob/master/LICENSE)
-   `iperf`: [BSD license](http://iperf.sourceforge.net/)
-   `memtier_benchmark`:
    [GPL v2](https://github.com/RedisLabs/memtier_benchmark)
-   `mesh_network`:
    [HP license](http://www.calculate-linux.org/packages/licenses/netperf)
-   `mongodb`: **Deprecated**.
    [GNU AGPL v3.0](http://www.mongodb.org/about/licensing/)
-   `mongodb_ycsb`: [GNU AGPL v3.0](http://www.mongodb.org/about/licensing/)
-   [`multichase`](https://github.com/google/multichase):
    [Apache v2](https://github.com/google/multichase/blob/master/LICENSE)
-   `netperf`:
    [HP license](http://www.calculate-linux.org/packages/licenses/netperf)
-   [`oldisim`](https://github.com/GoogleCloudPlatform/oldisim):
    [Apache v2](https://github.com/GoogleCloudPlatform/oldisim/blob/master/LICENSE.txt)
-   `object_storage_service`: Apache v2
-   `pgbench`: [PostgreSQL Licence](https://www.postgresql.org/about/licence/)
-   `ping`: No license needed.
-   `silo`: MIT License
-   `scimark2`: [public domain](http://math.nist.gov/scimark2/credits.html)
-   `speccpu2006`: [SPEC CPU2006](http://www.spec.org/cpu2006/)
-   [`SHOC`](https://github.com/vetter/shoc):
    [BSD 3-clause](https://github.com/vetter/shoc/blob/master/LICENSE.txt)
-   `sysbench_oltp`: [GPL v2](https://github.com/akopytov/sysbench)
-   [`TensorFlow`](https://github.com/tensorflow/tensorflow):
    [Apache v2](https://github.com/tensorflow/tensorflow/blob/master/LICENSE)
-   [`tomcat`](https://github.com/apache/tomcat):
    [Apache v2](https://github.com/apache/tomcat/blob/trunk/LICENSE)
-   [`unixbench`](https://github.com/kdlucas/byte-unixbench):
    [GPL v2](https://github.com/kdlucas/byte-unixbench/blob/master/LICENSE.txt)
-   [`wrk`](https://github.com/wg/wrk):
    [Modified Apache v2](https://github.com/wg/wrk/blob/master/LICENSE)
-   [`ycsb`](https://github.com/brianfrankcooper/YCSB) (used by `mongodb`,
    `hbase_ycsb`, and others):
    [Apache v2](https://github.com/brianfrankcooper/YCSB/blob/master/LICENSE.txt)

Some of the benchmarks invoked require Java. You must also agree with the
following license:

-   `openjdk-7-jre`:
    [GPL v2 with the Classpath Exception](http://openjdk.java.net/legal/gplv2+ce.html)

[SPEC CPU2006](https://www.spec.org/cpu2006/) benchmark setup cannot be
automated. SPEC requires that users purchase a license and agree with their
terms and conditions. PerfKit Benchmarker users must manually download
`cpu2006-1.2.iso` from the SPEC website, save it under the
`perfkitbenchmarker/data` folder (e.g.
`~/PerfKitBenchmarker/perfkitbenchmarker/data/cpu2006-1.2.iso`), and also supply
a runspec cfg file (e.g.
`~/PerfKitBenchmarker/perfkitbenchmarker/data/linux64-x64-gcc47.cfg`).
Alternately, PerfKit Benchmarker can accept a tar file that can be generated
with the following steps:

*   Extract the contents of `cpu2006-1.2.iso` into a directory named `cpu2006`
*   Run `cpu2006/install.sh`
*   Copy the cfg file into `cpu2006/config`
*   Create a tar file containing the `cpu2006` directory, and place it under the
    `perfkitbenchmarker/data` folder (e.g.
    `~/PerfKitBenchmarker/perfkitbenchmarker/data/cpu2006v1.2.tgz`).

PerfKit Benchmarker will use the tar file if it is present. Otherwise, it will
search for the iso and cfg files.

# Installation and Setup

Before you can run the PerfKit Benchmarker, you need account(s) on the cloud
provider(s) you want to benchmark (see
[providers](perfkitbenchmarker/providers/README.md)). You also need the software
dependencies, which are mostly command line tools and credentials to access your
accounts without a password. The following steps should help you get up and
running with PKB.

## Python 3

The recommended way to install and run PKB is in a virtualenv with the latest
version of Python 3. Most Linux distributions and recent Mac OS X versions
already have Python 3 installed at `/usr/bin/python3`.

If Python is not installed, you can likely install it using your distribution's
package manager, or see the
[Python Download page](https://www.python.org/downloads/).

```bash
python3 -m venv $HOME/my_virtualenv
source $HOME/my_virtualenv/bin/activate
```

## Install PerfKit Benchmarker

Download the latest PerfKit Benchmarker release from
[GitHub](http://github.com/GoogleCloudPlatform/PerfKitBenchmarker/releases). You
can also clone the working version with:

```bash
$ cd $HOME
$ git clone https://github.com/GoogleCloudPlatform/PerfKitBenchmarker.git
```

Install Python library dependencies:

```bash
$ pip install -r $HOME/PerfKitBenchmarker/requirements.txt
```

You may need to install additional dependencies depending on the cloud provider
you are using. For example, for
[AWS](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/perfkitbenchmarker/providers/aws/requirements.txt):

```bash
$ cd $HOME/PerfKitBenchmarker/perfkitbenchmarker/providers/aws
$ pip install -r requirements.txt
```

## Preprovisioned data

Some benchmarks may require data to be preprovisioned in a cloud. To
preprovision data, you will need to obtain the data and then upload it to that
cloud. See more information below about which benchmarks require preprovisioned
data and how to upload it to different clouds.


# Running a Single Benchmark

PerfKit Benchmarker can run benchmarks both on Cloud Providers (GCP, AWS, Azure,
DigitalOcean) as well as any "machine" you can SSH into.

## Example run on GCP

```bash
$ ./pkb.py --project=<GCP project ID> --benchmarks=iperf --machine_type=f1-micro
```

## Example run on AWS

```bash
$ cd PerfKitBenchmarker
$ ./pkb.py --cloud=AWS --benchmarks=iperf --machine_type=t2.micro
```

## Example run on Azure

```bash
$ ./pkb.py --cloud=Azure --machine_type=Standard_A0 --benchmarks=iperf
```

## Example run on AliCloud

```bash
$ ./pkb.py --cloud=AliCloud --machine_type=ecs.s2.large --benchmarks=iperf
```

## Example run on DigitalOcean

```bash
$ ./pkb.py --cloud=DigitalOcean --machine_type=16gb --benchmarks=iperf
```

## Example run on OpenStack

```bash
$ ./pkb.py --cloud=OpenStack --machine_type=m1.medium \
           --openstack_network=private --benchmarks=iperf
```

## Example run on Kubernetes

```bash
$ ./pkb.py --cloud=Kubernetes --benchmarks=iperf --kubectl=/path/to/kubectl --kubeconfig=/path/to/kubeconfig --image=image-with-ssh-server  --ceph_monitors=10.20.30.40:6789,10.20.30.41:6789
```

## Example run on Mesos

```bash
$ ./pkb.py --cloud=Mesos --benchmarks=iperf --marathon_address=localhost:8080 --image=image-with-ssh-server
```

## Example run on CloudStack

```bash
./pkb.py --cloud=CloudStack --benchmarks=ping --cs_network_offering=DefaultNetworkOffering
```

## Example run on Rackspace

```bash
$ ./pkb.py --cloud=Rackspace --machine_type=general1-2 --benchmarks=iperf
```

## Example run on ProfitBricks

```bash
$ ./pkb.py --cloud=ProfitBricks --machine_type=Small --benchmarks=iperf
```

# How to Run Windows Benchmarks

Install all dependencies as above and ensure that smbclient is installed on your
system if you are running on a linux controller:

```bash
$ which smbclient
/usr/bin/smbclient
```

Now you can run Windows benchmarks by running with `--os_type=windows`. Windows
has a different set of benchmarks than Linux does. They can be found under
[`perfkitbenchmarker/windows_benchmarks/`](perfkitbenchmarker/windows_benchmarks).
The target VM OS is Windows Server 2012 R2.

# How to Run Benchmarks with Juju

[Juju](https://jujucharms.com/) is a service orchestration tool that enables you
to quickly model, configure, deploy and manage entire cloud environments.
Supported benchmarks will deploy a Juju-modeled service automatically, with no
extra user configuration required, by specifying the `--os_type=juju` flag.

## Example

```bash
$ ./pkb.py --cloud=AWS --os_type=juju --benchmarks=cassandra_stress
```

## Benchmark support

Benchmark/Package authors need to implement the JujuInstall() method inside
their package. This method deploys, configures, and relates the services to be
benchmarked. Please note that other software installation and configuration
should be bypassed when `FLAGS.os_type == JUJU`. See
[`perfkitbenchmarker/linux_packages/cassandra.py`](perfkitbenchmarker/linux_packages/cassandra.py)
for an example implementation.

# How to Run All Standard Benchmarks

Run without the `--benchmarks` parameter and every benchmark in the standard set
will run serially which can take a couple of hours (alternatively, run with
`--benchmarks="standard_set"`). Additionally, if you don't specify
`--cloud=...`, all benchmarks will run on the Google Cloud Platform.

# How to Run All Benchmarks in a Named Set

Named sets are are groupings of one or more benchmarks in the benchmarking
directory. This feature allows parallel innovation of what is important to
measure in the Cloud, and is defined by the set owner. For example the GoogleSet
is maintained by Google, whereas the StanfordSet is managed by Stanford. Once a
quarter a meeting is held to review all the sets to determine what benchmarks
should be promoted to the `standard_set`. The Standard Set is also reviewed to
see if anything should be removed. To run all benchmarks in a named set, specify
the set name in the benchmarks parameter (e.g., `--benchmarks="standard_set"`).
Sets can be combined with individual benchmarks or other named sets.

# Useful Global Flags

The following are some common flags used when configuring PerfKit Benchmarker.

| Flag               | Notes                                                 |
| ------------------ | ----------------------------------------------------- |
| `--helpmatch=pkb`  | see all global flags                                  |
| `--helpmatch=hpcc` | see all flags associated with the hpcc benchmark. You |
:                    : can substitute any benchmark name to see the          :
:                    : associated flags.                                     :
| `--benchmarks`     | A comma separated list of benchmarks or benchmark     |
:                    : sets to run such as `--benchmarks=iperf,ping` . To    :
:                    : see the full list, run `./pkb.py                      :
:                    : --helpmatch=benchmarks | grep perfkitbenchmarker`     :
| `--cloud`          | Cloud where the benchmarks are run. See the table     |
:                    : below for choices.                                    :
| `--machine_type`   | Type of machine to provision if pre-provisioned       |
:                    : machines are not used. Most cloud providers accept    :
:                    : the names of pre-defined provider-specific machine    :
:                    : types (for example, GCP supports                      :
:                    : `--machine_type=n1-standard-8` for a GCE              :
:                    : n1-standard-8 VM). Some cloud providers support YAML  :
:                    : expressions that match the corresponding VM spec      :
:                    : machine_type property in the [YAML                    :
:                    : configs](#configurations-and-configuration-overrides) :
:                    : (for example, GCP supports `--machine_type="{cpus\:   :
:                    : 1, memory\: 4.5GiB}"` for a GCE custom VM with 1 vCPU :
:                    : and 4.5GiB memory). Note that the value provided by   :
:                    : this flag will affect all provisioned machines; users :
:                    : who wish to provision different machine types for     :
:                    : different roles within a single benchmark run should  :
:                    : use the [YAML                                         :
:                    : configs](#configurations-and-configuration-overrides) :
:                    : for finer control.                                    :
| `--zones`          | This flag allows you to override the default zone.    |
:                    : See the table below.                                  :
| `--data_disk_type` | Type of disk to use. Names are provider-specific, but |
:                    : see table below.                                      :

The default cloud is 'GCP', override with the `--cloud` flag. Each cloud has a
default zone which you can override with the `--zones` flag, the flag supports
the same values that the corresponding Cloud CLIs take:

| Cloud name   | Default zone  | Notes                                       |
| ------------ | ------------- | ------------------------------------------- |
| GCP          | us-central1-a |                                             |
| AWS          | us-east-1a    |                                             |
| Azure        | eastus2       |                                             |
| AliCloud     | West US       |                                             |
| DigitalOcean | sfo1          | You must use a zone that supports the       |
:              :               : features 'metadata' (for cloud config) and  :
:              :               : 'private_networking'.                       :
| OpenStack    | nova          |                                             |
| CloudStack   | QC-1          |                                             |
| Rackspace    | IAD           | OnMetal machine-types are available only in |
:              :               : IAD zone                                    :
| Kubernetes   | k8s           |                                             |
| ProfitBricks | AUTO          | Additional zones: ZONE_1, ZONE_2, or ZONE_3 |

Example:

```bash
./pkb.py --cloud=GCP --zones=us-central1-a --benchmarks=iperf,ping
```

The disk type names vary by provider, but the following table summarizes some
useful ones. (Many cloud providers have more disk types beyond these options.)

Cloud name | Network-attached SSD | Network-attached HDD
---------- | -------------------- | --------------------
GCP        | pd-ssd               | pd-standard
AWS        | gp2                  | standard
Azure      | Premium_LRS          | Standard_LRS
Rackspace  | cbs-ssd              | cbs-sata

Also note that `--data_disk_type=local` tells PKB not to allocate a separate
disk, but to use whatever comes with the VM. This is useful with AWS instance
types that come with local SSDs, or with the GCP `--gce_num_local_ssds` flag.

If an instance type comes with more than one disk, PKB uses whichever does *not*
hold the root partition. Specifically, on Azure, PKB always uses `/dev/sdb` as
its scratch device.

## Proxy configuration for VM guests.

If the VM guests do not have direct Internet access in the cloud environment,
you can configure proxy settings through `pkb.py` flags.

To do that simple setup three flags (All urls are in notation ): The flag values
use the same `<protocol>://<server>:<port>` syntax as the corresponding
environment variables, for example `--http_proxy=http://proxy.example.com:8080`
.

| Flag            | Notes                                                   |
| --------------- | ------------------------------------------------------- |
| `--http_proxy`  | Needed for package manager on Guest OS and for some     |
:                 : Perfkit packages                                        :
| `--https_proxy` | Needed for package manager or Ubuntu guest and for from |
:                 : github downloaded packages                              :
| `--ftp_proxy`   | Needed for some Perfkit packages                        |

## Preprovisioned Data

As mentioned above, some benchmarks require preprovisioned data. This section
describes how to preprovision this data.

### Benchmarks with Preprovisioned Data

#### Sample Preprovision Benchmark

This benchmark demonstrates the use of preprovisioned data. Create the following
file to upload using the command line:

```bash
echo "1234567890" > preprovisioned_data.txt
```

To upload, follow the instructions below with a filename of
`preprovisioned_data.txt` and a benchmark name of `sample`.

### Clouds with Preprovisioned Data

#### Google Cloud

To preprovision data on Google Cloud, you will need to upload each file to
Google Cloud Storage using gsutil. First, you will need to create a storage
bucket that is accessible from VMs created in Google Cloud by PKB. Then copy
each file to this bucket using the command

```bash
gsutil cp <filename> gs://<bucket>/<benchmark-name>/<filename>
```

To run a benchmark on Google Cloud that uses the preprovisioned data, use the
flag `--gcp_preprovisioned_data_bucket=<bucket>`.

#### AWS

To preprovision data on AWS, you will need to upload each file to S3 using the
AWS CLI. First, you will need to create a storage bucket that is accessible from
VMs created in AWS by PKB. Then copy each file to this bucket using the command

```bash
aws s3 cp <filename> s3://<bucket>/<benchmark-name>/<filename>
```

To run a benchmark on AWS that uses the preprovisioned data, use the flag
`--aws_preprovisioned_data_bucket=<bucket>`.

# Configurations and Configuration Overrides

Each benchmark now has an independent configuration which is written in YAML.
Users may override this default configuration by providing a configuration. This
allows for much more complex setups than previously possible, including running
benchmarks across clouds.

A benchmark configuration has a somewhat simple structure. It is essentially
just a series of nested dictionaries. At the top level, it contains VM groups.
VM groups are logical groups of homogenous machines. The VM groups hold both a
`vm_spec` and a `disk_spec` which contain the parameters needed to create
members of that group. Here is an example of an expanded configuration:

```yaml
hbase_ycsb:
  vm_groups:
    loaders:
      vm_count: 4
      vm_spec:
        GCP:
          machine_type: n1-standard-1
          image: ubuntu-16-04
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
          image: ubuntu-16-04
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
          image: ubuntu-16-04
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

You can achieve the same effect by specifying the `--config_override` flag. The
value of the flag should be a path within the YAML (with keys delimited by
periods), an equals sign, and finally the new value:

```bash
--config_override=cluster_boot.vm_groups.default.vm_count=100
```

See the section below for how to use static (i.e. pre-provisioned) machines in
your config.

# Advanced: How To Run Benchmarks Without Cloud Provisioning (e.g., local workstation)

It is possible to run PerfKit Benchmarker without running the Cloud provisioning
steps. This is useful if you want to run on a local machine, or have a benchmark
like iperf run from an external point to a Cloud VM.

In order to do this you need to make sure:

*   The static (i.e. not provisioned by PerfKit Benchmarker) machine is ssh'able
*   The user PerfKitBenchmarker will login as has 'sudo' access. (*** Note we
    hope to remove this restriction soon ***)

Next, you will want to create a YAML user config file describing how to connect
to the machine as follows:

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

*   The `ip_address` is the address where you want benchmarks to run.
*   `ssh_private_key` is where to find the private ssh key.
*   `zone` can be anything you want. It is used when publishing results.
*   `disk_specs` is used by all benchmarks which use disk (i.e., `fio`,
    `bonnie++`, many others).

In the same file, configure any number of benchmarks (in this case just iperf),
and reference the static VM as follows:

```yaml
iperf:
  vm_groups:
    vm_1:
      static_vms:
        - *vm1
```

I called my file `iperf.yaml` and used it to run iperf from Siberia to a GCP VM
in us-central1-f as follows:

```bash
$ ./pkb.py --benchmarks=iperf --machine_type=f1-micro --benchmark_config_file=iperf.yaml --zones=us-central1-f --ip_addresses=EXTERNAL
```

*   `ip_addresses=EXTERNAL` tells PerfKit Benchmarker not to use 10.X (ie
    Internal) machine addresses that all Cloud VMs have. Just use the external
    IP address.

If a benchmark requires two machines like iperf, you can have two machines in
the same YAML file as shown below. This means you can indeed run between two
machines and never provision any VMs in the Cloud.

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

# Specifying Flags in Configuration Files

You can now specify flags in configuration files by using the `flags` key at the
top level in a benchmark config. The expected value is a dictionary mapping flag
names to their new default values. The flags are only defaults; it's still
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

# Using Elasticsearch Publisher

PerfKit data can optionally be published to an Elasticsearch server. To enable
this, the `elasticsearch` Python package must be installed.

```bash
$ pip install elasticsearch
```

Note: The `elasticsearch` Python library and Elasticsearch must have matching
major versions.

The following are flags used by the Elasticsearch publisher. At minimum, all
that is needed is the `--es_uri` flag.

| Flag         | Notes                                                     |
| ------------ | --------------------------------------------------------- |
| `--es_uri`   | The Elasticsearch server address and port (e.g.           |
:              : localhost\:9200)                                          :
| `--es_index` | The Elasticsearch index name to store documents (default: |
:              : perfkit)                                                  :
| `--es_type`  | The Elasticsearch document type (default: result)         |

Note: Amazon ElasticSearch service currently does not support transport on port
9200 therefore you must use endpoint with port 80 eg.
`search-<ID>.es.amazonaws.com:80` and allow your IP address in the cluster.

# Using InfluxDB Publisher

No additional packages need to be installed in order to publish Perfkit data to
an InfluxDB server.

InfluxDB Publisher takes in the flags for the Influx uri and the Influx DB name.
The publisher will default to the pre-set defaults, identified below, if no uri
or DB name is set. However, the user is required to at the very least call the
`--influx_uri` flag to publish data to Influx.

| Flag               | Notes                               | Default        |
| ------------------ | ----------------------------------- | -------------- |
| `--influx_uri`     | The Influx DB address and port.     | localhost:8086 |
:                    : Expects the format hostname\:port   :                :
| `--influx_db_name` | The name of Influx DB database that | perfkit        |
:                    : you wish to publish to or create    :                :

# How to Extend PerfKit Benchmarker

First start with the
[CONTRIBUTING.md](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/CONTRIBUTING.md)
file. It has the basics on how to work with PerfKitBenchmarker, and how to
submit your pull requests.

In addition to the
[CONTRIBUTING.md](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/CONTRIBUTING.md)
file we have added a lot of comments into the code to make it easy to:

*   Add new benchmarks (e.g.: `--benchmarks=<new benchmark>`)
*   Add new package/os type support (e.g.: `--os_type=<new os type>`)
*   Add new providers (e.g.: `--cloud=<new provider>`)
*   etc.

Even with lots of comments we make to support more detailed documention. You
will find the documentation we have on the
[wiki](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki). Missing
documentation you want? Start a page and/or open an
[issue](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/issues) to get
it added.

# Integration Testing

If you wish to run unit or integration tests, ensure that you have `tox >=
2.0.0` installed.

In addition to regular unit tests, which are run via
[`hooks/check-everything`](hooks/check-everything), PerfKit Benchmarker has
integration tests, which create actual cloud resources and take time and money
to run. For this reason, they will only run when the variable
`PERFKIT_INTEGRATION` is defined in the environment. The command

```bash
$ tox -e integration
```

will run the integration tests. The integration tests depend on having installed
and configured all of the relevant cloud provider SDKs, and will fail if you
have not done so.

# Planned Improvements

Many... please add new requests via GitHub issues.
