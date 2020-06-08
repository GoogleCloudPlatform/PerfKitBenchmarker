# Getting Started

## Installing Prerequisites
---
Before you can run the PerfKit Benchmaker, you will need account(s) on the
Cloud provider(s) you want to benchmark:

- [Google Cloud Platform](https://cloud.google.com/)
- [Amazon Web Services](https://aws.amazon.com/)
- [Microsoft Azure](https://azure.microsoft.com/)
- [Alibaba Cloud](https://intl.aliyun.com/)
- [DigitalOcean](https://www.digitalocean.com/)
- [Rackspace Public Cloud](https://www.rackspace.com/)
- [ProfitBricks](https://www.profitbricks.com/)

Alternatively, you will need credentials for the following, if it applies:

- Kubernetes
- OpenStack
- CloudStack
- Apache Mesos
- Pre-provisioned machines

You also need the software dependencies, which are mostly command line tools and
credentials to access your accounts without a password.

If you are running on Windows, you will need to install GitHub Windows
since it includes tools like `openssl` and an `ssh` client. Alternatively you can
install Cygwin since it should include the same tools.

### Python 2.7 and pip
If you are running on Windows, get the latest version of Python 2.7
[here](https://www.python.org/downloads/windows/). This should have pip bundled
with it. Make sure your `PATH` environment variable is set so that you can use
both `python` and `pip` on the command line (you can have the installer do it
for you if you select the correct option).

Most Linux distributions and recent Mac OS X version already have Python 2.7
installed.
If Python is not installed, you can likely install it using your distribution's package manager, or see the [Python Download page](https://www.python.org/downloads/).

If you need to install `pip`, see [these instructions](http://pip.readthedocs.org/en/stable/installing/).

### Install GitHub Windows - *Windows Only*
Instructions: https://windows.github.com/

Make sure that `openssl`/`ssh`/`scp`/`ssh-keygen` are on your path (you will need to update the `PATH` environment variable).
The path to these commands should be

`C:\Users\<user>\AppData\Local\GitHub\PortableGit_<guid>\bin`

## Downloading PerfKit Benchmarker
---
### Download packaged release
The packaged releases are available for download from the [GitHub Perfkit Benchmarker releases page.](http://github.com/GoogleCloudPlatform/PerfKitBenchmarker/releases)
Download the latest release and extract the compressed file.

### Download latest from source

Download the PerfKit Benchmarker source code by cloning the GitHub repository.

```bash
  git clone https://github.com/GoogleCloudPlatform/PerfKitBenchmarker.git
  cd PerfKitBenchmarker
```

## Installation
---
After downloading PerfKit Benchmarker, simply install the `pip` dependencies
using the `requirements.txt` file. A virtualenv is recommended.

```bash
  cd /path/to/PerfKitBenchmarker
  sudo pip install -r requirements.txt
```

## Cloud CLI Setup
---
PerfKit Benchmarker can run benchmarks both on Cloud Providers (GCP,
AWS, Azure, DigitalOcean, etc) as well as any "machine" you can SSH into.

To quickly get started benchmarking the Google Cloud Platform:

### Install gcloud and setup authentication

Instructions: [https://developers.google.com/cloud/sdk/](https://developers.google.com/cloud/sdk/)

If you're using OS X or Linux you can run the command below.

When prompted pick the local folder, then Python project, then the defaults for all the rest
```bash
  curl https://sdk.cloud.google.com | bash
```
Restart your shell window (or logout/ssh again if running on a VM)

On Windows, visit the same page and follow the Windows installation instructions on the page.

Set your credentials up: [https://developers.google.com/cloud/sdk/gcloud/#gcloud.auth](https://developers.google.com/cloud/sdk/gcloud/#gcloud.auth)

 Run the command below.
It will print a web page URL. Navigate there, authorize the gcloud instance you just installed to use the services
it lists, copy the access token and give it to the shell prompt.
```
  gcloud auth login
```

You will need a project ID before you can run. Please navigate to
[https://console.developers.google.com](https://console.developers.google.com) and create one.

### Other Cloud Systems
For instructions on how to configure PerfKit Benchmarker  and run a benchmark
for other cloud systems see:

- [Amazon Web Services](providers/aws.md)
- [Microsoft Azure](providers/azure.md)
- [Alibaba Cloud](providers/alicloud.md)
- [DigitalOcean](providers/digitalocean.md)
- [Rackspace Public Cloud](providers/rackspace.md)
- [ProfitBricks](providers/profitbricks.md)
- [Kubernetes](providers/kubernetes.md)
- [OpenStack](providers/openstack.md)
- [CloudStack](providers/cloudstack.md)
- [Apache Mesos](providers/mesos.md)
- [Pre-provisioned machines](providers/staticvm.md)

## Running a Single Benchmark
---

### Example run on GCP
```bash
  python pkb.py --project=<GCP project ID> --benchmarks='iperf' --machine_type='f1-micro'
```

For a list of all available benchmarks see [Benchmarks](benchmarks/linux-benchmarks.md).
## Running a Benchmark Set
---
Benchmark set is a grouping of one or more benchmarks that can be sequentially
executed one after the other. For more info on benchmark sets see [Benchmark Sets](benchmarks/benchmark-sets.md).

### Example run on GCP
```bash
  python pkb.py --project=<GCP project ID> --benchmarks='standard_set' \
                --machine_type='f1-micro'
```

## Useful Global Flags
---

The following are some common flags used when configuring
PerfKit Benchmarker.

Flag | Notes
-----|------
`--help`         | see all flags
`--benchmarks`   | A comma separated list of benchmarks or benchmark sets to run such as `--benchmarks=iperf,ping` . To see the full list, run `./pkb.py --help`
`--cloud`        | Cloud where the benchmarks are run. See the table below for choices.
`--machine_type` | Type of machine to provision if pre-provisioned machines are not used. Most cloud providers accept the names of pre-defined provider-specific machine types (for example, GCP supports `--machine_type=n1-standard-8` for a GCE n1-standard-8 VM). Some cloud providers support YAML expressions that match the corresponding VM spec machine_type property in the [YAML configs](#configurations-and-configuration-overrides) (for example, GCP supports `--machine_type="{cpus: 1, memory: 4.5GiB}"` for a GCE custom VM with 1 vCPU and 4.5GiB memory). Note that the value provided by this flag will affect all provisioned machines; users who wish to provision different machine types for different roles within a single benchmark run should use the [YAML configs](#configurations-and-configuration-overrides) for finer control.
`--zone`         | This flag allows you to override the default zone. See the table below.
`--data_disk_type` | Type of disk to use. Names are provider-specific, but see table below.

### Cloud and Zones
The default cloud is 'GCP', override with the `--cloud` flag.
Each cloud has a default zone which you can override with the `--zone` flag,
the flag supports the same values that the corresponding Cloud CLIs take:

Cloud name | Default zone | Notes
-------|---------|-------
GCP | us-central1-a | |
AWS | us-east-1a | |
Azure | East US | |
AliCloud | West US | |
DigitalOcean | sfo1 | You must use a zone that supports the features 'metadata' (for cloud config) and 'private_networking'.
OpenStack | nova | |
CloudStack | QC-1 | |
Rackspace | IAD | OnMetal machine-types are available only in IAD zone
Kubernetes | k8s | |
ProfitBricks | ZONE_1 | Additional zones: ZONE_2

Example:

```bash
python pkb.py --cloud='GCP' --zone='us-central1-a' --benchmarks='iperf,ping'
```

### Storage Disk Types
The disk type names vary by provider, but the following table summarizes some
useful ones. Many cloud providers have more disk types beyond these options.

Cloud name | Network-attached SSD | Network-attached HDD
-----------|----------------------|---------------------
GCP | pd-ssd | pd-standard
AWS | gp2 | standard
Azure | premium-storage | standard-disk
Rackspace | cbs-ssd | cbs-sata

Also note that `--data_disk_type=local` tells PKB not to allocate a separate
disk, but to use whatever comes with the VM. This is useful with AWS instance
types that come with local SSDs, or with the GCP `--gce_num_local_ssds` flag.

If an instance type comes with more than one disk, PKB uses whichever does *not*
hold the root partition. Specifically, on Azure, PKB always uses `/dev/sdb` as
its scratch device.
