PerfKitBenchmarker
==================

PerfKit Benchmarker is an open effort to define a canonical set of benchmarks to measure and compare cloud
offerings.  It's designed to operate via vendor provided command line tools. The benchmarks are not
tuned (ie the defaults) because this is what most users will use.  This should help us drive to great defaults.
Only in the rare cause where there is a common practice like setting the buffer pool size of a database do we
change any settings.


KNOWN ISSUES
============


LICENSING
=========

PerfKitBenchmarker provides wrappers and workload definitions around popular benchmark tools. We made it very simple
to use and automate everything we can.  It instantiates VMs on the Cloud provider of your choice, automatically
installs benchmarks, and run the workloads without user interaction.

Due to the level of automation you will not see prompts for software installed as part of a benchmark run.
Therefore you must accept the license of each benchmarks individually, and take responsibility for using them
before you use the PerfKitBenchmarker.

In its current release these are the benchmarks that are executed:

  - `aerospike`: Apache V2 for the client and GNU AGPL v3.0 for the server (http://www.aerospike.com/products-services/)
  - `bonnie++`: GPL v2 (http://www.coker.com.au/bonnie++/readme.html)
  - `cassandra_ycsb`: Apache v2 (http://cassandra.apache.org/)
  - `cassandra_stress`: Apache v2 (http://cassandra.apache.org/)
  - `cluster_boot`: MIT License
  - `coremark`: EEMBC (https://www.eembc.org/)
  - `copy_throughput`: Apache v2.
  - `fio`: GPL v2 (https://github.com/axboe/fio/blob/master/COPYING)
  - `hadoop_terasort`: Apache v2 (http://hadoop.apache.org/)
  - `hpcc`: Original BSD license (http://icl.cs.utk.edu/hpcc/faq/#263)
  - `iperf`: BSD license(http://iperf.sourceforge.net/)
  - `memtier_benchmark`: GPL v2 (https://github.com/RedisLabs/memtier_benchmark)
  - `mesh_network`: HP license (http://www.calculate-linux.org/packages/licenses/netperf)
  - `mongodb`: **Deprecated**. GNU AGPL v3.0 (http://www.mongodb.org/about/licensing/)
  - `mongodb_ycsb`: GNU AGPL v3.0 (http://www.mongodb.org/about/licensing/)
  - `netperf`: HP license (http://www.calculate-linux.org/packages/licenses/netperf)
  - `oldisim`: Apache v2.
  - `object_storage_service`: Apache v2.
  - `ping`: No license needed.
  - `silo`: MIT License
  - `scimark2`: public domain (http://math.nist.gov/scimark2/credits.html)
  - `speccpu2006`: Spec CPU2006 (http://www.spec.org/cpu2006/)
  - `sysbench_oltp`: GPL v2 (https://github.com/akopytov/sysbench)
  - `unixbench`: GPL v2 (https://code.google.com/p/byte-unixbench/)
  - `ycsb` (used by `mongodb`, `hbase_ycsb`, and others): Apache V2 (https://github.com/brianfrankcooper/YCSB/blob/master/LICENSE.txt)

Some of the benchmarks invoked require Java. You must also agree with the following license:

  - openjdk-7-jre: GPL v2 with the Classpath Exception (http://openjdk.java.net/legal/gplv2+ce.html)

[CoreMark](http://www.eembc.org/coremark/) setup cannot be automated. EEMBC requires users to agree with their terms and conditions, and PerfKit
Benchmarker users must manually download the CoreMark tarball from their website and save it under the
`perfkitbenchmarker/data` folder (e.g. `~/PerfKitBenchmarker/perfkitbenchmarker/data/coremark_v1.0.tgz`)

[SpecCPU2006](https://www.spec.org/cpu2006/) benchmark setup cannot be automated. SPEC requires users to purchase a license and agree with their terms and conditions.
PerfKit Benchmarker users must manually download SpecCPU2006 tarball from their website and save it under
the `perfkitbenchmarker/data` folder (e.g. `~/PerfKitBenchmarker/perfkitbenchmarker/data/cpu2006v1.2.tgz`)

Installing Prerequisites
========================
Before you can run the PerfKit Benchmaker on Cloud providers you need accounts and access:

* Get a GCE account to run tests on GCE. Our site is https://cloud.google.com
* Get an AWS account to run tests on AWS. Their site is http://aws.amazon.com/
* Get an Azure account to run tests on Azure. Their site is http://azure.microsoft.com/
* Get a DigitalOcean account to run tests on DigitalOcean. Their site is https://www.digitalocean.com/

You also need the software dependencies, which are mostly command line tools and credentials to access your
accounts without a password.  The following steps should help you get the CLI tool auth in place.

If you are running on Windows, you will need to install GitHub Windows
since it includes tools like `openssl` and an `ssh` client. Alternatively you can
install Cygwin since it should include the same tools.

## Install Python 2.7 and `pip`
If you are running on Windows, get the latest version of Python 2.7
[here](https://www.python.org/downloads/windows/). This should have pip bundled
with it. Make sure your `PATH` environment variable is set so that you can use
both `python` and `pip` on the command line (you can have the installer do it
for you if you select the correct option).

Most Linux distributions and recent Mac OS X version already have Python 2.7
installed.
If Python is not installed, you can likely install it using your distribution's package manager, or see the [Python Download page](https://www.python.org/downloads/).

If you need to install `pip`, see [these instructions](http://pip.readthedocs.org/en/latest/installing.html).

## (*Windows Only*) Install GitHub Windows

Instructions: https://windows.github.com/

Make sure that `openssl`/`ssh`/`scp`/`ssh-keygen` are on your path (you will need to update the `PATH` environment variable).
The path to these commands should be

`C:\\Users\\\<user\>\\AppData\\Local\\GitHub\\PortableGit\_\<guid\>\\bin`

## Install `gcloud` and setup authentication
Instructions: https://developers.google.com/cloud/sdk/. If you're using OS X or Linux you can run the command below.

When prompted pick the local folder, then Python project, then the defaults for all the rest
```
$ curl https://sdk.cloud.google.com | bash
```
Restart your shell window (or logout/ssh again if running on a VM)

On Windows, visit the same page and follow the Windows installation instructions on the page.

Set your credentials up: https://developers.google.com/cloud/sdk/gcloud/#gcloud.auth. Run the command below.
It will print a web page URL. Navigate there, authorize the gcloud instance you just installed to use the services
it lists, copy the access token and give it to the shell prompt.
```
$ gcloud auth login
```

You will need a project ID before you can run. Please navigate to https://console.developers.google.com and
create one.

## Install AWS CLI and setup authentication
Make sure you have installed pip (see the section above).

Follow instructions at http://aws.amazon.com/cli/ or run the following command (omit the 'sudo' on Windows)
```
$ sudo pip install awscli
```
Navigate to the AWS console to create access credentials: https://console.aws.amazon.com/ec2/
* On the console click on your name (top left)
* Click on "Security Credentials"
* Click on "Access Keys", the create New Access Key. Download the file, it contains the Access key and Secret keys
  to access services. Note the values and delete the file.

Configure the CLI using the keys from the previous step
```
$ aws configure
```

## Windows Azure CLI and credentials
You first need to install node.js and NPM.
This version of Perfkit Benchmarker is compatible with azure version 0.9.3.

Go [here](https://nodejs.org/download/), and follow the setup instructions.

Next, run the following (omit the `sudo` on Windows):

```
$ sudo npm install azure-cli -g
$ azure account download
```

Read the output of the previous command. It will contain a webpage URL. Open that in a browser. It will download
a file (`.publishsettings`) file. Copy to the folder you're running PerfKit Benchmarker. In my case the file was called
`Free Trial-7-18-2014-credentials.publishsettings`
```
$ azure account import [path to .publishsettings file]
```

Test that azure is installed correctly
```
$ azure vm list
```

## DigitalOcean configuration and credentials

PerfKitBenchmarker uses the *curl* tool to interact with
DigitalOcean's REST API. This API uses oauth for authentication.
Please set this up as follows:

Log in to your DigitalOcean account and create a Personal Access Token
for use by PerfKitBenchmarker with read/write access in Settings /
API: https://cloud.digitalocean.com/settings/applications

Save a copy of the authentication token it shows, this is a
64-character hex string.

Create a curl configuration file containing the needed authorization
header. The double quotes are required. Example:

```
$ cat > ~/.config/digitalocean-oauth.curl
header = "Authorization: Bearer 9876543210fedc...ba98765432"
^D
```

Confirm that the authentication works:

```
$ curl --config ~/.config/digitalocean-oauth.curl https://api.digitalocean.com/v2/sizes
{"sizes":[{"slug":"512mb","memory":512,"vcpus":1,...
```

PerfKitBenchmarker uses the file location `~/.config/digitalocean-oauth.curl`
by default, you can use the `--digitalocean_curl_config` flag to
override the path.

## Create and Configure a `.boto` file for object storage benchmarks

In order to run object storage benchmark tests, you need to have a properly configured `~/.boto` file.

Here is how:

* Create the `~/.boto` file (If you already have ~/.boto, you can skip this step. Consider making a backup copy of your existing .boto file.)

To create a new `~/.boto` file, issue the following command and follow the instructions given by this command:
```
$ gsutil config
```
As a result, a `.boto` file is created under your home directory.

* Open the `.boto` file and edit the following fields:
1. In the [Credentials] section:

`gs_oauth2_refresh_token`: set it to be the same as the `refresh_token` field in your gcloud credential file (~/.config/gcloud/credentials), which was setup as part of the `gcloud auth login` step.

`aws_access_key_id`, `aws_secret_access_key`: set these to be the AWS access keys you intend to use for these tests, or you can use the same keys as those in your existing AWS credentials file (`~/.aws/credentials`).

2. In the `[GSUtil]` section:

`default_project_id`: if it is not already set, set it to be the google cloud storage project ID you intend to use for this test. (If you used `gsutil config` to generate the `.boto` file, you should have been prompted to supply this information at this step).

3. In the [OAuth2] section:
`client_id`, `client_secret`: set these to be the same as those in your gcloud credentials file (`~/.config/gcloud/credentials`), which was setup as part of the 'gcloud auth login' step.


## Install PerfKit

[Download PerfKitBenchmarker](http://github.com/GoogleCloudPlatform/PerfKitBenchmarker/releases) from GitHub.

## Install PerfKit Benchmakrer dependencies
```
$ cd /path/to/PerfKitBenchmarker
$ sudo pip install -r requirements.txt
```

RUNNING A SINGLE BENCHMARK
==========================
PerfKitBenchmarks can run benchmarks both on Cloud Providers (GCP,
AWS, Azure, DigitalOcean) as well as any "machine" you can SSH into.

## Example run on GCP
```
$ ./pkb.py --project=<GCP project ID> --benchmarks=iperf --machine_type=f1-micro
```

## Example run on AWS
```
$ cd PerfKitBenchmarker
$ ./pkb.py --cloud=AWS --benchmarks=iperf --machine_type=t1.micro
```

## Example run on Azure
```
$ ./pkb.py --cloud=Azure --machine_type=ExtraSmall --benchmarks=iperf
```

## Example run on DigitalOcean
```
$ ./pkb.py --cloud=DigitalOcean --machine_type=16gb --benchmarks=iperf
```

HOW TO RUN ALL STANDARD BENCHMARKS
==================
Run without the --benchmarks parameter and every benchmark in the standard set will run serially which can take a couple of hours (alternatively run with --benchmarks="standard_set").  Additionally if you dont specify --cloud=... all benchmarks will run on the Google Cloud Platform.


HOW TO RUN ALL BENCHMARKS IN A NAMED SET
==================
Named sets are are grouping of one or more benchmarks in the benchmarking directory. This feature allows parallel innovation of what is important to measure in the Cloud, and is defined by the set owner. For example the GoogleSet is maintained by Google, whereas the StanfordSet is managed by Stanford. Once a quarter a meeting is held to review all the sets to determine what benchmarks should be promoted to the standard_set. The Standard Set is also reviewed to see if anything should be removed.
To run all benchmarks in a named set, specify the set name in the benchmarks parameter (e.g. --benchmarks="standard_set"). Sets can be combined with individual benchmarks or other named sets.


USEFUL GLOBAL FLAGS
==================

The following are some common flags used when configuring
PerfKitBenchmaker.

Flag | Notes
-----|------
`--help`       | see all flags
`--cloud`      | Check where the bechmarks are run.  Choices are `GCP`, `AWS`, `Azure`, or `DigitalOcean`
`--zone`       | This flag allows you to override the default zone. See below.
`--benchmarks` | A comma separated list of benchmarks or benchmark sets to run such as `--benchmarks=iperf,ping` . To see the full list, run `./pkb.py --help`

The zone (region) as specified with the --zone flag uses the same
value that the Cloud CLIs take:

Cloud | Default | Notes
-------|---------|-------
GCP | us-central1-a | |
AWS | us-east-1a | |
Azure | East US | |
DigitalOcean | sfo1 | You must use a zone that supports the features 'metadata' (for cloud config) and 'private_networking'.

## Proxy configuration for VM guests.

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

ADVANCED: HOW TO RUN BENCHMARKS WITHOUT CLOUD PROVISIONING (eg: local workstation)
==================
It is possible to run PerfKitBenchmarker without running the Cloud provioning steps.  This is useful if you want to run on a local machine, or have a benchmark like iperf run from an external point to a Cloud VM.

In order to do this you need to make sure:
* The static (ie not provisioned by PerfKitBenchmarker) machine is ssh'able
* The user PerfKitBenchmarker will login as has 'sudo' access.  (*** Note we hope to remove this restriction soon ***)

Next you will want to create a JSON file describing how to connect to the machine as follows:
```
[
 {"ip_address": "170.200.60.23",
  "user_name": "voellm",
  "keyfile_path": "/home/voellm/perfkitkeys/my_key_file.pem",
  "scratch_disk_mountpoints": ["/scratch-disk"],
  "zone": "Siberia"}
]
```


* The `ip_address` is the address where you want benchmarks to run.
* `keyfile_file` is where to find the private ssh key.
* `zone` can be anything you want.  It is used when publishing results.
* `scratch_disk_mountpoints` is used by all benchmarks which use disk (i.e., `fio`, `bonnie++`, many others).

I called my file Siberia.json and used it to run iperf from Siberia to a GCP VM in us-central1-f as follows:
```
./pkb.py --benchmarks=iperf --machine_type=f1-micro --static_vm_file=Siberia.json --zone=us-central1-f --ip_addresses=EXTERNAL
```
* ip_addresses=EXTERNAL tells PerfKitBechmarker not to use 10.X (ie Internal) machine addresses that all Cloud VMs have.  Just use the external IP address.

If a benchmark requires two machines like iperf you can have two both machines into the same json file as shown below.  This means you can indeed run between two machines and never provision any VM's in the Cloud.

```
[
  {
    "ip_address": "<ip1>",
    "user_name": "connormccoy",
    "keyfile_path": "/home/connormccoy/.ssh/google_compute_engine",
    "internal_ip": "10.240.223.37",
    "install_packages", false
  },
  {
    "ip_address": "<ip2>",
    "user_name": "connormccoy",
    "keyfile_path": "/home/connormccoy/.ssh/google_compute_engine",
    "scratch_disk_mountpoints": ["/tmp/google-pkb"],
    "internal_ip": "10.240.234.189",
    "ssh_port": 2222
  }
]
```

HOW TO EXTEND PerfKitBenchmarker
=================
First start with the [CONTRIBUTING.md] (https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/CONTRIBUTING.md) 
file.  It has the basics on how to work with PerfKitBenchmarker, and how to submit your pull requests.

In addition to the [CONTRIBUTING.md] (https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/CONTRIBUTING.md)
file we have added a lot of comments into the code to make it easy to;
* Add new benchmarks (eg: --benchmarks=<new benchmark>)
* Add new package/os type support (eg: --os_type=<new os type>)
* Add new providers (eg: --cloud=<new provider>)
* etc...

Even with lots of comments we make to support more detailed documention.  You will find the documatation we have on the [Wiki pages] (https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki).  Missing documentation you want?  Start a page and/or open an [issue] (https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/issues) to get it added.

PLANNED IMPROVEMENTS
=======================
Many... please add new requests via GitHub issues.
