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

  - Bonnie++: GPL v2 (http://www.coker.com.au/bonnie++/readme.html)
  - cluster_boot_benchmark: MIT license.
  - coremark: EEMBC (https://www.eembc.org/)
  - fio: GPL v2 (https://github.com/axboe/fio/blob/master/LICENSE)
  - hadoop: Apache v2 (http://hadoop.apache.org/)
  - hpcc: Original BSD license (http://icl.cs.utk.edu/hpcc/faq/#263)
  - iperf: BSD license(http://iperf.sourceforge.net/)
  - netperf: HP license (http://www.calculate-linux.org/packages/licenses/netperf)
  - YCSB: Apache V2 (https://github.com/brianfrankcooper/YCSB/blob/master/LICENSE.txt)
  - memtier_benchmark: GPL v2 (https://github.com/RedisLabs/memtier_benchmark)
  - sysbench: GPL v2 (https://launchpad.net/sysbench)
  - unixbench: GPL v2 (https://code.google.com/p/byte-unixbench/)
  - speccpu2006 - Spec CPU2006 (http://www.spec.org/cpu2006/)
  - mesh_benchmark: HP license (http://www.calculate-linux.org/packages/licenses/netperf)
  * copy_benchmark: No license needed.
  - object_storage_benchmark: No license needed.
  - ping_benchmark: No license needed.
  - Aerospike: Apache V2 for the client and GNU AGPL v3.0 for the server (http://www.aerospike.com/products-services/)

Some of the benchmarks invoked require Java. You must also agree with the following license:
  - openjdk-7-jre: GPL v2 with the Classpath Exception (http://openjdk.java.net/legal/gplv2+ce.html)

CoreMark setup cannot be automated. EEMBC requires users to agree with their terms and conditions, and PerfKit
Benchmarker users must manually download the coremark tarball from their website and save it under the
`perfkitbenchmarker/data` folder (e.g. `~/PerfKitBenchmarker/perfkitbenchmarker/data/coremark_v1.0.tgz`)

SpecCPU2006 benchmark setup cannot be automated. SPEC requires users to agree with their terms and conditions,
and PerfKit Benchmarker users must manually download SpecCPU2006 tarball from their website and save it under
the `./perfkitbenchmarker/data` folder (e.g. `~/PerfKitBenchmarker/perfkitbenchmarker/data/cpu2006v1.2.tgz`)

HOW TO GET SETUP
==================
Before you can run the PerfKit Benchmaker on Cloud providers you need accounts and access:
* Get a GCE account to run tests on GCE. Our site is https://cloud.google.com
* Get an AWS account to run tests on AWS. Their site is http://aws.amazon.com/
* Get an Azure account to run tests on Azure. Their site is http://azure.microsoft.com/

You also need the software dependencies, which are mostly command line tools and credentials to access your
accounts without a password.  The following steps should help you get the CLI tool auth in place.

## Install gcloud and setup authentication
Instructions: https://developers.google.com/cloud/sdk/. If you're using linux you can run the command below.

When prompted pick the local folder, then Python project, then the defaults for all the rest
```
$ curl https://sdk.cloud.google.com | bash
```

Restart your shell window (or logout/ssh again if running on a VM)
Set your credentials up: https://developers.google.com/cloud/sdk/gcloud/#gcloud.auth. Run the command below.
It will print a web page URL. Navigate there, authorize the gcloud instance you just installed to use the services
it lists, copy the access token and give it to the shell prompt.
```
$ gcloud auth login
```

You will need a project ID before you can run. Please navigate to https://console.developers.google.com and
create one.

## Install AWS CLI and setup authentication
Install pip. http://pip.readthedocs.org/en/latest/installing.html
```
$ sudo apt-get install python-pip -y
```

Follow instructions at http://aws.amazon.com/cli/ or run the following command
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
```
$ sudo apt-get install build-essential -y
$ wget http://nodejs.org/dist/v0.10.26/node-v0.10.26.tar.gz
$ tar xzvf node-v0.10.26.tar.gz
$ cd node-v0.10.26
$ ./configure --prefix=/usr
$ make
$ sudo make install
$ chmod +x /usr/bin/node
$ cd ..
$ sudo npm install azure-cli -g
$ azure account download
```

Read the output of the previous command. It will contain a webpage URL. Open that in a browser. It will download
a file (.publishsettings) file. Copy to the folder you're running PerfKit Benchmarker. In my case the file was called
"Free Trial-7-18-2014-credentials.publishsettings"
```
$ azure account import [path to .publishsettings file]
```

Test that azure is installed correctly
```
$ azure vm list
```

## Install PerfKit dependencies
```
$ sudo pip install python-gflags jinja2 setuptools
```

Pull the PerfKit Benchmarker from GitHub.

HOW TO RUN ONE BENCHMARK
==================
PerfKitBenchmarks can run benchmarks both on Cloud Providers (GCP, AWS, Azure) as well as any "machine" you can SSH into.

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

HOW TO RUN ALL BENCHMARKS
==================
Run without the --benchmarks parameter and every benchmark will run serially which can take a couple of hours.  Additionally if you dont specify --cloud=... all benchmarks will run on the Google Cloud Platform.

USEFUL GLOBAL FLAGS
==================
```
The following are some common flags used when configuring PerfKitBenchmaker.
--help           : see all flags
--cloud          : Check where the bechmarks are run.  Choices are GCP, AWS, or AZURE
--zone           : This flag always you to override the default zone.  It is thats the same value that the Cloud CLI's take such as --zone=us-central1-a is use for GCP, --zone=us-east-1a is used for AWS, and --zone='East US' is used by AZURE.
--benchmarks     : A comman separted list of benchmarks to run such as --benchmarks=iperf,ping . To see the full list just run ./pkd.py --help
```
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
  "zone": "Siberia"}
]
```

* The ip_address is the address where you want benchmarks to run.
* The my_key_file.pem is the same key you pass to the ssh machine using the -i option.
* keyfile_file is where to find the private ssh key.
* zone can be anything you want.  It is used when publishing results.

I called my file Siberia.json and used it to run iperf from Siberia to a GCP VM in us-central1-f as follows:
```
./pkb.py --benchmarks=iperf --machine_type=f1-micro --static_vm_file=Siberia.json --zone=us-central1-f --ip_addresses=EXTERNAL
```
* ip_addresses=EXTERNAL tells PerfKitBechmarker not to use 10.X (ie Internal) machine addresses that all Cloud VMs have.  Just use the external IP address.

If a benchmark requires two machines like iperf you can have two static VM files specified as a comma separated list.  This means you can indeed run between two machines and never provision any VM's in the Cloud.

PLANNED IMPROVEMENTS
=======================
Many... please add new requests via GitHub issues.
