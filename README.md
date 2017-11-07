PerfKit Benchmarker
==================

PerfKit Benchmarker is an open effort to define a canonical set of benchmarks to measure and compare cloud
offerings.  It's designed to operate via vendor provided command line tools. The benchmark default settings are not
tuned for any particular platform or instance type. These settings are recommended for consistency across services.
Only in the rare case where there is a common practice like setting the buffer pool size of a database do we
change any settings.

This README is designed to give you the information you need to get running with the benchmarker and the basics of working with the code.  The [wiki](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki) contains more detailed information:

* [FAQ](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/FAQ)
* [Tech Talks](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/Tech-Talks)
* [Governing rules](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/Governing-Rules)
* [Community meeting decks and notes](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/Community-Meeting-Notes-Decks)
* [Design documents](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/Design-Docs)
* You are always welcome to [open an issue](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/issues), or to join us on #PerfKitBenchmarker on freenode to discuss issues you're having, pull requests, or anything else related to PerfKitBenchmarker


Known Issues
============


Licensing
=========

PerfKit Benchmarker provides wrappers and workload definitions around popular benchmark tools. We made it very simple
to use and automate everything we can.  It instantiates VMs on the Cloud provider of your choice, automatically
installs benchmarks, and runs the workloads without user interaction.

Due to the level of automation you will not see prompts for software installed as part of a benchmark run.
Therefore you must accept the license of each of the benchmarks individually, and take responsibility for using them
before you use the PerfKit Benchmarker.

In its current release these are the benchmarks that are executed:

  - `aerospike`: [Apache v2 for the client](http://www.aerospike.com/aerospike-licensing/)
    and [GNU AGPL v3.0 for the server](https://github.com/aerospike/aerospike-server/blob/master/LICENSE)
  - `bonnie++`: [GPL v2](http://www.coker.com.au/bonnie++/readme.html)
  - `cassandra_ycsb`: [Apache v2](http://cassandra.apache.org/)
  - `cassandra_stress`: [Apache v2](http://cassandra.apache.org/)
  - `cloudsuite3.0`: [CloudSuite 3.0 license](http://cloudsuite.ch/pages/license/)
  - `cluster_boot`: MIT License
  - `coremark`: [EEMBC](https://www.eembc.org/)
  - `copy_throughput`: Apache v2
  - `fio`: [GPL v2](https://github.com/axboe/fio/blob/master/COPYING)
  - [`gpu_pcie_bandwidth`](https://developer.nvidia.com/cuda-downloads): [NVIDIA
    Software Licence
    Agreement](http://docs.nvidia.com/cuda/eula/index.html#nvidia-driver-license)
  - `hadoop_terasort`: [Apache v2](http://hadoop.apache.org/)
  - `hpcc`: [Original BSD license](http://icl.cs.utk.edu/hpcc/faq/#263)
  - [`hpcg`](https://github.com/hpcg-benchmark/hpcg/): [BSD
    3-clause](https://github.com/hpcg-benchmark/hpcg/blob/master/LICENSE)
  - `iperf`: [BSD license](http://iperf.sourceforge.net/)
  - `memtier_benchmark`: [GPL v2](https://github.com/RedisLabs/memtier_benchmark)
  - `mesh_network`: [HP license](http://www.calculate-linux.org/packages/licenses/netperf)
  - `mongodb`: **Deprecated**. [GNU AGPL v3.0](http://www.mongodb.org/about/licensing/)
  - `mongodb_ycsb`: [GNU AGPL v3.0](http://www.mongodb.org/about/licensing/)
  - [`multichase`](https://github.com/google/multichase):
    [Apache v2](https://github.com/google/multichase/blob/master/LICENSE)
  - `netperf`: [HP license](http://www.calculate-linux.org/packages/licenses/netperf)
  - [`oldisim`](https://github.com/GoogleCloudPlatform/oldisim):
    [Apache v2](https://github.com/GoogleCloudPlatform/oldisim/blob/master/LICENSE.txt)
  - `object_storage_service`: Apache v2
  - `pgbench`: [PostgreSQL Licence](https://www.postgresql.org/about/licence/)
  - `ping`: No license needed.
  - `silo`: MIT License
  - `scimark2`: [public domain](http://math.nist.gov/scimark2/credits.html)
  - `speccpu2006`: [SPEC CPU2006](http://www.spec.org/cpu2006/)
  - [`SHOC`](https://github.com/vetter/shoc): [BSD
    3-clause](https://github.com/vetter/shoc/blob/master/LICENSE.txt)
  - `sysbench_oltp`: [GPL v2](https://github.com/akopytov/sysbench)
  - [`TensorFlow`](https://github.com/tensorflow/tensorflow): [Apache
    v2](https://github.com/tensorflow/tensorflow/blob/master/LICENSE)
  - [`tomcat`](https://github.com/apache/tomcat):
    [Apache v2](https://github.com/apache/tomcat/blob/trunk/LICENSE)
  - [`unixbench`](https://github.com/kdlucas/byte-unixbench):
    [GPL v2](https://github.com/kdlucas/byte-unixbench/blob/master/LICENSE.txt)
  - [`wrk`](https://github.com/wg/wrk):
    [Modified Apache v2](https://github.com/wg/wrk/blob/master/LICENSE)
  - [`ycsb`](https://github.com/brianfrankcooper/YCSB) (used by `mongodb`, `hbase_ycsb`, and others):
    [Apache v2](https://github.com/brianfrankcooper/YCSB/blob/master/LICENSE.txt)

Some of the benchmarks invoked require Java. You must also agree with the following license:

  - `openjdk-7-jre`: [GPL v2 with the Classpath Exception](http://openjdk.java.net/legal/gplv2+ce.html)

[CoreMark](http://www.eembc.org/coremark/) setup cannot be automated. EEMBC requires users to agree with their terms and conditions, and PerfKit
Benchmarker users must manually download the CoreMark tarball from their website and save it under the
`perfkitbenchmarker/data` folder (e.g. `~/PerfKitBenchmarker/perfkitbenchmarker/data/coremark_v1.0.tgz`)

[SPEC CPU2006](https://www.spec.org/cpu2006/) benchmark setup cannot be
automated. SPEC requires that users purchase a license and agree with their
terms and conditions. PerfKit Benchmarker users must manually download
`cpu2006-1.2.iso` from the SPEC website, save it under the
`perfkitbenchmarker/data` folder (e.g.
`~/PerfKitBenchmarker/perfkitbenchmarker/data/cpu2006-1.2.iso`), and also
supply a runspec cfg file (e.g.
`~/PerfKitBenchmarker/perfkitbenchmarker/data/linux64-x64-gcc47.cfg`).
Alternately, PerfKit Benchmarker can accept a tar file that can be generated
with the following steps:

* Extract the contents of `cpu2006-1.2.iso` into a directory named `cpu2006`
* Run `cpu2006/install.sh`
* Copy the cfg file into `cpu2006/config`
* Create a tar file containing the `cpu2006` directory, and place it under the
  `perfkitbenchmarker/data` folder (e.g.
  `~/PerfKitBenchmarker/perfkitbenchmarker/data/cpu2006v1.2.tgz`).

PerfKit Benchmarker will use the tar file if it is present. Otherwise, it will
search for the iso and cfg files.


Installing PerfKit Benchmarker and Prerequisites
================================================
Before you can run the PerfKit Benchmarker, you need account(s) on the cloud provider(s) you want to benchmark:

* [Google Cloud Platform](https://cloud.google.com)
* [AWS](http://aws.amazon.com)
* [Azure](http://azure.microsoft.com)
* [AliCloud](http://www.aliyun.com)
* [DigitalOcean](https://www.digitalocean.com)
* [Rackspace Cloud](https://www.rackspace.com)
* [ProfitBricks](https://www.profitbricks.com/)

You also need the software dependencies, which are mostly command line tools and credentials to access your
accounts without a password.  The following steps should help you get the CLI tool auth in place.

If you are running on Windows, you will need to install GitHub Windows
since it includes tools like `openssl` and an `ssh` client. Alternatively, you can
install Cygwin since it should include the same tools.

## Install Python 2.7 and `pip`
If you are running on Windows, get the latest version of Python 2.7
[here](https://www.python.org/downloads/windows/). This should have `pip` bundled
with it. Make sure your `PATH` environment variable is set so that you can use
both `python` and `pip` on the command line (you can have the installer do it
for you if you select the correct option).

Most Linux distributions and recent Mac OS X versions already have Python 2.7
installed.
If Python is not installed, you can likely install it using your distribution's package manager, or see the [Python Download page](https://www.python.org/downloads/).

If you need to install `pip`, see [these instructions](http://pip.readthedocs.org/en/stable/installing/).

## (*Windows Only*) Install GitHub Windows

Instructions: https://windows.github.com/

Make sure that `openssl`/`ssh`/`scp`/`ssh-keygen` are on your path (you will need to update the `PATH` environment variable).
The path to these commands should be

`C:\\Users\\\<user\>\\AppData\\Local\\GitHub\\PortableGit\_\<guid\>\\bin`

## Install PerfKit

[Download PerfKit Benchmarker](http://github.com/GoogleCloudPlatform/PerfKitBenchmarker/releases) from GitHub.

## Install PerfKit Benchmarker dependencies
```bash
$ cd /path/to/PerfKitBenchmarker
$ sudo pip install -r requirements.txt
```

## Cloud account setup

This section describes the setup steps needed for each cloud system. Note that you only need to perform setup steps on the clouds you wish to test. If you only want to test Google Cloud, you only need to install and configure `gcloud`.
* [Google Cloud](#install-gcloud-and-setup-authentication)
* [OpenStack](#install-openstack-cli-client-and-setup-authentication)
* [Kubernetes](#kubernetes-configuration-and-credentials)
* [Mesos](#mesos-configuration)
* [Cloudstack](#cloudstack-install-dependencies-and-set-the-api-keys)
* [AWS](#install-aws-cli-and-setup-authentication)
* [Azure](#windows-azure-cli-and-credentials)
* [AliCloud](#install-alicloud-cli-and-setup-authentication)
* [DigitalOcean](#digitalocean-configuration-and-credentials)
* [RackSpace](#installing-clis-and-credentials-for-rackspace)
* [ProfitBricks](#profitbricks-configuration-and-credentials)

After configuring the clouds you intend to use, skip to [Running a Single Benchmark](#running-a-single-benchmark), unless you are going to use an object storage benchmark, in which case you need to [configure a boto file](#create-and-configure-a-boto-file-for-object-storage-benchmarks).

### Install `gcloud` and setup authentication
Instructions: https://developers.google.com/cloud/sdk/. If you're using OS X or Linux, you can run the command below:

```bash
$ curl https://sdk.cloud.google.com | bash
```

When prompted, pick the local folder, then Python project, then the defaults for all the rest.

Restart your shell window (or logout/ssh again if running on a VM)

On Windows, visit the same page and follow the Windows installation instructions on the page.

Next, create a project by visiting [Google Cloud Console](https://console.developers.google.com).  After that, run:

```bash
$ gcloud init
```

which helps you authenticate, set your project, and set some defaults.

Alternatively, if that is already set up, and you just need to authenticate, you can use:

```bash
$ gcloud auth login
```

For help, see [`gcloud` docs](https://cloud.google.com/sdk/gcloud/).

### Install OpenStack CLI client and setup authentication
Make sure you have installed pip (see the section above).

Install OpenStack CLI utilities via the following command:

```bash
$ sudo pip install -r perfkitbenchmarker/providers/openstack/requirements.txt
```

To setup credentials and endpoint information simply set the environment
variables using an OpenStack RC file. For help, see [`OpenStack` docs](http://docs.openstack.org/cli-reference/common/cli_set_environment_variables_using_openstack_rc.html)

### Kubernetes configuration and credentials
Perfkit uses the `kubectl` binary in order to communicate with a Kubernetes cluster - you need to pass the path to the `kubectl` binary using the `--kubectl` flag. It's recommended to use [version 1.0.1](https://storage.googleapis.com/kubernetes-release/release/v1.0.1/bin/linux/amd64/kubectl).
Authentication to a Kubernetes cluster is done via a [`kubeconfig` file](https://github.com/kubernetes/kubernetes/blob/release-1.0/docs/user-guide/kubeconfig-file.md). Its path is passed using the `--kubeconfig` flag.

**Image prerequisites**  
Please refer to the [Image prerequisites for Docker based clouds](#image-prerequisites-for-docker-based-clouds).

**Kubernetes cluster configuration**  
If your Kubernetes cluster is running on CoreOS:

1. Fix `$PATH` environment variable so that the appropriate binaries can be found:

   ```bash
   $ sudo mkdir /etc/systemd/system/kubelet.service.d
   $ sudo vim /etc/systemd/system/kubelet.service.d/10-env.conf
   ```

   Add the following line to the `[Service]` section:

   ```
   Environment=PATH=/opt/bin:/usr/bin:/usr/sbin:$PATH
   ```

2. Reboot the node:

   ```bash
   $ sudo reboot
   ```

Note that some benchmarks must be run within a privileged container. By default Kubernetes doesn't allow containers to be scheduled in privileged mode - you have to add the `--allow-privileged=true` flag to `kube-apiserver` and each `kubelet` startup command.

**Ceph integration**  
When you run benchmarks with the standard scratch disk type (`--scratch_disk_type=standard` - which is a default option), Ceph storage will be used. There are some configuration steps you need to follow before you will be able to spawn Kubernetes PODs with Ceph volume. On each Kubernetes node, and on the machine which is running the Perfkit benchmarks, do the following:

1. Copy `/etc/ceph` directory from Ceph-host.

2. Install `ceph-common` package so that `rbd` command is available:

  * If your Kubernetes cluster is running on CoreOS, then you need to create a bash script called `rbd` which will run the `rbd` command inside a Docker container:

      ```bash
      #!/usr/bin/bash
      /usr/bin/docker run -v /etc/ceph:/etc/ceph -v /dev:/dev -v /sys:/sys  --net=host --privileged=true --rm=true ceph/rbd $@
      ```

      Save the file as `rbd` and run:

      ```bash
      $ chmod +x rbd
      $ sudo mkdir /opt/bin
      $ sudo cp rbd /opt/bin
      ```

      Install [`rbdmap`](https://github.com/ceph/ceph-docker/tree/master/examples/coreos/rbdmap):

      ```bash
      $ git clone https://github.com/ceph/ceph-docker.git
      $ cd ceph-docker/examples/coreos/rbdmap/
      $ sudo mkdir /opt/sbin
      $ sudo cp rbdmap /opt/sbin
      $ sudo cp ceph-rbdnamer /opt/bin
      $ sudo cp 50-rbd.rules /etc/udev/rules.d
      $ sudo reboot
      ```

You have [two Ceph authentication options](http://kubernetes.io/v1.0/examples/rbd/README.html) available:

1. Keyring - pass the path to the keyring file using `--ceph_keyring` flag
2. Secret. In this case you have to create a secret first:

   Retrieve base64-encoded Ceph admin key:

   ```bash
   $ ceph auth get-key client.admin | base64
   QVFEYnpPWlZWWnJLQVJBQXdtNDZrUDlJUFo3OXdSenBVTUdYNHc9PQ==  
   ```

   Create a file called `create_ceph_admin.yml` and replace the `key` value with the output from the previous command:

   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: my-ceph-secret
   data:
     key: QVFEYnpPWlZWWnJLQVJBQXdtNDZrUDlJUFo3OXdSenBVTUdYNHc9PQ==
   ```

   Add secret to Kubernetes:  

   ```bash
   $ kubectl create -f create_ceph_admin.yml
   ```

   You will have to pass the Secret name (using `--ceph_secret` flag) when running the benchmakrs. In this case it should be: `--ceph_secret=my-ceph-secret`.

### Mesos configuration
Mesos provider communicates with Marathon framework in order to manage Docker instances. Thus it is required to setup Marathon framework along with the Mesos cluster. In order to connect to Mesos you need to provide IP address and port to Marathon framework using `--marathon_address` flag.

Provider has been tested with Mesos v0.24.1 and Marathon v0.11.1.

**Overlay network**  
Mesos on its own doesn't provide any solution for overlay networking. You need to configure your cluster so that the instances will live in the same network. For this purpose you may use Flannel, Calico, Weave, etc.

**Mesos cluster configuration**  
Make sure your Mesos-slave nodes are reachable (by hostname) from the machine which is used to run the benchmarks. In case they are not, edit the `/etc/hosts` file appropriately.

**Image prerequisites**  
Please refer to the [Image prerequisites for Docker based clouds](#image-prerequisites-for-docker-based-clouds).

### Cloudstack: Install dependencies and set the API keys

```bash
$ sudo pip install -r perfkitbenchmarker/providers/cloudstack/requirements.txt
```

Get the API key and SECRET from Cloudstack. Set the following environement variables.

```bash
export CS_API_URL=<insert API endpoint>
export CS_API_KEY=<insert API key>
export CS_API_SECRET=<insert API secret>
```

Specify the network offering when running the benchmark. If using VPC
(`--cs_use_vpc`), also specify the VPC offering (`--cs_vpc_offering`).

```bash
$ ./pkb.py --cloud=CloudStack --benchmarks=ping --cs_network_offering=DefaultNetworkOffering
```

### Install AWS CLI and setup authentication
Make sure you have installed pip (see the section above).

Follow instructions at http://aws.amazon.com/cli/ or run the following command (omit the 'sudo' on Windows)

```bash
$ sudo pip install -r perfkitbenchmarker/providers/aws/requirements.txt
```

Navigate to the AWS console to create access credentials: https://console.aws.amazon.com/ec2/
* On the console click on your name (top left)
* Click on "Security Credentials"
* Click on "Access Keys", the create New Access Key. Download the file, it contains the Access key and Secret keys
  to access services. Note the values and delete the file.

Configure the CLI using the keys from the previous step:

```bash
$ aws configure
```

### Windows Azure CLI and credentials

You first need to install node.js and NPM.  This version of Perfkit Benchmarker
is known to be compatible with Azure CLI version 0.10.4, and will likely work
with any version newer than that.

Go [here](https://nodejs.org/download/), and follow the setup instructions.

Next, run the following (omit the `sudo` on Windows):

```bash
$ sudo npm install azure-cli -g
$ azure login
```
Test that `azure` is installed correctly:

```bash
$ azure vm list
```

Finally, make sure Azure is in Resource Management mode and that your account is
authorized to allocate VMs and networks from Azure:

```bash
$ azure config mode arm
$ azure provider register Microsoft.Compute
$ azure provider register Microsoft.Network
```

### Install AliCloud CLI and setup authentication
Make sure you have installed pip (see the section above).

Run the following command to install `aliyuncli` (omit the `sudo` on Windows)

1. Install python development tools:

   In Debian or Ubuntu:

   ```bash
   $ sudo apt-get install -y python-dev
   ```

   In CentOS:

   ```bash
   $ sudo yum install python-devel
   ```

2. Install aliyuncli tool and python SDK for ECS:

   ```bash
   $ sudo pip install -r perfkitbenchmarker/providers/alicloud/requirements.txt
   ```
   In some CentOS version, you may need:
   
   ```bash
   $ sudo yum install libffi-devel.x86_64
   $ sudo yum install openssl-devel.x86_64
   $ sudo pip install 'colorama<=0.3.3'
   ```

   To check if AliCloud is installed:

   ```bash
   $ aliyuncli --help
   ```

   Check if `aliyuncli ecs` command is ready:

   ```bash
   $ aliyuncli ecs help
   ```

   If you see the "usage" message, you should follow step 3.
   Otherwise, jump to step 4.

3. Dealing with an exception when it runs on some specific version of Ubuntu.
   Get the python lib path: `/usr/lib/python2.7/dist-packages`

   ```bash
   $ python
   > from distutils.sysconfig import get_python_lib
   > get_python_lib()
   '/usr/lib/python2.7/dist-packages'
   ```

   Copy to the right directory (for Python 2.7.X):

   ```bash
   $ sudo cp -r /usr/local/lib/python2.7/dist-packages/aliyun* /usr/lib/python2.7/dist-packages/
   ```

   Check again:

   ```bash
   $ aliyuncli ecs help
   ```

4. Navigate to the [AliCloud console](https://home.console.alicloud.com/#/) to create access credentials:
   * Login first
   * Click on "AccessKeys" (top right)
   * Click on "Create Access Key", copy and store the "Access Key ID" and "Access Key Secret" to a safe place.
   * Configure the CLI using the Access Key ID and Access Key Secret from the previous step

   ```bash
   $ aliyuncli configure
   ```

### DigitalOcean configuration and credentials

1. Install `doctl`, the DigitalOcean CLI, following the instructions at
`https://github.com/digitalocean/doctl`.

2. Authenticate with `doctl`. The easiest way is running `doctl auth login` and
   following the instructions, but any of the options at the `doctl` site will
   work.

### Installing CLIs and credentials for Rackspace

In order to interact with the Rackspace Public Cloud, PerfKitBenchmarker makes
use of RackCLI. You can find the instructions to install and configure
RackCLI here: https://developer.rackspace.com/docs/rack-cli/

To run PerfKit Benchmarker against Rackspace is very easy. Simply make sure
Rack CLI is installed and available in your PATH, optionally use the flag
`--rack_path` to indicate the path to the binary.

For a Rackspace UK Public Cloud account, unless it's your default RackCLI
profile then it's recommended that you create a profile for your UK account.
Once configured, use flag `--profile` to specify which RackCLI profile to use.
You can find more details here: https://developer.rackspace.com/docs/rack-cli/configuration/#config-file

**Note:** Not all flavors are supported on every region. Always check first
if the flavor is supported in the region.


### ProfitBricks configuration and credentials

Get started by running:
```bash
$ sudo pip install -r perfkitbenchmarker/providers/profitbricks/requirements.txt
```

PerfKit Benchmarker uses the 
<a href='http://docs.python-requests.org/en/master/'>Requests</a> module 
to interact with ProfitBricks' REST API. HTTP Basic authentication is used 
to authorize access to the API. Please set this up as follows:

Create a configuration file containing the email address and password 
associated with your ProfitBricks account, separated by a colon. 
Example:

```bash
$ less ~/.config/profitbricks-auth.cfg
email:password
```

The PerfKit Benchmarker will automatically base64 encode your credentials 
before making any calls to the REST API.

PerfKit Benchmarker uses the file location `~/.config/profitbricks-auth.cfg`
by default. You can use the `--profitbricks_config` flag to
override the path.


## Image prerequisites for Docker based clouds
Docker instances by default don't allow to SSH into them. Thus it is important to configure your Docker image so that it has SSH server installed. You can use your own image or build a new one based on a Dockerfile placed in `tools/docker_images` directory - in this case please refer to [Docker images document](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/tree/master/tools/docker_images).


## Create and configure a `.boto` file for object storage benchmarks

In order to run object storage benchmark tests, you need to have a properly configured `~/.boto` file.  The directions require that you have installed `google-cloud-sdk`.  The directions for doing that are in the [gcloud installation section](#install-gcloud-and-setup-authentication).

Here is how:

* Create the `~/.boto` file (If you already have ~/.boto, you can skip this step. Consider making a backup copy of your existing .boto file.)

To create a new `~/.boto` file, issue the following command and follow the instructions given by this command:

```bash
$ gsutil config
```

As a result, a `.boto` file is created under your home directory.

Open the `.boto` file and edit the following fields:

1. In the [Credentials] section:

   `gs_oauth2_refresh_token`: set it to be the same as the `refresh_token` field in your gcloud credential file (~/.config/gcloud/credentials.db), which was setup as part of the `gcloud auth login` step. To see the refresh token, run
   ```bash
   $ strings ~/.config/gcloud/credentials.db.
   ```

   `aws_access_key_id`, `aws_secret_access_key`: set these to be the AWS access keys you intend to use for these tests, or you can use the same keys as those in your existing AWS credentials file (`~/.aws/credentials`).

2. In the `[GSUtil]` section:

   `default_project_id`: if it is not already set, set it to be the google cloud storage project ID you intend to use for this test. (If you used `gsutil config` to generate the `.boto` file, you should have been prompted to supply this information at this step).

3. In the `[OAuth2]` section:

   `client_id`, `client_secret`: set these to be the same as those in your gcloud credentials file (`~/.config/gcloud/credentials.db`), which was setup as part of the `gcloud auth login` step.


Running a Single Benchmark
==========================
PerfKit Benchmarker can run benchmarks both on Cloud Providers (GCP,
AWS, Azure, DigitalOcean) as well as any "machine" you can SSH into.

## Example run on GCP

```bash
$ ./pkb.py --project=<GCP project ID> --benchmarks=iperf --machine_type=f1-micro
```

## Example run on AWS

```bash
$ cd PerfKitBenchmarker
$ ./pkb.py --cloud=AWS --benchmarks=iperf --machine_type=t1.micro
```

## Example run on Azure

```bash
$ ./pkb.py --cloud=Azure --machine_type=ExtraSmall --benchmarks=iperf
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
$ ./pkb.py --cloud=Kubernetes --benchmarks=iperf --kubectl=/path/to/kubectl --kubeconfig=/path/to/kubeconfig --image=image-with-ssh-server  --ceph_monitors=10.20.30.40:6789,10.20.30.41:6789 --kubernetes_nodes=10.20.30.42,10.20.30.43
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

How to Run Windows Benchmarks
==================
Install all dependencies as above and ensure that smbclient is installed on
your system if you are running on a linux controller:
```bash
$ which smbclient
/usr/bin/smbclient
```

Now you can run Windows benchmarks by running with `--os_type=windows`. Windows has a
different set of benchmarks than Linux does. They can be found under
[`perfkitbenchmarker/windows_benchmarks/`](perfkitbenchmarker/windows_benchmarks).
The target VM OS is Windows Server 2012 R2.

How to Run Benchmarks with Juju
==================

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

How to Run All Standard Benchmarks
==================
Run without the `--benchmarks` parameter and every benchmark in the standard set will run serially which can take a couple of hours (alternatively, run with `--benchmarks="standard_set"`).  Additionally, if you don't specify `--cloud=...`, all benchmarks will run on the Google Cloud Platform.


How to Run All Benchmarks in a Named Set
==================
Named sets are are groupings of one or more benchmarks in the benchmarking directory. This feature allows parallel innovation of what is important to measure in the Cloud, and is defined by the set owner. For example the GoogleSet is maintained by Google, whereas the StanfordSet is managed by Stanford. Once a quarter a meeting is held to review all the sets to determine what benchmarks should be promoted to the `standard_set`. The Standard Set is also reviewed to see if anything should be removed.
To run all benchmarks in a named set, specify the set name in the benchmarks parameter (e.g., `--benchmarks="standard_set"`). Sets can be combined with individual benchmarks or other named sets.


Useful Global Flags
==================

The following are some common flags used when configuring
PerfKit Benchmarker.

Flag | Notes
-----|------
`--helpmatch=pkb`         | see all global flags
`--helpmatch=hpcc` | see all flags associated with the hpcc benchmark. You can substitute any benchmark name to see the associated flags.
`--benchmarks`   | A comma separated list of benchmarks or benchmark sets to run such as `--benchmarks=iperf,ping` . To see the full list, run `./pkb.py --help`
`--cloud`        | Cloud where the benchmarks are run. See the table below for choices.
`--machine_type` | Type of machine to provision if pre-provisioned machines are not used. Most cloud providers accept the names of pre-defined provider-specific machine types (for example, GCP supports `--machine_type=n1-standard-8` for a GCE n1-standard-8 VM). Some cloud providers support YAML expressions that match the corresponding VM spec machine_type property in the [YAML configs](#configurations-and-configuration-overrides) (for example, GCP supports `--machine_type="{cpus: 1, memory: 4.5GiB}"` for a GCE custom VM with 1 vCPU and 4.5GiB memory). Note that the value provided by this flag will affect all provisioned machines; users who wish to provision different machine types for different roles within a single benchmark run should use the [YAML configs](#configurations-and-configuration-overrides) for finer control.
`--zones`         | This flag allows you to override the default zone. See the table below.
`--data_disk_type` | Type of disk to use. Names are provider-specific, but see table below.

The default cloud is 'GCP', override with the `--cloud` flag. Each cloud has a default
zone which you can override with the `--zones` flag, the flag supports the same values
that the corresponding Cloud CLIs take:

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
ProfitBricks | AUTO | Additional zones: ZONE_1, ZONE_2, or ZONE_3

Example:

```bash
./pkb.py --cloud=GCP --zones=us-central1-a --benchmarks=iperf,ping
```

The disk type names vary by provider, but the following table summarizes some
useful ones. (Many cloud providers have more disk types beyond these options.)

Cloud name | Network-attached SSD | Network-attached HDD
-----------|----------------------|---------------------
GCP | pd-ssd | pd-standard
AWS | gp2 | standard
Azure | Premium_LRS | Standard_LRS
Rackspace | cbs-ssd | cbs-sata

Also note that `--data_disk_type=local` tells PKB not to allocate a separate
disk, but to use whatever comes with the VM. This is useful with AWS instance
types that come with local SSDs, or with the GCP `--gce_num_local_ssds` flag.

If an instance type comes with more than one disk, PKB uses whichever does *not*
hold the root partition. Specifically, on Azure, PKB always uses `/dev/sdb` as
its scratch device.

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

Configurations and Configuration Overrides
==================
Each benchmark now has an independent configuration which is written in YAML.
Users may override this default configuration by providing a configuration.
This allows for much more complex setups than previously possible,
including running benchmarks across clouds.

A benchmark configuration has a somewhat simple structure. It is essentially
just a series of nested dictionaries. At the top level, it contains VM groups.
VM groups are logical groups of homogenous machines. The VM groups hold both a
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

Advanced: How To Run Benchmarks Without Cloud Provisioning (e.g., local workstation)
==================
It is possible to run PerfKit Benchmarker without running the Cloud provisioning steps.  This is useful if you want to run on a local machine, or have a benchmark like iperf run from an external point to a Cloud VM.

In order to do this you need to make sure:
* The static (i.e. not provisioned by PerfKit Benchmarker) machine is ssh'able
* The user PerfKitBenchmarker will login as has 'sudo' access.  (*** Note we hope to remove this restriction soon ***)

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
$ ./pkb.py --benchmarks=iperf --machine_type=f1-micro --benchmark_config_file=iperf.yaml --zones=us-central1-f --ip_addresses=EXTERNAL
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

Specifying Flags in Configuration Files
=================
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

Using Elasticsearch Publisher
=================
PerfKit data can optionally be published to an Elasticsearch server. To enable this, the
`elasticsearch` Python package must be installed.

```bash
$ sudo pip install elasticsearch
```

Note: The `elasticsearch` Python library and Elasticsearch must have matching major versions.

The following are flags used by the Elasticsearch publisher. At minimum, all that is needed
is the `--es_uri` flag.

Flag | Notes
-----|------
`--es_uri`         | The Elasticsearch server address and port (e.g. localhost:9200)
`--es_index`       | The Elasticsearch index name to store documents (default: perfkit)
`--es_type`        | The Elasticsearch document type (default: result)

Note: Amazon ElasticSearch service currently does not support transport on port 9200 therefore you must use endpoint with port 80 eg. `search-<ID>.es.amazonaws.com:80` and allow your IP address in the cluster.

Using InfluxDB Publisher
=================
No additional packages need to be installed in order to publish Perfkit data to an InfluxDB
server.

InfluxDB Publisher takes in the flags for the Influx uri and the Influx DB name. The publisher
will default to the pre-set defaults, identified below, if no uri or DB name is set. However,
the user is required to at the very least call the `--influx_uri` flag to publish data to Influx.


| Flag               | Notes                                                                | Default        |
|--------------------|----------------------------------------------------------------------|----------------|
| `--influx_uri`     | The Influx DB address and port. Expects the format hostname:port     | localhost:8086 |
| `--influx_db_name` | The name of Influx DB database that you wish to publish to or create | perfkit        |

How to Extend PerfKit Benchmarker
=================
First start with the [CONTRIBUTING.md](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/CONTRIBUTING.md)
file.  It has the basics on how to work with PerfKitBenchmarker, and how to submit your pull requests.

In addition to the [CONTRIBUTING.md](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/CONTRIBUTING.md)
file we have added a lot of comments into the code to make it easy to:

* Add new benchmarks (e.g.: `--benchmarks=<new benchmark>`)
* Add new package/os type support (e.g.: `--os_type=<new os type>`)
* Add new providers (e.g.: `--cloud=<new provider>`)
* etc.

Even with lots of comments we make to support more detailed documention.  You will find the documentation we have on the [wiki](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki).  Missing documentation you want?  Start a page and/or open an [issue](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/issues) to get it added.

Integration Testing
===================
If you wish to run unit or integration tests, ensure that you have `tox >= 2.0.0` installed.

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


Planned Improvements
=======================
Many... please add new requests via GitHub issues.
