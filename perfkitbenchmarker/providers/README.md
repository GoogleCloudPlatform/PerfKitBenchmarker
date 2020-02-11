Before you can run the PerfKit Benchmarker, you need account(s) on the cloud
provider(s) you want to benchmark:

*   [Google Cloud Platform](https://cloud.google.com)
*   [AWS](http://aws.amazon.com)
*   [Azure](http://azure.microsoft.com)
*   [AliCloud](http://www.aliyun.com)
*   [DigitalOcean](https://www.digitalocean.com)
*   [Rackspace Cloud](https://www.rackspace.com)
*   [ProfitBricks](https://www.profitbricks.com/)

## Cloud account setup

This section describes the setup steps needed for each cloud system. Note that
you only need to perform setup steps on the clouds you wish to test. If you only
want to test Google Cloud, you only need to install and configure `gcloud`.

*   [Google Cloud](#install-gcloud-and-setup-authentication)
*   [OpenStack](#install-openstack-cli-client-and-setup-authentication)
*   [Kubernetes](#kubernetes-configuration-and-credentials)
*   [Mesos](#mesos-configuration)
*   [Cloudstack](#cloudstack-install-dependencies-and-set-the-api-keys)
*   [AWS](#install-aws-cli-and-setup-authentication)
*   [Azure](#windows-azure-cli-and-credentials)
*   [AliCloud](#install-alicloud-cli-and-setup-authentication)
*   [DigitalOcean](#digitalocean-configuration-and-credentials)
*   [RackSpace](#installing-clis-and-credentials-for-rackspace)
*   [ProfitBricks](#profitbricks-configuration-and-credentials)

After configuring the clouds you intend to use, skip to
[Running a Single Benchmark](#running-a-single-benchmark), unless you are going
to use an object storage benchmark, in which case you need to
[configure a boto file](#create-and-configure-a-boto-file-for-object-storage-benchmarks).

### Install `gcloud` and setup authentication

Follow the instructions at: https://developers.google.com/cloud/sdk/. When
prompted, pick the local folder, then Python project, then the defaults for all
the rest.

Restart your shell window (or logout/ssh again if running on a VM)

Next, create a project by visiting
[Google Cloud Console](https://console.developers.google.com). After that, run:

```bash
$ gcloud init
```

which helps you authenticate, set your project, and set some defaults.

Alternatively, if that is already set up, and you just need to authenticate, you
can use:

```bash
$ gcloud auth login
```

For help, see [`gcloud` docs](https://cloud.google.com/sdk/gcloud/).

### Install OpenStack CLI client and setup authentication

Make sure you have installed pip (see the section above).

Install OpenStack CLI utilities via the following command:

```bash
$ pip install -r perfkitbenchmarker/providers/openstack/requirements.txt
```

To setup credentials and endpoint information simply set the environment
variables using an OpenStack RC file. For help, see
[`OpenStack` docs](http://docs.openstack.org/cli-reference/common/cli_set_environment_variables_using_openstack_rc.html)

### Kubernetes configuration and credentials

Perfkit uses the `kubectl` binary in order to communicate with a Kubernetes
cluster - you need to pass the path to the `kubectl` binary using the
`--kubectl` flag. It's recommended to use
[version 1.0.1](https://storage.googleapis.com/kubernetes-release/release/v1.0.1/bin/linux/amd64/kubectl).
Authentication to a Kubernetes cluster is done via a
[`kubeconfig` file](https://github.com/kubernetes/kubernetes/blob/release-1.0/docs/user-guide/kubeconfig-file.md).
Its path is passed using the `--kubeconfig` flag.

**Image prerequisites** Please refer to the
[Image prerequisites for Docker based clouds](#image-prerequisites-for-docker-based-clouds).

**Kubernetes cluster configuration** If your Kubernetes cluster is running on
CoreOS:

1.  Fix `$PATH` environment variable so that the appropriate binaries can be
    found:

    ```bash
    $ sudo mkdir /etc/systemd/system/kubelet.service.d
    $ sudo vim /etc/systemd/system/kubelet.service.d/10-env.conf
    ```

    Add the following line to the `[Service]` section:

    ```
    Environment=PATH=/opt/bin:/usr/bin:/usr/sbin:$PATH
    ```

2.  Reboot the node:

    ```bash
    $ sudo reboot
    ```

Note that some benchmarks must be run within a privileged container. By default
Kubernetes doesn't allow containers to be scheduled in privileged mode - you
have to add the `--allow-privileged=true` flag to `kube-apiserver` and each
`kubelet` startup command.

**Ceph integration** When you run benchmarks with the standard scratch disk type
(`--scratch_disk_type=standard` - which is a default option), Ceph storage will
be used. There are some configuration steps you need to follow before you will
be able to spawn Kubernetes PODs with Ceph volume. On each Kubernetes node, and
on the machine which is running the Perfkit benchmarks, do the following:

1.  Copy `/etc/ceph` directory from Ceph-host.

2.  Install `ceph-common` package so that `rbd` command is available:

    *   If your Kubernetes cluster is running on CoreOS, then you need to create
        a bash script called `rbd` which will run the `rbd` command inside a
        Docker container:

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

        Install
        [`rbdmap`](https://github.com/ceph/ceph-docker/tree/master/examples/coreos/rbdmap):

        ```bash
        $ git clone https://github.com/ceph/ceph-docker.git
        $ cd ceph-docker/examples/coreos/rbdmap/
        $ sudo mkdir /opt/sbin
        $ sudo cp rbdmap /opt/sbin
        $ sudo cp ceph-rbdnamer /opt/bin
        $ sudo cp 50-rbd.rules /etc/udev/rules.d
        $ sudo reboot
        ```

You have
[two Ceph authentication options](http://kubernetes.io/v1.0/examples/rbd/README.html)
available:

1.  Keyring - pass the path to the keyring file using `--ceph_keyring` flag
2.  Secret. In this case you have to create a secret first:

    Retrieve base64-encoded Ceph admin key:

    ```bash
    $ ceph auth get-key client.admin | base64
    QVFEYnpPWlZWWnJLQVJBQXdtNDZrUDlJUFo3OXdSenBVTUdYNHc9PQ==
    ```

    Create a file called `create_ceph_admin.yml` and replace the `key` value
    with the output from the previous command:

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

    You will have to pass the Secret name (using `--ceph_secret` flag) when
    running the benchmakrs. In this case it should be:
    `--ceph_secret=my-ceph-secret`.

### Mesos configuration

Mesos provider communicates with Marathon framework in order to manage Docker
instances. Thus it is required to setup Marathon framework along with the Mesos
cluster. In order to connect to Mesos you need to provide IP address and port to
Marathon framework using `--marathon_address` flag.

Provider has been tested with Mesos v0.24.1 and Marathon v0.11.1.

**Overlay network** Mesos on its own doesn't provide any solution for overlay
networking. You need to configure your cluster so that the instances will live
in the same network. For this purpose you may use Flannel, Calico, Weave, etc.

**Mesos cluster configuration** Make sure your Mesos-slave nodes are reachable
(by hostname) from the machine which is used to run the benchmarks. In case they
are not, edit the `/etc/hosts` file appropriately.

**Image prerequisites** Please refer to the
[Image prerequisites for Docker based clouds](#image-prerequisites-for-docker-based-clouds).

### Cloudstack: Install dependencies and set the API keys

```bash
$ pip install -r perfkitbenchmarker/providers/cloudstack/requirements.txt
```

Get the API key and SECRET from Cloudstack. Set the following environement
variables.

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

Follow instructions at http://aws.amazon.com/cli/ or run the following command
(omit the 'sudo' on Windows)

```bash
$ pip install -r perfkitbenchmarker/providers/aws/requirements.txt
```

Navigate to the AWS console to create access credentials:
https://console.aws.amazon.com/ec2/

*   On the console click on your name (top left)
*   Click on "Security Credentials"
*   Click on "Access Keys", the create New Access Key. Download the file, it
    contains the Access key and Secret keys to access services. Note the values
    and delete the file.

Configure the CLI using the keys from the previous step:

```bash
$ aws configure
```

### Windows Azure CLI and credentials

This version of Perfkit Benchmarker is known to be compatible with Azure CLI
version 2.0.75, and will likely work with any version newer than that.

Follow the instructions at
https://docs.microsoft.com/en-us/cli/azure/install-azure-cli or on Linux, run
the following commands:

```bash
$ curl -L https://aka.ms/InstallAzureCli | bash
$ az login
```

Test that `azure` is installed correctly:

```bash
$ az vm list
```

Finally, make sure that your account is authorized to allocate VMs and networks
from Azure:

```bash
$ az provider register -n Microsoft.Compute
$ az provider register -n Microsoft.Network
```

### Install AliCloud CLI and setup authentication

1.  Download Linux installer from
    [Aliyun Github](https://github.com/aliyun/aliyun-cli). Follow instructions
    from Readme to install for your OS.

2.  Verify that aliyun CLI is working as expected:

    ```bash
    $ aliyun ecs help
    ```

3.  Navigate to the [AliCloud console](https://home.console.alicloud.com/#/) to
    create access credentials:

    *   Login first
    *   Click on "AccessKeys" (top right)
    *   Click on "Create Access Key", copy and store the "Access Key ID" and
        "Access Key Secret" to a safe place.
    *   Configure the CLI using the Access Key ID and Access Key Secret from the
        previous step

    ```bash
    $ aliyun configure
    ```

### DigitalOcean configuration and credentials

1.  Install `doctl`, the DigitalOcean CLI, following the instructions at
    `https://github.com/digitalocean/doctl`.

2.  Authenticate with `doctl`. The easiest way is running `doctl auth login` and
    following the instructions, but any of the options at the `doctl` site will
    work.

### Installing CLIs and credentials for Rackspace

In order to interact with the Rackspace Public Cloud, PerfKitBenchmarker makes
use of RackCLI. You can find the instructions to install and configure RackCLI
here: https://developer.rackspace.com/docs/rack-cli/

To run PerfKit Benchmarker against Rackspace is very easy. Simply make sure Rack
CLI is installed and available in your PATH, optionally use the flag
`--rack_path` to indicate the path to the binary.

For a Rackspace UK Public Cloud account, unless it's your default RackCLI
profile then it's recommended that you create a profile for your UK account.
Once configured, use flag `--profile` to specify which RackCLI profile to use.
You can find more details here:
https://developer.rackspace.com/docs/rack-cli/configuration/#config-file

**Note:** Not all flavors are supported on every region. Always check first if
the flavor is supported in the region.

### ProfitBricks configuration and credentials

Get started by running:

```bash
$ pip install -r
perfkitbenchmarker/providers/profitbricks/requirements.txt
```

PerfKit Benchmarker uses the
<a href='http://docs.python-requests.org/en/master/'>Requests</a> module to
interact with ProfitBricks' REST API. HTTP Basic authentication is used to
authorize access to the API. Please set this up as follows:

Create a configuration file containing the email address and password associated
with your ProfitBricks account, separated by a colon. Example:

```bash
$ less ~/.config/profitbricks-auth.cfg
email:password
```

The PerfKit Benchmarker will automatically base64 encode your credentials before
making any calls to the REST API.

PerfKit Benchmarker uses the file location `~/.config/profitbricks-auth.cfg` by
default. You can use the `--profitbricks_config` flag to override the path.

## Image prerequisites for Docker based clouds

Docker instances by default don't allow to SSH into them. Thus it is important
to configure your Docker image so that it has SSH server installed. You can use
your own image or build a new one based on a Dockerfile placed in
`tools/docker_images` directory - in this case please refer to
[Docker images document](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/tree/master/tools/docker_images).

## Create and configure a `.boto` file for object storage benchmarks

In order to run object storage benchmark tests, you need to have a properly
configured `~/.boto` file. The directions require that you have installed
`google-cloud-sdk`. The directions for doing that are in the
[gcloud installation section](#install-gcloud-and-setup-authentication).

Here is how:

*   Create the `~/.boto` file (If you already have ~/.boto, you can skip this
    step. Consider making a backup copy of your existing .boto file.)

To create a new `~/.boto` file, issue the following command and follow the
instructions given by this command:

```bash
$ gsutil config
```

As a result, a `.boto` file is created under your home directory.

Open the `.boto` file and edit the following fields:

1.  In the [Credentials] section:

    `gs_oauth2_refresh_token`: set it to be the same as the `refresh_token`
    field in your gcloud credential file (~/.config/gcloud/credentials.db),
    which was setup as part of the `gcloud auth login` step. To see the refresh
    token, run

    ```bash
    $ strings ~/.config/gcloud/credentials.db.
    ```

    `aws_access_key_id`, `aws_secret_access_key`: set these to be the AWS access
    keys you intend to use for these tests, or you can use the same keys as
    those in your existing AWS credentials file (`~/.aws/credentials`).

2.  In the `[GSUtil]` section:

    `default_project_id`: if it is not already set, set it to be the google
    cloud storage project ID you intend to use for this test. (If you used
    `gsutil config` to generate the `.boto` file, you should have been prompted
    to supply this information at this step).

3.  In the `[OAuth2]` section:

    `client_id`, `client_secret`: set these to be the same as those in your
    gcloud credentials file (`~/.config/gcloud/credentials.db`), which was setup
    as part of the `gcloud auth login` step.
