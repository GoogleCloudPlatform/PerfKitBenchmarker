# Continuous Cross-Cloud and On-Premises-to-Cloud Network Performance Monitoring with PerfKitBenchmarker

This is a hands-on tutorial/lab for running network performance benchmarks
between VMs located on different clouds. More specifically, we will run iPerf
throughput benchmarks between Google Cloud and AWS, and explain how this example
may be extended for other cloud pairs. Additionally, this tutorial details how
users may run network performance benchmarks between On-Premises machines and
the cloud using PerfKitBenchmarker. We will also review how to configure these
benchmarks to run on a regular basis, allowing us to continuously monitor a key
metric we are interested in.

## Prerequisites

This tutorial assumes the reader has a basic familiarity with the Linux command
line, Google Cloud Compute Engine, and AWS EC2.

We also assume the reader has installed PerfKitBenchmarker. If not, we ask the
reader first read through the Beginner Walkthrough.

## What You’ll Do

This lab presents a process for easily performing cross-cloud network
performance benchmarking by using PerfKitBenchmarker.

In this example scenario, we are interested in evaluating TCP throughput
performance between Google Cloud and AWS in the Virginia(USA) geographical area.
Google Cloud and AWS both host cloud regions here, with us-east4 and us-east-1,
respectively. We also consider an example scenario of evaluating TCP throughput
performance between a local on-premises machine and a Google Cloud VM.

Finally, we review how we can use the cron utility to regularly perform
benchmarks, allowing us to actively monitor a performance metric of interest.

In this lab, we will: - Create a set of benchmark configuration files - Run
benchmarks for AWS <—> Google Cloud TCP throughput - Run benchmarks for On-Prem
<—> Google Cloud TCP throughput - Set up a cron job to schedule
PerfKitBenchmarker runs

## What You’ll Need

For this tutorial, you’ll need: - A Google Cloud project - An AWS account -
Installation of PerfKitBenchmarker

## Cross-Cloud Benchmarking Example

### Verify Google Cloud/AWS CLI and PerfKitBenchmarker Installation

Before proceeding, verify that the Google Cloud and AWS CLIs are authenticated
on the machine which will be running PerfKitBenchmarker. Also, verify that
PerfKitBenchmarker is installed and the appropriate Python environment is
active, if appropriate.

### Create Benchmark Configurations

First, we need to create benchmark configurations for PerfKitBenchmarker to run.
In general, we recommend specifying these with YAML files, as they allow for
easy sharing and reproduction of testing configurations in the future, if
necessary.

As mentioned, we will be using the iPerf benchmark. The default YAML
configuration for all benchmarks in PerfKitBenchmarker are available through
their respective Python benchmark file, defined with the BENCHMARK_CONFIG
variable. For Linux benchmarks, these files are located in the
perfkitbenchmarker/linux_benchmarks directory. Below is the default iPerf
configuration.

```
iperf:
  description: Run iperf
  vm_groups:
    vm_1:
      vm_spec: *default_dual_core
    vm_2:
      vm_spec: *default_dual_core
```

We will be extending this default configuration with several other options.
First, create a new directory for storing our configurations. Then, create a new
file in this directory titled example.yaml and copy the following text to the
file.

```
iperf:
  vm_groups:
    vm_1:
      cloud: AWS
      vm_spec:
        AWS:
          machine_type: m6i.large
          zone: us-east-1a
    vm_2:
      cloud: GCP
      vm_spec:
        GCP:
          machine_type: n2-standard-2
          zone: us-east4-a
  flags:
    iperf_runtime_in_seconds: 30
    iperf_sending_thread_count: 1
    iperf_interval: 0.5
    gcp_min_cpu_platform: icelake
    ip_addresses: EXTERNAL
    os_type: ubuntu2004
```

There are a few differences between the default configuration and the new one
above. First, we explicitly defined the vm_1 and vm_2 fields. As is hopefully
clear, vm_1 is specified as an AWS m6i.large VM located in the us-east-1a zone
and vm_2 is a Google Cloud n2-standard-2 VM located in the us-east4-a zone.

Different VM specifications may be tested by modifying the respective cloud,
machine_type, and zone fields. For example, we could instead test with an Azure
Standard_D2s_v5 VM by replacing the vm_1 field with the following.

```
vm_1:
  cloud: Azure
  vm_spec:
    Azure:
      machine_type: Standard_D2s_v5
      zone: eastus-1
```

We also specified a number of additional flags. To minimize the data transfer
costs associated with this example, we specify a shorter test length duration of
30 seconds. We also choose to spin up the Google Cloud n2-standard-2 VMs using
the Ice Lake CPU platform, to better match the AWS m6i.large VMs. As we are
performing these tests across clouds, we must also specify the use of external
IPs. Finally, we specify that Ubuntu 20.04 should be used for both the VMs in
this test.

Hopefully, these flag specifications are intuitive to understand. Other flag
options for a given benchmark can be viewed by referring to the respective
Python benchmark file.

### Run the Example Benchmark

Now, we can run PerfKitBenchmarker using the benchmark configuration just
created. To do this, navigate to the base of the PerfKitBenchmarker directory
and execute the following command.

```
python pkb.py --benchmarks=iperf —benchmark_config_file=CONFIG_LOCATION/example.yaml —accept_licenses
```

Be sure to replace CONFIG_LOCATION with a path to the directory created earlier.

If all is well, the benchmarks should execute successfully!

### Example Output

On a successful run, we should see output similar to the following, once
PerfKitBenchmarker has finished cleaning up. The actual output will be longer,
as more details of the benchmark run will be displayed.

```
-------------------------PerfKitBenchmarker Results Summary-------------------------
IPERF:
  Throughput                         4739.000000 Mbits/sec
. . .
ip_type="external" netpwr="185523.97" receiving_machine_type="n2-standard-2" receiving_zone="us-east4-a" retry_packet_count="795" rtt="3193.0" rtt_unit="us" run_number="0" runtime_in_seconds="30" sending_machine_type="m6i.large" sending_thread_count="1" sending_zone="us-east-1a" tcp_window_size="1.13" transfer_mbytes="16948" vm_1_boot_disk_size="None" vm_1_spot_block_duration_minutes="None" vm_1_spot_price="None" vm_2_gce_nic_type="['GVNIC']" write_packet_count=“135585")
. . .
```

### Multiple Cross-Cloud Benchmarks

If we are interested in performing benchmarks across a large number of
cross-cloud links, the approach here can be extended by simply creating new
configuration files for each cross-cloud zone-pair. When these configurations
are located in the same directory, we can run all the benchmarks with a command
like the following:

```
for i in CONFIG_LOCATION/*.yaml; do python pkb.py --benchmarks=iperf --benchmark_config_file="$i"; done
```

Following the scenario considered here, if we were interested in assessing
cross-cloud throughput and performance between all Google Cloud and AWS zones in
the Virginia area, we could create a new configuration file for each zone-pair,
replacing the zone fields in the VM specifications as appropriate. If all the
configuration files are saved in the same directory, we could execute the
command above to run all the benchmarks in one go.

Note: This approach runs through each benchmark sequentially, which can take a
large amount of time if many configurations are to be tested. If one wishes to
run a large set of benchmarks efficiently, a tool like pkb_scheduler can also be
used.

## On-Prem to Cloud Benchmarking Example

Now, we consider a scenario where we want to assess TCP throughput performance
between machines located on-premises and Google Cloud.

### Verify Google Cloud CLI and PerfKitBenchmarker Installation

As before, first verify that the Google Cloud CLIs are authenticated on the
machine which will be running PerfKitBenchmarker. Also, verify that
PerfKitBenchmarker is installed and the appropriate Python environment is
active, if appropriate.

### Prepare On-Prem Machine

Next, we should ensure that the relevant On-Prem machine is ready for use with
PerfKitBenchmarker. To do this, we should first make sure that the machine to be
tested is ssh-able by the machine which will be running PerfKitBenchmarker. The
user which PerfKitBenchmarker will login as should also have “sudo” access.

### Create Benchmark Configurations

Now, we can create configuration files for PerfKitBenchmarker to run. As with
the previous scenario, we will be running an iPerf benchmark. A basic template
configuration for this use-case is presented below. Create a new file titled
on_prem_example.yaml and copy the following text to the file.

```
static_vms:
- &id001
  ip_address: XX.XXX.XXX.XX
  ssh_private_key: ssh_key
  user_name: perfkit
  zone: On-Prem
iperf:
  flags:
    iperf_runtime_in_seconds: 30
    iperf_sending_thread_count: 1
    iperf_interval: 0.5
    gcp_min_cpu_platform: icelake
    ip_addresses: EXTERNAL
    os_type: ubuntu2004
  vm_groups:
    vm_1:
      static_vms:
      - *id001
    vm_2:
      cloud: GCP
      vm_spec:
        GCP:
          machine_type: n2-standard-2
          zone: us-east4-a
```

Hopefully, the template is intuitive to follow. Various details of the on-prem
machine should be included in the static_vms field. These include the IP address
of the on-prem machine, the location of the private ssh key to use, and the user
which PerfKitBenchmaker will login as on the on-prem machine. Although zone is
technically a free-field, we recommend using it as an identifier for the
physical location of the machine. For our use-case, we simply use “On-Prem”.

### Run the Example Benchmark

As before, we can now run PerfKitBenchmarker using the configuration we just
created. To do this, navigate to the base of the PerfKitBenchmarker directory
and execute the following command.

```
python pkb.py --benchmarks=iperf —benchmark_config_file=CONFIG_LOCATION/on_prem_example.yaml —accept_licenses
```

Be sure to replace CONFIG_LOCATION with a path to the directory created earlier.

If all is well, the benchmarks should execute successfully!

### Example Output

We emulated this scenario by utilizing a VM we already had provisioned in Google
Cloud’s us-east4-c zone. We treat this as an “on-prem” machine by adding its
details to the static_vms field in the configuration file. Thus,
PerfKitBenchmarker only provisioned one new VM in us-east4-a when we ran our
test.

One should see output similar to the following when following this on-prem
example.

```
-------------------------PerfKitBenchmarker Results Summary-------------------------
IPERF:
  Throughput                         6746.000000 Mbits/sec
. . .
ip_type="external" netpwr="1086645.9" receiving_machine_type="n2-standard-2" receiving_zone="us-east4-a" retry_packet_count="11271" rtt="776.0" rtt_unit="us" run_number="0" runtime_in_seconds="30" sending_machine_type="None" sending_thread_count="1" sending_zone="on_prem" tcp_window_size="2.12" transfer_mbytes="24126" vm_1_image="None" vm_2_gce_nic_type="['GVNIC']" write_packet_count=“193006")
```

From the meta-data, we can see that PerfKitBenchmarker did indeed perform an
iPerf test with the “on_prem” machine as the sending_zone and us-east4-a as the
receiving_zone.

## Continuous Performance Monitoring Example

Now, we consider a scenario where we want to continuously monitor a performance
metric of interest. More specifically, we are interested in monitoring the
latency between an on-premises machine and the cloud. We use the Ping benchmark
to assess latency, and want to run these tests between an on-premises machine
and a n2-standard-2 VM in Google Cloud’s us-east4-a zone. An example
configuration to perform this task is shown below.

### Create Benchmark Configurations

```
static_vms:
- &id001
  ip_address: XX.XXX.XXX.XX
  ssh_private_key: ssh_key
  user_name: perfkit
  zone: On-Prem
ping:
  flags:
    gcp_min_cpu_platform: icelake
    ip_addresses: EXTERNAL
    os_type: ubuntu2004
  vm_groups:
    vm_1:
      static_vms:
      - *id001
    vm_2:
      cloud: GCP
      vm_spec:
        GCP:
          machine_type: n2-standard-2
          zone: us-east4-a
```

### Create shell script

For scenarios like this, we recommend creating a simple shell script which
contains the PerfKitBenchmarker commands we wish to run. For one, this allows us
to monitor multiple benchmark configurations with the same cron job. We present
a simple template for this scenario below.

```
#!/bin/bash

#activate python environment
source /home/user/miniconda3/etc/profile.d/conda.sh
conda activate ENV_NAME

#go to PerfKitBenchmarker location
cd /home/user/PerfKitBenchmarker;

#PerfKitBenchmarker commands
python pkb.py --benchmarks=ping —benchmark_config_file=CONFIG_LOCATION/on_prem_ping_example.yaml —accept_licenses
```

Hopefully, the script is intuitive to follow. First, we activate the appropriate
python environment for use with PerfKitBenchmarker. We then navigate to where we
have installed PerfKitBenchmarker on our machine, and execute the appropriate
PerfKitBenchmarker command.

### Set up Cron Job

Now, we should configure cron to execute the script above at regular intervals.
First, we can open and edit the crontab file with the following command.

```
crontab -e
```

Then, we can add a cron line similar to the following. Here, the cron expression
indicates that we should run our script 4 times everyday, at 0:00, 6:00, 12:00,
and 18:00. More specifically, we first navigate to the appropriate directory
before executing the shell script.

```
0 0,6,12,18 * * * cd /home/user; sh script.sh
```

### Performance Monitoring

Now, we will perform ping benchmarks at regular intervals. For effective
monitoring, we recommend augmenting the PerfKitBenchmarker commands in this
tutorial with the bq_project and bigquery_table flags. When these flags are set,
PerfKitBenchmarker will upload all test results to a BigQuery database, which
further allows us to visualize results with LookerStudio dashboards. For details
on how this can be accomplished, we ask readers to refer to the Beginner
Tutorial (tasks 8 - 9).
