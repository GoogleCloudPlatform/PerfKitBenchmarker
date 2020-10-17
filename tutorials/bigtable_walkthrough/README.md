# Benchmarking Bigtable with PerfKit Benchmarker

This is a hands-on lab/tutorial for generating benchmarks for Google Cloud
Bigtable.

## Overview

### Introducing PerfKit Benchmarker

[PerfKit Benchmarker](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker)
is an open source framework with commonly accepted benchmarking tools that you
can use to measure and compare cloud providers. PKB automates setup and teardown
of resources, including Virtual Machines (VMs). Additionally, PKB installs and
runs the benchmark software tests and provides patterns for saving the test
output for future analysis and debugging.

Check out the
[PerfKit Benchmarker README](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/README.md)
for a detailed introduction.

### Bigtable Tutorial Overview

PKB supports
[__custom configuration__](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/PerfkitBenchmarker-Configurations)
files in which you can set the machine type, number of machines, parameters for
loading/running, and many other options.

This tutorial uses the provided PKB configuration files in the [data](./data)
folder to run latency and throughput benchmarks for Bigtable using the
[YCSB Workload files](https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload)
in [perfkitbenchmarker/data/ycsb](../../perfkitbenchmarker/data/ycsb/).

__Note__: This tutorial has been created specifically to run the following YCSB
Workloads:

*   [workloadx](../../perfkitbenchmarker/data/ycsb/workloadx): Write-Only
*   [workloada](../../perfkitbenchmarker/data/ycsb/workloada): 50/50 Read/Write
*   [workloadb](../../perfkitbenchmarker/data/ycsb/workloadb): 95/5 Mostly Read
*   [workloadc](../../perfkitbenchmarker/data/ycsb/workloadc): Read-only

When you finish this tutorial, you will have the tools needed to create your own
combination of PKB Configurations and YCSB Workloads to run custom benchmarks.

## What you'll do

This lab demonstrates an end-to-end workflow for running benchmark tests and
uploading the result data to Google Cloud.

In this lab, you will:

*   Install PerfKit Benchmarker in your Cloud Shell
*   Create a BigQuery dataset for benchmark result data storage
*   Start a benchmark test for Bigtable latency
*   Work with the test result data in BigQuery
*   Learn how to run your own benchmarks for throughput and latency

## Prerequisites

*   Basic familiarity with Linux command line
*   Basic familiarity with Google Cloud

## Set up

### What you'll need

To complete this lab, you'll need:

*   Access to a standard internet browser (Chrome browser recommended), where
    you can access the Cloud Console and the Cloud Shell
*   A Google Cloud project

### Sign in to Cloud Console

In your browser, open the [Cloud Console](https://console.cloud.google.com).

Select your project using the project selector dropdown at the top of page.

### Activate the Cloud Bigtable API

PerfKit Benchmarker uses the Cloud Bigtable and Cloud Bigtable Admin APIs to
provision Bigtable instances for benchmarking and to delete the instances when
benchmarks are complete.
[Click here](https://console.cloud.google.com/flows/enableapi?apiid=bigtable,bigtableadmin.googleapis.com)
to enable the Bigtable APIs for your project.

> __Note__: You can ignore the prompts for generating credentials, given this
> tutorials uses Cloud Shell to run benchmarks.

### Activate the Cloud Shell

From the Cloud Console click the __Activate Cloud Shell__ icon on the top right
toolbar. You may need to click __Continue__ the first time.

It should only take a few moments to provision and connect to your Cloud Shell
environment.

This Cloud Shell virtual machine is loaded with all the development tools you'll
need. It offers a persistent 5GB home directory, and runs on Google Cloud,
greatly enhancing network performance and authentication. All of your work in
this lab can be done within a browser on your Google Chromebook.

Once connected to the Cloud Shell, you can verify your setup.

1.  Check that you're already authenticated.

    ```
    gcloud auth list
    ```

    **Expected output**

    ```
     Credentialed accounts:
    ACTIVE  ACCOUNT
    *       <myaccount>@<mydomain>.com
    ```

    **Note:** `gcloud` is the powerful and unified command-line tool for Google
    Cloud. Full documentation is available from
    https://cloud.google.com/sdk/gcloud. It comes pre-installed on Cloud Shell.
    Notice `gcloud` supports tab-completion.

1.  Verify your project is known.

    ```
    gcloud config list project
    ```

    **Expected output**

    ```
    [core]
    project = <PROJECT_ID>
    ```

    If it is not, you can set it with this command:

    ```
    gcloud config set project <PROJECT_ID>
    ```

    **Expected output**

    ```
    Updated property [core/project].
    ```

## Task 1. Install PerfKit Benchmarker

In this lab, you use Cloud Shell and the
[PerfKit Benchmarker repo](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker).

1.  Set up a virtualenv isolated Python environment within Cloud Shell.

    ```sh
    sudo apt-get install python3-venv -y
    python3 -m venv $HOME/my_virtualenv
    source $HOME/my_virtualenv/bin/activate
    ```

1.  Ensure Google Cloud SDK tools like bq find the proper Python.

    ```sh
    export CLOUDSDK_PYTHON=$HOME/my_virtualenv/bin/python
    ```

1.  Clone the PerfKitBenchmarker GitHub Repository.

    > __Note__: For compatibility and consistency, these instructions are pinned
    > to version
    > [1.15.2](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/releases/tag/v1.15.2)
    > of PerfKit Benchmarker.

    ```sh
    cd $HOME && git clone https://github.com/GoogleCloudPlatform/PerfKitBenchmarker.git
    git checkout v1.15.2
    ```

    ```sh
    cd PerfKitBenchmarker/
    ```

1.  Install PKB dependencies.

    ```sh
    pip install --upgrade pip
    pip install -r requirements.txt
    ```

> __Note__: These setup instructions are specific for running Bigtable
> benchmarks. Comprehensive instructions for running other benchmarks can be
> located by reviewing the
> [README in the PKB repo](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker),
> or just look through the code.

## Task 2. Create a BigQuery dataset for benchmark result data storage

By default, PKB logs test output to the terminal and to result files under
`/tmp/perfkitbenchmarker/runs/`.

A recommended practice is to push your result data to
[BigQuery](https://cloud.google.com/bigquery/), a serverless, highly-scalable,
cost-effective data warehouse. You can then use BigQuery to review your test
results over time and create data visualizations.

Using the BigQuery command-line tool `bq`, initialize an empty __dataset__.

```sh
bq mk pkb_results
```

__Output (do not copy)__

```output
Dataset '[PROJECT-ID]:pkb_results' successfully created.
```

You can safely ignore any warnings about the `imp` module.

You can also create datasets using the
[BigQuery UI](https://cloud.google.com/bigquery/) in the Cloud Console. The
dataset can be named anything, but you need to use the dataset name in options
on the command-line when you run tests.

## Task 3: Create a Bigtable Instance and load the test data

Now that you have installed PKB in your Cloud Shell and created a BigQuery
dataset, it's time to prepare your Bigtable instance.

First, you'll need to create a Bigtable instance and load it with test data.

1.  Create a Bigtable Instance for testing:

    ```sh
    gcloud bigtable instances create pkb-benchmarks \
      --cluster=pkb-benchmarks-1 \
      --cluster-zone=us-central1-b \
      --cluster-num-nodes=3 \
      --display-name=pkb-benchmarks
    ```

1.  Grab and review the PKB Configuration file for loading test data.

    ```sh
    cat ./tutorials/bigtable_walkthrough/data/load.yaml
    ```

    __Output (do not copy)__

    ```output
    # PKB Configuration to load data into a Bigtable instance for running benchmarks
    # Use the latency.yaml and throughput.yaml files in this directory
    # to run benchmarks against this test data.
    benchmarks:
      - cloud_bigtable_ycsb:
          flags:
            ycsb_skip_run_stage: True

            # Point to the existing 3-node Bigtable cluster
            google_bigtable_instance_name: pkb-benchmarks
            google_bigtable_static_table_name: test_data

            # Provisioning: 3 GCE VM clients to load data
            zones: us-central1-b
            num_vms: 3
            machine_type: n1-highcpu-16
            gce_network_name: default

            # Data size: 1 column per row, row size 1KB, 100GB total size
            ycsb_field_count: 1
            ycsb_field_length: 1000
            ycsb_record_count: 100000000

            ycsb_preload_threads: "320"

            # Lock environment to a particular version
            hbase_bin_url: https://storage.googleapis.com/cbt_ycsb_client_jar/hbase-1.4.7-bin.tar.gz
            ycsb_version: 0.17.0
    ```

1.  Run the load step using `pkb.py`.

    ```sh
    ./pkb.py --benchmark_config_file=./tutorials/bigtable_walkthrough/data/load.yaml
    ```

## Task 4. Run a benchmark test

Now that you've created a Bigtable instance and loaded it with test data, you
can run benchmarks against this instance.

__Note__: This tutorial has been created specifically to run the following YCSB
Workloads:

*   [workloadx](../../perfkitbenchmarker/data/ycsb/workloadx): Write-Only
*   [workloada](../../perfkitbenchmarker/data/ycsb/workloada): 50/50 Read/Write
*   [workloadb](../../perfkitbenchmarker/data/ycsb/workloadb): 95/5 Mostly Read
*   [workloadc](../../perfkitbenchmarker/data/ycsb/workloadc): Read-only

__Note__: This tutorial splits the benchmarks into two workload categories:

*   [throughput.yaml](./data/throughput.yaml): A maximum-throughput workload
*   [latency.yaml](./data/latency.yaml): A latency-sensitive workload

1.  Grab and review the PKB Configuration file for latency testing.

    ```sh
    cat ./tutorials/bigtable_walkthrough/data/latency.yaml
    ```

    __Output (do not copy)__

    ```output
    # Bigtable latency benchmark PKB configuration
    # Use the --ycsb_workload_files arg to provide a workload when running
    benchmarks:
      - cloud_bigtable_ycsb:
          flags:
            # Skip the load stage, which is done using load.yaml
            ycsb_skip_load_stage: True

            # Provisioning: 3 GCE VM clients
            zones: us-central1-b
            num_vms: 3
            machine_type: n1-highcpu-16
            gce_network_name: default

            # Point to the existing 3-node Bigtable cluster
            google_bigtable_instance_name: pkb-benchmarks
            google_bigtable_static_table_name: test_data

            # Run Phase: Run for 30min by setting very high operation_count
            ycsb_timelimit: 1800 # 30 minutes
            ycsb_operation_count: 100000000 # Very high
            # One thread per core to minimize latency
            ycsb_threads_per_client: "16"
            ycsb_run_parameters: requestdistribution=zipfian,dataintegrity=true

            # Data size: 1 column per row, row size 1KB, 100GB total size
            ycsb_field_count: 1
            ycsb_field_length: 1000
            ycsb_record_count: 100000000

            # Workload file commented out so users can provide their own
            # ycsb_workload_files=workloada

            # Lock environment to a particular version
            hbase_bin_url: https://storage.googleapis.com/cbt_ycsb_client_jar/hbase-1.4.7-bin.tar.gz
            ycsb_version: 0.17.0

            ycsb_measurement_type: hdrhistogram
    ```

1.  Grab and review [workloada](./perfkitbenchmarker/data/ycsb/workloada), an
    YCSB workload with 50/50 read/write traffic . For more info on YCSB
    workloads and parameters, see
    [YCSB's "Running a Workload" docs](https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload).

    ```sh
    cat ./perfkitbenchmarker/data/ycsb/workloada
    ```

1.  Run the latency test using `workloada` and store the results in your
    BigQuery dataset.

    > __Note__: Each benchmark can take up to 40 minutes to complete when
    > accounting for time to provision, run, and de-provision the required
    > resources.

    ```sh
    ./pkb.py \
      --benchmark_config_file=./tutorials/bigtable_walkthrough/data/latency.yaml \
      --ycsb_workload_files=workloada \
      --bigquery_table=pkb_results.bigtable_benchmarks
    ```

1.  Monitor the benchmark in action.

    [Monitor](https://cloud.google.com/bigtable/docs/monitoring-instance#console-overview)
    your Bigtable instance in Cloud Console as the benchmark runs.

    Verify you see no ERROR messages in the PKB command's output.

    __Example Output (do not copy)__

    ```output
    INFO     Verbose logging to: /tmp/perfkitbenchmarker/runs/dfff5602/pkb.log
    <...>
    INFO     Running: ssh ...
    <...>
    INFO     Cleaning up benchmark cloud_bigtable_ycsb...
    <...>
    ----------------------------------------------------------------------
    Name                 UID                   Status     Failed Substatus
    ----------------------------------------------------------------------
    cloud_bigtable_ycsb  cloud_bigtable_ycsb   SUCCEEDED
    ----------------------------------------------------------------------
    ```

    If the benchmark fails, verify the provisioned GCE VMs and Bigtable instance
    have been removed using the
    [GCE UI](https://console.cloud.google.com/compute/instances) and
    [Bigtable UI](https://console.cloud.google.com/bigtable/instances). If the
    provisioned resources have not been removed, remove them manually using the
    UI.

1.  Once the benchmark is complete, query `pkb_results.bigtable_benchmarks` to
    view the test results in BigQuery.

    In Cloud Shell, run a `bq` command.

    ```sh
    bq query 'SELECT * FROM pkb_results.bigtable_benchmarks'
    ```

    You can also see your data using the __Query editor__ in the
    [BigQuery UI](https://console.cloud.google.com/bigquery).

## Task 5. Run more benchmarks

You can use different combinations of `--benchmark_config_file` and
`--ycsb_workload_files` to run latency or throughput benchmarks on any of the
common workloads provided in [data/ycsb](../../perfkitbenchmarker/data/ycsb/)
folder. You can also modify those workloads to run your own custom benchmarks.

> The following workloads represent common Bigtable use-cases:
>
> *   [workloadx](../../perfkitbenchmarker/data/ycsb/workloadx): Write-Only
> *   [workloada](../../perfkitbenchmarker/data/ycsb/workloada): 50/50
>     Read/Write
> *   [workloadb](../../perfkitbenchmarker/data/ycsb/workloadb): 95/5 Mostly
>     Read
> *   [workloadc](../../perfkitbenchmarker/data/ycsb/workloadc): Read-only

For example, you can run a write-only throughput benchmark by setting the
`--benchmark_config_file` argument to point at the
[throughput.yaml](./tutorials/bigtable_walkthrough/data/throughput.yaml) PKB
Configuration file and the `--ycsb_workload_files` argument to point at
[workloadx](../../perfkitbenchmarker/data/ycsb/workloadx).

Example: Write-only throughput benchmark command:

```sh
./pkb.py \
--benchmark_config_file=./tutorials/bigtable_walkthrough/data/throughput.yaml \
--ycsb_workload_files=workloadx \
--bigquery_table=pkb_results.bigtable_benchmarks
```

## Cleanup

When you are finished running benchmarks, you should delete the `pkb-benchmarks`
Bigtable instance. You can delete this instance with the following command in
Cloud Shell:

```sh
gcloud bigtable instances delete pkb-benchmarks
```

You may also wish to remove the `pkb_results` dataset in BigQuery. You can
remove this dataset with the following command in Cloud Shell:

```sh
bq rm pkb_results
```

## Congratulations!

You have completed the Bigtable PerfKit Benchmarker tutorial!

### Learn More

*   Learn more about
    [optimizing performance](https://cloud.google.com/bigtable/docs/performance)
    of your workloads on Google Cloud Bigtable.
*   Follow the
    [PKB repo](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker).

### Credits

> __Note:__ This lab borrows heavily from
> [instructions for Inter-Region Reports](../inter_region_reports), originally
> prepared by the networking research team at the
> [AT&T Center for Virtualization](https://www.smu.edu/Provost/virtualization)
> at __Southern Methodist University__.
