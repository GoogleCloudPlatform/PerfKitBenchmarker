# Benchmarking Bigtable with PerfKit Benchmarker

This is a hands-on lab/tutorial about running YCSB benchmarks against Google
Cloud Bigtable.

Table of contents
=================

<!--ts-->
   * [Overview](#overview)
   * [Task 1: Set up a benchmarking project](#task-1-set-up-a-benchmarking-project)
   * [Task 2: Create a BigQuery dataset](#task-2-create-a-bigquery-dataset)
   * [Task 3: Generate benchmark configs](#task-3-generate-benchmark-configs)
   * [Task 4: Generate a runner config](#task-4-generate-a-runner-config)
   * [Task 5: Run the benchmarks](#task-5-run-the-benchmarks)
   * [Task 6: View the results](#task-6-view-the-results)
   * [Task 7: Cleanup](#task-7-cleanup)
   * [Additional tips](#additional-tips)
   * [Reference results](#reference-results)
<!--te-->

## Overview

### PerfKit Benchmarker

[PerfKit Benchmarker (PKB)](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker)
is an open source framework with commonly accepted benchmarking tools that you
can use to measure and compare cloud providers. In this tutorial, we use
[YCSB](https://github.com/brianfrankcooper/YCSB) as the benchmarking tool.

PKB automates setup and teardown of resources, including Virtual Machines (VMs)
that host the clients (servers are generally remote, hosted by the providers).
We refer to the VMs as **"worker VMs"** in this tutorial. PKB also installs and
runs benchmarks on the worker VMs, and provides patterns for saving the results.
Please check out the
[PKB README](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/README.md)
for a detailed introduction.

### Bigtable benchmarking overview

**Prerequisites**:

*   Basic familiarity with Linux command line
*   Basic familiarity with Google Cloud
*   A standard internet browser (Chrome browser recommended)

**Highlights**: This tutorial leverages additional features to facilitate
Bigtable benchmarking:

*   A pre-built docker image that handles package installation and benchmark
    invocation
*   Two runner modes where you can run benchmarks in parallel or in sequence
*   Two working queries that retrieve the results comprehensively
*   Checkpoints that keep track of the progress

**Runner VM**: PKB can bring up worker VMs, but PKB itself needs to run on a
dedicated machine. This tutorial creates a runner VM to invokes PKB via a
wrapper script. The script is included in the docker image, and the docker image
will be installed on the runner VM.

**Configs**: PKB supports
[custom configuration](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/PerfkitBenchmarker-Configurations)
files in which you can set parameters such as number of VMs, number of threads,
YCSB workload settings, etc. In this tutorial, we organize the parameters in a
more structured way tailored to Bigtable. You can easily build your configs from
the samples in the [config folder](./batch_configs). All the sample configs use
*workloada* as described below.

**Workloads**: The commonly used workloads are:

*   [workloada](../../perfkitbenchmarker/data/ycsb/workloada): 50/50 Read/Write
*   [workloadb](../../perfkitbenchmarker/data/ycsb/workloadb): 95/5 Read/Write
*   [workloadc](../../perfkitbenchmarker/data/ycsb/workloadc): Read-only
*   [workloadx](../../perfkitbenchmarker/data/ycsb/workloadx): Write-Only

Note that the workloads above only perform point read/write. We will add support
for batch read/write in the future release.

**Codelab and tips**: You will learn how to run benchmarks effectively by
working through a codelab and additional tips -- the main body of the tutorial.

**Benchmark and test**: A benchmark may be run multiple times, and each
benchmark run is referred to as a test.

**Reference results**: In the end, we provide some results from our experiments,
and you can use them as reference.

### What you'll do

You will go through a lab that demonstrates the end-to-end user journey of
benchmarking. The following sections correspond to the individual tasks.

## Task 1: Set up a benchmarking project

### Sign in to a Google Cloud project

In your browser, open the [Cloud Console](https://console.cloud.google.com), and
create a new project or select an existing project using the project selector
dropdown at the top of the page. The project should already have some APIs
enabled including the BigQuery API. You need to enable additional APIs and check
the quotas.

### Enable the Bigtable API

PKB and YCSB use the Bigtable API to send admin and data requests such as
create/delete Bigtable instance, read/update Bigtable table rows.
[Click here](https://console.cloud.google.com/flows/enableapi?apiid=bigtable,bigtableadmin.googleapis.com)
to enable the Bigtable APIs for your project.

### Enable the Compute Engine API

PKB uses Compute Engine API to provision worker VMs that run YCSB benchmarks. In
this lab, you also need to create a runner VM to run PKB within a docker
container.
[Click here](https://console.cloud.google.com/flows/enableapi?apiid=compute.googleapis.com)
to enable the Compute Engine API for your project.

### Check the Quotas of Bigtable nodes and VM CPUs

This tutorial requires 6 Bigtable nodes and 6 VMs (N1 machine type) in
`us-central1-b`. You should check
[the Quotas page](https://console.cloud.google.com/iam-admin/quotas) to ensure
you have sufficient nodes and CPUs (you can edit the quotas if necessary):

*  Check `SSD nodes per zone` for zone `us-central1-b` (should be >= 6)
*  Check `CPUs` for region `us-central1` (should be >= 82)

> TIP: to have a better understanding of the VM quotas, please refer to
[this page](https://cloud.google.com/compute/quotas#cpu_quota).

## Task 2: Create a BigQuery dataset

Starting from this task, you will execute some linux commands on the runner VM
of the benchmarking project. By doing so, you can save the effort of dealing
with gcloud installation, authentication, and API credentials.

### Create and log in to runner VM

Within the Cloud Console, you can create a GCE VM via the interface:
[click here](https://console.cloud.google.com/compute/instances)
to land on the "VM instances" page, and click the "CREATE INSTANCE" button at
the top to create a VM instance. You can name it as "pkb-runner". To configure
the VM properly, you only need to change the following settings:

1.  For "Machine type", select "e2-standard-2" (2 vCPUs, 8GB memory).
1.  For "Access Scope", select "Allow full access to all Cloud APIs".

To log in, you can click the associated "SSH" button on the "VM instances" page.

### Verify the gcloud config on the VM

The VM should already have gcloud installed, and you can verify the config with
command:

```sh
gcloud config list
```

**Expected output**

```
[core]
account = <the default service account>
project = <the project id>
```

The project id should correspond to the benchmarking project. If not, you can
set it with command:

```sh
gcloud config set project <the project id>
```

**Expected output**

```
Updated property [core/project].
```

Please remember the **project id** as it will be needed by the subsequent tasks.

### Create a BigQuery dataset for benchmark results

By default, PKB logs benchmark results to the terminal and to files under
*/tmp/perfkitbenchmarker/runs/*. You can still view them, but it's not easy to
locate the target metrics or files. Moreover, the result data are not
permanently saved. A recommended practice is to push your results to
[BigQuery](https://cloud.google.com/bigquery/), you can then use BigQuery to
view them any time.

Using the BigQuery command line tool `bq`, initialize an empty **dataset**
`pkb_results`.

```sh
bq mk pkb_results
```

**Expected output**

```output
Dataset '<the project id>:pkb_results' successfully created.
```

> **Note**: The command above doesn't create any table to store the results.
When running the benchmarks, please use `codelab` as the table id, and specify
the full table name as `<the project id>:pkb_results.codelab`. The table will be
created automatically by the benchmarks if it doesn't exist.

## Task 3: Generate benchmark configs

Benchmark configs are YAML files that specifies PKB command line flags for
specific benchmarks (one YAML file per benchmark, and the file name cannot be
*runner.yaml*). This format is defined by PKB (see its `--benchmark_config_file`
flag) as follows:

```
benchmarks:
- cloud_bigtable_ycsb:
    flags:
      <flag_1>: <value_1>
      <flag_2>: <value_2>
      ...
      <flag_n>: <value_n>
```

For this lab, we want to run two benchmarks: one is to have a light workload to
measure the latency of lightly-utilized Servers; the other is to have a heavy
workload to measure the throughput of overloaded servers. They are already
created in [the parallel_tests folder](./batch_configs/parallel_tests):
*latency.yaml* and *throughput.yaml*, and you can directly use them. Note that
the throughput test does not fully exploit the server capacity because of the
target throughput setting. For more realistic test, please refer to
[Reference results](#reference-results).

To download the configs, you should first create a config folder:

```sh
mkdir ~/pkb_configs
```

Then you can execute the following commands:

```sh
curl https://raw.githubusercontent.com/GoogleCloudPlatform/PerfKitBenchmarker/master/tutorials/bigtable_walkthrough/batch_configs/parallel_tests/latency.yaml \
--output ~/pkb_configs/latency.yaml
```

```sh
curl https://raw.githubusercontent.com/GoogleCloudPlatform/PerfKitBenchmarker/master/tutorials/bigtable_walkthrough/batch_configs/parallel_tests/throughput.yaml \
--output ~/pkb_configs/throughput.yaml
```

> **TIP**: You can learn how to build the sample configs by reading the
[config template](./batch_configs/templates/benchmark_config.yaml).

## Task 4: Generate a runner config

Runner config is a new concept introduced by the wrapper script included in the
docker image, and isn't read directly by PKB. It has a reserved name
*runner.yaml*, and is placed **in the same folder** as the benchmark configs.
The format should be one of the following:

```
runner:
  unordered_tests:  # All the benchmarks in the same folder will be run.
    num_iterations: <number of iterations per benchmark>
    concurrency: <max number of benchmarks that can be run in parallel>

  pkb_flags:
    <common_flag_1>: <value_1>
    <common_flag_2>: <value_2>
    ...
    <common_flag_n>: <value_n>
```

```
runner:
  ordered_tests:  # You can specify a subset of benchmarks to run.
  - <stage_0_benchmarks>  # space-separated benchmark configs for the first stage
  - <stage_1_benchmarks>  # spece-separated benchmark configs for the second stage
  ...

  pkb_flags:
    <common_flag_1>: <value_1>
    <common_flag_2>: <value_2>
    ...
    <common_flag_n>: <value_n>
```

The `pkb_flags` block is the place where common flags can be specified. If you
don't have common flags, you can remove the whole block. Otherwise, please keep
in mind that they may be overridden by the PKB flags in the benchmark configs.

The two formats correspond to two runner modes:

1.  `unordered_tests`: all the benchmarks are run in an arbitrary order with the
    specified iterations and concurrency. Note that this runner mode will force
    the tests that access the same Bigtable instance to run one at a time, so
    the tests will not interfere with each other.
1.  `ordered tests`: the benchmarks are run in a specific order. The benchmarks
    in a later stage will start only after the previous stages are finished;
    benchmarks in the same stage run at the same time (even if they use the same
    Bigtable instance).

We have created a runner config in
[the parallel_tests folder](./batch_configs/parallel_tests): *runner.yaml*. You
can download it with command:

```sh
curl https://raw.githubusercontent.com/GoogleCloudPlatform/PerfKitBenchmarker/master/tutorials/bigtable_walkthrough/batch_configs/parallel_tests/runner.yaml \
--output ~/pkb_configs/runner.yaml
```

As the runner config uses a hard-coded project id: `test_project`, you should
replace it with your project id. You can use your preferred editor (such as
`nano` and `vim`) to modify the file, or execute the following command:

```sh
PROJECT_ID=<the project id>
sed -i "s/test_project/${PROJECT_ID}/g" ~/pkb_configs/runner.yaml
```

> **TIP**: You can learn how to build the sample config by reading the
[config template](./batch_configs/templates/runner_config.yaml).

## Task 5: Run the benchmarks

1.  Check your config folder which should have the following files:

    ```sh
    ls ~/pkb_configs
    ```

    **Expected output**

    ```
    latency.yaml  runner.yaml  throughput.yaml
    ```

1.  Install docker on the runner VM via command (one-off operation):

    ```sh
    sudo apt update
    sudo apt install docker.io
    ```

1.  Set up authentication (one-off operation,
    [reference](https://cloud.google.com/container-registry/docs/advanced-authentication#gcloud-helper))

    ```sh
    sudo gcloud auth configure-docker
    ```

1.  Execute the following docker command (note that `sudo` is needed to avoid
    the `permission denied` error):

    ```sh
    sudo docker run --init --rm -it -v ~/pkb_configs:/pkb/configs \
    -v /tmp:/tmp gcr.io/cloud-bigtable-ecosystem/pkb:latest
    ```

    When you execute the command for the first time, the docker image will be
    downloaded (taking about 5 minutes), and executed in a docker container,
    which in effect launches the benchmarking. The end-to-end runtime of the
    benchmarking is about 20 minutes. While the benchmarks are running, their
    PKB logs will be printed out in the terminal. As the logs are interleaved
    when there are multiple running benchmarks, it's recommended to simply treat
    the terminal output as the indicator of progression, and check the final
    message in the terminal when the benchmarking is finished.

    If everything runs well, you will see the following message:

    ```output
    All tests have passed. To rerun the tests, please clean up the status markers before executing the invocation command.
    ```

    Otherwise, the benchmarking will terminate early with message:

    ```output
    Some tests have failures. You can retry by running the same invocation command again.
    ```

1.  (Optional) In the case of failure, you may want to check the individual log
    files corresponding to each benchmark run. If you can easily find the
    messages like the example below in the terminal, then you can directly open
    and view the log file.

    ```output
    ----------------------------------------------------------------------
    Name                 UID                   Status     Failed Substatus
    ----------------------------------------------------------------------
    cloud_bigtable_ycsb  cloud_bigtable_ycsb0  FAILED     UNCATEGORIZED
    ----------------------------------------------------------------------
    Success rate: 0.00% (0/1)
    2021-09-27 21:17:04,780 0cde98c0 MainThread pkb.py:1472 INFO     Complete logs can be found at: /tmp/perfkitbenchmarker/runs/0cde98c0/pkb.log
    ```

    Otherwise, additional effort is needed. As the benchmarking terminates as
    soon as there is a failure, you can list the run IDs and check the most
    recent ones:

    ```sh
    ls -lt /tmp/perfkitbenchmarker/runs
    ```

    For a run ID `0cde98c0`, the relevant log file is
    */tmp/perfkitbenchmarker/runs/0cde98c0/pkb.log*. To determine if it's from
    a failed run, you can scroll to the bottom of the file, and check the status
    (like the one printed out in the terminal).

1.  (Optional) If we announce a new release of the docker image (at the top of
    this tutorial), you should download it to replace the existing one as
    `docker run` doesn't pull down the updated version in this case. No changes
    are needed for the other commands, but you should clean up the previously
    downloaded image first:

    ```sh
    sudo docker rmi gcr.io/cloud-bigtable-ecosystem/pkb:latest
    ```

## Task 6: View the results

You can easily find the performance results in two places:

1.  .json files under the folder */tmp/pkb_workspace/* of the runner VM. For
    example, the results of all the benchmark runs of *latency.yaml* are written
    to files */tmp/pkb_workspace/latency-\<end_timestamp\>.json*, where
    `<end_timestamp>` is epoch time like`1630760021.8509068`. The .json files
    are actually copied from */tmp/perfkitbenchmarker/runs/* for easier access.
1.  BigQuery table in your benchmarking project. As we have set
    `bigquery_table: <the project id>:pkb_results.codelab` in the runner config,
    the results are also exported to BigQuery. You can find the BigQuery table
    on [Cloud Console](https://console.cloud.google.com/bigquery), where the
    left panel should list the benchmarking project id.

To help you view the results in BigQuery, we have built two queries: one is for
the performance (latency and throughput), the other is for the cost along with
resource utilization. You can click "COMPOSE NEW QUERY" on the Cloud Console,
copy the query over, and click the "RUN" button. The results will be shown at
the bottom, where all the records in the BigQuery table `codelab` are displayed
in descending order of the time of test.

You can further explore the results by exporting them to a spreadsheet via `SAVE
RESULTS`.

<details><summary>Query for Performance</summary>

```sql
WITH
  aggregated AS (
  SELECT
    run_uri,
    ARRAY_AGG(metric) as metrics,
    ARRAY_AGG(value) as values_,
    ANY_VALUE(timestamp) as timestamp,
    labels,
  FROM
    `pkb_results.codelab`
  WHERE
    REGEXP_EXTRACT(labels, "\\|stage:(\\w+)\\|") = 'run'
  GROUP BY
    run_uri, labels)
SELECT
  SUBSTR(run_uri, 0, 12) as run_uri,
  TIMESTAMP_SECONDS(CAST(timestamp AS int64)) as end_timestamp,
  REGEXP_EXTRACT(labels, "\\|experiment:([^|]+)\\|") AS experiment,
  REGEXP_EXTRACT(labels, "\\|workload_name:([^|]+)\\|") AS workload_name,
  REGEXP_EXTRACT(labels, "\\|workload_index:([^|]+)\\|") AS workload_index,
  REGEXP_EXTRACT(labels, "\\|operation:([^|]+)\\|") AS operation,
  CAST(CAST(REGEXP_EXTRACT(labels, "-target (\\d+)") as int64) * CAST(REGEXP_EXTRACT(labels, "\\|vm_count:(\\w+)\\|") as int64) as int64) AS target_throughput,
  FORMAT('%.2f', (SELECT value_ from UNNEST(values_) as value_ with offset as value_offset where metrics[offset(value_offset)] = "overall Throughput" limit 1)) as actual_throughput,
  (SELECT value_ from UNNEST(values_) as value_ with offset as value_offset where metrics[offset(value_offset)] = "read p50 latency" OR metrics[offset(value_offset)] = "update p50 latency" limit 1) as p50_latency_ms,
  (SELECT value_ from UNNEST(values_) as value_ with offset as value_offset where metrics[offset(value_offset)] = "read p99 latency" OR metrics[offset(value_offset)] = "update p99 latency" limit 1) as p99_latency_ms,
  (SELECT value_ from UNNEST(values_) as value_ with offset as value_offset where metrics[offset(value_offset)] = "read p99.9 latency" OR metrics[offset(value_offset)] = "update p99.9 latency" limit 1) as p999_latency_ms,
  CAST(REGEXP_EXTRACT(labels, "\\|fieldlength:(\\w+)\\|") AS int64) AS fieldlength,
  CAST(REGEXP_EXTRACT(labels, "\\|fieldcount:(\\w+)\\|") AS int64) AS fieldcount,
  CAST(REGEXP_EXTRACT(labels, "\\|recordcount:(\\w+)\\|") AS int64) AS recordcount,
  REGEXP_EXTRACT(labels, "-p readallfields=(\\w+)") AS readallfields,
  REGEXP_EXTRACT(labels, "-p requestdistribution=(\\w+)") AS requestdistribution,
  CAST(REGEXP_EXTRACT(labels, "-threads (\\d+)") as int64) AS threads_per_vm,
  CAST(REGEXP_EXTRACT(labels, "\\|vm_count:(\\w+)\\|") as int64) AS vm_count,
  REGEXP_EXTRACT(labels, "\\|machine_type:([^|]+)\\|") AS machine_type,
  REGEXP_EXTRACT(labels, "\\|bigtable_node_count:\\[?(\\w+).*\\|") AS bigtable_node_count,
  LOWER(REGEXP_EXTRACT(labels, "\\|bigtable_storage_type:\\[?'?(\\w+).*\\|")) AS bigtable_storage_type,
FROM
  aggregated
ORDER BY
  timestamp DESC
```

</details>

<details><summary>Query for Cost</summary>

```sql
WITH
  qps AS (
  SELECT
    run_uri,
    timestamp,
    REGEXP_EXTRACT(labels, "\\|experiment:([^|]+)\\|") AS experiment,
    REGEXP_EXTRACT(labels, "\\|workload_name:([^|]+)\\|") AS workload_name,
    REGEXP_EXTRACT(labels, "\\|workload_index:([^|]+)\\|") AS workload_index,
    value as actual_throughput,
    CAST((CAST(REGEXP_EXTRACT(labels, "\\|fieldlength:(\\w+)\\|") AS int64) + 39) * CAST(REGEXP_EXTRACT(labels, "\\|fieldcount:(\\w+)\\|") AS int64) * CAST(REGEXP_EXTRACT(labels, "\\|recordcount:(\\w+)\\|") AS int64) / 1073741824 as int64) as baseline_table_gb,
    CAST(REGEXP_EXTRACT(labels, "\\|bigtable_node_count:\\[?(\\w+).*\\|") AS int64) AS bigtable_node_count,
    LOWER(REGEXP_EXTRACT(labels, "\\|bigtable_storage_type:\\[?'?(\\w+).*\\|")) AS bigtable_storage_type,
  FROM
    `pkb_results.codelab`
  WHERE
    REGEXP_EXTRACT(labels, "\\|stage:(\\w+)\\|") = 'run' AND metric = 'overall Throughput'
  ),
  cpu_all AS (
  SELECT
    run_uri,
    REGEXP_EXTRACT(labels, "\\|cpu_average_utilization:(.+?)\\|") AS avg_cpu_all_nodes,
    REGEXP_EXTRACT(labels, "\\|workload_index:([^|]+)\\|") AS workload_index,
  FROM
    `pkb_results.codelab`
  WHERE
    metric = 'cpu_load_array'
  ),
  cpu_hottest AS (
  SELECT
    run_uri,
    REGEXP_EXTRACT(labels, "\\|cpu_average_utilization:(.+?)\\|") AS avg_cpu_hottest_node,
    REGEXP_EXTRACT(labels, "\\|workload_index:([^|]+)\\|") AS workload_index,
  FROM
    `pkb_results.codelab`
  WHERE
    metric = 'cpu_load_hottest_node_array'
  )
SELECT
  SUBSTR(qps.run_uri, 0, 12) as run_uri,
  TIMESTAMP_SECONDS(CAST(timestamp AS int64)) as end_timestamp,
  qps.experiment AS experiment,
  qps.workload_name  AS workload_name,
  qps.workload_index AS workload_index,
  FORMAT('%.2f', qps.actual_throughput) AS actual_throughput,
  cpu_all.avg_cpu_all_nodes AS avg_cpu_all_nodes,
  cpu_hottest.avg_cpu_hottest_node AS avg_cpu_hottest_node,
  qps.bigtable_node_count AS bigtable_node_count,
  qps.baseline_table_gb AS baseline_table_gb,
  FORMAT('%.2f', qps.bigtable_node_count * 0.65 * 24 * 30 + qps.baseline_table_gb * (CASE WHEN qps.bigtable_storage_type='ssd' THEN 0.17 ELSE 0.026 END)) AS baseline_monthly_cost_usd,
FROM qps
INNER JOIN cpu_all ON qps.run_uri = cpu_all.run_uri AND qps.workload_index = cpu_all.workload_index
INNER JOIN cpu_hottest ON qps.run_uri = cpu_hottest.run_uri AND qps.workload_index = cpu_hottest.workload_index
ORDER BY
  timestamp DESC
```

</details>

## Task 7: Cleanup

When you finish the benchmarking, you should delete the resources to avoid extra
charges to your account. The resources include Bigtable instances, GCE VMs, and
BigQuery dataset.

1.  Normally, ephemeral Bigtable instances will be cleaned up automatically in
    the end of a benchmark run. However, it's still possible that some instances
    are left behind for some reason. You can list them with command:

    ```sh
    gcloud bigtable instances list
    ```

    The ephemeral instances have id like `pkb-bigtable-<run ID>`. The deletion
    command is:

    ```sh
    gcloud bigtable instances delete <the instance id>
    ```
1.  Normally, worker VMs will also be cleaned up automatically. In case some VMs
    are left behind, you can go to the
    [VM instances page](https://console.cloud.google.com/compute/instances), and
    delete the worker VMs with name `pkb-<run ID>-<vm ID>` as well as the runner
    VM by selecting them and clicking the "DELETE" button.
1.  To remove the BigQuery dataset `pkb_results`, the command is:

    ```sh
    bq rm pkb_results
    ```

## Additional tips

### How to send feedbacks or report bugs

You can file the issues on
[GitHub](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/issues), and
we will address them promptly.

### Use detachable shell session for benchmarking

For long-running benchmarking, we recommend using persistent shell session such
as [Linux GNU Screen](https://www.gnu.org/software/screen/). By doing so, you
can log out without interrupting the tests. Otherwise, logging
out or losing SSH session will result in undesirable state: the benchmark in
the main thread will be cancelled, but other benchmarks may keep running.

### The runner config is not needed for certain use case

If you just want to run the benchmarks one after another in an arbitrary order,
and each benchmark config sets all the PKB flags in the
[runner config template](./batch_configs/templates/runner_config.yaml), then you
don't need to prepare a dedicated *runner.yaml*.

### Resume the tests after early termination

If there are test failures or accidental cancelation (e.g., `Ctrl+C`), you can
rerun the `docker run` command in [Task 5](#task-5-run-the-benchmarks). In the
new invocation, already finished tests/stages will be skipped, thanks to the
status markers in the folder *~/pkb_configs*. Assuming that we
want to run the benchmark *latency.yaml* twice:

1.  If the two benchmark runs succeed, there will be a marker file
    *latency.yaml.FINISHED*. The content of the file will be
    `Failed: 0 Passed: 2`, and the benchmark will be skipped when you rerun the
    `docker run` command.
1.  Otherwise, if at least one benchmark run is finished (with or without
    failures), there will be a marker file *latency.yaml.PROCESSING*. The
    content may be one of the following:
    1.  `Failed: 0 Passed: 1` or `Failed: 1 Passed: 1`: the benchmark will be
        invoked once if you rerun the `docker run` command.
    1.  `Failed: 1 Passed: 0` or `Failed: 2 Passed: 0`: the benchmark will be
        invoked twice if you rerun the `docker run` command.
1.  If you want to rerun the benchmark from scratch, you can delete all the
    marker files in the config folder.

### Exercise the benchmarks in a specific order

The lab above exercises one use case: the benchmarks are running in parallel,
and each benchmark creates an ephemeral Bigtable instance. Now, we will try
sequential benchmarking with a persistent Bigtable instance, the steps are:

1.  Create a Bigtable table and load it with test data
1.  Run the "latency" benchmark against the table
1.  Run the "throughput" benchmark against the table

The benchmarking tool can handle the steps easily. We will just redo
[Task 3](#task-3-generate-benchmark-configs),
[Task 4](#task-4-generate-a-runner-config), and
[Task 5](#task-5-run-the-benchmarks).

1.  For Task 3 and Task 4, download all the sample files under
    [the sequential_tests folder](./batch_configs/sequential_tests), then update
    *runner.yaml* with commands:

    ```sh
    sudo rm ~/pkb_configs/*
    curl https://raw.githubusercontent.com/GoogleCloudPlatform/PerfKitBenchmarker/master/tutorials/bigtable_walkthrough/batch_configs/sequential_tests/latency.yaml \
    --output ~/pkb_configs/latency.yaml
    curl https://raw.githubusercontent.com/GoogleCloudPlatform/PerfKitBenchmarker/master/tutorials/bigtable_walkthrough/batch_configs/sequential_tests/throughput.yaml \
    --output ~/pkb_configs/throughput.yaml
    curl https://raw.githubusercontent.com/GoogleCloudPlatform/PerfKitBenchmarker/master/tutorials/bigtable_walkthrough/batch_configs/sequential_tests/load.yaml \
    --output ~/pkb_configs/load.yaml
    curl https://raw.githubusercontent.com/GoogleCloudPlatform/PerfKitBenchmarker/master/tutorials/bigtable_walkthrough/batch_configs/sequential_tests/runner.yaml \
    --output ~/pkb_configs/runner.yaml
    PROJECT_ID=<the project id>
    sed -i "s/test_project/${PROJECT_ID}/g" ~/pkb_configs/runner.yaml
    ```

1.  For Task 5, check the config folder *~/pkb_configs*:

    ```sh
    ls ~/pkb_configs
    ```

    **Expected output**

    ```
    latency.yaml  load.yaml  runner.yaml  throughput.yaml
    ```

    Before starting the benchmarking, you should create a Bigtable instance so
    that the instance will persist:

    ```sh
    gcloud bigtable instances create pkb-benchmarks \
    --cluster-config=id=pkb-benchmarks-1,zone=us-central1-b,nodes=3 \
    --display-name=pkb-benchmarks
    ```

    Then, you can execute the `docker run` command again:

    ```sh
    sudo docker run --init --rm -it -v ~/pkb_configs:/pkb/configs \
    -v /tmp:/tmp gcr.io/cloud-bigtable-ecosystem/pkb:latest
    ```

    The end-to-end runtime is about 40 minutes.

    In the end, you should manually delete the Bigtable instance:

    ```sh
    gcloud bigtable instances delete pkb-benchmarks
    ```

### Test failures due to temporary unavailability of the service

This error usually occurs when the Bigtable instance is severely overloaded.
You may try reducing the client threads or increasing the capacity of Bigtable
instance. Also, a future release of the
[Bigtable HBase client](https://github.com/googleapis/java-bigtable-hbase/tree/master)
can make the test more resilient to server issues.

## Reference results

In this section, we list some performance and cost data from our experiments.
You can use them as reference to tune your benchmarks. Unlike the lab above, the
experiments use a more realistic setup: running 30-minute workload against a
100GB table without client throttling (i.e., removing the target throughput
setting). Each benchmark takes about 70 minutes including the time of table
loading.

**The performance and cost shown below are not guaranteed or permanent**:

1.  There are factors that may impact the performance, such as the
    **network congestion** and the **distance** between the VMs and the Bigtable
    instances. Also note that the test table is pre-split with 200 keys.
1.  New releases of Bigtable may improve the performance.
1.  The cost estimate is calculated based on the Bigtable nodes and an
    **approximation** of the **logical table size**. The **actual disk usage**
    is charged in practice.


Common metadata (excerpts from BigQuery query results):

| fieldlength (Byte) | fieldcount | recordcount | readallfields | requestdistribution | threads_per_vm | machine_type  | bigtable_node_count | bigtable_storage_type |
| ------------------ | ---------- | ----------- | ------------- | ------------------- | -------------- | ------------- | ------------------- | --------------------- |
| 1024               | 1          | 100000000   | false         | zipfian             | 32             | n1-highcpu-16 | 3                   | ssd                   |

Performance results (excerpts from BigQuery query results, `a / b` in the table indicates the read/update latency):

| experiment     | workload_name | actual_throughput | p50_latency_ms | p99_latency_ms  | p999_latency_ms   | vm_count |
| -------------- | ------------- | ----------------- | -------------- | --------------- | ----------------- | -------- |
| light_workload | workloada     | 6419.68           | 4.623 / 4.451  | 10.535 / 9.391  | 80.447 / 59.711   | 1        |
| light_workload | workloadb     | 6188.45           | 4.715 / 4.723  | 9.543 / 8.879   | 105.023 / 82.687  | 1        |
| light_workload | workloadc     | 6469.17           | 4.387 / na     | 12.583 / na     | 114.815 / na      | 1        |
| light_workload | workloadx     | 6892.63           | na / 4.351     | na / 7.835      | na / 43.167       | 1        |
| heavy_workload | workloada     | 22188.64          | 4.203 / 4.061  | 63.743 / 64.191 | 151.039 / 165.247 | 6        |
| heavy_workload | workloadb     | 24750.26          | 4.183 / 4.163  | 57.663 / 57.887 | 112.895 / 121.919 | 6        |
| heavy_workload | workloadc     | 28044.21          | 4.235 / na     | 50.847 / na     | 99.007 / na       | 6        |
| heavy_workload | workloadx     | 24894.84          | na / 4.085     | na / 57.951     | na / 74.751       | 6        |

Resource utilization and monthly cost estimates for the above eight experiments (excerpts from BigQuery query results):

| experiment      | workload_name | avg_cpu_all_nodes | avg_cpu_hottest_node  | bigtable_node_count | baseline_table_gb | baseline_monthly_cost_usd |
| --------------- | ------------- | ----------------- | --------------------- | ------------------- | ----------------- | ------------------------- |
| light_workload  | workloada     | 0.357             | 0.403                 | 3                   | 99                | 1420.83                   |
| light_workload  | workloadb     | 0.26              | 0.291                 | 3                   | 99                | 1420.83                   |
| light_workload  | workloadc     | 0.254             | 0.285                 | 3                   | 99                | 1420.83                   |
| light_workload  | workloadx     | 0.437             | 0.498                 | 3                   | 99                | 1420.83                   |
| heavy_workload  | workloada     | 0.881             | 0.982                 | 3                   | 99                | 1420.83                   |
| heavy_workload  | workloadb     | 0.88              | 0.987                 | 3                   | 99                | 1420.83                   |
| heavy_workload  | workloadc     | 0.919             | 0.988                 | 3                   | 99                | 1420.83                   |
| heavy_workload  | workloadx     | 0.879             | 0.963                 | 3                   | 99                | 1420.83                   |
