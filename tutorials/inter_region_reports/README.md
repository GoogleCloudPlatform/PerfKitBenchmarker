# Reproducing Inter-Region Tests with PerfKit Benchmarker

This is a hands-on lab/tutorial about running PerfKit Benchmarker (PKB) on
Google Cloud. You can reproduce your own
[inter-region latency and throughput reports](https://datastudio.google.com/c/u/0/reporting/fc733b10-9744-4a72-a502-92290f608571/page/70YCB)
by following these instructions. This lab is referenced in the Google Cloud
[VPC Network performance online documentation](https://cloud.google.com/vpc/docs/vpc#network-performance).

## Overview

### Introducing PerfKit Benchmarker

[PerfKit Benchmarker](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker)
is an open source framework with commonly accepted benchmarking tools that you
can use to measure and compare cloud providers. PKB automates setup and teardown
of resources, including Virtual Machines (VMs), on whichever cloud provider you
choose. Additionally, PKB installs and runs the benchmark software tests and
provides patterns for saving the test output for future analysis and debugging.

Check out the
[PerfKit Benchmarker README](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/blob/master/README.md)
for a detailed introduction.

## What you'll do

This lab demonstrates an end-to-end workflow for running benchmark tests,
uploading the result data to Google Cloud, and rendering reports based on that
data.

In this lab, you will:

*   Install PerfKit Benchmarker
*   Create a BigQuery dataset for benchmark result data storage
*   Start a benchmark test for latency
*   Start a benchmark test for throughput
*   Work with the test result data in BigQuery
*   Create a new [Data Studio](https://datastudio.google.com) data source and
    report

__Note:__ this lab is biased to running __networking__ benchmarks, on __Google
Cloud__.

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

### Activate the Cloud Shell

From the Cloud Console click the __Activate Cloud Shell__ icon on the top right
toolbar:

![alt text](img/cloud_shell_icon.png "Cloud Shell Icon")

You may need to click __Continue__ the first time.

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

### Disable OS Login, in favor of legacy SSH keys

[OS Login](https://cloud.google.com/compute/docs/oslogin/) is now enabled by
default on this project, and any VM instances created. OS Login enables the use
of Compute Engine IAM roles to manage SSH access to Linux instances.

PKB, however, uses legacy SSH keys for authentication, so OS Login must be
disabled.

In Cloud Shell, disable OS Login for the project.

```
gcloud compute project-info add-metadata --metadata enable-oslogin=FALSE
```

## Task 1. Install PerfKit Benchmarker

in this lab, you use Cloud Shell and the
[PKB repo in GitHub](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker).

1.  Set up a virtualenv isolated Python environment within Cloud Shell.

    ```
    python3 -m venv $HOME/my_virtualenv
    ```

    ```
    source $HOME/my_virtualenv/bin/activate
    ```

1.  Ensure Google Cloud SDK tools like bq find the proper Python.

    ```
    export CLOUDSDK_PYTHON=$HOME/my_virtualenv/bin/python
    ```

1.  Clone the PerfKitBenchmarker repository.

    ```
    cd $HOME && git clone https://github.com/GoogleCloudPlatform/PerfKitBenchmarker.git
    ```

    ```
    cd PerfKitBenchmarker/
    ```

1.  Install PKB dependencies.

    ```
    pip install -r requirements.txt
    ```

    You can safely ignore any warnings about upgrading `pip`.

> __Note__: these setup instructions are specific for running network
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
results over time, and create data visualizations.

In order to create online reports, this lab sends data to tables in a BigQuery
dataset. BigQuery dataset tables can then be used as data sources for reports.

Using the BigQuery command-line tool `bq`, initialize an empty __dataset__.

```
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

## Task 3. Start a benchmark test for latency

PKB supports
[__custom configuration__](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker/wiki/PerfkitBenchmarker-Configurations)
files in which you can set the cloud provider, zone, machine type, and many
other options for each VM.

1.  Grab and review the custom config file for latency:
    `all_region_latency.yaml`

    ```
    cat ./tutorials/inter_region_reports/data/all_region_latency.yaml
    ```

    __Output (do not copy)__

    ```output
    # ping benchmark for latency.
    ping:
      flag_matrix: inter_region
      flag_matrix_filters:
        inter_region: "zones < extra_zones"
      flag_matrix_defs:
        inter_region:
          gce_network_tier: [premium]
          zones: [asia-east1-a,asia-east2-a,asia-northeast1-a,asia-northeast2-a,asia-south1-a,asia-southeast1-a,australia-southeast1-a,europe-north1-a,europe-west1-c,europe-west2-a,europe-west3-a,europe-west4-a,europe-west6-a,northamerica-northeast1-a,southamerica-east1-a,us-central1-a,us-east1-b,us-east4-a,us-west1-a,us-west2-a]
          extra_zones: [asia-east1-a,asia-east2-a,asia-northeast1-a,asia-northeast2-a,asia-south1-a,asia-southeast1-a,australia-southeast1-a,europe-north1-a,europe-west1-c,europe-west2-a,europe-west3-a,europe-west4-a,europe-west6-a,northamerica-northeast1-a,southamerica-east1-a,us-central1-a,us-east1-b,us-east4-a,us-west1-a,us-west2-a]
          machine_type: [n1-standard-2]
      flags:
        cloud: GCP
        ping_also_run_using_external_ip: True
    ```

1.  Run the latency tests.

    ```
    ./pkb.py --benchmarks=ping \
        --benchmark_config_file=./tutorials/inter_region_reports/data/all_region_latency.yaml \
        --bq_project=$(gcloud info --format='value(config.project)') \
        --bigquery_table=pkb_results.all_region_results
    ```

    This test pass usually takes ~12 minutes for each region pair. Test output
    will be pushed to the BigQuery table `pkb_results.all_region_results`.

    __Output (do not copy)__

    ```output
    ...
    -------------------------PerfKitBenchmarker Results Summary-------------------------
    PING:
      Min Latency            33.101000 ms        (ip_type="internal" ...)
      Average Latency        33.752000 ms        (ip_type="internal" ...)
      Max Latency            34.023000 ms        (ip_type="internal" ...)
      Latency Std Dev         0.407000 ms        (ip_type="internal" ...)
      ...
      Min Latency            34.440000 ms        (ip_type="external" ...)
      Average Latency        34.903000 ms        (ip_type="external" ...)
      Max Latency            38.060000 ms        (ip_type="external" ...)
      Latency Std Dev         0.460000 ms        (ip_type="external" ...)
    ...
    ----------------------------------------
    Name  UID    Status     Failed Substatus
    ----------------------------------------
    ping  ping0  SUCCEEDED
    ----------------------------------------
    Success rate: 100.00% (1/1)
    ...
    ```

    Test results show that this benchmark runs 4 times between the two VM
    instances in different regions:

    *   traffic over __external IPs__, vm1>vm2
    *   traffic over __internal IPs__, vm1>vm2
    *   traffic over __external IPs__, vm2>vm1
    *   traffic over __internal IPs__, vm2>vm1

## Task 4. Start a benchmark test for throughput

1.  Review the custom config file for throughput tests: `all_region_iperf.yaml`.

    ```
    cat tutorials/inter_region_reports/data/all_region_iperf.yaml
    ```

    __Output (do not copy)__

    ```output
    # iperf benchmark for throughput.
    iperf:
      flag_matrix: inter_region
      flag_matrix_filters:
        inter_region: "zones < extra_zones"
      flag_matrix_defs:
        inter_region:
          gce_network_tier: [premium]
          zones: [asia-east1-a,asia-east2-a,asia-northeast1-a,asia-northeast2-a,asia-south1-a,asia-southeast1-a,australia-southeast1-a,europe-north1-a,europe-west1-c,europe-west2-a,europe-west3-a,europe-west4-a,europe-west6-a,northamerica-northeast1-a,southamerica-east1-a,us-central1-a,us-east1-b,us-east4-a,us-west1-a,us-west2-a]
          extra_zones: [asia-east1-a,asia-east2-a,asia-northeast1-a,asia-northeast2-a,asia-south1-a,asia-southeast1-a,australia-southeast1-a,europe-north1-a,europe-west1-c,europe-west2-a,europe-west3-a,europe-west4-a,europe-west6-a,northamerica-northeast1-a,southamerica-east1-a,us-central1-a,us-east1-b,us-east4-a,us-west1-a,us-west2-a]
          machine_type: [n1-standard-2]
      flags:
        cloud: GCP
        iperf_runtime_in_seconds: 60
        iperf_sending_thread_count: 1,4,32
    ```

1.  Run the throughput tests.

    ```
    ./pkb.py --benchmarks=iperf \
        --benchmark_config_file=./tutorials/inter_region_reports/data/all_region_iperf.yaml \
        --bq_project=$(gcloud info --format='value(config.project)') \
        --bigquery_table=pkb_results.all_region_results
    ```

    This test pass usually takes ~20 minutes for each region pair. It runs
    throughput tests with 1, 4, and 32 threads.

    Test output will be pushed to the BigQuery table
    `pkb_results.all_region_results`.

    __Output (do not copy)__

    ```output
    ...
    -------------------------PerfKitBenchmarker Results Summary-------------------------
    IPERF:
      Throughput   4810.000000 Mbits/sec   (ip_type="external" ... receiving_zone="us-east1-b" ...)
      Throughput   9768.000000 Mbits/sec   (ip_type="internal" ... receiving_zone="us-east1-b" ...)
      Throughput   7116.000000 Mbits/sec   (ip_type="external" ... receiving_zone="us-central1-a" ...)
      Throughput   9747.000000 Mbits/sec   (ip_type="internal" ... receiving_zone="us-central1-a" ...)
    ...
    ------------------------------------------
    Name   UID     Status     Failed Substatus
    ------------------------------------------
    iperf  iperf0  SUCCEEDED
    ------------------------------------------
    Success rate: 100.00% (1/1)
    ...
    ```

    Test results show that, for each threadcount value, this benchmark runs 4
    times between the two VM instances in different regions:

    *   traffic over __external IPs__, vm1>vm2
    *   traffic over __internal IPs__, vm1>vm2
    *   traffic over __external IPs__, vm2>vm1
    *   traffic over __internal IPs__, vm2>vm1

## Task 5. Work with the test result data in BigQuery

1.  Query `pkb_results.all_region_results` to view the test results in BigQuery.

    In Cloud Shell, run a `bq` command.

    ```
    bq query 'SELECT test, metric, value, product_name FROM pkb_results.all_region_results'
    ```

    You can also see your data using the __Query editor__ in the
    [BigQuery UI](https://console.cloud.google.com/bigquery).

    __Output (do not copy)__

    ```output
    ...
    +-------+--------------------+--------------------+--------------------+
    | test  |       metric       |       value        |    product_name    |
    +-------+--------------------+--------------------+--------------------+
    | iperf | Throughput         |              724.0 | PerfKitBenchmarker |
    | iperf | Throughput         |              717.0 | PerfKitBenchmarker |
    | iperf | Throughput         |              733.0 | PerfKitBenchmarker |
    | iperf | Throughput         |              701.0 | PerfKitBenchmarker |
    | iperf | Throughput         |             2866.0 | PerfKitBenchmarker |
    | iperf | Throughput         |             2849.0 | PerfKitBenchmarker |
    | iperf | Throughput         |             2433.0 | PerfKitBenchmarker |
    | iperf | Throughput         |             2880.0 | PerfKitBenchmarker |
    | iperf | Throughput         |             7909.0 | PerfKitBenchmarker |
    | iperf | Throughput         |             9790.0 | PerfKitBenchmarker |
    | iperf | Throughput         |             6985.0 | PerfKitBenchmarker |
    | iperf | Throughput         |             9708.0 | PerfKitBenchmarker |
    | iperf | lscpu              |                0.0 | PerfKitBenchmarker |
    | iperf | proccpu            |                0.0 | PerfKitBenchmarker |
    | iperf | proccpu_mapping    |                0.0 | PerfKitBenchmarker |
    | iperf | End to End Runtime | 1114.2245268821716 | PerfKitBenchmarker |
    | ping  | End to End Runtime |   755.233142375946 | PerfKitBenchmarker |
    | ping  | Min Latency        |             33.296 | PerfKitBenchmarker |
    | ping  | Average Latency    |             33.378 | PerfKitBenchmarker |
    | ping  | Max Latency        |             33.687 | PerfKitBenchmarker |
    | ping  | Latency Std Dev    |              0.214 | PerfKitBenchmarker |
    | ping  | Min Latency        |             34.613 | PerfKitBenchmarker |
    | ping  | Average Latency    |             34.916 | PerfKitBenchmarker |
    | ping  | Max Latency        |             38.789 | PerfKitBenchmarker |
    | ping  | Latency Std Dev    |              0.512 | PerfKitBenchmarker |
    | ping  | Min Latency        |             34.657 | PerfKitBenchmarker |
    | ping  | Average Latency    |             34.711 | PerfKitBenchmarker |
    | ping  | Max Latency        |              34.79 | PerfKitBenchmarker |
    | ping  | Latency Std Dev    |              0.227 | PerfKitBenchmarker |
    | ping  | Min Latency        |             33.051 | PerfKitBenchmarker |
    | ping  | Average Latency    |             34.339 | PerfKitBenchmarker |
    | ping  | Max Latency        |             34.812 | PerfKitBenchmarker |
    | ping  | Latency Std Dev    |               0.63 | PerfKitBenchmarker |
    | ping  | proccpu_mapping    |                0.0 | PerfKitBenchmarker |
    | ping  | proccpu            |                0.0 | PerfKitBenchmarker |
    | ping  | lscpu              |                0.0 | PerfKitBenchmarker |
    +-------+--------------------+--------------------+--------------------+
    ```

1.  Create a BigQuery __view__ which makes working with the data easier.

    ```
    sed "s/<PROJECT_ID>/$(gcloud info --format='value(config.project)')/g" \
        ./tutorials/inter_region_reports/data/all_region_result_view.sql \
        > ./view.sql
    ```

    ```
    bq mk --view="$(cat ./view.sql)" pkb_results.all_region_result_view
    ```

1.  Verify you can retrive data through the view.

    ```
    bq query --nouse_legacy_sql \
    'SELECT test, metric, value, unit, sending_zone, receiving_zone, sending_thread_count, ip_type, product_name, thedate FROM pkb_results.all_region_result_view ORDER BY thedate'
    ```

    __Output (do not copy)__

    ~~~output
    +-------+-----------------+--------+-----------+---------------+----------------+----------------------+
    | test  |     metric      | value  |   unit    | sending_zone  | receiving_zone | sending_thread_count |
    +-------+-----------------+--------+-----------+---------------+----------------+----------------------+
    | ping  | Min Latency     | 33.051 | ms        | us-central1-a | us-east1-b     | NULL                 |...
    | ping  | Average Latency | 34.339 | ms        | us-central1-a | us-east1-b     | NULL                 |...
    | ping  | Max Latency     | 34.812 | ms        | us-central1-a | us-east1-b     | NULL                 |...
    | ping  | Latency Std Dev |   0.63 | ms        | us-central1-a | us-east1-b     | NULL                 |...
    | ping  | Min Latency     | 34.657 | ms        | us-east1-b    | us-central1-a  | NULL                 |...
    | ping  | Average Latency | 34.711 | ms        | us-east1-b    | us-central1-a  | NULL                 |...
    | ping  | Max Latency     |  34.79 | ms        | us-east1-b    | us-central1-a  | NULL                 |...
    | ping  | Latency Std Dev |  0.227 | ms        | us-east1-b    | us-central1-a  | NULL                 |...
    | ping  | Min Latency     | 34.613 | ms        | us-central1-a | us-east1-b     | NULL                 |...
    | ping  | Average Latency | 34.916 | ms        | us-central1-a | us-east1-b     | NULL                 |...
    | ping  | Max Latency     | 38.789 | ms        | us-central1-a | us-east1-b     | NULL                 |...
    | ping  | Latency Std Dev |  0.512 | ms        | us-central1-a | us-east1-b     | NULL                 |...
    | ping  | Min Latency     | 33.296 | ms        | us-east1-b    | us-central1-a  | NULL                 |...
    | ping  | Average Latency | 33.378 | ms        | us-east1-b    | us-central1-a  | NULL                 |...
    | ping  | Max Latency     | 33.687 | ms        | us-east1-b    | us-central1-a  | NULL                 |...
    | ping  | Latency Std Dev |  0.214 | ms        | us-east1-b    | us-central1-a  | NULL                 |...
    | iperf | Throughput      |  724.0 | Mbits/sec | us-central1-a | us-east1-b     | 1                    |...
    | iperf | Throughput      |  717.0 | Mbits/sec | us-central1-a | us-east1-b     | 1                    |...
    | iperf | Throughput      |  733.0 | Mbits/sec | us-east1-b    | us-central1-a  | 1                    |...
    | iperf | Throughput      |  701.0 | Mbits/sec | us-east1-b    | us-central1-a  | 1                    |...
    | iperf | Throughput      | 2866.0 | Mbits/sec | us-central1-a | us-east1-b     | 4                    |...
    | iperf | Throughput      | 2849.0 | Mbits/sec | us-central1-a | us-east1-b     | 4                    |...
    | iperf | Throughput      | 2433.0 | Mbits/sec | us-east1-b    | us-central1-a  | 4                    |...
    | iperf | Throughput      | 2880.0 | Mbits/sec | us-east1-b    | us-central1-a  | 4                    |...
    | iperf | Throughput      | 7909.0 | Mbits/sec | us-central1-a | us-east1-b     | 32                   |...
    | iperf | Throughput      | 9790.0 | Mbits/sec | us-central1-a | us-east1-b     | 32                   |...
    | iperf | Throughput      | 6985.0 | Mbits/sec | us-east1-b    | us-central1-a  | 32                   |...
    | iperf | Throughput      | 9708.0 | Mbits/sec | us-east1-b    | us-central1-a  | 32                   |...
    +-------+-----------------+--------+-----------+---------------+----------------+----------------------+
        ```
    ~~~

You will learn to __visualize__ such data, in the next section.

## Task 6. Create a new Data Studio data source and report

1.  Open [Data Studio](https://datastudio.google.com).

1.  Click __Create__ > __Report__.

    *   You may need to click __Get Started__ the first time.
    *   You may need to __Accept__ terms and conditions.
    *   You may need to choose __No, thanks__ to email.
    *   If you don't see an _Untitled Report_, click __Create__ > __Report__
        again.

1.  Click __BigQuery__ under __Add data to report__ > __Connect to data__.

    *   You may need to click __Authorize__.
    *   Click __My Projects__.
    *   Click the project-id you're using.
    *   Click the __pkb\_results__ dataset created earlier.
    *   Click the __all\_region\_result\_view__ created earlier.
    *   Click __Add__.
    *   Click __Add to report__ to confirm.

    This creates a new data source in your new reports.

1.  Click __Untitled Report__ and name your report __Inter-region Dashboard__.

1.  Remove the default __Chart__ > __Table__.

    *   Select the default table object in the report body.
    *   Press the __Delete__ key on your keyboard.

1.  Create a Pivot table chart for your iperf results.

    *   Click __Add a chart__.
    *   Click __Pivot table__.
    *   Drop the Pivot table in the upper left corner of the report body.
    *   Drag the chart boundary to make it a rectangle with room for 4 columns.

1.  Set iperf Pivot table settings.

    *   For __Row dimension__ click __Add dimension__ and choose
        __receiving\_region__.
    *   For __Column dimension__ click __Add dimension__ and choose
        __sending\_region__.
    *   For __Metric__ click __Add metric__ and choose __value__.
    *   The metric defaults to _SUM_, click _SUM_ and choose __Average__.
    *   Remove the __Record Count__ metric.
    *   Scroll settings down to __Filter__ and click __Add a filter__.
    *   Click __Create a filter__.
    *   Name the filter __iperf filter__.
    *   For field, choose __test__.
    *   For condition, choose __Equal to__.
    *   For value, type __iperf__.
    *   Click __Save__.

1.  Create a Pivot table chart for your ping results.

    *   Click __Add a chart__.
    *   Click __Pivot table__.
    *   Drop the Pivot table below the first table.
    *   Drag the chart boundary to make it a rectangle with room for 4 columns.

1.  Set ping Pivot table settings.

    *   For __Row dimension__ click __Add dimension__ and choose
        __receiving\_region__.
    *   For __Column dimension__ click __Add dimension__ and choose
        __sending\_region__.
    *   For __Metric__ click __Add metric__ and choose __value__.
    *   The metric defaults to _SUM_, click _SUM_ and choose __Average__.
    *   Remove the __Record Count__ metric.
    *   Scroll settings down to __Filter__ and click __Add a filter__.
    *   Click __Create a filter__.
    *   Name the filter __ping filter__.
    *   For field, choose __test__.
    *   For condition, choose __Equal to__.
    *   For value, type __ping__.
    *   Click __Save__.

1.  Click __View__ to see your rendered report.

    Click __Edit__ to edit again. You can customize the report setup to create
    your own
    [inter-region latency and throughput reports](https://datastudio.google.com/c/u/0/reporting/fc733b10-9744-4a72-a502-92290f608571/page/70YCB).

Enjoy.

## Cleanup

Note that the following resources may have been created, that you may wish to
remove.

*   The `pkb_results` dataset in BigQuery
*   The `all_region_results` table in the `pkb_results` dataset
*   Any reports you copied/created in Data Studio

## Congratulations!

You have completed the Reproducing Inter-Region Tests with PerfKit Benchmarker
lab!

### What was covered

You installed PerfKit Benchmarker, and ran benchmark tests in the cloud.

You learned how to build an end-to-end workflow for running benchmarks,
gathering data, and visualizing performance trends.

### Learn More

*   Watch
    [Performance Benchmarking on Google Cloud Platform](https://youtu.be/fNMzlTmufy0)
    with tools, best practices, and methodologies from the PerfKitBenchmarker
    team.
*   Check out the blog post: [Performance art: Making cloud network performance
    benchmarking faster and
    easier](https://cloud.google.com/blog/products/networking/perfkit-benchmarker-for-evaluating-cloud-network-performance)
*   Read about more details in the
    [Measuring Cloud Network Performance with PerfKit Benchmarker white paper](http://services.google.com/fh/files/misc/measuring-cloud-network-performance-perfkit-wp.pdf)
*   Follow the
    [PKB repo](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker).

### Credits

> __Note:__ the original version of this lab was prepared by the networking
> research team at the
> [AT&T Center for Virtualization](https://www.smu.edu/Provost/virtualization)
> at __Southern Methodist University__.
