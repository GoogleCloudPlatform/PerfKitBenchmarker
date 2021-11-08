---
title: Getting Started
layout: page
---

## Python

PerfKitBenchmarker requires [Python](https://www.python.org/downloads/) and is
tested on Python 3.7. We recommend using a
[virtual environment](https://docs.python.org/3/tutorial/venv.html) to manage
different Python versions and their required `pip` dependencies.

Create a virtual environment, this will set python3 as the default version.

```sh
python3 -m venv pkb-virtualenv
```

Activate this virtual environment. When you're finished you can simply type
`deactivate`.

```sh
source pkb-virtualenv/bin/activate
```

## PerfKitBenchmarker

### Environment

PerfkitBenchmarker is hosted
[here](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker).

1.  Clone PKB to your local machine:

    ```
    cd $HOME && git clone https://github.com/GoogleCloudPlatform/PerfKitBenchmarker.git
    ```

1.  Activate the virtualenv that you installed earlier.

    ```
    source $HOME/pkb-virtualenv/bin/activate
    ```

1.  Install dependencies.

    ```
    cd $HOME/PerfKitBenchmarker && pip install -r requirements.txt
    ```

1.  Install the cloud provider's command line interface, for example gcloud from
    https://developers.google.com/cloud/sdk/.

### Running

This procedure demonstrates how to run only selective stages of a benchmark.
This technique can be useful for examining a machine after it has been prepared,
but before the benchmark runs.

This example shows how to provision and prepare the `cluster_boot` benchmark
without actually running the benchmark.

1.  Change to your local version of PKB: `cd $HOME/PerfKitBenchmarker`

1.  Run provision, prepare, and run stages of `cluster_boot`.

    ```
    ./pkb.py --benchmarks=cluster_boot --machine_type=n1-standard-2 --zones=us-central1-f --run_stage=provision,prepare,run
    ```

1.  The output from the console will tell you the run URI for your benchmark.
    Try to ssh into the VM. The machine "Default-0" came from the VM group which
    is specified in the benchmark_config for cluster_boot.

    ```
    ssh -F /tmp/perfkitbenchmarker/runs/<run_uri>/ssh_config default-0
    ```

1.  Now that you have examined the machines, teardown the instances that were
    made and cleanup.

    ```
    ./pkb.py --benchmarks=cluster_boot --run_stage=teardown -run_uri=<run_uri>
    ```
