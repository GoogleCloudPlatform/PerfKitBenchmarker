# PKB Scheduler

The PKB scheduler is a tool for running PKB benchmarks periodically and logging/publishing the results. It also provides a web dashboard to view the completion status of past runs.

## Running the scheduler

You need to create a scheduler config file. Here's an example:

```yaml
pkb_executable: /home/tedsta/code/PerfKitBenchmarker/pkb.py
logs_path: logs/
results_path: results/
completion_status_path: .pkb_completion_status/
results_gcs_bucket: pkb-data
benchmark_config: pkb_configs.yaml
run_benchmarks:
  netperf_same_zone:
    AWS:
      seconds_between_launches: 300
      run_processes: 1
    GCP:
      seconds_between_launches: 1800
      run_processes: 4
```

And here's its corresponding PKB benchmark config file:

```yaml
netperf_same_zone:
  name: netperf
  flags:
    netperf_histogram_buckets: 1000
  flag_matrix_defs:
    AWS:
      cloud: [AWS]
      zones: [us-west-1a]
      machine_type: [c4.xlarge]
      netperf_num_streams: ["1"]
      netperf_benchmarks: ["TCP_STREAM"]
    Azure:
      cloud: [Azure]
      zones: [westus]
      machine_type: [Standard_F4, Standard_F8]
      netperf_num_streams: ["1"]
      netperf_benchmarks: ["TCP_STREAM"]
    GCP:
      gce_network_name: [default]
      cloud: [GCP]
      zones: [us-east1-b, us-west1-a]
      machine_type: [n1-standard-4, n1-standard-8]
      netperf_num_streams: ["1"]
      netperf_benchmarks: ["TCP_STREAM"]
```

Notice you don't have to run every benchmark/flag_matrix that is defined. Here we don't run the Azure flag matrix in netperf_same_zone.

The `pkb_executable`, `benchmark_config`, and `run_benchmarks` fields in the scheduler config are required. If the `results_gcs_bucket` field is omitted, the results won't be published to GCS, but they'll still be stored locally.

## Running the scheduler dashboard

The code is stored in `dashboard/`. Run:

```
dashboard/deploy.py <dashboard config>
```

from the directory where the PKB scheduler is running. It'll be hosted on port 8080.

Here's an example dashboard config:

```yaml
scheduler_config: scheduler_config.yaml
categories:
  "Networking":
  - benchmark: netperf_same_zone
    flag_axes:
      AWS: [zones, machine_type]
      GCP: [zones, machine_type]
```

As you can see you can group benchmarks into categories. In the example we just have one category named "Networking".
