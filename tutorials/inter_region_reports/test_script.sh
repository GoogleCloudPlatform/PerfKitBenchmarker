#!/bin/bash -x

# name: inter_region_reports
# description: Execute commands from the inter_region_reports tutorial.

# Set up
gcloud config list project
gcloud compute project-info add-metadata --metadata enable-oslogin=FALSE

# Task 1. Install PerfKit Benchmarker
python3 -m venv $HOME/my_virtualenv
source $HOME/my_virtualenv/bin/activate
export CLOUDSDK_PYTHON=$HOME/my_virtualenv/bin/python
cd $HOME && git clone https://github.com/GoogleCloudPlatform/PerfKitBenchmarker.git
cd PerfKitBenchmarker/
pip install -r requirements.txt

# Task 2. Create a BigQuery dataset for benchmark result data storage
bq mk pkb_results

# Task 3. Start a benchmark test for latency
cat ./tutorials/inter_region_reports/data/all_region_latency.yaml
./pkb.py --benchmarks=ping \
    --benchmark_config_file=./tutorials/inter_region_reports/data/all_region_latency.yaml \
    --bq_project=$(gcloud info --format='value(config.project)') \
    --bigquery_table=pkb_results.all_region_results

# Task 4. Start a benchmark test for throughput
cat tutorials/inter_region_reports/data/all_region_iperf.yaml
./pkb.py --benchmarks=iperf \
    --benchmark_config_file=./tutorials/inter_region_reports/data/all_region_iperf.yaml \
    --bq_project=$(gcloud info --format='value(config.project)') \
    --bigquery_table=pkb_results.all_region_results

# Task 5. Work with the test result data in BigQuery
bq query 'SELECT test, metric, value, product_name FROM pkb_results.all_region_results'
sed "s/<PROJECT_ID>/$(gcloud info --format='value(config.project)')/g" \
    ./tutorials/inter_region_reports/data/all_region_result_view.sql \
    > ./view.sql
bq mk --view="$(cat ./view.sql)" pkb_results.all_region_result_view
bq query --nouse_legacy_sql \
'SELECT test, metric, value, unit, sending_zone, receiving_zone, sending_thread_count, ip_type, product_name, thedate FROM pkb_results.all_region_result_view ORDER BY thedate'

# Task 6. Create a new [Data Studio](https://datastudio.google.com) data source and
#         report
#   The rest is UI-based, not testable here.

