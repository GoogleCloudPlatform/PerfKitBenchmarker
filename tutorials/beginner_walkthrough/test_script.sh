#!/bin/bash -x

# name: beginner_walkthrough
# description: Execute commands from the beginner_walkthrough tutorial.

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

# Task 3. Explore PKB command-line flags
#         Need to do this before Task 2, because Task 2 blocks.
./pkb.py --helpmatch=pkb
./pkb.py --helpmatch=benchmarks | grep perfkitbenchmarker
./pkb.py --helpmatch=benchmarks | grep perfkitbenchmarker | wc -l
./pkb.py --helpmatchmd=linux_benchmarks
./pkb.py --helpmatchmd=netperf
./pkb.py --helpmatchmd=iperf

# Task 2. Start one benchmark test
./pkb.py --benchmarks=iperf

# Task 4. Consider different network benchmarks
#         No tests.

# Task 5. Explore the results of a benchmark test
#         No tests.

# Task 6. Run more benchmark tests using PerfKit Benchmarker
./pkb.py --benchmarks=netperf --netperf_benchmarks="TCP_RR,TCP_STREAM"
./pkb.py --benchmarks=netperf --netperf_benchmarks="UDP_RR,UDP_STREAM"
./pkb.py --benchmarks=ping --zone=us-east1-b --machine_type=f1-micro
#     We can't do the 32 Gbps tests in our constrained environment.

# Task 7. Understand custom configuration files and benchmark sets
#         No tests.

# Task 8. Push test result data to [BigQuery](https://cloud.google.com/bigquery)
bq mk samples_mart
export PROJECT=$(gcloud info --format='value(config.project)')
bq load --project_id=$PROJECT \
    --source_format=NEWLINE_DELIMITED_JSON \
    samples_mart.results \
    ./tutorials/beginner_walkthrough/data/samples_mart/sample_results.json \
    ./tutorials/beginner_walkthrough/data/samples_mart/results_table_schema.json
bq query 'SELECT * FROM samples_mart.results LIMIT 200'

# Task 9. Query and visualize result data with
#   [Data Studio](https://datastudio.google.com)
bq mk example_dataset
./pkb.py --benchmarks=iperf \
    --bq_project=$PROJECT \
    --bigquery_table=example_dataset.network_tests
bq query 'SELECT product_name, test, metric, value FROM example_dataset.network_tests'
bq mk example_dataset
bq load --project_id=$PROJECT \
    --autodetect \
    --source_format=NEWLINE_DELIMITED_JSON \
    example_dataset.results \
    ./tutorials/beginner_walkthrough/data/bq_pkb_sample.json

cat << EOF > ./results_view.sql
SELECT
    value,
    unit,
    metric,
    test,
    TIMESTAMP_MICROS(CAST(timestamp * 1000000 AS int64)) AS thedate,
    REGEXP_EXTRACT(labels, r"\|vm_1_cloud:(.*?)\|") AS vm_1_cloud,
    REGEXP_EXTRACT(labels, r"\|vm_2_cloud:(.*?)\|") AS vm_2_cloud,
    REGEXP_EXTRACT(labels, r"\|sending_zone:(.*?)\|") AS sending_zone,
    REGEXP_EXTRACT(labels, r"\|receiving_zone:(.*?)\|") AS receiving_zone,
    REGEXP_EXTRACT(labels, r"\|sending_zone:(.*?-.*?)-.*?\|") AS sending_region,
    REGEXP_EXTRACT(labels, r"\|receiving_zone:(.*?-.*?)-.*?\|") AS receiving_region,
    REGEXP_EXTRACT(labels, r"\|vm_1_machine_type:(.*?)\|") AS machine_type,
    REGEXP_EXTRACT(labels, r"\|ip_type:(.*?)\|") AS ip_type
FROM
   \`$PROJECT.example_dataset.results\`
EOF

bq mk \
--use_legacy_sql=false \
--description '"This is my view"' \
--view "$(cat ./results_view.sql)" \
example_dataset.results_view
#   The rest is UI-based, not testable here.

# Done
