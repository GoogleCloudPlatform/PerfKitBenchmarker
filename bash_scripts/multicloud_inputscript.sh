#!/bin/bash
# Short Script that will run selected test for all specified instant types based on the input values

# Requirements:
# Must have PerfkitBenchmarker Reposition on your Machine
# Must have authenticated with cloud providers and have appropriate permission  
# Must have virtual environment with python and other requirements

# delimiter for spliting arrays
IFS=' '
# Default Benchmark to run can be changed with CMDLINE Args
BENCHMARK="iperf"
# datetime
DT=`date +"%Y-%m-%d-%H:%M:%S"`
# Google Project ID 
PROJECT=$(gcloud info --format='value(config.project)')
# Array of Log file Names
logArr=()
# Array of Table Names
bqtArr=()

# array of machine types
declare -A machine_type_arr
machine_type_arr["AWS"]="c5n.4xlarge m5.xlarge g4dn.xlarge"
machine_type_arr["Azure"]="Standard_D16_v4 Standard_D4_v3 Standard_NVas_v4"
machine_type_arr["GCP"]="c2d-standard-16 n2-standard-4 n1-standard-4"

# array of availibility zones
declare -A az_arr
az_arr["AWS"]="us-east-1a us-east-1a us-east-1a"
az_arr["Azure"]="eastus eastus eastus2"
az_arr["GCP"]="us-east4-a us-east4-a us-east4-a"

# Function for running benchmark
task(){
    echo "Running task with $1, $2, $3"
    echo "/home/jetryan4/PerfKit/PerfKitBenchmarker/pkb.py --cloud=$1 --benchmarks=${BENCHMARK} --machine_type=$2 --zone=$3"
    /home/jetryan4/PerfKit/PerfKitBenchmarker/pkb.py --cloud="$1" --benchmarks=${BENCHMARK} --machine_type="$2" --zone="$3"
    # sleep 5
}

# Function for Assembling Log File
lf(){
    touch "${BENCHMARK}_logs_${DT}/${1}_${2//./_}_output_logs_${3//:/-}.log"
    templog="${BENCHMARK}_logs_${DT}/${1}_${2//./_}_output_logs_${3//:/-}.log"
}

# Function to Generate Big Query Output Table Name 
# cloud_machinetype_zone
bqt(){
    table_name="${1}_${2//./_}_${3}"
}

# Check Error or Success return the percentage frpn the 3 to bottom of the page
eos(){
    err_val=`cat $1 | tail -n 3 | head -n 1 | grep -Eo '[0-9]+\.[0-9]+%'`
}

# Function for uploading result of a query to Google Big Query
bqup(){
    bq mk "${BENCHMARK}"_test
    bq load --project_id="${PROJECT}" \
        --autodetect \
        --source_format=NEWLINE_DELIMITED_JSON \
        "${BENCHMARK}"_test."$2" \
        "$1"
}

# Function for pushing logs to google cloud??? 

# Result location capture 
rlc(){
    # Capture location of results file and moving to the documents
    file_val=`cat $1 | tail -n 1 | sed 's:.*/\([[:alnum:]]\{8\}\)/.*:\1:'`
    echo $file_val
    cp -r /tmp/perfkitbenchmarker/runs/${file_val}/ /mnt/c/Users/jetry/Documents/Run3/
    # Capture output location from logs to capture path of results
    bqup "/tmp/perfkitbenchmarker/runs/${file_val}/perfkitbenchmarker_results.json" "$2"
}

# MAIN FUNCTION

# Currently only accepts one benchmark at a time
# accepts space delimited list for all other field types
# only currently operates for same size lists for all field types

#b) BENCHMARK 
#m) MACHINETYPE 
#c) CLOUD 
#z) ZONE 

# Capture CMDLine Args
while getopts b:m:c:z: flag
do
    case "${flag}" in
        b) BENCHMARK=${OPTARG};;
        m) MACHINETYPE=${OPTARG};;
        c) CLOUD=${OPTARG};;
        z) ZONE=${OPTARG};;
    esac
done

# Make Log dir
mkdir "${BENCHMARK}_logs_${DT}"

# Num iterator
NUM=0

for cloud in ${CLOUD}
do
    read -a cloud_machines <<< ${MACHINETYPE}
    read -a cloud_azs <<< ${ZONE}

    echo "Calling a Task on ${cloud} with ${cloud_machines[$NUM]} in ${cloud_azs[$NUM]}"

    lf "$cloud" "${cloud_machines[$NUM]}" "$DT" "${cloud_azs[$NUM]}"
    echo "Temp Log output is: ${templog}"
    logArr+=(${templog})
    bqt "$cloud" "${cloud_machines[$NUM]}" "${cloud_azs[$NUM]}"
    echo "BQ table name is: ${table_name}"
    bqtArr+=(${table_name})

    task "$cloud" "${cloud_machines[$NUM]}" "${cloud_azs[$NUM]}" >> $templog 2>&1 &
    process_id=$!
    echo "The Process ID is: ${process_id}"

    ((NUM++))
done

#wait for all background jobs to finish
wait

echo "All jobs finished"

# For loop going through all the results captured per cloud

for index in ${!logArr[*]}
do
    # Need to add check if the benchmark is a success
    echo "Capture path location from end of log file: ${logArr[$index]}"
    # IF succesful run extract location, create bqtable, and log upload
    eos "${logArr[$index]}"
    if [ "$err_val" = "0.00%" ] ; then
        echo "FAILED: ${logArr[$index]} Check logs for more information"
        continue
    fi
    rlc "${logArr[$index]}" "${bqtArr[$index]}" 
done