#!/bin/bash
# Short Script that will run iperf test for all specified instant types
# Requirements:
# Must have PerfkitBenchmarker Reposition on your Machine
# Must have authenticated with cloud providers and have appropriate permission  
# Must have virtual environment with python and other requirements

# delimiter for spliting arrays
IFS=' '
# Benchmark to run
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

# for item in ${!machine_type_arr[@]}; do
#     echo ${item} ${machine_type_arr[${item}]}
# done 

# Function for running benchmark
task(){
    echo "Running task with $1, $2, $3"
    echo "/home/jetryan4/PerfKit/PerfKitBenchmarker/pkb.py --cloud=$1 --benchmarks=${BENCHMARK} --machine_type=$2 --zone=$3"
    /home/jetryan4/PerfKit/PerfKitBenchmarker/pkb.py --cloud="$1" --benchmarks=${BENCHMARK} --machine_type="$2" --zone="$3"
    # sleep 5
}

# Function for Assembling Log File
lf(){
    touch "logs_$DT/${1}_${2}_output_logs_${3}"
    templog="logs_$DT/${1}_${2}_output_logs_${3}"
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
    # file_val=`cat $1 | tail -n 1 | cut -d "/" -f 5`
    file_val=`cat $1 | tail -n 1 | sed 's:.*/\([[:alnum:]]\{8\}\)/.*:\1:'`
    echo $file_val
    cp -r /tmp/perfkitbenchmarker/runs/${file_val}/ /mnt/c/Users/jetry/Documents/Run3/
    # Capture output location from logs to capture path of results
    bqup "/tmp/perfkitbenchmarker/runs/${file_val}/perfkitbenchmarker_results.json" "$2"
}

# Make Log dir
mkdir "logs_$DT"

for cloud in AWS Azure GCP
do
    read -a cloud_machines <<< ${machine_type_arr[${cloud}]}
    read -a cloud_azs <<< ${az_arr[${cloud}]}

    for NUM in {0..2}
    do
        
        if [[ $NUM -ne 1 ]]; then
            continue
        fi
        
        echo "Calling a Task on ${cloud} with ${cloud_machines[$NUM]} in ${cloud_azs[$NUM]}"
        
        lf "$cloud" "${cloud_machines[$NUM]}" "$DT" "${cloud_azs[$NUM]}"
        logArr+=(${templog})
        bqt "$cloud" "${cloud_machines[$NUM]}" "${cloud_azs[$NUM]}"
        bqtArr+=(${table_name})
        # Task Calling
        task "$cloud" "${cloud_machines[$NUM]}" "${cloud_azs[$NUM]}" >> $templog 2>&1 &
        process_id=$!
        echo $process_id

        # wait $T1_ID
        # wait $T2_ID
        # wait $T3_ID
        # cloud_machines=$(echo ${machine_type_arr[${cloud}]} | tr " " "\n")
        # for item in ${cloud_machines[@]}
        # do
        #     echo "> [$item]"
        # done
        # echo ${cloud_machines[0]}
        # echo ${cloud_azs[0]}
        # echo ${cloud_machines[1]}
        # echo ${cloud_azs[1]}
        # echo ${cloud_machines[2]}
        # echo ${cloud_azs[2]}
        # break
    done
    # if [ "$cloud" = "Azure" ] ; then
    #     break
    # fi
done

#wait for all background jobs to finish
wait

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

# Sending all 
# /home/jetryan4/PerfKit/PerfKitBenchmarker/pkb.py --helpmatch=iperf