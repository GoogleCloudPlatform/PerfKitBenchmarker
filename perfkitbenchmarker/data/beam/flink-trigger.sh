#!/bin/bash
while getopts c: flag
do
    case "${flag}" in
        c) classname=${OPTARG};;
        --) shift
        break;;
    esac
done
echo "Executing $0"
command="flink run -c ${classname} job.jar --runner=FlinkRunner"
space=" "
for arg in ${*:4}
do
    command=${command}${space}${arg}
done
echo "Running flink job with classname $classname and args '${*:4}'..."
eval "$command"
echo "Flink job done."
