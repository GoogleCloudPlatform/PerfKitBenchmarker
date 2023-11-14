#!/bin/bash
# l1=("a1" "a2" "a3")
# l2=("n1" "n2" "n3")
# for ind in ${!l1[*]}; do
#     echo "Capture path location from end of log file: ${l1[$ind]} ${l2[$ind]}"
# done
while getopts u:a:f: flag
do
    case "${flag}" in
        u) username=${OPTARG};;
        a) age=${OPTARG};;
        f) fullname=${OPTARG};;
    esac
done
echo "Username: $username";
echo "Age: $age";
echo "Full Name: $fullname";