#!/bin/bash
echo 'Executing $0'
echo 'Copying job jar file $1 into VM...'
gsutil cp "$1" ./job.jar
echo 'Finished copying job jar file.'
