#!/bin/bash

if [ $1 == 'install' ]
then
  sudo yum install -y make
elif [ $1 == 'remove' ]
then
  sudo yum erase -y make
fi
