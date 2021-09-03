#!/bin/bash

if [ $1 == 'install' ]
then
  sudo apt-get install -y make
elif [ $1 == 'remove' ]
then
  sudo apt-get remove -y make
fi
