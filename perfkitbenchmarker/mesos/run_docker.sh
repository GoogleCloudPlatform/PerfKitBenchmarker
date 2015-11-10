#!/bin/bash

function cleanup {{
  docker rm {0};
  {5} 
}}

trap cleanup SIGKILL SIGSTOP SIGHUP SIGINT SIGTERM;

DOCKER_CMD="/bin/mkdir /root/.ssh \n 
(echo {2}) >> /root/.ssh/authorized_keys \n /usr/sbin/sshd -D"

{3}
docker run --name {0} -p $PORT0:22 {4} {1} bash -c "$DOCKER_CMD";
