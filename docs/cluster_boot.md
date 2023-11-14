# Cluster Boot Test

## How the test works
The cluster_boot benchmark in the perfkitbenchmarker repository is a tool used to measure the time it takes to boot up and initialize a cluster of virtual machines (VMs) in a cloud environment. It simulates the process of starting multiple VM instances, waiting for them to become available, and then verifying their readiness. The benchmark measures the time it takes for the VMs to become fully operational, including the time required for provisioning, network configuration, and any additional initialization tasks. 

## Configuration options and defaults
To access the help for this test, run the following command  
```./pkb.py --helpmatch=cluster_boot```  

perfkitbenchmarker.linux_benchmarks.cluster_boot_benchmark:
```
  --cluster_boot_callback_external_ip: External ip address to use for collecting first egress/ingress packet, requires
    installation of tcpdump on the runner and tcpdump port is reachable from the VMs.
    (default: '')
  --cluster_boot_callback_internal_ip: Internal ip address to use for collecting first egress/ingress packet, requires
    installation of tcpdump on the runner and tcpdump port is reachable from the VMs.
    (default: '')
  --[no]cluster_boot_linux_boot_metrics: Collect detailed linux boot metrics.
    (default: 'false')
  --cluster_boot_tcpdump_port: Port the runner uses to test VM network connectivity. By default, pick a random unused port.
    (default: '0')
    (an integer)
  --[no]cluster_boot_test_port_listening: Test the time it takes to successfully connect to the port that is used to run the
    remote command.
    (default: 'false')
  --[no]cluster_boot_time_reboot: Whether to reboot the VMs during the cluster boot benchmark to measure reboot performance.
    (default: 'false')
```
### See example configuration here: 


## Metrics captured
Time to Create Async Return (seconds):  
The time taken for the cluster creation request to return an asynchronous response, measured in seconds.

Time to Running (seconds):  
The time taken for the cluster to transition from the creation state to the running state, measured in seconds.

Time to SSH - External (seconds):  
The time taken to establish an SSH connection with the cluster's external IP address, measured in seconds.

Boot Time (seconds):  
The total time taken for the cluster to boot up and become fully operational, measured in seconds.

Cluster Boot Time (seconds):  
The boot time specifically for the cluster, indicating the time required for all the nodes in the cluster to be ready, measured in seconds.

End to end Runtime (seconds): 
The total runtime of the test from initiation to (teardown complete?)
