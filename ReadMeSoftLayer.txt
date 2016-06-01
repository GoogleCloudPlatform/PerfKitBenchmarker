
1) Install SoftLayer requirements
	sudo pip install -r requirements-softlayer.txt
	
2) Run SoftLayer setup program
	slcli setup

3) To view system create options used below issue the following commands
   slcli vs  create-options
   
3) Run benchmarks

Examples:
Simple benchmark with defaults 
>python pkb.py --cloud=SoftLayer --benchmarks=iperf

Benchmark with 1Gib Nic card specified
>python pkb.py --cloud=SoftLayer --benchmarks=ping  --machine_type="{ \"nic\": 1000}"

Benchmark with the Toronto 1 datacenter specified and machine type 4 cpus, 4G memory, Redhat OS, 1Gib NIC card
>python pkb.py --cloud=SoftLayer --benchmarks=iperf --zones=tor01 --machine_type="{\"cpus\": 4, \"memory\": 4096, \"os\": \"REDHAT_LATEST_64\", \"nic\": 1000}"

The Redis Benchmark with a Redis parameter and datacenter specified
>python pkb.py --cloud=SoftLayer --benchmarks=redis --redis_clients=2  --zones=tor01

A private & public VLAN id specified to ensure VMs are located on the same vlan. 
VLAN ids can be queried with: slcli vlan list
The risk of specifing a VLAN is there is a chance no resources will be available for that VLAN 
>python pkb.py --cloud=SoftLayer --benchmarks=ping  --machine_type="{ \"nic\": 1000, \"public_vlan_id\": 1205613, \"private_vlan_id\": 1205615}"

Storage benchmark with SAN attached
>python pkb.py --cloud=SoftLayer --benchmarks=fio  --machine_type="{ \"nic\": 1000 \"san\": "true" }" 

Disk IO benchmark
>python pkb.py --cloud=SoftLayer --benchmarks=bonnie++ --zones=tor01 --machine_type="{\"cpus\": 4, \"memory\": 4096, \"nic\": 1000}"


>python pkb.py --cloud=SoftLayer --benchmarks=unixbench --machine_type="{\"cpus\": 4, \"memory\": 4096, \"nic\": 1000}"

Windows (in progress)
>python pkb.py --cloud=SoftLayer --default_timeout=2400 --benchmarks=ntttcp --os_type=windows --zones=tor01 --machine_type="{\"cpus\": 4, \"memory\": 4096, \"nic\": 1000}"

A benchmark with two zones
>python pkb.py --cloud=SoftLayer --benchmarks=iperf --zones=tor01,wdc04
