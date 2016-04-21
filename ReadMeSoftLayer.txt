
1) Install SoftLayer requirements
	sudo pip install -r requirements-softlayer.txt

2) Run SoftLayer setup program
	slcli setup


3) Run benchmarks
Examples:
>python pkb.py --cloud=SoftLayer --benchmarks=iperf
>python pkb.py --cloud=SoftLayer --benchmarks=iperf --zone=tor01 --machine_type="{\"cpus\": 4, \"memory\": 4096, \"os\": \"REDHAT_LATEST_64\", \"nic\": 1000}"
>python pkb.py --cloud=SoftLayer --benchmarks=redis --redis_clients=2  --zone=tor01