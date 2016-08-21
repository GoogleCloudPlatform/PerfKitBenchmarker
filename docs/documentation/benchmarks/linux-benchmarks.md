# Linux Benchmarks

## System
---
Benchmark | Name | Description
----------|------|-----------
Cluster Boot | `cluster_boot` | Records the time required to boot a cluster of VMs.
UnixBench | `unixbench` | Unix bench is a holistic performance benchmark, measuing CPU performance,some memory bandwidth, and disk.

## CPU
---
Benchmark | Name | Description
----------|------|-----------
SPEC CPU2006 | `speccpu2006` | The SPEC CPU 2006 benchmark is SPEC's next-generation, industry-standardized, CPU-intensive benchmark suite, stressing a system's processor, memory subsystem and compiler.
COREMARK | `coremark` | CoreMark's primary goals are simplicity and providing a method for benchmarking only a processor's core features.
Scimark2 | `scimark2` | SciMark2 is a Java (and C) benchmark for scientific and numerical computing. It measures several computational kernels and reports a composite score in approximate Mflops (Millions of floating point operations per second).

## Memory
---
Benchmark | Name | Description
----------|------|-----------
MultiChase | `multichase` |  pointer chaser benchmark. It measures the average latency of pointer-chase operations.
SILO | `silo` | Silo is a high performance, scalable in-memory database for modern multicore machines.


## Storage
---
Benchmark | Name | Description
----------|------|-----------
SPEC SFS2014 | `specsfs2014` | The SPEC SFS® 2014 benchmark is the latest version of the Standard Performance Evaluation Corporation benchmark suite measuring file server throughput and response time, providing a standardized method for comparing performance across different vendor platforms.
Block Storage | `block_storage_workloads` | Runs fio benchmarks to simulate logging, database and streaming.
Flexible I/O | `fio` | Run fio benchmarks
Copy Throughput | `copy_throughput` | cp and dd between two attached disks on same vm. scp copy across different vms using external networks.

## Network
---
Benchmark | Name | Description
----------|------|-----------
Mesh Network | `mesh_network` | Runs TCP_RR, TCP_STREAM benchmarks from netperf and compute total throughput and average latency inside mesh network.
Iperf | `iperf` | Runs Iperf to collect network throughput.
NetPerf | `netperf` | Runs TCP_RR, TCP_CRR, and TCP_STREAM benchmarks from netperf across two machines.
ping | `ping` | Runs ping using the internal IP addresses of VMs in the same zone.


## Web Workloads
---
Benchmark | Name | Description
----------|------|-----------
OLDISIM | `oldisim` | oldisim is a framework to support benchmarks that emulate Online Data-Intensive (OLDI) workloads, such as web search and social networking. oldisim includes sample workloads built on top of this framework.
Tomcat / Wrk | `tomcat_wrk` | Run wrk against a simple Tomcat web server.

## EFPL CloudSuite
For more info see: [CloudSuite Benchmarks](http://cloudsuite.ch/benchmarks/)

Benchmark | Name | Description
----------|------|-----------
Data Caching | `cloudsuite_data_caching` | Runs the Data Caching benchmark.
Data Serving | `cloudsuite_data_serving` | Runs the Data Serving benchmark.
Graph Analytics | `cloudsuite_graph_analytics` | Runs the Graph Analytics Benchmark.
In-Memory Analytics | `cloudsuite_in_memory_analytics` | Runs the In-Memory Analytics benchmark.
Media Streaming | `cloudsuite_media_streaming` | Runs the media streaming benchmark.
Web Search | `cloudsuite_web_search` | Runs the web search benchmark.
Web Serving | `cloudsuite_web_serving` | Runs the web serving benchmark.

## NoSQL Workloads
---

Benchmark | Name | Description
----------|------|-----------
Aerospike | `aerospike_ycsb` | Runs YCSB against Aerospike.
Cassandra | `cassandra_ycsb` | Runs YCSB against Cassandra.
HBase | `hbase_ycsb` | Runs YCSB against HBase.
MongoDB | `mongodb_ycsb` | Runs YCSB against MongoDB.
Redis | `redis_ycsb` | Runs YCSB against Redis.


## [Big] Data
---
Benchmark | Name | Description
----------|------|-----------
Hadoop TeraSort | `hadoop_terasort` | Runs TeraSort on Hadoop.
Sysbench OLTP | `sysbench_oltp` | Runs sysbench --oltp.


## [Big] Compute
---
Benchmark | Name | Description
----------|------|-----------
HPCC | `hpcc` | Runs HPC Challenge benchmarks.

## Services
---
Benchmark | Name | Description
----------|------|-----------
Cloud BigTable | `cloud_bigtable_ycsb` | Runs YCSB against Google Cloud BigTable.
Cloud Datastore | `cloud_datastore_ycsb` | Runs YCSB against Google Cloud Datastore.
Object Storage | `object_storage_service` | Object Storage benchmarks.
MySQL Service | `mysql_service` | This is a set of benchmarks that measures performance of MySQL Databases on managed MySQL services.
