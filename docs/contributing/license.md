# Licensing

PerfKit Benchmarker provides wrappers and workload definitions around popular benchmark tools. We made it very simple
to use and automate everything we can.  It instantiates VMs on the Cloud provider of your choice, automatically
installs benchmarks, and runs the workloads without user interaction.

Due to the level of automation you will not see prompts for software installed as part of a benchmark run.
Therefore you must accept the license of each of the benchmarks individually, and take responsibility for using them
before you use the PerfKit Benchmarker.

In its current release these are the benchmarks that are executed:

  - `aerospike`: [Apache v2 for the client](http://www.aerospike.com/aerospike-licensing/)
    and [GNU AGPL v3.0 for the server](https://github.com/aerospike/aerospike-server/blob/master/LICENSE)
  - `bonnie++`: [GPL v2](http://www.coker.com.au/bonnie++/readme.html)
  - `cassandra_ycsb`: [Apache v2](http://cassandra.apache.org/)
  - `cassandra_stress`: [Apache v2](http://cassandra.apache.org/)
  - `cloudsuite3.0`: [CloudSuite 3.0 license](http://cloudsuite.ch/licenses/)
  - `cluster_boot`: MIT License
  - `coremark`: [EEMBC](https://www.eembc.org/)
  - `copy_throughput`: Apache v2
  - `fio`: [GPL v2](https://github.com/axboe/fio/blob/master/COPYING)
  - `hadoop_terasort`: [Apache v2](http://hadoop.apache.org/)
  - `hpcc`: [Original BSD license](http://icl.cs.utk.edu/hpcc/faq/#263)
  - `iperf`: [BSD license](http://iperf.sourceforge.net/)
  - `memtier_benchmark`: [GPL v2](https://github.com/RedisLabs/memtier_benchmark)
  - `mesh_network`: [HP license](http://www.calculate-linux.org/packages/licenses/netperf)
  - `mongodb`: **Deprecated**. [GNU AGPL v3.0](http://www.mongodb.org/about/licensing/)
  - `mongodb_ycsb`: [GNU AGPL v3.0](http://www.mongodb.org/about/licensing/)
  - [`multichase`](https://github.com/google/multichase):
    [Apache v2](https://github.com/google/multichase/blob/master/LICENSE)
  - `netperf`: [HP license](http://www.calculate-linux.org/packages/licenses/netperf)
  - [`oldisim`](https://github.com/GoogleCloudPlatform/oldisim):
    [Apache v2](https://github.com/GoogleCloudPlatform/oldisim/blob/master/LICENSE.txt)
  - `object_storage_service`: Apache v2
  - `ping`: No license needed.
  - `silo`: MIT License
  - `scimark2`: [public domain](http://math.nist.gov/scimark2/credits.html)
  - `speccpu2006`: [SPEC CPU2006](http://www.spec.org/cpu2006/)
  - `sysbench_oltp`: [GPL v2](https://github.com/akopytov/sysbench)
  - [`tomcat`](https://github.com/apache/tomcat):
    [Apache v2](https://github.com/apache/tomcat/blob/trunk/LICENSE)
  - [`unixbench`](https://github.com/kdlucas/byte-unixbench):
    [GPL v2](https://github.com/kdlucas/byte-unixbench/blob/master/LICENSE.txt)
  - [`wrk`](https://github.com/wg/wrk):
    [Modified Apache v2](https://github.com/wg/wrk/blob/master/LICENSE)
  - [`ycsb`](https://github.com/brianfrankcooper/YCSB) (used by `mongodb`, `hbase_ycsb`, and others):
    [Apache v2](https://github.com/brianfrankcooper/YCSB/blob/master/LICENSE.txt)

Some of the benchmarks invoked require Java. You must also agree with the following license:

  - `openjdk-7-jre`: [GPL v2 with the Classpath Exception](http://openjdk.java.net/legal/gplv2+ce.html)

[CoreMark](http://www.eembc.org/coremark/) setup cannot be automated. EEMBC requires users to agree with their terms and conditions, and PerfKit
Benchmarker users must manually download the CoreMark tarball from their website and save it under the
`perfkitbenchmarker/data` folder (e.g. `~/PerfKitBenchmarker/perfkitbenchmarker/data/coremark_v1.0.tgz`)

[SPEC CPU2006](https://www.spec.org/cpu2006/) benchmark setup cannot be
automated. SPEC requires that users purchase a license and agree with their
terms and conditions. PerfKit Benchmarker users must manually download
`cpu2006-1.2.iso` from the SPEC website, save it under the
`perfkitbenchmarker/data` folder (e.g.
`~/PerfKitBenchmarker/perfkitbenchmarker/data/cpu2006-1.2.iso`), and also
supply a runspec cfg file (e.g.
`~/PerfKitBenchmarker/perfkitbenchmarker/data/linux64-x64-gcc47.cfg`).
Alternately, PerfKit Benchmarker can accept a tar file that can be generated
with the following steps:

* Extract the contents of `cpu2006-1.2.iso` into a directory named `cpu2006`
* Run `cpu2006/install.sh`
* Copy the cfg file into `cpu2006/config`
* Create a tar file containing the `cpu2006` directory, and place it under the
  `perfkitbenchmarker/data` folder (e.g.
  `~/PerfKitBenchmarker/perfkitbenchmarker/data/cpu2006v1.2.tgz`).

PerfKit Benchmarker will use the tar file if it is present. Otherwise, it will
search for the iso and cfg files.
