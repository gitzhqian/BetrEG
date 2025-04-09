# BetrEngine

BetrEngine is a testbed for multi-version in-memory storage engine, designed to facilitate research and experimentation with core DBMS techniques.

## ✨ Features

- ✅ Supports **multiple concurrency control algorithms**
- ✅ Implements **heap-organized** and **index-organized** version storage schemes
- ✅ Enables **version prefetching** for read performance optimization
- ✅ Benchmarks included:
  - [YCSB](https://github.com/brianfrankcooper/YCSB) benchmark implementation
  - Full [TPC-C](http://www.tpc.org/tpcc/) benchmark implementation

This testbed is based on the DBx1000 system, whose concurrency control scalability study can be found in the following paper:

[1] Xiangyao Yu, George Bezerra, Andrew Pavlo, Srinivas Devadas, Michael Stonebraker, [Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores](http://www.vldb.org/pvldb/vol8/p209-yu.pdf), VLDB 2014
    
    
    
Build & Test
------------

To build the database.

    make -j

To test the database

    python test.py
    
Configuration
-------------

DBMS configurations can be changed in the config.h file. Please refer to README for the meaning of each configuration. Here we only list several most important ones. 

    THREAD_CNT        : Number of worker threads running in the database.
    WORKLOAD          : Supported workloads include YCSB and TPCC
    CC_ALG            : Concurrency control algorithm. Seven algorithms are supported 
                        (DL_DETECT, NO_WAIT, HEKATON, SILO, TICTOC, MVCC, WOUND_WAIT, REBIRTH_RETIRE) 
    MAX_TXN_PER_PART  : Number of transactions to run per thread per partition.
                        
Configurations can also be specified as command argument at runtime. Run the following command for a full list of program argument. 
    
    ./rundb -g 

Run
---

The DBMS can be run with 

    ./rundb

 
