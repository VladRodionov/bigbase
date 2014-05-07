BigBase
======

###Q: What is BigBase?

BigBase is a read-optimized version of HBase NoSQL data store and is FULLY, 100% HBase compatible.
100% compatibility means that the upgrade from HBase to BigBase and other way around does not 
involve data migration and even can be made without stopping the cluster (via rolling restart). 

###Q: What do you mean by 'read-optimized'?

By 'read-optimized' we mean that general purpose of BigBase is to improve both read operations performance
and read operations latency distribution.

###Q: How do you achieve this?

By introducing multi-level caching to HBase. L1 - row cache, L2 - block cache in RAM and L3 - disk cache on SSD (***).  

###Q: What HBase versions are supported?

HBase 0.94.x onwards (0.94, 0.96, 0.98). You may try 0.92 as well, but this version has not been tested with BigBase.
> **Note:** BigBase has two versions: 0.94 and 0.96+. see 'releases' section.

###Q: Can I upgrade existing HBase cluster, what are requirements and how long will this upgrade take?

Yes, you can upgrade existing HBase 0.94+ cluster. Actually, this is the only option we provide right now.
The time varies. You will need to stop the cluster, install new binaries and start cluster again. 
This will take from several minutes to maximum half an hour depending on the cluster size and skill 
level of an upgrade operator. You can do rolling upgrade as well.

###Q: I do not have HBase cluster can I try BigBase?

Currently, we do not provide BigBase as a full HBase distributive, you will need to install HBase first. 
Go to [HBase](http://hbase.apache.org) for download and installation instructions. 

###Q: What is the difference between BigBase and 'vanilla' version of HBase (0.94, 0.96, 0.98)?

There are two major new features in BigBase, which are either missing in HBase or have sub-optimal implementation: 
off heap RowCache and off heap BlockCache. RowCache is designed after original BigTable's scan cache. It caches
hot rows data and does it in off heap memory. Read [Row-Cache WiKi](https://github.com/VladRodionov/bigbase/wiki/BigBase-Row-Cache) for more information. Another major new feature is 
off heap BlockCache. HBase 0.96 onwards has already off heap block cache (which is called bucket cache), but BigBase's 
block cache implementation is far superior in several aspects: scalability, latency and performance, besides this, 
off heap BlockCache is supported in BigBase 0.94 onwards and 'vanilla' HBase does not support off heap block cache in 0.94.x.
    
### User group

https://groups.google.com/forum/#!forum/bigbase-user

### HBaseCon 2014 presentation

http://www.slideshare.net/bigbase/hbase-extreme-makeover

### License

BigBase is licensed under [Affero GPL 3](http://www.gnu.org/licenses/agpl-3.0.html).
 
(***) This feature has not been tested in 1.0.0 and will be available in the next 1.1.0 release. 

 





