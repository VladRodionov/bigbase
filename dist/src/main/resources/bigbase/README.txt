BigBase - read optimized, fully HBase-compatible, NoSQL Data Store

Q: What is BigBase?

BigBase is a read-optimized version of HBase NoSQL data store and is FULLY, 100% HBase compatible.
100% compatibility means that the upgrade from HBase to BigBase and other way around does not 
involve data migration and even can be made without stopping the cluster (via rolling restart). 

Q: What do you mean by 'read-optimized'?

By 'read-optimized' we mean that general purpose of BigBase is to improve both read operations performance
and read operations latency distribution.

Q: How do you achieve this?

By introducing multi-level caching to HBase. L1 - row cache, L2 - block cache in RAM and L3 - disk cache on SSD (***).  

Q: What HBase versions are supported?

HBase 0.94.x onwards (0.94, 0.96, 0.98). You may try 0.92 as well, but this version has not been tested with BigBase.
Note: BigBase block cache is not compatible with HBase 0.96+ but row cache cacn be used with any HBase 0.94 onwards.
Fulle support for 0.96+ is scheduled for the next BigBase release (1.1.0)

Q: Can I upgrade existing HBase cluster, what are requirements and how long will this upgrade take?

Yes, you can upgrade existing HBase 0.94+ cluster. Actually, this is the only option we provide right now.
The time varies. You will need to stop the cluster, install new binaries and start cluster again. 
This will take from several minutes to maximum half an hour depending on the cluster size and skill 
level of an upgrade operator. 

Q: I do not have HBase cluster can I try BigBase?

Currently, we do not provide BigBase as a full HBase distributive, you will need to install HBase first. 
Go to hbase.apache.org for download and installation instructions. 

Q: What is the difference between BigBase and 'vanilla' version of HBase (0.94, 0.96, 0.98)?

There are two major new features in BigBase, which are either missing in HBase or have sub-optimal implementation: 
off heap RowCache and off heap BlockCache. RowCache is designed after original BigTable's scan cache. It caches
hot rows data and does it in off heap memory. Read ROWCACHE.txt for more information. Another major new feature is 
off heap BlockCache. HBase 0.96 onwards has already off heap block cache (which is called bucket cache), but BigBase's 
block cache implementation is far superior in several aspects: scalability, latency and performance, besides this, 
off heap BlockCache is supported in BigBase 0.94 onwards and 'vanilla' HBase does not support off heap block cache in 0.94.x.
    

BLOCK-CACHE. 

BigBase BlockCache vs. HBase bucket cache (0.96+)

# HBase versions supported:

BigBase 1.0.0 - 0.94.x (0.96+ in 1.1.0) 
HBase   - 0.96.x onwards

# Off Heap:

BigBase - all data (keys and values) are stored off Java heap.
HBase   - keys are stored on Java heap. One key overhead is ~ 200 bytes.

# Compression:

BigBase - Yes, Snappy and LZ4, LZ4, Deflate codecs are supported
HBase   - No. 

Depending on an application, data block compression ratio varies between 2 and 4, on average. BigBase allows to keep 2-4x time
more data in a cache.

# Disk (SSD) cache support (1.1.0 release). 

Both, BigBase and HBase supports cache on disk mode to fully utilize fast solid state drives as a data storage for cached blocks
of data, but there is significant difference in implementations.

A. BigBase cache is SSD-friendly, it writes data to SSD only in sequential mode with write amplification close to 1.
   HBase bucket cache writes data in random order, with write amplification >> 1, this results in excessive SSD wearing and
   poor read/write operation latencies consistency (99% of latency is quite high).
B. BigBase file cache bypasses OS page cache for both: read and writes. 
C. As we already mentioned, HBase bucket cache implementation keeps block keys in Java heap. This limits SSD cache scalability by
   available JVM heap. 
   
# Hybrid cache (RAM + SSD) support (1.1.0)

BigBase - Yes
HBase   - No, either on RAM or on disk        

# Persistence.

Both implementation support storing block cache meta data upon region server shutdown and loading this data upon server start up.
This allows to avoid cold start effect when data is not available after server restart, but BigBase supports persistence for transient 
(in memory) caches as well for both: row and block cache.

# Periodic snapshots

BigBase - Yes ( 1.1.0)
HBase - No

Snapshots allows to survive unexpected Region Server crash. When server starts after crash or after not clean shutdown, 
the last snapshot will be loaded and access to the cached data on disk will be restored.

# Low latency optimization

BigBase - Yes. OffHeap block cache does not use JVM heap at all, is SSD - friendly, bypasses OS page cache on read/write,
          evicts data in a real - time (Real-Time data eviction)
HBase   - No.  Bucket cache use JVM heap for keys, not SSD- friendly, pollutes OS page cache, evicts data in batches.

# Scalability 

BigBase  - High. 100s GB of RAM and terrabytes of SSDs
HBase    - Medium. Each data block in a cache consumes ~200 byte on Java heap. This limits bucket cache's scalability. 


### BlockCache Summary

# HBase Bucket Cache (0.96+)

* Poor scalability. 

HBase keeps cache keys on heap (~ 200 bytes per one key). The practical limit is 50M keys or blocks 
(due to Java heap pressure). This results in a limited external cache size. We can safely say that 500GB (may be even less) 
is a practical limit on HBase 0.96 disk cache size.

* Sub-optimal performance. 

HBase 0.96 does not support off heap cache in memory for hot data set and disk cache for 
cold data set at the same time - either cache is off heap or disk based (SSD). This may results in a sub optimal cache performance. 

* No HA and true persistence.  

HBase 0.96 cache is semi-transient and does not survive RS crash.

* Not for low latency applications. 

As since large portion of a cache data HBase keeps in Java heap - there is increased GC - related latency spikes 
which are unpredictable and can be very high and frequent. Another feature (batched eviction) contribute to latency spikes: 
it takes ~1 sec per 1M blocks in cache to execute eviction operation. 

* Not SSD friendly. 

Write operations are random. This increases SSD wearing, decreases endurance, reduces performance and increases latencies 
in high percentiles: 99% and above. 
 
# BigBase BlockCache

* Excellent scalability. 

BigBase keeps all data off Java heap and there is no any Java GC - related overhead associated with off-heap cache. 
The block overhead for disk-based cache is < 60 bytes. The practical limit for disk (SSD) cache is 16M blocks per 1 GB of RAM. 
Modern server (256GB) can handle   > 4B blocks of data (> 20TB-1PB of cache).

* Optimal performance. 

BigBase supports off heap cache in memory for hot data set and disk (network) cache for cold data set at the same time. 
This results in optimal cache performance. 

* HA and true persistence.  
BigBase can store/load block cache data and makes periodic snapshots of disk/network - based cache metadata.

* Low latency optimized. 

BigBase  keeps all the data off Java heap and is not subjected to Java GC. Real-time (continuous) cache data eviction 
has very low overhead and does not introduce any significant latency spikes.

* SSD friendly. 

There is no random write IO. All writes are sequential. This makes BigBase block cache SSD friendly and operation latencies
 are more consistent and predictable.


Read ROWCACHE.txt doc file on RowCache design and features.

(***) L3 cache (SSD) is going to be supported in the next release of BigBase (1.1.0). The feature has been implemented but not tested yet. 

 

 





