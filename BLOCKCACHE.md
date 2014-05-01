##BigBase BlockCache vs. HBase bucket cache. 

### HBase versions supported:

* BigBase 1.0.0 - 0.94.x (0.96+ in 1.1.0). Row Cache supports 0.94+. 
* HBase   - 0.96.x onwards

### Off Heap:

* BigBase - all data (keys and values) are stored off Java heap.
* HBase   - keys are stored on Java heap. One key overhead is ~ 200 bytes.

### Compression:

* BigBase - Yes, Snappy and LZ4, LZ4, Deflate codecs are supported
* HBase   - No. 

Depending on an application, data block compression ratio varies between 2 and 4, on average. BigBase allows to keep 2-4x time
more data in a cache.

### Disk (SSD) cache support (1.1.0 release). 

Both, BigBase and HBase supports cache on disk mode to fully utilize fast solid state drives as a data storage for cached blocks
of data, but there is significant difference in implementations.

* BigBase cache is SSD-friendly, it writes data to SSD only in sequential mode with write amplification close to 1. HBase bucket cache writes data in random order, with write amplification >> 1, this results in excessive SSD wearing andpoor read/write operation latencies consistency (99% of latency is quite high).
* BigBase file cache bypasses OS page cache for both: read and writes. 
* As we already mentioned, HBase bucket cache implementation keeps block keys in Java heap. This limits SSD cache scalability by available JVM heap. 
   
### Hybrid cache (RAM + SSD) support (1.1.0)

* BigBase - Yes
* HBase   - No, either on RAM or on disk        

### Persistence.

Both implementation support storing block cache meta data upon region server shutdown and loading this data upon server start up.
This allows to avoid cold start effect when data is not available after server restart, but BigBase supports persistence for transient 
(in memory) caches as well for both: row and block cache.

### Periodic snapshots

* BigBase - Yes ( 1.1.0)
* HBase - No

Snapshots allows to survive unexpected Region Server crash. When server starts after crash or after not clean shutdown, 
the last snapshot will be loaded and access to the cached data on disk will be restored.

### Low latency optimization

* BigBase - Yes. OffHeap block cache does not use JVM heap at all, is SSD - friendly, bypasses OS page cache on read/write,
          evicts data in a real - time (Real-Time data eviction)
* HBase   - No.  Bucket cache use JVM heap for keys, not SSD- friendly, pollutes OS page cache, evicts data in batches.

### Scalability 

* BigBase  - High. 100s GB of RAM and terrabytes of SSDs
* HBase    - Medium. Each data block in a cache consumes ~200 byte on Java heap. This limits bucket cache's scalability. 


## BlockCache Summary

### HBase Bucket Cache (0.96+)

* **Poor scalability**. HBase keeps cache keys on heap (~ 200 bytes per one key). The practical limit is 50M keys or blocks 
(due to Java heap pressure). This results in a limited external cache size. We can safely say that 500GB (may be even less) 
is a practical limit on HBase 0.96 disk cache size.

* **Sub-optimal performance**. HBase 0.96 does not support off heap cache in memory for hot data set and disk cache for cold data set at the same time - either cache is off heap or disk based (SSD). This may results in a sub optimal cache performance. 

* **No true persistence**.  HBase bucket cache ('offheap') is semi-transient and does not survive RS crash.

* **Not for low latency applications**. As since large portion of a cache data HBase keeps in Java heap - there is increased GC - related latency spikes 
which are unpredictable and can be very high and frequent. Another feature (batched eviction) contribute to latency spikes: 
it takes ~1 sec per 1M blocks in cache to execute eviction operation. 

* **Not SSD friendly**. Write operations are random. This increases SSD wearing, decreases endurance, reduces performance and increases latencies 
in high percentiles: 99% and above. 
 
### BigBase BlockCache

* **Excellent scalability**. BigBase keeps all data off Java heap and there is no any Java GC - related overhead associated with off-heap cache. 
The block overhead for disk-based cache is < 60 bytes. The practical limit for disk (SSD) cache is 16M blocks per 1 GB of RAM. 
Modern server (256GB) can handle   > 4B blocks of data (> 20TB-1PB of cache).

* **Optimal performance**. BigBase supports off heap cache in memory for hot data set and disk (network) cache for cold data set at the same time. 
This results in optimal cache performance. 

* **True persistence**. BigBase can store/load block cache data and makes periodic snapshots of disk/network - based cache metadata.

* **Low latency optimized**. BigBase  keeps all the data off Java heap and is not subjected to Java GC. Real-time (continuous) cache data eviction 
has very low overhead and does not introduce any significant latency spikes.

* **SSD friendly**. There is no random write IO. All writes are sequential. This makes BigBase block cache SSD friendly and operation latencies
 are more consistent and predictable.


Read ROWCACHE.txt doc file on RowCache design and features.

(***) L3 cache (SSD) is going to be supported in the next release of BigBase (1.1.0). The feature has been implemented but not tested yet. 

 

 





