FAQ - Frequently Asked Questions
======

### Q: Cache statistics. Where can I find BigBase block and row caches statistics.

Block cache statistics is available through JMX. You can use any JMX client to connect to Region Server or you can find it on 
Region Server's UI page: http://server:60030.

Both block cache and row cache log periodically full stats into Region Server log file. Look for 'BigBaseBlockCache.StatristicsThread' 
and `BigBaseRowCache.StatisticsThread`.

### Q: My region server throws exception: java.lang.OutOfMemoryError: Direct buffer memory after upgrade.

You  are hitting direct memory limit per JVM process because of a very large (> 200) number of handler threads. BigBase requires 2.5 MB of Direct memory 
per one handler thread. The number of threads in your HBase  Region Server handler pool is specified by "hbase.regionserver.handler.count" configuration parameter. 
All JVMs have limit on maximum size of Direct memory which can be allocated by application. Oracle JVM sets this limit to max heap size (-Xmx),
some others (OpenJDK?) have a predefined value .

You can try increasing Direct memory limit, using the following config parameter:
```
$ java -XX:MaxDirectMemorySize=2G, for example. 
```
Calculate your own max size base on the handler count.

### Q: What is the recommended value for 'heap.blockcache.ratio' configuration parameter

BigBase block cache keeps all non-DATA blocks on Java heap (if enabled). The rule of thumb to have at least 2% of total off heap
memory to be allocated for on heap cache. For example:

Total off heap size: offheap.blockcache.size = 30G
Max JVM heap size  : 5G

You will need `0.6G (0.02 * 30G)` for on heap cache, therefore you will need specify:
`heap.blockcache.ratio = 0.6G/5G = 0.12` (this is the fraction of max JVM heap - not of off heap block cache size)

The larger your  blocks - the smaller on heap cache you will need. 
 
`BigBaseBlockCache.StatisticsThread` logs stats separately for off heap and on heap caches. Check your RS log file and
adjust on heap block cache size accordingly. Ideally, you will need close to 100% of hitRate for on heap block cache.

### Q: What eviction algorithms are used in BigBase caches?

The eviction algorithm in both caches : **Fast LRU** (Least Recently Used). We will add support for other (LFU, FIFO, LRU-2Q) in 
the next release. 

### Q: Is BigBase block cache scan resistant?

No. You need explicitly set cache blocks to 'false' on Scan instance if you want to avoid cache pollution. See HBase API documentation. 
Scan-resistance eviction will be implemented in the next release of BigBase. The upside of not having scan-resistant cache is it
is much easier to pre-cache data in BigBase - just execute Scan operation with block caching enabled.

### Q: Do you support IN_MEMORY tables?

There is no special treatment for in memory tables in BigBase. The cache itself takes care of optimal data caching.
We will add scan-resistance and in memory tables support, as an option, in the next BigBase release (1.1.0).

### Q: What Java version do you recommend?

BigBase has been tested under Java 6, but should work with Java 7 as well. For optimal performance we recommend Oracle JDK 6/7.
OpenJDK has a bug in `sun.misc.Unsafe` class which affects BigBase performance (but not functionality) in some cases.

### Q: Which versions of HBase are supported?

BigBase 1.0.0 has been tested with HBase 0.94.x, but should work with 0.92.x as well. The BigBase block cache is not compatible with HBase 0.96+ yet, but 
row cache can be used with HBase 0.96.x,98.x and trunk (1.0) versions. 

Full support for 0.96+ is scheduled for the next release (1.1.0). 
 

 
