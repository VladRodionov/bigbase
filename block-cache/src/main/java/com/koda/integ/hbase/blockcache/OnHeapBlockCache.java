/*******************************************************************************
* Copyright (c) 2013 Vladimir Rodionov. All Rights Reserved
*
* This code is released under the GNU Affero General Public License.
*
* See: http://www.fsf.org/licensing/licenses/agpl-3.0.html
*
* VLADIMIR RODIONOV MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY
* OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
* IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR
* NON-INFRINGEMENT. Vladimir Rodionov SHALL NOT BE LIABLE FOR ANY DAMAGES SUFFERED
* BY LICENSEE AS A RESULT OF USING, MODIFYING OR DISTRIBUTING THIS SOFTWARE OR
* ITS DERIVATIVES.
*
* Author: Vladimir Rodionov
*
*******************************************************************************/
package com.koda.integ.hbase.blockcache;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CachedBlock;
import org.apache.hadoop.hbase.io.hfile.CachedBlockQueue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.util.StringUtils;


 
// TODO: Auto-generated Javadoc
/**
 * The Class OnHeapBlockCache.
 */
public class OnHeapBlockCache implements BlockCache, HeapSize {

  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(OnHeapBlockCache.class);

  /** The Constant LRU_MIN_FACTOR_CONFIG_NAME. */
  static final String LRU_MIN_FACTOR_CONFIG_NAME = "hbase.lru.blockcache.min.factor";
  
  /** The Constant LRU_ACCEPTABLE_FACTOR_CONFIG_NAME. */
  static final String LRU_ACCEPTABLE_FACTOR_CONFIG_NAME = "hbase.lru.blockcache.acceptable.factor";

  /** Default Configuration Parameters. */

  /** Backing Concurrent Map Configuration */
  static final float DEFAULT_LOAD_FACTOR = 0.75f;
  
  /** The Constant DEFAULT_CONCURRENCY_LEVEL. */
  static final int DEFAULT_CONCURRENCY_LEVEL = 16;

  /** Eviction thresholds. */
  static final float DEFAULT_MIN_FACTOR = 0.75f;
  
  /** The Constant DEFAULT_ACCEPTABLE_FACTOR. */
  static final float DEFAULT_ACCEPTABLE_FACTOR = 0.85f;

  /** Priority buckets. */
  static final float DEFAULT_SINGLE_FACTOR = 0.25f;
  
  /** The Constant DEFAULT_MULTI_FACTOR. */
  static final float DEFAULT_MULTI_FACTOR = 0.50f;
  
  /** The Constant DEFAULT_MEMORY_FACTOR. */
  static final float DEFAULT_MEMORY_FACTOR = 0.25f;


  /** Concurrent map (the cache). */
  private final ConcurrentHashMap<BlockCacheKey,CachedBlock> map;

  /** Eviction lock (locked when eviction in process). */
  private final ReentrantLock evictionLock = new ReentrantLock(true);

  /** Volatile boolean to track if we are in an eviction process or not. */
  private volatile boolean evictionInProgress = false;

  /** Eviction thread. */
  private final EvictionThread evictionThread;


  /** Current size of cache. */
  private final AtomicLong size;

  /** Current number of cached elements. */
  private final AtomicLong elements;

  /** Cache access count (sequential ID). */
  private final AtomicLong count;

  /** Cache statistics. */
  protected final CacheStats stats;

  /** Maximum allowable size of cache (block put if size > max, evict). */
  private long maxSize;

  /** Approximate block size. */
  private long blockSize;

  /** Acceptable size of cache (no evictions if size < acceptable). */
  private float acceptableFactor;

  /** Minimum threshold of cache (when evicting, evict until size < min). */
  private float minFactor;

  /** Single access bucket size. */
  private float singleFactor;

  /** Multiple access bucket size. */
  private float multiFactor;

  /** In-memory bucket size. */
  private float memoryFactor;

  /** Overhead of the structure itself. */
  private long overhead;

  /**
   * Default constructor.  Specify maximum size and expected average block
   * size (approximation is fine).
   *
   * <p>All other factors will be calculated based on defaults specified in
   * this class.
   * @param maxSize maximum size of cache, in bytes
   * @param blockSize approximate size of each block, in bytes
   * @param conf configuration
   */
  public OnHeapBlockCache(long maxSize, long blockSize, Configuration conf) {
    this(maxSize, blockSize, true, conf);
  }

  /**
   * Constructor used for testing.  Allows disabling of the eviction thread.
   *
   * @param maxSize the max size
   * @param blockSize the block size
   * @param evictionThread the eviction thread
   * @param conf the conf
   */
  public OnHeapBlockCache(long maxSize, long blockSize, boolean evictionThread, Configuration conf) {
    this(maxSize, blockSize, evictionThread,
        (int)Math.ceil(1.2*maxSize/blockSize),
        DEFAULT_LOAD_FACTOR, 
        DEFAULT_CONCURRENCY_LEVEL,
        conf.getFloat(LRU_MIN_FACTOR_CONFIG_NAME, DEFAULT_MIN_FACTOR), 
        conf.getFloat(LRU_ACCEPTABLE_FACTOR_CONFIG_NAME, DEFAULT_ACCEPTABLE_FACTOR), 
        DEFAULT_SINGLE_FACTOR, 
        DEFAULT_MULTI_FACTOR,
        DEFAULT_MEMORY_FACTOR);
  }


  /**
   * Configurable constructor.  Use this constructor if not using defaults.
   * @param maxSize maximum size of this cache, in bytes
   * @param blockSize expected average size of blocks, in bytes
   * @param evictionThread whether to run evictions in a bg thread or not
   * @param mapInitialSize initial size of backing ConcurrentHashMap
   * @param mapLoadFactor initial load factor of backing ConcurrentHashMap
   * @param mapConcurrencyLevel initial concurrency factor for backing CHM
   * @param minFactor percentage of total size that eviction will evict until
   * @param acceptableFactor percentage of total size that triggers eviction
   * @param singleFactor percentage of total size for single-access blocks
   * @param multiFactor percentage of total size for multiple-access blocks
   * @param memoryFactor percentage of total size for in-memory blocks
   */
  public OnHeapBlockCache(long maxSize, long blockSize, boolean evictionThread,
      int mapInitialSize, float mapLoadFactor, int mapConcurrencyLevel,
      float minFactor, float acceptableFactor,
      float singleFactor, float multiFactor, float memoryFactor) {
    if(singleFactor + multiFactor + memoryFactor != 1) {
      throw new IllegalArgumentException("Single, multi, and memory factors " +
          " should total 1.0");
    }
    if(minFactor >= acceptableFactor) {
      throw new IllegalArgumentException("minFactor must be smaller than acceptableFactor");
    }
    if(minFactor >= 1.0f || acceptableFactor >= 1.0f) {
      throw new IllegalArgumentException("all factors must be < 1");
    }
    this.maxSize = maxSize;
    this.blockSize = blockSize;
    map = new ConcurrentHashMap<BlockCacheKey,CachedBlock>(mapInitialSize,
        mapLoadFactor, mapConcurrencyLevel);
    this.minFactor = minFactor;
    this.acceptableFactor = acceptableFactor;
    this.singleFactor = singleFactor;
    this.multiFactor = multiFactor;
    this.memoryFactor = memoryFactor;
    this.stats = new CacheStats();
    this.count = new AtomicLong(0);
    this.elements = new AtomicLong(0);
    this.overhead = calculateOverhead(maxSize, blockSize, mapConcurrencyLevel);
    this.size = new AtomicLong(this.overhead);
    if(evictionThread) {
      this.evictionThread = new EvictionThread(this);
      this.evictionThread.start(); // FindBugs SC_START_IN_CTOR
    } else {
      this.evictionThread = null;
    }
  }

  /**
   * Sets the max size.
   *
   * @param maxSize the new max size
   */
  public void setMaxSize(long maxSize) {
    this.maxSize = maxSize;
    if(this.size.get() > acceptableSize() && !evictionInProgress) {
      runEviction();
    }
  }

  // BlockCache implementation

  /**
   * Cache the block with the specified name and buffer.
   * <p>
   * It is assumed this will NEVER be called on an already cached block.  If
   * that is done, an exception will be thrown.
   * @param cacheKey block's cache key
   * @param buf block buffer
   * @param inMemory if block is in-memory
   */
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
    CachedBlock cb = map.get(cacheKey);
    if(cb != null) {
      throw new RuntimeException("Cached an already cached block");
    }
    cb = new CachedBlock(cacheKey, buf, count.incrementAndGet(), inMemory);
    long newSize = updateSizeMetrics(cb, false);
    map.put(cacheKey, cb);
    elements.incrementAndGet();
    if(newSize > acceptableSize() && !evictionInProgress) {
      runEviction();
    }
  }

  /**
   * Cache the block with the specified name and buffer.
   * <p>
   * It is assumed this will NEVER be called on an already cached block.  If
   * that is done, it is assumed that you are reinserting the same exact
   * block due to a race condition and will update the buffer but not modify
   * the size of the cache.
   * @param cacheKey block's cache key
   * @param buf block buffer
   */
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    cacheBlock(cacheKey, buf, false);
  }

  /**
   * Helper function that updates the local size counter and also updates any
   * per-cf or per-blocktype metrics it can discern from given.
   *
   * @param cb the cb
   * @param evict the evict
   * @return the long
   * {@link CachedBlock}
   */
  protected long updateSizeMetrics(CachedBlock cb, boolean evict) {
    long heapsize = cb.heapSize();
    if (evict) {
      heapsize *= -1;
    }

    return size.addAndGet(heapsize);
  }

  /**
   * Get the buffer of the block with the specified name.
   *
   * @param cacheKey block's cache key
   * @param caching true if the caller caches blocks on cache misses
   * @param repeat Whether this is a repeat lookup for the same block
   * (used to avoid double counting cache misses when doing double-check locking)
   * @return buffer of specified cache key, or null if not in cache
   * {@see HFileReaderV2#readBlock(long, long, boolean, boolean, boolean, BlockType)}
   */
  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat) {
    CachedBlock cb = map.get(cacheKey);
    if(cb == null) {
      if (!repeat) stats.miss(caching);
      return null;
    }
    stats.hit(caching);
    cb.access(count.incrementAndGet());
    return cb.getBuffer();
  }


  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.io.hfile.BlockCache#evictBlock(org.apache.hadoop.hbase.io.hfile.BlockCacheKey)
   */
  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    CachedBlock cb = map.get(cacheKey);
    if (cb == null) return false;
    evictBlock(cb);
    return true;
  }

  /**
   * Evicts all blocks for a specific HFile. This is an
   * expensive operation implemented as a linear-time search through all blocks
   * in the cache. Ideally this should be a search in a log-access-time map.
   * 
   * <p>
   * This is used for evict-on-close to remove all blocks of a specific HFile.
   *
   * @param hfileName the hfile name
   * @return the number of blocks evicted
   */
  @Override
  public int evictBlocksByHfileName(String hfileName) {
    int numEvicted = 0;
    for (BlockCacheKey key : map.keySet()) {
      if (key.getHfileName().equals(hfileName)) {
        if (evictBlock(key))
          ++numEvicted;
      }
    }
    return numEvicted;
  }

  /**
   * Evict block.
   *
   * @param block the block
   * @return the long
   */
  protected long evictBlock(CachedBlock block) {
    map.remove(block.getCacheKey());
    updateSizeMetrics(block, true);
    elements.decrementAndGet();
    stats.evicted();
    return block.heapSize();
  }

  /**
   * Multi-threaded call to run the eviction process.
   */
  private void runEviction() {
    if(evictionThread == null) {
      evict();
    } else {
      evictionThread.evict();
    }
  }

  /**
   * Eviction method.
   */
  void evict() {

    // Ensure only one eviction at a time
    if(!evictionLock.tryLock()) return;

    try {
      evictionInProgress = true;
      long currentSize = this.size.get();
      long bytesToFree = currentSize - minSize();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Block cache LRU eviction started; Attempting to free " +
          StringUtils.byteDesc(bytesToFree) + " of total=" +
          StringUtils.byteDesc(currentSize));
      }

      if(bytesToFree <= 0) return;

      // Instantiate priority buckets
      BlockBucket bucketSingle = new BlockBucket(bytesToFree, blockSize,
          singleSize());
      BlockBucket bucketMulti = new BlockBucket(bytesToFree, blockSize,
          multiSize());
      BlockBucket bucketMemory = new BlockBucket(bytesToFree, blockSize,
          memorySize());

      // Scan entire map putting into appropriate buckets
      for(CachedBlock cachedBlock : map.values()) {
        switch(cachedBlock.getPriority()) {
          case SINGLE: {
            bucketSingle.add(cachedBlock);
            break;
          }
          case MULTI: {
            bucketMulti.add(cachedBlock);
            break;
          }
          case MEMORY: {
            bucketMemory.add(cachedBlock);
            break;
          }
        }
      }

      PriorityQueue<BlockBucket> bucketQueue =
        new PriorityQueue<BlockBucket>(3);

      bucketQueue.add(bucketSingle);
      bucketQueue.add(bucketMulti);
      bucketQueue.add(bucketMemory);

      int remainingBuckets = 3;
      long bytesFreed = 0;

      BlockBucket bucket;
      while((bucket = bucketQueue.poll()) != null) {
        long overflow = bucket.overflow();
        if(overflow > 0) {
          long bucketBytesToFree = Math.min(overflow,
            (bytesToFree - bytesFreed) / remainingBuckets);
          bytesFreed += bucket.free(bucketBytesToFree);
        }
        remainingBuckets--;
      }

      if (LOG.isDebugEnabled()) {
        long single = bucketSingle.totalSize();
        long multi = bucketMulti.totalSize();
        long memory = bucketMemory.totalSize();
        LOG.debug("Block cache LRU eviction completed; " +
          "freed=" + StringUtils.byteDesc(bytesFreed) + ", " +
          "total=" + StringUtils.byteDesc(this.size.get()) + ", " +
          "single=" + StringUtils.byteDesc(single) + ", " +
          "multi=" + StringUtils.byteDesc(multi) + ", " +
          "memory=" + StringUtils.byteDesc(memory));
      }
    } finally {
      stats.evict();
      evictionInProgress = false;
      evictionLock.unlock();
    }
  }

  /**
   * Used to group blocks into priority buckets.  There will be a BlockBucket
   * for each priority (single, multi, memory).  Once bucketed, the eviction
   * algorithm takes the appropriate number of elements out of each according
   * to configuration parameters and their relatives sizes.
   */
  private class BlockBucket implements Comparable<BlockBucket> {

    /** The queue. */
    private CachedBlockQueue queue;
    
    /** The total size. */
    private long totalSize = 0;
    
    /** The bucket size. */
    private long bucketSize;

    /**
     * Instantiates a new block bucket.
     *
     * @param bytesToFree the bytes to free
     * @param blockSize the block size
     * @param bucketSize the bucket size
     */
    public BlockBucket(long bytesToFree, long blockSize, long bucketSize) {
      this.bucketSize = bucketSize;
      queue = new CachedBlockQueue(bytesToFree, blockSize);
      totalSize = 0;
    }

    /**
     * Adds the.
     *
     * @param block the block
     */
    public void add(CachedBlock block) {
      totalSize += block.heapSize();
      queue.add(block);
    }

    /**
     * Free.
     *
     * @param toFree the to free
     * @return the long
     */
    public long free(long toFree) {
      CachedBlock cb;
      long freedBytes = 0;
      while ((cb = queue.pollLast()) != null) {
        freedBytes += evictBlock(cb);
        if (freedBytes >= toFree) {
          return freedBytes;
        }
      }
      return freedBytes;
    }

    /**
     * Overflow.
     *
     * @return the long
     */
    public long overflow() {
      return totalSize - bucketSize;
    }

    /**
     * Total size.
     *
     * @return the long
     */
    public long totalSize() {
      return totalSize;
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(BlockBucket that) {
      if(this.overflow() == that.overflow()) return 0;
      return this.overflow() > that.overflow() ? 1 : -1;
    }
  }

  /**
   * Get the maximum size of this cache.
   * @return max size in bytes
   */
  public long getMaxSize() {
    return this.maxSize;
  }

  /**
   * Get the current size of this cache.
   * @return current size in bytes
   */
  public long getCurrentSize() {
    return this.size.get();
  }

  /**
   * Get the current size of this cache.
   * @return current size in bytes
   */
  public long getFreeSize() {
    return getMaxSize() - getCurrentSize();
  }

  /**
   * Get the size of this cache (number of cached blocks).
   *
   * @return number of cached blocks
   */
  public long size() {
    return this.elements.get();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.io.hfile.BlockCache#getBlockCount()
   */
  @Override
  public long getBlockCount() {
    return this.elements.get();
  }

  /**
   * Get the number of eviction runs that have occurred.
   *
   * @return the eviction count
   */
  public long getEvictionCount() {
    return this.stats.getEvictionCount();
  }

  /**
   * Get the number of blocks that have been evicted during the lifetime
   * of this cache.
   *
   * @return the evicted count
   */
  public long getEvictedCount() {
    return this.stats.getEvictedCount();
  }

  /**
   * Gets the eviction thread.
   *
   * @return the eviction thread
   */
  EvictionThread getEvictionThread() {
    return this.evictionThread;
  }

  /*
   * Eviction thread.  Sits in waiting state until an eviction is triggered
   * when the cache size grows above the acceptable level.<p>
   *
   * Thread is triggered into action by {@link LruBlockCache#runEviction()}
   */
  /**
   * The Class EvictionThread.
   */
  static class EvictionThread extends HasThread {
    
    /** The cache. */
    private WeakReference<OnHeapBlockCache> cache;
    
    /** The go. */
    private boolean go = true;
    // flag set after enter the run method, used for test
    /** The entering run. */
    private boolean enteringRun = false;

    /**
     * Instantiates a new eviction thread.
     *
     * @param cache the cache
     */
    public EvictionThread(OnHeapBlockCache cache) {
      super(Thread.currentThread().getName() + ".LruBlockCache.EvictionThread");
      setDaemon(true);
      this.cache = new WeakReference<OnHeapBlockCache>(cache);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.util.HasThread#run()
     */
    @Override
    public void run() {
      enteringRun = true;
      while (this.go) {
        synchronized(this) {
          try {
            this.wait();
          } catch(InterruptedException e) {}
        }
        OnHeapBlockCache cache = this.cache.get();
        if(cache == null) break;
        cache.evict();
      }
    }

    /**
     * Evict.
     */
    public void evict() {
      synchronized(this) {
        this.notify(); // FindBugs NN_NAKED_NOTIFY
      }
    }

    /**
     * Shutdown.
     */
    void shutdown() {
      this.go = false;
      interrupt();
    }

    /**
     * Used for the test.
     *
     * @return true, if is entering run
     */
    boolean isEnteringRun() {
      return this.enteringRun;
    }
  }

  /*
   * Statistics thread.  Periodically prints the cache statistics to the log.
   */
  /**
   * The Class StatisticsThread.
   */
  static class StatisticsThread extends Thread {
    
    /** The lru. */
    OnHeapBlockCache lru;

    /**
     * Instantiates a new statistics thread.
     *
     * @param lru the lru
     */
    public StatisticsThread(OnHeapBlockCache lru) {
      super("LruBlockCache.StatisticsThread");
      setDaemon(true);
      this.lru = lru;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
      lru.logStats();
    }
  }

  /**
   * Log stats.
   */
  public void logStats() {
    if (!LOG.isDebugEnabled()) return;
    // Log size
    long totalSize = heapSize();
    long freeSize = maxSize - totalSize;
    OnHeapBlockCache.LOG.debug("Stats: " +
        "total=" + StringUtils.byteDesc(totalSize) + ", " +
        "free=" + StringUtils.byteDesc(freeSize) + ", " +
        "max=" + StringUtils.byteDesc(this.maxSize) + ", " +
        "blocks=" + size() +", " +
        "accesses=" + stats.getRequestCount() + ", " +
        "hits=" + stats.getHitCount() + ", " +
        "hitRatio=" +
          (stats.getHitCount() == 0 ? "0" : (StringUtils.formatPercent(stats.getHitRatio(), 2)+ ", ")) + ", " +
        "cachingAccesses=" + stats.getRequestCachingCount() + ", " +
        "cachingHits=" + stats.getHitCachingCount() + ", " +
        "cachingHitsRatio=" +
          (stats.getHitCachingCount() == 0 ? "0" : (StringUtils.formatPercent(stats.getHitCachingRatio(), 2)+ ", ")) + ", " +
        "evictions=" + stats.getEvictionCount() + ", " +
        "evicted=" + stats.getEvictedCount() + ", " +
        "evictedPerRun=" + stats.evictedPerEviction());
  }

  /**
   * Get counter statistics for this cache.
   * 
   * <p>Includes: total accesses, hits, misses, evicted blocks, and runs
   * of the eviction processes.
   *
   * @return the stats
   */
  public CacheStats getStats() {
    return this.stats;
  }

  /** The Constant CACHE_FIXED_OVERHEAD. */
  public final static long CACHE_FIXED_OVERHEAD = ClassSize.align(
      (3 * Bytes.SIZEOF_LONG) + (8 * ClassSize.REFERENCE) +
      (5 * Bytes.SIZEOF_FLOAT) + Bytes.SIZEOF_BOOLEAN
      + ClassSize.OBJECT);

  // HeapSize implementation
  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.io.HeapSize#heapSize()
   */
  public long heapSize() {
    return getCurrentSize();
  }

  /**
   * Calculate overhead.
   *
   * @param maxSize the max size
   * @param blockSize the block size
   * @param concurrency the concurrency
   * @return the long
   */
  public static long calculateOverhead(long maxSize, long blockSize, int concurrency){
    // FindBugs ICAST_INTEGER_MULTIPLY_CAST_TO_LONG
    return CACHE_FIXED_OVERHEAD + ClassSize.CONCURRENT_HASHMAP +
        ((long)Math.ceil(maxSize*1.2/blockSize)
            * ClassSize.CONCURRENT_HASHMAP_ENTRY) +
        (concurrency * ClassSize.CONCURRENT_HASHMAP_SEGMENT);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.io.hfile.BlockCache#getBlockCacheColumnFamilySummaries(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries(Configuration conf) throws IOException {

    Map<String, Path> sfMap = FSUtils.getTableStoreFilePathMap(
        FileSystem.get(conf),
        FSUtils.getRootDir(conf));

    // quirky, but it's a compound key and this is a shortcut taken instead of
    // creating a class that would represent only a key.
    Map<BlockCacheColumnFamilySummary, BlockCacheColumnFamilySummary> bcs =
      new HashMap<BlockCacheColumnFamilySummary, BlockCacheColumnFamilySummary>();

    for (CachedBlock cb : map.values()) {
      String sf = cb.getCacheKey().getHfileName();
      Path path = sfMap.get(sf);
      if ( path != null) {
        BlockCacheColumnFamilySummary lookup =
          BlockCacheColumnFamilySummary.createFromStoreFilePath(path);
        BlockCacheColumnFamilySummary bcse = bcs.get(lookup);
        if (bcse == null) {
          bcse = BlockCacheColumnFamilySummary.create(lookup);
          bcs.put(lookup,bcse);
        }
        bcse.incrementBlocks();
        bcse.incrementHeapSize(cb.heapSize());
      }
    }
    List<BlockCacheColumnFamilySummary> list =
        new ArrayList<BlockCacheColumnFamilySummary>(bcs.values());
    Collections.sort( list );
    return list;
  }

  // Simple calculators of sizes given factors and maxSize

  /**
   * Acceptable size.
   *
   * @return the long
   */
  private long acceptableSize() {
    return (long)Math.floor(this.maxSize * this.acceptableFactor);
  }
  
  /**
   * Min size.
   *
   * @return the long
   */
  private long minSize() {
    return (long)Math.floor(this.maxSize * this.minFactor);
  }
  
  /**
   * Single size.
   *
   * @return the long
   */
  private long singleSize() {
    return (long)Math.floor(this.maxSize * this.singleFactor * this.minFactor);
  }
  
  /**
   * Multi size.
   *
   * @return the long
   */
  private long multiSize() {
    return (long)Math.floor(this.maxSize * this.multiFactor * this.minFactor);
  }
  
  /**
   * Memory size.
   *
   * @return the long
   */
  private long memorySize() {
    return (long)Math.floor(this.maxSize * this.memoryFactor * this.minFactor);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.io.hfile.BlockCache#shutdown()
   */
  public void shutdown() {
    this.evictionThread.shutdown();
  }

  /** Clears the cache. Used in tests. */
  public void clearCache() {
    map.clear();
  }



}
