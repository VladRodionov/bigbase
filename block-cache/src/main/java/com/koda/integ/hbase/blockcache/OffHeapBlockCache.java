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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.util.StringUtils;

import com.koda.cache.CacheManager;
import com.koda.cache.CacheScanner;
import com.koda.cache.OffHeapCache;
import com.koda.cache.eviction.EvictionListener;
import com.koda.cache.eviction.EvictionPolicy;
import com.koda.compression.Codec;
import com.koda.compression.CodecType;
import com.koda.config.CacheConfiguration;
import com.koda.integ.hbase.storage.ExtStorage;
import com.koda.integ.hbase.storage.ExtStorageManager;
import com.koda.integ.hbase.storage.StorageHandle;
import com.koda.integ.hbase.util.CacheableSerializer;
import com.koda.integ.hbase.util.Utils;
import com.koda.io.serde.SerDe;
import com.koda.persistence.PersistenceMode;
import com.koda.persistence.rawfs.RawFSConfiguration;
import com.koda.persistence.rawfs.RawFSStore;


// TODO: Auto-generated Javadoc
/**
 * An off-heap  block cache implementation that is memory-aware,
 * memory-bound using an LRU eviction algorithm, and concurrent: backed by a.
 *
 * {@link OffHeapCache} and with a non-blocking fast eviction  giving
 * constant-time {@link #cacheBlock} and {@link #getBlock} operations.<p>
 * 
 * Contains three levels of block priority to allow for
 * scan-resistance and in-memory families.  A block is added with an inMemory
 * flag if necessary, otherwise a block becomes a single access priority.  Once
 * a blocked is accessed again, it changes to multiple access.  This is used
 * to prevent scans from thrashing the cache, adding a least-frequently-used
 * element to the eviction algorithm.<p>
 * 
 * Each priority is given its own chunk of the total cache to ensure
 * fairness during eviction.  Each priority will retain close to its maximum
 * size, however, if any priority is not using its entire chunk the others
 * are able to grow beyond their chunk size.<p>
 * 
 * Instantiated at a minimum with the total size and average block size.
 * All sizes are in bytes.  The block size is not especially important as this
 * cache is fully dynamic in its sizing of blocks.  It is only used for
 * pre-allocating data structures.<p>
 * 
 * The detailed constructor defines the sizes for the three priorities (they
 * should total to the maximum size defined).  It also sets the levels that
 * trigger and control the eviction thread.<p>
 * 
 * The acceptable size is the cache size level which triggers the eviction
 * process to start.  It evicts enough blocks to get the size below the
 * minimum size specified.<p>
 * 
 * 
 * TODO:
 * 
 * 1. Block data encoding support (see fb-89 L2) ???
 * 
 * 2. Implement:
 * 
 * Each priority is given its own chunk of the total cache to ensure
 * fairness during eviction.  Each priority will retain close to its maximum
 * size, however, if any priority is not using its entire chunk the others
 * are able to grow beyond their chunk size.
 * 
 * 
 * Notes on Cassandra and SSD. Cassandra (1.1+) has so -called flexible data placement
 * (mixed storage support) feature,
 * which allows to place particular CFs into separate mounts (SSD)
 * http://www.datastax.com/dev/blog/whats-new-in-cassandra-1-1-flexible-data-file-placement
 * 
 * This is not so efficient as a true (SSD - backed) block cache in HBase
 * 
 * Some additional links (to use a reference):
 * http://readwrite.com/2012/04/27/cassandra-11-brings-cache-tuning-mixed-storage-support#awesm=~ofb1zhDSfBqb90
 * 
 * 
 * 3. Flexible memory limits. Currently, we have 4 (young, tenured, permanent and external) caches and each cache
 *    relies on its limits to activate eviction. This may result in sub-par usage of an available memory.
 * + 4. 16 bytes hashed values for 'external' cache keys (MD5 or what?).
 * + 5. Compression for external storage.   
 *   6. External storage to keep both keys and blocks
 *      Format:
 *      0..3 total record size
 *      4..7 key size
 *      8..11 value size
 *      12 .. (key size + 12) key data
 *      x .. value data   
 * 
 * 
 * TODO: It seems not all cached data are HFileBlock?
 */
public class OffHeapBlockCache implements BlockCache, HeapSize {

  /** The Constant YOUNG_GEN_FACTOR. */
  public final static String BLOCK_CACHE_YOUNG_GEN_FACTOR = "offheap.blockcache.young.gen.factor";  
  
  /** The Constant BLOCK_CACHE_MEMORY_SIZE. */
  public final static String BLOCK_CACHE_MEMORY_SIZE = "offheap.blockcache.size";  
  
  public final static String HEAP_BLOCK_CACHE_MEMORY_RATIO = "offheap.blockcache.onheap.ratio";
  
  /** The Constant BLOCK_CACHE_IMPL. */
  public final static String BLOCK_CACHE_IMPL = "offheap.blockcache.impl";
     
  /** The Constant EXT_STORAGE_FACTOR. */
  public final static String BLOCK_CACHE_EXT_STORAGE_MEMORY_SIZE = "offheap.blockcache.storage.ref.size";  
  
  /** The Constant _COMPRESSION. */
  public final static String BLOCK_CACHE_COMPRESSION = "offheap.blockcache.compression";  
    
  /** The Constant OVERFLOW_TO_EXT_STORAGE_ENABLED. */
  public final static String BLOCK_CACHE_OVERFLOW_TO_EXT_STORAGE_ENABLED = "offheap.blockcache.storage.enabled";
  
  /** The Constant EXT_STORAGE_IMPL. */
  public final static String BLOCK_CACHE_EXT_STORAGE_IMPL = "offheap.blockcache.storage.impl";
  
  /** The Constant BLOCK_CACHE_ONHEAP_ENABLED. */
  public final static String BLOCK_CACHE_ONHEAP_ENABLED = "offheap.blockcache.onheap.enabled";
  
  public final static String BLOCK_CACHE_TEST_MODE = "offheap.blockcache.test.mode";
  
  public final  static String BLOCK_CACHE_PERSISTENT = "offheap.blockcache.persistent";
  
  public final static String BLOCK_CACHE_SNAPSHOTS ="offheap.blockcache.snapshots.enabled";
  
  public final static String BLOCK_CACHE_SNAPSHOT_INTERVAL ="offheap.blockcache.snapshots.interval";
  
  public final static String BLOCK_CACHE_DATA_ROOTS = "offheap.blockcache.storage.dir";
  
  /** Default is LRU2Q, possible values: LRU, LFU, RANDOM, FIFO */
  public final static String BLOCK_CACHE_EVICTION = "offheap.blockcache.eviction";
  
  public final static String BLOCK_CACHE_BUFFER_SIZE = "offheap.blockcache.nativebuffer.size";
  
  public final static int DEFAULT_BLOCK_CACH_BUFFER_SIZE = 1024*1024; // 1 MB
      
  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(OffHeapBlockCache.class);

  /** Default Configuration Parameters. */
  static final int EXT_STORAGE_REF_SIZE = 64; 
  
  /** Young generation. */
  static final float DEFAULT_YOUNG_FACTOR = 0.5f;
  
  /** 10 % of JVM heap size is dedicated to on heap cache */
  static final float DEFAULT_HEAP_BLOCK_CACHE_MEMORY_RATIO = 0.1f;
  

  /** Statistics thread. */
  static final int statThreadPeriod = 60000;

  /** Main (off - heap) cache. All blocks go to this cache first*/
  private OffHeapCache offHeapCache;
    
  /** External storage handle cache. */
  private OffHeapCache extStorageCache;


  /** Cache statistics - combined */
  private final CacheStats stats;
  
  /** On-heap cache stats */
  private final CacheStats onHeapStats;
  
  /** Off-heap cache stats */
  private final CacheStats offHeapStats;  

  /** External cache stats -L3 */
  private final CacheStats extStats;
  
  /** External references cache stats in RAM*/
  private final CacheStats extRefStats;
  
  
  /** Maximum allowable size of cache (block put if size > max, evict). */
  private long blockCacheMaxSize;
  
  /** Maximum allowable size of external storage cache. */
  private long extCacheMaxSize;
  
  /** Approximate block size. */
  private long blockSize;
  
  /** Direct buffer block size */
  private int nativeBufferSize = DEFAULT_BLOCK_CACH_BUFFER_SIZE;

  /** Single access bucket size. */
  private float youngGenFactor;
  

  /**
   *  Data overflow to external storage.
   */
  private boolean overflowExtEnabled = false;
  
  /**
   *  Save Ref cache on shutdown
   */
  private boolean isPersistent = false;
  
  @SuppressWarnings("unused")
  private boolean isSnapshotsEnabled = false;
  
  @SuppressWarnings("unused")
  private long snapshotsInterval = 0;
  
  /** external storage (file or network - based). */
  
  private ExtStorage storage;
  
  /** The stat thread. */
  private StatisticsThread statThread;
  
  /** The deserializer. */
  private AtomicReference<CacheableDeserializer<Cacheable>> deserializer = 
    new AtomicReference<CacheableDeserializer<Cacheable>>();

  /** Fast on-heap cache to store NON-DATA blocks (INDEX, BLOOM etc). */
  private OnHeapBlockCache onHeapCache;
  
  private AtomicLong fatalExternalReads = new AtomicLong(0);
  /**
   * Instantiates a new off heap block cache.
   *
   * @param conf the conf
   */
  public OffHeapBlockCache(Configuration conf)
  {
      this.blockSize = conf.getInt("hbase.offheapcache.minblocksize",
    		  HColumnDescriptor.DEFAULT_BLOCKSIZE);  
 
      blockCacheMaxSize = conf.getLong(BLOCK_CACHE_MEMORY_SIZE, 0L);
      if(blockCacheMaxSize == 0L){
        throw new RuntimeException("off heap block cache size is not defined");
      }
      nativeBufferSize = conf.getInt(BLOCK_CACHE_BUFFER_SIZE, DEFAULT_BLOCK_CACH_BUFFER_SIZE);      
      extCacheMaxSize = conf.getLong(BLOCK_CACHE_EXT_STORAGE_MEMORY_SIZE, (long) (0.1 * blockCacheMaxSize));
      youngGenFactor = conf.getFloat(BLOCK_CACHE_YOUNG_GEN_FACTOR, DEFAULT_YOUNG_FACTOR);
      overflowExtEnabled = conf.getBoolean(BLOCK_CACHE_OVERFLOW_TO_EXT_STORAGE_ENABLED, false);
      isPersistent = conf.getBoolean(BLOCK_CACHE_PERSISTENT, false);
      if(isPersistent){
    	  // Check if we have already set CacheableDeserializer
    	  // We need to set deserializer before starting cache
    	  // because we can have already cached blocks on cache start up
    	  // and first get before put will fail. 
    	  if(CacheableSerializer.getDeserializer() == null){
    		  CacheableSerializer.setHFileDeserializer();
    	  } else{
    		  LOG.info("CacheableSerializer is already set.");
    	  }
      }
      isSnapshotsEnabled = conf.getBoolean(BLOCK_CACHE_SNAPSHOTS, false);
      snapshotsInterval = conf.getInt(BLOCK_CACHE_SNAPSHOT_INTERVAL, 600) * 1000;
      
      String[] dataRoots = getDataRoots(conf.get(BLOCK_CACHE_DATA_ROOTS));
      
      if(isPersistent && dataRoots == null){
        dataRoots = getHDFSRoots(conf);
        
        if(dataRoots == null){
          LOG.warn("Data roots are not defined. Set persistent mode to false.");
          isPersistent = false;
        }
      }
      
      adjustMaxMemory();
      
      /** Possible values: none, snappy, gzip, lz4, lz4hc */
      // TODO: LZ4 is not supported on all platforms
      // TODO: default compression is LZ4?
      CodecType codec = CodecType.LZ4;      
       
      String value = conf.get(BLOCK_CACHE_COMPRESSION);
      if(value != null){
        codec = CodecType.valueOf(value.toUpperCase());
      }
      
      
      try {

      CacheConfiguration cacheCfg =  new CacheConfiguration();
      cacheCfg.setCacheName("block-cache");
      
      cacheCfg.setSerDeBufferSize(nativeBufferSize);
      
      cacheCfg.setMaxMemory(blockCacheMaxSize);
      cacheCfg.setCodecType(codec);
      String evictionPolicy =conf.get(BLOCK_CACHE_EVICTION, "LRU").toUpperCase();
      cacheCfg.setEvictionPolicy(evictionPolicy);
      // Set this only for LRU2Q
      cacheCfg.setLRU2QInsertPoint(youngGenFactor);
      setBucketNumber(cacheCfg);      

      CacheManager manager = CacheManager.getInstance();


      if(overflowExtEnabled == true){
        LOG.info("Overflow to external storage is enabled.");
          // External storage handle cache
        CacheConfiguration extStorageCfg  = new CacheConfiguration();
        extStorageCfg.setCacheName("extStorageCache");
        extStorageCfg.setMaxMemory(extCacheMaxSize);
        extStorageCfg.setEvictionPolicy(EvictionPolicy.FIFO.toString());
        extStorageCfg.setSerDeBufferSize(4096);// small
        extStorageCfg.setPreevictionListSize(40);
        extStorageCfg.setKeyClassName(byte[].class.getName());
        extStorageCfg.setValueClassName(byte[].class.getName());
        // calculate bucket number
        // 50 is estimate of a record size
        int buckets =  (extCacheMaxSize / EXT_STORAGE_REF_SIZE) > 
        Integer.MAX_VALUE? Integer.MAX_VALUE -1: (int) (extCacheMaxSize / EXT_STORAGE_REF_SIZE);       
        extStorageCfg.setBucketNumber(buckets);
        if(isPersistent){
          // TODO - this in memory cache has same data dirs as a major cache.
          RawFSConfiguration storeConfig = new RawFSConfiguration();
          
          storeConfig.setStoreName(extStorageCfg.getCacheName());
          
          storeConfig.setDiskStoreImplementation(RawFSStore.class);
          
          storeConfig.setDbDataStoreRoots(dataRoots);
          storeConfig.setPersistenceMode(PersistenceMode.ONDEMAND);
          storeConfig.setDbCompressionType(CodecType.LZ4);
          storeConfig.setDbSnapshotInterval(15);
          //storeConfig.setTotalWorkerThreads(Runtime.getRuntime().availableProcessors() /2);
          //storeConfig.setTotalIOThreads(1);          
          extStorageCfg.setDataStoreConfiguration(storeConfig);
        }        
        
        // This will initiate the load of stored cache data
        // if persistence is enabled
        extStorageCache = manager.getCache(extStorageCfg, null);
        // Initialize external storage
        storage = ExtStorageManager.getInstance().getStorage(conf, extStorageCache);
      } else{
          LOG.info("Overflow to external storage is disabled.");  
          if(isPersistent){
            RawFSConfiguration storeConfig = new RawFSConfiguration();
            
            storeConfig.setStoreName(cacheCfg.getCacheName());
            
            storeConfig.setDiskStoreImplementation(RawFSStore.class);
            
            storeConfig.setDbDataStoreRoots(dataRoots);
            storeConfig.setPersistenceMode(PersistenceMode.ONDEMAND);
            storeConfig.setDbSnapshotInterval(15);
            cacheCfg.setDataStoreConfiguration(storeConfig); 
            // Load cache data
            offHeapCache = manager.getCache(cacheCfg, null);
          }
      }
      
      if(offHeapCache == null){
        offHeapCache = manager.getCache(cacheCfg, null);
      }
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    } 
    boolean onHeapEnabled = conf.getBoolean(BLOCK_CACHE_ONHEAP_ENABLED, true);
    if(onHeapEnabled){
      long onHeapCacheSize = calculateOnHeapCacheSize(conf);
      if(onHeapCacheSize > 0){
        onHeapCache = new OnHeapBlockCache(onHeapCacheSize, blockSize, conf);
        LOG.info("Created fast on-heap cache. Size="+onHeapCacheSize);
      } else{
        LOG.warn("Conflicting configuration options. On-heap cache is disabled.");
      }
    }
    
    this.stats = new CacheStats();
    this.onHeapStats = new CacheStats();
    this.offHeapStats = new CacheStats();
    this.extStats = new CacheStats();
    this.extRefStats = new CacheStats();
    
    EvictionListener listener = new EvictionListener(){

      @Override
      public void evicted(long ptr, Reason reason, long nanoTime) {
        stats.evict();
        stats.evicted();
        
      }
      
    };
    
    offHeapCache.setEvictionListener(listener);
    // Cacheable serializer registration
    
    CacheableSerializer serde = new CacheableSerializer();    
    offHeapCache.getSerDe().registerSerializer(serde);
    
//    if( extStorageCache != null){
//      //StorageHandleSerializer serde2 = new StorageHandleSerializer();
//      //  SmallByteArraySerializer serde2 = new SmallByteArraySerializer();
//      //	extStorageCache.getSerDe().registerSerializer(serde2);
//    }
    // Start statistics thread
    statThread = new StatisticsThread(this);
    statThread.start();
    
  }
  
  private String[] getHDFSRoots(Configuration conf) {
    // Use default dfs data directories
    String str = conf.get("dfs.data.dir");
    if(str == null) return null;
    String[] dirs = str.split(",");
    for(int i=0 ; i < dirs.length; i++){
      dirs[i] = dirs[i].trim() + File.separator + "blockcache";
    }
    return dirs;
  }

  private String[] getDataRoots(String roots)
  {
    if (roots == null) return null;
    String[] rts = roots.split(",");
    String[] retValue = new String[rts.length];
    for( int i=0; i < retValue.length; i++){
      retValue[i] = rts[i].trim();
    }
    return retValue;
  }
  
  
  /**
   * Calculate on heap cache size.
   *
   * @param conf the conf
   * @return the long
   */
  private long calculateOnHeapCacheSize(Configuration conf) {
    float cachePercentage = conf.getFloat(
        HEAP_BLOCK_CACHE_MEMORY_RATIO, DEFAULT_HEAP_BLOCK_CACHE_MEMORY_RATIO);
    if (cachePercentage == 0L) {
      // block cache disabled on heap
      return 0L;
    }
    if (cachePercentage > 1.0) {
      throw new IllegalArgumentException(HEAP_BLOCK_CACHE_MEMORY_RATIO
          + " must be between 0.0 and 1.0, and not > 1.0");
    }

    // Calculate the amount of heap to give the heap.
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    long cacheSize = (long) (mu.getMax() * cachePercentage);
    return cacheSize;
  }
  

  /**
   * Adjust max memory.
   */
  private void adjustMaxMemory() {
    if(overflowExtEnabled == true && extCacheMaxSize == 0){
        // By default we set back 5% to external ref cache 
        extCacheMaxSize =  (long) (0.05 * blockCacheMaxSize);
    	blockCacheMaxSize = (long) (0.95 * blockCacheMaxSize);
    } else if(overflowExtEnabled == true){
    	blockCacheMaxSize -= extCacheMaxSize;
    }
    LOG.info("Block cache max size ="+blockCacheMaxSize+" external cache support (in RAM)="+extCacheMaxSize);
  }

  /**
   * Sets the bucket number.
   *
   * @param cfg the new bucket number
   */
  private void setBucketNumber(CacheConfiguration cfg)
  {
     long memSize = cfg.getMaxMemory();
     float compFactor = getAvgCompression(cfg.getCodecType());
     long bSize= (long) (blockSize / compFactor);
     int bucketNumber = (int) (memSize / bSize);
     cfg.setBucketNumber(bucketNumber);
  }
  
  /**
   * Gets the avg compression.
   *
   * @param codec the codec
   * @return the avg compression
   */
  private float getAvgCompression(CodecType codec)
  {
   switch(codec)
   {
      case NONE: return 1.0f;
      case SNAPPY: return 2.0f;
      case LZ4: return 2.0f;
      case LZ4HC: return 3.0f;
      case DEFLATE : return 4.0f;
      default: return 1.0f;
   }
  }
  
 
  // BlockCache implementation


  /**
   * Get the maximum size of this cache. It returns only max size of a data cache 
   * @return max size in bytes
   */
  public long getMaxSize() {
    return this.blockCacheMaxSize + (onHeapEnabled()? onHeapCache.getMaxSize(): 0);
  }

  /*
   * TODO: run stats thread
   * Statistics thread.  Periodically prints the cache statistics to the log.
   */
  /**
   * The Class StatisticsThread.
   */
  static class StatisticsThread extends Thread {
    
    /** The cache. */
    OffHeapBlockCache cache;

    /**
     * Instantiates a new statistics thread.
     *
     * @param cache the cache
     */
    public StatisticsThread(OffHeapBlockCache cache) {
      super("BigBaseBlockCache.StatisticsThread");
      setDaemon(true);
      this.cache = cache;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
      LOG.info(Thread.currentThread().getName()+" started.");
      while(true){
        try {
          Thread.sleep(statThreadPeriod);
        } catch (InterruptedException e) {}
        cache.logStats();
        cache.logStatsOffHeap();
        cache.logStatsOnHeap();
        cache.logStatsExternal();
        cache.logStatsOffHeapExt();
      }
    }
  }

  /**
   * Log stats.
   */
  protected void logStats() {
    // Log size
    long totalSize = getCurrentSize();
    long freeSize = getMaxSize() - totalSize;
    OffHeapBlockCache.LOG.info("[BLOCK CACHE]: " +
        "total=" + StringUtils.byteDesc(totalSize) + ", " +        
        "free=" + StringUtils.byteDesc(freeSize) + ", " +
        "max=" + StringUtils.byteDesc(getMaxSize()) + ", " +
        "blocks=" + size() +", " +
        "accesses=" + stats.getRequestCount() + ", " +
        "hits=" + stats.getHitCount() + ", " +
        "hitRatio=" + (stats.getRequestCount()>0 ? StringUtils.formatPercent(stats.getHitRatio(), 2): "0.00")  + "%, "+
        "cachingAccesses=" + stats.getRequestCachingCount() + ", " +
        "cachingHits=" + stats.getHitCachingCount() + ", " +
        "cachingHitsRatio=" +
        (stats.getRequestCachingCount() > 0 ?StringUtils.formatPercent(stats.getHitCachingRatio(), 2): "0.00") + "%, " +

        "evicted=" + getEvictedCount() );

  }

  protected void logStatsOffHeap() {
    // Log size
    long totalSize = offHeapCache.getTotalAllocatedMemorySize();
    long maxSize = offHeapCache.getMemoryLimit();
    long freeSize = maxSize - totalSize;
    OffHeapBlockCache.LOG.info("[L2-OFFHEAP] : " +
        "total=" + StringUtils.byteDesc(totalSize) + ", " +        
        "free=" + StringUtils.byteDesc(freeSize) + ", " +
        "max=" + StringUtils.byteDesc(maxSize) + ", " +
        "blocks=" + offHeapCache.size() +", " +
        "accesses=" + offHeapStats.getRequestCount() + ", " +
        "hits=" + offHeapStats.getHitCount() + ", " +
        "hitRatio=" + (offHeapStats.getRequestCount()>0 ? StringUtils.formatPercent(offHeapStats.getHitRatio(), 2): "0.00") + "%, "+
        "cachingAccesses=" + offHeapStats.getRequestCachingCount() + ", " +
        "cachingHits=" + offHeapStats.getHitCachingCount() + ", " +
        "cachingHitsRatio=" +
        (offHeapStats.getRequestCachingCount() > 0 ?StringUtils.formatPercent(offHeapStats.getHitCachingRatio(), 2): "0.00") + "%, " +

        "evicted=" + offHeapCache.getEvictedCount() );

  } 
  
  protected void logStatsOffHeapExt() {
	    // Log size
	    long totalSize = extStorageCache.getAllocatedMemorySize();
	    long maxSize = extStorageCache.getMemoryLimit();
	    long freeSize = maxSize - totalSize;
	    OffHeapBlockCache.LOG.info("[L3-OFFHEAP] : " +
	        "total=" + StringUtils.byteDesc(totalSize) + ", " +        
	        "free=" + StringUtils.byteDesc(freeSize) + ", " +
	        "max=" + StringUtils.byteDesc(maxSize) + ", " +
	        "refs=" + extStorageCache.size() +", " +
	        "accesses=" + extRefStats.getRequestCount() + ", " +
	        "hits=" + extRefStats.getHitCount() + ", " +
	        "hitRatio=" + (extRefStats.getRequestCount()>0 ? StringUtils.formatPercent(extRefStats.getHitRatio(), 2): "0.00") + "%, "+
	        "cachingAccesses=" + extRefStats.getRequestCachingCount() + ", " +
	        "cachingHits=" + extRefStats.getHitCachingCount() + ", " +
	        "cachingHitsRatio=" +
	        (extRefStats.getRequestCachingCount() > 0 ?StringUtils.formatPercent(offHeapStats.getHitCachingRatio(), 2): "0.00") + "%, " +

	        "evicted=" + extStorageCache.getEvictedCount() );

	  }  
  
  protected void logStatsOnHeap() {
    if(onHeapEnabled() == false) return;
    // Log size
    long totalSize = onHeapCache.getCurrentSize();
    long maxSize = onHeapCache.getMaxSize();
    long freeSize = maxSize - totalSize;   
    
    OnHeapBlockCache.LOG.info("[L2-HEAP]    : " +
        "total=" + StringUtils.byteDesc(totalSize) + ", " +        
        "free=" + StringUtils.byteDesc(freeSize) + ", " +
        "max=" + StringUtils.byteDesc(maxSize) + ", " +
        "blocks=" + onHeapCache.size() +", " +
        "accesses=" + onHeapStats.getRequestCount() + ", " +
        "hits=" + onHeapStats.getHitCount() + ", " +
        "hitRatio=" + (onHeapStats.getRequestCount()>0 ? StringUtils.formatPercent(onHeapStats.getHitRatio(), 2): "0.00") + "%, "+
        "cachingAccesses=" + onHeapStats.getRequestCachingCount() + ", " +
        "cachingHits=" + onHeapStats.getHitCachingCount() + ", " +
        "cachingHitsRatio=" +
        (onHeapStats.getRequestCachingCount() > 0 ?StringUtils.formatPercent(onHeapStats.getHitCachingRatio(), 2): "0.00") + "%, " +

        "evicted=" + onHeapCache.getEvictedCount() );

  }  
  
  protected void logStatsExternal() {
	    if( storage == null) return;
	    // Log size
	    long totalSize = storage.size();
	    long maxSize = storage.getMaxStorageSize() ;
	    long freeSize = maxSize - totalSize;   
	    
	    OnHeapBlockCache.LOG.info("[L3-DISK]    : " +
	        "total=" + StringUtils.byteDesc(totalSize) + ", " +        
	        "free=" + StringUtils.byteDesc(freeSize) + ", " +
	        "max=" + StringUtils.byteDesc(maxSize) + ", " +
	         
	        "accesses=" + extStats.getRequestCount() + ", " +
	        "hits=" + extStats.getHitCount() + ", " +
	        "hitRatio=" + (extStats.getRequestCount()>0 ? StringUtils.formatPercent(extStats.getHitRatio(), 2): "0.00") + "%, "+
	        "cachingAccesses=" + extStats.getRequestCachingCount() + ", " +
	        "cachingHits=" + extStats.getHitCachingCount() + ", " +
	        "cachingHitsRatio=" +
	        (extStats.getRequestCachingCount() > 0 ?StringUtils.formatPercent(extStats.getHitCachingRatio(), 2): "0.00") + "%, "); 
	       // "\nFATAL READS="+fatalExternalReads.get());

	  }  
  /**
   * HeapSize implementation - returns zero if auxCache is disabled.
   *
   * @return the long
   */

  /* (non-Javadoc)
  * @see org.apache.hadoop.hbase.io.HeapSize#heapSize()
  */
 public long heapSize() {
    return onHeapCache == null? 0: onHeapCache.heapSize();
  }
 

 
 
  /**
   * Add block to cache.
   * @param cacheKey The block's cache key.
   * @param buf The block contents wrapped in a ByteBuffer.
   * @param inMemory Whether block should be treated as in-memory
   */
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory){
      boolean contains = false;
    try {
      String blockName = cacheKey.toString();
      contains = offHeapCache.contains(blockName);
      if ( contains) {
        // TODO - what does it mean? Can we ignore this?
        throw new RuntimeException("Cached an already cached block: "+blockName);
      } 
      // Always cache block to off-heap cache first
      offHeapCache.put(blockName, buf);
      
      if(buf.getBlockType() != BlockType.DATA && onHeapEnabled()){
        // Cache on-heap only non-data blocks
        onHeapCache.cacheBlock(cacheKey, buf, inMemory);
      }
      if( isExternalStorageEnabled()){
        // FIXME This code disables storage in non-test mode???
        byte[] hashed = Utils.hash128(blockName);
        // TODO : do we really need to check this?
        if( extStorageCache.contains(hashed) == false){
          // Store external if we found object in a block cache and not in external cache
          // ONLY IN TEST MODE
          storeExternalWithCodec(blockName, buf, false);
        }
      }
    } catch (Exception e) {
      LOG.error(e);
      throw new RuntimeException(e);
    }   
  }

  /**
   * On heap enabled.
   *
   * @return true, if successful
   */
  private final boolean onHeapEnabled()
  {
     return onHeapCache != null; 
  }
  
  /**
   * Store external with codec.
   * Format:
   * 0..3  - total record size (-4)
   * 4..7  - size of a key in bytes (16 if use hash128)
   * 8 .. x - key data
   * x+1 ..x+1- IN_MEMORY flag ( 1- in memory, 0 - not)
   * x+2 ... block, serialized and compressed 
   *
   * @param blockName the block name
   * @param buf the buf
   * @param inMemory the in memory
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void storeExternalWithCodec(String blockName, Cacheable buf, boolean inMemory) throws IOException{
    // If external storage is disable - bail out
    if (overflowExtEnabled == false){
      return;
    }
    byte[] hashed = Utils.hash128(blockName);

    ByteBuffer buffer = extStorageCache.getLocalBufferWithAddress()
        .getBuffer();
    deserializer.set(buf.getDeserializer());

    SerDe serde = extStorageCache.getSerDe();
    Codec codec = extStorageCache.getCompressionCodec();
    buffer.clear();

    buffer.position(4);

    // Save key
    buffer.putInt(hashed.length);
    buffer.put(hashed);
    buffer.put(inMemory ? (byte) 1 : (byte) 0);
    
    if (buf != null) {
      serde.writeCompressed(buffer, buf, codec);
      int pos = buffer.position();
      buffer.putInt(0, pos - 4);
    } else {
      buffer.putInt(0, 0);
    }
    buffer.flip();
    StorageHandle handle = storage.storeData(buffer);

    try {
      // WE USE byte array as a key
      extStorageCache.put(hashed, handle.toBytes());
    } catch (Exception e) {
      throw new IOException(e);
    }
  
  }
  
  /**
   * Gets the external storage cache.
   *
   * @return the ext storage cache
   */
  public OffHeapCache getExtStorageCache()
  {
    return extStorageCache;
  }

  /**
   * Read external with codec.
   *
   * @param blockName the block name
   * @return the cacheable
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("unused")
  private Cacheable readExternalWithCodec(String blockName, boolean repeat, boolean caching) throws IOException
  {
    if(overflowExtEnabled == false) return null;
    // Check if we have  already this block in external storage cache
    try {
    // We use 16 - byte hash for external storage cache  
    byte[] hashed = Utils.hash128(blockName);  
    
    StorageHandle handle = storage.newStorageHandle();    
    byte[] data = (byte[])extStorageCache.get(hashed);
    if( data == null ) {
    	if(repeat == false) extRefStats.miss(caching);
    	return null;
    } else{
    	extRefStats.hit(caching);
    }
    // Initialize handle 
    handle.fromBytes(data);

    ByteBuffer buffer = extStorageCache.getLocalBufferWithAddress().getBuffer();
    SerDe serde = extStorageCache.getSerDe();
    Codec codec = extStorageCache.getCompressionCodec();
    
    buffer.clear();
    
    StorageHandle newHandle = storage.getData(handle, buffer);
    if(buffer.position() > 0) buffer.flip();
    int size = buffer.getInt();
    if(size == 0) {
    	// BIGBASE-45
    	// Remove reference from reference cache
    	// reference is in L3-RAM cache but no object in L3-DISK cache was found
    	// remove only if handle is invalid
    	if(storage.isValid(handle) == false){
    		extStorageCache.remove(hashed);
    	}
    	return null;
    }
    // Skip key
    int keySize = buffer.getInt();
    buffer.position(8 + keySize);

    boolean inMemory = buffer.get() == (byte) 1;
    
    buffer.limit(size + 4);
    Cacheable obj = (Cacheable) serde.readCompressed(buffer/*, codec*/);    
    offHeapCache.put(blockName, obj);
 
    if( newHandle.equals(handle) == false){
      extStorageCache.put(hashed, newHandle.toBytes());
    }
    
    return obj;
    
    } catch (Throwable e) {
      fatalExternalReads.incrementAndGet();
      throw new IOException(e);
    }
    
  }

/**
   * Add block to cache (defaults to not in-memory).
   * @param cacheKey The block's cache key.
   * @param buf The object to cache.
   */
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf){
    cacheBlock(cacheKey, buf, false);
  }

  /**
   * Fetch block from cache.
   *
   * @param cacheKey Block to fetch.
   * @param caching Whether this request has caching enabled (used for stats)
   * @param repeat Whether this is a repeat lookup for the same block
   * (used to avoid double counting cache misses when doing double-check locking)
   * @return Block or null if block is not in 2 cache.
   * {@see HFileReaderV2#readBlock(long, long, boolean, boolean, boolean, BlockType)}
   */
  
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching, boolean repeat){
    try{
      String blockName = cacheKey.toString();
      
      Cacheable bb = onHeapEnabled()? (Cacheable) onHeapCache.getBlock(cacheKey, caching, repeat): null;
        
      if(bb == null) {
        if(repeat == false) onHeapStats.miss(caching);
        bb = (Cacheable)offHeapCache.get(blockName);  
        if(bb == null){
          if(repeat == false){
            offHeapStats.miss(caching);
          }
        } else{
          offHeapStats.hit(caching);
        }
      } else{
        // We need touch dataBlockCache
        offHeapCache.touch(blockName);
        onHeapStats.hit(caching);
      }

      if( bb == null){
          // Try to load from external cache
    	  
         bb = readExternalWithCodec(blockName, repeat, caching);
         
         if(bb == null){
           if(repeat == false) extStats.miss(caching);
         } else{
           extStats.hit(caching);
         }
         
      } else if(isExternalStorageEnabled()){
        byte[] hashed = Utils.hash128(blockName);
        if(extStorageCache.contains(hashed) == false){      
          // FIXME: double check 'contains'
          // Store external if we found object in a block cache and not in external (L3) cache
          storeExternalWithCodec(blockName, bb, false);
        } else{
        	// Touch L3-RAM cache
        	extStorageCache.touch(hashed);
        }
      }
      if(bb == null) {
        if(repeat == false) {
          stats.miss(caching);
        }
        return null;
      }
      
      stats.hit(caching);   
      return bb;
    }catch(Exception e)
    {
      LOG.error( cacheKey.toString(),e);
      throw new RuntimeException(e);
    }
  }

  
  private final boolean isExternalStorageEnabled()
  {
	  return extStorageCache != null;
  }
  
  /**
   * Evict block from cache.
   * @param cacheKey Block to evict
   * @return true if block existed and was evicted, false if not
   */
  public boolean evictBlock(BlockCacheKey cacheKey){
    // We ignore this as since eviction is automatic
    // always return true
    boolean result = false;
    try {
      result = offHeapCache.remove(cacheKey.toString());
    } catch (Exception e) {
      throw new RuntimeException(e);     
    }
    return result || onHeapEnabled() ? onHeapCache.evictBlock(cacheKey): true;
  }

  /**
   * Evicts all blocks for the given HFile.
   *
   * @param hfileName the hfile name
   * @return the number of blocks evicted
   */
  public int evictBlocksByHfileName(final String hfileName){
    // We ignore this as since eviction is automatic
    // always return '0' - will it breaks anything?

    // Single threaded
    // TODO: do we need global lock? 
    // TODO: it seems not fast enough
    // multiple scanners at the same time has not been tested
    // thouroghly yet. 
   
    Runnable r = new Runnable() {

      public void run() {
        LOG.info("Evict blocks for file "+hfileName);
        int scannerNo = 0;
        long total = 0;
        long startTime = System.currentTimeMillis();
        while (scannerNo < 256) {
          CacheScanner scanner = offHeapCache.getScanner(scannerNo++, 256);

          List<String> keys = new ArrayList<String>();

          while (scanner.hasNext()) {
            try {
              String key = (String) scanner.nextKey();
              if (key.startsWith(hfileName)) {
                keys.add(key);
              }
            } catch (Exception e) {
              LOG.error("Failed evictBlocksByHfileName ", e);
              break;
            }
          }

          scanner.close();
          // Remove all keys
          for (String key : keys) {
            try {
              if (offHeapCache.remove(key)) {
                total++;
              }
            } catch (Exception e) {
              LOG.error("Failed evictBlocksByHfileName ", e);
              break;
            }
          }
        }

        LOG.info(hfileName + " : evicted " + total + " in "
            + (System.currentTimeMillis() - startTime) + "ms");
        if (onHeapEnabled()) {
          onHeapCache.evictBlocksByHfileName(hfileName);
        }
      }
    };
    
    new Thread(r).start();
    
    return (int) 0;
  }

  /**
   * Get the total statistics for this block cache.
   * @return Stats
   */
  public CacheStats getStats(){
    return this.stats;
  }

  /**
   * Get the on-heap cache statistics for this block cache.
   * @return Stats
   */  
  public CacheStats getOnHeapStats()
  {
    return onHeapStats;
  }
  
  /**
   * Get the off-heap cache statistics for this block cache.
   * @return Stats
   */    
  public CacheStats getOffHeapStats()
  {
    return offHeapStats;
  }
  /**
   * Get the external cache statistics for this block cache.
   * @return Stats
   */    
  public CacheStats getExtStats()
  {
    return extStats;
  }
  
  /**
   *  Gets the external ref cache stats
   * @return Stats
   */
  
  public CacheStats getExtRefStats()
  {
	  return extRefStats;
  }
  
  
  public OffHeapCache getOffHeapCache()
  {
    return offHeapCache;
  }
  
  public OnHeapBlockCache getOnHeapCache()
  {
    return onHeapCache;
  }
  
  public ExtStorage getExternalStorage()
  {
    return storage;
  }
  
  
  /**
   * Shutdown the cache.
   */
  public void shutdown(){
      // Shutdown all caches
      try {
        offHeapCache.shutdown();
        
        if(extStorageCache != null){
          extStorageCache.shutdown();
        }
        if(storage != null){
          storage.shutdown(isPersistent);
        }
    } catch (Exception e) {
      LOG.error("Shutdown failed", e);
    } 
  }

  /**
   * Returns the total size of the block cache, in items.
   * @return size of cache, in bytes
   */
  public final long size(){
//    if( storage != null){
//      return estimateExtStorageSize();
//    }
    return offHeapCache.size() + (onHeapEnabled()? onHeapCache.size(): 0) ;
  }

  @SuppressWarnings("unused")
  private final long estimateExtStorageSize() {
    if(storage == null){
      return 0;
    } else{
      long extSize = storage.size();
      long offHeapSize = offHeapCache.getTotalAllocatedMemorySize();
      long items = offHeapCache.size();
      if(items == 0) return 0;
      long avgItemSize = (offHeapSize / items);
      return extSize / avgItemSize;
    }
  }

  /**
   * It reports only RAM.
   * Returns the free size of the block cache, in bytes.
   * @return free space in cache, in bytes
   */
  public final long getFreeSize(){
    return getMaxSize() - getCurrentSize();
  }

  /**
   * It reports only RAM.
   * Returns the occupied size of the block cache, in bytes.
   * @return occupied space in cache, in bytes
   */
  public long getCurrentSize(){
      return offHeapCache.getAllocatedMemorySize() + 
      /*(extStorageCache != null? extStorageCache.getAllocatedMemorySize():0) +*/
      (onHeapEnabled()? onHeapCache.getCurrentSize(): 0);
  }

  /**
   * It reports only RAM.
   * Returns the number of evictions that have occurred.
   * @return number of evictions
   */
  public long getEvictedCount(){
    return offHeapCache.getEvictedCount() + 
    //(extStorageCache != null? extStorageCache.getEvictedCount():0);
    (onHeapEnabled()? onHeapCache.getEvictedCount(): 0);
  }

  /**
   * RAM only
   * Returns the number of blocks currently cached in the block cache.
   * @return number of blocks in the cache
   */
  public long getBlockCount()
  {
    return size();
  }

  /**
   * Performs a BlockCache summary and returns a List of BlockCacheColumnFamilySummary objects.
   * This method could be fairly heavy-weight in that it evaluates the entire HBase file-system
   * against what is in the RegionServer BlockCache.
   * <br><br>
   * The contract of this interface is to return the List in sorted order by Table name, then
   * ColumnFamily.
   *
   * @param conf HBaseConfiguration
   * @return List of BlockCacheColumnFamilySummary
   * @throws IOException exception
   */
  public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries(Configuration conf) throws IOException
  {
    // TODO
    // it seems that this API is not used
    return new ArrayList<BlockCacheColumnFamilySummary>();
  }
  
  public void dumpCacheStats()
  {
    if(onHeapEnabled()){
      LOG.info("On heap stats:");
      LOG.info("Size="+ onHeapCache.heapSize());
      LOG.info("hits="+onHeapStats.getHitCount() + " miss="+onHeapStats.getMissCount() + " hit ratio="+onHeapStats.getHitRatio());
    }
    LOG.info("Off heap stats:");
    LOG.info("Size="+ offHeapCache.getAllocatedMemorySize());
    LOG.info("hits="+offHeapStats.getHitCount() + " miss="+offHeapStats.getMissCount() + " hit ratio="+offHeapStats.getHitRatio());
    
    if( storage != null){
      LOG.info("External storage stats:");
      LOG.info("Size="+ storage.size());
      LOG.info("hits="+extStats.getHitCount() + " miss="+extStats.getMissCount() + " hit ratio="+extStats.getHitRatio());
    }
    
    LOG.info("Overall stats:");
    LOG.info("hits="+stats.getHitCount() + " miss="+stats.getMissCount() + " hit ratio="+stats.getHitRatio());    
    
  }
}

