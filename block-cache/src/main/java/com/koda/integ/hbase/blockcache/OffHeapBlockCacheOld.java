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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.util.StringUtils;

import com.koda.KodaException;
import com.koda.NativeMemoryException;
import com.koda.cache.CacheManager;
import com.koda.cache.OffHeapCache;
import com.koda.cache.eviction.EvictionListener;
import com.koda.compression.Codec;
import com.koda.compression.CodecType;
import com.koda.config.CacheConfiguration;
import com.koda.integ.hbase.storage.ExtStorage;
import com.koda.integ.hbase.storage.ExtStorageManager;
import com.koda.integ.hbase.storage.StorageHandle;
import com.koda.integ.hbase.util.CacheableSerializer;
import com.koda.integ.hbase.util.ConfigHelper;
import com.koda.integ.hbase.util.StorageHandleSerializer;
import com.koda.integ.hbase.util.Utils;
import com.koda.io.serde.SerDe;


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
 */
public class OffHeapBlockCacheOld implements BlockCache, HeapSize {

  /** The Constant YOUNG_GEN_FACTOR. */
  public final static String YOUNG_GEN_FACTOR = "blockcache.young.gen.factor";	
  
  /** The Constant TENURED_GEN_FACTOR. */
  public final static String TENURED_GEN_FACTOR = "blockcache.tenured.gen.factor";	
  
  /** The Constant PERM_GEN_FACTOR. */
  public final static String PERM_GEN_FACTOR = "blockcache.perm.gen.factor";	
  
  /** The Constant EXT_STORAGE_FACTOR. */
  public final static String EXT_STORAGE_FACTOR = "blockcache.ext.storage.factor";	
  
  /** The Constant YOUNG_GEN_COMPRESSION. */
  public final static String YOUNG_GEN_COMPRESSION = "blockcache.young.gen.compression";	
  
  /** The Constant TENURED_GEN_COMPRESSION. */
  public final static String TENURED_GEN_COMPRESSION = "blockcache.tenured.gen.compression";	
  
  /** The Constant PERM_GEN_COMPRESSION. */
  public final static String PERM_GEN_COMPRESSION = "blockcache.perm.gen.compression";	
  
  /** The Constant EXT_STORAGE_COMPRESSION. */
  public final static String EXT_STORAGE_COMPRESSION ="blockcache.ext.storage.compression";
  
  /** The Constant OVERFLOW_TO_EXT_STORAGE_ENABLED. */
  public final static String OVERFLOW_TO_EXT_STORAGE_ENABLED = "blockcache.overflow.ext.enabled";
  
  /** The Constant EXT_STORAGE_IMPL. */
  public final static String EXT_STORAGE_IMPL = "blockcache.ext.storage.class";
  

  
  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(OffHeapBlockCacheOld.class);

  /** Default Configuration Parameters. */


  /** Priority buckets */
  /** Young generation */
  static final float DEFAULT_YOUNG_FACTOR = 0.25f;
  
  /** Tenured generation. */
  static final float DEFAULT_TENURED_FACTOR = 0.50f;
  
  /** Permanent generation. */
  static final float DEFAULT_PERM_FACTOR = 0.25f;
  
  /** External storage. */
  static final float DEFAULT_EXT_STORAGE_FACTOR = 0.0f;

  /** Statistics thread. */
  static final int statThreadPeriod = 60 * 5;

  /** Young generation cache. All new blocks go to this cache first*/
  private final OffHeapCache youngGenCache;
  
  /** Blocks get promoted to this cache when some condition are met. */
  private final OffHeapCache tenGenCache;
  /** Permanent. All table with inMemory = true have their blocks cached directly here*/
  private final OffHeapCache permGenCache;
  
  /** External storage handle cache. */
  private OffHeapCache extStorageCache;


  /** Cache statistics. */
  private final CacheStats stats;

  /** Maximum allowable size of cache (block put if size > max, evict). */
  private long maxSize;

  /** Approximate block size. */
  private long blockSize;

  /** Single access bucket size. */
  private float youngGenFactor;

  /** Multiple access bucket size. */
  private float tenGenFactor;

  /** In-memory bucket size. */
  private float permGenFactor;
  
  /** External storage handle cache factor. */
  
  private float extStorageFactor;

  /**
   *  Data overflow to external storage.
   */
  private boolean overflowExtEnabled = false;
  
  /** external storage (file or network - based). */
  
  private ExtStorage storage;
  
  /** The deserializer. */
  private AtomicReference<CacheableDeserializer<Cacheable>> deserializer = 
	  new AtomicReference<CacheableDeserializer<Cacheable>>();

  /**
   * Instantiates a new off heap block cache.
   *
   * @param conf the conf
   */
  public OffHeapBlockCacheOld(Configuration conf)
  {
	    this.blockSize = conf.getInt("hbase.offheapcache.minblocksize",
	            HFile.DEFAULT_BLOCKSIZE);  
	 
	    CacheConfiguration cacheCfg = ConfigHelper.getCacheConfiguration(conf);
	    maxSize = cacheCfg.getMaxGlobalMemory();
	    if(maxSize == 0){
	    	// USE max memory
	    	maxSize = cacheCfg.getMaxMemory();
	    	LOG.warn("[OffHeapBlockCache] Gloabal max memory is not specified, using max memory instead.");
	    }
	    if(maxSize == 0){
	    	LOG.fatal(CacheConfiguration.MAX_GLOBAL_MEMORY +" is not specified.");
	    	throw new RuntimeException("[OffHeapBlockCache]"+CacheConfiguration.MAX_GLOBAL_MEMORY +" is not specified.");
	    }
	    
	    //TODO make sure sum == 1
	    youngGenFactor = conf.getFloat(YOUNG_GEN_FACTOR, DEFAULT_YOUNG_FACTOR);
	    tenGenFactor   = conf.getFloat(TENURED_GEN_FACTOR, DEFAULT_PERM_FACTOR);
	    permGenFactor  = conf.getFloat(PERM_GEN_FACTOR, DEFAULT_PERM_FACTOR);
	    extStorageFactor = conf.getFloat(EXT_STORAGE_FACTOR, DEFAULT_EXT_STORAGE_FACTOR);
	    overflowExtEnabled = conf.getBoolean(OVERFLOW_TO_EXT_STORAGE_ENABLED, false);
	    
	    
	    long youngSize =  (long) (youngGenFactor * maxSize);
	    long tenSize   =  (long) (tenGenFactor * maxSize);
	    long permSize  =  (long) (permGenFactor * maxSize); 
	    long extStorageSize = (long)(extStorageFactor * maxSize);
	    
	    /** Possible values: none, snappy, gzip, lz4 */
	    // TODO: LZ4 is not supported on all platforms
	    CodecType youngGenCodec = CodecType.LZ4;	    
	    CodecType tenGenCodec = CodecType.LZ4;
	    CodecType permGenCodec = CodecType.LZ4;
	    CodecType extStorageCodec = CodecType.LZ4; 
	    
	    String value = conf.get(YOUNG_GEN_COMPRESSION);
	    if(value != null){
	    	youngGenCodec = CodecType.valueOf(value.toUpperCase());
	    }
	    
	    value = conf.get(TENURED_GEN_COMPRESSION);
	    if(value != null){
	    	tenGenCodec = CodecType.valueOf(value.toUpperCase());
	    }
	    
	    value = conf.get(PERM_GEN_COMPRESSION);
	    if(value != null){
	    	permGenCodec = CodecType.valueOf(value.toUpperCase());
	    }
	    
	    value = conf.get(EXT_STORAGE_COMPRESSION);
	    if( value != null){
	    	extStorageCodec = CodecType.valueOf(value.toUpperCase());
	    }
	    
	    
	    try {
			//TODO - Verify we have deep enough copy
	    	CacheConfiguration youngCfg = cacheCfg.copy();
			youngCfg.setMaxMemory(youngSize);
			// Disable disk persistence for young gen
			//TODO - Do we really need disabling
			//youngCfg.setDataStoreConfiguration(null);
			
			// TODO - enable exceed over limit mode
			
			//youngCfg.setCompressionEnabled(youngGenCodec !=CodecType.NONE);
			youngCfg.setCodecType(youngGenCodec);
			String name = youngCfg.getCacheName();
			youngCfg.setCacheName(name+"_young");
			
			setBucketNumber(youngCfg);
			
			CacheConfiguration tenCfg   = cacheCfg.copy();
			tenCfg.setMaxMemory(tenSize);
			// TODO - enable exceed over limit mode
			//tenCfg.setCompressionEnabled(tenGenCodec != CodecType.NONE);
			tenCfg.setCodecType(tenGenCodec);
			name = tenCfg.getCacheName();
			tenCfg.setCacheName(name+"_tenured");
			
			setBucketNumber(tenCfg);
			
			CacheConfiguration permCfg  = cacheCfg.copy();
			
			permCfg.setMaxMemory(permSize);
			// TODO - enable exceed over limit mode
			//permCfg.setCompressionEnabled(permGenCodec != CodecType.NONE);
			permCfg.setCodecType(permGenCodec);
			name = permCfg.getCacheName();
			permCfg.setCacheName(name+"_perm");
			
			setBucketNumber(permCfg);
			
			CacheManager manager = CacheManager.getInstance();
			//TODO add ProgressListener
			youngGenCache = manager.getCache(youngCfg, null);
			// TODO - do we need this?
			//youngGenCache.setEvictionAuto(false);
			tenGenCache = manager.getCache(tenCfg, null);
			// TODO - do we need this?
			//tenGenCache.setEvictionAuto(false);
			permGenCache = manager.getCache(permCfg, null);
			// TODO - do we need this?			
			//permGenCache.setEvictionAuto(false);
			if(overflowExtEnabled == true){
				LOG.info("Overflow to external storage is enabled.");
		    	// External storage handle cache
				CacheConfiguration extStorageCfg  = cacheCfg.copy();
				
				permCfg.setMaxMemory(extStorageSize);
				permCfg.setCodecType(extStorageCodec);
				name = permCfg.getCacheName();
				permCfg.setCacheName(name+"_ext");
				// calculate bucket number
				// 50 is estimate of a record size
				int buckets =  (extStorageSize / 50) > 
				Integer.MAX_VALUE? Integer.MAX_VALUE -1: (int) (extStorageSize / 50);				
				extStorageCfg.setBucketNumber(buckets);
				extStorageCache = manager.getCache(extStorageCfg, null);
				// Initialize external storage
				storage = ExtStorageManager.getInstance().getStorage(conf, extStorageCache);
		    } else{
		      LOG.info("Overflow to external storage is disabled.");		    	
		    }
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
	    
		this.stats = new CacheStats();
		
		EvictionListener listener = new EvictionListener(){

			@Override
			public void evicted(long ptr, Reason reason, long nanoTime) {
				stats.evict();
				stats.evicted();
				
			}
			
		};
		
		youngGenCache.setEvictionListener(listener);
		// TODO separate eviction listener		
		tenGenCache.setEvictionListener(listener);
		permGenCache.setEvictionListener(listener);
		
		// Cacheable serializer registration
		
		CacheableSerializer serde = new CacheableSerializer();
		
		youngGenCache.getSerDe().registerSerializer(serde);
		tenGenCache.getSerDe().registerSerializer(serde);
		permGenCache.getSerDe().registerSerializer(serde);
		
		if( extStorageCache != null){
			StorageHandleSerializer serde2 = new StorageHandleSerializer();
			extStorageCache.getSerDe().registerSerializer(serde2);
		}
		
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
	  	case DEFLATE : return 4.0f;
	  	default: return 1.0f;
	 }
  }
  
 
  // BlockCache implementation


  /**
   * Get the maximum size of this cache.
   * @return max size in bytes
   */
  public long getMaxSize() {
    return this.maxSize;
  }



  /*
   * Statistics thread.  Periodically prints the cache statistics to the log.
   */
  /**
   * The Class StatisticsThread.
   */
  static class StatisticsThread extends Thread {
    
    /** The cache. */
    OffHeapBlockCacheOld cache;

    /**
     * Instantiates a new statistics thread.
     *
     * @param cache the cache
     */
    public StatisticsThread(OffHeapBlockCacheOld cache) {
      super("LruBlockCache.StatisticsThread");
      setDaemon(true);
      this.cache = cache;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Thread#run()
     */
    @Override
    public void run() {
      cache.logStats();
    }
  }

  /**
   * Log stats.
   */
  public void logStats() {
    if (!LOG.isDebugEnabled()) return;
    // Log size
    long totalSize = getCurrentSize();
    long freeSize = maxSize - totalSize;
    OffHeapBlockCacheOld.LOG.debug("LRU Stats: " +
        "total=" + StringUtils.byteDesc(totalSize) + ", " +
        "free=" + StringUtils.byteDesc(freeSize) + ", " +
        "max=" + StringUtils.byteDesc(this.maxSize) + ", " +
        "blocks=" + size() +", " +
        "accesses=" + stats.getRequestCount() + ", " +
        "hits=" + stats.getHitCount() + ", " +
        "hitRatio=" + StringUtils.formatPercent(stats.getHitRatio(), 2) + "%, "+
        "cachingAccesses=" + stats.getRequestCachingCount() + ", " +
        "cachingHits=" + stats.getHitCachingCount() + ", " +
        "cachingHitsRatio=" +
          StringUtils.formatPercent(stats.getHitCachingRatio(), 2) + "%, " +

        "evicted=" + getEvictedCount() );

  }

 // HeapSize implementation - returns zero
  // as since we do not consume heap
  /* (non-Javadoc)
  * @see org.apache.hadoop.hbase.io.HeapSize#heapSize()
  */
 public long heapSize() {
    return 0;
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
			
			// TODO remove these checks?
			if (inMemory) {
				contains = permGenCache.contains(blockName);
			} else {
				contains =  tenGenCache.contains(blockName);
				if ( !contains) {
					contains =  youngGenCache.contains(blockName);
				}
			}

			if ( contains) {
			  // TODO - what does it mean? Can we ignore this?
				throw new RuntimeException("Cached an already cached block: "+blockName);
			}
			if (inMemory) {
				permGenCache.put(blockName, buf);
				storeExternalWithCodec(blockName, buf, true);
			} else{
				youngGenCache.put(blockName, buf);
			}
		} catch (Exception e) {
			LOG.error(e);
			throw new RuntimeException(e);
		}	  
  }

  
  /**
   * Store external.
   *
   * @param blockName the block name
   * @param buf the buf
   * @param inMemory the in memory
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("unused")
  private void storeExternal(String blockName, Cacheable buf, boolean inMemory) throws IOException{
	  // If external storage is disable - bail out
	  if(overflowExtEnabled == false) return;
	  // Check if we have  already this block in external storage cache
	  if(extStorageCache.contains(blockName)) return;

	  ByteBuffer buffer = extStorageCache.getLocalBufferWithAddress().getBuffer();
	  deserializer.set(buf.getDeserializer());
	  buffer.clear();

	  buffer.position(4);
	  buffer.put( inMemory? (byte) 1: (byte) 0);
	  buf.serialize(buffer);
	  buffer.putInt(0, buffer.position() - 4);
	  
	  StorageHandle handle = storage.storeData(buffer);
	
	  try{
		  extStorageCache.put(blockName, handle);
	  } catch(Exception e){
		  throw new IOException(e);
	  }
	
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
		// Check if we have already this block in external storage cache
		if (extStorageCache.contains(hashed)){  
		  return;
		}

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

		StorageHandle handle = storage.storeData(buffer);

		try {
			// WE USE byte array as a key
			extStorageCache.put(hashed, handle);
		} catch (Exception e) {
			throw new IOException(e);
		}
	
  }
  
  /**
   * Gets the ext storage cache.
   *
   * @return the ext storage cache
   */
  public OffHeapCache getExtStorageCache()
  {
	  return extStorageCache;
  }
  /**
   * Read external.
   *
   * @param blockName the block name
   * @return the cacheable
   * @throws IOException Signals that an I/O exception has occurred.
   */
  @SuppressWarnings("unused")
  private Cacheable readExternal(String blockName) throws IOException
  {
	  if(overflowExtEnabled == false) return null;
	  // Check if we have  already this block in external storage cache
	  try {
		StorageHandle handle = (StorageHandle) extStorageCache.get(blockName);
		if( handle == null ) return null;
		ByteBuffer buffer = extStorageCache.getLocalBufferWithAddress().getBuffer();
		
		buffer.clear();
		
		StorageHandle newHandle = storage.getData(handle, buffer);
		int size = buffer.getInt(0);
		if(size == 0) return null;
		boolean inMemory = buffer.get(4) == (byte) 1;
		buffer.position(5);
		buffer.limit(size + 4);
		if(deserializer.get() == null) return null;
		CacheableDeserializer<Cacheable> deserializer = this.deserializer.get();
		Cacheable obj = deserializer.deserialize(buffer);
		if(inMemory){
			permGenCache.put(blockName, obj);
		} else{
			tenGenCache.put(blockName, obj);
		}
		
		if( newHandle.equals(handle) == false){
			extStorageCache.put(blockName, newHandle);
		}
		
		return obj;
		
	  } catch (NativeMemoryException e) {
		  throw new IOException(e);
	  }
	  
  }
  
  /**
   * Read external with codec.
   *
   * @param blockName the block name
   * @return the cacheable
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private Cacheable readExternalWithCodec(String blockName) throws IOException
  {
	  if(overflowExtEnabled == false) return null;
	  // Check if we have  already this block in external storage cache
	  try {
		// We use 16 - byte hash for external storage cache  
		byte[] hashed = Utils.hash128(blockName);  
		StorageHandle handle = (StorageHandle) extStorageCache.get(hashed);
		if( handle == null ) return null;
		ByteBuffer buffer = extStorageCache.getLocalBufferWithAddress().getBuffer();
		SerDe serde = extStorageCache.getSerDe();
		@SuppressWarnings("unused")
    Codec codec = extStorageCache.getCompressionCodec();
		
		buffer.clear();
		
		StorageHandle newHandle = storage.getData(handle, buffer);
		if(buffer.position() > 0) buffer.flip();
		int size = buffer.getInt();
		if(size == 0) return null;
		// Skip key
		int keySize = buffer.getInt();
		buffer.position(8 + keySize);
		boolean inMemory = buffer.get() == (byte) 1;
		
		//buffer.position(5);
		buffer.limit(size + 4);
	    Cacheable obj = (Cacheable) serde.readCompressed(buffer/*, codec*/);		
		if(inMemory){
			permGenCache.put(blockName, obj);
		} else{
			tenGenCache.put(blockName, obj);
		}		
		if( newHandle.equals(handle) == false){
			extStorageCache.put(hashed, newHandle);
		}
		
		return obj;
		
	  } catch (NativeMemoryException e) {
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
			
			Cacheable bb = (Cacheable)tenGenCache.get(blockName);  
			if(bb == null){
				bb = (Cacheable)permGenCache.get(blockName);
			}
			if( bb == null){
				bb = (Cacheable)youngGenCache.get(blockName);
				if(bb != null){
					// Add to tenured Gen
					tenGenCache.put(blockName, bb);
					storeExternalWithCodec(blockName, bb, false);
					// TODO: do we have to remove it explicitly?
					// youngGenCache.remove(blockName);

				} else{
					// Try to load from external cache
					bb = readExternalWithCodec(blockName);
				}
				
			}
			if(bb == null) {
				stats.miss(caching);
				return null;
			}
			
			stats.hit(caching);		
			return bb;
		}catch(Exception e)
		{
			throw new RuntimeException(e);
		}
  }

  /**
   * Evict block from cache.
   * @param cacheKey Block to evict
   * @return true if block existed and was evicted, false if not
   */
  public boolean evictBlock(BlockCacheKey cacheKey){
	  // We ignore this as since eviction is automatic
	  // always return true
	  return true;
  }

  /**
   * Evicts all blocks for the given HFile.
   *
   * @param hfileName the hfile name
   * @return the number of blocks evicted
   */
  public int evictBlocksByHfileName(String hfileName){
	  // We ignore this as since eviction is automatic
	  // always return '0' - will it breaks anything?
	  return 0;
  }

  /**
   * Get the statistics for this block cache.
   * @return Stats
   */
  public CacheStats getStats(){
	  return this.stats;
  }

  /**
   * Shutdown the cache.
   */
  public void shutdown(){
	    // Shutdown all caches
	    try {
			youngGenCache.shutdown();
		    tenGenCache.shutdown();
		    permGenCache.shutdown();
		    if(extStorageCache != null){
		    	extStorageCache.shutdown();
		    }
		} catch (KodaException e) {
			LOG.error(e);
		}
  }

  /**
   * Returns the total size of the block cache, in items.
   * @return size of cache, in bytes
   */
  public long size(){
	  return youngGenCache.size() + tenGenCache.size() +permGenCache.size() ;
  }

  /**
   * Returns the free size of the block cache, in bytes.
   * @return free space in cache, in bytes
   */
  public long getFreeSize(){
	  return getMaxSize() - getCurrentSize();
  }

  /**
   * Returns the occupied size of the block cache, in bytes.
   * @return occupied space in cache, in bytes
   */
  public long getCurrentSize(){
	    return youngGenCache.getTotalAllocatedMemorySize() + 
		tenGenCache.getTotalAllocatedMemorySize() +
		     permGenCache.getTotalAllocatedMemorySize();
  }

  /**
   * Returns the number of evictions that have occurred.
   * @return number of evictions
   */
  public long getEvictedCount(){
	  return youngGenCache.getEvictedCount() + tenGenCache.getEvictedCount() + permGenCache.getEvictedCount();
  }

  /**
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
}

