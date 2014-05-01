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
package org.apache.hadoop.hbase.io.hfile;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.util.StringUtils;

import com.koda.integ.hbase.blockcache.OffHeapBlockCache;

// TODO: Auto-generated Javadoc
/**
 * Stores all of the cache objects and configuration for a single HFile.
 */
public class CacheConfig {
  
  /** The Constant LOG. */
  private static final Log LOG = LogFactory.getLog(CacheConfig.class.getName());

  /**
   * Configuration key to cache data blocks on write. There are separate
   * switches for bloom blocks and non-root index blocks.
   */
  public static final String CACHE_BLOCKS_ON_WRITE_KEY =
      "hbase.rs.cacheblocksonwrite";

  /**
   * Configuration key to cache leaf and intermediate-level index blocks on
   * write.
   */
  public static final String CACHE_INDEX_BLOCKS_ON_WRITE_KEY =
      "hfile.block.index.cacheonwrite";

  /**
   * Configuration key to cache compound bloom filter blocks on write.
   */
  public static final String CACHE_BLOOM_BLOCKS_ON_WRITE_KEY =
      "hfile.block.bloom.cacheonwrite";

  /**
   * TODO: Implement this (jgray)
   * Configuration key to cache data blocks in compressed format.
   */
  public static final String CACHE_DATA_BLOCKS_COMPRESSED_KEY =
      "hbase.rs.blockcache.cachedatacompressed";

  /**
   * Configuration key to evict all blocks of a given file from the block cache
   * when the file is closed.
   */
  public static final String EVICT_BLOCKS_ON_CLOSE_KEY =
      "hbase.rs.evictblocksonclose";

  // Defaults

  /** The Constant DEFAULT_CACHE_DATA_ON_READ. */
  public static final boolean DEFAULT_CACHE_DATA_ON_READ = true;
  
  /** The Constant DEFAULT_CACHE_DATA_ON_WRITE. */
  public static final boolean DEFAULT_CACHE_DATA_ON_WRITE = false;
  
  /** The Constant DEFAULT_IN_MEMORY. */
  public static final boolean DEFAULT_IN_MEMORY = false;
  
  /** The Constant DEFAULT_CACHE_INDEXES_ON_WRITE. */
  public static final boolean DEFAULT_CACHE_INDEXES_ON_WRITE = false;
  
  /** The Constant DEFAULT_CACHE_BLOOMS_ON_WRITE. */
  public static final boolean DEFAULT_CACHE_BLOOMS_ON_WRITE = false;
  
  /** The Constant DEFAULT_EVICT_ON_CLOSE. */
  public static final boolean DEFAULT_EVICT_ON_CLOSE = false;
  
  /** The Constant DEFAULT_COMPRESSED_CACHE. */
  public static final boolean DEFAULT_COMPRESSED_CACHE = false;

  /** Local reference to the block cache, null if completely disabled. */
  private final BlockCache blockCache;

  /** Whether blocks should be cached on read (default is on if there is a cache but this can be turned off on a per-family or per-request basis). */
  private boolean cacheDataOnRead;

  /** Whether blocks should be flagged as in-memory when being cached. */
  private final boolean inMemory;

  /** Whether data blocks should be cached when new files are written. */
  private boolean cacheDataOnWrite;

  /** Whether index blocks should be cached when new files are written. */
  private final boolean cacheIndexesOnWrite;

  /** Whether compound bloom filter blocks should be cached on write. */
  private final boolean cacheBloomsOnWrite;

  /** Whether blocks of a file should be evicted when the file is closed. */
  private boolean evictOnClose;

  /** Whether data blocks should be stored in compressed form in the cache. */
  private final boolean cacheCompressed;

  /**
   * Create a cache configuration using the specified configuration object and
   * family descriptor.
   * @param conf hbase configuration
   * @param family column family configuration
   */
  public CacheConfig(Configuration conf, HColumnDescriptor family) {
    this(CacheConfig.instantiateBlockCache(conf),
        family.isBlockCacheEnabled(),
        family.isInMemory(),
        // For the following flags we enable them regardless of per-schema settings
        // if they are enabled in the global configuration.
        conf.getBoolean(CACHE_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_DATA_ON_WRITE) || family.shouldCacheDataOnWrite(),
        conf.getBoolean(CACHE_INDEX_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_INDEXES_ON_WRITE) || family.shouldCacheIndexesOnWrite(),
        conf.getBoolean(CACHE_BLOOM_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_BLOOMS_ON_WRITE) || family.shouldCacheBloomsOnWrite(),
        conf.getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY,
            DEFAULT_EVICT_ON_CLOSE) || family.shouldEvictBlocksOnClose(),
        conf.getBoolean(CACHE_DATA_BLOCKS_COMPRESSED_KEY, DEFAULT_COMPRESSED_CACHE)
     );
  }

  /**
   * Create a cache configuration using the specified configuration object and
   * defaults for family level settings.
   * @param conf hbase configuration
   */
  public CacheConfig(Configuration conf) {
    this(CacheConfig.instantiateBlockCache(conf),
        DEFAULT_CACHE_DATA_ON_READ,
        DEFAULT_IN_MEMORY, // This is a family-level setting so can't be set
                           // strictly from conf
        conf.getBoolean(CACHE_BLOCKS_ON_WRITE_KEY, DEFAULT_CACHE_DATA_ON_WRITE),
        conf.getBoolean(CACHE_INDEX_BLOCKS_ON_WRITE_KEY,
            DEFAULT_CACHE_INDEXES_ON_WRITE),
            conf.getBoolean(CACHE_BLOOM_BLOCKS_ON_WRITE_KEY,
                DEFAULT_CACHE_BLOOMS_ON_WRITE),
        conf.getBoolean(EVICT_BLOCKS_ON_CLOSE_KEY, DEFAULT_EVICT_ON_CLOSE),
        conf.getBoolean(CACHE_DATA_BLOCKS_COMPRESSED_KEY,
            DEFAULT_COMPRESSED_CACHE)
     );
  }

  /**
   * Create a block cache configuration with the specified cache and
   * configuration parameters.
   * @param blockCache reference to block cache, null if completely disabled
   * @param cacheDataOnRead whether data blocks should be cached on read
   * @param inMemory whether blocks should be flagged as in-memory
   * @param cacheDataOnWrite whether data blocks should be cached on write
   * @param cacheIndexesOnWrite whether index blocks should be cached on write
   * @param cacheBloomsOnWrite whether blooms should be cached on write
   * @param evictOnClose whether blocks should be evicted when HFile is closed
   * @param cacheCompressed whether to store blocks as compressed in the cache
   */
  CacheConfig(final BlockCache blockCache,
      final boolean cacheDataOnRead, final boolean inMemory,
      final boolean cacheDataOnWrite, final boolean cacheIndexesOnWrite,
      final boolean cacheBloomsOnWrite, final boolean evictOnClose,
      final boolean cacheCompressed) {
    this.blockCache = blockCache;
    this.cacheDataOnRead = cacheDataOnRead;
    this.inMemory = inMemory;
    this.cacheDataOnWrite = cacheDataOnWrite;
    this.cacheIndexesOnWrite = cacheIndexesOnWrite;
    this.cacheBloomsOnWrite = cacheBloomsOnWrite;
    this.evictOnClose = evictOnClose;
    this.cacheCompressed = cacheCompressed;
  }

  /**
   * Constructs a cache configuration copied from the specified configuration.
   *
   * @param cacheConf the cache conf
   */
  public CacheConfig(CacheConfig cacheConf) {
    this(cacheConf.blockCache, cacheConf.cacheDataOnRead, cacheConf.inMemory,
        cacheConf.cacheDataOnWrite, cacheConf.cacheIndexesOnWrite,
        cacheConf.cacheBloomsOnWrite, cacheConf.evictOnClose,
        cacheConf.cacheCompressed);
  }

  /**
   * Checks whether the block cache is enabled.
   *
   * @return true, if is block cache enabled
   */
  public boolean isBlockCacheEnabled() {
    return this.blockCache != null;
  }

  /**
   * Returns the block cache.
   * @return the block cache, or null if caching is completely disabled
   */
  public BlockCache getBlockCache() {
    return this.blockCache;
  }

  /**
   * Returns whether the blocks of this HFile should be cached on read or not.
   * @return true if blocks should be cached on read, false if not
   */
  public boolean shouldCacheDataOnRead() {
    return isBlockCacheEnabled() && cacheDataOnRead;
  }

  /**
   * Should we cache a block of a particular category? We always cache
   * important blocks such as index blocks, as long as the block cache is
   * available.
   *
   * @param category the category
   * @return true, if successful
   */
  public boolean shouldCacheBlockOnRead(BlockCategory category) {
    boolean shouldCache = isBlockCacheEnabled()
        && (cacheDataOnRead ||
            category == BlockCategory.INDEX ||
            category == BlockCategory.BLOOM);
    return shouldCache;
  }

  /**
   * Checks if is in memory.
   *
   * @return true if blocks in this file should be flagged as in-memory
   */
  public boolean isInMemory() {
    return isBlockCacheEnabled() && this.inMemory;
  }

  /**
   * Should cache data on write.
   *
   * @return true if data blocks should be written to the cache when an HFile is
   * written, false if not
   */
  public boolean shouldCacheDataOnWrite() {
    return isBlockCacheEnabled() && this.cacheDataOnWrite;
  }

  /**
   * Only used for testing.
   * @param cacheDataOnWrite whether data blocks should be written to the cache
   *                         when an HFile is written
   */
  public void setCacheDataOnWrite(boolean cacheDataOnWrite) {
    this.cacheDataOnWrite = cacheDataOnWrite;
  }

  /**
   * Should cache indexes on write.
   *
   * @return true if index blocks should be written to the cache when an HFile
   * is written, false if not
   */
  public boolean shouldCacheIndexesOnWrite() {
    return isBlockCacheEnabled() && this.cacheIndexesOnWrite;
  }

  /**
   * Should cache blooms on write.
   *
   * @return true if bloom blocks should be written to the cache when an HFile
   * is written, false if not
   */
  public boolean shouldCacheBloomsOnWrite() {
    return isBlockCacheEnabled() && this.cacheBloomsOnWrite;
  }

  /**
   * Should evict on close.
   *
   * @return true if blocks should be evicted from the cache when an HFile
   * reader is closed, false if not
   */
  public boolean shouldEvictOnClose() {
    return isBlockCacheEnabled() && this.evictOnClose;
  }

  /**
   * Only used for testing.
   * @param evictOnClose whether blocks should be evicted from the cache when an
   *                     HFile reader is closed
   */
  public void setEvictOnClose(boolean evictOnClose) {
    this.evictOnClose = evictOnClose;
  }

  /**
   * Should cache compressed.
   *
   * @return true if blocks should be compressed in the cache, false if not
   */
  public boolean shouldCacheCompressed() {
    return isBlockCacheEnabled() && this.cacheCompressed;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    if (!isBlockCacheEnabled()) {
      return "CacheConfig:disabled";
    }
    return "CacheConfig:enabled " +
      "[cacheDataOnRead=" + shouldCacheDataOnRead() + "] " +
      "[cacheDataOnWrite=" + shouldCacheDataOnWrite() + "] " +
      "[cacheIndexesOnWrite=" + shouldCacheIndexesOnWrite() + "] " +
      "[cacheBloomsOnWrite=" + shouldCacheBloomsOnWrite() + "] " +
      "[cacheEvictOnClose=" + shouldEvictOnClose() + "] " +
      "[cacheCompressed=" + shouldCacheCompressed() + "]";
  }

  // Static block cache reference and methods

  /**
   * Static reference to the block cache, or null if no caching should be used
   * at all.
   */
  private static BlockCache globalBlockCache;

  /** Boolean whether we have disabled the block cache entirely. */
  private static boolean blockCacheDisabled = false;

  /**
   * Returns the block cache or <code>null</code> in case none should be used.
   *
   * @param conf  The current configuration.
   * @return The block cache or <code>null</code>.
   */
  @SuppressWarnings("deprecation")
  private static synchronized BlockCache instantiateBlockCache(
      Configuration conf) {
    if (globalBlockCache != null) return globalBlockCache;
    if (blockCacheDisabled) return null;
    
    long offHeapCacheSize = conf.getLong(OffHeapBlockCache.BLOCK_CACHE_MEMORY_SIZE, 0L);

    if(offHeapCacheSize > 0L ){
      String offHeapCacheImplClassName = 
        conf.get(OffHeapBlockCache.BLOCK_CACHE_IMPL, OffHeapBlockCache.class.getName());    
      try{
        Class<?> cls = Class.forName(offHeapCacheImplClassName);        
        Constructor<?> ctr = cls.getDeclaredConstructor(Configuration.class );
        globalBlockCache = (BlockCache) ctr.newInstance(conf);
        LOG.info("Allocating BigBaseBlockCache with maximum size " +
            StringUtils.humanReadableInt(offHeapCacheSize));        
        return globalBlockCache;
      } catch(Exception e){
        e.printStackTrace();
        LOG.error("Could not instantiate '"+offHeapCacheImplClassName+"' class, will resort to standard cache impl.");
      }
    }
    
    float cachePercentage = conf.getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY,
      HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT);
    if (cachePercentage == 0L) {
      blockCacheDisabled = true;
      return null;
    }
    if (cachePercentage > 1.0) {
      throw new IllegalArgumentException(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY +
        " must be between 0.0 and 1.0, and not > 1.0");
    }

    // Calculate the amount of heap to give the heap.
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    long cacheSize = (long)(mu.getMax() * cachePercentage);    

    if (offHeapCacheSize <= 0) {
      LOG.info("Allocating LruBlockCache with maximum size " +
          StringUtils.humanReadableInt(cacheSize));

      globalBlockCache = new LruBlockCache(cacheSize,
          StoreFile.DEFAULT_BLOCKSIZE_SMALL, conf);
    } 
    return globalBlockCache;
  }
}
