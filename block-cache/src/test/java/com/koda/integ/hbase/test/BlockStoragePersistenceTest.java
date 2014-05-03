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
package com.koda.integ.hbase.test;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import com.koda.cache.CacheManager;
import com.koda.integ.hbase.blockcache.OffHeapBlockCache;
import com.koda.integ.hbase.storage.FileExtStorage;

// TODO: Auto-generated Javadoc
/**
 * The Class BlockCacheSimpleRegionTests.
 */
public class BlockStoragePersistenceTest extends TestCase{
  
  /** The Constant LOG. */
  final static Log LOG = LogFactory.getLog(BlockStoragePersistenceTest.class);
  
  /** The Constant TEST_UTIL. */
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();  
  
  /** The region. */
  private static HRegion region;
  
  /** The table name. */
  private static String TABLE_NAME ="TEST";
  
  /** The cf. */
  private static byte[] CF = "cf".getBytes();
  
  /** The cqq. */
  private static byte[][] CQQ ;
  
  /** The n. */
  static int N =2000000;
  
  /** The m. */
  static int M = 100;

  /** The row prefix. */
  static String ROW_PREFIX = "";
  
  /** The cache size. */
  private static long cacheSize = 500000000L; // 50M
  
  private static long extRefCacheSize = 5000000;// 5M
  
  private static long fileStoreSize = 5000000000L; // 5G
  
  private static long fileSizeLimit = 50000000;
  
  private static String dataDir = "/tmp/ramdisk/data";
  
  private static String cacheDataDir = "/tmp/ramdisk/sys";
  
  /** The cache impl class. */
  private static String cacheImplClass = OffHeapBlockCache.class.getName();  
  /** The young gen factor. */
  private static Float youngGenFactor = 1.f;
  
  /** The cache compression. */
  private static String cacheCompression = "LZ4";
  
  /** The cache overflow enabled. */
  @SuppressWarnings("unused")
  private static boolean cacheOverflowEnabled = false; 
  
  /** The block size. */
  private static int BLOCK_SIZE = 16 * 1024;
  
  /** The bloom block size. */
  private static int BLOOM_BLOCK_SIZE = 4 * 1024;
  
  /** The cache. */
  private static BlockCache cache;
  
  private static long blockCacheSize;
  
  /* (non-Javadoc)
   * @see junit.framework.TestCase#setUp()
   */
  @Override
  protected void setUp() throws IOException
  {
      
    if(region != null) return;
    // Init columns
    CQQ = new byte[5][];
    for(int i=0; i < CQQ.length; i++){
      CQQ[i] = ("cq"+i).getBytes();
    }
    
    HColumnDescriptor desc = new HColumnDescriptor(CF);
    desc.setCacheDataOnWrite(true);
    desc.setCacheIndexesOnWrite(true);
    desc.setCacheBloomsOnWrite(true);
    desc.setBlocksize(BLOCK_SIZE);
    desc.setBloomFilterType(BloomType.ROW);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(OffHeapBlockCache.BLOCK_CACHE_MEMORY_SIZE, Long.toString(cacheSize));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_IMPL, cacheImplClass);
    conf.set(OffHeapBlockCache.BLOCK_CACHE_YOUNG_GEN_FACTOR, Float.toString(youngGenFactor));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_COMPRESSION, cacheCompression);
    //conf.set(OffHeapBlockCache.BLOCK_CACHE_OVERFLOW_TO_EXT_STORAGE_ENABLED, Boolean.toString(cacheOverflowEnabled));
    conf.set("io.storefile.bloom.block.size", Integer.toString(BLOOM_BLOCK_SIZE));
    conf.set("hfile.block.cache.size", "0.5");
    
    // Enable File Storage
    conf.set(FileExtStorage.FILE_STORAGE_FILE_SIZE_LIMIT, Integer.toString((int)fileSizeLimit));
    conf.set(FileExtStorage.FILE_STORAGE_MAX_SIZE, Long.toString(fileStoreSize));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_OVERFLOW_TO_EXT_STORAGE_ENABLED, Boolean.toString(true));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_EXT_STORAGE_IMPL, FileExtStorage.class.getName());
    conf.set(FileExtStorage.FILE_STORAGE_BASE_DIR, dataDir);
    conf.set(OffHeapBlockCache.BLOCK_CACHE_TEST_MODE, Boolean.toString(true));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_EXT_STORAGE_MEMORY_SIZE, Long.toString(extRefCacheSize));
    
    // Enable persistence
    conf.set(OffHeapBlockCache.BLOCK_CACHE_DATA_ROOTS, cacheDataDir);
    conf.set(OffHeapBlockCache.BLOCK_CACHE_PERSISTENT, Boolean.toString(true));
    
    region = TEST_UTIL.createTestRegion(TABLE_NAME, desc);            
    populateData();    
    cache = new CacheConfig(conf).getBlockCache();
    LOG.info("Block cache: "+ cache.getClass().getName()+ "Size="+cache.getCurrentSize());
    LOG.info("Shutting down the region");
    long startTime = System.currentTimeMillis();
    blockCacheSize = ((OffHeapBlockCache)cache).getExtStorageCache().size();
    cache.shutdown();
    //region.close();
    LOG.info("Done in "+ (System.currentTimeMillis() - startTime));

  }
    
  public void testCacheLoad()
  {
    CacheManager.getInstance().clearCaches();
    
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(OffHeapBlockCache.BLOCK_CACHE_MEMORY_SIZE, Long.toString(cacheSize));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_IMPL, cacheImplClass);
    conf.set(OffHeapBlockCache.BLOCK_CACHE_YOUNG_GEN_FACTOR, Float.toString(youngGenFactor));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_COMPRESSION, cacheCompression);
    conf.set("io.storefile.bloom.block.size", Integer.toString(BLOOM_BLOCK_SIZE));
    conf.set("hfile.block.cache.size", "0.5");
    
    // Enable File Storage
    conf.set(FileExtStorage.FILE_STORAGE_FILE_SIZE_LIMIT, Integer.toString((int)fileSizeLimit));
    conf.set(FileExtStorage.FILE_STORAGE_MAX_SIZE, Long.toString(fileStoreSize));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_OVERFLOW_TO_EXT_STORAGE_ENABLED, Boolean.toString(true));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_EXT_STORAGE_IMPL, FileExtStorage.class.getName());
    conf.set(FileExtStorage.FILE_STORAGE_BASE_DIR, dataDir);
    conf.set(OffHeapBlockCache.BLOCK_CACHE_TEST_MODE, Boolean.toString(true));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_EXT_STORAGE_MEMORY_SIZE, Long.toString(extRefCacheSize));
    
    // Enable persistence
    conf.set(OffHeapBlockCache.BLOCK_CACHE_DATA_ROOTS, cacheDataDir);
    conf.set(OffHeapBlockCache.BLOCK_CACHE_PERSISTENT, Boolean.toString(true));
    
    LOG.info("Loading new cache ...");
    long startTime = System.currentTimeMillis();
    OffHeapBlockCache newCache = new OffHeapBlockCache(conf);
    LOG.info("Loading new cache done in "+ (System.currentTimeMillis() - startTime)+"ms");
    
    assertEquals(blockCacheSize , newCache.getExtStorageCache().size());
    

    
  }
  
    /**
     * Populate data.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void populateData() throws IOException {

      LOG.info("Populating data ... ");
      for(int i=0; i < N; i++){
        Put p = new Put((ROW_PREFIX+i).getBytes());
        for(int k=0; k < CQQ.length; k++ ){
          p.add(CF, CQQ[k], Bytes.toBytes(i));
        }
        region.put(p);
        if(region.getMemstoreSize().get() > (250 * 1000000)){
          // Flush
          region.flushcache();
        }
        if(i % 100000 == 0){
          LOG.info(i);
        }
      }
      long start = System.currentTimeMillis();
      LOG.info("Memstore size before flush="+region.getMemstoreSize());
      
      boolean result = region.flushcache();
      LOG.info("Memstore size after flush="+region.getMemstoreSize()+" result="+result+" time="+(System.currentTimeMillis() -start));
      try {
          Thread.sleep(1000);
      } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      
    }

    static long startTestTime;
    
    
}
