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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.koda.integ.hbase.blockcache.OffHeapBlockCache;
import com.koda.integ.hbase.storage.FileExtStorage;

// TODO: Auto-generated Javadoc
/**
 * The Class BlockCacheSimpleRegionTests.
 */
public class BlockCacheStorageSimpleRegionTests extends TestCase{
  
  /** The Constant LOG. */
  final static Log LOG = LogFactory.getLog(BlockCacheStorageSimpleRegionTests.class);
  
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
  private static long cacheSize = 50000000L; // 50M
  
  private static long extRefCacheSize = 5000000;// 5M
  
  private static long fileStoreSize = 5000000000L; // 5G
  
  private static long fileSizeLimit = 50000000;
  
  private static String dataDir = "/tmp/ramdisk/data";
  
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
    
    region = TEST_UTIL.createTestRegion(TABLE_NAME, desc);            
    populateData();    
    cache = new CacheConfig(conf).getBlockCache();
    LOG.info("Block cache: "+ cache.getClass().getName()+ "Size="+cache.getCurrentSize());


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
      LOG.info("Compaction starts");
      region.compactStores(true);
      LOG.info("Compaction finished");
      
    }

    static long startTestTime;
    
    /**
     * _test sequential read.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void testSequentialRead() throws IOException
    {
      
      startTestTime = System.currentTimeMillis();
      
      LOG.info("Seq read starts");
      int nulls = 0;
      long start= System.currentTimeMillis();
      for(int i=0; i < N ; i++){
        Get get = new Get((ROW_PREFIX+i).getBytes());
        get.addFamily(CF);
        Result r = region.get(get);
        if(r.isEmpty()) nulls++;
        if( i % 100000 == 0){
          LOG.info("read "+ i+" nulls ="+nulls);
        }
      }
      LOG.info("Read "+N+" kvs in "+(System.currentTimeMillis() -start)+"ms. NULLS="+nulls);;
      assertEquals(0, nulls);
      logCacheStats();
    }
    
    /**
     * _test random read.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void testRandomRead() throws IOException
    {
      LOG.info("Random read starts");
      int nulls = 0;
      Random rnd = new Random();
      long start= System.currentTimeMillis();
      for(int i=0; i < N ; i++){
        int k = rnd.nextInt(N);
        Get get = new Get((ROW_PREFIX+k).getBytes());
        get.addFamily(CF);

        Result r = region.get(get);
        if(r.isEmpty()) nulls++;
        if( i % 100000 == 0){
          LOG.info("read "+ i+" nulls ="+nulls);
        }
      }
      LOG.info("Read "+N+" kvs in "+(System.currentTimeMillis() -start)+"ms. NULLS="+nulls);;
      assertEquals(0, nulls);
      logCacheStats();
    }
  
    /**
     * Test store scanner.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void testStoreScanner() throws IOException
    {
      LOG.info("Test store scanner");
      Scan scan = new Scan();
      scan.setStartRow(region.getStartKey());
      scan.setStopRow(region.getEndKey());
      Store store = region.getStore(CF);
      StoreScanner scanner = new StoreScanner(store,  store.getScanInfo(), scan,  null);
      long start = System.currentTimeMillis();
      int total = 0;
      List<Cell> result = new ArrayList<Cell>();
      while(scanner.next(result)){
        total++; result.clear();
      }
      
      LOG.info("Test store scanner finished. Found "+total +" in "+(System.currentTimeMillis() - start)+"ms");
      LOG.info("cache hits ="+cache.getStats().getHitCount()+" miss="+cache.getStats().getMissCount());

    }
    
    /**
     * Test region scanner.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void testRegionScanner() throws IOException
    {
      LOG.info("Test Region scanner");
      Scan scan = new Scan();
      scan.setStartRow(region.getStartKey());
      scan.setStopRow(region.getEndKey());
      RegionScanner scanner = region.getScanner(scan);
      //Store store = region.getStore(CF);
      //StoreScanner scanner = new StoreScanner(store,  store.getScanInfo(), scan,  null);
      long start = System.currentTimeMillis();
      int total = 0;
      List<Cell> result = new ArrayList<Cell>();
      while(scanner.next(result)){
        total++; result.clear();
      }
      
      LOG.info("Test Region scanner finished. Found "+total +" in "+(System.currentTimeMillis() - start)+"ms");
      LOG.info("cache hits ="+cache.getStats().getHitCount()+" miss="+cache.getStats().getMissCount());

    }
    
    /**
     * _test store scanner after compaction.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void testStoreScannerAfterCompaction() throws IOException
    {
      LOG.info("Test store scanner after compaction");
      LOG.info("Compaction starts");
      region.compactStores(true);
      LOG.info("Compaction finished");
      
      Scan scan = new Scan();
      scan.setStartRow(region.getStartKey());
      scan.setStopRow(region.getEndKey());
      scan.setCacheBlocks(true);
      Store store = region.getStore(CF);
      StoreScanner scanner = new StoreScanner(store,  store.getScanInfo(), scan,  null);
      long start = System.currentTimeMillis();
      int total = 0;
      List<Cell> result = new ArrayList<Cell>();
      while(scanner.next(result)){
        total++; result.clear();
      }
      
      LOG.info("Test store scanner finished. Found "+total +" in "+(System.currentTimeMillis() - start)+"ms");
      LOG.info("cache hits ="+cache.getStats().getHitCount()+" miss="+cache.getStats().getMissCount());
      scan = new Scan();
      scan.setStartRow(region.getStartKey());
      scan.setStopRow(region.getEndKey());
      scan.setCacheBlocks(true);
      //Store store = region.getStore(CF);
      scanner = new StoreScanner(store,  store.getScanInfo(), scan,  null);
      start = System.currentTimeMillis();
      total = 0;
      result = new ArrayList<Cell>();
      while(scanner.next(result)){
        total++; result.clear();
      }
      
      LOG.info("Test store scanner finished (2). Found "+total +" in "+(System.currentTimeMillis() - start)+"ms");
      LOG.info("cache hits ="+cache.getStats().getHitCount()+" miss="+cache.getStats().getMissCount());
      
    }
    
    /**
     * _test store file scanner.
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void testStoreFileScanner() throws IOException
    {
        LOG.info("StoreFileScanner full starts");
        long start = System.currentTimeMillis();
        Map<byte[], Store> storeMap = region.getStores();
        Collection<Store> stores = storeMap.values();
        Store store = stores.iterator().next();
        Collection<StoreFile> files = store.getStorefiles();
        start = System.currentTimeMillis();        
        int count = 0;
        for(StoreFile file: files){
          LOG.info(file.getPath());
          StoreFile.Reader reader = file.createReader();
          StoreFileScanner scanner = reader.getStoreFileScanner(false, false);          
          scanner.seek(KeyValue.LOWESTKEY);
          while(scanner.next() != null){
            count++; 
          }
          scanner.close();
          reader.close(false);
        }
        
        long end = System.currentTimeMillis();
        LOG.info("StoreFileScanner full finished in "+(end-start)+"ms. Found "+count+" records");

        
    }
    
    /**
     * _test random direct scanners.
     * 
     * FAIL after compaction
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void _testRandomDirectScanners() throws IOException
    {
      LOG.info("Random StoreFile scanners . Running "+(N/10)+ " of size "+M+ " scanners");
      Random r = new Random();
      long totalScanned =0;
      long start = System.currentTimeMillis();
      Map<byte[], Store> storeMap = region.getStores();
      Collection<Store> stores = storeMap.values();
      Store store = stores.iterator().next();
      Collection<StoreFile> files = store.getStorefiles();
      StoreFile[] array = new StoreFile[files.size()];
      files.toArray(array);
      for(int i =0; i < N/10; i++){

        StoreFile file = array[r.nextInt(files.size())];        
        byte[] row = (ROW_PREFIX+r.nextInt(N)).getBytes();         
        StoreFile.Reader reader = file.createReader();
        StoreFileScanner scanner = reader.getStoreFileScanner(false, false);
        KeyValue kv = new KeyValue(row, CF, CQQ[0]);
        //LOG.info(i+" Seek "+kv);
        scanner.seek(kv);
        int total = 0;             
        while(total ++ < M && scanner.next() != null){
          totalScanned++; 
        }
        if(i % 100000 == 0 && i > 0){
          LOG.info("Scanner "+i+" scanned="+totalScanned+" avg per scanner="+(totalScanned/i));
        }
        scanner.close();
        reader.close(false);
        
      }
      LOG.info("Random StoreFile scanners done. "+(N/10)+" in "+
          (System.currentTimeMillis() - start)+"ms. Total scanned="+totalScanned+" Avg. ="+((totalScanned * 10)/ N));
    }
    
    /**
     * _test random scanners.
     * 
     * FAIL after compaction
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void _testRandomScanners() throws IOException
    {
      LOG.info("Random Store scanners . Running "+(N/10)+ " of size "+M+ " scanners");
      Random r = new Random();
      long totalScanned =0;
      long start = System.currentTimeMillis();

      Store store = region.getStore(CF);      
      for(int i =0; i < N/10; i++){

        byte[] row = (ROW_PREFIX+r.nextInt(N)).getBytes();  
        Scan scan = new Scan();
        scan.setStartRow(row);     
        //LOG.info(i+" Seek "+ new String(row));

        StoreScanner scanner = new StoreScanner(store,  store.getScanInfo(), scan,  null);

        int total = 0;     
        List<Cell> result = new ArrayList<Cell>();
        while(total ++ < M && scanner.next(result) != false){
          totalScanned++; result.clear();
        }
        if(i % 100000 == 0 && i > 0){
          LOG.info("Scanner "+i+" scanned="+totalScanned+" avg per scanner="+(totalScanned/i));
        }
        scanner.close();
        
      }
      LOG.info("Random Store scanners done. "+(N/10)+" in "+
          (System.currentTimeMillis() - start)+"ms. Total scanned="+totalScanned+" Avg. ="+((totalScanned * 10)/ N));
      logCacheStats();
      
      //((OffHeapBlockCache)cache).dumpCacheStats();

      LOG.info("Total test time =" +(System.currentTimeMillis() - startTestTime)+"ms");
      
    }
    
    
    /**
     * Log cache stats.
     */
    private void logCacheStats()
    {
      if(cache instanceof OffHeapBlockCache ){
        ((OffHeapBlockCache) cache).dumpCacheStats();
      } else{
        LOG.info("Cache size = "+cache.getCurrentSize()+" hit ratio="+cache.getStats().getHitRatio()+" hit count="+cache.getStats().getHitCount());
      }
    }
    
    /**
     * The main method.
     *
     * @param args the arguments
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static void main(String[] args) throws IOException
    {
      BlockCacheStorageSimpleRegionTests test = new BlockCacheStorageSimpleRegionTests();
      test.setUp();
      //test._testRandomRead();
      for(int i=0; i < 1000; i++){
        //test.testStoreFileScanner();
      }
    }
}
