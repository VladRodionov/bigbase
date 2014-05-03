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
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

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
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
//import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import com.koda.integ.hbase.blockcache.OffHeapBlockCache;
import com.koda.integ.hbase.storage.FileExtStorage;

// TODO: Auto-generated Javadoc
/**
 * The Class BlockCacheMultithreadedTest.
 */
public class BlockCacheMultithreadedTest extends TestCase{
  
  /** The Constant LOG. */
  final static Log LOG = LogFactory.getLog(BlockCacheMultithreadedTest.class);
  
  /**
   * The Enum TestType.
   */
  static enum TestType{
    
    /** The get. */
    GET, 
 /** The scan. */
 SCAN
  }
  
  /** The Constant THREADS. */
  private final static String THREADS = "-threads";
  
  /** The Constant DURATION. */
  private final static String DURATION = "-duration";
  
  /** The Constant TEST. */
  private final static String TEST     = "-test";
  
  /** The Constant TEST_UTIL. */
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();  
  
  /** The regions. */
  private static HRegion[] regions;
  
  /** The table name. */
  private static String TABLE_NAME ="TEST";
  
  /** The cf. */
  static byte[] CF = "cf".getBytes();
  
  /** The cqq. */
  private static byte[][] CQQ ;  
  
  /** The n. */
  static int N =2000000;
  
  /** The m. */
  static int M = 100;

  /** The row prefix. */
  static String ROW_PREFIX = "";
  
  /** The cache size. */
  private static long cacheSize = 5000000000L; // 5G
  
  /** The cache impl class. */
  private static String cacheImplClass =OffHeapBlockCache.class.getName();//"com.koda.integ.hbase.blockcache.OffHeapBlockCache";
  
  /** The young gen factor. */
  private static Float youngGenFactor = 1.f; // LRU
  
  /** The cache compression. */
  private static String cacheCompression = "None";
  
  /** The cache overflow enabled. */
  private static boolean cacheOverflowEnabled = false; 
  
  private static long extRefCacheSize = 100000000;// 100M
  
  private static long fileStoreSize = 5000000000L; // 5G
  
  private static long fileSizeLimit = 50000000;
  
  private static String dataDir = "/tmp/ramdisk/data";  
  
  /** The on heap cache enabled. */
  private static boolean onHeapCacheEnabled = true;
  
  /** The on heap cache ratio. */
  private static float   onHeapCacheRatio = 0.2f;
  
  /** The block size. */
  private static int BLOCK_SIZE = 16 * 1024;
  
  /** The bloom block size. */
  private static int BLOOM_BLOCK_SIZE = 64 * 1024;
  
  /** The index block size. */
  private static int INDEX_BLOCK_SIZE = 64 * 1024;
  
  /** The num threads. */
  private static int numThreads = 8;
  
  /** The duration. */
  @SuppressWarnings("unused")
  private static int duration   = 600 * 1000; // 
  
  /** The test type. */
  private static TestType testType = TestType.GET;
  
  /** The total ops. */
  static AtomicLong totalOps = new AtomicLong(0);
  
  /** The cache. */
  private static BlockCache cache;
  
  /* (non-Javadoc)
   * @see junit.framework.TestCase#setUp()
   */
  @Override
  protected void setUp() throws IOException
  {
      
    if(regions != null) return;
    regions = new HRegion[numThreads];
    // Init columns
    CQQ = new byte[5][];
    for(int i=0; i < CQQ.length; i++){
      CQQ[i] = ("cq"+i).getBytes();
    }
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(OffHeapBlockCache.BLOCK_CACHE_MEMORY_SIZE, Long.toString(cacheSize));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_IMPL, cacheImplClass);
    conf.set(OffHeapBlockCache.BLOCK_CACHE_YOUNG_GEN_FACTOR, Float.toString(youngGenFactor));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_COMPRESSION, cacheCompression);
    conf.set(OffHeapBlockCache.BLOCK_CACHE_OVERFLOW_TO_EXT_STORAGE_ENABLED, Boolean.toString(cacheOverflowEnabled));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_ONHEAP_ENABLED, Boolean.toString(onHeapCacheEnabled));
    conf.set("io.storefile.bloom.block.size", Integer.toString(BLOOM_BLOCK_SIZE));
    conf.set("hfile.index.block.max.size", Integer.toString(INDEX_BLOCK_SIZE));
    conf.set("hfile.block.cache.size", Float.toString(onHeapCacheRatio));    
    // Enable File Storage
    conf.set(FileExtStorage.FILE_STORAGE_FILE_SIZE_LIMIT, Integer.toString((int)fileSizeLimit));
    conf.set(FileExtStorage.FILE_STORAGE_MAX_SIZE, Long.toString(fileStoreSize));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_OVERFLOW_TO_EXT_STORAGE_ENABLED, Boolean.toString(cacheOverflowEnabled));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_EXT_STORAGE_IMPL, FileExtStorage.class.getName());
    conf.set(FileExtStorage.FILE_STORAGE_BASE_DIR, dataDir);
    conf.set(OffHeapBlockCache.BLOCK_CACHE_TEST_MODE, Boolean.toString(true));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_EXT_STORAGE_MEMORY_SIZE, Long.toString(extRefCacheSize));    
    
    for(int i=0; i < numThreads; i++){
      HColumnDescriptor desc = new HColumnDescriptor(CF);
      desc.setCacheDataOnWrite(true);
      desc.setCacheIndexesOnWrite(true);
      desc.setCacheBloomsOnWrite(true);
      desc.setBlocksize(BLOCK_SIZE);
      desc.setBloomFilterType(BloomType.ROW);

    
      regions[i] = TEST_UTIL.createTestRegion(TABLE_NAME, desc);            
      populateData(regions[i]);  
  
    }
        
    
    cache = new CacheConfig(conf).getBlockCache();
    LOG.info("Block cache: "+ cache.getClass().getName()+ " Size="+cache.getCurrentSize());
    
    for(int i=0; i < numThreads; i++)
    {
      regions[i].compactStores(true);   
      cacheRegion(regions[i]);
    }
    LOG.info("After compact & pre-caching. Block cache: "+ cache.getClass().getName()+ " Size="+cache.getCurrentSize());    
   
    
  }
    
    /**
     * Populate data.
     *
     * @param region the region
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void populateData(HRegion region) throws IOException {

      LOG.info("Populating data ... "+region);
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

 
    /**
     * Cache region.
     *
     * @param region the region
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void cacheRegion(HRegion region) throws IOException
    {
      LOG.info("Cache region starts");
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
      
      LOG.info("Cache region finished. Found "+total +" in "+(System.currentTimeMillis() - start)+"ms");
      //LOG.info("cache hits ="+cache.getStats().getHitCount()+" miss="+cache.getStats().getMissCount());

    }

    
    /**
     * Log cache stats.
     */
    static void logCacheStats()
    {
      LOG.info("Cache size = "+cache.getCurrentSize()+" hit ratio="+cache.getStats().getHitRatio()+" hit count="+cache.getStats().getHitCount());
    }
    
    /**
     * The main method.
     *
     * @param args the arguments
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static void main(String[] args) throws IOException
    {
      final BlockCacheMultithreadedTest test = new BlockCacheMultithreadedTest();
      parseArgs(args);
      test.setUp();
      long startTime = System.currentTimeMillis();

      
      WorkerThread[] workers = new WorkerThread[numThreads];
      for(int i =0; i < numThreads; i++){
        workers[i] = new WorkerThread(regions[i], testType);
        workers[i].start();
      }
      for(int i =0; i < numThreads; i++){
        try {
          workers[i].join();
        } catch (InterruptedException e) {
          LOG.error(e);
        }
      }
      
      LOG.info("Finished "+(totalOps.get()) * 1000 / (System.currentTimeMillis() - startTime)+" OPS");
    
    }
    
    /**
     * Parses the args.
     *
     * @param args the args
     */
    private static void parseArgs(String[] args) {

      int i = 0;
      while (i < args.length) {
        if (args[i].equals(THREADS)) {
          numThreads = Integer.parseInt(args[++i]);
        } else if (args[i].equals(DURATION)) {
          duration = Integer.parseInt(args[++i]) * 1000;
        } else if (args[i].equals(TEST)) {
          String test = args[++i].toUpperCase();
          if(test.equals(TestType.GET.name())){
            testType = TestType.GET;
          } else{
            testType = TestType.SCAN;
          }         
        } 
        i++;
      }
    }
}

class WorkerThread extends Thread
{
  HRegion region;
  BlockCacheMultithreadedTest.TestType type;
  public WorkerThread(HRegion region, BlockCacheMultithreadedTest.TestType type)
  {
    super();
    this.region = region;
    this.type = type;
  }
  
  public void run()
  {
    if (type == BlockCacheMultithreadedTest.TestType.GET){
      try {
        testRandomRead();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } else{
      try {
        testRandomScanners();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
  //static long startTime ;
  
  public void testRandomRead() throws IOException
  {
    //startTime = System.currentTimeMillis();
    BlockCacheMultithreadedTest.LOG.info("Random read starts");
    int nulls = 0;
    Random rnd = new Random();
    long start= System.currentTimeMillis();
    for(int i=0; i < BlockCacheMultithreadedTest.N ; i++){
      int k = rnd.nextInt(BlockCacheMultithreadedTest.N);
      Get get = new Get((BlockCacheMultithreadedTest.ROW_PREFIX+k).getBytes());
      get.addFamily(BlockCacheMultithreadedTest.CF);

      Result r = region.get(get);
      if(r.isEmpty()) nulls++;
      if( i % 100000 == 0 && i > 0){
        BlockCacheMultithreadedTest.LOG.info("read "+ i+" nulls ="+nulls);
      }
      BlockCacheMultithreadedTest.totalOps.incrementAndGet();
    }
    BlockCacheMultithreadedTest.LOG.info("Read "+
        BlockCacheMultithreadedTest.N+" kvs in "+(System.currentTimeMillis() -start)+"ms. NULLS="+nulls);;

    BlockCacheMultithreadedTest.logCacheStats();
  }


 

  
  public void testRandomScanners() throws IOException
  {
    BlockCacheMultithreadedTest.LOG.info("Random Store scanners . Running "+
        (BlockCacheMultithreadedTest.N/10)+ " of size "+BlockCacheMultithreadedTest.M+ " scanners");
    Random r = new Random();
    long totalScanned =0;
    long start = System.currentTimeMillis();

    for(int i =0; i < BlockCacheMultithreadedTest.N/10; i++){

      byte[] row = (BlockCacheMultithreadedTest.ROW_PREFIX+
          r.nextInt(BlockCacheMultithreadedTest.N)).getBytes();  
      Scan scan = new Scan();
      scan.setStartRow(row);        
      Store store = region.getStore(BlockCacheMultithreadedTest.CF);
      StoreScanner scanner = new StoreScanner(store,  store.getScanInfo(), scan,  null);

      int total = 0;     
      List<Cell> result = new ArrayList<Cell>();
      while(total ++ < BlockCacheMultithreadedTest.M && scanner.next(result) != false){
        totalScanned++; result.clear();        
      }
      if(i % 100000 == 0 && i > 0){
        BlockCacheMultithreadedTest.LOG.info("Scanner "+i+" scanned="+totalScanned+" avg per scanner="+(totalScanned/i));
      }
      scanner.close();
      BlockCacheMultithreadedTest.totalOps.incrementAndGet();
    }
    BlockCacheMultithreadedTest.LOG.info("Random Store scanners done. "+
        (BlockCacheMultithreadedTest.N/10)+" in "+
        (System.currentTimeMillis() - start)+"ms. Total scanned="+
        totalScanned+" Avg. ="+((totalScanned * 10)/ BlockCacheMultithreadedTest.N));
    
    //BlockCacheMultithreadedTest.LOG.info("Test run time="+(System.currentTimeMillis() -startTime)+"ms");
  
  }
  
}
