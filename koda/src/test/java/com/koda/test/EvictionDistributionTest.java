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
package com.koda.test;

import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.koda.IOUtils;
import com.koda.KodaException;
import com.koda.NativeMemoryException;
import com.koda.cache.CacheManager;
import com.koda.cache.CacheScanner;
import com.koda.cache.OffHeapCache;
import com.koda.cache.eviction.EvictionAlgo;
import com.koda.common.util.NumericHistogram;
import com.koda.config.CacheConfiguration;

public class EvictionDistributionTest extends TestCase{
  private final static Logger LOG = Logger
  .getLogger(EvictionDistributionTest.class);
  
  static OffHeapCache sCache;
  static int N = 1450000;
  static int M = 10;
  /** The SIZE. */
  private static int SIZE = 100;
  static long memoryLimit = 2500000000L;
  static{
    try {
      initCache();
    } catch (KodaException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private static void initCache() throws KodaException {
    CacheManager manager = CacheManager.getInstance();
    CacheConfiguration config = new CacheConfiguration();
    config.setBucketNumber(N);
    config.setMaxMemory(memoryLimit);
    config.setEvictionPolicy("LRU");
    config.setHistogramEnabled(true);
    config.setHistogramUpdateInterval(1000);
    config.setCandidateListSize(50);
    config.setPreevictionListSize(20);
    //config.setDefaultExpireTimeout(10);
    config.setCacheName("test");
    sCache = manager.createCache(config);
    LOG.info("Eviction policy="+sCache.getEvictionAlgo().getClass().getName());
  }
   
  
  public void testBulkPut() throws NativeMemoryException {
    
    LOG.info("Test Bulk Put:" + Thread.currentThread().getName());
    String key = "key";
    long t1 = System.currentTimeMillis();

    byte[] buffer = new byte[SIZE];
    org.yamm.util.Utils.memset(buffer, (byte)5);
    try {


      for (int i = 0; i < N; i++) {
        String s = key + i;
        sCache.put(s, buffer);
        if( i % 1000 == 0){
          Thread.sleep(7);
        }
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
    long t2 = System.currentTimeMillis();
    LOG.info(Thread.currentThread().getName() + "-" + N + " puts in "
        + (t2 - t1) + " ms" + "; cache size =" + sCache.size()
        + " Memory =" + sCache.getAllocatedMemorySize());
  }
  
 
  
   static NumericHistogram cacheHist, hist;
  

   
    public void testAnalyze() throws NativeMemoryException, IOException {
      LOG.info("Test Analyze Cache size =" + sCache.size() + ": "
          + Thread.currentThread().getName());

      LOG.info("Build histogram for core cache:");
      long address = sCache.getBufferAddress();
      int bucketSize  = sCache.getTotalBuckets(); 

      cacheHist = new NumericHistogram();
      cacheHist.allocate(sCache.getCacheConfiguration().getHistogramBins());
      hist = new NumericHistogram();
      hist.allocate(sCache.getCacheConfiguration().getHistogramBins());
      EvictionAlgo algo = sCache.getEvictionAlgo();
      
      CacheScanner scanner = sCache.getScanner(0, 1);
      int total = 0;
      long t1 = System.currentTimeMillis();
      while(scanner.hasNext()){
        long ptr = scanner.nextPointer();          
        if(ptr > 0) total++;
        long time = algo.translate(IOUtils.getUInt(OffHeapCache.getRealAddress(ptr), 4));
        cacheHist.add(time);
      }
      LOG.info("Cache scanned. Histogram built. Found objects:"+total+" in "+(System.currentTimeMillis() - t1)+" ms");
      
      //int total = sCache.getCacheConfiguration().getHistogramSamples();
      int scanned = 0, found=0;
      for(int i=0; i < bucketSize; i++){
        long ptr = IOUtils.getLong(address, i * 8);
        scanned++;
        if(ptr != 0){
          found++;
          long time = algo.translate(IOUtils.getUInt(OffHeapCache.getRealAddress(ptr), 4));
          hist.add(time);
          //if( found == total) break;
        }
      }
      
      LOG.info("Overall cache: \n"+ cacheHist.toStringLong(10)+
          "\nMax age: "+(long)(cacheHist.quantile(1.) - cacheHist.quantile(0.)));
      LOG.info("Core cache (scanned="+scanned+": found:"+found+")  : \n" + 
          hist.toStringLong(10)+"\nMax age: "+(long)(hist.quantile(1.) - hist.quantile(0.)));
    }
    
    public void testScanner() throws NativeMemoryException, IOException{
      LOG.info("Test scanner started.");  
      CacheScanner scanner = sCache.getScanner(0, 1);
      int total = 0;
      long start = System.currentTimeMillis();
      while(scanner.hasNext()){
          Object key = scanner.nextKey();  
          if(key != null) total++;
      }
      LOG.info("Test scanner finished in :"+(System.currentTimeMillis() - start)+"ms. Total objects found=" + total);
    }
    
    public void testRandomSelections()
    {
      _testRandom(15);
      _testRandom(30);
      _testRandom(50);
      _testRandom(100);
      _testRandom(300);
      _testRandom(500);
      _testRandom(1000);

    }
    
    public void _testRandom(int K)
    {
      int M = 100;
      
      LOG.info("Random selection "+K+": 100 times Average starts" );
      long[] times = new long[M];
      for(int i=0; i < M; i++){
        long minTime = Long.MAX_VALUE;
        for(int j =0; j < K; j++){
          long t = randomAccessTimeFromCore();
          if( t < minTime) minTime = t;
        }
        
        times[i] = minTime;
      }
      
      long avg = avg(times);
      long max = (long)cacheHist.quantile(1.);
      long min = (long)cacheHist.quantile(0.);
      long cmax = (long)hist.quantile(1.);
      long cmin = (long)hist.quantile(0.);

      
      LOG.info("Random selection "+K+" finished. Q="+(double)(avg - min)/(max -min)+ " Q-CORE="+(double)(avg - min)/(cmax -cmin));
    }
    
    private final long avg(long[] arr){
      long total = 0;
      for(int i=0; i < arr.length; i++){
        total+= arr[i];
      }
      return total/arr.length;
    }
    
    private long randomAccessTimeFromCore()
    {
      long address = sCache.getBufferAddress();
      int bucketSize  = sCache.getTotalBuckets(); 
      int start = new Random().nextInt(bucketSize);
      EvictionAlgo algo = sCache.getEvictionAlgo();

      int count =0, i = start;
      while(count++ < bucketSize){
        long ptr = IOUtils.getLong(address, i * 8);
        if(ptr != 0){
          long time = algo.translate(IOUtils.getUInt(OffHeapCache.getRealAddress(ptr), 4));
          return time;
        } else{
          i++; 
          if(i == bucketSize) i = 0;
        }
        
      }
      return 0;
    }
}
