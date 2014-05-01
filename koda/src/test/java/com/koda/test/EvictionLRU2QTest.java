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

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.koda.KodaException;
import com.koda.NativeMemoryException;
import com.koda.cache.CacheManager;
import com.koda.cache.OffHeapCache;
import com.koda.common.util.NumericHistogram;
import com.koda.config.CacheConfiguration;

public class EvictionLRU2QTest extends TestCase{
  private final static Logger LOG = Logger
  .getLogger(EvictionLRU2QTest.class);
  
  static OffHeapCache sCache;
  static int N = 1450000;
  static int M = 10;
  /** The SIZE. */
  private static int SIZE = 100;
  static long memoryLimit = 250000000L;
  static float min = 0.4f, max = 0.5f;

 
  
  public void testAll() throws KodaException, NativeMemoryException
  {
    singleTest(0.);
    singleTest(0.05);
    singleTest(0.1);
    singleTest(0.15);
    singleTest(0.20);
    singleTest(0.25);
    singleTest(0.30);
    singleTest(0.35);
    singleTest(0.40);
    singleTest(0.45);
    singleTest(0.5);
    singleTest(0.75);
    singleTest(1);
    
  }
  
  public void singleTest(double insertPoint) throws KodaException, NativeMemoryException
  {

    
    LOG.info("Running test for insert_point="+insertPoint);
    min = 1 - (float)(insertPoint*1.25);
    max = 1 - (float) insertPoint + 0.1f;
    sCache = createCache(insertPoint);
    _testBulkPut();
    NumericHistogram hist = sCache.getObjectHistogram();
    LOG.info("\n"+hist.toStringLong(20));
    
    _testPutTrash();
    hist = sCache.getObjectHistogram();
    LOG.info("\n"+hist.toStringLong(20));
    
    _testCheckAlgorithm();
    LOG.info("Running test for insert_point="+insertPoint+
        " finished. Eviction attemts="+sCache.getTotalEvictionAttempts()+" succeded "+sCache.getEvictedCount());
  }
   
  private static OffHeapCache createCache(double insertPoint) throws KodaException{
    CacheManager manager = CacheManager.getInstance();
    CacheConfiguration config = new CacheConfiguration();
    config.setBucketNumber(N);
    config.setMaxMemory(memoryLimit);
    config.setEvictionPolicy("LRU2Q");
    config.setHistogramEnabled(true);
    // Set low histogram update for test
    config.setHistogramUpdateInterval(100);
    config.setCandidateListSize(15);
    config.setLRU2QInsertPoint(insertPoint);
    config.setCacheName("test");
    sCache = manager.createCache(config);
    LOG.info("Eviction policy="+sCache.getEvictionAlgo().getClass().getName());
    return sCache;
  }
  
  
  
  @SuppressWarnings("static-access")
  public void _testBulkPut() throws NativeMemoryException {
    
    LOG.info("Test Bulk Put:" + Thread.currentThread().getName());
    String key = "key";
    long t1 = System.currentTimeMillis();

    byte[] buffer = new byte[SIZE];
    org.yamm.util.Utils.memset(buffer, (byte)5);
    int i = 0; 
    try {
     while(sCache.isEvictionActive() == false){
        String s = key + i;
        sCache.put(s, buffer);
        if( i % 1000 == 0){
          // Do periodic sleep to have a better time distribution in a cache
          Thread.sleep(7);
        }
        i++;
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
    N = i;
    long t2 = System.currentTimeMillis();
    LOG.info(Thread.currentThread().getName() + "-" + N + " puts in "
        + (t2 - t1) + " ms" + "; cache size =" + sCache.size()
        + " Memory =" + sCache.getAllocatedMemorySize());
    sCache.dumpStats();
    sCache.resetStats();
  }
  
  @SuppressWarnings("static-access")
  public void _testPutTrash(){
    // We trash already filled cache with batch of objects. LRU2Q must kick in and save
    // top 50% of a cache.
    LOG.info("Test Trash Put:" + Thread.currentThread().getName());
    String key = "KEY";
    long t1 = System.currentTimeMillis();

    byte[] buffer = new byte[SIZE];
    org.yamm.util.Utils.memset(buffer, (byte)5);
    int i = 0; 
    try {
     for(; i < N; i++){
        String s = key + i;
        sCache.put(s, buffer);
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
    
    long t2 = System.currentTimeMillis();
    LOG.info(Thread.currentThread().getName() + "-" + N + " trash puts in "
        + (t2 - t1) + " ms" + "; cache size =" + sCache.size()
        + " Memory =" + sCache.getAllocatedMemorySize());
    sCache.dumpStats();
    sCache.resetStats();
  }
  
 public void _testCheckAlgorithm() throws NativeMemoryException
 {
   LOG.info("Test Check Algorithm :" + Thread.currentThread().getName());
   String key = "key";
   long t1 = System.currentTimeMillis();

   byte[] buffer = new byte[SIZE];
   org.yamm.util.Utils.memset(buffer, (byte)5);
   int i = 0, nulls =0; 
   try {
    for(; i < N; i++){
       String s = key + i;
       Object v = sCache.get(s);
       if(v == null) nulls ++;
     }
   } catch (Throwable e) {
     e.printStackTrace();
   }
   
   long t2 = System.currentTimeMillis();
   float R = ((float)(N- nulls)/N);
   LOG.info(Thread.currentThread().getName() + "-" + N + " gets in "
       + (t2 - t1) + " ms" + "; cache size =" + sCache.size()
       + " Memory =" + sCache.getAllocatedMemorySize()+" found nulls="+nulls+" R="+R);
  // assertTrue (R > min && R < max);
   t1 = System.currentTimeMillis();
   LOG.info("Clear the cache");
   sCache.clear();
   LOG.info("Done in "+(System.currentTimeMillis() - t1)+"ms. Cache size="+sCache.size());
 }
 
}
