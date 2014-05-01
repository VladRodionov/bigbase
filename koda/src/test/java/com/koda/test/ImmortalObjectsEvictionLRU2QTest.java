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
import com.koda.config.CacheConfiguration;

public class ImmortalObjectsEvictionLRU2QTest extends TestCase{
  private final static Logger LOG = Logger
  .getLogger(ImmortalObjectsEvictionLRU2QTest.class);
  
  static OffHeapCache sCache;
  static int N = 1450000;
  static int M = 10;
  static int totalImmortalObjects;
  /** The SIZE. */
  private static int SIZE = 100;
  static long memoryLimit = 250000000L;
  
  
  public void test50() throws KodaException, NativeMemoryException
  {
    LOG.info("Running test 0.5 insert point (fast eviction)");
    totalImmortalObjects = 0;

    sCache = createCache(0.5f);
    _testBulkPut();    
    _testPutTrash();    
    _testCheckAlgorithm();
    LOG.info("Running test finished. Eviction attemts="+sCache.getTotalEvictionAttempts()+" succeded "+sCache.getEvictedCount());
    
  }
  
  public void test25() throws KodaException, NativeMemoryException
  {
    LOG.info("Running test 0.25 insert point (complex eviction)");
    totalImmortalObjects = 0;
    sCache = createCache(0.25f);
    _testBulkPut();    
    _testPutTrash();    
    _testCheckAlgorithm();
    LOG.info("Running test finished. Eviction attemts="+sCache.getTotalEvictionAttempts()+" succeded "+sCache.getEvictedCount());
    
  }
  
  private static OffHeapCache createCache(float insertPoint) throws KodaException{
    CacheManager manager = CacheManager.getInstance();
    CacheConfiguration config = new CacheConfiguration();
    config.setBucketNumber(N);
    config.setMaxMemory(memoryLimit);
    config.setEvictionPolicy("LRU2Q");
    config.setHistogramEnabled(true);
    config.setLRU2QInsertPoint(insertPoint);
    // Set low histogram update for test
    config.setHistogramUpdateInterval(100);
    config.setCandidateListSize(15);
    config.setCacheName("test");
    sCache = manager.createCache(config);
    LOG.info("Eviction policy="+sCache.getEvictionAlgo().getClass().getName());
    return sCache;
  }
  
  
  
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
        if( i % 4 == 0){
          // Make 25% of objects IMMORTAL
          sCache.put(s, buffer, OffHeapCache.IMMORTAL);
          totalImmortalObjects++; 
        } else{
          sCache.put(s, buffer);
                   
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
        + " Memory =" + sCache.getAllocatedMemorySize()+" total immortals="+totalImmortalObjects);
 
  }
  
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
    for(; i < N; i+= 4){
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
   assertTrue (nulls == 0);
   t1 = System.currentTimeMillis();
   LOG.info("Clear the cache");
   sCache.clear();
   LOG.info("Done in "+(System.currentTimeMillis() - t1)+"ms. Cache size="+sCache.size());
 }
 
}
