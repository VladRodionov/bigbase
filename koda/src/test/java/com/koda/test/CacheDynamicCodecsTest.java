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

import com.esotericsoftware.minlog.Log;
import com.koda.KodaException;
import com.koda.NativeMemoryException;
import com.koda.cache.CacheManager;
import com.koda.cache.CacheScanner;
import com.koda.cache.OffHeapCache;
import com.koda.compression.Codec;
import com.koda.compression.CodecFactory;
import com.koda.compression.DeflateCodec;
import com.koda.config.CacheConfiguration;
@SuppressWarnings("unused")
public class CacheDynamicCodecsTest extends TestCase{
  private final static Logger LOG = Logger
  .getLogger(CacheDynamicCodecsTest.class);
  
  static OffHeapCache sCache;
  static int N = 500000;
  static int M = 10;
  /** The SIZE. */
  private static int SIZE = 1000;
  static long memoryLimit = 250000000L;
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
    config.setCandidateListSize(15);
    config.setPreevictionListSize(20);
    //config.setDefaultExpireTimeout(10);
    // NO EXPLICIT COMPRESSION SET
    config.setCacheName("test");
    sCache = manager.createCache(config);
    LOG.info("Eviction policy="+sCache.getEvictionAlgo().getClass().getName());
  }
  

  public void _testDeflate() throws NativeMemoryException, IOException{
    LOG.info("Test Deflate:" + Thread.currentThread().getName());
    
    DeflateCodec codec = new DeflateCodec();
    byte[] buffer = new byte[SIZE];
    Random r = new Random();
    try{
      org.yamm.util.Utils.memset(buffer, (byte)5);
      sCache.put("test", buffer, codec);

      Object value = sCache.get("test");
    
    } finally{
      sCache.remove("test");
    }
    
    LOG.info("Test Deflate finished");

  }
  
  
  public void testBulkPut() throws NativeMemoryException {
    
    LOG.info("Test Bulk Put:" + Thread.currentThread().getName());
    String key = "key";
    long t1 = System.currentTimeMillis();
    Codec[] codecs = CodecFactory.getCodecs();
    byte[] buffer = new byte[SIZE];
    Random r = new Random();
    org.yamm.util.Utils.memset(buffer, (byte)5);
    try {

      for (int i = 0; i < N; i++) {
        String s = key + i;        
        sCache.put(s, buffer, codecs[r.nextInt(codecs.length)]);
        if( i > 0 && (i % 100000) == 0){
          LOG.info("Put "+i);
        }
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
    long t2 = System.currentTimeMillis();
    LOG.info(Thread.currentThread().getName() + "-" + N + " puts in "
        + (t2 - t1) + " ms" + "; cache size =" + sCache.size()
        + " Memory =" + sCache.getAllocatedMemorySize());
    
    LOG.info("Codecs stats:");
    for(Codec c: codecs){
      Log.info(c.getType()+" ratio="+ c.getAvgCompressionRatio());
    }
  }
  
  public void testBulkGet() throws NativeMemoryException, IOException {
    LOG.info("Test Bulk Get - Random. Cache size =" + sCache.size() + ": "
        + Thread.currentThread().getName());
    String key = "key";
    byte[] buffer;
    Random r = new Random();
    int counter = 0;
    long t1 = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      //int k = r.nextInt(N);
      String s = key + i;
      buffer = (byte[])sCache.get(s);
      assertEquals(0, org.yamm.util.Utils.cmp(buffer, (byte)5));
      if (buffer == null){
        counter++;
      } else{
        
      }
    }
    long t2 = System.currentTimeMillis();
    LOG.info(Thread.currentThread().getName() + "-" + N + " gets in "
        + (t2 - t1) + " ms" + "; cache size =" + sCache.size()
        + "; nulls=" + counter);
    assertEquals(0, counter);
  }
  
  public void testScanner() throws NativeMemoryException, IOException{
    LOG.info("Test scanner started.");  
    CacheScanner scanner = sCache.getScanner(0, 1);
    int total = 0;
    long start = System.currentTimeMillis();
    while(scanner.hasNext()){
        //Object key = scanner.nextKey(); 
        Object value = scanner.nextValue();
        if(value != null) total++;
    }
    assertEquals(N, total);
    LOG.info("Test scanner finished in :"+(System.currentTimeMillis() - start)+"ms. Total objects found=" + total);
  }
  
  
  public void testShortScanners() throws NativeMemoryException, IOException{
    LOG.info("Test short scanners started.");
    int totalBuckets = sCache.getTotalBuckets();
    int total = 0;
    int start = 0;
    int range = 2;
    long startTime = System.currentTimeMillis();
    while( start < totalBuckets){
      int end = Math.min(start + range -1, totalBuckets -1);    
      CacheScanner scanner = sCache.getRangeScanner(start, end);
      while(scanner.hasNext()){
        //Object key = scanner.nextKey(); 
        //Object value = scanner.nextValue();
        long ptr = scanner.nextPointer();
        if(ptr != 0L) {
//          CodecType type = sCache.getCodecTypeForRecord(ptr);
//          LOG.info(type);
          total++;        }
      }
      scanner.close();
      start += range;
    }
    
    assertEquals(N, total);
    LOG.info("Test short scanner finished in :"+(System.currentTimeMillis() - startTime)+"ms. Total objects found=" + total);
  }
  
  public void testBulkRemove() throws NativeMemoryException, IOException {
    LOG.info("Test Bulk Remove. Cache size =" + sCache.size() + ": "
        + Thread.currentThread().getName());
    String key = "key";
    long t1 = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      
      String s = key + i;
      sCache.remove(s);
    }
    long t2 = System.currentTimeMillis();
    LOG.info(Thread.currentThread().getName() + "-" + N + " removes in "
        + (t2 - t1) + " ms" + "; cache size =" + sCache.size());

  }
  
  public void testBulkGetAgain() throws NativeMemoryException, IOException {
    LOG.info("Test Bulk Get Again. Cache size =" + sCache.size() + ": "
        + Thread.currentThread().getName());
    String key = "key";
    byte[] buffer;
    int counter = 0;
    long t1 = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {     
      String s = key + i;
      buffer = (byte[])sCache.get(s);
      if (buffer == null)
        counter++;
    }
    long t2 = System.currentTimeMillis();
    LOG.info(Thread.currentThread().getName() + "-" + N + " gets in "
        + (t2 - t1) + " ms" + "; cache size =" + sCache.size()
        + "; nulls=" + counter);
    assertEquals(N, counter);
  }
  

   

   

    


}
