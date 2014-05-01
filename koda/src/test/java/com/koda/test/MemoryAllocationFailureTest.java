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

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.koda.KodaException;
import com.koda.NativeMemory;
import com.koda.NativeMemoryException;
import com.koda.cache.CacheManager;
import com.koda.cache.OffHeapCache;
import com.koda.config.CacheConfiguration;

public class MemoryAllocationFailureTest extends TestCase{
  private final static Logger LOG = Logger
  .getLogger(MemoryAllocationFailureTest.class);
  
  static OffHeapCache sCache;
  static int N = 1000000;
  static int M = 10;
  static int failedIndex ;
  /** The SIZE. */
  private static int SIZE = 250;
  static long memoryLimit = 250000000L;
  static{
    try {
      initCache();
    } catch (KodaException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @SuppressWarnings("deprecation")
  private static void initCache() throws KodaException {
    CacheManager manager = CacheManager.getInstance();
    CacheConfiguration config = new CacheConfiguration();
    config.setBucketNumber(N);
    config.setMaxMemory(memoryLimit);
    config.setEvictionPolicy("LRU");
    config.setCandidateListSize(15);
    config.setCacheName("test");
    
    //System.setProperty("koda.malloc.debug", "true");
    
    sCache = manager.createCache(config);
    
    // Increase Max memory
    sCache.setMemoryLimit(memoryLimit * 2);
    
    LOG.info("Eviction policy="+sCache.getEvictionAlgo().getClass().getName());
  }
  

  public void testBulkPutInitial() throws NativeMemoryException {
    
    LOG.info("Test Bulk Put:" + Thread.currentThread().getName());
    String key = "key";
    long t1 = System.currentTimeMillis();

    byte[] buffer = new byte[SIZE];
    org.yamm.util.Utils.memset(buffer, (byte)5);
    int i = 0;
    try {

      
      for ( ; i < N; i++) {
        String s = key + i;
        sCache.put(s, buffer);
        Object value = sCache.get(s);
        if( value == null){
          LOG.info("Failed on i="+i + " Avg record size ="+ ((sCache.getAllocatedMemorySize() )/ i)+"b");
          failedIndex  = i;
          break;
        } else if( i > 0  && (i % 100000 == 0)){
          LOG.info("stored "+i+" objects");
        }
        
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
    long t2 = System.currentTimeMillis();
    LOG.info(Thread.currentThread().getName() + "-" + i + " puts in "
        + (t2 - t1) + " ms" + "; cache size =" + sCache.size()
        + "\n Memory =" + sCache.getTotalAllocatedMemorySize()+
        " Malloc allocd :"+NativeMemory.getMalloc().memoryAllocated()+
        " Native memory alloc =" + NativeMemory.getTotalAllocatedMemory());
  }
  
  /**
   * This test works only if memory allocator allows allocation failure
   * 
   * @throws NativeMemoryException
   * @throws IOException
   */
  public void _testVerifyRemoveOnFailedUpdate() throws NativeMemoryException, IOException
  {
    LOG.info("Test verify remove on failed update:" + Thread.currentThread().getName());

    // check that key is in the cache
    String vkey = "key"+(N -1);
    assertNotNull(sCache.get(vkey));
    
    // Now update with new SIZE which is greater
    byte[] buffer = new byte[2*SIZE];
    org.yamm.util.Utils.memset(buffer, (byte)5);
    
    sCache.put(vkey, buffer);
    
    assertNull(sCache.get(vkey));
    
    LOG.info("Test finished OK");
    
  }
  
  public void testVerifyCompactionWorks() throws NativeMemoryException {
    
    LOG.info("Test verify compaction works:" + Thread.currentThread().getName());
    String key = "key";
    long t1 = System.currentTimeMillis();

    byte[] buffer = new byte[2*SIZE];
    org.yamm.util.Utils.memset(buffer, (byte)5);
    int i = 0;
    try {
      for ( ; i < N; i++) {
        String s = key + i;
        sCache.remove(s);
        if( i > 0  && (i % 100000 == 0)){
          LOG.info("removed "+i+" objects");
        }
      }
      
      LOG.info(Thread.currentThread().getName() + "-" + i + " removes in "
          + (System.currentTimeMillis() - t1) + " ms" + "; cache size =" + sCache.size()
          + "\n Memory =" + sCache.getTotalAllocatedMemorySize()+
          " Malloc allocd :"+NativeMemory.getMalloc().memoryAllocated()+
          " Native memory alloc =" + NativeMemory.getTotalAllocatedMemory());     
      
      t1 = System.currentTimeMillis();
      
      i = 0;
      for ( ; i < N; i++) {
        String s = key + i;
        sCache.put(s, buffer);
        Object value = sCache.get(s);
        if( value == null){
          LOG.info("Failed on i="+i + " Avg record size ="+ ((sCache.getAllocatedMemorySize() )/ i)+"b");
          failedIndex  = i;
          break;
        } else if( i > 0  && (i % 100000 == 0)){
          LOG.info("stored "+i+" objects");
        }
        
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
    long t2 = System.currentTimeMillis();
    LOG.info(Thread.currentThread().getName() + "-" + i + " puts in "
        + (t2 - t1) + " ms" + "; cache size =" + sCache.size()
        + "\n Memory =" + sCache.getTotalAllocatedMemorySize()+
        " Malloc allocd :"+NativeMemory.getMalloc().memoryAllocated()+
        " Native memory alloc =" + NativeMemory.getTotalAllocatedMemory());
  }
  

}
