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
@SuppressWarnings("unused")
public class CacheBasicTest extends TestCase{
	private final static Logger LOG = Logger
	.getLogger(CacheBasicTest.class);
	
	static OffHeapCache sCache;
	static int N = 1000000;
	static int M = 10;
	/** The SIZE. */
	private static int SIZE = 100;
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
		config.setCacheName("test");
		sCache = manager.createCache(config);
		LOG.info("Eviction policy="+sCache.getEvictionAlgo().getClass().getName());
	}
	
	public void _testSimplePutGet() throws NativeMemoryException, IOException
	{
		System.out.println("Test simple put / get / delete started");
		String key = "key";
		String value = "value";
		
		sCache.put(key, value);
		
		String v = (String)sCache.get(key);
		
		assertEquals(value, v);
		
		sCache.remove(key);
		
		v = (String) sCache.get(key);
		
		assertNull(v);
		System.out.println("Test simple put / get / delete finished");
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
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		long t2 = System.currentTimeMillis();
		LOG.info(Thread.currentThread().getName() + "-" + N + " puts in "
				+ (t2 - t1) + " ms" + "; cache size =" + sCache.size()
				+ " Memory =" + sCache.getAllocatedMemorySize());
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
	
	 public void testBulkPutWithEviction() throws NativeMemoryException {
	    
	    LOG.info("Test Bulk Put with eviction:" + Thread.currentThread().getName());
	    String key = "key";
	    long t1 = System.currentTimeMillis();

	    byte[] buffer = new byte[SIZE];
	    org.yamm.util.Utils.memset(buffer, (byte)5);
	    try {


	      for (int i = 0; i < N * M; i++) {
	        String s = key + i;
	        //sCache.put(s, buffer);
	        put(s, buffer);
	        if(i > 0 && i % 1000000 == 0){
	          LOG.info("inserted "+i+" RPS="+ (long)(i)*1000/(System.currentTimeMillis() - t1)+" obj="+sCache.size()+" evicted="+sCache.getEvictedCount() +" failed put="+sCache.getFailedPuts());
	          LOG.info("allocated="+sCache.getTotalAllocatedMemorySize() + " eviction active="+sCache.isEvictionActive());
	        }
	      }
	    } catch (Throwable e) {
	      e.printStackTrace();
	    }
	    long t2 = System.currentTimeMillis();
	    LOG.info(Thread.currentThread().getName() + "-" + N*M + " puts in "
	        + (t2 - t1) + " ms" + "; cache size =" + sCache.size()
	        + " Memory =" + sCache.getAllocatedMemorySize());
	  }
	 
	  private void put(String key, byte[] buffer) throws NativeMemoryException, IOException
	  {
	    sCache.put(key, buffer);
	  }
	 
	  public void testBulkGetAfterEviction() throws NativeMemoryException, IOException {
	    LOG.info("Test Bulk Get - Random. Cache size =" + sCache.size() + ": "
	        + Thread.currentThread().getName());
	    String key = "key";
	    byte[] buffer;
	    Random r = new Random();
	    int counter = 0;
	    long t1 = System.currentTimeMillis();
	    long size = sCache.size();
	    for (int i = M*N -(int)size; i < N*M; i++) {
	      String s = key + i;
	      buffer = (byte[])sCache.get(s);
	      //assertEquals(0, org.yamm.util.Utils.cmp(buffer, (byte)5));
	      if (buffer == null){
	        counter++;
	      } else{
	        
	      }
	    }
	    long t2 = System.currentTimeMillis();
	    LOG.info(Thread.currentThread().getName() + "-" + sCache.size() + " gets in "
	        + (t2 - t1) + " ms" + "; cache size =" + sCache.size()
	        + "; nulls=" + counter + " R = " +((double)(sCache.size() - counter)/(sCache.size())));
	    LOG.info("Build histogram for core cache:");
	    long address = sCache.getBufferAddress();
	    int bucketSize  = sCache.getTotalBuckets()/ 256; 

	    NumericHistogram cacheHist = sCache.getObjectHistogram();
	    NumericHistogram hist = new NumericHistogram();
	    hist.allocate(sCache.getCacheConfiguration().getHistogramBins());
	    EvictionAlgo algo = sCache.getEvictionAlgo();
	    int total = sCache.getCacheConfiguration().getHistogramSamples();
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
	    
	    LOG.info("Overall cache: \n"+ cacheHist.toStringLong(10));
	    LOG.info("Core cache (scanned="+scanned+": found:"+found+")  : \n" + hist.toStringLong(10));
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
	  
	  public static void main(String[] args) throws NativeMemoryException, IOException
	  {
//	      Runnable r = new Runnable(){
//	        
//	        public void run(){
//	          CacheBasicTest test = new CacheBasicTest();
//	          try {
//              test.testBulkPutWithEviction();
//              test.testBulkGetAfterEviction();  
//            } catch (NativeMemoryException e) {
//              // TODO Auto-generated catch block
//              e.printStackTrace();
//            } catch (IOException e) {
//              // TODO Auto-generated catch block
//              e.printStackTrace();
//            }
//	          
//	        }
//	      };
//	      new Thread(r).start();
	  }
}
