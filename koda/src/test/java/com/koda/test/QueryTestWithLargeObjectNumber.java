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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.koda.KodaException;
import com.koda.NativeMemoryException;
import com.koda.cache.CacheManager;
import com.koda.cache.OffHeapCache;
import com.koda.cache.Query;
import com.koda.cache.Reduce;
import com.koda.cache.Scan;
import com.koda.cache.eviction.EvictionAlgo;
import com.koda.config.CacheConfiguration;
import com.koda.util.Configuration;

// TODO: Auto-generated Javadoc
/**
 * The Class OffHeapHashMapPerfTest.
 */
public class QueryTestWithLargeObjectNumber {



	/** The N. */
	public static int N = 10 * 1024 * 1024;
	public static int BUCKETS = 3000000;

	/** The SIZE. */
	private static int SIZE = 10;
	/** The Constant LOG. */

	private final static Logger LOG = Logger
			.getLogger(QueryTestWithLargeObjectNumber.class);


	/** The m cache. */
	private static OffHeapCache mCache;


	/**
	 * Test bulk put bytes.
	 *
	 * @param key the key
	 * @param n the n
	 * @throws NativeMemoryException the j emalloc exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void testBulkPutBytes(String key, int n)
			throws NativeMemoryException, IOException {
		LOG.info("Test Bulk Put Bytes:" + Thread.currentThread().getName()+" n="+n);

		byte[] buffer = new byte[SIZE];
		byte[] keySuff = key.getBytes();
		byte[] keyBuf = new byte[keySuff.length+4];
		System.arraycopy(keySuff, 0, keyBuf, 4, keySuff.length);
		
		long t1 = System.currentTimeMillis();
		for (int i = 0; i < n; i++) {

			byte[] bkey = getKey(i, keyBuf);//s.getBytes();
			mCache.put(bkey, buffer);
			if( i % 100000 == 0){
			  LOG.info(Thread.currentThread().getName()+" loaded "+i+"/"+n);
			}
		}
		long t2 = System.currentTimeMillis();
		LOG.info(Thread.currentThread().getName() + "-" + n + " puts in "
				+ (t2 - t1) + " ms" + "; cache size =" + mCache.size()
				+ " memory =" + mCache.getAllocatedMemorySize());
	}

	/**
	 * Gets the key.
	 *
	 * @param i the i
	 * @param key the key
	 * @return the key
	 */
	static byte[] getKey(int i, byte[] key)
	{
		key[0] = (byte)(i>>>24);
		key[1] = (byte)(i>>>16);
		key[2] = (byte)(i>>>8);
		key[3] = (byte)(i);
		return key;
	}
	
	
	
   /**
    * Gets the key long.
    *
    * @param i the i
    * @param key the key
    * @return the key long
    */
   static byte[] getKeyLong(long i, byte[] key)
   {
	   key[0] = (byte) (i >>> 56);
	   key[1] = (byte) (i >>> 48);
	   key[2] = (byte) (i >>> 40);
	   key[3] = (byte) (i >>> 32);
	   key[4] = (byte) (i >>> 24);
	   key[5] = (byte) (i >>> 16);
	   key[6] = (byte) (i >>> 8);
	   key[7] = (byte) (i);
	   return key;
   }
	
	

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws NativeMemoryException the j emalloc exception
	 * @throws KodaException the koda exception
	 */
	public final static void main(String[] args) throws NativeMemoryException, KodaException {

		int number = 4;// number of threads
		long memoryLimit = 1200000000; 
			
		if (args.length > 0) {
			number = Integer.parseInt(args[0]);
			if (args.length > 1) {
				N = Integer.parseInt(args[1]);
			}
			if (args.length > 2) {
				memoryLimit = Long.parseLong(args[2]);
			}
		}

		CacheManager manager = CacheManager.getInstance();
		CacheConfiguration config = new CacheConfiguration();
		config.setBucketNumber(BUCKETS);
		config.setMaxMemory(memoryLimit);
		config.setEvictionPolicy("LRU");
		config.setDefaultExpireTimeout(10);
		config.setCacheName("test");
		//config.setMaxQueryProcessors(1);
		mCache = manager.createCache(config);	

		String[] keyPrefix = new String[number];
		Random r = new Random();
		for (int i = 0; i < number; i++) {
			keyPrefix[i] = "Thread[" + r.nextInt(1024 * 1024) + "]";
		}

		int opNumber = N / number;

		//while(true){ // Infinite Loop
			long t1 = System.currentTimeMillis();

			waitToFinish(startTest(TestType.PUT_BYTES, keyPrefix, number, opNumber));
		
			long t2 = System.currentTimeMillis();
			//		
			LOG.info("Total=" + (t2 - t1) + " ms");
			EvictionAlgo algo = mCache.getEvictionAlgo();
			LOG.info("Eviction stats:");
			LOG.info("  number of attempts =" + algo.getTotalEvictionAttempts());
			LOG.info("  number of evicted  =" + algo.getTotalEvictedItems());
			LOG.info("\n Query started ");
			t1 = System.currentTimeMillis();
			Query<Long> query = new Query<Long>(null, Count.class, new Sum());
			long total =0;
			
			try {
				total = query.execute(mCache, new Configuration());
			} catch (ExecutionException e) {
				LOG.error(e);
			} catch (InterruptedException e) {				
				LOG.error(e);
			}
			t2 = System.currentTimeMillis();
			if(N != total){
				LOG.error("FAILED: total ="+total+" expected "+N);
			} else{
				LOG.info("Total entries ="+total+" Time="+(t2-t1)+" ms");
			}
			
			System.exit(0); // We need to shutdown ExecutorService
			
		//}

	}


	/**
	 * The Class Sum.
	 */
	@SuppressWarnings("serial")
    public static class Sum implements Reduce<Long>
	{

		/* (non-Javadoc)
		 * @see com.koda.cache.Reduce#reduce(java.util.List)
		 */
		@Override
		public Long reduce(List<Long> results) {
			long sum = 0;
			for(Long l: results) sum+= l;
			return sum;
		}

		/* (non-Javadoc)
		 * @see com.koda.cache.Reduce#configure(com.koda.util.Configuration)
		 */
		@Override
		public void configure(Configuration cfg) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	/**
	 * The Class Count.
	 */
	public static class Count implements Scan<Long>
	{

		/** The count. */
		long count;
		
		/** The start time. */
		long startTime;
		
		/**
		 * Instantiates a new count.
		 */
		public Count(){
			startTime = System.currentTimeMillis();
		}
		
		/* (non-Javadoc)
		 * @see com.koda.cache.Scan#getResult()
		 */
		@Override
		public Long getResult() {			
			return count;
		}

		/* (non-Javadoc)
		 * @see com.koda.cache.Scan#scan(long)
		 */
		@Override
		public final void scan(final long ptr) {
			count++;			
		}

		/* (non-Javadoc)
		 * @see com.koda.cache.Scan#finish()
		 */
		@Override
		public void finish() {
		LOG.info("Scan finished in :"+(System.currentTimeMillis() - startTime)+" ms");
			
		}

		/* (non-Javadoc)
		 * @see com.koda.cache.Scan#configure(com.koda.util.Configuration)
		 */
		@Override
		public void configure(Configuration cfg) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	
	/**
	 * Wait to finish.
	 *
	 * @param threads the threads
	 */
	static void waitToFinish(Thread[] threads)
	{
		for (int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (Exception e) {
				// ignore
			}
		}
	}
	
	
	/**
	 * Start test.
	 *
	 * @param type the type
	 * @param keyPrefix the key prefix
	 * @param number the number
	 * @param opNumber the op number
	 * @return the execute thread[]
	 */
	static ExecuteThread[] startTest(TestType type, String[] keyPrefix, int number, int opNumber)
	{
		ExecuteThread[] threadArray = new ExecuteThread[number];
		for (int i = 0; i < number; i++) {
			threadArray[i] = new ExecuteThread(type, keyPrefix[i],
					opNumber);
			threadArray[i].start();
		}
		
		return threadArray;
	}
	
	/**
	 * Start test scan.
	 *
	 * @param type the type
	 * @param keyPrefix the key prefix
	 * @param number the number
	 * @return the execute thread[]
	 */
	static ExecuteThread[] startTestScan(TestType type, String[] keyPrefix, int number)
	{
		ExecuteThread[] threadArray = new ExecuteThread[number];
		for (int i = 0; i < number; i++) {
			threadArray[i] = new ExecuteThread(type, keyPrefix[i], i,
					number);
			threadArray[i].start();
		}
		
		return threadArray;
	}
	
	/**
	 * Test scan.
	 * 
	 */

	/**
	 * The Class ExecuteThread.
	 */
	static class ExecuteThread extends Thread {

		/** The m type. */
		TestType mType;

		/** The total number of queris. */
		int n;

		/** The m thread index. */
		int mThreadIndex;

		/** The m total threads. */
		int mTotalThreads;

		/**
		 * Instantiates a new execute thread.
		 * 
		 * @param type
		 *            the type
		 * @param keyPrefix
		 *            the key prefix
		 * @param n
		 *            the n
		 */
		public ExecuteThread(TestType type, String keyPrefix, int n) {
			super(keyPrefix);
			mType = type;
			this.n = n;

		}

		/**
		 * Instantiates a new execute thread.
		 * 
		 * @param type
		 *            the type
		 * @param keyPrefix
		 *            the key prefix
		 * @param index
		 *            the index
		 * @param total
		 *            the total
		 */
		public ExecuteThread(TestType type, String keyPrefix, int index, int total) {
			super(keyPrefix);
			mType = type;
			mThreadIndex = index;
			mTotalThreads = total;

		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Thread#run()
		 */
		public void run() {
			try {
				switch (mType) {
	
				case PUT_BYTES:
					testBulkPutBytes(getName(), n);
					break;
					
				}
			} catch (Exception e) {
				LOG.error(e);
			}
		}

	}
}
