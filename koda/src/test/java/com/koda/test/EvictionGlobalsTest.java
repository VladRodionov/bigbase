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
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.koda.KodaException;
import com.koda.NativeMemoryException;
import com.koda.cache.CacheManager;
import com.koda.cache.OffHeapCache;
import com.koda.config.CacheConfiguration;

// TODO: Auto-generated Javadoc
/**
 * The Class EvictionGlobalsTest.
 */
public class EvictionGlobalsTest extends TestCase {

	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(EvictionGlobalsTest.class);
	
	/** The key counter. */
	private static AtomicLong keyCounter = new AtomicLong(0);
	
	/** The value. */
	private static byte[] value = new byte[1000];
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	protected void setUp() throws Exception {
		
		super.setUp();
	}
	
	
	/**
	 * Gets the cache.
	 *
	 * @param cacheMax the cache max
	 * @param globalMax the global max
	 * @param totalBuckets the total buckets
	 * @return the cache
	 * @throws NativeMemoryException the native memory exception
	 * @throws KodaException the koda exception
	 */
	private OffHeapCache getCache(long cacheMax, long globalMax, int totalBuckets) throws NativeMemoryException, KodaException {
		CacheManager manager = CacheManager.getInstance();
		CacheConfiguration config = new CacheConfiguration();
		config.setBucketNumber(totalBuckets);
		config.setMaxMemory(cacheMax);
		config.setMaxGlobalMemory(globalMax);
		config.setEvictionPolicy("LRU");
		config.setDefaultExpireTimeout(6000);
		config.setCacheName("test" + (number++));
		config.setSerDeBufferSize(4096);
		return manager.createCache(config);
		
	}
	
	static int number =0;
	/**
	 * Insert rows.
	 *
	 * @param cache the cache
	 * @param num the num
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void insertRows(OffHeapCache cache, int num) throws NativeMemoryException, IOException
	{
		for(int i=0; i < num; i++)
		{
			cache.put(keyCounter.get()+"", value);
			keyCounter.incrementAndGet();
		}
	}
	
	/**
	 * Test global no cache max.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws KodaException the koda exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testGlobalNoCacheMax() throws NativeMemoryException, KodaException, IOException
	{
		// When Global Max is set and cache max is not set
		// cache size must not exceed global max
		LOG.info("Global - No Cache Max started");
		long cacheSize = 0;
		long globalSize = 100000000;
		int buckets = 100000;
		
		// Create cache with 0 size limit and non-zero global limit
		OffHeapCache cache = getCache( cacheSize, globalSize, buckets);
		
		insertRows(cache, 100);
		// Test 1
		assertEquals(100, cache.size());
		insertRows(cache, 400);
		// Test 2
		assertEquals(500, cache.size());
		insertRows(cache, buckets);
		long memSize = cache.getAllocatedMemorySize();
		
		LOG.info("Global="+globalSize+" memSize="+memSize);
		
		assertTrue( Math.abs(memSize - globalSize) < 0.10*globalSize );
		
		insertRows(cache, buckets);
		memSize = cache.getAllocatedMemorySize();
		LOG.info("Global="+globalSize+" memSize="+memSize);
		assertTrue( Math.abs(memSize - globalSize) < 0.10*globalSize );
		
		cache.clear();
		LOG.info("Global - No Cache Max finished");
				
		
	}

	
	/**
	 * Test global with two caches.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws KodaException the koda exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testGlobalWithTwoCaches() throws NativeMemoryException, KodaException, IOException
	{
		// Observe effect of overall allocation exceeds global max limit
		LOG.info("Global - Two caches started");

		long globalSize = 100000000;
		long cacheSize = (long)(0.75 * globalSize);
		int buckets = 100000;
		
		// Create cache with 0 size limit and non-zero global limit
		OffHeapCache cacheOne = getCache( cacheSize, globalSize, buckets);
		OffHeapCache cacheTwo = getCache( cacheSize, globalSize, buckets);
		
		insertRows(cacheOne, (int)(0.75*buckets));
		long oneSize = cacheOne.getAllocatedMemorySize();
		assertTrue(Math.abs(oneSize - cacheSize) < 0.1 * 0.75* globalSize);
		
		insertRows(cacheTwo, (int)(0.75*buckets));
		long twoSize = cacheTwo.getAllocatedMemorySize();
		
		assertTrue(Math.abs(twoSize - 0.25 * globalSize) < 0.15 * 0.25* globalSize);				
		
		LOG.info("Global - Two caches finished. Global="+globalSize+" one="+oneSize+" two="+twoSize);
		
	}
	
	/**
	 * Test manual eviction effect.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws KodaException the koda exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testManualEvictionEffect() throws NativeMemoryException, KodaException, IOException
	{
		// When cache in manual eviction mode
		// cache size will exceed cache max size and WON'T exceed global max
		// When Global Max is set and cache max is not set
		// cache size may  exceed global max as well
		LOG.info("Global - Manual Eviction started");
		long cacheSize = 10000000;
		long globalSize = 100000000;
		int buckets = 100000;
		
		OffHeapCache.resetGlobals();
		
		// Create cache with 0 size limit and non-zero global limit
		OffHeapCache cache = getCache( cacheSize, globalSize, buckets);
		
		cache.setEvictionAuto(false);
		
		insertRows(cache, 100);
		// Test 1
		assertEquals(100, cache.size());
		insertRows(cache, 400);
		// Test 2
		assertEquals(500, cache.size());
		insertRows(cache, buckets);
		long memSize = cache.getAllocatedMemorySize();
		
		LOG.info("Cache max size="+ cacheSize+" Global Max="+globalSize+" memSize="+memSize);
		
		assertTrue( Math.abs(memSize - globalSize) < 0.10*globalSize );
		
		assertTrue( Math.abs(memSize - cacheSize) > 0.10*cacheSize);
		insertRows(cache, buckets);
		memSize = cache.getAllocatedMemorySize();
		LOG.info("Cache max size="+ cacheSize+" Global Max="+globalSize+" memSize="+memSize);
		assertTrue( Math.abs(memSize - globalSize) < 0.10*globalSize );
		assertTrue( Math.abs(memSize - cacheSize)  > 0.10 * cacheSize);
		cache.clear();
		LOG.info("Global - Manual Eviction finished");
	}
	
	
	/**
	 * Test manual eviction api.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws KodaException the koda exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testManualEvictionAPI() throws NativeMemoryException, KodaException, IOException
	{
		// Set cache into manual mode
		// call evict() manually
		
		// When cache in manual eviction mode
		// cache size will excced cache max size and WON'T exceed global max
		// When Global Max is set and cache max is not set
		// cache size may  exceed global max as well
		LOG.info("Global - Manual Eviction API  started");
		long cacheSize = 10000000;
		long globalSize = 100000000;
		int buckets = 100000;
		OffHeapCache.resetGlobals();
		// Create cache with 0 size limit and non-zero global limit
		OffHeapCache cache = getCache( cacheSize, globalSize, buckets);
		
		cache.setEvictionAuto(false);
		
		insertRows(cache, 2*buckets);
		
		assertTrue(buckets > cache.size());
		long totalItems = cache.size();
		long memSize = cache.getAllocatedMemorySize();
		
		LOG.info("Cache max size="+ cacheSize+" Global Max="+globalSize+" memSize="+memSize);
		int toEvict = 100;
		cache.evictMultipleObjects(toEvict);
		
		assertEquals(totalItems - toEvict, cache.size());
		LOG.info("Cache size after eviction of "+toEvict+ " objects="+cache.size()+
											" memory="+cache.getAllocatedMemorySize());

		cache.clear();
		LOG.info("Global - Manual Eviction API finished");
	}
	
	public void testNullValues() throws NativeMemoryException, KodaException, IOException
	{
		LOG.info("Test null value started");
		long cacheSize = 10000000;
		long globalSize = 100000000;
		int buckets = 100000;
		
		// Create cache with 0 size limit and non-zero global limit
		OffHeapCache cache = getCache( cacheSize, globalSize, buckets);
		cache.put("123", null);
		boolean contains = cache.contains("123");
		
		assertTrue(contains);
		assertFalse(cache.contains("12"));
		
		LOG.info("Found for key="+"123"+" value="+cache.get("123"));
		
		LOG.info("Test null value finished");
		
		cache.clear();
	}
	
	
}
