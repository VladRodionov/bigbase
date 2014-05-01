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
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Random;

import com.koda.integ.hbase.blockcache.OffHeapBlockCache;
import com.koda.integ.hbase.stub.ByteArrayCacheable;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;


// TODO: Auto-generated Javadoc
/**
 * The Class BlockCacheSimpleTest.
 */
public class BlockCacheSimpleTest extends TestCase{
	
	/** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(BlockCacheSimpleTest.class);
  
  /** The cache size. */
  private static long cacheSize = 1000000000L; // 2G
  
  /** The cache impl class. */
  private static String cacheImplClass ="com.koda.integ.hbase.blockcache.BigBaseBlockCache";
  
  /** The young gen factor. */
  private static Float youngGenFactor = 0.5f;
  
  /** The cache compression. */
  private static String cacheCompression = "LZ4";
  
  /** The cache overflow enabled. */
  private static boolean cacheOverflowEnabled = false;	
  
  private static String cacheEviction = "LRU";
  
  private static String onHeapRatio = "0.08";
  
	/** The cache. */
	static BlockCache cache; 
	
	/** The conf. */
	static Configuration conf;
	
	/** The r. */
	private Random r = new Random();
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	protected void setUp() throws Exception {
		if(cache != null) return;
		conf = HBaseConfiguration.create();

	   // Cache configuration
    conf.set(OffHeapBlockCache.BLOCK_CACHE_MEMORY_SIZE, Long.toString(cacheSize));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_IMPL, cacheImplClass);
    conf.set(OffHeapBlockCache.BLOCK_CACHE_YOUNG_GEN_FACTOR, Float.toString(youngGenFactor));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_COMPRESSION, cacheCompression);
    conf.set(OffHeapBlockCache.BLOCK_CACHE_OVERFLOW_TO_EXT_STORAGE_ENABLED, Boolean.toString(cacheOverflowEnabled));	  
	  conf.set(OffHeapBlockCache.BLOCK_CACHE_EVICTION, cacheEviction);
	  conf.set(OffHeapBlockCache.HEAP_BLOCK_CACHE_MEMORY_RATIO, onHeapRatio);
	}

	
	/**
	 * Test create cache.
	 */
	public void testCreateCache()
	{
		LOG.info("Test create cache started");
		
	   	try{
    		Class<?> cls = Class.forName("com.koda.integ.hbase.blockcache.OffHeapBlockCache");
    		
    		Constructor<?> ctr = cls.getDeclaredConstructor(Configuration.class );
    		cache = (BlockCache) ctr.newInstance(conf);
    		assertTrue(true);
    		LOG.info("Test create cache finished.");
    	} catch(Exception e){
    		LOG.error("Could not instantiate 'com.koda.integ.hbase.blockcache.OffHeapBlockCache'+ class, will resort to standard cache impl.");
    		assertTrue(false);
    	}
		
	}
	
	
	/**
	 * Test byte cacheable.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testByteCacheable() throws IOException
	{
		LOG.info("Test byte cacheable started");
		
		byte[] array = new byte[30000];
		Random r = new Random();
		r.nextBytes(array);
		
		ByteArrayCacheable bac = new ByteArrayCacheable(array);
		
		ByteBuffer buf = ByteBuffer.allocate(100000);
		
		bac.serialize(buf);
		
		buf.flip();
		
		ByteArrayCacheable bb = (ByteArrayCacheable)ByteArrayCacheable.deserializer.deserialize(buf);
		
		assertTrue( bb.equals(bac));		
		LOG.info("Test byte cacheable finished");		
	}
	
	/**
	 * Test cache put get.
	 */
	public void testCachePutGet()
	{
		LOG.info("Test cache put-get started");
		
		ByteArrayCacheable bac = getValue(300000 + r.nextInt(10000));

		BlockCacheKey key = new BlockCacheKey("file", 1000);
				
		cache.cacheBlock(key, bac);
		
		ByteArrayCacheable result = (ByteArrayCacheable) cache.getBlock(key, true, true);
		
		assertTrue(result != null);
		assertTrue( result.equals(bac));
		
		LOG.info("Test cache put-get finished");
	}
	
	/**
	 * _test cache put get multi.
	 */
	public void _testCachePutGetMulti()
	{
		LOG.info("Test cache put-get multi started");
		
		int total = 10000;
		long start = System.currentTimeMillis();
		for(int i=0 ; i < total ; i++){
			ByteArrayCacheable bac = getValue(30000 + i);
			BlockCacheKey key = new BlockCacheKey("file"+i, 1000);			
			cache.cacheBlock(key, bac);
		}
		
		LOG.info("Stored "+total+" blocks in "+(System.currentTimeMillis() - start)+" ms");
		
		start = System.currentTimeMillis();
		for(int i=0 ; i < total ; i++){			
			BlockCacheKey key = new BlockCacheKey("file"+i, 1000);	
			ByteArrayCacheable result = (ByteArrayCacheable) cache.getBlock(key, true, true);		
			assertTrue(result != null);
			assertTrue( result.equals(getValue(30000 + i)));
		}
		LOG.info("Get "+total+" blocks in "+(System.currentTimeMillis() - start)+" ms");
		
		LOG.info("Test cache put-get multi finished");
	}
	
	
	/**
	 * Test cache put get multi perf.
	 */
	public void testCachePutGetMultiPerf()
	{
		LOG.info("Test cache put-get multi perf started");
		
		int total = 100000;
		long start = System.currentTimeMillis();
		for(int i=0 ; i < total ; i++){
			ByteArrayCacheable bac = getValueSingle();
			BlockCacheKey key = new BlockCacheKey("file"+i, 1000);			
			cache.cacheBlock(key, bac);
		}
		
		LOG.info("Stored "+total+" blocks in "+(System.currentTimeMillis() - start)+" ms");
		
		start = System.currentTimeMillis();
		int nulls = 0;
		for(int i=0 ; i < total ; i++){			
			BlockCacheKey key = new BlockCacheKey("file"+i, 1000);	
			ByteArrayCacheable result = (ByteArrayCacheable) cache.getBlock(key, true, true);		
			//assertTrue(result != null);
			if(result == null) nulls ++;
			//assertTrue( result.equals(getValueSingle()));
		}
		LOG.info("Get "+total+" blocks in "+(System.currentTimeMillis() - start)+" ms. Nulls="+nulls);
		
		LOG.info("Test cache put-get multi perf finished");
	}
	
	/** The single. */
	static byte[] single = new byte[5000];
	
	static{
		Random rr = new Random();
		rr.nextBytes(single);
	}
	
	/** The bac single. */
	static ByteArrayCacheable bacSingle = new ByteArrayCacheable(single);
	
	/**
	 * Gets the value single.
	 *
	 * @return the value single
	 */
	private ByteArrayCacheable getValueSingle() {
		
		return bacSingle;
	}

	/**
	 * Gets the value.
	 *
	 * @param size the size
	 * @return the value
	 */
	private ByteArrayCacheable getValue(int size)
	{
		Random r = new Random(size);
		byte[] array = new byte[size];
		r.nextBytes(array);
		return new ByteArrayCacheable(array); 
	}
}
