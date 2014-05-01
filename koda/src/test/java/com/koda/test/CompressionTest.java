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
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.xerial.snappy.Snappy;

import com.koda.KodaException;
import com.koda.NativeMemoryException;
import com.koda.cache.OffHeapCache;
import com.koda.compression.CodecType;

// TODO: Auto-generated Javadoc
/**
 * The Class CompressionTest.
 */
public class CompressionTest extends TestCase {

	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(CompressionTest.class);
	
	
	/**
	 * Test compression snappy raw.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testCompressionSnappyRaw() throws IOException
	{
		ByteBuffer src = ByteBuffer.allocateDirect(1024);
		ByteBuffer dst = ByteBuffer.allocateDirect(1024);
		String value = "0000000000000000000000000000000000000000000000000000000000000000000000000000000"+
        "0000000000000000000000000000000000000000000000000000000000000000000000000000000" +
        "0000000000000000000000000000000000000000000000000000000000000000000000000000000";
        src.put(value.getBytes());
        src.flip();
        int len = Snappy.compress(src, dst);
        LOG.info("Compressed size ="+len+" dst pos ="+dst.position()+" dst limit="+dst.limit());
        len = Snappy.uncompress(dst, src);
        LOG.info("Uncompressed size="+len);
        LOG.info("Original size="+value.length());
        
	}
	

	
	/**
	 * Test no compression.
	 *
	 * @throws KodaException the koda exception
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testNoCompression() throws KodaException, NativeMemoryException, IOException
	{
		LOG.info("Test compression disabled");
		com.koda.cache.CacheManager manager = com.koda.cache.CacheManager.getInstance();
		com.koda.config.CacheConfiguration config = new com.koda.config.CacheConfiguration();
		config.setBucketNumber(1000000);
		config.setMaxMemory(100000000);
		config.setEvictionPolicy("LRU");
		config.setDefaultExpireTimeout(6000);
		config.setCacheName("test");

		OffHeapCache mCache = manager.createCache(config);
		
		String key = "1234567890";
		String value = "0000000000000000000000000000000000000000000000000000000000000000000000000000000"+
		               "0000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		               "0000000000000000000000000000000000000000000000000000000000000000000000000000000";
		mCache.put(key, value);
		
		String s = (String)mCache.get(key);
		
		assertTrue(value.equals(s));
		
		s = (String) mCache.get(key+"0");
		
		assertTrue(s == null);
		
		byte[] bvalue = value.getBytes();
		byte[] bkey = key.getBytes();
		
		mCache.put(bkey, bvalue);
		
		byte[] result = (byte[]) mCache.get(bkey);
		
		assertTrue(bvalue.length == result.length);
		for(int i=0; i < bvalue.length; i++)
		{
			assertTrue(bvalue[i] == result[i]);
		}
		
		LOG.info("Done");
		
	}
	
	/**
	 * Test compression snappy.
	 *
	 * @throws KodaException the koda exception
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testCompressionSnappy() throws KodaException, NativeMemoryException, IOException
	{
		LOG.info("Test compression Snappy started");
		com.koda.cache.CacheManager manager = com.koda.cache.CacheManager.getInstance();
		com.koda.config.CacheConfiguration config = new com.koda.config.CacheConfiguration();
		config.setBucketNumber(1000000);
		config.setMaxMemory(100000000);
		config.setEvictionPolicy("LRU");
		config.setDefaultExpireTimeout(6000);
		config.setCacheName("test");
		//config.setCompressionEnabled(true);
		config.setCodecType(CodecType.SNAPPY);
		OffHeapCache mCache = manager.createCache(config);
		
		String key = "1234567890";
		String value = "0000000000000000000000000000000000000000000000000000000000000000000000000000000"+
		               "0000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		               "0000000000000000000000000000000000000000000000000000000000000000000000000000000";
		mCache.put(key, value);
		
		String s = (String)mCache.get(key);
		
		assertTrue(value.equals(s));
		
		s = (String) mCache.get(key+"0");
		
		byte[] bvalue = value.getBytes();
		byte[] bkey = key.getBytes();
		
		mCache.put(bkey, bvalue);
		
		byte[] result = (byte[]) mCache.get(bkey);
		
		assertTrue(bvalue.length == result.length);
		for(int i=0; i < bvalue.length; i++)
		{
			assertTrue(bvalue[i] == result[i]);
		}
		
		LOG.info("Done");
		
	}
	
	/**
	 * Test compression snappy.
	 *
	 * @throws KodaException the koda exception
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testCompressionLZ4() throws KodaException, NativeMemoryException, IOException
	{
		LOG.info("Test compression LZ4 started");
		com.koda.cache.CacheManager manager = com.koda.cache.CacheManager.getInstance();
		com.koda.config.CacheConfiguration config = new com.koda.config.CacheConfiguration();
		config.setBucketNumber(1000000);
		config.setMaxMemory(100000000);
		config.setEvictionPolicy("LRU");
		config.setDefaultExpireTimeout(6000);
		config.setCacheName("test");
		//config.setCompressionEnabled(true);
		config.setCodecType(CodecType.LZ4);
		OffHeapCache mCache = manager.createCache(config);
		
		String key = "1234567890";
		String value = "0000000000000000000000000000000000000000000000000000000000000000000000000000000"+
		               "0000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		               "0000000000000000000000000000000000000000000000000100000000000000000000000000000";
		mCache.put(key, value);
		
		String s = (String)mCache.get(key);
		
		assertTrue(value.equals(s));
		
		s = (String) mCache.get(key+"0");
		
		byte[] bvalue = value.getBytes();
		byte[] bkey = key.getBytes();
		
		mCache.put(bkey, bvalue);
		
		byte[] result = (byte[]) mCache.get(bkey);
		
		assertTrue(bvalue.length == result.length);
		for(int i=0; i < bvalue.length; i++)
		{
			assertTrue(bvalue[i] == result[i]);
		}
		
		LOG.info("Done");
		
	}
	
	 public void testCompressionLZ4HC() throws KodaException, NativeMemoryException, IOException
	  {
	    LOG.info("Test compression LZ4HC started");
	    com.koda.cache.CacheManager manager = com.koda.cache.CacheManager.getInstance();
	    com.koda.config.CacheConfiguration config = new com.koda.config.CacheConfiguration();
	    config.setBucketNumber(1000000);
	    config.setMaxMemory(100000000);
	    config.setEvictionPolicy("LRU");
	    config.setDefaultExpireTimeout(6000);
	    config.setCacheName("test");
	    //config.setCompressionEnabled(true);
	    config.setCodecType(CodecType.LZ4HC);
	    OffHeapCache mCache = manager.createCache(config);
	    
	    String key = "1234567890";
	    String value = "0000000000000000000000000000000000000000000000000000000000000000000000000000000"+
	                   "0000000000000000000000000000000000000000000000000000000000000000000000000000000" +
	                   "0000000000000000000000000000000000000000000000000100000000000000000000000000000";
	    mCache.put(key, value);
	    
	    String s = (String)mCache.get(key);
	    
	    assertTrue(value.equals(s));
	    
	    s = (String) mCache.get(key+"0");
	    
	    byte[] bvalue = value.getBytes();
	    byte[] bkey = key.getBytes();
	    
	    mCache.put(bkey, bvalue);
	    
	    byte[] result = (byte[]) mCache.get(bkey);
	    
	    assertTrue(bvalue.length == result.length);
	    for(int i=0; i < bvalue.length; i++)
	    {
	      assertTrue(bvalue[i] == result[i]);
	    }
	    
	    LOG.info("Done");
	    
	  }

	
	/**
	 * Test compression deflate.
	 *
	 * @throws KodaException the koda exception
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testCompressionDeflate() throws KodaException, NativeMemoryException, IOException
	{
		LOG.info("Test compression Deflate started");
		com.koda.cache.CacheManager manager = com.koda.cache.CacheManager.getInstance();
		com.koda.config.CacheConfiguration config = new com.koda.config.CacheConfiguration();
		config.setBucketNumber(1000000);
		config.setMaxMemory(100000000);
		config.setEvictionPolicy("LRU");
		config.setDefaultExpireTimeout(6000);
		config.setCacheName("test");
		//config.setCompressionEnabled(true);
		config.setCodecType(CodecType.DEFLATE);
		OffHeapCache mCache = manager.createCache(config);
		
		String key = "1234567890";
		String value = "0000000000000000000000000000000000000000000000000000000000000000000000000000000"+
		               "0000000000000000000000000000000000000000000000000000000000000000000000000000000" +
		               "0000000000000000000000000000000000000000000000000000000000000000000000000000000";
		mCache.put(key, value);
		
		String s = (String)mCache.get(key);
		
		assertTrue(value.equals(s));
		
		LOG.info("Done");
	}
}
