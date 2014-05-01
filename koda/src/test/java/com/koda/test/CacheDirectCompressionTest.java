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
import java.nio.ByteOrder;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.koda.KodaException;
import com.koda.NativeMemoryException;
import com.koda.cache.OffHeapCache;
import com.koda.compression.CodecType;

public class CacheDirectCompressionTest extends TestCase{

	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(CacheDirectCompressionTest.class);	
	
	public void testCompressionOverMinLimit() throws KodaException, NativeMemoryException, IOException
	{
		LOG.info("Test direct cache compression over min limit started");
		com.koda.cache.CacheManager manager = com.koda.cache.CacheManager.getInstance();
		com.koda.config.CacheConfiguration config = new com.koda.config.CacheConfiguration();
		config.setBucketNumber(10000);
		config.setMaxMemory(100000000);
		config.setEvictionPolicy("LRU");
		//config.setDefaultExpireTimeout(6000);
		config.setCacheName("test");
		//config.setCompressionEnabled(true);
		config.setCodecType(CodecType.LZ4);
		OffHeapCache mCache = manager.createCache(config);
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		buf.order(ByteOrder.nativeOrder());
		String key = "key-key";
		String value = "value-value-value-value-value-value-value-value-value-value-value-value-value-value-value"+
		"value-value-value-value-value-value-value-value-value-value-value-value-value-value-value" +
		"value-value-value-value-value-value-value-value-value-value-value-value-value-value-value";
		LOG.info("Original size="+value.length());
		byte[] bkey = key.getBytes();
		byte[] bvalue = value.getBytes();

		
		buf.putInt(0, bkey.length);
		buf.putInt(4, bvalue.length);
		buf.position(8);
		buf.put(bkey);
		buf.put(bvalue);
		
		mCache.putCompress(buf, OffHeapCache.NO_EXPIRE);
		
		buf.clear();
		buf.putInt(0, bkey.length);
		buf.position(4);
		buf.put( bkey);
		
		mCache.getDecompress(buf);
		
		int valueSize = buf.getInt(0);
		assertEquals(bvalue.length, valueSize);
		byte[] val = new byte[valueSize];
		buf.position(12);
		buf.get(val);
		
		assertEquals(value, new String(val));
		
		LOG.info("Finished OK");
	}
	
	public void testCompressionBelowMinLimit() throws KodaException, NativeMemoryException, IOException
	{
		LOG.info("Test direct cache compression below min limit started");
		com.koda.cache.CacheManager manager = com.koda.cache.CacheManager.getInstance();
		com.koda.config.CacheConfiguration config = new com.koda.config.CacheConfiguration();
		config.setBucketNumber(10000);
		config.setMaxMemory(100000000);
		config.setEvictionPolicy("LRU");
		//config.setDefaultExpireTimeout(6000);
		config.setCacheName("test");
		//config.setCompressionEnabled(true);
		config.setCodecType(CodecType.LZ4);
		OffHeapCache mCache = manager.createCache(config);
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		buf.order(ByteOrder.nativeOrder());
		String key = "key-key";
		String value = "value-value-value-value-value-value-value-value-value-value-value-value-value-value-value";
		LOG.info("Original size="+value.length());
		byte[] bkey = key.getBytes();
		byte[] bvalue = value.getBytes();

		
		buf.putInt(0, bkey.length);
		buf.putInt(4, bvalue.length);
		buf.position(8);
		buf.put(bkey);
		buf.put(bvalue);
		
		mCache.putCompress(buf, OffHeapCache.NO_EXPIRE);
		
		buf.clear();
		buf.putInt(0, bkey.length);
		buf.position(4);
		buf.put( bkey);
		
		mCache.getDecompress(buf);
		
		int valueSize = buf.getInt(0);
		assertEquals(bvalue.length, valueSize);
		byte[] val = new byte[valueSize];
		buf.position(12);
		buf.get(val);
		
		assertEquals(value, new String(val));
		
		LOG.info("Finished OK");
	}
	
	public void testCompressionBelowMinLimitCompressionDisabled() throws KodaException, NativeMemoryException, IOException
	{
		LOG.info("Test direct cache compression below min limit compression disabled started");
		com.koda.cache.CacheManager manager = com.koda.cache.CacheManager.getInstance();
		com.koda.config.CacheConfiguration config = new com.koda.config.CacheConfiguration();
		config.setBucketNumber(10000);
		config.setMaxMemory(100000000);
		config.setEvictionPolicy("LRU");
		config.setCacheName("test");
		//config.setCompressionEnabled(true);
		//config.setCodecType(CodecType.LZ4);
		OffHeapCache mCache = manager.createCache(config);
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		buf.order(ByteOrder.nativeOrder());
		String key = "key-key";
		String value = "value-value-value-value-value-value-value-value-value-value-value-value-value-value-value";
		LOG.info("Original size="+value.length());
		byte[] bkey = key.getBytes();
		byte[] bvalue = value.getBytes();

		
		buf.putInt(0, bkey.length);
		buf.putInt(4, bvalue.length);
		buf.position(8);
		buf.put(bkey);
		buf.put(bvalue);
		
		mCache.putCompress(buf, OffHeapCache.NO_EXPIRE);
		
		buf.clear();
		buf.putInt(0, bkey.length);
		buf.position(4);
		buf.put( bkey);
		
		mCache.getDecompress(buf);
		
		int valueSize = buf.getInt(0);
		assertEquals(bvalue.length, valueSize);
		byte[] val = new byte[valueSize];
		buf.position(12);
		buf.get(val);
		
		assertEquals(value, new String(val));
		
		LOG.info("Finished OK");
	}
	public void testCompressionOverMinLimitCompressionDisabled() throws KodaException, NativeMemoryException, IOException
	{
		LOG.info("Test direct cache compression over min compression disabled limit started");
		com.koda.cache.CacheManager manager = com.koda.cache.CacheManager.getInstance();
		com.koda.config.CacheConfiguration config = new com.koda.config.CacheConfiguration();
		config.setBucketNumber(10000);
		config.setMaxMemory(100000000);
		config.setEvictionPolicy("LRU");
		//config.setDefaultExpireTimeout(6000);
		config.setCacheName("test");
		//config.setCompressionEnabled(true);
		//config.setCodecType(CodecType.LZ4);
		OffHeapCache mCache = manager.createCache(config);
		ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		buf.order(ByteOrder.nativeOrder());
		String key = "key-key";
		String value = "value-value-value-value-value-value-value-value-value-value-value-value-value-value-value"+
		"value-value-value-value-value-value-value-value-value-value-value-value-value-value-value" +
		"value-value-value-value-value-value-value-value-value-value-value-value-value-value-value";
		LOG.info("Original size="+value.length());
		byte[] bkey = key.getBytes();
		byte[] bvalue = value.getBytes();

		
		buf.putInt(0, bkey.length);
		buf.putInt(4, bvalue.length);
		buf.position(8);
		buf.put(bkey);
		buf.put(bvalue);
		
		mCache.putCompress(buf, OffHeapCache.NO_EXPIRE);
		
		buf.clear();
		buf.putInt(0, bkey.length);
		buf.position(4);
		buf.put( bkey);
		
		mCache.getDecompress(buf);
		
		int valueSize = buf.getInt(0);
		assertEquals(bvalue.length, valueSize);
		byte[] val = new byte[valueSize];
		buf.position(12);
		buf.get(val);
		
		assertEquals(value, new String(val));
		
		LOG.info("Finished OK");
	}
		

}
