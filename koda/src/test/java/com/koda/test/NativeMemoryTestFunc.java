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

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.koda.IOUtils;
import com.koda.NativeMemory;
import com.koda.NativeMemoryException;

// TODO: Auto-generated Javadoc
/**
 * The Class NativeMemoryTestFunc.
 */
public class NativeMemoryTestFunc extends TestCase {
	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(NativeMemoryTestFunc.class);
	
	
	/**
	 * Test write read byte array malloc memcopy.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void testWriteReadByteArrayMallocMemcopy() throws NativeMemoryException
	{
		LOG.info("Test WriteRead Byte Array Malloc - memcopy");
		byte[] buf = new byte[100];
		for(int i=0; i < buf.length; i++)
		{
			buf[i] = (byte) i;
		}
		
		long ptr = NativeMemory.malloc(buf.length);
		LOG.info("ptr="+Long.toHexString(ptr));
		
		NativeMemory.memcpy(buf, 0, buf.length, ptr, 0 );
		
		byte[] buffer = new byte[buf.length];
		
		NativeMemory.memcpy(ptr, 0, buffer, 0, buf.length);
		assertEquals(buf.length, buffer.length);
		for(int i=0; i < buf.length; i++)
		{
			if(buf[i] != buffer[i]) assertTrue(false);
		}
		
		LOG.info("Test WriteRead Byte Array Malloc - memcopy DONE.");
		
		
	}
	
	/**
	 * Test write read byte buffer malloc memcopy.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void testWriteReadByteBufferMallocMemcopy() throws NativeMemoryException
	{
		LOG.info("Test WriteRead Byte Buffer Malloc - memcopy");
		ByteBuffer buf = ByteBuffer.allocateDirect(100);
		for(int i=0; i < 100; i++)
		{
			buf.put((byte) i);
		}
		
		long ptr = NativeMemory.malloc(buf.capacity());
		LOG.info("ptr="+Long.toHexString(ptr));
		NativeMemory.memcpy(buf, 0, buf.capacity(), ptr, 0);
		ByteBuffer buffer  = ByteBuffer.allocateDirect(buf.capacity());

		NativeMemory.memcpy(ptr, 0, buffer, 0, buffer.capacity());
		assertEquals(buf.capacity(), buffer.capacity());
		buf.position(0);
		for(int i=0; i < buf.capacity(); i++)
		{
			if(buf.get() != buffer.get()) assertTrue(false);
		}
		
		LOG.info("Test WriteRead Byte Buffer Malloc - memcopy DONE.");
		
		
	}
	
	/**
	 * Test write read int perf.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void testWriteReadIntPerf() throws NativeMemoryException
	{
		LOG.info(" Write-Read Int perf test");
		int N = 10000000;
		long ptr = NativeMemory.malloc(N*4);
		LOG.info("ptr="+Long.toHexString(ptr));
		long t1 = System.nanoTime();
		
		for(int i=0; i < N; i++){
			IOUtils.putInt(ptr, i, i);
		}
		
		long t2 = System.nanoTime();
		
		LOG.info("PUT INT "+N+" times = "+(t2-t1)+" ns");
		LOG.info(" Write-Read Int perf test done.");
		
		
	}


	
	/**
	 * Test write read byte.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void testWriteReadByte() throws NativeMemoryException
	{

		LOG.info(" Write-Read Byte test.");
		long ptr = NativeMemory.malloc(16);
		LOG.info("ptr="+Long.toHexString(ptr));

		IOUtils.putByte(ptr, 0, (byte)120);
		byte val = IOUtils.getByte(ptr, 0);
		
		assertEquals(120, val);
		
		IOUtils.putByte(ptr, 1, (byte)120);
		val = IOUtils.getByte(ptr, 1);
		
		assertEquals(120, val);
		
		LOG.info(" Write-Read Byte test done.");
	}
	
	/**
	 * Test write read u byte.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void testWriteReadUByte() throws NativeMemoryException
	{

		LOG.info(" Write-Read unsigned Byte test.");
		long ptr = NativeMemory.malloc(16);
		LOG.info("ptr="+Long.toHexString(ptr));

		IOUtils.putUByte(ptr, 0, (short)200);
		short val = IOUtils.getUByte(ptr, 0);
		
		assertEquals(200, val);
		IOUtils.putUByte(ptr, 1, (short)200);
		val = IOUtils.getUByte(ptr, 1);
		
		assertEquals(200, val);
		LOG.info(" Write-Read unsigned  Byte test done.");
	}
	
	/**
	 * Test write read short.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void testWriteReadShort() throws NativeMemoryException
	{

		LOG.info(" Write-Read Short test.");
		long ptr = NativeMemory.malloc(16);
		LOG.info("ptr="+Long.toHexString(ptr));

		IOUtils.putShort(ptr, 0, (short)20000);
		short val = IOUtils.getShort(ptr, 0);
		
		assertEquals(20000, val);
		IOUtils.putShort(ptr, 1, (short)20000);
		val = IOUtils.getShort(ptr, 1);
		
		assertEquals(20000, val);
		LOG.info(" Write-Read Short test done.");
	}
	
	/**
	 * Test write read u short.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void testWriteReadUShort() throws NativeMemoryException
	{

		LOG.info(" Write-Read unsigned Short test.");
		long ptr = NativeMemory.malloc(16);
		LOG.info("ptr="+Long.toHexString(ptr));

		IOUtils.putUShort(ptr, 0, 40000);
		int val = IOUtils.getUShort(ptr, 0);
		
		assertEquals(40000, val);
		
		IOUtils.putUShort(ptr, 1, 40000);
		val = IOUtils.getUShort(ptr, 1);
		
		assertEquals(40000, val);
		
		LOG.info(" Write-Read unsigned  Short test done.");
	}
	
	/**
	 * Test write read int.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void testWriteReadInt() throws NativeMemoryException
	{

		LOG.info(" Write-Read Int test.");
		long ptr = NativeMemory.malloc(16);
		LOG.info("ptr="+Long.toHexString(ptr));

		IOUtils.putInt(ptr, 0, 20000);
		int val = IOUtils.getInt(ptr, 0);
		
		assertEquals(20000, val);
		IOUtils.putInt(ptr, 1, 20000);
		val = IOUtils.getInt(ptr, 1);
		
		assertEquals(20000, val);
		LOG.info(" Write-Read Int test done.");
	}
	
	/**
	 * Test write read u int.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void testWriteReadUInt() throws NativeMemoryException
	{

		LOG.info(" Write-Read unsigned Int test.");
		long ptr = NativeMemory.malloc(16);
		LOG.info("ptr="+Long.toHexString(ptr));

		IOUtils.putUInt(ptr, 0, 4000000000L);
		long val = IOUtils.getUInt(ptr, 0);
		
		assertEquals(4000000000L, val);
		
		IOUtils.putUInt(ptr, 1, 4000000000L);
		val = IOUtils.getUInt(ptr, 1);
		
		assertEquals(4000000000L, val);		
		LOG.info(" Write-Read unsigned  Int test done.");
	}
	
	/**
	 * Test write read long.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void testWriteReadLong() throws NativeMemoryException
	{

		LOG.info(" Write-Read Long test.");
		long ptr = NativeMemory.malloc(16);
		LOG.info("ptr="+Long.toHexString(ptr));

		IOUtils.putLong(ptr, 0, 20000L);
		long val = IOUtils.getLong(ptr, 0);
		
		assertEquals(20000L, val);
		
		IOUtils.putLong(ptr, 1, 20000L);
		val = IOUtils.getLong(ptr, 1);
		
		assertEquals(20000L, val);
		
		LOG.info(" Write-Read Long test done.");
	}
	
	/**
	 * Test write read float.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void testWriteReadFloat() throws NativeMemoryException
	{

		LOG.info(" Write-Read Float test.");
		long ptr = NativeMemory.malloc(16);
		LOG.info("ptr="+Long.toHexString(ptr));

		IOUtils.putFloat(ptr, 0, 20000.1f);
		float val = IOUtils.getFloat(ptr, 0);
		
		assertEquals(20000.1f, val);
		
		IOUtils.putFloat(ptr, 1, 20000.1f);
		val = IOUtils.getFloat(ptr, 1);
		
		assertEquals(20000.1f, val);
		
		LOG.info(" Write-Read Float test done.");
	}
	
	/**
	 * Test write read double.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void testWriteReadDouble() throws NativeMemoryException
	{

		LOG.info(" Write-Read Double test.");
		long ptr = NativeMemory.malloc(16);
		LOG.info("ptr="+Long.toHexString(ptr));

		IOUtils.putDouble(ptr, 0, 20000.1d);
		double val = IOUtils.getDouble(ptr, 0);
		
		assertEquals(20000.1d, val);
		
		IOUtils.putDouble(ptr, 1, 20000.1d);
		val = IOUtils.getDouble(ptr, 1);
		
		assertEquals(20000.1d, val);
		
		LOG.info(" Write-Read Double test done.");
	}
	

    
	/**
	 * Test write read string malloc.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void testWriteReadStringMalloc() throws NativeMemoryException
	{

		LOG.info(" Write-Read String Malloc test.");
		String s = "1234567890ABCDEF";
		long ptr = NativeMemory.malloc(s.length()*2);
		LOG.info("ptr="+Long.toHexString(ptr));
		NativeMemory.memcpy(s, 0, s.length(), ptr, 0);

		String ss = NativeMemory.memread2s(ptr);
		LOG.info("ss="+ss);
		assertEquals(s, ss);
		
		LOG.info(" Write-Read String Malloc test done.");
	}
	

}
