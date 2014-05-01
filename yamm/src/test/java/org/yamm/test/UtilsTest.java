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
package org.yamm.test;

import java.nio.ByteBuffer;
import java.util.Random;

import org.yamm.util.Utils;

import com.koda.common.util.UnsafeAccess;

import junit.framework.TestCase;


// TODO: Auto-generated Javadoc
/**
 * The Class UtilsTest.
 */
public class UtilsTest extends TestCase{

	
	/**
	 * Test memcpy.
	 */
	public void testByteArrayMemcpy(){
		
		System.out.println("ByteArray memcpy test");
		byte[] arr = new byte[36];
		Utils.memset(arr, (byte)1);
		long memory = Utils.malloc(arr.length);

		Utils.memcpy(arr, 0, arr.length, memory);

		Utils.memset(arr, (byte)2);
		Utils.memcpy(memory, arr, 0, arr.length);		
		assertEquals(0, Utils.cmp(arr, (byte)1));
		Utils.free(memory);
		System.out.println("Finished OK.");
	}
	
	/**
	 * Test char array memcpy.
	 */
	public void testCharArrayMemcpy(){
		
		System.out.println("CharArray memcpy test");
		char[] arr = new char[36];
		Utils.memset(arr, (char)1);
		long memory = Utils.malloc(2*arr.length);
		Utils.memcpy(arr, 0, arr.length, memory);

		Utils.memset(arr, (char)2);
		Utils.memcpy(memory, arr, 0, arr.length);		
		assertEquals(0, Utils.cmp(arr, (char)1));
		Utils.free(memory);
		System.out.println("Finished OK.");
	}
	
	/**
	 * Test char array memcpy.
	 */
	public void testShortArrayMemcpy(){
		
		System.out.println("ShortArray memcpy test");
		short[] arr = new short[36];
		Utils.memset(arr, (short)1);
		long memory = Utils.malloc(2*arr.length);
		Utils.memcpy(arr, 0, arr.length, memory);

		Utils.memset(arr, (short)2);
		Utils.memcpy(memory, arr, 0, arr.length);		
		assertEquals(0, Utils.cmp(arr, (short)1));
		Utils.free(memory);
		System.out.println("Finished OK.");
	}

	/**
	 * Test char array memcpy.
	 */
	public void testIntArrayMemcpy(){
		
		System.out.println("IntArray memcpy test");
		int[] arr = new int[36];
		Utils.memset(arr, (int)1);
		long memory = Utils.malloc(4*arr.length);
		Utils.memcpy(arr, 0, arr.length, memory);

		Utils.memset(arr, (int)2);
		Utils.memcpy(memory, arr, 0, arr.length);		
		assertEquals(0, Utils.cmp(arr, (int)1));
		Utils.free(memory);
		System.out.println("Finished OK.");
	}
	
	/**
	 * Test char array memcpy.
	 */
	public void testLongArrayMemcpy(){
		
		System.out.println("LongArray memcpy test");
		long[] arr = new long[36];
		Utils.memset(arr, (long)1);
		long memory = Utils.malloc(8*arr.length);
		Utils.memcpy(arr, 0, arr.length, memory);

		Utils.memset(arr, (long)2);
		Utils.memcpy(memory, arr, 0, arr.length);		
		assertEquals(0, Utils.cmp(arr, (long)1));
		Utils.free(memory);
		System.out.println("Finished OK.");
	}	
	
	/**
	 * Test char array memcpy.
	 */
//	public void testFloatArrayMemcpy(){
//		
//		System.out.println("FloatArray memcpy test");
//		float[] arr = new float[36];
//		Utils.memset(arr, (float)1);
//		long memory = Utils.malloc(4*arr.length);
//		Utils.memcpy(arr, 0, arr.length, memory);
//
//		Utils.memset(arr, (float)2);
//		Utils.memcpy(memory, arr, 0, arr.length);		
//		assertEquals(0, Utils.cmp(arr, (float)1));
//		Utils.free(memory);
//		System.out.println("Finished OK.");
//	}	

	
	/**
	 * Test double array memcpy.
	 */
//	public void testDoubleArrayMemcpy(){
//		
//		System.out.println("DoubleArray memcpy test");
//		double[] arr = new double[36];
//		Utils.memset(arr, (double)1);
//		long memory = Utils.malloc(8*arr.length);
//		Utils.memcpy(arr, 0, arr.length, memory);
//
//		Utils.memset(arr, (double)2);
//		Utils.memcpy(memory, arr, 0, arr.length);		
//		assertEquals(0, Utils.cmp(arr, (double)1));
//		Utils.free(memory);
//		System.out.println("Finished OK.");
//	}	
	
	/**
	 * Test String memcpy.
	 */
	
	public void testStringMemcpy()
	{
		System.out.println("String memcpy test");
		String s = "12685388900-927653987902";
		long memory = Utils.malloc(2*s.length() + 4);
		Utils.memcpy(s, 0, s.length(), memory);
		String ss = Utils.readString(memory);
		assertEquals(s, ss);
		Utils.free(memory);
		System.out.println("Finished OK.");
	}
	

	/**
	 * Test ByteBuffer on-heap.
	 */
	
	public void testByteBufferOnHeap()
	{
		System.out.println("ByteBuffer on-heap test");
		ByteBuffer buf = ByteBuffer.allocate(16);
		String s = "0123456789012345";
		buf.put(s.getBytes());
		long memory = Utils.malloc(16);
		Utils.memcpy(buf, 0, 16, memory);
		buf.rewind();
		buf.put("hhh".getBytes());
		Utils.memcpy(memory, buf, 0, 16);
		byte[] bbuf = new byte[16];
		buf.rewind();
		buf.get(bbuf);
		String ss = new String(bbuf);
		assertEquals(s, ss);
		Utils.free(memory);
		System.out.println("Finished OK.");		
	}

	
	/**
	 * Test byte buffer off heap.
	 */
	public void testByteBufferOffHeap()
	{
		System.out.println("ByteBuffer off-heap test");
		ByteBuffer buf = ByteBuffer.allocateDirect(16);
		String s = "0123456789012345";
		buf.put(s.getBytes());
		long memory = Utils.malloc(16);
		Utils.memcpy(buf, 0, 16, memory);
		buf.rewind();
		buf.put("hhh".getBytes());
		Utils.memcpy(memory, buf, 0, 16);
		byte[] bbuf = new byte[16];
		buf.rewind();
		buf.get(bbuf);
		String ss = new String(bbuf);
		assertEquals(s, ss);
		Utils.free(memory);
		System.out.println("Finished OK.");		
	}
	
	/**
	 * Test primitive types.
	 */
	public void testPrimitiveTypes()
	{
		byte b = (byte) 1;
		char c = (char) 250;
		short s = (short) 250;
		int i = 60000;
		long l = 3000000000L;
		float f = 6;
		double d = 7;
		System.out.println("Primitive types test");
		long memory = Utils.malloc(8);
		
		Utils.putByte(memory, b);
		byte bb = Utils.getByte(memory);
		assertEquals(b, bb);
		
		Utils.putUByte(memory, s);
		short ss = Utils.getUByte(memory);
		assertEquals(s, ss);
		
		Utils.putShort(memory, s);
		ss = Utils.getShort(memory);
		assertEquals(s, ss);

		Utils.putUShort(memory, i);
		int ii = Utils.getUShort(memory);
		assertEquals(i, ii);		
		
		Utils.putChar(memory, c);
		char cc = Utils.getChar(memory);
		assertEquals(c, cc);
		
		Utils.putUChar(memory, i);
		ii = Utils.getUChar(memory);
		assertEquals(i, ii);		
		
		Utils.putInt(memory, i);
		ii = Utils.getInt(memory);
		System.out.println(i+" "+ii);
		assertEquals(i, ii);

		Utils.putUInt(memory, l);
		long ll = Utils.getUInt(memory);
		System.out.println(l+" "+ll);
		assertEquals(l, ll);		
		
		
		Utils.putUInt(memory, 10);
		ll = Utils.getUInt(memory);
		assertEquals(10, ll);
		
		Utils.putUInt(memory, (long)Integer.MAX_VALUE +10);
		ll = Utils.getUInt(memory);
		System.out.println(((long)Integer.MAX_VALUE +10) +" "+ll);
		assertEquals((long)Integer.MAX_VALUE +10, ll);
		
		Utils.putLong(memory, l);
		ll = Utils.getLong(memory);
		assertEquals(l, ll);
		
		Utils.putFloat(memory, f);
		float ff = Utils.getFloat(memory);
		assertEquals(f, ff);
		
		Utils.putDouble(memory, d);
		double dd = Utils.getDouble(memory);
		assertEquals(d, dd);


		System.out.println("Finished OK.");
	}
	
	public void testMemcmp()
	{
		System.out.println("Test memcmp started");
		long src = Utils.malloc(17);
		long dst = Utils.malloc(17);
		
		byte[] value = "01234567890123456".getBytes();
		Utils.memcpy(value, 0, value.length, src);
		Utils.memcpy(value, 0, value.length, dst);
		
		assertEquals(0, Utils.memcmp(src, dst, 17));
		
		UnsafeAccess.getUnsafe().putByte(src + 16, (byte)0);
		
		assertTrue(Utils.memcmp(src, dst, 17) < 0);
		
		UnsafeAccess.getUnsafe().putByte(src, (byte)'1');
		assertTrue(Utils.memcmp(src, dst, 17) > 0);
		
		System.out.println("Test memcmp finished");
		
		
	}
	
	/**
	 * Test random.
	 */
	public void testRandom(){
		
		Random r = new Random();
		
		long start = System.currentTimeMillis();
		long sum = 0;
		for(int i=0; i < 10000000; i++){
			sum += r.nextInt(64);
		}
		System.out.println("Time for 10M random ="+ (System.currentTimeMillis() - start)+" ms");
	}
	
}
