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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.yamm.util.Utils;

import sun.misc.Unsafe;

import com.koda.common.util.UnsafeAccess;

// TODO: Auto-generated Javadoc
/**
 * The Class UnsafeTest.
 */
public class UnsafeTest {

	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws IllegalArgumentException the illegal argument exception
	 * @throws SecurityException the security exception
	 * @throws IllegalAccessException the illegal access exception
	 * @throws NoSuchFieldException the no such field exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void main(String[] args) throws IllegalArgumentException, SecurityException, IllegalAccessException, NoSuchFieldException, IOException
	{
//		Unsafe unsafe = UnsafeAccess.getUnsafe();
//		int n = 10;
//		byte[] byteArr = new byte[8*n];
//		System.out.println("byte array off="+unsafe.arrayBaseOffset(byte[].class)+" indexSize="+unsafe.arrayIndexScale(byte[].class));		
//		char[] charArr = new char[4*n];
//		System.out.println("char array off="+unsafe.arrayBaseOffset(char[].class));
//		short[] shortArr = new short[4*n];
//		System.out.println("short array off="+unsafe.arrayBaseOffset(short[].class));
//		int[] intArr = new int[2*n];
//		System.out.println("int array off="+unsafe.arrayBaseOffset(int[].class));
//		long[] longArr = new long[n];
//		System.out.println("long array off="+unsafe.arrayBaseOffset(long[].class));
//		
//		long addr = unsafe.allocateMemory(8*n);
//		
//		System.out.println("Test 0: byte arr address "+ toAddress(byteArr) );
//		
//		System.out.println("Test 1: memcopy byte array to char array.");
//		
//		for(int i=0; i < 8*n; i++) byteArr[i] = 1;
//		
//		
//		unsafe.copyMemory(byteArr,  unsafe.arrayBaseOffset(byte[].class) , 
//				charArr, unsafe.arrayBaseOffset(byte[].class), 8*n);
//				
//
//		for(int i=0; i < 4*n; i++)
//		{
//			if(charArr[i] != 1 + (1 << 8)){
//				System.out.println("FAILED: i="+i + " v="+charArr[i]);
//				System.exit(-1);
//			}
//		}
//		
//		
//		System.out.println("Test 2: memcopy byte array to off heap and back.");
//				
//		
//		unsafe.copyMemory(byteArr,  unsafe.arrayBaseOffset(byte[].class) , 
//				null, addr, 8*n);
//				
//		for(int i=0; i < 8*n; i++) byteArr[i] = 0;		
//		
//		unsafe.copyMemory(null, addr, byteArr,  unsafe.arrayBaseOffset(byte[].class) , 8*n);		
//		
//		for(int i=0; i < 8*n; i++)
//		{
//			if(byteArr[i] != 1 ){
//				System.out.println("FAILED: i="+i + " v="+byteArr[i]);
//				System.exit(-1);
//			}
//		}
//		
//		Class clz = String.class;
//		
//		Field[] fields = clz.getDeclaredFields();
//		for(Field f: fields){
//			System.out.println(f.getName()+" : "+f.getType());
//		}
//		
//		String test = "TESTSTRING";
//        Field field1 = String.class.getDeclaredField("offset");
//        field1.setAccessible(true);
//		System.out.println("offset="+field1.getInt(test));
//		
//        Field field2 = String.class.getDeclaredField("count");
//        field2.setAccessible(true);
//		System.out.println("count="+field2.getInt(test));
//        Field field3 = String.class.getDeclaredField("hash");
//        field3.setAccessible(true);		
//		System.out.println("hash="+field3.getInt(test));
//		
//		Field valueField = String.class.getDeclaredField("value");
//		
//		String s2 = new String();
//		
//		long valueOffset  = unsafe.objectFieldOffset(valueField);
//		long countOffset  = unsafe.objectFieldOffset(field2);
//		long offsetOffset = unsafe.objectFieldOffset(field1);
//		
//		unsafe.putInt(s2, countOffset, unsafe.getInt(test, countOffset));
//		unsafe.putInt(s2, offsetOffset, unsafe.getInt(test, offsetOffset));
//		unsafe.putObject(s2, valueOffset, unsafe.getObject(test, valueOffset));
//		
//		System.out.println(s2);
//		
		//testByteBuffer();
		//testDirectByteBuffer();
		//mmapTest();
		//lcpTest();
		//testLCP();
		//testSort();
		//longCopyTest();
		//testSortJavaPerf();
		//testSort();
	}
	
	/**
	 * Long copy test.
	 */
	@SuppressWarnings("unused")
  private static void longCopyTest(){
		int N = 33;
		long [] arr = new long[N];
		for(int i =0; i< N; i++) arr[i] = i;
		Unsafe unsafe = UnsafeAccess.getUnsafe();
		long ptr = unsafe.allocateMemory(N*8);
		Utils.memcpy(arr, 0, N, ptr);
		
		dumpLongArray(ptr, N);
		for(int i =0; i< N; i++) arr[i] = 0;
		
		Utils.memcpy(ptr, arr, 0, N);
		
		for(int i =0; i< N; i++) System.out.println(arr[i]);
		
	}
	
//	private static void testSortJava()
//	{
//		int N = 33;
//		int M = 1000000;
//		Unsafe unsafe = UnsafeAccess.getUnsafe();
//		long src = unsafe.allocateMemory(N * 8);
//		
//		Random r = new Random();
//		for(int i= 0; i < N ; i ++){
//			unsafe.putLong(src + i * 8, r.nextInt(M));
//		}
//		System.out.println("Array:");
//		dumpLongArray(src, N);
//		
//		long[] sortedArr = Utils.sort(src, N);
//		
//		System.out.println("Sorted:");
//		
//		for(int i=0; i < N; i++){
//			System.out.println(sortedArr[i]);
//		}
//	}
//	
//	private static void testSortJavaPerf()
//	{
//		int N = 1000;
//		int M = 100000;
//		Unsafe unsafe = UnsafeAccess.getUnsafe();
//		long src = unsafe.allocateMemory(N * 8);
//		long[] arr = new long[N*8];
//		Random r = new Random();
//		for(int i= 0; i < N ; i ++){
//			unsafe.putLong(src + i * 8, r.nextInt(Integer.MAX_VALUE -2));
//		}
//		System.out.println("Start sort");
//		//dumpLongArray(src, N);
//		long start = System.currentTimeMillis();
//		for(int i=0; i < M; i++ ){
//			//long[] sortedArr = 
//				Utils.sort(src, N);
//		}
//		
//		System.out.println("Sorted "+M+"X"+N+" in "+(System.currentTimeMillis() - start)+"ms");
//		
//		//for(int i=0; i < N; i++){
//		//	System.out.println(sortedArr[i]);
//		//}
//	}
	
//	private static void testSort()
//	{
//		int N = 1000;
//		int M = 100000;
//		Unsafe unsafe = UnsafeAccess.getUnsafe();
//		long src = unsafe.allocateMemory(N * 8);
//		long dst = unsafe.allocateMemory(N * 8);
//		Random r = new Random();
//		for(int i= 0; i < N ; i ++){
//			unsafe.putLong(src + i * 8, r.nextInt(M));
//		}
//		//System.out.println("Array:");
//		//dumpLongArray(src, N);
//		long t = System.currentTimeMillis();
//		for(int i=0; i < M; i++){
//			long sorted = Utils.sort(src, dst, N);
//		}
//		
//		System.out.println("Sorted in "+(System.currentTimeMillis() - t)+"ms");
//		//dumpLongArray(sorted, N);
//	}
	
	/**
 * Dump long array.
 *
 * @param ptr the ptr
 * @param size the size
 */
private static void dumpLongArray(long ptr, int size)
	{
		Unsafe unsafe = UnsafeAccess.getUnsafe();
		for(int i=0; i < size; i++){
			System.out.println(unsafe.getLong(ptr + i * 8));
		}
	}
	
	/**
	 * Test byte buffer.
	 *
	 * @throws SecurityException the security exception
	 * @throws NoSuchFieldException the no such field exception
	 * @throws IllegalArgumentException the illegal argument exception
	 * @throws IllegalAccessException the illegal access exception
	 */
	static void testByteBuffer() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		System.out.println("Byte Buffer Test");
		ByteBuffer bf = ByteBuffer.allocate(16);
		bf.put("0123456789012345".getBytes());
		Field[] fields = bf.getClass().getSuperclass().getDeclaredFields();
		for(Field f: fields){
			System.out.println(f.getName()+" : "+f.getType());
		}
		Field hb = java.nio.ByteBuffer.class.getDeclaredField("hb");
		hb.setAccessible(true);
		byte[] buf = (byte[])hb.get(bf);
		System.out.println(new String(buf));
		
		

		
		System.out.println("Byte Buffer Test Done");
	}

	/**
	 * Test direct byte buffer.
	 *
	 * @throws SecurityException the security exception
	 * @throws NoSuchFieldException the no such field exception
	 * @throws IllegalArgumentException the illegal argument exception
	 * @throws IllegalAccessException the illegal access exception
	 */
	@SuppressWarnings("unchecked")
  static void testDirectByteBuffer() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		System.out.println("Direct Byte Buffer Test");
		ByteBuffer bf = ByteBuffer.allocateDirect(16);
		
		bf.put("0123456789012345".getBytes());
		
		Field[] fields = bf.getClass().getDeclaredFields();
		for(Field f: fields){
			System.out.println(f.getName()+" : "+f.getType());
		}
		Field hb = bf.getClass().getDeclaredField("arrayBaseOffset");
		hb.setAccessible(true);
		Long buf = (Long) hb.get(bf);
		System.out.println(buf);

		
		System.out.println(bf.getClass().getSuperclass().getName());
		Class clz = bf.getClass().getSuperclass();
		fields = clz.getDeclaredFields();
		for(Field f: fields){
			System.out.println(f.getName()+" : "+f.getType());
		}
		
		clz = clz.getSuperclass();
		System.out.println(clz.getName());
		fields = clz.getDeclaredFields();
		for(Field f: fields){
			System.out.println(f.getName()+" : "+f.getType());
		}

		clz = clz.getSuperclass();
		System.out.println(clz.getName());
		fields = clz.getDeclaredFields();
		for(Field f: fields){
			System.out.println(f.getName()+" : "+f.getType());
		}
		Field addr = java.nio.Buffer.class.getDeclaredField("address");
		addr.setAccessible(true);
		Long address = addr.getLong(bf);
		System.out.println("address ="+address);
		Unsafe unsafe = UnsafeAccess.getUnsafe();
		for(int i=0; i < 32; i++) System.out.print(unsafe.getByte(address+i)+" ");
		System.out.println();
		System.out.println("Direct Byte Buffer Test Done");
	}
	
	/**
	 * Lcp test.
	 */
	@SuppressWarnings("unused")
  private static void lcpTest()
	{
		long a = 0xffffffffffffffffL;
		
		long b = a;
		
		System.out.println("LCP of "+ Long.toHexString(a) +" and " + Long.toHexString(b)+" ="+(Long.numberOfLeadingZeros( a ^ b)/8));
		
		b = a ^ 0xfeL;
		
		System.out.println("LCP of "+ Long.toHexString(a) +" and " + Long.toHexString(b)+" ="+(Long.numberOfLeadingZeros( a ^ b)/8));
		
		b = a ^ 0xfefeL;
		
		System.out.println("LCP of "+ Long.toHexString(a) +" and " + Long.toHexString(b)+" ="+(Long.numberOfLeadingZeros( a ^ b)/8));

		b = a ^ 0xfefefeL;
		
		System.out.println("LCP of "+ Long.toHexString(a) +" and " + Long.toHexString(b)+" ="+(Long.numberOfLeadingZeros( a ^ b)/8));
		
		b = a ^ 0xfefefefeL;
		
		System.out.println("LCP of "+ Long.toHexString(a) +" and " + Long.toHexString(b)+" ="+(Long.numberOfLeadingZeros( a ^ b)/8));

		b = a ^ 0xfefefefefeL;
		
		System.out.println("LCP of "+ Long.toHexString(a) +" and " + Long.toHexString(b)+" ="+(Long.numberOfLeadingZeros( a ^ b)/8));

		b = a ^ 0xfefefefefefeL;
		
		System.out.println("LCP of "+ Long.toHexString(a) +" and " + Long.toHexString(b)+" ="+(Long.numberOfLeadingZeros( a ^ b)/8));
		b = a ^ 0xfefefefefefefeL;
		
		System.out.println("LCP of "+ Long.toHexString(a) +" and " + Long.toHexString(b)+" ="+(Long.numberOfLeadingZeros( a ^ b)/8));
		b = a ^ 0xfefefefefefefefeL;
		
		System.out.println("LCP of "+ Long.toHexString(a) +" and " + Long.toHexString(b)+" ="+(Long.numberOfLeadingZeros( a ^ b)/8));
		
		int x = 8;
		int y = 3;
		int m = x *( 1 - (x-y)>>>31) + y*((x-y)>>>31);
		System.out.println("m ="+m);
		x = 18;
		y = 3890;
		m = x *( 1 - (y-x)>>>31) + y*((y-x)>>>31);
		
		x = 18000;
		y = 3890;
		m = x *( 1 - (y-x)>>>31) + y*((y-x)>>>31);
		System.out.println("m ="+m);
	}
	
	
//	public static void testLCP(){
//		
//		int N = 100000;
//		int M = 10000;
//		Unsafe unsafe = getUnsafe();
//		long ptr = unsafe.allocateMemory(N * 8);
//		for(int i = 0; i < N; i++){
//			unsafe.putLong(ptr + i *8, i);
//		}
//		int prefix = 0;
//		long t = System.currentTimeMillis();
//		for(int i=0; i < M; i++){
//			prefix = Utils.largestCommonPrefixBytes(ptr, N);
//		}
//		long tt = System.currentTimeMillis();
//		
//		System.out.println("Prefix="+prefix+" Time="+(tt-t)+" for "+(N*M));
//		
//		int c = 250;
//		byte b = (byte)c;
//		System.out.println(b+" "+(b & 0xff));
//		
//	}
	
	
	
	
	/**
 * Mmap test.
 *
 * @throws IOException Signals that an I/O exception has occurred.
 * @throws SecurityException the security exception
 * @throws NoSuchFieldException the no such field exception
 * @throws IllegalArgumentException the illegal argument exception
 * @throws IllegalAccessException the illegal access exception
 */
@SuppressWarnings("unused")
private static void mmapTest() 
		throws IOException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException
	{
		System.out.println("\nmmap test \n");
		File zero = new File("/dev/null");
		FileInputStream fis = new FileInputStream(zero);
		ByteBuffer buf = fis.getChannel().map(FileChannel.MapMode.PRIVATE, 0, 1024*1024);
		Field addr = java.nio.Buffer.class.getDeclaredField("address");
		addr.setAccessible(true);
		Long address = addr.getLong(buf);
		System.out.println("address ="+address);
		Unsafe unsafe = UnsafeAccess.getUnsafe();
		for(int i=0; i < 32; i++) System.out.print(unsafe.getByte(address+i)+" ");
		System.out.println("mmap done");
	}
	
	/**
	 * To address.
	 *
	 * @param obj the obj
	 * @return the long
	 */
	static long toAddress(Object obj) {
	    Object[] array = new Object[] {obj};
	    long baseOffset = getUnsafe().arrayBaseOffset(Object[].class);
	    return getUnsafe().getLong(array, baseOffset);
	}

	/**
	 * From address.
	 *
	 * @param address the address
	 * @return the object
	 */
	static Object fromAddress(long address) {
	    Object[] array = new Object[] {null};
	    long baseOffset = getUnsafe().arrayBaseOffset(Object[].class);
	    getUnsafe().putLong(array, baseOffset, address);
	    return array[0];
	}
	
	/**
	 * Gets the unsafe.
	 *
	 * @return the unsafe
	 */
	static Unsafe getUnsafe(){
		return UnsafeAccess.getUnsafe();
	}

}
