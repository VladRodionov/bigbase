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
package org.yamm.util;

import java.lang.reflect.Field;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import sun.misc.Unsafe;

import com.koda.common.util.UnsafeAccess;

// TODO: Auto-generated Javadoc
/**
 * The Class Utils.
 */
public class Utils {

	/** The Constant unsafe. */
	final static Unsafe unsafe = UnsafeAccess.getUnsafe();
	
		
	private static boolean copyMemorySupport = true;
	
	static{
        try {
        	     unsafe.getClass().getDeclaredMethod(
        	                   "copyMemory",
        	                    new Class[] { Object.class, long.class, Object.class, long.class, long.class });
        	  
        	System.out.println("sun.misc.Unsafe.copyMemory: available");
        } catch (NoSuchMethodError t) {
        	System.out.println("sun.misc.Unsafe.copyMemory: unavailable");       	     
        	copyMemorySupport = false;
        } catch (SecurityException e) {
          e.printStackTrace();
		} catch (NoSuchMethodException e) {
			//e.printStackTrace();
			System.out.println("sun.misc.Unsafe.copyMemory: unavailable");
			copyMemorySupport = false;
		}
	
	}
	
	/** The Constant BYTE_ARRAY_OFFSET. */
	public final static int BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
	
	/** The Constant LONG_ARRAY_OFFSET. */
	public final static int LONG_ARRAY_OFFSET = unsafe.arrayBaseOffset(long[].class);
	
	/** The Constant CHAR_ARRAY_OFFSET. */
	public final static int CHAR_ARRAY_OFFSET = unsafe.arrayBaseOffset(char[].class);
	
	/** The Constant SHORT_ARRAY_OFFSET. */
	public final static int SHORT_ARRAY_OFFSET = unsafe.arrayBaseOffset(short[].class);
	
	/** The Constant INT_ARRAY_OFFSET. */
	public final static int INT_ARRAY_OFFSET = unsafe.arrayBaseOffset(int[].class);
	
	/** The Constant BOOLEAN_ARRAY_OFFSET. */
	public final static int BOOLEAN_ARRAY_OFFSET = unsafe.arrayBaseOffset(boolean[].class);
	
	/** The Constant FLOAT_ARRAY_OFFSET. */
	public final static int FLOAT_ARRAY_OFFSET = unsafe.arrayBaseOffset(float[].class);
	
	/** The Constant DOUBLE_ARRAY_OFFSET. */
	public final static int DOUBLE_ARRAY_OFFSET = unsafe.arrayBaseOffset(double[].class);
	// ByteBuffer support
	/** The direct buffer address field. */
	public static Field 	DIRECT_BUFFER_ADDRESS_FIELD;	
	
	/** The byte buffer array field. */
	public static Field    BYTE_BUFFER_ARRAY_FIELD;
	
	/** The string buffer field. */
	public static Field    STRING_BUFFER_FIELD;
	
	static{
		try {
			DIRECT_BUFFER_ADDRESS_FIELD = java.nio.Buffer.class.getDeclaredField("address");			
			BYTE_BUFFER_ARRAY_FIELD = java.nio.ByteBuffer.class.getDeclaredField("hb");
			DIRECT_BUFFER_ADDRESS_FIELD.setAccessible(true);
			BYTE_BUFFER_ARRAY_FIELD.setAccessible(true);
			STRING_BUFFER_FIELD = String.class.getDeclaredField("value");
			STRING_BUFFER_FIELD.setAccessible(true);
			
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
		
	/**
	 * Copy data from byte array to memory location.
	 *
	 * @param arr the arr
	 * @param off the off
	 * @param size the size
	 * @param memory the memory
	 */
	public static void memcpy(byte[] arr, int off, int size, long memory)
	{
		if(copyMemorySupport){
			memcpyUnsafe(arr, off, size, memory);
		} else{
			memcpySafe(arr, off, size, memory);
		}
	}

	private static void memcpySafe(byte[] arr, int off, int size, long memory) {
		
		//System.out.println("byte[] safe copy");
		for(int i= off; i < size + off; i++ ){
			unsafe.putByte( memory + i - off, arr[i]);
		}		
	}

	private static void memcpyUnsafe(byte[] arr, int off, int size, long memory)
	{
		unsafe.copyMemory(arr, BYTE_ARRAY_OFFSET + off, null, memory, size);		
	}
	
	public static void copyDirectToHeap(ByteBuffer src, ByteBuffer dst) throws IllegalArgumentException, IllegalAccessException
	{
	  if( dst.remaining() < src.remaining()) throw new BufferUnderflowException();
	  int srcPos = src.position();
	  int len    = src.remaining();
	  int dstPos = dst.position();
	  if(src.isDirect() == false || dst.isDirect() == true) throw new IllegalArgumentException();
	  
	  long srcAddress = DIRECT_BUFFER_ADDRESS_FIELD.getLong(src);
	  byte[] buffer = dst.array();
	  int dstOffset = dst.arrayOffset();
	  memcpy(srcAddress + srcPos, buffer, dstOffset + dstPos, len);
	  
	}
	
	 public static void copyHeapToDirect(ByteBuffer src, ByteBuffer dst) throws IllegalArgumentException, IllegalAccessException
	  {
	    if( dst.remaining() < src.remaining()) throw new BufferUnderflowException();
	    int srcPos = src.position();
	    int len    = src.remaining();
	    int dstPos = dst.position();
	    if(src.isDirect() == true || dst.isDirect() == false) throw new IllegalArgumentException();
	    
	    long dstAddress = DIRECT_BUFFER_ADDRESS_FIELD.getLong(src);
	    byte[] buffer = src.array();
	    int srcOffset = src.arrayOffset();
	    memcpy(buffer, srcOffset + srcPos, len, dstAddress + dstPos);
	    
	  }
	/**
	 * Memcpy.
	 *
	 * @param memory the memory
	 * @param arr the arr
	 * @param off the off
	 * @param size the size
	 */
	public static void memcpy(long memory, byte[] arr, int off, int size )
	{
		if(copyMemorySupport){
			memcpyUnsafe(memory, arr, off, size);
		} else{
			memcpySafe(memory, arr, off, size);
		}
	}
	
	private static void memcpySafe(long memory, byte[] arr, int off, int size) {
		
		for(int i=0; i < size; i++){
			arr[off + i] = unsafe.getByte(memory + i);
		}
		
	}

	private static void memcpyUnsafe(long memory, byte[] arr, int off, int size )
	{
		unsafe.copyMemory( null, memory, arr, BYTE_ARRAY_OFFSET + off, size);
	}
	
	/**
	 * Memcpy.
	 *
	 * @param arr the arr
	 * @param off the off
	 * @param size the size
	 * @param memory the memory
	 */
	public static void memcpy(long[] arr, int off, int size, long memory)
	{
		if(copyMemorySupport){
			memcpyUnsafe(arr, off, size, memory);
		} else{
			memcpySafe(arr, off, size, memory);
		}		
	}
	
	
	private static void memcpySafe(long[] arr, int off, int size, long memory) {
		
		for(int i= off; i < size + off; i++ ){
			unsafe.putLong( memory + (i - off) * 8, arr[i]);
		}		
	}

	private static void memcpyUnsafe(long[] arr, int off, int size, long memory)
	{
		unsafe.copyMemory(arr, LONG_ARRAY_OFFSET + off * 8, null, memory, size*8);
	}	
	
	/**
	 * Memcpy.
	 *
	 * @param memory the memory
	 * @param arr the arr
	 * @param off the off
	 * @param size the size
	 */
	public static void memcpy(long memory, long[] arr, int off, int size )
	{
		if(copyMemorySupport){
			memcpyUnsafe(memory, arr, off, size);
		} else{
			memcpySafe(memory, arr, off, size);
		}	
	}
	
	public static void memcpySafe(long memory, long[] arr, int off, int size )
	{
		for(int i =0; i < size; i++){
			arr[off + i] = unsafe.getLong(memory + i * 8);
		}		
	}
	
	private static void memcpyUnsafe(long memory, long[] arr, int off, int size )
	{
		unsafe.copyMemory( null, memory, arr, LONG_ARRAY_OFFSET + off*8, size*8);
	}
	
	/**
	 * Memcpy.
	 *
	 * @param arr the arr
	 * @param off the off
	 * @param size the size
	 * @param memory the memory
	 */
	public static void memcpy(short[] arr, int off, int size, long memory)
	{

		if(copyMemorySupport){
			memcpyUnsafe( arr, off, size, memory );
		} else{
			memcpySafe( arr, off, size, memory );
		}	
	}
	
	private static void memcpySafe(short[] arr, int off, int size, long memory) {
		for(int i= off; i < size + off; i++ ){
			unsafe.putShort( memory + (i - off) * 2, arr[i]);
		}
		
	}

	private static void memcpyUnsafe(short[] arr, int off, int size, long memory) {
		unsafe.copyMemory(arr, SHORT_ARRAY_OFFSET + off * 2, null, memory, size * 2);
		
	}

	/**
	 * Memcpy.
	 *
	 * @param memory the memory
	 * @param arr the arr
	 * @param off the off
	 * @param size the size
	 */
	public static void memcpy(long memory, short[] arr, int off, int size )
	{
		if(copyMemorySupport){
			memcpyUnsafe( memory, arr, off, size );
		} else{
			memcpySafe( memory, arr, off, size);
		}	
	}

	private static void memcpySafe(long memory, short[] arr, int off, int size) {
		for(int i =0; i < size; i++){
			arr[off + i] = unsafe.getShort(memory + i * 2);
		}
		
	}

	private static void memcpyUnsafe(long memory, short[] arr, int off, int size) {
		unsafe.copyMemory( null, memory, arr, SHORT_ARRAY_OFFSET + off * 2, size * 2);		
	}

	/**
	 * Memcpy.
	 *
	 * @param arr the arr
	 * @param off the off
	 * @param size the size
	 * @param memory the memory
	 */
	public static void memcpy(char[] arr, int off, int size, long memory)
	{

		if(copyMemorySupport){
			memcpyUnsafe( arr, off, size, memory );
		} else{
			memcpySafe( arr, off, size, memory );
		}	
	}
	
	private static void memcpySafe(char[] arr, int off, int size, long memory) {
		for(int i= off; i < size + off; i++ ){
			unsafe.putChar( memory + (i - off) * 2, arr[i]);
		}
		
	}

	private static void memcpyUnsafe(char[] arr, int off, int size, long memory) {
		unsafe.copyMemory(arr, CHAR_ARRAY_OFFSET + off * 2, null, memory, size * 2);
		
	}
	
	/**
	 * Memcpy.
	 *
	 * @param memory the memory
	 * @param arr the arr
	 * @param off the off
	 * @param size the size
	 */
	public static void memcpy(long memory, char[] arr, int off, int size )
	{
		if(copyMemorySupport){
			memcpyUnsafe( memory, arr, off, size );
		} else{
			memcpySafe( memory, arr, off, size);
		}	
	}

	private static void memcpySafe(long memory, char[] arr, int off, int size) {
		for(int i =0; i < size; i++){
			arr[off + i] = unsafe.getChar(memory + i * 2);
		}
		
	}

	private static void memcpyUnsafe(long memory, char[] arr, int off, int size) {
		unsafe.copyMemory( null, memory, arr, CHAR_ARRAY_OFFSET + off * 2, size * 2);		
	}
	
	/**
	 * Memcpy.
	 *
	 * @param arr the arr
	 * @param off the off
	 * @param size the size
	 * @param memory the memory
	 */
	public static void memcpy(int[] arr, int off, int size, long memory)
	{

		if(copyMemorySupport){
			memcpyUnsafe( arr, off, size, memory );
		} else{
			memcpySafe( arr, off, size, memory );
		}	
	}
	
	private static void memcpyUnsafe(int[] arr, int off, int size, long memory) {
		unsafe.copyMemory(arr, INT_ARRAY_OFFSET + off * 4, null, memory, size * 4);		
	}

	private static void memcpySafe(int[] arr, int off, int size, long memory) {
		for(int i= off; i < size + off; i++ ){
			unsafe.putInt( memory + (i - off) * 4, arr[i]);
		}
		
	}

	public static void memcpy(long memory, int[] arr, int off, int size )
	{
		if(copyMemorySupport){
			memcpyUnsafe( memory, arr, off, size );
		} else{
			memcpySafe( memory, arr, off, size);
		}	
	}

	private static void memcpySafe(long memory, int[] arr, int off, int size) {
		for(int i =0; i < size; i++){
			arr[off + i] = unsafe.getInt(memory + i * 4);
		}
		
	}

	private static void memcpyUnsafe(long memory, int[] arr, int off, int size) {
		unsafe.copyMemory( null, memory, arr, INT_ARRAY_OFFSET + off * 4, size * 4);		
	}
	


	
//	/**
//	 * Memcpy.
//	 *
//	 * @param arr the arr
//	 * @param off the off
//	 * @param size the size
//	 * @param memory the memory
//	 */
//	public static void memcpy(float[] arr, int off, int size, long memory)
//	{
//		unsafe.copyMemory(arr, FLOAT_ARRAY_OFFSET + off * 4, null, memory, size*4);
//	}
//	
//	/**
//	 * Memcpy.
//	 *
//	 * @param memory the memory
//	 * @param arr the arr
//	 * @param off the off
//	 * @param size the size
//	 */
//	public static void memcpy(long memory, float[] arr, int off, int size )
//	{
//		unsafe.copyMemory( null, memory, arr, FLOAT_ARRAY_OFFSET + off*4, size*4);
//	}	
//
//	/**
//	 * Memcpy.
//	 *
//	 * @param arr the arr
//	 * @param off the off
//	 * @param size the size
//	 * @param memory the memory
//	 */
//	public static void memcpy(double[] arr, int off, int size, long memory)
//	{
//		unsafe.copyMemory(arr, DOUBLE_ARRAY_OFFSET + off * 8, null, memory, size*8);
//	}
//	
//	/**
//	 * Memcpy.
//	 *
//	 * @param memory the memory
//	 * @param arr the arr
//	 * @param off the off
//	 * @param size the size
//	 */
//	public static void memcpy(long memory, double[] arr, int off, int size )
//	{
//		unsafe.copyMemory( null, memory, arr, DOUBLE_ARRAY_OFFSET + off*8, size*8);
//	}	
//	
//	/**
//	 * ByteBuffer operations.
//	 *
//	 * @param buf the buf
//	 * @param off the off
//	 * @param size the size
//	 * @param memory the memory
//	 */
	
	public static void memcpy(ByteBuffer buf, int off, int size, long memory)
	{
		if(buf.isDirect()){
			try {
				long ptr = DIRECT_BUFFER_ADDRESS_FIELD.getLong(buf);
				unsafe.copyMemory(ptr + off, memory, size);
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else{
			try {
				byte[] bbuf = (byte[])BYTE_BUFFER_ARRAY_FIELD.get(buf);
				memcpy(bbuf, off, size, memory);
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static long getBufferAddress(ByteBuffer buf){
		if(buf.isDirect()){
			try {
				return DIRECT_BUFFER_ADDRESS_FIELD.getLong(buf);
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		return 0L;
	}
	
	public static long getAlignedOffset (ByteBuffer buf, int align){
	  long address = getBufferAddress(buf);
	  if(address == 0) return -1;
	  long res = address % align;
	  return (align - res) % align;
	}
	
	public static long getBufferAddress(LongBuffer buf){
		if(buf.isDirect()){
			try {
				return DIRECT_BUFFER_ADDRESS_FIELD.getLong(buf);
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		return 0L;
	}
	
	/**
	 * Memcpy.
	 *
	 * @param memory the memory
	 * @param buf the buf
	 * @param off the off
	 * @param size the size
	 */
	public static void memcpy(long memory, ByteBuffer buf, int off, int size)
	{
		if(buf.isDirect()){
			try {
				long ptr = DIRECT_BUFFER_ADDRESS_FIELD.getLong(buf);
				unsafe.copyMemory(memory , ptr + off, size);
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else{
			try {
				byte[] bbuf = (byte[])BYTE_BUFFER_ARRAY_FIELD.get(buf);
				memcpy(memory, bbuf, off, size);
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * String operations.
	 *
	 * @param s the s
	 * @param off the off
	 * @param size the size
	 * @param memory the memory
	 */
	
	public static void memcpy(String s, int off, int size, long memory)
	{
		try {
			char[] value = (char[])STRING_BUFFER_FIELD.get(s);
			unsafe.putInt(memory, size);
			memcpy(value, off, size, memory + 4);
			
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Read string.
	 *
	 * @param memory the memory
	 * @return the string
	 */
	public static String readString(long memory)
	{
		char[] value = new char[unsafe.getInt(memory)];
		memcpy(memory+4, value, 0, value.length);
		return new String(value);
	}
	
	/**
	 * Primitive types operations.
	 *
	 * @param memory the memory
	 * @param b the b
	 */

	
	public static void putByte(long memory, byte b)
	{
		unsafe.putByte(memory, b);
	}
	
	/**
	 * Gets the byte.
	 *
	 * @param memory the memory
	 * @return the byte
	 */
	public static byte getByte(long memory)
	{
		return unsafe.getByte(memory);
	}
	
	/**
	 * Put u byte.
	 *
	 * @param memory the memory
	 * @param b the b
	 */
	public static void putUByte(long memory, short b)
	{
		unsafe.putByte(memory, (byte) b);
	}
	
	/**
	 * Gets the u byte.
	 *
	 * @param memory the memory
	 * @return the u byte
	 */
	public static short getUByte(long memory)
	{
		return (short) (unsafe.getByte(memory) & 0xff);
	}
	
	/**
	 * Put char.
	 *
	 * @param memory the memory
	 * @param b the b
	 */
	public static void putChar(long memory, char b)
	{
		unsafe.putChar(memory, b);
	}
	
	/**
	 * Gets the char.
	 *
	 * @param memory the memory
	 * @return the char
	 */
	public static char getChar(long memory)
	{
		return unsafe.getChar(memory);
	}
	
	/**
	 * Put u char.
	 *
	 * @param memory the memory
	 * @param b the b
	 */
	public static void putUChar(long memory, int b)
	{
		unsafe.putChar(memory, (char) b);
	}
	
	/**
	 * Gets the u char.
	 *
	 * @param memory the memory
	 * @return the u char
	 */
	public static int getUChar(long memory)
	{
		return (int) (unsafe.getChar(memory) & 0xffff);
	}
	
	/**
	 * Put short.
	 *
	 * @param memory the memory
	 * @param b the b
	 */
	public static void putShort(long memory, short b)
	{
		unsafe.putShort(memory, b);
	}
	
	/**
	 * Gets the short.
	 *
	 * @param memory the memory
	 * @return the short
	 */
	public static short getShort(long memory)
	{
		return unsafe.getShort(memory);
	}
	
	/**
	 * Put u short.
	 *
	 * @param memory the memory
	 * @param b the b
	 */
	public static void putUShort(long memory, int b)
	{
		unsafe.putShort(memory, (short) b);
	}
	
	/**
	 * Gets the u short.
	 *
	 * @param memory the memory
	 * @return the u short
	 */
	public static int getUShort(long memory)
	{
		return (int) (unsafe.getShort(memory) & 0xffff);
	}	
	
	/**
	 * Put int.
	 *
	 * @param memory the memory
	 * @param b the b
	 */
	public static void putInt(long memory, int b)
	{
		unsafe.putInt(memory, b);
	}
	
	/**
	 * Gets the int.
	 *
	 * @param memory the memory
	 * @return the int
	 */
	public static int getInt(long memory)
	{
		return unsafe.getInt(memory);
	}
	
	/**
	 * Put u int.
	 *
	 * @param memory the memory
	 * @param b the b
	 */
	public static void putUInt(long memory, long b)
	{
		unsafe.putInt(memory, (int) b);
	}
	
	/**
	 * Gets the u int.
	 *
	 * @param memory the memory
	 * @return the u int
	 */
	public static long getUInt(long memory)
	{
		int v = unsafe.getInt(memory);
		return  v & 0xffffffffL;//(0xffffffffL + 1)* (v >>> 31) + v ;
	}	

	/**
	 * Put long.
	 *
	 * @param memory the memory
	 * @param b the b
	 */
	public static void putLong(long memory, long b)
	{
		unsafe.putLong(memory, b);
	}
	
	/**
	 * Gets the long.
	 *
	 * @param memory the memory
	 * @return the long
	 */
	public static long getLong(long memory)
	{
		return unsafe.getLong(memory);
	}	

	/**
	 * Put float.
	 *
	 * @param memory the memory
	 * @param b the b
	 */
	public static void putFloat(long memory, float b)
	{
		unsafe.putFloat(memory, b);
	}
	
	/**
	 * Gets the float.
	 *
	 * @param memory the memory
	 * @return the float
	 */
	public static float getFloat(long memory)
	{
		return unsafe.getFloat(memory);
	}	
	
	/**
	 * Put double.
	 *
	 * @param memory the memory
	 * @param b the b
	 */
	public static void putDouble(long memory, double b)
	{
		unsafe.putDouble(memory, b);
	}
	
	/**
	 * Gets the double.
	 *
	 * @param memory the memory
	 * @return the double
	 */
	public static double getDouble(long memory)
	{
		return unsafe.getDouble(memory);
	}
	
	/**
	 * Miscellaneous.
	 *
	 * @param size the size
	 * @return the long
	 */
	
	public static long malloc(long size)
	{
		return unsafe.allocateMemory(size);
	}
	
	/**
	 * Free.
	 *
	 * @param ptr the ptr
	 */
	public static void free(long ptr)
	{
		System.out.println(ptr);
		unsafe.freeMemory(ptr);
	}
	
	/**
	 * Memset.
	 *
	 * @param arr the arr
	 * @param v the v
	 */
	public static void memset(byte[] arr, byte v)
	{
		for(int i=0; i < arr.length; i++){
			arr[i] = v;
		}
	}

	/**
	 * Memset.
	 *
	 * @param arr the arr
	 * @param v the v
	 */
	public static void memset(char[] arr, char v)
	{
		for(int i=0; i < arr.length; i++){
			arr[i] = v;
		}
	}
	
	
	/**
	 * Memset.
	 *
	 * @param arr the arr
	 * @param v the v
	 */
	public static void memset(short[] arr, short v)
	{
		for(int i=0; i < arr.length; i++){
			arr[i] = v;
		}
	}
	
	
	/**
	 * Memset.
	 *
	 * @param arr the arr
	 * @param v the v
	 */
	public static void memset(int[] arr, int v)
	{
		for(int i=0; i < arr.length; i++){
			arr[i] = v;
		}
	}

	
	/**
	 * Memset.
	 *
	 * @param arr the arr
	 * @param v the v
	 */
	public static void memset(float[] arr, float v)
	{
		for(int i=0; i < arr.length; i++){
			arr[i] = v;
		}
	}	

	/**
	 * Memset.
	 *
	 * @param arr the arr
	 * @param v the v
	 */
	public static void memset(double[] arr, double v)
	{
		for(int i=0; i < arr.length; i++){
			arr[i] = v;
		}
	}	
	
	
	/**
	 * Memset.
	 *
	 * @param arr the arr
	 * @param v the v
	 */
	public static void memset(long[] arr, long v)
	{
		for(int i=0; i < arr.length; i++){
			arr[i] = v;
		}
	}	
	
	/**
	 * Cmp.
	 *
	 * @param arr the arr
	 * @param v the v
	 * @return the int
	 */
	public static int cmp(byte[] arr, byte v)
	{
		for(int i=0; i < arr.length; i++){
			if( arr[i] > v) return 1; else if(arr[i] < v) return -1;
		}
		return 0;
	}
	

	/**
	 * Cmp.
	 *
	 * @param arr the arr
	 * @param v the v
	 * @return the int
	 */
	public static int cmp(char[] arr, char v)
	{
		for(int i=0; i < arr.length; i++){
			if( arr[i] > v) return 1; else if(arr[i] < v) return -1;
		}
		return 0;
	}
	
	/**
	 * Cmp.
	 *
	 * @param arr the arr
	 * @param v the v
	 * @return the int
	 */
	public static int cmp(short[] arr, short v)
	{
		for(int i=0; i < arr.length; i++){
			if( arr[i] > v) return 1; else if(arr[i] < v) return -1;
		}
		return 0;
	}
	
	/**
	 * Cmp.
	 *
	 * @param arr the arr
	 * @param v the v
	 * @return the int
	 */
	public static int cmp(int[] arr, int v)
	{
		for(int i=0; i < arr.length; i++){
			if( arr[i] > v) return 1; else if(arr[i] < v) return -1;
		}
		return 0;
	}	
	
	/**
	 * Cmp.
	 *
	 * @param arr the arr
	 * @param v the v
	 * @return the int
	 */
	public static int cmp(long[] arr, long v)
	{
		for(int i=0; i < arr.length; i++){
			if( arr[i] > v) return 1; else if(arr[i] < v) return -1;
		}
		return 0;
	}	
	
	/**
	 * Cmp.
	 *
	 * @param arr the arr
	 * @param v the v
	 * @return the int
	 */
	public static int cmp(float[] arr, float v)
	{
		for(int i=0; i < arr.length; i++){
			if( arr[i] > v) return 1; else if(arr[i] < v) return -1;
		}
		return 0;
	}
	
	/**
	 * Cmp.
	 *
	 * @param arr the arr
	 * @param v the v
	 * @return the int
	 */
	public static int cmp(double[] arr, double v)
	{
		for(int i=0; i < arr.length; i++){
			if( arr[i] > v) return 1; else if(arr[i] < v) return -1;
		}
		return 0;
	}

	/**
	 * TODO Real memcmp for Karma
	 * 
	 * @param src
	 * @param dst
	 * @param size
	 * @return
	 */
	public static int memcmp2(long src, long dst, int size) {
		int num_8 = size >>> 3;
		long end = src + num_8 * 8;
	    long stop = src + size;
		while( src < end){
	    	if( unsafe.getLong(src) != unsafe.getLong(dst)){
	    		return -1;
	    	}
	    	src +=8; dst +=8;
	    }
	    
	    while( src < stop){
	    	
	    	if(unsafe.getByte(src++) != unsafe.getByte(dst++)) return -1;
	    	
	    }
		return 0;
	}	
	
	public static long memcmp(long src, long dst, int size) {
		final int num_4 = size >>> 2;
		final long end = src + num_4 * 4;
	    final long stop = src + size;
		while( src < end){
			final long v1 = unsafe.getInt(src) & 0xffffffffL;
			final long v2 = unsafe.getInt(dst) & 0xffffffffL;
	    	if( v1 != v2){
	    		return v1 - v2;
	    	}
	    	src +=4; dst +=4;
	    }
	    
	    while( src < stop){
	    	final short s1 = (short)(unsafe.getByte(src++) & 0xff);
	    	final short s2 = (short)(unsafe.getByte(dst++) & 0xff);
	    	if(s1 != s2) return s1 -s2;
	    	
	    }
		return 0;
	}	
	
}
